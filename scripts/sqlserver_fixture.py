"""Disposable CI provisioning and verified local observation-fixture reuse."""

import argparse
import configparser
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import subprocess
import sys
import tempfile
import time

ENGINE_VERSION = "17.0.5005.3-1"
ENGINE_SHA256 = "2494143ed6b9921c078e56c0c3877e5014d6a998fc19957fe6857a34a737061e"
ENGINE_URL = (
    "https://packages.microsoft.com/ubuntu/24.04/mssql-server-2025/pool/main/"
    f"m/mssql-server/mssql-server_{ENGINE_VERSION}_amd64.deb"
)
SQLCMD_URL = (
    "https://github.com/microsoft/go-sqlcmd/releases/download/v1.10.0/"
    "sqlcmd-linux-amd64.tar.bz2"
)
SQLCMD_SHA256 = "92516d98c63d99b0994de5b61350c91f6915f9b76f139a59039fbcb225c2e987"
SQLCMD_BINARY_SHA256 = "5c043495deff92687243e4c337e09494d497557a0217bdca81afefb1e4a2b4ce"
SERVER_TLS = Path("/var/opt/mssql/secrets/kuberic-observer-ci")
OWNER = "kuberic-sqlserver-observer-ci\n"


class FixtureError(Exception):
    pass


def command(stage, args, *, env=None, stdin=None, check=True, timeout=180, process_group=False):
    try:
        if process_group:
            with subprocess.Popen(
                args, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, start_new_session=True,
            ) as process:
                try:
                    stdout, stderr = process.communicate(input=stdin, timeout=timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        print("The owned test command group has already exited.")
                    process.communicate(timeout=5)
                    raise
                result = subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
        else:
            result = subprocess.run(
                args, env=env, input=stdin, text=True, capture_output=True, timeout=timeout
            )
    except subprocess.TimeoutExpired as error:
        raise FixtureError(f"{stage} exceeded its deadline") from error
    if check and result.returncode:
        # Provisioning output can contain administrative SQL or driver payloads.
        raise FixtureError(f"{stage} failed with exit code {result.returncode}")
    return result


def runner_root():
    for name, expected in [
        ("GITHUB_ACTIONS", "true"),
        ("RUNNER_ENVIRONMENT", "github-hosted"),
        ("RUNNER_OS", "Linux"),
        ("RUNNER_ARCH", "X64"),
    ]:
        if os.environ.get(name) != expected:
            raise FixtureError("provisioning/cleanup requires a disposable GitHub-hosted Linux x64 runner")
    temporary = os.environ.get("RUNNER_TEMP", "")
    if not temporary or not Path(temporary).is_absolute() or any(c in temporary for c in "\r\n"):
        raise FixtureError("RUNNER_TEMP must be an absolute single-line path")
    root = Path(temporary).resolve() / "sqlserver-observer"
    if root.is_symlink():
        raise FixtureError("fixture directory must not be a symlink")
    return root


def verify_digest(path, expected):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise FixtureError(f"checksum mismatch for {path.name}")


def download(root, name, url, digest):
    destination = root / name
    command(
        f"download {name}",
        ["curl", "--fail", "--silent", "--show-error", "--location", "--retry", "3",
         "--max-time", "180", "--output", str(destination), url],
        timeout=600,
    )
    verify_digest(destination, digest)
    return destination


def secret_file(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(value)


def sql(root, text, username, password, *, check=True):
    env = os.environ.copy()
    env.update(SQLCMDPASSWORD=password, SSL_CERT_FILE=str(root / "ca.crt"))
    return command(
        "verified-TLS fixture administration",
        [str(root / "sqlcmd"), "-S", "localhost,1433", "-U", username,
         "-N", "true", "-b", "-l", "5", "-t", "30", "-h", "-1", "-W", "-s", "|"],
        env=env, stdin=text + "\nGO\n", check=check, timeout=40,
    )


def certificates(root):
    for prefix, subject in [
        ("ca", "Kuberic observer CI CA"),
        ("bad-ca", "Untrusted observer CI CA"),
    ]:
        command(
            "generate fixture CA",
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
             "-subj", f"/CN={subject}", "-addext", "basicConstraints=critical,CA:TRUE",
             "-keyout", str(root / f"{prefix}.key"), "-out", str(root / f"{prefix}.crt")],
        )
    command(
        "generate server key",
        ["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
         "-keyout", str(root / "server.key"), "-out", str(root / "server.csr")],
    )
    (root / "server.ext").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\nbasicConstraints=critical,CA:FALSE\n"
    )
    command(
        "sign server certificate",
        ["openssl", "x509", "-req", "-in", str(root / "server.csr"),
         "-CA", str(root / "ca.crt"), "-CAkey", str(root / "ca.key"), "-CAcreateserial",
         "-days", "2", "-extfile", str(root / "server.ext"), "-out", str(root / "server.crt")],
    )
    command("create server TLS directory", [
        "sudo", "-n", "install", "-d", "-o", "mssql", "-g", "mssql", "-m", "700", str(SERVER_TLS)
    ])
    for name in ["server.key", "server.crt"]:
        command("install server TLS file", [
            "sudo", "-n", "install", "-o", "mssql", "-g", "mssql", "-m", "600",
            str(root / name), str(SERVER_TLS / name)
        ])


def write_configs(root, server_name):
    config = {
        "mode": "observe_only",
        "host": "localhost",
        "port": 1433,
        "availability_group": "kuberic-ci-absent",
        "expected_server_name": server_name,
        "replica_id": "native-ci-1",
        "incarnation": secrets.token_hex(16),
        "observer_username_file": str(root / "observer-username"),
        "observer_password_file": str(root / "observer-password"),
        "ca_certificate_file": str(root / "ca.crt"),
    }
    for name in ["absent", "denied", "bad-tls"]:
        current = config.copy()
        if name == "denied":
            current["observer_username_file"] = str(root / "denied-username")
            current["observer_password_file"] = str(root / "denied-password")
        if name == "bad-tls":
            current["ca_certificate_file"] = str(root / "bad-ca.crt")
        (root / f"{name}.json").write_text(json.dumps(current) + "\n")


def fixture_environment(root):
    return {
        "SQLSERVER_TEST_EULA_ACCEPTED": "true",
        "SQLSERVER_TEST_PACKAGE_VERSION": ENGINE_VERSION,
        "SQLSERVER_TEST_PACKAGE_SHA256": ENGINE_SHA256,
        "SQLSERVER_LIVE_ABSENT_CONFIG": str(root / "absent.json"),
        "SQLSERVER_LIVE_DENIED_CONFIG": str(root / "denied.json"),
        "SQLSERVER_LIVE_BAD_TLS_CONFIG": str(root / "bad-tls.json"),
    }


def verify_loopback_binding():
    listeners = command("verify loopback binding", ["ss", "-H", "-ltn", "( sport = :1433 )"]).stdout
    rows = [line.split() for line in listeners.splitlines()]
    if not rows or any(len(row) < 4 or row[3] != "127.0.0.1:1433" for row in rows):
        raise FixtureError("SQL Server is not exclusively listening on loopback")


def install_fixture(root, context):
    release = Path("/etc/os-release").read_text()
    if 'ID=ubuntu\n' not in release or 'VERSION_ID="24.04"\n' not in release:
        raise FixtureError("the native fixture requires Ubuntu 24.04")
    if Path("/opt/mssql/bin/sqlservr").exists() or Path("/var/opt/mssql").exists():
        raise FixtureError("refusing to take over an existing SQL Server installation or storage")
    if command("check fixture port", ["ss", "-H", "-ltn", "( sport = :1433 )"]).stdout.strip():
        raise FixtureError("fixture port 1433 is already occupied")
    if root.exists() and (
        not root.is_dir() or root.stat().st_uid != os.getuid()
        or root.stat().st_mode & 0o077 or any(root.iterdir())
    ):
        raise FixtureError("new fixture directory must be empty, private and owned")
    os.umask(0o077)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    (root / "owner").write_text(OWNER)
    context["created"] = True
    save_context(root, context)
    engine = download(root, "mssql-server.deb", ENGINE_URL, ENGINE_SHA256)
    client = download(root, "sqlcmd.tar.bz2", SQLCMD_URL, SQLCMD_SHA256)
    command("update package metadata", ["sudo", "-n", "apt-get", "update", "-qq"], timeout=600)
    command(
        "install pinned engine",
        ["sudo", "-n", "env", "DEBIAN_FRONTEND=noninteractive", "apt-get", "install",
         "-y", "-qq", str(engine)], timeout=600,
    )
    installed = command(
        "verify installed engine", ["dpkg-query", "-W", "-f=${Version}", "mssql-server"]
    ).stdout
    if installed != ENGINE_VERSION:
        raise FixtureError("installed engine does not match the pinned package")
    engine.unlink()
    command("extract pinned SQL client", ["tar", "-xjf", str(client), "-C", str(root), "sqlcmd"])
    client.unlink()
    certificates(root)
    for setting, value in [
        ("network.ipaddress", "127.0.0.1"), ("network.tcpport", "1433"),
        ("network.tlscert", str(SERVER_TLS / "server.crt")),
        ("network.tlskey", str(SERVER_TLS / "server.key")),
        ("network.tlsprotocols", "1.2"), ("network.forceencryption", "1"),
        ("hadr.hadrenabled", "1"), ("memory.memorylimitmb", "2048"),
    ]:
        command("configure isolated engine", [
            "sudo", "-n", "/opt/mssql/bin/mssql-conf", "-q", "set", setting, value
        ])
    sa_password = secrets.token_urlsafe(32) + "Aa1!"
    env = os.environ.copy()
    env.update(ACCEPT_EULA="Y", MSSQL_PID="EnterpriseDeveloper", MSSQL_SA_PASSWORD=sa_password)
    activate_fixture(
        root, context,
        "initialize Enterprise Developer fixture",
        ["sudo", "-n", "--preserve-env=ACCEPT_EULA,MSSQL_PID,MSSQL_SA_PASSWORD",
         "/opt/mssql/bin/mssql-conf", "-n", "-q", "setup"], env=env,
    )
    command("disable automatic service startup", ["sudo", "-n", "systemctl", "disable", "mssql-server"])
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        ready = sql(root, "SET NOCOUNT ON; SELECT @@SERVERNAME;", "sa", sa_password, check=False)
        if ready.returncode == 0:
            server_name = ready.stdout.strip()
            if not server_name or any(c in server_name for c in "\r\n"):
                raise FixtureError("native server name is missing or malformed")
            break
        time.sleep(2)
    else:
        raise FixtureError("fixture did not become verified-TLS/login ready")
    verify_loopback_binding()
    for username, prefix, permitted in [
        ("kuberic_observer", "observer", True), ("kuberic_denied", "denied", False)
    ]:
        password = secrets.token_urlsafe(32) + "Aa1!"
        secret_file(root / f"{prefix}-username", username)
        secret_file(root / f"{prefix}-password", password)
        batch = f"CREATE LOGIN [{username}] WITH PASSWORD = N'{password}', CHECK_POLICY = ON;"
        if permitted:
            for permission in [
                "VIEW SERVER STATE", "VIEW SERVER PERFORMANCE STATE",
                "VIEW ANY DEFINITION", "VIEW ANY DATABASE",
            ]:
                batch += f"\nGRANT {permission} TO [{username}];"
        sql(root, batch, "sa", sa_password)
    write_configs(root, server_name)
    print(f"Provisioned SQL Server {ENGINE_VERSION} for three observe-only cases.")


def local_fixture(directory):
    if directory.is_symlink():
        raise FixtureError("local fixture directory must not be a symlink")
    try:
        root = directory.resolve(strict=True)
        metadata = root.stat()
        if not root.is_dir() or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
            raise FixtureError("local fixture must be a private directory owned by the current user")
        for name in [
            "absent.json", "denied.json", "bad-tls.json", "ca.crt", "bad-ca.crt", "server.crt",
            "observer-username", "observer-password", "denied-username", "denied-password", "sqlcmd",
        ]:
            path = root / name
            if path.is_symlink() or not path.is_file() or path.stat().st_uid != os.getuid():
                raise FixtureError("local fixture requires owned, regular, non-symlink files")
            if name.endswith(".json") and path.stat().st_size > 65_536:
                raise FixtureError("local fixture configuration exceeds 64 KiB")
        for name in ["observer-username", "observer-password", "denied-username", "denied-password"]:
            path = root / name
            if path.stat().st_mode & 0o077 or path.stat().st_size > 4096:
                raise FixtureError("local credential files must be private and bounded")
            value = path.read_text()
            if not value or "\n" in value or "\x00" in value:
                raise FixtureError("local credential files must contain exact nonempty UTF-8 values")
        configs = {
            name: json.loads((root / f"{name}.json").read_text())
            for name in ["absent", "denied", "bad-tls"]
        }
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise FixtureError("cannot read the configured local fixture files") from error
    for name, config in configs.items():
        username = "denied-username" if name == "denied" else "observer-username"
        password = "denied-password" if name == "denied" else "observer-password"
        ca = "bad-ca.crt" if name == "bad-tls" else "ca.crt"
        if not isinstance(config, dict) or any(
            config.get(key) != value
            for key, value in {
                "mode": "observe_only", "host": "localhost", "port": 1433,
                "observer_username_file": str(root / username),
                "observer_password_file": str(root / password),
                "ca_certificate_file": str(root / ca),
            }.items()
        ):
            raise FixtureError("local fixture must use its own credentials/CA and the loopback endpoint")
        if not isinstance(config.get("expected_server_name"), str) or not config["expected_server_name"]:
            raise FixtureError("local fixture requires an exact expected SQL Server identity")
        if any(not isinstance(config.get(key), str) or not config[key] for key in [
            "availability_group", "replica_id", "incarnation",
        ]):
            raise FixtureError("local fixture requires exact AG, replica and incarnation attribution")
    if any(
        config.get(key) != configs["absent"].get(key)
        for config in configs.values()
        for key in ["expected_server_name", "availability_group", "replica_id", "incarnation"]
    ):
        raise FixtureError("local fixture configurations must describe the same exact target")
    verify_digest(root / "sqlcmd", SQLCMD_BINARY_SHA256)
    return root, configs["absent"]["expected_server_name"]


def verify_local_service(root):
    installed = command(
        "verify local engine package", ["dpkg-query", "-W", "-f=${Version}", "mssql-server"]
    ).stdout
    if installed != ENGINE_VERSION:
        raise FixtureError("local engine does not match the pinned fixture package")
    settings = configparser.ConfigParser(interpolation=None)
    try:
        settings.read_string(command(
            "read local service settings", ["sudo", "-n", "cat", "/var/opt/mssql/mssql.conf"]
        ).stdout)
    except configparser.Error as error:
        raise FixtureError("local SQL Server service settings are malformed") from error
    for section, key, value in [
        ("network", "ipaddress", "127.0.0.1"), ("network", "tcpport", "1433"),
        ("network", "forceencryption", "1"), ("network", "tlsprotocols", "1.2"),
        ("hadr", "hadrenabled", "1"), ("memory", "memorylimitmb", "2048"),
    ]:
        if settings.get(section, key, fallback="") != value:
            raise FixtureError("refusing to manage a service outside the isolated project fixture profile")
    certificate = Path(settings.get("network", "tlscert", fallback=""))
    allowed = [
        Path("/var/opt/mssql/secrets/kuberic-observer"),
        Path("/var/opt/mssql/secrets/kuberic-observer-ci"),
    ]
    if certificate.parent not in allowed or certificate.name != "server.crt":
        raise FixtureError("local service certificate is not in a project fixture namespace")
    if settings.get("network", "tlskey", fallback="") != str(certificate.parent / "server.key"):
        raise FixtureError("local service key is not bound to the project fixture")
    actual = command(
        "verify service certificate binding", ["sudo", "-n", "sha256sum", str(certificate)]
    ).stdout.split()
    if not actual or actual[0] != hashlib.sha256((root / "server.crt").read_bytes()).hexdigest():
        raise FixtureError("local service certificate does not match the supplied fixture")
    command(
        "verify local certificate chain",
        ["openssl", "verify", "-CAfile", str(root / "ca.crt"), str(root / "server.crt")],
    )


def service_status():
    output = command("inspect local service generation", [
        "sudo", "-n", "systemctl", "show", "mssql-server",
        "--property=ActiveState", "--property=InvocationID", "--property=MainPID",
    ]).stdout
    properties = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
    state = properties.get("ActiveState")
    incarnation = properties.get("InvocationID", "")
    pid = properties.get("MainPID", "")
    if state not in ["active", "inactive", "failed"] or not pid.isascii() or not pid.isdecimal():
        raise FixtureError("local service state is unavailable or transitioning")
    if state == "active" and (
        int(pid) == 0 or len(incarnation) != 32
        or any(c not in "0123456789abcdef" for c in incarnation)
    ):
        raise FixtureError("active local service lacks an exact process/generation identity")
    return state, incarnation, pid


def wait_for_local(root, expected_server_name):
    username = (root / "observer-username").read_text()
    password = (root / "observer-password").read_text()
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        ready = sql(
            root,
            "SET NOCOUNT ON; SELECT @@SERVERNAME, SERVERPROPERTY('ProductVersion'), "
            "SERVERPROPERTY('Edition'), SERVERPROPERTY('EngineEdition'), SERVERPROPERTY('IsHadrEnabled');",
            username, password, check=False,
        )
        if ready.returncode == 0:
            values = [value.strip() for value in ready.stdout.strip().split("|")]
            if len(values) != 5 or values[0].casefold() != expected_server_name.casefold() or values[1:] != [
                ENGINE_VERSION.split("-")[0], "Enterprise Developer Edition (64-bit)", "3", "1"
            ]:
                raise FixtureError("running SQL Server does not match the exact configured fixture")
            return
        time.sleep(2)
    raise FixtureError("local fixture did not become verified-TLS/login ready")


def stop_local_generation(generation):
    current = service_status()
    if current[0] in ["inactive", "failed"]:
        print("The locally started fixture is already inactive.")
        return
    if current != generation:
        raise FixtureError("refusing to stop a replacement SQL Server process/generation")
    command("stop locally started fixture", ["sudo", "-n", "systemctl", "stop", "mssql-server"])
    if service_status()[0] not in ["inactive", "failed"]:
        raise FixtureError("locally started fixture did not stop")
    print("Stopped the exact locally started fixture; configuration and credentials retained.")


def local_lock():
    directory = Path.home() / ".local" / "state" / "kuberic-mssql"
    if directory.is_symlink():
        raise FixtureError("local lifecycle state directory must not be a symlink")
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    metadata = directory.stat()
    if metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
        raise FixtureError("local lifecycle state must be private and owned by the current user")
    return directory / "fixture.lock"


def fixture_root(directory=None):
    if directory is not None:
        root = directory.expanduser()
    elif os.environ.get("SQLSERVER_FIXTURE_DIR"):
        root = Path(os.environ["SQLSERVER_FIXTURE_DIR"]).expanduser()
    elif os.environ.get("GITHUB_ACTIONS") == "true":
        return runner_root()
    else:
        root = Path.home() / ".local" / "state" / "kuberic-mssql" / "fixture"
    if root.is_symlink() or any(c in str(root) for c in "\r\n"):
        raise FixtureError("fixture directory must be a non-symlink single-line path")
    return root.resolve()


@contextmanager
def lifecycle_lock():
    descriptor = os.open(local_lock(), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as lease:
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise FixtureError("another fixture lifecycle is already running") from error
        yield


def save_context(root, context):
    with tempfile.NamedTemporaryFile(
        mode="w", dir=root, prefix=".fixture-run-", delete=False
    ) as output:
        path = Path(output.name)
        try:
            json.dump(context, output)
            output.flush()
            os.fsync(output.fileno())
            os.replace(path, root / "fixture-run.json")
        finally:
            path.unlink(missing_ok=True)


def load_context(root):
    path = root / "fixture-run.json"
    if path.is_symlink() or not path.is_file():
        raise FixtureError("fixture has no exact ownership record")
    metadata = path.stat()
    if metadata.st_uid != os.getuid() or metadata.st_mode & 0o077 or metadata.st_size > 4096:
        raise FixtureError("fixture ownership record must be private, owned and bounded")
    try:
        context = json.loads(path.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise FixtureError("fixture ownership record is unreadable or malformed") from error
    if (
        not isinstance(context, dict) or context.get("schema_version") != 1
        or set(context) != {"schema_version", "root", "created", "ephemeral", "generation", "ready"}
        or context.get("root") != str(root)
        or any(type(context.get(key)) is not bool for key in ["created", "ephemeral", "ready"])
    ):
        raise FixtureError("fixture ownership record does not match this fixture")
    generation = context.get("generation")
    if generation is not None and (
        not isinstance(generation, list) or len(generation) != 3 or generation[0] != "active"
        or not isinstance(generation[1], str) or len(generation[1]) != 32
        or any(c not in "0123456789abcdef" for c in generation[1])
        or not isinstance(generation[2], str) or not generation[2].isascii()
        or not generation[2].isdecimal() or int(generation[2]) == 0
    ):
        raise FixtureError("fixture ownership record has an invalid process/generation")
    return context


def activate_fixture(root, context, stage, args, *, env=None):
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGINT, signal.SIGTERM])
    try:
        try:
            started = command(stage, args, env=env, check=False)
        finally:
            current = service_status()
            if current[0] == "active":
                context["generation"] = list(current)
                save_context(root, context)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
    if started.returncode or context["generation"] is None:
        raise FixtureError("fixture failed to start with a captured process/generation")


def ensure_fixture(root):
    context_path = root / "fixture-run.json"
    if context_path.is_symlink():
        raise FixtureError("fixture ownership record must not be a symlink")
    if context_path.exists():
        context = load_context(root)
        if not context["ready"]:
            raise FixtureError("incomplete fixture preparation requires cleanup before retry")
    else:
        context = {
            "schema_version": 1, "root": str(root), "created": False,
            "ephemeral": os.environ.get("GITHUB_ACTIONS") == "true",
            "generation": None, "ready": False,
        }
        if not (root / "absent.json").exists():
            install_fixture(root, context)
        else:
            local_fixture(root)
            verify_local_service(root)
            original = service_status()
            save_context(root, context)
            if original[0] == "active":
                print("Reusing the verified running fixture; setup and reconfiguration skipped.")
            else:
                activate_fixture(
                    root, context, "start configured fixture",
                    ["sudo", "-n", "systemctl", "start", "mssql-server"],
                )
    _, expected_server_name = local_fixture(root)
    verify_local_service(root)
    current = service_status()
    if current[0] != "active" or (
        context["generation"] is not None and list(current) != context["generation"]
    ):
        raise FixtureError("prepared fixture is no longer running under its captured generation")
    wait_for_local(root, expected_server_name)
    verify_loopback_binding()
    context["ready"] = True
    save_context(root, context)
    if os.environ.get("GITHUB_ENV"):
        with Path(os.environ["GITHUB_ENV"]).open("a") as output:
            for name, value in fixture_environment(root).items():
                output.write(f"{name}={value}\n")
    print("Fixture is ready; provisioning ran no tests.")
    return context


def provision(directory=None):
    root = fixture_root(directory)
    with lifecycle_lock():
        return ensure_fixture(root)


def run_test_cases(directory=None, *, include_ag=False):
    env = os.environ.copy()
    env["SQLSERVER_TEST_EULA_ACCEPTED"] = "true"
    root = fixture_root(directory)
    if directory is not None or os.environ.get("SQLSERVER_FIXTURE_DIR") or (root / "fixture-run.json").exists():
        context = load_context(root)
        if not context["ready"]:
            raise FixtureError("fixture is not ready; run the shared provision command first")
        _, expected_server_name = local_fixture(root)
        verify_local_service(root)
        if context["generation"] is not None and list(service_status()) != context["generation"]:
            raise FixtureError("fixture generation changed before testing")
        wait_for_local(root, expected_server_name)
        env.pop("SQLSERVER_TEST_IMAGE", None)
        env.update(fixture_environment(root))
    args = ["cargo", "test", "--locked", "--test", "live_observation", "--", "--ignored"]
    if not include_ag:
        args += ["--skip", "live_present_availability_group"]
    args += ["--test-threads=1"]
    result = command("run live observation cases", args, env=env, check=False, timeout=600)
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    if result.returncode:
        raise FixtureError(f"live observation cases failed with exit code {result.returncode}")


def validate_fixture(directory=None):
    root = fixture_root(directory)
    with lifecycle_lock():
        failure = None
        previous_handler = signal.getsignal(signal.SIGTERM)
        previous_interrupt = signal.getsignal(signal.SIGINT)

        def interrupted(signum, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupted)
        try:
            ensure_fixture(root)
            env = os.environ.copy()
            env.pop("SQLSERVER_TEST_IMAGE", None)
            env.update(fixture_environment(root))
            for stage, args in [
                ("run shared observation cases", ["just", "test-live", str(root)]),
                ("run shared CLI validation", ["just", "test-live-cli", str(root / "observation.json")]),
            ]:
                result = command(stage, args, env=env, check=False, timeout=600, process_group=True)
                print(result.stdout, end="")
                print(result.stderr, end="", file=sys.stderr)
                if result.returncode:
                    raise FixtureError(f"{stage} failed with exit code {result.returncode}")
        except (FixtureError, KeyboardInterrupt) as error:
            failure = error
        finally:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            try:
                release_fixture(root)
            except FixtureError as error:
                if failure is not None:
                    raise FixtureError(f"validation failed; cleanup also failed: {error}") from error
                raise
            finally:
                signal.signal(signal.SIGTERM, previous_handler)
                signal.signal(signal.SIGINT, previous_interrupt)
        if failure is not None:
            raise failure


def verify_cli(path):
    try:
        report = json.loads(path.read_text())
        snapshot = report["observation"]["value"]
        instance = snapshot["instance"]
        observed_at = report["observation"]["observed_at_unix_millis"]
        valid = (
            report["schema_version"] == 1
            and report["fresh"] is True
            and report["observation"]["status"] == "present"
            and report["max_age_millis"] == 60_000
            and 0 <= time.time_ns() // 1_000_000 - observed_at <= report["max_age_millis"]
            and instance["product_version"] == ENGINE_VERSION.split("-")[0]
            and instance["product_major_version"] == 17
            and instance["edition"] == "Enterprise Developer Edition (64-bit)"
            and instance["engine_edition"] == 3
            and instance["hadr_enabled"] is True
            and instance["host_platform"] == "Linux"
            and instance["architecture"] == "x86_64"
            and instance["server_name"] == report["source"]["expected_server_name"]
            and snapshot["availability_group"]["status"] == "absent"
        )
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise FixtureError("CLI report is missing the expected native observation shape") from error
    if not valid:
        raise FixtureError("CLI report does not prove a fresh supported native absent-AG observation")
    print("Verified fresh SQL Server 2025 Enterprise Developer CLI observation.")


def release_fixture(root):
    if not (root / "fixture-run.json").exists():
        print("No fixture ownership record was created; existing services/files left untouched.")
        return
    context = load_context(root)
    if context["created"] and context["ephemeral"] and root != runner_root():
        raise FixtureError("disposable deletion is restricted to the exact generated CI fixture directory")
    if context["generation"] is not None:
        stop_local_generation(tuple(context["generation"]))
    else:
        print("No service generation was started by this run; existing instances preserved.")
    if context["created"] and context["ephemeral"]:
        owner = root / "owner"
        if not owner.is_file() or owner.read_text() != OWNER:
            raise FixtureError("refusing disposable fixture deletion without exact ownership")
        tls = command("check server TLS directory", [
            "sudo", "-n", "test", "-d", str(SERVER_TLS)
        ], check=False)
        if tls.returncode not in [0, 1]:
            raise FixtureError("could not inspect the owned server TLS directory")
        if tls.returncode == 0:
            command("remove fixture server key/certificate", [
                "sudo", "-n", "rm", "-f", "--", str(SERVER_TLS / "server.key"), str(SERVER_TLS / "server.crt")
            ])
            command("remove fixture server TLS directory", ["sudo", "-n", "rmdir", str(SERVER_TLS)])
        shutil.rmtree(root)
        print("Removed the owned disposable fixture credentials/artifacts.")
    else:
        (root / "fixture-run.json").unlink()
        print("Fixture files retained for the next run.")


def cleanup(directory=None):
    with lifecycle_lock():
        release_fixture(fixture_root(directory))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["provision", "verify-cli", "cleanup", "validate", "test", "test-all"])
    parser.add_argument("path", nargs="?", help="report JSON or fixture directory (or SQLSERVER_FIXTURE_DIR)")
    args = parser.parse_args()
    path = Path(args.path) if args.path else None
    try:
        if args.action == "provision":
            provision(path)
        elif args.action == "cleanup":
            cleanup(path)
        elif args.action == "validate":
            validate_fixture(path)
        elif args.action in ["test", "test-all"]:
            run_test_cases(path, include_ag=args.action == "test-all")
        elif path is None:
            parser.error("verify-cli requires a JSON report path")
        else:
            verify_cli(path)
    except FixtureError as error:
        print(f"Fixture error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Fixture command interrupted.", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
