"""Owned SQL Server container fixture; tests and observer always run on the host."""

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ENGINE_VERSION = "17.0.5005.3"
IMAGE_REPOSITORY = "mcr.microsoft.com/mssql/server"
IMAGE_TAG = "2025-CU9-ubuntu-24.04"
IMAGE_DIGEST = "sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726"
IMAGE = f"{IMAGE_REPOSITORY}@{IMAGE_DIGEST}"
SQLCMD_URL = (
    "https://github.com/microsoft/go-sqlcmd/releases/download/v1.10.0/"
    "sqlcmd-linux-amd64.tar.bz2"
)
SQLCMD_SHA256 = "92516d98c63d99b0994de5b61350c91f6915f9b76f139a59039fbcb225c2e987"
SQLCMD_BINARY_SHA256 = "5c043495deff92687243e4c337e09494d497557a0217bdca81afefb1e4a2b4ce"
CONTAINER_HOSTNAME = "kuberic-mssql-observer"
CONTAINER_MEMORY = 3 * 1024 * 1024 * 1024
LABEL_MANAGED = "io.kuberic.mssql.fixture"
LABEL_ROOT = "io.kuberic.mssql.fixture-root"
LABEL_IMAGE = "io.kuberic.mssql.image-digest"
OWNER = "kuberic-sqlserver-observer-container-v1\n"


class FixtureError(Exception):
    pass


def command(stage, args, *, env=None, stdin=None, check=True, timeout=180, process_group=False):
    try:
        if process_group:
            with subprocess.Popen(
                args,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            ) as process:
                try:
                    stdout, stderr = process.communicate(input=stdin, timeout=timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
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
        raise FixtureError(f"{stage} failed with exit code {result.returncode}")
    return result


def docker(stage, args, **kwargs):
    return command(stage, ["sudo", "-n", "docker", *args], **kwargs)


def runner_root():
    for name, expected in [
        ("GITHUB_ACTIONS", "true"),
        ("RUNNER_ENVIRONMENT", "github-hosted"),
        ("RUNNER_OS", "Linux"),
        ("RUNNER_ARCH", "X64"),
    ]:
        if os.environ.get(name) != expected:
            raise FixtureError("the generated CI fixture requires a GitHub-hosted Linux x64 runner")
    temporary = os.environ.get("RUNNER_TEMP", "")
    if not temporary or not Path(temporary).is_absolute() or any(c in temporary for c in "\r\n"):
        raise FixtureError("RUNNER_TEMP must be an absolute single-line path")
    return Path(temporary).resolve() / "sqlserver-observer"


def fixture_root(directory=None):
    if directory is not None and str(directory):
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
        [
            "curl", "--fail", "--silent", "--show-error", "--location", "--retry", "3",
            "--max-time", "180", "--output", str(destination), url,
        ],
        timeout=600,
    )
    verify_digest(destination, digest)
    return destination


def secret_file(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(value)


def certificates(root):
    for prefix, subject in [
        ("ca", "Kuberic observer container CA"),
        ("bad-ca", "Untrusted observer container CA"),
    ]:
        command(
            "generate fixture CA",
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-subj", f"/CN={subject}", "-addext", "basicConstraints=critical,CA:TRUE",
                "-keyout", str(root / f"{prefix}.key"), "-out", str(root / f"{prefix}.crt"),
            ],
        )
    command(
        "generate server key",
        [
            "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
            "-subj", f"/CN={CONTAINER_HOSTNAME}", "-keyout", str(root / "server.key"),
            "-out", str(root / "server.csr"),
        ],
    )
    (root / "server.ext").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1,"
        f"DNS:{CONTAINER_HOSTNAME}\nextendedKeyUsage=serverAuth\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\nbasicConstraints=critical,CA:FALSE\n"
    )
    command(
        "sign server certificate",
        [
            "openssl", "x509", "-req", "-in", str(root / "server.csr"),
            "-CA", str(root / "ca.crt"), "-CAkey", str(root / "ca.key"), "-CAcreateserial",
            "-days", "2", "-extfile", str(root / "server.ext"), "-out", str(root / "server.crt"),
        ],
    )


def write_configs(root):
    config = {
        "mode": "observe_only",
        "host": "localhost",
        "port": 1433,
        "availability_group": "kuberic-container-absent",
        "expected_server_name": CONTAINER_HOSTNAME,
        "replica_id": "container-fixture-1",
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


def root_fingerprint(root):
    return hashlib.sha256(str(root).encode()).hexdigest()


def container_name(root):
    return f"kuberic-mssql-observer-{root_fingerprint(root)[:12]}"


def fixture_environment(root):
    return {
        "SQLSERVER_TEST_EULA_ACCEPTED": "true",
        "SQLSERVER_TEST_IMAGE": IMAGE,
        "SQLSERVER_LIVE_ABSENT_CONFIG": str(root / "absent.json"),
        "SQLSERVER_LIVE_DENIED_CONFIG": str(root / "denied.json"),
        "SQLSERVER_LIVE_BAD_TLS_CONFIG": str(root / "bad-tls.json"),
    }


def prepare_container_files(root):
    container = root / "container"
    container.mkdir(mode=0o700)
    (container / "mssql.conf.source").write_text(
        "[network]\n"
        "forceencryption = 1\n"
        "tlscert = /var/opt/mssql/secrets/kuberic/server.crt\n"
        "tlskey = /var/opt/mssql/secrets/kuberic/server.key\n"
        "tlsprotocols = 1.2\n"
        "[hadr]\n"
        "hadrenabled = 1\n"
        "[memory]\n"
        "memorylimitmb = 2048\n"
    )
    secret_file(
        container / "container.env",
        "ACCEPT_EULA=Y\n"
        "MSSQL_PID=EnterpriseDeveloper\n"
        f"MSSQL_SA_PASSWORD={(root / 'sa-password').read_text()}\n"
        "MSSQL_ENABLE_HADR=1\n"
        "MSSQL_MEMORY_LIMIT_MB=2048\n",
    )
    for source, destination, mode in [
        (container / "mssql.conf.source", container / "mssql.conf", "644"),
        (root / "server.crt", container / "server.crt", "644"),
        (root / "server.key", container / "server.key", "600"),
    ]:
        command(
            "install container fixture file",
            ["sudo", "-n", "install", "-o", "10001", "-g", "0", "-m", mode, str(source), str(destination)],
        )
    (container / "mssql.conf.source").unlink()


def prepare_fixture_files(root):
    os.umask(0o077)
    archive = download(root, "sqlcmd.tar.bz2", SQLCMD_URL, SQLCMD_SHA256)
    command("extract pinned SQL client", ["tar", "-xjf", str(archive), "-C", str(root), "sqlcmd"])
    archive.unlink()
    verify_digest(root / "sqlcmd", SQLCMD_BINARY_SHA256)
    certificates(root)
    secret_file(root / "sa-password", secrets.token_urlsafe(32) + "Aa1!")
    for username, prefix in [
        ("kuberic_observer", "observer"),
        ("kuberic_denied", "denied"),
    ]:
        secret_file(root / f"{prefix}-username", username)
        secret_file(root / f"{prefix}-password", secrets.token_urlsafe(32) + "Aa1!")
    write_configs(root)
    prepare_container_files(root)


def initialize_fixture_root(root, context):
    if root.exists() and (
        not root.is_dir() or root.stat().st_uid != os.getuid()
        or root.stat().st_mode & 0o077 or any(root.iterdir())
    ):
        raise FixtureError("new fixture directory must be empty, private and owned")
    os.umask(0o077)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    (root / "owner").write_text(OWNER)
    context["created_files"] = True
    save_context(root, context)


def file_sha256(path, *, privileged=False):
    if privileged:
        output = command(
            "hash container-mounted fixture file", ["sudo", "-n", "sha256sum", str(path)]
        ).stdout.split()
        if not output:
            raise FixtureError("cannot hash container-mounted fixture file")
        return output[0]
    return hashlib.sha256(path.read_bytes()).hexdigest()


def local_fixture(directory):
    if directory.is_symlink():
        raise FixtureError("fixture directory must not be a symlink")
    try:
        root = directory.resolve(strict=True)
        metadata = root.stat()
        if not root.is_dir() or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
            raise FixtureError("fixture must be a private directory owned by the current user")
        required = [
            "owner", "absent.json", "denied.json", "bad-tls.json", "ca.crt", "bad-ca.crt",
            "server.crt", "observer-username", "observer-password", "denied-username",
            "denied-password", "sa-password", "sqlcmd",
        ]
        for name in required:
            path = root / name
            if path.is_symlink() or not path.is_file() or path.stat().st_uid != os.getuid():
                raise FixtureError("fixture requires owned, regular, non-symlink files")
        container = root / "container"
        if container.is_symlink() or not container.is_dir() or container.stat().st_uid != os.getuid():
            raise FixtureError("fixture requires an owned container mount directory")
        for name in ["container.env", "mssql.conf", "server.crt", "server.key"]:
            path = container / name
            if path.is_symlink() or not path.is_file():
                raise FixtureError("fixture container mount files are missing or unsafe")
        if (root / "owner").read_text() != OWNER:
            raise FixtureError("fixture ownership marker is invalid")
        for name in ["observer-username", "observer-password", "denied-username", "denied-password", "sa-password"]:
            path = root / name
            if path.stat().st_mode & 0o077 or path.stat().st_size > 4096:
                raise FixtureError("fixture credential files must be private and bounded")
            value = path.read_text()
            if not value or "\n" in value or "\x00" in value:
                raise FixtureError("fixture credential files must contain exact nonempty UTF-8 values")
        configs = {
            name: json.loads((root / f"{name}.json").read_text())
            for name in ["absent", "denied", "bad-tls"]
        }
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise FixtureError("cannot read the configured fixture files") from error
    for name, config in configs.items():
        username = "denied-username" if name == "denied" else "observer-username"
        password = "denied-password" if name == "denied" else "observer-password"
        ca = "bad-ca.crt" if name == "bad-tls" else "ca.crt"
        expected = {
            "mode": "observe_only",
            "host": "localhost",
            "port": 1433,
            "expected_server_name": CONTAINER_HOSTNAME,
            "observer_username_file": str(root / username),
            "observer_password_file": str(root / password),
            "ca_certificate_file": str(root / ca),
        }
        if not isinstance(config, dict) or any(config.get(key) != value for key, value in expected.items()):
            raise FixtureError("fixture configs must reference the exact host endpoint and fixture files")
    verify_digest(root / "sqlcmd", SQLCMD_BINARY_SHA256)
    expected_conf = (
        "[network]\nforceencryption = 1\n"
        "tlscert = /var/opt/mssql/secrets/kuberic/server.crt\n"
        "tlskey = /var/opt/mssql/secrets/kuberic/server.key\n"
        "tlsprotocols = 1.2\n[hadr]\nhadrenabled = 1\n"
        "[memory]\nmemorylimitmb = 2048\n"
    )
    if (root / "container" / "mssql.conf").read_text() != expected_conf:
        raise FixtureError("container SQL Server configuration is not the supported fixture profile")
    expected_env = (
        "ACCEPT_EULA=Y\nMSSQL_PID=EnterpriseDeveloper\n"
        f"MSSQL_SA_PASSWORD={(root / 'sa-password').read_text()}\n"
        "MSSQL_ENABLE_HADR=1\nMSSQL_MEMORY_LIMIT_MB=2048\n"
    )
    environment = root / "container" / "container.env"
    if environment.stat().st_uid != os.getuid() or environment.stat().st_mode & 0o077:
        raise FixtureError("container environment file must be private and user-owned")
    if environment.read_text() != expected_env:
        raise FixtureError("container environment does not match the supported fixture profile")
    for source, mounted in [
        (root / "server.crt", root / "container" / "server.crt"),
        (root / "server.key", root / "container" / "server.key"),
    ]:
        if file_sha256(source) != file_sha256(mounted, privileged=True):
            raise FixtureError("container TLS mount does not match the fixture certificate/key")
    return root


def docker_inspect(name, *, allow_absent=False):
    result = docker("inspect SQL Server fixture container", ["inspect", name], check=False)
    if result.returncode:
        if allow_absent:
            return None
        raise FixtureError("SQL Server fixture container is missing or inaccessible")
    try:
        values = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise FixtureError("Docker returned malformed container metadata") from error
    if not isinstance(values, list) or len(values) != 1:
        raise FixtureError("Docker returned ambiguous container metadata")
    return values[0]


def image_id():
    result = docker("inspect pinned SQL Server image", ["image", "inspect", IMAGE], check=False)
    if result.returncode:
        docker("pull pinned SQL Server image", ["pull", IMAGE], timeout=900)
        result = docker("inspect pinned SQL Server image", ["image", "inspect", IMAGE])
    try:
        image = json.loads(result.stdout)[0]
    except (json.JSONDecodeError, IndexError, KeyError) as error:
        raise FixtureError("Docker returned malformed image metadata") from error
    labels = image.get("Config", {}).get("Labels", {})
    if (
        image.get("Architecture") != "amd64"
        or image.get("Os") != "linux"
        or labels.get("com.microsoft.version") != ENGINE_VERSION
        or IMAGE not in image.get("RepoDigests", [])
    ):
        raise FixtureError("pinned SQL Server image metadata is unsupported or inconsistent")
    return image["Id"]


def expected_mounts(root):
    return {
        str(root / "container" / "mssql.conf"): ("/var/opt/mssql/mssql.conf", False),
        str(root / "container" / "server.crt"): ("/var/opt/mssql/secrets/kuberic/server.crt", False),
        str(root / "container" / "server.key"): ("/var/opt/mssql/secrets/kuberic/server.key", False),
    }


def verify_container(root, metadata, *, require_running=True, expected_id=None):
    name = container_name(root)
    labels = metadata.get("Config", {}).get("Labels", {})
    state = metadata.get("State", {})
    bindings = metadata.get("HostConfig", {}).get("PortBindings", {}).get("1433/tcp", [])
    mounts = {
        mount.get("Source"): (mount.get("Destination"), mount.get("RW"))
        for mount in metadata.get("Mounts", [])
    }
    expected_labels = {
        LABEL_MANAGED: "observe-only",
        LABEL_ROOT: root_fingerprint(root),
        LABEL_IMAGE: IMAGE_DIGEST,
    }
    environment = metadata.get("Config", {}).get("Env", [])
    required_environment = {
        "ACCEPT_EULA=Y",
        "MSSQL_PID=EnterpriseDeveloper",
        "MSSQL_ENABLE_HADR=1",
        "MSSQL_MEMORY_LIMIT_MB=2048",
    }
    if (
        metadata.get("Name") != f"/{name}"
        or (expected_id is not None and metadata.get("Id") != expected_id)
        or metadata.get("Image") != image_id()
        or metadata.get("Config", {}).get("Hostname") != CONTAINER_HOSTNAME
        or any(labels.get(key) != value for key, value in expected_labels.items())
        or bindings != [{"HostIp": "127.0.0.1", "HostPort": "1433"}]
        or metadata.get("HostConfig", {}).get("Memory") != CONTAINER_MEMORY
        or metadata.get("HostConfig", {}).get("MemorySwap") != CONTAINER_MEMORY
        or metadata.get("HostConfig", {}).get("RestartPolicy", {}).get("Name") not in ["", "no"]
        or not required_environment.issubset(environment)
        or len([value for value in environment if value.startswith("MSSQL_SA_PASSWORD=")]) != 1
        or any(mounts.get(source) != value for source, value in expected_mounts(root).items())
        or (require_running and not state.get("Running"))
    ):
        raise FixtureError("refusing an unrelated or modified SQL Server container")
    return metadata["Id"], bool(state.get("Running"))


def create_container(root):
    if command("check host SQL port", ["ss", "-H", "-ltn", "( sport = :1433 )"]).stdout.strip():
        raise FixtureError("host port 1433 is already occupied")
    image_id()
    name = container_name(root)
    args = [
        "run", "--detach", "--name", name, "--hostname", CONTAINER_HOSTNAME,
        "--restart", "no", "--memory", str(CONTAINER_MEMORY), "--memory-swap", str(CONTAINER_MEMORY),
        "--publish", "127.0.0.1:1433:1433",
        "--label", f"{LABEL_MANAGED}=observe-only",
        "--label", f"{LABEL_ROOT}={root_fingerprint(root)}",
        "--label", f"{LABEL_IMAGE}={IMAGE_DIGEST}",
        "--env-file", str(root / "container" / "container.env"),
    ]
    for source, (destination, _) in expected_mounts(root).items():
        args += ["--mount", f"type=bind,source={source},target={destination},readonly"]
    args.append(IMAGE)
    result = docker("create SQL Server fixture container", args, timeout=180)
    container_id = result.stdout.strip()
    metadata = docker_inspect(name)
    verify_container(root, metadata, expected_id=container_id)
    return container_id


def start_container(root, container_id):
    name = container_name(root)
    metadata = docker_inspect(name)
    _, running = verify_container(root, metadata, require_running=False, expected_id=container_id)
    if not running:
        docker("start SQL Server fixture container", ["start", name])
        metadata = docker_inspect(name)
        verify_container(root, metadata, expected_id=container_id)


def wait_for_container(root):
    username = (root / "observer-username").read_text()
    password = (root / "observer-password").read_text()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        ready = sql(
            root,
            "SET NOCOUNT ON; SELECT @@SERVERNAME, SERVERPROPERTY('ProductVersion'), "
            "SERVERPROPERTY('Edition'), SERVERPROPERTY('EngineEdition'), SERVERPROPERTY('IsHadrEnabled');",
            username, password, check=False,
        )
        if ready.returncode == 0:
            values = [value.strip() for value in ready.stdout.strip().split("|")]
            if values != [
                CONTAINER_HOSTNAME, ENGINE_VERSION, "Enterprise Developer Edition (64-bit)", "3", "1"
            ]:
                raise FixtureError("running SQL Server container has unsupported native identity")
            return
        time.sleep(2)
    raise FixtureError("SQL Server container did not become verified-TLS/login ready")


def sql(root, text, username, password, *, check=True):
    env = os.environ.copy()
    env.update(SQLCMDPASSWORD=password, SSL_CERT_FILE=str(root / "ca.crt"))
    return command(
        "verified-TLS fixture administration",
        [
            str(root / "sqlcmd"), "-S", "localhost,1433", "-U", username,
            "-N", "true", "-b", "-l", "5", "-t", "30", "-h", "-1", "-W", "-s", "|",
        ],
        env=env, stdin=text + "\nGO\n", check=check, timeout=40,
    )


def initialize_logins(root):
    sa_password = (root / "sa-password").read_text()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        ready = sql(root, "SET NOCOUNT ON; SELECT @@SERVERNAME;", "sa", sa_password, check=False)
        if ready.returncode == 0:
            break
        time.sleep(2)
    else:
        raise FixtureError("SQL Server container did not become ready for fixture initialization")
    for prefix, permitted in [("observer", True), ("denied", False)]:
        username = (root / f"{prefix}-username").read_text()
        password = (root / f"{prefix}-password").read_text()
        batch = (
            f"IF SUSER_ID(N'{username}') IS NULL "
            f"CREATE LOGIN [{username}] WITH PASSWORD = N'{password}', CHECK_POLICY = ON;"
        )
        if permitted:
            for permission in [
                "VIEW SERVER STATE", "VIEW SERVER PERFORMANCE STATE",
                "VIEW ANY DEFINITION", "VIEW ANY DATABASE",
            ]:
                batch += f"\nGRANT {permission} TO [{username}];"
        sql(root, batch, "sa", sa_password)


def local_lock():
    directory = Path.home() / ".local" / "state" / "kuberic-mssql"
    if directory.is_symlink():
        raise FixtureError("fixture lifecycle state directory must not be a symlink")
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    metadata = directory.stat()
    if metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
        raise FixtureError("fixture lifecycle state must be private and owned")
    return directory / "fixture.lock"


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
    with tempfile.NamedTemporaryFile(mode="w", dir=root, prefix=".fixture-run-", delete=False) as output:
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
        not isinstance(context, dict)
        or set(context) != {
            "schema_version", "root", "created_files", "created_container",
            "started_container", "ephemeral", "container_id", "ready",
        }
        or context.get("schema_version") != 2
        or context.get("root") != str(root)
        or any(
            type(context.get(key)) is not bool
            for key in ["created_files", "created_container", "started_container", "ephemeral", "ready"]
        )
        or (
            context.get("container_id") is not None
            and (
                not isinstance(context["container_id"], str)
                or len(context["container_id"]) != 64
                or any(c not in "0123456789abcdef" for c in context["container_id"])
            )
        )
    ):
        raise FixtureError("fixture ownership record does not match this container fixture")
    return context


def new_context(root):
    return {
        "schema_version": 2,
        "root": str(root),
        "created_files": False,
        "created_container": False,
        "started_container": False,
        "ephemeral": os.environ.get("GITHUB_ACTIONS") == "true",
        "container_id": None,
        "ready": False,
    }


def ensure_fixture(root):
    context_path = root / "fixture-run.json"
    if context_path.is_symlink():
        raise FixtureError("fixture ownership record must not be a symlink")
    if context_path.exists():
        context = load_context(root)
        if not context["ready"]:
            raise FixtureError("incomplete container preparation requires cleanup before retry")
    else:
        context = new_context(root)
        if not (root / "owner").exists():
            initialize_fixture_root(root, context)
            prepare_fixture_files(root)
        local_fixture(root)
        metadata = docker_inspect(container_name(root), allow_absent=True)
        if metadata is None:
            context["created_container"] = True
            context["started_container"] = True
            save_context(root, context)
            context["container_id"] = create_container(root)
            save_context(root, context)
            initialize_logins(root)
        else:
            container_id, running = verify_container(root, metadata, require_running=False)
            context["container_id"] = container_id
            save_context(root, context)
            if running:
                print("Reusing the verified running SQL Server container.")
            else:
                context["started_container"] = True
                save_context(root, context)
                start_container(root, container_id)
                save_context(root, context)
    metadata = docker_inspect(container_name(root))
    verify_container(root, metadata, expected_id=context["container_id"])
    wait_for_container(root)
    context["ready"] = True
    save_context(root, context)
    if os.environ.get("GITHUB_ENV"):
        with Path(os.environ["GITHUB_ENV"]).open("a") as output:
            for name, value in fixture_environment(root).items():
                output.write(f"{name}={value}\n")
    print("Container fixture is ready; host tests have not run yet.")
    return context


def provision(directory=None):
    root = fixture_root(directory)
    with lifecycle_lock():
        return ensure_fixture(root)


def run_test_cases(directory=None, *, include_ag=False):
    root = fixture_root(directory)
    context = load_context(root)
    if not context["ready"]:
        raise FixtureError("container fixture is not ready")
    local_fixture(root)
    metadata = docker_inspect(container_name(root))
    verify_container(root, metadata, expected_id=context["container_id"])
    wait_for_container(root)
    env = os.environ.copy()
    for name in ["SQLSERVER_TEST_PACKAGE_VERSION", "SQLSERVER_TEST_PACKAGE_SHA256"]:
        env.pop(name, None)
    env.update(fixture_environment(root))
    args = ["cargo", "test", "--locked", "--test", "live_observation", "--", "--ignored"]
    if not include_ag:
        args += ["--skip", "live_present_availability_group"]
    args += ["--test-threads=1"]
    result = command("run host live observation cases", args, env=env, check=False, timeout=600)
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    if result.returncode:
        raise FixtureError(f"host live observation cases failed with exit code {result.returncode}")


def stop_exact_container(root, container_id):
    name = container_name(root)
    metadata = docker_inspect(name)
    _, running = verify_container(root, metadata, require_running=False, expected_id=container_id)
    if running:
        docker("stop owned SQL Server container", ["stop", "--time", "30", name], timeout=45)
    metadata = docker_inspect(name)
    verify_container(root, metadata, require_running=False, expected_id=container_id)
    if metadata.get("State", {}).get("Running"):
        raise FixtureError("owned SQL Server container did not stop")


def remove_exact_container(root, container_id):
    stop_exact_container(root, container_id)
    name = container_name(root)
    docker("remove owned SQL Server container", ["rm", name])
    if docker_inspect(name, allow_absent=True) is not None:
        raise FixtureError("owned SQL Server container still exists after removal")


def remove_fixture_files(root):
    if (root / "owner").read_text() != OWNER:
        raise FixtureError("refusing fixture deletion without exact ownership marker")
    container = root / "container"
    if container.exists():
        command("remove container-only mounted files", ["sudo", "-n", "rm", "-rf", "--", str(container)])
    shutil.rmtree(root)


def release_fixture(root):
    path = root / "fixture-run.json"
    if not path.exists():
        print("No fixture ownership record exists; containers/files left untouched.")
        return
    context = load_context(root)
    if context["created_container"] and context["container_id"] is None:
        metadata = docker_inspect(container_name(root), allow_absent=True)
        if metadata is not None:
            container_id, _ = verify_container(root, metadata, require_running=False)
            context["container_id"] = container_id
    if context["container_id"] is not None:
        if context["created_container"]:
            remove_exact_container(root, context["container_id"])
        elif context["started_container"]:
            stop_exact_container(root, context["container_id"])
        else:
            print("Preserved the SQL Server container that was running before this invocation.")
    if context["created_files"] and context["ephemeral"]:
        if root != runner_root():
            raise FixtureError("disposable fixture deletion is restricted to the exact CI fixture path")
        remove_fixture_files(root)
        print("Removed owned disposable fixture files.")
    else:
        path.unlink()
        print("Fixture files retained for reuse.")


def cleanup(directory=None):
    with lifecycle_lock():
        release_fixture(fixture_root(directory))


def validate_fixture(directory=None):
    root = fixture_root(directory)
    with lifecycle_lock():
        failure = None
        previous_term = signal.getsignal(signal.SIGTERM)
        previous_int = signal.getsignal(signal.SIGINT)

        def interrupted(signum, frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupted)
        try:
            ensure_fixture(root)
            env = os.environ.copy()
            env.update(fixture_environment(root))
            for stage, args in [
                ("run shared host observation cases", ["just", "test-live", str(root)]),
                ("run shared host CLI validation", ["just", "test-live-cli", str(root / "observation.json")]),
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
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)
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
            and instance["product_version"] == ENGINE_VERSION
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
        raise FixtureError("CLI report is missing the expected container observation shape") from error
    if not valid:
        raise FixtureError("CLI report does not prove a fresh supported container absent-AG observation")
    print("Verified fresh SQL Server 2025 container CLI observation.")


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
