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
CONTEXT_SCHEMA_VERSION = 5
ENDPOINT_PORT = 5022
AVAILABILITY_GROUP_PREFIX = "kuberic-progress-"
LEGACY_AVAILABILITY_GROUP_NAME = "kuberic-progress-ag"
PEER_SERVER_NAMES = ("kuberic-mssql-peer-1", "kuberic-mssql-peer-2")


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
    return command(stage, ["docker", *args], **kwargs)


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


def base_config(root):
    return {
        "mode": "observe_only",
        "host": "localhost",
        "port": 1433,
        "expected_server_name": CONTAINER_HOSTNAME,
        "replica_id": "container-fixture-1",
        "incarnation": secrets.token_hex(16),
        "observer_username_file": str(root / "observer-username"),
        "observer_password_file": str(root / "observer-password"),
        "ca_certificate_file": str(root / "ca.crt"),
    }


def write_configs(root):
    config = base_config(root)
    config["availability_group"] = "kuberic-container-absent"
    for name in ["absent", "denied", "bad-tls"]:
        current = config.copy()
        if name == "denied":
            current["observer_username_file"] = str(root / "denied-username")
            current["observer_password_file"] = str(root / "denied-password")
        if name == "bad-tls":
            current["ca_certificate_file"] = str(root / "bad-ca.crt")
        (root / f"{name}.json").write_text(json.dumps(current) + "\n")


def write_present_config(root, availability_group_name):
    if not valid_availability_group_name(availability_group_name):
        raise FixtureError("fixture availability-group name is invalid")
    absent = json.loads((root / "absent.json").read_text())
    config = base_config(root)
    config["availability_group"] = availability_group_name
    config["incarnation"] = absent["incarnation"]
    path = root / "present.json"
    temporary = root / ".present.json"
    temporary.write_text(json.dumps(config) + "\n")
    os.replace(temporary, path)


def root_fingerprint(root):
    return hashlib.sha256(str(root).encode()).hexdigest()


def container_name(root):
    return f"kuberic-mssql-observer-{root_fingerprint(root)[:12]}"


def fixture_environment(root, *, include_ag=False):
    environment = {
        "SQLSERVER_TEST_EULA_ACCEPTED": "true",
        "SQLSERVER_TEST_IMAGE": IMAGE,
        "SQLSERVER_LIVE_ABSENT_CONFIG": str(root / "absent.json"),
        "SQLSERVER_LIVE_DENIED_CONFIG": str(root / "denied.json"),
        "SQLSERVER_LIVE_BAD_TLS_CONFIG": str(root / "bad-tls.json"),
    }
    if include_ag:
        if not (root / "present.json").is_file():
            raise FixtureError("present-AG configuration is unavailable before SQL-object readiness")
        environment["SQLSERVER_LIVE_AG_CONFIG"] = str(root / "present.json")
    return environment


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
        for name in [
            "observer-username", "observer-password", "denied-username", "denied-password",
            "sa-password",
        ]:
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
        or metadata.get("Config", {}).get("User") != "mssql"
        or metadata.get("Config", {}).get("Hostname") != CONTAINER_HOSTNAME
        or any(labels.get(key) != value for key, value in expected_labels.items())
        or bindings != [{"HostIp": "127.0.0.1", "HostPort": "1433"}]
        or metadata.get("HostConfig", {}).get("Memory") != CONTAINER_MEMORY
        or metadata.get("HostConfig", {}).get("MemorySwap") != CONTAINER_MEMORY
        or metadata.get("HostConfig", {}).get("RestartPolicy", {}).get("Name") not in ["", "no"]
        or not required_environment.issubset(environment)
        or len([value for value in environment if value.startswith("MSSQL_SA_PASSWORD=")]) != 1
        or mounts != expected_mounts(root)
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


def sql(root, text, username, password, *, check=True, stage="verified-TLS fixture administration"):
    env = os.environ.copy()
    env.update(SQLCMDPASSWORD=password, SSL_CERT_FILE=str(root / "ca.crt"))
    return command(
        stage,
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


def admin_sql(root, text, *, check=True, stage="verified-TLS fixture administration"):
    return sql(
        root,
        text,
        "sa",
        (root / "sa-password").read_text(),
        check=check,
        stage=stage,
    )


def sql_rows(root, text, stage):
    result = admin_sql(root, "SET NOCOUNT ON;\n" + text, stage=stage)
    return [
        [column.strip() for column in line.split("|")]
        for line in result.stdout.splitlines()
        if line.strip()
    ]


def one_optional_row(rows, object_name):
    if len(rows) > 1:
        raise FixtureError(f"SQL Server returned ambiguous {object_name} metadata")
    return rows[0] if rows else None


def new_availability_group_name():
    return AVAILABILITY_GROUP_PREFIX + secrets.token_hex(16)


def valid_availability_group_name(name):
    if not isinstance(name, str) or not name.startswith(AVAILABILITY_GROUP_PREFIX):
        return False
    nonce = name[len(AVAILABILITY_GROUP_PREFIX):]
    return len(nonce) == 32 and all(character in "0123456789abcdef" for character in nonce)


def availability_group_record(name=None):
    return {
        "name": name or new_availability_group_name(),
        "group_id": None,
        "profile": None,
        "created": False,
        "state": "not_started",
    }


def expected_ag_profile():
    replicas = [
        {
            "server_name": CONTAINER_HOSTNAME,
            "endpoint_url": f"TCP://{CONTAINER_HOSTNAME}:{ENDPOINT_PORT}",
            "availability_mode": "SYNCHRONOUS_COMMIT",
            "failover_mode": "EXTERNAL",
            "seeding_mode": "AUTOMATIC",
        },
        *[
            {
                "server_name": server,
                "endpoint_url": f"TCP://{server}:{ENDPOINT_PORT}",
                "availability_mode": "SYNCHRONOUS_COMMIT",
                "failover_mode": "EXTERNAL",
                "seeding_mode": "AUTOMATIC",
            }
            for server in PEER_SERVER_NAMES
        ],
    ]
    return {
        "cluster_type": "external",
        "required_synchronized_secondaries": 1,
        "basic_features": False,
        "distributed": False,
        "database_count": 0,
        "replicas": sorted(replicas, key=lambda replica: replica["server_name"]),
    }


def inspect_availability_group(root, name):
    if not valid_availability_group_name(name):
        raise FixtureError("fixture availability-group name is invalid")
    group = one_optional_row(
        sql_rows(
            root,
            "SELECT CONVERT(varchar(36), group_id), cluster_type_desc, "
            "CONVERT(varchar(10), required_synchronized_secondaries_to_commit), "
            "CONVERT(varchar(1), basic_features), CONVERT(varchar(1), is_distributed), "
            "CONVERT(varchar(20), sequence_number) "
            "FROM sys.availability_groups "
            f"WHERE name = N'{name}';",
            "inspect availability group",
        ),
        "availability group",
    )
    replica_rows = sql_rows(
        root,
        "SELECT replica_server_name, endpoint_url, availability_mode_desc, "
        "failover_mode_desc, seeding_mode_desc "
        "FROM sys.availability_replicas "
        f"WHERE group_id = (SELECT group_id FROM sys.availability_groups "
        f"WHERE name = N'{name}') ORDER BY replica_server_name;",
        "inspect availability-group replicas",
    )
    database_count = one_optional_row(
        sql_rows(
            root,
            "SELECT CONVERT(varchar(10), COUNT(*)) "
            "FROM sys.availability_databases_cluster "
            f"WHERE group_id = (SELECT group_id FROM sys.availability_groups "
            f"WHERE name = N'{name}');",
            "inspect availability-group databases",
        ),
        "availability-group database count",
    )
    if group is None:
        return None
    if (
        len(group) != 6
        or database_count is None
        or len(database_count) != 1
        or any(len(row) != 5 for row in replica_rows)
    ):
        raise FixtureError("SQL Server returned malformed availability-group metadata")
    replicas = [
        {
            "server_name": row[0],
            "endpoint_url": row[1],
            "availability_mode": row[2],
            "failover_mode": row[3],
            "seeding_mode": row[4],
        }
        for row in replica_rows
    ]
    return {
        "group_id": group[0].lower(),
        "configuration_sequence": int(group[5]),
        "profile": {
            "cluster_type": group[1],
            "required_synchronized_secondaries": int(group[2]),
            "basic_features": group[3] == "1",
            "distributed": group[4] == "1",
            "database_count": int(database_count[0]),
            "replicas": sorted(replicas, key=lambda replica: replica["server_name"]),
        },
    }


def validate_availability_group(snapshot):
    return (
        snapshot is not None
        and snapshot["configuration_sequence"] > 0
        and snapshot["profile"] == expected_ag_profile()
    )


def create_availability_group(root, name):
    if not valid_availability_group_name(name):
        raise FixtureError("fixture availability-group name is invalid")
    replicas = [
        (CONTAINER_HOSTNAME, f"TCP://{CONTAINER_HOSTNAME}:{ENDPOINT_PORT}"),
        *[(server, f"TCP://{server}:{ENDPOINT_PORT}") for server in PEER_SERVER_NAMES],
    ]
    definitions = ",\n".join(
        f"N'{server}' WITH (ENDPOINT_URL = N'{endpoint}', "
        "AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, FAILOVER_MODE = EXTERNAL, "
        "SEEDING_MODE = AUTOMATIC)"
        for server, endpoint in replicas
    )
    admin_sql(
        root,
        f"CREATE AVAILABILITY GROUP [{name}] "
        "WITH (CLUSTER_TYPE = EXTERNAL, "
        "REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1) "
        "FOR REPLICA ON\n"
        f"{definitions};",
        stage="create fixture availability group",
    )


def bind_created_availability_group(root, context, observed):
    record = context["availability_group"]
    if not validate_availability_group(observed):
        raise FixtureError("fixture availability group does not match its exact profile")
    if record["group_id"] is not None and record["group_id"] != observed["group_id"]:
        raise FixtureError("fixture availability-group identity changed")
    record["group_id"] = observed["group_id"]
    record["profile"] = observed["profile"]
    record["state"] = "created" if record["created"] else "verified"
    save_context(root, context)


def availability_group_matches_record(observed, record):
    return (
        validate_availability_group(observed)
        and observed["group_id"] == record["group_id"]
        and observed["profile"] == record["profile"]
    )


def provision_availability_group(root, context):
    record = context["availability_group"]
    name = record["name"]
    observed = inspect_availability_group(root, name)
    if record["state"] == "not_started":
        if observed is not None:
            raise FixtureError(f"refusing same-name unowned availability group: {record['name']}")
        record.update(created=True, state="creating")
        save_context(root, context)
        create_availability_group(root, name)
    elif record["state"] == "creating":
        if observed is None:
            create_availability_group(root, name)
        elif not validate_availability_group(observed):
            raise FixtureError("fixture availability-group profile changed during creation")
    elif record["state"] in ["created", "verified"]:
        if observed is None or not availability_group_matches_record(observed, record):
            raise FixtureError("owned availability group is absent or mismatched")
    else:
        raise FixtureError("availability-group ownership state requires cleanup")

    bind_created_availability_group(root, context, inspect_availability_group(root, name))
    final = inspect_availability_group(root, name)
    if not availability_group_matches_record(final, record):
        raise FixtureError("owned availability-group identity changed")
    write_present_config(root, name)
    return final


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


def valid_object_state(value):
    return value in ["not_started", "creating", "created", "verified", "cleaning", "cleaned"]


def validate_availability_group_record(record):
    if (
        not isinstance(record, dict)
        or set(record) != {"name", "group_id", "profile", "created", "state"}
        or not valid_availability_group_name(record["name"])
        or type(record["created"]) is not bool
        or not valid_object_state(record["state"])
    ):
        return False
    group_id = record["group_id"]
    valid_group_id = (
        isinstance(group_id, str)
        and len(group_id) == 36
        and all(character in "0123456789abcdef-" for character in group_id)
    )
    if not record["created"]:
        return (
            record["state"] == "not_started"
            and group_id is None
            and record["profile"] is None
        )
    if record["state"] in ["creating", "cleaned"]:
        return (
            group_id is None
            and record["profile"] is None
            or valid_group_id
            and record["profile"] == expected_ag_profile()
        )
    return valid_group_id and record["profile"] == expected_ag_profile()


def validate_legacy_availability_group_record(record):
    return (
        isinstance(record, dict)
        and set(record) == {"name", "group_id", "profile", "created", "state"}
        and record["name"] == LEGACY_AVAILABILITY_GROUP_NAME
        and type(record["created"]) is bool
        and valid_object_state(record["state"])
    )


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
    legacy_fields = {
        "schema_version", "root", "created_files", "created_container",
        "started_container", "ephemeral", "container_id", "ready",
    }
    current_fields = legacy_fields | {"availability_group"}
    if (
        not isinstance(context, dict)
        or set(context) not in [legacy_fields, current_fields]
        or context.get("schema_version") not in [2, 4, CONTEXT_SCHEMA_VERSION]
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
    migrated = False
    if context["schema_version"] == 2:
        if set(context) != legacy_fields:
            raise FixtureError("legacy fixture ownership record has incompatible fields")
        context["schema_version"] = CONTEXT_SCHEMA_VERSION
        context["availability_group"] = availability_group_record()
        migrated = True
    elif context["schema_version"] == 4:
        record = context.get("availability_group")
        if set(context) != current_fields or not validate_legacy_availability_group_record(record):
            raise FixtureError("legacy fixture availability-group record is incompatible")
        if record["created"] and record["state"] not in ["cleaned"]:
            raise FixtureError(
                "active legacy fixed-name availability-group ownership must be cleaned "
                "before schema migration"
            )
        context["schema_version"] = CONTEXT_SCHEMA_VERSION
        context["availability_group"] = availability_group_record()
        migrated = True
    elif (
        set(context) != current_fields
        or not validate_availability_group_record(context["availability_group"])
    ):
        raise FixtureError("fixture availability-group ownership record is incompatible")
    if context["availability_group"]["created"] and context["container_id"] is None:
        raise FixtureError("availability-group ownership requires an exact container ID")
    if migrated:
        save_context(root, context)
    return context


def new_context(root):
    return {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "root": str(root),
        "created_files": False,
        "created_container": False,
        "started_container": False,
        "ephemeral": os.environ.get("GITHUB_ACTIONS") == "true",
        "container_id": None,
        "ready": False,
        "availability_group": availability_group_record(),
    }


def ensure_fixture(root):
    context_path = root / "fixture-run.json"
    if context_path.is_symlink():
        raise FixtureError("fixture ownership record must not be a symlink")
    if context_path.exists():
        context = load_context(root)
        if not context["ready"]:
            raise FixtureError("incomplete container preparation requires cleanup before retry")
        save_context(root, context)
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
    local_fixture(root)
    metadata = docker_inspect(container_name(root))
    verify_container(root, metadata, expected_id=context["container_id"])
    wait_for_container(root)
    provision_availability_group(root, context)
    context["ready"] = True
    save_context(root, context)
    if os.environ.get("GITHUB_ENV"):
        with Path(os.environ["GITHUB_ENV"]).open("a") as output:
            for name, value in fixture_environment(root, include_ag=True).items():
                output.write(f"{name}={value}\n")
    print("Container and exact availability-group fixture are ready; host tests have not run yet.")
    return context


def provision(directory=None):
    root = fixture_root(directory)
    with lifecycle_lock():
        return ensure_fixture(root)


def cargo_binary():
    configured = os.environ.get("CARGO")
    if configured:
        resolved = shutil.which(configured)
        if resolved:
            return resolved
        candidate = Path(configured).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        raise FixtureError("CARGO does not identify an executable Cargo binary")
    resolved = shutil.which("cargo")
    if resolved:
        return resolved
    fallback = Path.home() / ".cargo" / "bin" / "cargo"
    if fallback.is_file() and os.access(fallback, os.X_OK):
        return str(fallback)
    raise FixtureError("Cargo is unavailable through CARGO, PATH, or the user-local fallback")


def live_test_command(*, include_ag, include_kuberic):
    args = [cargo_binary(), "test", "--locked"]
    if include_kuberic:
        args += ["--all-features", "--tests"]
    else:
        args += ["--test", "live_observation"]
    args += ["--", "--ignored"]
    if not include_ag:
        args += [
            "--skip", "live_present_availability_group",
            "--skip", "live_kuberic_progress_matches_fresh_direct_observation",
        ]
    args += ["--test-threads=1"]
    return args


def run_test_cases(directory=None, *, include_ag=False, include_kuberic=False):
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
    env.update(fixture_environment(root, include_ag=include_ag))
    args = live_test_command(include_ag=include_ag, include_kuberic=include_kuberic)
    result = command("run host live cases", args, env=env, check=False, timeout=900)
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    if result.returncode:
        raise FixtureError(f"host live cases failed with exit code {result.returncode}")


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


def drop_availability_group(root, name, group_id):
    if not valid_availability_group_name(name):
        raise FixtureError("fixture availability-group name is invalid")
    if (
        not isinstance(group_id, str)
        or len(group_id) != 36
        or any(character not in "0123456789abcdef-" for character in group_id)
    ):
        raise FixtureError("fixture availability-group ID is invalid")
    expected_replicas = [
        (CONTAINER_HOSTNAME, f"TCP://{CONTAINER_HOSTNAME}:{ENDPOINT_PORT}"),
        *[(server, f"TCP://{server}:{ENDPOINT_PORT}") for server in PEER_SERVER_NAMES],
    ]
    replica_checks = "\n".join(
        "AND EXISTS (SELECT 1 FROM sys.availability_replicas AS replica "
        "WHERE replica.group_id = ag.group_id "
        f"AND replica.replica_server_name COLLATE Latin1_General_100_BIN2 = N'{server}' "
        f"AND replica.endpoint_url COLLATE Latin1_General_100_BIN2 = N'{endpoint}' "
        "AND replica.availability_mode_desc COLLATE Latin1_General_100_BIN2 "
        "= N'SYNCHRONOUS_COMMIT' "
        "AND replica.failover_mode_desc COLLATE Latin1_General_100_BIN2 = N'EXTERNAL' "
        "AND replica.seeding_mode_desc COLLATE Latin1_General_100_BIN2 = N'AUTOMATIC')"
        for server, endpoint in expected_replicas
    )
    admin_sql(
        root,
        "DECLARE @expected_group_id uniqueidentifier = "
        f"CONVERT(uniqueidentifier, N'{group_id}');\n"
        "IF NOT EXISTS (\n"
        "SELECT 1 FROM sys.availability_groups AS ag\n"
        f"WHERE ag.name COLLATE Latin1_General_100_BIN2 = N'{name}' "
        "AND ag.group_id = @expected_group_id\n"
        "AND ag.cluster_type_desc COLLATE Latin1_General_100_BIN2 = N'EXTERNAL'\n"
        "AND ag.required_synchronized_secondaries_to_commit = 1\n"
        "AND ag.basic_features = 0 AND ag.is_distributed = 0\n"
        "AND (SELECT COUNT(*) FROM sys.availability_databases_cluster AS database_entry "
        "WHERE database_entry.group_id = ag.group_id) = 0\n"
        "AND (SELECT COUNT(*) FROM sys.availability_replicas AS replica "
        "WHERE replica.group_id = ag.group_id) = 3\n"
        f"{replica_checks}\n"
        ")\n"
        "BEGIN\n"
        "THROW 51000, 'fixture availability-group identity or profile changed', 1;\n"
        "END;\n"
        f"DROP AVAILABILITY GROUP [{name}];",
        stage="drop fixture availability group",
    )


def cleanup_availability_group(root, context):
    record = context["availability_group"]
    if not record["created"] or record["state"] in ["not_started", "cleaned"]:
        (root / "present.json").unlink(missing_ok=True)
        return
    observed = inspect_availability_group(root, record["name"])
    if (
        record["state"] == "creating"
        and record["group_id"] is None
        and observed is None
    ):
        record["state"] = "cleaned"
        save_context(root, context)
        (root / "present.json").unlink(missing_ok=True)
        return
    if observed is None:
        raise FixtureError(
            f"refusing absent owned availability group during cleanup: {record['name']}"
        )
    if record["state"] == "creating":
        bind_created_availability_group(root, context, observed)
    if not availability_group_matches_record(observed, record):
        raise FixtureError(
            f"refusing mismatched owned availability group during cleanup: {record['name']}"
        )
    record["state"] = "cleaning"
    save_context(root, context)
    drop_availability_group(root, record["name"], record["group_id"])
    if inspect_availability_group(root, record["name"]) is not None:
        raise FixtureError("owned availability group still exists after cleanup")
    record["state"] = "cleaned"
    save_context(root, context)
    (root / "present.json").unlink(missing_ok=True)


def container_cleanup_satisfies_availability_group(root, context):
    if context["availability_group"]["created"]:
        context["availability_group"]["state"] = "cleaned"
        save_context(root, context)
    (root / "present.json").unlink(missing_ok=True)


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
            save_context(root, context)
    if context["container_id"] is not None:
        metadata = docker_inspect(container_name(root), allow_absent=True)
        if metadata is None:
            if not context["created_container"]:
                raise FixtureError(
                    "borrowed SQL Server container disappeared before availability-group cleanup"
                )
            container_cleanup_satisfies_availability_group(root, context)
        else:
            _, running = verify_container(
                root, metadata, require_running=False, expected_id=context["container_id"]
            )
            if context["created_container"]:
                remove_exact_container(root, context["container_id"])
                container_cleanup_satisfies_availability_group(root, context)
            else:
                if not running:
                    start_container(root, context["container_id"])
                cleanup_availability_group(root, context)
                if context["started_container"]:
                    stop_exact_container(root, context["container_id"])
                else:
                    print(
                        "Preserved the SQL Server container that was running before this invocation."
                    )
    elif context["availability_group"]["created"]:
        raise FixtureError(
            "availability-group cleanup requires the exact recorded container"
        )
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
            env.update(fixture_environment(root, include_ag=True))
            for stage, args in [
                ("run shared host live cases", ["just", "test-live-shared", str(root)]),
            ]:
                result = command(stage, args, env=env, check=False, timeout=600, process_group=True)
                print(result.stdout, end="")
                print(result.stderr, end="", file=sys.stderr)
                if result.returncode:
                    raise FixtureError(f"{stage} failed with exit code {result.returncode}")
            observer = Path("target/debug/sqlserver-observer")
            if not observer.is_file():
                command(
                    "build observer binary",
                    [cargo_binary(), "build", "--locked", "--bin", "sqlserver-observer"],
                    env=env,
                    timeout=600,
                    process_group=True,
                )
            cli = command(
                "run shared host CLI observation",
                [
                    str(observer),
                    "--config",
                    env["SQLSERVER_LIVE_ABSENT_CONFIG"],
                ],
                env=env,
                check=False,
                timeout=120,
                process_group=True,
            )
            if cli.returncode:
                raise FixtureError(
                    f"run shared host CLI observation failed with exit code {cli.returncode}"
                )
            report = root / "observation.json"
            report.write_text(cli.stdout)
            verify_cli(report)
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
    parser.add_argument(
        "action",
        choices=[
            "provision", "verify-cli", "cleanup", "validate", "test", "test-all",
            "test-kuberic", "test-shared",
        ],
    )
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
        elif args.action in ["test", "test-all", "test-kuberic", "test-shared"]:
            run_test_cases(
                path,
                include_ag=args.action != "test",
                include_kuberic=args.action in ["test-kuberic", "test-shared"],
            )
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
