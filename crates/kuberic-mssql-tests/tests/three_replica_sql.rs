use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use kuberic_mssql_tests::three_replica::{
    AdminDeadlines, AdminEndpoint, AdminError, AdminSession, BoundedProcessRunner,
    ChildDisposition, CommandSpec, ContainerInspection, ContainerLimits, ContainerMount,
    ContainerPort, ContainerRequest, DockerApi, DockerCli, DockerError, EnvironmentVariable,
    LoginFiles, MemberReadinessEvidence, OwnedLabels, PrivateFile, ProcessError, ProcessResult,
    ProcessRunner, ResourcePolicy, SQL_SERVER_UID, SecretValue, SqlMemberIncarnation,
    SqlServerContainerSpec, TlsAssets, validated_identifier,
};

#[derive(Default)]
struct ScriptedRunner {
    results: Mutex<VecDeque<Result<ProcessResult, ProcessError>>>,
    commands: Mutex<Vec<CommandSpec>>,
}

impl ProcessRunner for ScriptedRunner {
    fn run(&self, command: &CommandSpec) -> Result<ProcessResult, ProcessError> {
        self.commands.lock().unwrap().push(command.clone());
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted result")
    }
}

fn successful(stdout: impl Into<String>) -> Result<ProcessResult, ProcessError> {
    Ok(ProcessResult {
        status: 0,
        stdout: stdout.into(),
        stderr: String::new(),
        child: ChildDisposition {
            pid: Some(42),
            terminated: false,
            reaped: true,
        },
    })
}

fn request(directory: &Path) -> ContainerRequest {
    let data = directory.join("member-1");
    fs::create_dir(&data).unwrap();
    ContainerRequest::sql_server(
        SqlServerContainerSpec {
            name: "km-three-run-1".to_owned(),
            hostname: "km0123456789n1".to_owned(),
            network_name: "km-three-network".to_owned(),
            data_directory: data,
            environment_file: directory.join("container.env"),
            sa_password: SecretValue::from_test("request-secret-Aa1!"),
        },
        OwnedLabels::container("0123456789ab", 1),
        ResourcePolicy::default(),
        [("ACCEPT_EULA", "Y")],
    )
    .unwrap()
}

fn inspection(request: &ContainerRequest) -> ContainerInspection {
    ContainerInspection {
        id: "sha256:container-1".to_owned(),
        name: request.name.clone(),
        hostname: request.hostname.clone(),
        image_id: "sha256:image".to_owned(),
        labels: request.labels.as_map(),
        environment: request
            .environment_file_contents()
            .lines()
            .map(str::to_owned)
            .collect(),
        user: "mssql".to_owned(),
        running: false,
        restart_policy: "no".to_owned(),
        network_mode: request.network_name.clone(),
        network_names: vec![request.network_name.clone()],
        mounts: vec![ContainerMount {
            source: request.data_directory.canonicalize().unwrap(),
            destination: PathBuf::from("/var/opt/mssql"),
            read_only: false,
        }],
        ports: vec![ContainerPort {
            container_port: 1433,
            host_ip: "127.0.0.1".to_owned(),
            host_port: 49171,
        }],
        limits: ContainerLimits::from_policy(ResourcePolicy::default()),
    }
}

#[test]
fn launch_request_freezes_exact_environment_limits_mount_port_labels_and_hostname() {
    let directory = tempfile::tempdir().unwrap();
    let request = request(directory.path());
    assert!(request.hostname.len() <= 15);
    assert_eq!(
        request.environment_keys().collect::<Vec<_>>(),
        [
            "ACCEPT_EULA",
            "MSSQL_PID",
            "MSSQL_SA_PASSWORD",
            "MSSQL_ENABLE_HADR",
            "MSSQL_MEMORY_LIMIT_MB"
        ]
    );
    assert_eq!(request.limits.memory_bytes, 3 * 1024 * 1024 * 1024);
    assert_eq!(
        request.limits.memory_swap_bytes,
        request.limits.memory_bytes
    );
    assert_eq!(request.limits.nano_cpus, 2_000_000_000);
    request
        .verify_inspection(&inspection(&request), "sha256:image", false)
        .unwrap();
}

#[test]
fn duplicate_or_changed_controlled_environment_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let request = request(directory.path());
    let mut duplicate = inspection(&request);
    duplicate.environment.push("MSSQL_ENABLE_HADR=1".to_owned());
    assert_eq!(
        request
            .verify_inspection(&duplicate, "sha256:image", false)
            .unwrap_err(),
        DockerError::OwnershipMismatch
    );

    let mut wrong = inspection(&request);
    *wrong
        .environment
        .iter_mut()
        .find(|entry| entry.starts_with("MSSQL_PID="))
        .unwrap() = "MSSQL_PID=Developer".to_owned();
    assert_eq!(
        request
            .verify_inspection(&wrong, "sha256:image", false)
            .unwrap_err(),
        DockerError::OwnershipMismatch
    );
}

#[test]
fn create_uses_private_env_file_and_never_places_password_in_argv_or_debug() {
    let directory = tempfile::tempdir().unwrap();
    let request = request(directory.path());
    let runner = ScriptedRunner::default();
    runner
        .results
        .lock()
        .unwrap()
        .push_back(successful("sha256:container-1\n"));
    let docker = DockerCli::new(runner);
    docker
        .create_container(&request, Duration::from_secs(30))
        .unwrap();
    let command = docker.runner().commands.lock().unwrap()[0].clone();
    let arguments = command
        .arguments()
        .iter()
        .map(|argument| argument.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(arguments.contains("--env-file"));
    assert!(!arguments.contains("request-secret-Aa1!"));
    assert!(!format!("{request:?}").contains("request-secret-Aa1!"));
}

#[test]
fn structured_inspection_parses_every_launch_property() {
    let directory = tempfile::tempdir().unwrap();
    let request = request(directory.path());
    let expected = inspection(&request);
    let json = serde_json::json!([{
        "Id": expected.id,
        "Name": format!("/{}", expected.name),
        "Image": expected.image_id,
        "Config": {
            "Hostname": expected.hostname,
            "Labels": expected.labels,
            "Env": expected.environment,
            "User": expected.user,
        },
        "State": {"Running": false},
        "HostConfig": {
            "RestartPolicy": {"Name": "no"},
            "NetworkMode": expected.network_mode,
            "Memory": expected.limits.memory_bytes,
            "MemorySwap": expected.limits.memory_swap_bytes,
            "NanoCpus": expected.limits.nano_cpus,
        },
        "Mounts": [{
            "Source": expected.mounts[0].source,
            "Destination": "/var/opt/mssql",
            "RW": true,
        }],
        "NetworkSettings": {
            "Networks": {request.network_name.clone(): {}},
            "Ports": {"1433/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49171"}]},
        },
    }]);
    let runner = ScriptedRunner::default();
    runner
        .results
        .lock()
        .unwrap()
        .push_back(successful(json.to_string()));
    let docker = DockerCli::new(runner);
    let parsed = docker
        .inspect_container("sha256:container-1", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    request
        .verify_inspection(&parsed, "sha256:image", false)
        .unwrap();
}

#[test]
fn frozen_running_inspection_rejects_exact_port_binding_drift() {
    let directory = tempfile::tempdir().unwrap();
    let request = request(directory.path());
    let mut frozen = inspection(&request);
    frozen.running = true;
    let mut changed = frozen.clone();
    changed.ports[0].host_port += 1;
    assert_eq!(
        request
            .verify_frozen_running_inspection(&changed, &frozen, "sha256:image")
            .unwrap_err(),
        DockerError::OwnershipMismatch
    );
}

#[test]
fn readiness_rejects_wrong_identity_version_edition_hadr_and_start() {
    let valid = MemberReadinessEvidence {
        server_name: "km0123456789n1".to_owned(),
        product_version: "17.0.5005.3".to_owned(),
        edition: "Enterprise Developer Edition (64-bit)".to_owned(),
        engine_edition: 3,
        hadr_enabled: true,
        sql_start_unix_millis: 1_800_000_000_000,
    };
    valid.verify("km0123456789n1").unwrap();
    let cases = [
        (
            MemberReadinessEvidence {
                server_name: "other".to_owned(),
                ..valid.clone()
            },
            AdminError::WrongIdentity,
        ),
        (
            MemberReadinessEvidence {
                product_version: "17.0.5005.2".to_owned(),
                ..valid.clone()
            },
            AdminError::WrongVersion,
        ),
        (
            MemberReadinessEvidence {
                engine_edition: 2,
                ..valid.clone()
            },
            AdminError::WrongEdition,
        ),
        (
            MemberReadinessEvidence {
                hadr_enabled: false,
                ..valid.clone()
            },
            AdminError::HadrDisabled,
        ),
        (
            MemberReadinessEvidence {
                sql_start_unix_millis: 0,
                ..valid.clone()
            },
            AdminError::InvalidStartIdentity,
        ),
    ];
    for (evidence, expected) in cases {
        assert_eq!(evidence.verify("km0123456789n1").unwrap_err(), expected);
    }
}

#[test]
fn logical_incarnation_is_bound_to_container_id_and_sql_start() {
    let binding = SqlMemberIncarnation {
        ordinal: 1,
        server_name: "km0123456789n1".to_owned(),
        container_id: "sha256:container-1".to_owned(),
        sql_start_unix_millis: 1_800_000_000_000,
    };
    binding
        .verify("sha256:container-1", 1_800_000_000_000)
        .unwrap();
    assert!(
        binding
            .verify("sha256:replacement", 1_800_000_000_000)
            .is_err()
    );
    assert!(
        binding
            .verify("sha256:container-1", 1_800_000_000_001)
            .is_err()
    );
}

#[test]
fn private_secrets_are_no_overwrite_mode_0600_and_redacted() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("secret");
    let value = SecretValue::from_test("actual-secret-Aa1!");
    let file = PrivateFile::create(&path, &value).unwrap();
    assert_eq!(
        fs::symlink_metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(PrivateFile::create(&path, &value).is_err());
    file.verify().unwrap();
    assert!(!format!("{value:?}").contains("actual-secret-Aa1!"));
}

#[test]
fn tls_generation_is_bounded_private_verified_and_no_overwrite() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let members: [PathBuf; 3] = std::array::from_fn(|index| {
        let path = root.join(format!("member-{}", index + 1));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        let acl = format!(
            "u:{}:rwx,u:{SQL_SERVER_UID}:rwx,d:u:{}:rwx,d:u:{SQL_SERVER_UID}:rwx,m:rwx,d:m:rwx",
            unsafe { libc::geteuid() },
            unsafe { libc::geteuid() }
        );
        std::process::Command::new("setfacl")
            .args(["-m", &acl])
            .arg(&path)
            .status()
            .unwrap()
            .success()
            .then_some(())
            .unwrap();
        path
    });
    let assets = TlsAssets::generate(
        &root,
        ["km0123456789n1", "km0123456789n2", "km0123456789n3"],
        [
            members[0].as_path(),
            members[1].as_path(),
            members[2].as_path(),
        ],
        Duration::from_secs(30),
        &BoundedProcessRunner,
    )
    .unwrap();
    assets.ca_certificate.verify().unwrap();
    assets.ca_private_key.verify().unwrap();
    for member in &assets.members {
        member.server_certificate.verify().unwrap();
        member.server_private_key.verify().unwrap();
        assert!(member.endpoint_exchange_directory.is_dir());
    }
    assert!(
        TlsAssets::generate(
            &root,
            ["km0123456789n1", "km0123456789n2", "km0123456789n3"],
            [
                members[0].as_path(),
                members[1].as_path(),
                members[2].as_path(),
            ],
            Duration::from_secs(30),
            &BoundedProcessRunner,
        )
        .is_err()
    );
}

#[tokio::test]
async fn admin_deadline_and_endpoint_rejection_paths_are_exercised() {
    assert!(validated_identifier("km_admin_01234567").is_ok());
    assert_eq!(
        validated_identifier("bad];DROP LOGIN sa--").unwrap_err(),
        AdminError::InvalidIdentifier
    );
    let secret = "admin-super-secret-Aa1!";
    for error in [
        AdminError::SecretFile,
        AdminError::TlsOrLogin,
        AdminError::Query,
        AdminError::QueryDeadline,
    ] {
        assert!(
            !format!("{error}: {secret}")
                .split(": ")
                .next()
                .unwrap()
                .contains(secret)
        );
        assert!(!error.to_string().contains(secret));
    }
    let directory = tempfile::tempdir().unwrap();
    let ca = PrivateFile::create_text(directory.path().join("ca.crt"), "test-ca").unwrap();
    let username = PrivateFile::create_text(directory.path().join("username"), "sa").unwrap();
    let password =
        PrivateFile::create_text(directory.path().join("password"), "admin-super-secret-Aa1!")
            .unwrap();
    let login = LoginFiles {
        username: username.path().to_path_buf(),
        password: password.path().to_path_buf(),
    };
    let endpoint = AdminEndpoint {
        tcp_host: "0.0.0.0".to_owned(),
        tls_hostname: "localhost".to_owned(),
        port: 1433,
        ca_certificate: ca.path().to_path_buf(),
    };
    let error = AdminSession::connect(
        &endpoint,
        &login,
        AdminDeadlines {
            connect: Duration::from_secs(1),
            query: Duration::from_secs(1),
        },
    )
    .await
    .err()
    .expect("endpoint mismatch must be rejected");
    assert_eq!(error, AdminError::InvalidEndpoint);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    let endpoint = AdminEndpoint {
        tcp_host: "127.0.0.1".to_owned(),
        tls_hostname: "localhost".to_owned(),
        port,
        ca_certificate: ca.path().to_path_buf(),
    };
    let error = AdminSession::connect(
        &endpoint,
        &login,
        AdminDeadlines {
            connect: Duration::from_millis(25),
            query: Duration::from_secs(1),
        },
    )
    .await
    .err()
    .expect("controlled peer must exceed the connect deadline");
    assert_eq!(error, AdminError::ConnectDeadline);
    server.abort();
}

#[test]
fn environment_variable_validation_rejects_invalid_keys_and_values() {
    assert!(EnvironmentVariable::new("bad-key", SecretValue::from_test("value")).is_err());
    assert!(EnvironmentVariable::new("GOOD_KEY", SecretValue::from_test("line\nbreak")).is_err());
}

#[test]
fn network_inspection_requires_exact_bridge_identity() {
    let request = kuberic_mssql_tests::three_replica::NetworkRequest {
        name: "km-three-network".to_owned(),
        labels: OwnedLabels::network("0123456789ab"),
    };
    let valid = kuberic_mssql_tests::three_replica::NetworkInspection {
        id: "network-id".to_owned(),
        name: request.name.clone(),
        driver: "bridge".to_owned(),
        labels: request.labels.as_map(),
        attached_container_ids: Vec::new(),
    };
    request.verify_inspection(&valid).unwrap();
    let mut wrong = valid;
    wrong.driver = "overlay".to_owned();
    assert_eq!(
        request.verify_inspection(&wrong).unwrap_err(),
        DockerError::OwnershipMismatch
    );
}

#[test]
fn redacted_types_do_not_leak_through_nested_debug() {
    let variable =
        EnvironmentVariable::new("MSSQL_SA_PASSWORD", SecretValue::from_test("nested-secret"))
            .unwrap();
    assert!(!format!("{variable:?}").contains("nested-secret"));
    let labels = OwnedLabels::container("run", 1).as_map();
    assert_eq!(
        labels,
        BTreeMap::from([
            (
                "io.kuberic.mssql.fixture".to_owned(),
                "three-replica".to_owned()
            ),
            ("io.kuberic.mssql.kind".to_owned(), "container".to_owned()),
            ("io.kuberic.mssql.member".to_owned(), "1".to_owned()),
            ("io.kuberic.mssql.run".to_owned(), "run".to_owned()),
        ])
    );
}
