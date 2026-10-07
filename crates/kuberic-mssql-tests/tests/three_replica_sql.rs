use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use kuberic_mssql_tests::three_replica::{
    AdminDeadlines, AdminEndpoint, AdminError, AdminSession, AvailabilityGroupError,
    BoundedProcessRunner, ChildDisposition, CommandSpec, ContainerInspection, ContainerLimits,
    ContainerMount, ContainerPort, ContainerRequest, DataError, DatabaseEvidence, DockerApi,
    DockerCli, DockerError, EndpointEvidence, EnvironmentVariable, EvidenceError, IncarnationError,
    KubericMember, LoginFiles, MemberEvidence, MemberReadinessEvidence, OwnedLabels, PrivateFile,
    ProcessError, ProcessResult, ProcessRunner, ReadyMember, ReplicaProfileEvidence,
    ResourcePolicy, SQL_SERVER_UID, SecretValue, SeedingEvidence, SqlMember, SqlMemberIncarnation,
    SqlServerContainerSpec, TlsAssets, TopologyRun, validate_binding_incarnations,
    validate_endpoint_evidence, validate_marker_observations, validate_native_evidence,
    validated_identifier,
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
        sql_start_time: "2027-01-15T08:00:00".to_owned(),
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
        sql_start_time: "2027-01-15T08:00:00".to_owned(),
        sql_start_unix_millis: 1_800_000_000_000,
    };
    binding
        .verify(
            "sha256:container-1",
            "2027-01-15T08:00:00",
            1_800_000_000_000,
        )
        .unwrap();
    assert!(
        binding
            .verify(
                "sha256:replacement",
                "2027-01-15T08:00:00",
                1_800_000_000_000,
            )
            .is_err()
    );
    assert!(
        binding
            .verify(
                "sha256:container-1",
                "2027-01-15T08:00:01",
                1_800_000_000_001,
            )
            .is_err()
    );
}

#[test]
fn native_binding_rejects_sql_restart_after_member_readiness() {
    let launched: [ReadyMember; 3] = std::array::from_fn(|index| ReadyMember {
        ordinal: (index + 1) as u8,
        server_name: format!("km0123456789n{}", index + 1),
        container_id: format!("sha256:container-{}", index + 1),
        host_port: 49_171 + index as u16,
        sql_start_time: format!("2027-01-15T08:00:0{index}"),
        sql_start_unix_millis: 1_800_000_000_000 + index as i64,
        observer_config: PathBuf::from(format!("/fixture/member-{}/observer.json", index + 1)),
    });
    let frozen: [SqlMemberIncarnation; 3] = std::array::from_fn(|index| SqlMemberIncarnation {
        ordinal: launched[index].ordinal,
        server_name: launched[index].server_name.clone(),
        container_id: launched[index].container_id.clone(),
        sql_start_time: launched[index].sql_start_time.clone(),
        sql_start_unix_millis: launched[index].sql_start_unix_millis,
    });
    let mut fresh: [MemberReadinessEvidence; 3] =
        std::array::from_fn(|index| MemberReadinessEvidence {
            server_name: launched[index].server_name.clone(),
            product_version: "17.0.5005.3".to_owned(),
            edition: "Enterprise Developer Edition (64-bit)".to_owned(),
            engine_edition: 3,
            hadr_enabled: true,
            sql_start_time: launched[index].sql_start_time.clone(),
            sql_start_unix_millis: launched[index].sql_start_unix_millis,
        });
    let rebound = validate_binding_incarnations(&launched, &frozen, &fresh).unwrap();
    assert_eq!(rebound, frozen);

    fresh[1].sql_start_unix_millis += 1;
    assert!(matches!(
        validate_binding_incarnations(&launched, &frozen, &fresh),
        Err(AvailabilityGroupError::Incarnation(
            IncarnationError::SqlRestarted
        ))
    ));
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

fn native_run(root: &Path) -> TopologyRun {
    TopologyRun {
        run_id: "0123456789ab".to_owned(),
        resource_uid: "resource".to_owned(),
        members: std::array::from_fn(|index| SqlMember {
            ordinal: (index + 1) as u8,
            server_name: format!("km0123456789n{}", index + 1),
            container_name: format!("container-{}", index + 1),
            data_directory: root.join(format!("member-{}", index + 1)),
        }),
        kuberic_members: std::array::from_fn(|index| KubericMember {
            ordinal: (index + 1) as u8,
            replica_id: (index + 1) as i64,
            instance_id: format!("instance-{}", index + 1),
            pod_uid: format!("pod-{}", index + 1),
            pvc_uid: format!("pvc-{}", index + 1),
        }),
    }
}

fn guid(index: u8) -> String {
    format!("00000000-0000-4000-8000-{index:012x}")
}

fn healthy_native_evidence() -> [MemberEvidence; 3] {
    let profiles = (0..3)
        .map(|index| ReplicaProfileEvidence {
            native_replica_id: guid(index + 1),
            server_name: format!("km0123456789n{}", index + 1),
            endpoint_url: format!("TCP://km0123456789n{}:5022", index + 1),
            availability_mode: "SYNCHRONOUS_COMMIT".to_owned(),
            failover_mode: "EXTERNAL".to_owned(),
            seeding_mode: "AUTOMATIC".to_owned(),
        })
        .collect::<Vec<_>>();
    std::array::from_fn(|index| MemberEvidence {
        ordinal: (index + 1) as u8,
        observed_at_unix_millis: 10_000,
        server_name: format!("km0123456789n{}", index + 1),
        availability_group_name: "km_ag_0123456789ab".to_owned(),
        availability_group_id: guid(10),
        configuration_sequence: 4_294_967_307,
        cluster_type: "EXTERNAL".to_owned(),
        required_synchronized_secondaries: 1,
        basic_features: false,
        distributed: false,
        local_replica_id: guid((index + 1) as u8),
        local_role: if index == 0 { "PRIMARY" } else { "SECONDARY" }.to_owned(),
        replica_profiles: profiles.clone(),
        database: DatabaseEvidence {
            name: "km_db_0123456789ab".to_owned(),
            group_database_id: guid(20),
            local_database_id: 5,
            local_replica_id: guid((index + 1) as u8),
            database_guid: guid(21 + index as u8),
            family_guid: guid(22),
            recovery_fork_id: guid(23),
            state: "ONLINE".to_owned(),
            recovery_model: "FULL".to_owned(),
            synchronization_state: "SYNCHRONIZED".to_owned(),
            synchronization_health: "HEALTHY".to_owned(),
            database_state: "ONLINE".to_owned(),
            suspended: false,
        },
        automatic_seeding: if index == 0 {
            vec![2_u8, 3_u8]
                .into_iter()
                .map(|remote| SeedingEvidence {
                    group_database_id: guid(20),
                    remote_replica_id: guid(remote),
                    operation_id: guid(remote + 30),
                    is_source: true,
                    current_state: Some("COMPLETED".to_owned()),
                    performed_seeding: Some(true),
                    failure_state: Some(0),
                    error_code: Some(0),
                    completion_time: Some("2026-10-06T23:00:00".to_owned()),
                })
                .collect()
        } else {
            Vec::new()
        },
    })
}

#[test]
fn native_evidence_accepts_only_fresh_exact_healthy_three_member_topology() {
    let directory = tempfile::tempdir().unwrap();
    let validated = validate_native_evidence(
        &native_run(directory.path()),
        healthy_native_evidence(),
        10_001,
        Duration::from_secs(30),
    )
    .unwrap();
    assert_eq!(validated.primary_ordinal, 1);
    assert_eq!(validated.seeding_operation_ids.len(), 2);
}

#[test]
fn native_evidence_rejects_stale_mismatch_wrong_ids_profiles_and_roles() {
    let directory = tempfile::tempdir().unwrap();
    let run = native_run(directory.path());
    let mut stale = healthy_native_evidence();
    stale[0].observed_at_unix_millis = 1;
    assert_eq!(
        validate_native_evidence(&run, stale, 40_002, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::Stale
    );

    let mut mismatch = healthy_native_evidence();
    mismatch[1].availability_group_id = guid(99);
    assert_eq!(
        validate_native_evidence(&run, mismatch, 10_001, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::IdentityMismatch
    );

    let mut sequence = healthy_native_evidence();
    sequence[2].configuration_sequence += 1;
    assert_eq!(
        validate_native_evidence(&run, sequence, 10_001, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::SequenceMismatch
    );

    let mut wrong_profile_id = healthy_native_evidence();
    wrong_profile_id[2].replica_profiles[1].native_replica_id = guid(98);
    assert_eq!(
        validate_native_evidence(&run, wrong_profile_id, 10_001, Duration::from_secs(30))
            .unwrap_err(),
        EvidenceError::IdentityMismatch
    );

    let mut roles = healthy_native_evidence();
    roles[0].local_role = "SECONDARY".to_owned();
    assert_eq!(
        validate_native_evidence(&run, roles, 10_001, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::RoleMismatch
    );
}

#[test]
fn native_evidence_rejects_unsynchronized_suspended_wrong_lineage_and_database_ids() {
    let directory = tempfile::tempdir().unwrap();
    let run = native_run(directory.path());
    let mut unsynchronized = healthy_native_evidence();
    unsynchronized[2].database.synchronization_state = "SYNCHRONIZING".to_owned();
    assert_eq!(
        validate_native_evidence(&run, unsynchronized, 10_001, Duration::from_secs(30))
            .unwrap_err(),
        EvidenceError::Unsynchronized
    );

    let mut suspended = healthy_native_evidence();
    suspended[1].database.suspended = true;
    assert_eq!(
        validate_native_evidence(&run, suspended, 10_001, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::Suspended
    );

    let mut lineage = healthy_native_evidence();
    lineage[2].database.family_guid = guid(97);
    assert_eq!(
        validate_native_evidence(&run, lineage, 10_001, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::LineageMismatch
    );

    let mut database_id = healthy_native_evidence();
    database_id[1].database.group_database_id = guid(96);
    assert_eq!(
        validate_native_evidence(&run, database_id, 10_001, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::IdentityMismatch
    );
}

#[test]
fn native_evidence_rejects_explicit_seeding_failure() {
    let directory = tempfile::tempdir().unwrap();
    let run = native_run(directory.path());
    let mut evidence = healthy_native_evidence();
    evidence[0].automatic_seeding[0].current_state = Some("FAILED".to_owned());
    evidence[0].automatic_seeding[0].performed_seeding = Some(false);
    evidence[0].automatic_seeding[0].failure_state = Some(108);
    assert_eq!(
        validate_native_evidence(&run, evidence, 10_001, Duration::from_secs(30)).unwrap_err(),
        EvidenceError::SeedingFailed
    );
}

#[test]
fn stopped_or_misbound_endpoint_is_rejected() {
    let mut endpoint = EndpointEvidence {
        ordinal: 1,
        endpoint_name: "kuberic_hadr".to_owned(),
        state: "STARTED".to_owned(),
        port: 5022,
        certificate_name: "km_ep_01234567_1".to_owned(),
        certificate_thumbprint: "a".repeat(40),
    };
    validate_endpoint_evidence(&endpoint, "km_ep_01234567_1").unwrap();
    endpoint.state = "STOPPED".to_owned();
    assert!(validate_endpoint_evidence(&endpoint, "km_ep_01234567_1").is_err());
    endpoint.state = "STARTED".to_owned();
    endpoint.certificate_name = "replacement".to_owned();
    assert!(validate_endpoint_evidence(&endpoint, "km_ep_01234567_1").is_err());
}

#[test]
fn marker_observation_reports_bounded_timeout_until_all_three_are_readable() {
    let now = std::time::Instant::now();
    assert_eq!(
        validate_marker_observations(&[1, 2], now, now).unwrap_err(),
        DataError::Deadline
    );
    assert_eq!(
        validate_marker_observations(&[1, 2], now, now + Duration::from_secs(1)).unwrap_err(),
        DataError::MarkerMismatch
    );
    assert_eq!(
        validate_marker_observations(&[1, 2, 3], now, now).unwrap_err(),
        DataError::Deadline
    );
}
