use std::collections::VecDeque;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use kuberic_mssql::kuberic::{ObservationClock, SqlServerObservationSource};
use kuberic_mssql::observation::{
    AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, DatabaseSnapshot, InstanceMetadata,
    InstanceSnapshot, LocalDatabaseSnapshot, LocalRecoveryMetadata, LocalReplicaSnapshot,
    NativeProvenance, RecoveryLineageObservation, ReplicaSnapshot, ReplicaState,
};
use kuberic_mssql::runtime_error::RuntimeError;
use kuberic_mssql::runtime_host::{
    RuntimeEndpointResolver, RuntimeHostApplication, RuntimeHostArgs, RuntimeHostConfig,
};
use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, ConfigurationSequence, DatabaseIdentity,
    DatabaseLineage, DecimalProgress, Guid, NativeProgress, NativeRole, Observation,
    ReplicaIdentity as SqlReplicaIdentity, ServerName, SqlIdentifier,
};
use kuberic_runtime::control::proto::{self as wire, agent_control_client::AgentControlClient};
use kuberic_runtime::host::{ReplicaHost, RunningReplica};
use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ConfigurationMember, Epoch, PodUid, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid, derive_agent_generation,
    derive_initialization_id,
};
use tonic::{Request, transport::Channel};

const TOKEN: &str = "runtime-agent-token";
const OBSERVED_AT: u64 = 1_000;
const NATIVE_IDS: [&str; 3] = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
];

struct RepeatingSource {
    config: kuberic_mssql::runtime_config::ObserverConfig,
    samples: Mutex<VecDeque<InstanceSnapshot>>,
    fallback: InstanceSnapshot,
}

#[async_trait]
impl SqlServerObservationSource for RepeatingSource {
    fn observer_config(&self) -> &kuberic_mssql::runtime_config::ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        let value = self
            .samples
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.fallback.clone());
        Ok(Observation::Present {
            value,
            observed_at_unix_millis: OBSERVED_AT,
        })
    }
}

struct FixedClock;

impl ObservationClock for FixedClock {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
        Ok(OBSERVED_AT)
    }
}

struct HostAttempt {
    control: SocketAddr,
    task: tokio::task::JoinHandle<kuberic_runtime::host::Result<RunningReplica>>,
}

fn guid(value: &str) -> Guid {
    Guid::parse("test GUID", value).unwrap()
}

fn identities() -> [ReplicaIdentity; 3] {
    std::array::from_fn(|index| {
        let resource = ResourceUid::new("runtime-host-test");
        let replica_id = ReplicaId::new(index as i64 + 1);
        let pod_uid = PodUid::new(format!("pod-{}", index + 1));
        let pvc_uid = PvcUid::new(format!("pvc-{}", index + 1));
        let initialization = derive_initialization_id(&resource, replica_id, &pod_uid, &pvc_uid);
        ReplicaIdentity {
            replica_id,
            instance_id: ReplicaInstanceId::new(pod_uid.as_str()),
            agent_generation: derive_agent_generation(&initialization),
        }
    })
}

fn configuration() -> ConfigurationDescriptor {
    let identities = identities();
    ConfigurationDescriptor::new(
        Epoch::new(0, 42),
        ReplicaId::new(1),
        identities
            .into_iter()
            .enumerate()
            .map(|(index, identity)| ConfigurationMember {
                identity,
                role: if index == 0 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    )
}

fn snapshot(local_index: usize) -> InstanceSnapshot {
    let native_role = if local_index == 0 {
        NativeRole::Primary
    } else {
        NativeRole::Secondary
    };
    let local_identity = SqlReplicaIdentity::observed(
        format!("logical-{}", local_index + 1),
        guid(NATIVE_IDS[local_index]),
        format!("pod-{}", local_index + 1),
    )
    .unwrap();
    let database_lineage = DatabaseLineage {
        database: DatabaseIdentity {
            name: SqlIdentifier::new("app-db").unwrap(),
            group_database_id: guid("44444444-4444-4444-8444-444444444444"),
        },
        recovery_fork_id: guid("55555555-5555-4555-8555-555555555555"),
    };
    InstanceSnapshot {
        observed_at_unix_millis: OBSERVED_AT,
        instance: InstanceMetadata {
            server_name: ServerName::new(format!("sql-{}", local_index + 1)).unwrap(),
            property_server_name: ServerName::new(format!("sql-{}", local_index + 1)).unwrap(),
            product_version: "17.0.5005.3".into(),
            product_major_version: 17,
            edition: "Enterprise Developer Edition (64-bit)".into(),
            engine_edition: 3,
            hadr_enabled: true,
            host_platform: "Linux".into(),
            host_distribution: Some("Ubuntu".into()),
            architecture: "x86_64".into(),
            sqlserver_start_time: format!("2026-10-07T12:00:0{local_index}"),
        },
        availability_group: Observation::Present {
            value: AvailabilityGroupSnapshot {
                identity: AvailabilityGroupIdentity {
                    name: AvailabilityGroupName::new("app-ag").unwrap(),
                    group_id: guid("66666666-6666-4666-8666-666666666666"),
                },
                configuration_sequence: ConfigurationSequence::parse("42").unwrap(),
                cluster_type: "EXTERNAL".into(),
                required_synchronized_secondaries_to_commit: 1,
                basic_features: false,
                is_distributed: false,
                local_replica: LocalReplicaSnapshot {
                    identity: local_identity,
                    state_available: true,
                    role: Some(native_role.clone()),
                },
                replicas: (0..3)
                    .map(|index| ReplicaSnapshot {
                        replica_id: guid(NATIVE_IDS[index]),
                        server_name: ServerName::new(format!("sql-{}", index + 1)).unwrap(),
                        endpoint_url: Some(format!("TCP://sql-{}:5022", index + 1)),
                        availability_mode: "SYNCHRONOUS_COMMIT".into(),
                        failover_mode: "EXTERNAL".into(),
                        seeding_mode: "AUTOMATIC".into(),
                        state: (index == local_index).then_some(ReplicaState {
                            provenance: NativeProvenance::Local,
                            role: Some(native_role.clone()),
                            operational_state: Some("ONLINE".into()),
                            connected_state: Some("CONNECTED".into()),
                            recovery_health: Some("ONLINE".into()),
                            synchronization_health: Some("HEALTHY".into()),
                            last_connect_error_number: Some(0),
                        }),
                    })
                    .collect(),
                databases: vec![DatabaseSnapshot {
                    identity: database_lineage.database.clone(),
                    local: Some(LocalDatabaseSnapshot {
                        database_id: 5,
                        replica_id: guid(NATIVE_IDS[local_index]),
                        state: Some("ONLINE".into()),
                        recovery_model: Some("FULL".into()),
                        recovery: Some(LocalRecoveryMetadata {
                            database_guid: Some(guid("77777777-7777-4777-8777-777777777777")),
                            family_guid: Some(guid("88888888-8888-4888-8888-888888888888")),
                            recovery_fork_guid: Some(database_lineage.recovery_fork_id.clone()),
                            first_recovery_fork_guid: Some(
                                database_lineage.recovery_fork_id.clone(),
                            ),
                            fork_point_lsn: None,
                        }),
                    }),
                    replicas: vec![DatabaseReplicaSnapshot {
                        group_database_id: database_lineage.database.group_database_id.clone(),
                        replica_id: guid(NATIVE_IDS[local_index]),
                        database_id: 5,
                        provenance: NativeProvenance::Local,
                        lineage: RecoveryLineageObservation::Local {
                            value: database_lineage,
                        },
                        is_primary_replica: Some(local_index == 0),
                        synchronization_state: Some("SYNCHRONIZED".into()),
                        synchronization_health: Some("HEALTHY".into()),
                        database_state: Some("ONLINE".into()),
                        is_suspended: Some(false),
                        suspend_reason: None,
                        is_commit_participant: Some(true),
                        progress: NativeProgress {
                            hardened_block: Some(DecimalProgress::parse("100").unwrap()),
                            redone_record: Some(DecimalProgress::parse("100").unwrap()),
                            committed_record: Some(DecimalProgress::parse("100").unwrap()),
                        },
                    }],
                }],
                automatic_seeding: Vec::new(),
                physical_seeding: Vec::new(),
            },
            observed_at_unix_millis: OBSERVED_AT,
        },
    }
}

fn free_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn write_private(path: &Path, value: impl AsRef<[u8]>) {
    std::fs::write(path, value).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

async fn host_config(
    root: &Path,
    index: usize,
    controls: &[SocketAddr; 3],
    replications: &[SocketAddr; 3],
) -> RuntimeHostConfig {
    let username = root.join("observer-username");
    let password = root.join("observer-password");
    let token = root.join("agent-token");
    write_private(&username, "observer");
    write_private(&password, "password");
    write_private(&token, TOKEN);
    let observer = root.join("observer.json");
    write_private(
        &observer,
        format!(
            r#"{{
                "host":"sql-{}.example",
                "port":1433,
                "availability_group":"app-ag",
                "expected_server_name":"sql-{}",
                "replica_id":"logical-{}",
                "incarnation":"pod-{}",
                "observer_username_file":"{}",
                "observer_password_file":"{}"
            }}"#,
            index + 1,
            index + 1,
            index + 1,
            index + 1,
            username.display(),
            password.display()
        ),
    );
    let topology = root.join("topology.json");
    write_private(
        &topology,
        br#"{
            "schema_version":1,
            "members":[
                {"replica_id":1,"server_name":"sql-1","endpoint_url":"TCP://sql-1:5022"},
                {"replica_id":2,"server_name":"sql-2","endpoint_url":"TCP://sql-2:5022"},
                {"replica_id":3,"server_name":"sql-3","endpoint_url":"TCP://sql-3:5022"}
            ]
        }"#,
    );
    let all_identities = identities();
    let routes = root.join("routes.json");
    let route_values = (0..3)
        .filter(|peer| *peer != index)
        .map(|peer| {
            serde_json::json!({
                "replica_id": all_identities[peer].replica_id.value(),
                "instance_id": all_identities[peer].instance_id.as_str(),
                "agent_generation": all_identities[peer].agent_generation.as_str(),
                "control_endpoint": format!("http://{}", controls[peer]),
                "replication_endpoint": format!("http://{}", replications[peer])
            })
        })
        .collect::<Vec<_>>();
    write_private(
        &routes,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "routes": route_values
        }))
        .unwrap(),
    );
    let data_root = root.join("state");
    if !data_root.exists() {
        std::fs::create_dir(&data_root).unwrap();
        std::fs::set_permissions(&data_root, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    RuntimeHostConfig::load(
        RuntimeHostArgs {
            resource_uid: "runtime-host-test".into(),
            replica_id: index as i64 + 1,
            pod_uid: format!("pod-{}", index + 1),
            pvc_uid: format!("pvc-{}", index + 1),
            data_root,
            application_root: None,
            control_address: controls[index],
            replication_address: replications[index],
            control_endpoint: format!("http://{}", controls[index]),
            replication_endpoint: format!("http://{}", replications[index]),
            namespace: None,
            peer_routes: Some(routes),
            observer_config: observer,
            topology_config: topology,
            bearer_token_file: token,
            rpc_deadline_ms: 5_000,
            transport_window_capacity: 64,
            shutdown_deadline_ms: 10_000,
        },
        root,
    )
    .await
    .unwrap()
}

async fn start_attempts(root: &Path) -> Vec<HostAttempt> {
    let controls = std::array::from_fn(|_| free_address());
    let replications = std::array::from_fn(|_| free_address());
    let mut attempts = Vec::new();
    for index in 0..3 {
        let member_root = root.join(format!("member-{}", index + 1));
        if !member_root.exists() {
            std::fs::create_dir(&member_root).unwrap();
        }
        let config = host_config(&member_root, index, &controls, &replications).await;
        let source = Arc::new(RepeatingSource {
            config: config.observer().clone(),
            samples: Mutex::new(VecDeque::new()),
            fallback: snapshot(index),
        });
        let application = Arc::new(
            RuntimeHostApplication::with_observation_source(&config, source, Arc::new(FixedClock))
                .unwrap(),
        );
        let host = ReplicaHost::new(
            config.replica_process_config(),
            application.clone(),
            application.storage_state().unwrap(),
            Arc::new(RuntimeEndpointResolver::new(&config).unwrap()),
        )
        .with_application_storage_paths(application.storage_paths());
        attempts.push(HostAttempt {
            control: controls[index],
            task: tokio::spawn(host.start()),
        });
    }
    attempts
}

async fn assert_initialization_listeners(attempts: &mut [HostAttempt]) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    for attempt in attempts.iter_mut() {
        if attempt.task.is_finished() {
            let result = (&mut attempt.task).await;
            match result {
                Ok(Err(error)) => panic!("public host initialization failed: {error}"),
                Err(error) => panic!("public host initialization task failed: {error}"),
                Ok(Ok(_)) => panic!("public host became ready before initialization"),
            }
        }
    }
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    request
}

async fn status(
    address: SocketAddr,
    replica_id: i64,
) -> (AgentControlClient<Channel>, wire::AgentStatusReport) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut last_error: Option<String>;
    loop {
        match AgentControlClient::connect(format!("http://{address}")).await {
            Ok(mut client) => {
                match client
                    .get_status(authorized(wire::GetAgentStatusRequest {
                        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                        resource_uid: "runtime-host-test".into(),
                        replica_id,
                        expected_instance_id: format!("pod-{replica_id}"),
                    }))
                    .await
                {
                    Ok(report) => return (client, report.into_inner()),
                    Err(error) => last_error = Some(error.to_string()),
                }
            }
            Err(error) => last_error = Some(error.to_string()),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "public agent status failed at {address}: {}",
            last_error.as_deref().unwrap_or("no response")
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn initialization(session: &str, index: usize) -> Request<wire::ExecuteCommandRequest> {
    let identities = identities();
    let resource = ResourceUid::new("runtime-host-test");
    let replica_id = ReplicaId::new(index as i64 + 1);
    let pod_uid = PodUid::new(format!("pod-{}", index + 1));
    let pvc_uid = PvcUid::new(format!("pvc-{}", index + 1));
    let initialization = derive_initialization_id(&resource, replica_id, &pod_uid, &pvc_uid);
    authorized(wire::ExecuteCommandRequest {
        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        resource_uid: resource.to_string(),
        target: Some(identities[index].clone().into()),
        expected_process_session_id: session.into(),
        command: Some(
            wire::execute_command_request::Command::InitializeAgentStore(
                wire::InitializeAgentStoreCommand {
                    initialization_id: initialization.to_string(),
                    resource_uid: resource.to_string(),
                    local_replica_id: replica_id.value(),
                    expected_instance_id: format!("pod-{}", index + 1),
                    expected_pod_uid: format!("pod-{}", index + 1),
                    expected_pvc_uid: format!("pvc-{}", index + 1),
                    assigned_agent_generation: identities[index].agent_generation.to_string(),
                    effective_policy: Some(wire::EffectivePolicy {
                        replica_set_size: 3,
                        write_quorum: 2,
                        read_quorum: 2,
                        failover_delay_seconds: 30,
                    }),
                    bootstrap_configuration: Some(configuration().into()),
                    provisioning: None,
                },
            ),
        ),
    })
}

fn stale_configuration(old_session: &str, index: usize) -> Request<wire::ExecuteCommandRequest> {
    ensure_configuration(old_session, index, "stale-session-command")
}

fn ensure_configuration(
    session: &str,
    index: usize,
    operation_id: &str,
) -> Request<wire::ExecuteCommandRequest> {
    let identities = identities();
    authorized(wire::ExecuteCommandRequest {
        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        resource_uid: "runtime-host-test".into(),
        target: Some(identities[index].clone().into()),
        expected_process_session_id: session.into(),
        command: Some(wire::execute_command_request::Command::EnsureConfiguration(
            Box::new(wire::EnsureConfigurationCommand {
                operation_id: operation_id.into(),
                current_configuration: Some(configuration().into()),
                current_epoch: Some(configuration().epoch.into()),
                effective_policy: Some(wire::EffectivePolicy {
                    replica_set_size: 3,
                    write_quorum: 2,
                    read_quorum: 2,
                    failover_delay_seconds: 30,
                }),
                local_replica_id: index as i64 + 1,
                expected_instance_id: format!("pod-{}", index + 1),
                expected_agent_generation: identities[index].agent_generation.to_string(),
                transition_kind: wire::TransitionKind::Bootstrap as i32,
                primary_write_status: wire::AccessStatus::ReconfigurationPending as i32,
                current_only: false,
                ..Default::default()
            }),
        )),
    })
}

async fn initialize_attempts(attempts: &mut [HostAttempt]) -> (Vec<RunningReplica>, Vec<String>) {
    let mut clients = Vec::new();
    let mut sessions = Vec::new();
    for (index, attempt) in attempts.iter().enumerate() {
        let (client, report) = status(attempt.control, index as i64 + 1).await;
        clients.push(client);
        sessions.push(report.process_session_id);
    }
    let results = join_all(clients.into_iter().enumerate().map(|(index, mut client)| {
        let request = initialization(&sessions[index], index);
        async move { client.execute(request).await }
    }))
    .await;
    for result in results {
        result.unwrap();
    }
    let mut replicas = Vec::new();
    for attempt in attempts.iter_mut() {
        replicas.push(
            tokio::time::timeout(Duration::from_secs(30), &mut attempt.task)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let sessions = configure_attempts(attempts).await;
    (replicas, sessions)
}

async fn configure_attempts(attempts: &[HostAttempt]) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut clients = Vec::new();
        let mut sessions = Vec::new();
        for (index, attempt) in attempts.iter().enumerate() {
            let (client, report) = status(attempt.control, index as i64 + 1).await;
            clients.push(client);
            sessions.push(report.process_session_id);
        }
        let results = join_all(clients.into_iter().enumerate().map(|(index, mut client)| {
            let request = ensure_configuration(
                &sessions[index],
                index,
                &format!("bootstrap-configuration-{}", index + 1),
            );
            async move { client.execute(request).await }
        }))
        .await;
        if results.iter().all(Result::is_ok) {
            return sessions;
        }
        let non_retryable = results
            .iter()
            .filter_map(|result| result.as_ref().err())
            .filter(|error| error.code() != tonic::Code::Cancelled)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert!(non_retryable.is_empty(), "{non_retryable:?}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "public current configuration retries exceeded their deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn reports(attempts: &[HostAttempt]) -> Vec<wire::AgentStatusReport> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut reports = Vec::new();
        for (index, attempt) in attempts.iter().enumerate() {
            reports.push(status(attempt.control, index as i64 + 1).await.1);
        }
        if reports.iter().all(|report| {
            report.healthy
                && report.storage_state == wire::AgentStorageState::Initialized as i32
                && report.reported_fault != wire::FaultType::Permanent as i32
                && report.current_progress == 42
                && report.catch_up_capability == Some(42)
        }) {
            return reports;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "public reports did not converge: {:?}",
            reports
                .iter()
                .map(|report| (
                    report.replica_id,
                    report.healthy,
                    report.role,
                    report.current_progress,
                    report.catch_up_capability,
                    report
                        .current_configuration
                        .as_ref()
                        .map(|value| value.configuration_id.clone()),
                    report.storage_error.clone(),
                ))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn stop_all(replicas: &mut [RunningReplica]) {
    for replica in replicas.iter() {
        replica.shutdown();
    }
    for replica in replicas {
        tokio::time::timeout(Duration::from_secs(10), replica.wait())
            .await
            .unwrap()
            .unwrap();
    }
}

async fn public_hosts_initialize_report_and_restart_with_fresh_sessions_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut first_attempts = start_attempts(temporary.path()).await;
    assert_initialization_listeners(&mut first_attempts).await;
    let (mut first_replicas, first_sessions) = initialize_attempts(&mut first_attempts).await;
    let first_reports = reports(&first_attempts).await;
    assert_eq!(
        first_reports
            .iter()
            .filter(|report| report.role == wire::ReplicaRole::Primary as i32)
            .count(),
        1
    );
    assert_eq!(
        first_reports
            .iter()
            .filter(|report| report.role == wire::ReplicaRole::ActiveSecondary as i32)
            .count(),
        2
    );
    assert!(
        first_reports
            .iter()
            .all(|report| report.write_status != wire::AccessStatus::Granted as i32)
    );
    stop_all(&mut first_replicas).await;
    drop(first_replicas);

    let mut restarted_attempts = start_attempts(temporary.path()).await;
    let mut restarted = Vec::new();
    for attempt in &mut restarted_attempts {
        restarted.push(
            tokio::time::timeout(Duration::from_secs(30), &mut attempt.task)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
    }
    let restarted_reports = reports(&restarted_attempts).await;
    let restarted_sessions = restarted_reports
        .iter()
        .map(|report| report.process_session_id.clone())
        .collect::<Vec<_>>();
    assert!(
        first_sessions
            .iter()
            .zip(&restarted_sessions)
            .all(|(first, restarted)| first != restarted)
    );

    let (mut client, _) = status(restarted_attempts[0].control, 1).await;
    let error = client
        .execute(stale_configuration(&first_sessions[0], 0))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("stale agent process session"));
    stop_all(&mut restarted).await;
}

async fn application_binding_without_agent_metadata_is_reported_unsafe_case() {
    let temporary = tempfile::tempdir().unwrap();
    let controls = std::array::from_fn(|_| free_address());
    let replications = std::array::from_fn(|_| free_address());
    let root = temporary.path().join("member-1");
    std::fs::create_dir(&root).unwrap();
    let config = host_config(&root, 0, &controls, &replications).await;
    let source = Arc::new(RepeatingSource {
        config: config.observer().clone(),
        samples: Mutex::new(VecDeque::new()),
        fallback: snapshot(0),
    });
    let application = Arc::new(
        RuntimeHostApplication::with_observation_source(&config, source, Arc::new(FixedClock))
            .unwrap(),
    );
    let binding = kuberic_mssql::runtime_host::RuntimeBindingStore::new(&config).unwrap();
    binding.initialize().unwrap();
    let host = ReplicaHost::new(
        config.replica_process_config(),
        application.clone(),
        application.storage_state().unwrap(),
        Arc::new(RuntimeEndpointResolver::new(&config).unwrap()),
    )
    .with_application_storage_paths(application.storage_paths());
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut task = tokio::spawn(host.start_with_shutdown(receiver));
    tokio::time::sleep(Duration::from_millis(100)).await;
    if task.is_finished() {
        let result = (&mut task).await;
        match result {
            Ok(Err(error)) => panic!("unsafe-state host failed: {error}"),
            Err(error) => panic!("unsafe-state host task failed: {error}"),
            Ok(Ok(_)) => panic!("unsafe-state host unexpectedly became ready"),
        }
    }
    let (_client, report) = status(controls[0], 1).await;
    assert_eq!(report.storage_state, wire::AgentStorageState::Unsafe as i32);
    assert!(!report.healthy);
    assert!(
        report
            .storage_error
            .contains("application state exists without matching Kuberic agent metadata")
    );
    shutdown.send_replace(true);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), &mut task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none()
    );
}

#[test]
fn public_hosts_initialize_report_and_restart_with_fresh_sessions() {
    run_host_test(public_hosts_initialize_report_and_restart_with_fresh_sessions_case());
}

#[test]
fn application_binding_without_agent_metadata_is_reported_unsafe() {
    run_host_test(application_binding_without_agent_metadata_is_reported_unsafe_case());
}

fn run_host_test(future: impl std::future::Future<Output = ()> + Send + 'static) {
    std::thread::Builder::new()
        .name("mssql-public-host-test".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(future);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn public_host_test_uses_no_kuberic_testing_authority_or_runtime_api() {
    let source = include_str!("runtime_process_host.rs");
    let forbidden = [
        ["kuberic_runtime::test", "ing"].concat(),
        ["Sqlite", "Store"].concat(),
        ["Runtime", "Adapter"].concat(),
        ["Runtime", "Effect"].concat(),
        ["Agent", "Service"].concat(),
    ];
    for forbidden in forbidden {
        assert!(!source.contains(&forbidden), "{forbidden}");
    }
}
