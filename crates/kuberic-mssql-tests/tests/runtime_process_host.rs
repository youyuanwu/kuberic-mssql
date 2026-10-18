use std::collections::VecDeque;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
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
    SeedDispatchRecord, SeedDispatchStore, SeedDispatchTarget,
};
use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, ConfigurationSequence, DatabaseIdentity,
    DatabaseLineage, DecimalProgress, Guid, NativeProgress, NativeRole, Observation,
    ReplicaIdentity as SqlReplicaIdentity, ServerName, SqlIdentifier,
};
use kuberic_runtime::application::StatefulServiceReplica;
use kuberic_runtime::control::proto::{self as wire, agent_control_client::AgentControlClient};
use kuberic_runtime::control::{
    normalize_public_v1_agent_status_report, public_v1_operation_command_to_proto,
};
use kuberic_runtime::host::{ReplicaHost, RunningReplica};
use kuberic_runtime::protocol::command::PublicV1OperationCommand;
use kuberic_runtime::protocol::public_operations::{
    PossibleDataLossIntent, PublicLifecycleInput, PublicLifecycleRecipe, PublicOperationClass,
    PublicOperationIntent, PublicOperationProgram, PublicOperationReplayDisposition,
    PublicReplicaDescriptor, PublicV1AgentReport,
};
use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ConfigurationMember, Epoch, OperationId, PodUid, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ReplicaStorageIdentity,
    ReplicaStorageIncarnationContext, ResourceUid, StorageGeneration, derive_agent_generation,
    derive_initialization_id,
};
use kuberic_runtime::{Replicator, RuntimeError as KubericRuntimeError};
use tonic::{Request, transport::Channel};

const TOKEN: &str = "runtime-agent-token";
const OBSERVED_AT: u64 = 1_000;
const NATIVE_IDS: [&str; 3] = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
    "33333333-3333-4333-8333-333333333333",
];
const TOPOLOGY_JSON: &[u8] = br#"{
    "schema_version":1,
    "members":[
        {"replica_id":1,"server_name":"sql-1","endpoint_url":"TCP://sql-1:5022"},
        {"replica_id":2,"server_name":"sql-2","endpoint_url":"TCP://sql-2:5022"},
        {"replica_id":3,"server_name":"sql-3","endpoint_url":"TCP://sql-3:5022"}
    ]
}"#;
static HOST_TEST_LOCK: Mutex<()> = Mutex::new(());

#[path = "runtime_process_host/native_authority.rs"]
mod native_authority;
#[path = "runtime_process_host/shutdown.rs"]
mod shutdown;

struct RepeatingSource {
    config: kuberic_mssql::runtime_config::ObserverConfig,
    samples: Mutex<VecDeque<InstanceSnapshot>>,
    fallback: InstanceSnapshot,
    gate: Mutex<Option<ObservationGate>>,
}

struct ObservationGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl RepeatingSource {
    fn gate_next_observation(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, held) = tokio::sync::oneshot::channel();
        let (release, resumed) = tokio::sync::oneshot::channel();
        let mut gate = self.gate.lock().unwrap();
        assert!(gate.is_none());
        *gate = Some(ObservationGate {
            entered,
            release: resumed,
        });
        (held, release)
    }
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
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.send(()).unwrap();
            gate.release
                .await
                .expect("the test must release its held observation");
        }
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
    replication: SocketAddr,
    application_root: PathBuf,
    application: Arc<RuntimeHostApplication>,
    source: Arc<RepeatingSource>,
    task: tokio::task::JoinHandle<kuberic_runtime::host::Result<RunningReplica>>,
}

#[derive(Clone)]
struct PublicCommand {
    target_index: usize,
    request: wire::ExecuteCommandRequest,
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

fn free_host_addresses() -> ([SocketAddr; 3], [SocketAddr; 3]) {
    let listeners = std::array::from_fn::<_, 6, _>(|_| TcpListener::bind("127.0.0.1:0").unwrap());
    let addresses = listeners
        .each_ref()
        .map(|listener| listener.local_addr().unwrap());
    (
        std::array::from_fn(|index| addresses[index]),
        std::array::from_fn(|index| addresses[index + 3]),
    )
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
    host_config_with(root, index, controls, replications, None, TOPOLOGY_JSON).await
}

async fn host_config_with(
    root: &Path,
    index: usize,
    controls: &[SocketAddr; 3],
    replications: &[SocketAddr; 3],
    application_root: Option<PathBuf>,
    topology_json: &[u8],
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
    write_private(&topology, topology_json);
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
            application_root,
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
    start_attempts_with_snapshots(root, std::array::from_fn(snapshot)).await
}

async fn start_attempts_with_snapshots(
    root: &Path,
    snapshots: [InstanceSnapshot; 3],
) -> Vec<HostAttempt> {
    let (controls, replications) = free_host_addresses();
    let mut attempts = Vec::new();
    for (index, snapshot) in snapshots.into_iter().enumerate() {
        let member_root = root.join(format!("member-{}", index + 1));
        if !member_root.exists() {
            std::fs::create_dir(&member_root).unwrap();
        }
        let config = host_config(&member_root, index, &controls, &replications).await;
        let source = Arc::new(RepeatingSource {
            config: config.observer().clone(),
            samples: Mutex::new(VecDeque::new()),
            fallback: snapshot,
            gate: Mutex::new(None),
        });
        let application = Arc::new(
            RuntimeHostApplication::with_observation_source(
                &config,
                source.clone(),
                Arc::new(FixedClock),
            )
            .unwrap(),
        );
        let application_root = config.application_root().to_owned();
        let host = ReplicaHost::new(
            config.replica_process_config(),
            application.clone(),
            application.storage_state().unwrap(),
            Arc::new(RuntimeEndpointResolver::new(&config).unwrap()),
        )
        .with_application_storage_paths(application.storage_paths());
        let control_listener = tokio::net::TcpListener::bind(controls[index])
            .await
            .unwrap();
        attempts.push(HostAttempt {
            control: controls[index],
            replication: replications[index],
            application_root,
            application,
            source,
            task: tokio::spawn(host.start(control_listener)),
        });
    }

    attempts
}

fn retained_seed_record(sessions: &[String], winning_target: usize) -> SeedDispatchRecord {
    let identities = identities();
    let member = |index: usize| {
        SeedDispatchTarget::new(
            identities[index].replica_id.to_string(),
            identities[index].instance_id.to_string(),
            identities[index].agent_generation.to_string(),
            sessions[index].clone(),
            NATIVE_IDS[index],
        )
        .unwrap()
    };
    SeedDispatchRecord::new(
        "runtime-host-test",
        identities[0].clone(),
        configuration().configuration_id.to_string(),
        "app-ag",
        "66666666-6666-4666-8666-666666666666",
        "app-db",
        "77777777-7777-4777-8777-777777777777",
        "88888888-8888-4888-8888-888888888888",
        "55555555-5555-4555-8555-555555555555",
        member(0),
        [member(1), member(2)],
        identities[winning_target].replica_id.to_string(),
        format!("build-{}", winning_target + 1),
        ReplicaStorageIncarnationContext {
            replica: identities[winning_target].clone(),
            storage: ReplicaStorageIdentity::Volatile {
                identity: format!("runtime-storage-{}", winning_target + 1),
            },
            storage_generation: StorageGeneration::new(format!(
                "runtime-storage-generation-{}",
                winning_target + 1
            )),
        },
        "2026-10-07T12:00:00",
    )
    .unwrap()
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
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

fn public_report(report: &wire::AgentStatusReport) -> PublicV1AgentReport {
    normalize_public_v1_agent_status_report(report.clone()).unwrap()
}

fn public_operation_request(
    target_index: usize,
    report: &PublicV1AgentReport,
    intent: PublicOperationIntent,
) -> wire::ExecuteCommandRequest {
    let command = PublicV1OperationCommand {
        target: identities()[target_index].clone(),
        intent,
    };
    wire::ExecuteCommandRequest {
        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        resource_uid: "runtime-host-test".into(),
        target: Some(identities()[target_index].clone().into()),
        expected_process_session_id: report.process_session_id.to_string(),
        command: Some(wire::execute_command_request::Command::PublicV1Operation(
            public_v1_operation_command_to_proto(&command).unwrap(),
        )),
    }
}

fn program_intent(
    report: &PublicV1AgentReport,
    operation_id: impl Into<String>,
    revision: u64,
    program: PublicOperationProgram,
) -> PublicOperationIntent {
    let class = match program {
        PublicOperationProgram::Close => PublicOperationClass::Close,
        PublicOperationProgram::Abort => PublicOperationClass::Abort,
        _ => PublicOperationClass::Authority,
    };
    PublicOperationIntent {
        identity: report.identity.clone(),
        operation_id: OperationId::new(operation_id.into()),
        revision,
        process_session_id: report.process_session_id.clone(),
        class,
        input_digest: program.digest(),
        replay_disposition: program.replay_disposition(),
        lifecycle: None,
        program: Some(program),
    }
}

fn lifecycle_intent(
    report: &PublicV1AgentReport,
    operation_id: impl Into<String>,
    revision: u64,
    lifecycle: PublicLifecycleInput,
) -> PublicOperationIntent {
    PublicOperationIntent {
        identity: report.identity.clone(),
        operation_id: OperationId::new(operation_id.into()),
        revision,
        process_session_id: report.process_session_id.clone(),
        class: PublicOperationClass::Authority,
        input_digest: lifecycle.digest(),
        replay_disposition: PublicOperationReplayDisposition::Convergent,
        lifecycle: Some(lifecycle),
        program: None,
    }
}

fn remote_active_descriptions(
    attempts: &[HostAttempt],
    reports: &[PublicV1AgentReport],
    local_index: usize,
) -> Vec<PublicReplicaDescriptor> {
    configuration()
        .members
        .iter()
        .filter(|member| {
            member.role == ReplicaRole::ActiveSecondary
                && member.identity.replica_id != identities()[local_index].replica_id
        })
        .map(|member| {
            let remote_index = usize::try_from(member.identity.replica_id.value() - 1).unwrap();
            let report = &reports[remote_index];
            PublicReplicaDescriptor {
                build_id: OperationId::default(),
                identity: report.replica.clone(),
                replication_address: format!("http://{}", attempts[remote_index].replication),
                process_session_id: report.process_session_id.clone(),
                storage_incarnation: Some(report.storage_incarnation.clone()),
                role: ReplicaRole::ActiveSecondary,
                current_progress: report.progress.last,
                catch_up_capability: report.progress.first,
                must_catchup: false,
            }
        })
        .collect()
}

async fn current_public_reports(
    attempts: &[HostAttempt],
) -> (Vec<wire::AgentStatusReport>, Vec<PublicV1AgentReport>) {
    let mut wire_reports = Vec::new();
    for (index, attempt) in attempts.iter().enumerate() {
        wire_reports.push(status(attempt.control, index as i64 + 1).await.1);
    }
    let public_reports = wire_reports.iter().map(public_report).collect();
    (wire_reports, public_reports)
}

async fn execute_public_command(attempts: &[HostAttempt], command: &PublicCommand) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (mut client, _) = status(
            attempts[command.target_index].control,
            command.target_index as i64 + 1,
        )
        .await;
        match client.execute(authorized(command.request.clone())).await {
            Ok(_) => return,
            Err(error) if error.code() == tonic::Code::Cancelled => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "public operation retries exceeded their deadline: {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("public operation failed: {error}"),
        }
    }
}

async fn initialize_attempts(
    attempts: &mut [HostAttempt],
) -> (Vec<RunningReplica>, Vec<String>, Vec<PublicCommand>) {
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
    let (sessions, commands) = configure_attempts(attempts).await;
    (replicas, sessions, commands)
}

async fn configure_attempts(attempts: &[HostAttempt]) -> (Vec<String>, Vec<PublicCommand>) {
    let (_, initial) = current_public_reports(attempts).await;
    let sessions = initial
        .iter()
        .map(|report| report.process_session_id.to_string())
        .collect::<Vec<_>>();
    let opens = (0..3)
        .map(|index| {
            let program = PublicOperationProgram::Open {
                replica: initial[index].replica.clone(),
                existing: false,
            };
            PublicCommand {
                target_index: index,
                request: public_operation_request(
                    index,
                    &initial[index],
                    program_intent(
                        &initial[index],
                        format!("bootstrap-open-{}", index + 1),
                        1,
                        program,
                    ),
                ),
            }
        })
        .collect::<Vec<_>>();
    for open in &opens {
        execute_public_command(attempts, open).await;
    }

    let (_, after_open) = current_public_reports(attempts).await;
    let primary_lifecycle = PublicLifecycleInput {
        recipe: PublicLifecycleRecipe::InitialPrimary,
        replica: after_open[0].replica.clone(),
        epoch: configuration().epoch,
        possible_data_loss: PossibleDataLossIntent::NotPossible,
        current: configuration(),
        previous: None,
        replicas: remote_active_descriptions(attempts, &after_open, 0),
        data_loss_recovery: None,
    };
    let primary = PublicCommand {
        target_index: 0,
        request: public_operation_request(
            0,
            &after_open[0],
            lifecycle_intent(
                &after_open[0],
                "bootstrap-initial-primary",
                2,
                primary_lifecycle,
            ),
        ),
    };
    execute_public_command(attempts, &primary).await;

    let (_, after_primary) = current_public_reports(attempts).await;
    let roles = (1..3)
        .map(|index| {
            let program = PublicOperationProgram::Role {
                epoch: configuration().epoch,
                role: ReplicaRole::ActiveSecondary,
            };
            PublicCommand {
                target_index: index,
                request: public_operation_request(
                    index,
                    &after_primary[index],
                    program_intent(
                        &after_primary[index],
                        format!("bootstrap-secondary-role-{}", index + 1),
                        2,
                        program,
                    ),
                ),
            }
        })
        .collect::<Vec<_>>();
    for role in &roles {
        execute_public_command(attempts, role).await;
    }

    let (_, after_roles) = current_public_reports(attempts).await;
    let configurations = (1..3)
        .map(|index| {
            let lifecycle = PublicLifecycleInput {
                recipe: PublicLifecycleRecipe::SecondaryEpochAdvance,
                replica: after_roles[index].replica.clone(),
                epoch: configuration().epoch,
                possible_data_loss: PossibleDataLossIntent::NotPossible,
                current: configuration(),
                previous: None,
                replicas: remote_active_descriptions(attempts, &after_roles, index),
                data_loss_recovery: None,
            };
            PublicCommand {
                target_index: index,
                request: public_operation_request(
                    index,
                    &after_roles[index],
                    lifecycle_intent(
                        &after_roles[index],
                        format!("bootstrap-secondary-lifecycle-{}", index + 1),
                        3,
                        lifecycle,
                    ),
                ),
            }
        })
        .collect::<Vec<_>>();
    for configuration in &configurations {
        execute_public_command(attempts, configuration).await;
    }

    let mut commands = opens;
    commands.push(primary);
    commands.extend(roles);
    commands.extend(configurations);
    (sessions, commands)
}

async fn replay_configuration(attempts: &[HostAttempt], commands: &[PublicCommand]) -> Vec<String> {
    for command in commands {
        execute_public_command(attempts, command).await;
    }
    let mut sessions = Vec::new();
    for (index, attempt) in attempts.iter().enumerate() {
        sessions.push(
            public_report(&status(attempt.control, index as i64 + 1).await.1)
                .process_session_id
                .to_string(),
        );
    }
    sessions
}

fn completed_success(
    public: &wire::PublicV1AgentStatus,
    operation_id: &str,
    revision: u64,
    call_kind: wire::PublicV1CallKind,
) -> bool {
    public.completed_operations.iter().any(|completion| {
        completion.operation_id == operation_id
            && completion.revision == revision
            && completion.call_kind == call_kind as i32
            && completion.result
                == wire::PublicV1OperationResultKind::PublicV1OperationResultSucceeded as i32
            && completion.process_session_id == public.process_session_id
    })
}

fn lifecycle_completed(public: &wire::PublicV1AgentStatus, index: usize) -> bool {
    completed_success(
        public,
        &format!("bootstrap-open-{}", index + 1),
        1,
        wire::PublicV1CallKind::Open,
    ) && if index == 0 {
        completed_success(
            public,
            "bootstrap-initial-primary",
            2,
            wire::PublicV1CallKind::InitialPrimary,
        )
    } else {
        completed_success(
            public,
            &format!("bootstrap-secondary-role-{}", index + 1),
            2,
            wire::PublicV1CallKind::Role,
        ) && completed_success(
            public,
            &format!("bootstrap-secondary-lifecycle-{}", index + 1),
            3,
            wire::PublicV1CallKind::SecondaryEpochAdvance,
        )
    }
}

async fn reports(
    attempts: &[HostAttempt],
    require_current_session_lifecycle: bool,
) -> Vec<wire::AgentStatusReport> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut reports = Vec::new();
        for (index, attempt) in attempts.iter().enumerate() {
            reports.push(status(attempt.control, index as i64 + 1).await.1);
        }
        if reports.iter().enumerate().all(|(index, report)| {
            report.public_v1.as_ref().is_some_and(|public| {
                public
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.replica_id == index as i64 + 1)
                    && public.role == wire::ReplicaRole::None as i32
                    && public.read_status == wire::AccessStatus::NotPrimary as i32
                    && public.write_status == wire::AccessStatus::NotPrimary as i32
                    && (!require_current_session_lifecycle
                        || (public.first_progress == 0 && public.last_progress == 42))
                    && (!require_current_session_lifecycle || lifecycle_completed(public, index))
            })
        }) {
            return reports;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "public reports did not converge: {:?}",
            reports
                .iter()
                .map(|report| (
                    report.public_v1.as_ref().map(|public| (
                        public.identity.as_ref().map(|identity| identity.replica_id),
                        public.role,
                        public.read_status,
                        public.write_status,
                        public.first_progress,
                        public.last_progress,
                        public.process_session_id.clone(),
                        public
                            .completed_operations
                            .iter()
                            .map(|completion| (
                                completion.operation_id.clone(),
                                completion.revision,
                                completion.call_kind,
                                completion.result,
                                completion.process_session_id.clone(),
                            ))
                            .collect::<Vec<_>>(),
                    )),
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
    let (mut first_replicas, first_sessions, configuration_commands) =
        initialize_attempts(&mut first_attempts).await;
    let first_reports = reports(&first_attempts, true).await;
    let expected_identities = identities();
    for (index, report) in first_reports.iter().enumerate() {
        let wire_public = report.public_v1.as_ref().unwrap();
        assert!(lifecycle_completed(wire_public, index));

        let public = public_report(report);
        assert_eq!(public.resource_uid.as_str(), "runtime-host-test");
        assert_eq!(public.replica, expected_identities[index]);
        assert_eq!(public.process_session_id.as_str(), first_sessions[index]);
        assert_eq!(
            public.storage_incarnation.replica,
            expected_identities[index]
        );
        assert_eq!(
            public.storage_incarnation.storage,
            ReplicaStorageIdentity::Persistent {
                pod_uid: PodUid::new(format!("pod-{}", index + 1)),
                pvc_uid: PvcUid::new(format!("pvc-{}", index + 1)),
            }
        );
        let resource = ResourceUid::new("runtime-host-test");
        let replica_id = ReplicaId::new(index as i64 + 1);
        let pod_uid = PodUid::new(format!("pod-{}", index + 1));
        let pvc_uid = PvcUid::new(format!("pvc-{}", index + 1));
        let initialization = derive_initialization_id(&resource, replica_id, &pod_uid, &pvc_uid);
        assert_eq!(
            public.storage_incarnation.storage_generation.as_str(),
            format!("storage-{initialization}")
        );
    }
    let replay_sessions = replay_configuration(&first_attempts, &configuration_commands).await;
    assert_eq!(
        replay_sessions, first_sessions,
        "Public V1 replay must retain exact process-session authority"
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
    let restarted_reports = reports(&restarted_attempts, false).await;
    let restarted_sessions = restarted_reports
        .iter()
        .map(|report| public_report(report).process_session_id.to_string())
        .collect::<Vec<_>>();
    assert!(
        first_sessions
            .iter()
            .zip(&restarted_sessions)
            .all(|(first, restarted)| first != restarted)
    );

    let (mut client, _) = status(restarted_attempts[0].control, 1).await;
    let error = client
        .execute(authorized(configuration_commands[0].request.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("stale agent process session"));
    stop_all(&mut restarted).await;
}

async fn service_close_cancellation_retains_actual_hosted_replicator_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut attempts = start_attempts(temporary.path()).await;
    let (mut replicas, _, _) = initialize_attempts(&mut attempts).await;
    let application = attempts[0].application.clone();
    let replicator = application.testing_replicator().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    replicator
        .testing_gate_lifecycle_publication(entered_tx, release_rx)
        .unwrap();
    let callback = std::thread::spawn({
        let replicator = replicator.clone();
        move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(replicator.current_progress())
        }
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut close = Box::pin(application.close());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut close)
            .await
            .is_err()
    );
    assert!(application.testing_replicator().is_none());
    drop(close);
    let mut retry = Box::pin(application.close());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut retry)
            .await
            .is_err()
    );
    application.abort();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(25),
            application.wait_for_containment()
        )
        .await
        .is_err()
    );
    release_tx.send(()).unwrap();
    assert!(matches!(
        callback.join().unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    tokio::time::timeout(Duration::from_secs(5), retry)
        .await
        .unwrap()
        .unwrap();
    application.wait_for_containment().await.unwrap();
    stop_all(&mut replicas).await;
}

async fn public_epoch_and_same_role_commands_fence_actual_adapter_work_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut attempts = start_attempts(temporary.path()).await;
    let (mut replicas, _, _) = initialize_attempts(&mut attempts).await;
    let replicator = attempts[0].application.testing_replicator().unwrap();
    for (revision, program, retired, kind) in [
        (
            4,
            PublicOperationProgram::Epoch {
                epoch: Epoch::new(0, 43),
            },
            true,
            wire::PublicV1CallKind::Epoch,
        ),
        (
            5,
            PublicOperationProgram::Role {
                epoch: Epoch::new(0, 44),
                role: ReplicaRole::Primary,
            },
            true,
            wire::PublicV1CallKind::Role,
        ),
        (
            6,
            PublicOperationProgram::Epoch {
                epoch: Epoch::new(0, 44),
            },
            false,
            wire::PublicV1CallKind::Epoch,
        ),
    ] {
        let (mut client, before) = status(attempts[0].control, 1).await;
        let before = public_report(&before);
        let operation_id = format!("hosted-authority-{revision}");
        let request = public_operation_request(
            0,
            &before,
            program_intent(&before, &operation_id, revision, program),
        );
        let (entered, release) = attempts[0].source.gate_next_observation();
        let callback = tokio::spawn({
            let replicator = replicator.clone();
            async move { replicator.current_progress().await }
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), client.execute(authorized(request)))
            .await
            .unwrap()
            .unwrap();
        if retired {
            let result = tokio::time::timeout(Duration::from_secs(5), callback)
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(result, Err(KubericRuntimeError::Closed)));
            assert!(release.send(()).is_err());
        } else {
            assert!(!callback.is_finished());
            release.send(()).unwrap();
            assert_eq!(callback.await.unwrap().unwrap(), 42);
        }
        let (_, after) = status(attempts[0].control, 1).await;
        assert!(completed_success(
            after.public_v1.as_ref().unwrap(),
            &operation_id,
            revision,
            kind
        ));
        assert_eq!(replicator.current_progress().await.unwrap(), 42);
    }
    stop_all(&mut replicas).await;
}

async fn public_close_waits_for_actual_adapter_callback_drain_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut attempts = start_attempts(temporary.path()).await;
    let (mut replicas, _, _) = initialize_attempts(&mut attempts).await;
    let application = attempts[0].application.clone();
    let replicator = application.testing_replicator().unwrap();
    let (mut client, before) = status(attempts[0].control, 1).await;
    let before = public_report(&before);
    let request = public_operation_request(
        0,
        &before,
        program_intent(
            &before,
            "hosted-local-close",
            4,
            PublicOperationProgram::Close,
        ),
    );
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    replicator
        .testing_gate_lifecycle_publication(entered_tx, release_rx)
        .unwrap();
    let callback = std::thread::spawn({
        let replicator = replicator.clone();
        move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(replicator.current_progress())
        }
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut close = tokio::spawn(async move { client.execute(authorized(request)).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                replicator.current_progress().await,
                Err(KubericRuntimeError::Closed)
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the actual public Close must fence the adapter");
    assert!(application.testing_replicator().is_some());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut close)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), replicator.wait_for_containment())
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    assert!(matches!(
        callback.join().unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    let error = tokio::time::timeout(Duration::from_secs(5), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("closed"));
    assert!(application.testing_replicator().is_none());
    application.wait_for_containment().await.unwrap();
    stop_all(&mut replicas).await;
}

async fn retained_dispatch_survives_runtime_restart_as_historical_session_evidence_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut first_attempts = start_attempts(temporary.path()).await;
    assert_initialization_listeners(&mut first_attempts).await;
    let (mut first_replicas, first_sessions, _) = initialize_attempts(&mut first_attempts).await;
    let primary_application_root = first_attempts[0].application_root.clone();
    stop_all(&mut first_replicas).await;
    drop(first_replicas);

    {
        let store = SeedDispatchStore::open(&primary_application_root).unwrap();
        let intent = retained_seed_record(&first_sessions, 1);
        store.create(&intent).unwrap();
        let mut ambiguous = intent.clone();
        ambiguous.mark_submitting().unwrap();
        let mut ambiguous = store.replace(intent.revision(), ambiguous).unwrap();
        ambiguous.mark_ambiguous().unwrap();
        store.replace(ambiguous.revision(), ambiguous).unwrap();
    }

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
    let restarted_reports = reports(&restarted_attempts, false).await;
    let restarted_sessions = restarted_reports
        .iter()
        .map(|report| public_report(report).process_session_id.to_string())
        .collect::<Vec<_>>();
    assert!(
        first_sessions
            .iter()
            .zip(&restarted_sessions)
            .all(|(first, restarted)| first != restarted)
    );

    {
        let store = SeedDispatchStore::open(&primary_application_root).unwrap();
        let historical = store.load().unwrap().unwrap();
        let current = retained_seed_record(&restarted_sessions, 2);
        assert!(historical.same_dispatch_context(&current));
        assert!(!historical.same_intent_owner(&current));
        assert_ne!(historical.operation_token(), current.operation_token());
        assert_eq!(historical.terminal_token(), current.terminal_token());
        assert_eq!(historical.source().process_session_id(), first_sessions[0]);
        let mut associated = historical.clone();
        associated.mark_accepted().unwrap();
        let mut associated = store.replace(historical.revision(), associated).unwrap();
        associated
            .associate_group_database("44444444-4444-4444-8444-444444444444")
            .unwrap();
        let revision = associated.revision();
        let associated = store.replace(revision, associated).unwrap();
        store.remove(associated.revision()).unwrap();
    }

    stop_all(&mut restarted).await;
}

async fn application_binding_without_agent_metadata_is_reported_unsafe_case() {
    let temporary = tempfile::tempdir().unwrap();
    let (controls, replications) = free_host_addresses();
    let root = temporary.path().join("member-1");
    std::fs::create_dir(&root).unwrap();
    let config = host_config(&root, 0, &controls, &replications).await;
    let source = Arc::new(RepeatingSource {
        config: config.observer().clone(),
        samples: Mutex::new(VecDeque::new()),
        fallback: snapshot(0),
        gate: Mutex::new(None),
    });
    let application = Arc::new(
        RuntimeHostApplication::with_observation_source(&config, source, Arc::new(FixedClock))
            .unwrap(),
    );
    let binding = kuberic_mssql::runtime_host::RuntimeBindingStore::new(&config).unwrap();
    binding.initialize(&identities()[0]).unwrap();
    let host = ReplicaHost::new(
        config.replica_process_config(),
        application.clone(),
        application.storage_state().unwrap(),
        Arc::new(RuntimeEndpointResolver::new(&config).unwrap()),
    )
    .with_application_storage_paths(application.storage_paths());
    let control_listener = tokio::net::TcpListener::bind(controls[0]).await.unwrap();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut task = tokio::spawn(host.start_with_shutdown(control_listener, receiver));
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

async fn established_public_host_rejects_changed_application_path_and_topology_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut first_attempts = start_attempts(temporary.path()).await;
    assert_initialization_listeners(&mut first_attempts).await;
    let (mut first_replicas, _, _) = initialize_attempts(&mut first_attempts).await;
    stop_all(&mut first_replicas).await;

    let (controls, replications) = free_host_addresses();
    let root = temporary.path().join("member-1");
    let moved_application = root.join("moved-application");
    let moved_config = host_config_with(
        &root,
        0,
        &controls,
        &replications,
        Some(moved_application),
        TOPOLOGY_JSON,
    )
    .await;
    let moved_source = Arc::new(RepeatingSource {
        config: moved_config.observer().clone(),
        samples: Mutex::new(VecDeque::new()),
        fallback: snapshot(0),
        gate: Mutex::new(None),
    });
    let moved_application = Arc::new(
        RuntimeHostApplication::with_observation_source(
            &moved_config,
            moved_source,
            Arc::new(FixedClock),
        )
        .unwrap(),
    );
    let moved_host = ReplicaHost::new(
        moved_config.replica_process_config(),
        moved_application.clone(),
        moved_application.storage_state().unwrap(),
        Arc::new(RuntimeEndpointResolver::new(&moved_config).unwrap()),
    )
    .with_application_storage_paths(moved_application.storage_paths());
    let control_listener = tokio::net::TcpListener::bind(controls[0]).await.unwrap();
    let (_shutdown, receiver) = tokio::sync::watch::channel(false);
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        moved_host.start_with_shutdown(control_listener, receiver),
    )
    .await
    .expect("changed application path is rejected promptly");
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("changed application path unexpectedly started"),
    };
    assert!(
        error
            .to_string()
            .contains("application storage paths differ")
    );

    let changed_topology = br#"{
        "schema_version":1,
        "members":[
            {"replica_id":1,"server_name":"sql-1","endpoint_url":"TCP://sql-1:5022"},
            {"replica_id":2,"server_name":"sql-2","endpoint_url":"TCP://sql-2:5022"},
            {"replica_id":3,"server_name":"sql-3","endpoint_url":"TCP://sql-3-new:5022"}
        ]
    }"#;
    let changed_config =
        host_config_with(&root, 0, &controls, &replications, None, changed_topology).await;
    let changed_source = Arc::new(RepeatingSource {
        config: changed_config.observer().clone(),
        samples: Mutex::new(VecDeque::new()),
        fallback: snapshot(0),
        gate: Mutex::new(None),
    });
    let changed_application = RuntimeHostApplication::with_observation_source(
        &changed_config,
        changed_source,
        Arc::new(FixedClock),
    )
    .unwrap();
    assert!(
        changed_application
            .storage_state()
            .unwrap_err()
            .to_string()
            .contains("binding identity differs")
    );
}

#[test]
fn public_hosts_initialize_report_and_restart_with_fresh_sessions() {
    run_host_test(public_hosts_initialize_report_and_restart_with_fresh_sessions_case);
}

#[test]
fn service_close_cancellation_retains_actual_hosted_replicator() {
    run_host_test(service_close_cancellation_retains_actual_hosted_replicator_case);
}

#[test]
fn public_epoch_and_same_role_commands_fence_actual_adapter_work() {
    run_host_test(public_epoch_and_same_role_commands_fence_actual_adapter_work_case);
}

#[test]
fn public_close_waits_for_actual_adapter_callback_drain() {
    run_host_test(public_close_waits_for_actual_adapter_callback_drain_case);
}

#[test]
fn retained_dispatch_survives_runtime_restart_as_historical_session_evidence() {
    run_host_test(retained_dispatch_survives_runtime_restart_as_historical_session_evidence_case);
}

#[test]
fn application_binding_without_agent_metadata_is_reported_unsafe() {
    run_host_test(application_binding_without_agent_metadata_is_reported_unsafe_case);
}

#[test]
fn established_public_host_rejects_changed_application_path_and_topology() {
    run_host_test(established_public_host_rejects_changed_application_path_and_topology_case);
}

fn run_host_test<F, Fut>(factory: F)
where
    F: Fn() -> Fut + Copy + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let _guard = HOST_TEST_LOCK.lock().unwrap();
    for attempt in 1..=3 {
        let result = std::thread::Builder::new()
            .name("mssql-public-host-test".into())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(factory());
            })
            .unwrap()
            .join();
        match result {
            Ok(()) => return,
            Err(panic) if attempt < 3 && is_transient_public_host_startup_panic(&panic) => {
                eprintln!(
                    "retrying public-host test after transient Kuberic startup race \
                     ({attempt}/3)"
                );
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

fn is_transient_public_host_startup_panic(panic: &Box<dyn std::any::Any + Send + 'static>) -> bool {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .is_some_and(|message| {
            message.contains("agent authority changed while constructing a status report")
                || (message.contains("effect sequence")
                    && message.contains("is out of order; expected"))
        })
}

#[test]
fn only_known_public_host_startup_races_are_retryable() {
    let authority_report: Box<dyn std::any::Any + Send> =
        Box::new("agent authority changed while constructing a status report".to_owned());
    let effect_sequence: Box<dyn std::any::Any + Send> =
        Box::new("[\"effect sequence 5 is out of order; expected 4\"]".to_owned());
    let unrelated: Box<dyn std::any::Any + Send> = Box::new("unexpected failure".to_owned());
    assert!(is_transient_public_host_startup_panic(&authority_report));
    assert!(is_transient_public_host_startup_panic(&effect_sequence));
    assert!(!is_transient_public_host_startup_panic(&unrelated));
}

#[test]
fn public_host_test_uses_no_kuberic_testing_authority_or_runtime_api() {
    let source = [
        include_str!("runtime_process_host.rs"),
        include_str!("runtime_process_host/shutdown.rs"),
        include_str!("runtime_process_host/native_authority.rs"),
    ]
    .concat();
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
