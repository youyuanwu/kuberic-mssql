use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_mssql::kuberic::{
    ObservationClock, SqlServerObservationSource, SqlServerReplicator, SqlServerReplicatorFactory,
    SqlServerService, SqlServerServiceConfig,
};
use kuberic_mssql::observation::{
    AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, DatabaseSnapshot, InstanceMetadata,
    InstanceSnapshot, LocalDatabaseSnapshot, LocalRecoveryMetadata, LocalReplicaSnapshot,
    NativeProvenance, RecoveryLineageObservation, ReplicaSnapshot, ReplicaState,
};
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::runtime_error::RuntimeError;
use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, ConfigurationSequence, DatabaseIdentity,
    DatabaseLineage, DecimalProgress, Guid, NativeProgress, NativeRole, Observation,
    ObservationFailure, ObservationFailureKind, ReplicaIdentity, ServerName, SqlIdentifier,
    SqlServerTopologyExpectation, SqlServerTopologyMemberExpectation,
};
use kuberic_runtime::RuntimeError as KubericRuntimeError;
use kuberic_runtime::application::{OpenMode, StatefulServiceReplica};
use kuberic_runtime::protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationId, ConfigurationMember,
    EffectivePolicy, Epoch, InitializationId, OperationId, PartitionId, PodUid, ProcessSessionId,
    PvcUid, ReplicaId, ReplicaIdentity as KubericReplicaIdentity, ReplicaInstanceId, ReplicaRole,
    ResourceUid,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, ReplicaSetQuorumMode,
    Replicator,
};
use kuberic_runtime::testing::authority::AdmittedAuthority;
use kuberic_runtime::testing::describe_peer;
use kuberic_runtime::testing::effects::{RuntimeEffect, RuntimeEffectAction};
use kuberic_runtime::testing::hosting::PodRuntime;
use kuberic_runtime::testing::runtime_adapter::RuntimeAdapter;
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use tokio::sync::oneshot;

const OBSERVED_AT: u64 = 1_000;
const MAX_AGE: u64 = 100;
const AG_ID: &str = "11111111-1111-4111-8111-111111111111";
const LOCAL_ID: &str = "22222222-2222-4222-8222-222222222222";
const SECOND_ID: &str = "33333333-3333-4333-8333-333333333333";
const THIRD_ID: &str = "44444444-4444-4444-8444-444444444444";
const DATABASE_ID: &str = "55555555-5555-4555-8555-555555555555";
const DATABASE_GUID: &str = "66666666-6666-4666-8666-666666666666";
const FAMILY_GUID: &str = "77777777-7777-4777-8777-777777777777";
const FORK_ID: &str = "88888888-8888-4888-8888-888888888888";

fn run_runtime_effect_test<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    std::thread::Builder::new()
        .name("kuberic-runtime-effect-test".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(future);
        })
        .unwrap()
        .join()
        .unwrap();
}

struct ScriptedSource {
    config: ObserverConfig,
    samples: Mutex<VecDeque<Result<Observation<InstanceSnapshot>, RuntimeError>>>,
}

#[async_trait]
impl SqlServerObservationSource for ScriptedSource {
    fn observer_config(&self) -> &ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        self.samples
            .lock()
            .unwrap()
            .pop_front()
            .expect("test must provide one fresh observation per request")
    }
}

struct CountingSource {
    inner: ScriptedSource,
    observations: AtomicUsize,
}

#[async_trait]
impl SqlServerObservationSource for CountingSource {
    fn observer_config(&self) -> &ObserverConfig {
        self.inner.observer_config()
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        self.observations.fetch_add(1, Ordering::SeqCst);
        self.inner.observe().await
    }
}

struct GatedSource {
    config: ObserverConfig,
    samples: Mutex<VecDeque<Result<Observation<InstanceSnapshot>, RuntimeError>>>,
    observations: AtomicUsize,
    gate_at: usize,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait]
impl SqlServerObservationSource for GatedSource {
    fn observer_config(&self) -> &ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        let observation = self.observations.fetch_add(1, Ordering::SeqCst);
        let sample = self
            .samples
            .lock()
            .unwrap()
            .pop_front()
            .expect("test must provide one observation per request");
        if observation == self.gate_at {
            self.entered
                .lock()
                .unwrap()
                .take()
                .expect("gate must be entered once")
                .send(())
                .expect("test must wait for the observation gate");
            let release = self
                .release
                .lock()
                .unwrap()
                .take()
                .expect("gate must be released once");
            release
                .await
                .expect("test must release the observation gate");
        }
        sample
    }
}

struct ScriptedClock {
    times: Mutex<VecDeque<u64>>,
}

impl ObservationClock for ScriptedClock {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
        let mut times = self.times.lock().unwrap();
        if times.len() > 1 {
            Ok(times.pop_front().unwrap())
        } else {
            Ok(*times
                .front()
                .expect("test must provide a request-time clock value"))
        }
    }
}

fn observer_config() -> ObserverConfig {
    observer_config_for(0)
}

fn observer_config_for(index: usize) -> ObserverConfig {
    ObserverConfig::from_json(
        format!(
            r#"{{
                "host":"sql.example",
                "port":1433,
                "availability_group":"test-ag",
                "expected_server_name":"sql-{index}",
                "replica_id":"logical-{index}",
                "incarnation":"pod-{index}",
                "observer_username_file":"/secrets/username",
                "observer_password_file":"/secrets/password",
                "sample_timeout_ms":1000,
                "connect_timeout_ms":1000,
                "query_timeout_ms":1000,
                "poll_interval_ms":1000,
                "max_age_ms":{MAX_AGE}
            }}"#
        )
        .as_bytes(),
    )
    .unwrap()
}

fn guid(value: &str) -> Guid {
    Guid::parse("test GUID", value).unwrap()
}

fn replica(id: &str, server: &str) -> ReplicaSnapshot {
    ReplicaSnapshot {
        replica_id: guid(id),
        server_name: ServerName::new(server).unwrap(),
        endpoint_url: Some(format!("TCP://{server}:5022")),
        availability_mode: "SYNCHRONOUS_COMMIT".into(),
        failover_mode: "EXTERNAL".into(),
        seeding_mode: "AUTOMATIC".into(),
        state: None,
    }
}

fn snapshot(role: NativeRole) -> InstanceSnapshot {
    InstanceSnapshot {
        observed_at_unix_millis: OBSERVED_AT,
        instance: InstanceMetadata {
            server_name: ServerName::new("sql-0").unwrap(),
            property_server_name: ServerName::new("SQL-0").unwrap(),
            product_version: "17.0.5005.3".into(),
            product_major_version: 17,
            edition: "Enterprise Developer Edition (64-bit)".into(),
            engine_edition: 3,
            hadr_enabled: true,
            host_platform: "Linux".into(),
            host_distribution: Some("Ubuntu".into()),
            architecture: "x86_64".into(),
            sqlserver_start_time: "2026-10-06T12:00:00".into(),
        },
        availability_group: Observation::Present {
            value: AvailabilityGroupSnapshot {
                identity: AvailabilityGroupIdentity {
                    name: AvailabilityGroupName::new("test-ag").unwrap(),
                    group_id: guid(AG_ID),
                },
                configuration_sequence: ConfigurationSequence::parse("42").unwrap(),
                cluster_type: "EXTERNAL".into(),
                required_synchronized_secondaries_to_commit: 1,
                basic_features: false,
                is_distributed: false,
                local_replica: LocalReplicaSnapshot {
                    identity: ReplicaIdentity::observed("logical-0", guid(LOCAL_ID), "pod-0")
                        .unwrap(),
                    state_available: true,
                    role: Some(role),
                },
                replicas: vec![
                    replica(LOCAL_ID, "sql-0"),
                    replica(SECOND_ID, "sql-1"),
                    replica(THIRD_ID, "sql-2"),
                ],
                databases: Vec::new(),
                automatic_seeding: Vec::new(),
                physical_seeding: Vec::new(),
            },
            observed_at_unix_millis: OBSERVED_AT,
        },
    }
}

fn present(snapshot: InstanceSnapshot) -> Observation<InstanceSnapshot> {
    Observation::Present {
        value: snapshot,
        observed_at_unix_millis: OBSERVED_AT,
    }
}

fn database(id: &str, name: &str) -> DatabaseSnapshot {
    DatabaseSnapshot {
        identity: DatabaseIdentity {
            name: SqlIdentifier::new(name).unwrap(),
            group_database_id: guid(id),
        },
        local: None,
        replicas: Vec::new(),
    }
}

fn kuberic_identity(id: i64) -> KubericReplicaIdentity {
    KubericReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(format!("instance-{id}")),
        agent_generation: AgentGeneration::new(format!("generation-{id}")),
    }
}

fn sql_identity(index: usize) -> ReplicaIdentity {
    let native = [LOCAL_ID, SECOND_ID, THIRD_ID][index];
    ReplicaIdentity::observed(
        format!("logical-{index}"),
        guid(native),
        format!("pod-{index}"),
    )
    .unwrap()
}

fn configuration() -> ConfigurationDescriptor {
    ConfigurationDescriptor::new(
        Epoch::new(7, 42),
        ReplicaId::new(1),
        vec![
            ConfigurationMember {
                identity: kuberic_identity(1),
                role: ReplicaRole::Primary,
            },
            ConfigurationMember {
                identity: kuberic_identity(2),
                role: ReplicaRole::ActiveSecondary,
            },
            ConfigurationMember {
                identity: kuberic_identity(3),
                role: ReplicaRole::ActiveSecondary,
            },
        ],
        2,
    )
}

fn expected_lineage() -> DatabaseLineage {
    DatabaseLineage {
        database: DatabaseIdentity {
            name: SqlIdentifier::new("app-db").unwrap(),
            group_database_id: guid(DATABASE_ID),
        },
        recovery_fork_id: guid(FORK_ID),
    }
}

fn topology_expectation() -> SqlServerTopologyExpectation {
    topology_expectation_for(0)
}

fn topology_expectation_for(local_index: usize) -> SqlServerTopologyExpectation {
    SqlServerTopologyExpectation::new(
        ReplicaId::new(local_index as i64 + 1),
        format!("logical-{local_index}"),
        (0..3)
            .map(|index| {
                SqlServerTopologyMemberExpectation::new(
                    ReplicaId::new(index as i64 + 1),
                    ServerName::new(format!("sql-{index}")).unwrap(),
                    format!("TCP://sql-{index}:5022"),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn bound_snapshot() -> InstanceSnapshot {
    bound_snapshot_for(0)
}

fn bound_snapshot_for(local_index: usize) -> InstanceSnapshot {
    let native_role = if local_index == 0 {
        NativeRole::Primary
    } else {
        NativeRole::Secondary
    };
    let mut sample = snapshot(native_role.clone());
    sample.instance.server_name = ServerName::new(format!("sql-{local_index}")).unwrap();
    sample.instance.property_server_name = ServerName::new(format!("sql-{local_index}")).unwrap();
    sample.instance.sqlserver_start_time = format!("2026-10-06T12:00:0{local_index}");
    let group = match &mut sample.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    group.local_replica.identity = sql_identity(local_index);
    group.replicas[local_index].state = Some(ReplicaState {
        provenance: NativeProvenance::Local,
        role: Some(native_role),
        operational_state: Some("ONLINE".into()),
        connected_state: Some("CONNECTED".into()),
        recovery_health: Some("ONLINE".into()),
        synchronization_health: Some("HEALTHY".into()),
        last_connect_error_number: Some(0),
    });
    let lineage = expected_lineage();
    group.databases = vec![DatabaseSnapshot {
        identity: lineage.database.clone(),
        local: Some(LocalDatabaseSnapshot {
            database_id: 5,
            replica_id: guid([LOCAL_ID, SECOND_ID, THIRD_ID][local_index]),
            state: Some("ONLINE".into()),
            recovery_model: Some("FULL".into()),
            recovery: Some(LocalRecoveryMetadata {
                database_guid: Some(guid(DATABASE_GUID)),
                family_guid: Some(guid(FAMILY_GUID)),
                recovery_fork_guid: Some(guid(FORK_ID)),
                first_recovery_fork_guid: Some(guid(FORK_ID)),
                fork_point_lsn: None,
            }),
        }),
        replicas: vec![DatabaseReplicaSnapshot {
            group_database_id: guid(DATABASE_ID),
            replica_id: guid([LOCAL_ID, SECOND_ID, THIRD_ID][local_index]),
            database_id: 5,
            provenance: NativeProvenance::Local,
            lineage: RecoveryLineageObservation::Local { value: lineage },
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
    }];
    sample
}

fn bound_replica_set() -> ReplicaSetConfiguration {
    replica_set_with_session_prefix("session")
}

fn replica_set_with_session_prefix(prefix: &str) -> ReplicaSetConfiguration {
    let configuration = configuration();
    ReplicaSetConfiguration {
        replicas: configuration
            .members
            .iter()
            .map(|member| {
                let mut replica = ReplicaInformation::new(
                    OperationId::new(format!("current-{}", member.identity.replica_id)),
                    member.identity.clone(),
                    format!(
                        "replica-{}.example:5022",
                        member.identity.replica_id.value()
                    ),
                );
                replica.process_session_id = ProcessSessionId::new(format!(
                    "{prefix}-{}",
                    member.identity.replica_id.value()
                ));
                replica.role = member.role;
                replica.current_progress = 42;
                replica.catch_up_capability = 42;
                replica
            })
            .collect(),
        configuration,
    }
}

fn admitted_authority(local_index: usize) -> AdmittedAuthority {
    AdmittedAuthority {
        local_identity: kuberic_identity(local_index as i64 + 1),
        transition_kind: None,
        previous_configuration: None,
        current_configuration: configuration(),
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: None,
    }
}

fn runtime_effect(operation_id: &str, sequence: u64, action: RuntimeEffectAction) -> RuntimeEffect {
    RuntimeEffect {
        operation_id: OperationId::new(operation_id),
        sequence,
        action,
    }
}

async fn open_effect_runtime(
    local_index: usize,
    expectation: SqlServerTopologyExpectation,
    effective_policy: EffectivePolicy,
    source: Arc<dyn SqlServerObservationSource>,
    clock: Arc<dyn ObservationClock>,
) -> (
    tempfile::TempDir,
    Arc<SqliteStore>,
    Arc<SqlServerService>,
    Arc<PodRuntime>,
    Arc<RuntimeAdapter>,
) {
    let directory = tempfile::tempdir().expect("create isolated Kuberic testing directory");
    let database = SqliteStore::metadata_database_path(directory.path());
    let state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: ResourceUid::new("resource-a"),
        pod_uid: PodUid::new("contract-pod"),
        pvc_uid: PvcUid::new("contract-pvc"),
        initialization_id: InitializationId::new("contract-initialization"),
        local_identity: kuberic_identity(local_index as i64 + 1),
        effective_policy,
    });
    drop(SqliteStore::create_authorized(&database, state).unwrap());
    let store = Arc::new(SqliteStore::open_existing(&database, None).unwrap());
    let service = Arc::new(
        SqlServerService::with_observation_source_and_topology(
            SqlServerServiceConfig::new(
                ResourceUid::new("resource-a"),
                format!("replica-{}.example:5022", local_index + 1),
            )
            .unwrap(),
            source,
            clock,
            expectation,
        )
        .unwrap(),
    );
    let runtime = Arc::new(PodRuntime::new(
        kuberic_identity(local_index as i64 + 1),
        service.clone(),
        store.clone(),
    ));
    runtime
        .bind_replica_session(
            ResourceUid::new("resource-a"),
            ProcessSessionId::new(format!("session-{}", local_index + 1)),
        )
        .unwrap();
    let adapter = Arc::new(RuntimeAdapter::new(store.clone(), runtime.clone()));
    adapter
        .execute(runtime_effect(
            "open",
            1,
            RuntimeEffectAction::Open(OpenMode::Existing),
        ))
        .await
        .unwrap();
    (directory, store, service, runtime, adapter)
}

fn replicator_with(
    samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    times: Vec<u64>,
) -> Arc<SqlServerReplicator> {
    Arc::new(SqlServerReplicator::new(
        "sql-replication.example:5022".into(),
        Arc::new(ScriptedSource {
            config: observer_config(),
            samples: Mutex::new(samples.into()),
        }),
        Arc::new(ScriptedClock {
            times: Mutex::new(times.into()),
        }),
    ))
}

fn bound_replicator_with(
    samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    times: Vec<u64>,
) -> Arc<SqlServerReplicator> {
    bound_replicator_for(0, samples, times)
}

fn bound_replicator_for(
    local_index: usize,
    samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    times: Vec<u64>,
) -> Arc<SqlServerReplicator> {
    let expectation = topology_expectation_for(local_index);
    Arc::new(
        SqlServerReplicator::new_with_topology(
            format!("replica-{}.example:5022", local_index + 1),
            Arc::new(ScriptedSource {
                config: observer_config_for(local_index),
                samples: Mutex::new(samples.into()),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(times.into()),
            }),
            kuberic_identity(local_index as i64 + 1),
            expectation,
        )
        .unwrap(),
    )
}

fn gated_bound_replicator(
    samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    times: Vec<u64>,
    gate_at: usize,
) -> (
    Arc<SqlServerReplicator>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let expectation = topology_expectation();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let replicator = Arc::new(
        SqlServerReplicator::new_with_topology(
            "replica-1.example:5022".into(),
            Arc::new(GatedSource {
                config: observer_config(),
                samples: Mutex::new(samples.into()),
                observations: AtomicUsize::new(0),
                gate_at,
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(Some(release_rx)),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(times.into()),
            }),
            kuberic_identity(1),
            expectation,
        )
        .unwrap(),
    );
    (replicator, entered_rx, release_tx)
}

async fn opened_bound_replicator(
    samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    times: Vec<u64>,
) -> Arc<SqlServerReplicator> {
    let replicator = bound_replicator_with(samples, times);
    assert_eq!(replicator.open().await.unwrap(), "replica-1.example:5022");
    replicator
}

async fn opened_admitted_replicator(
    local_index: usize,
    mut samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    times: Vec<u64>,
) -> Arc<SqlServerReplicator> {
    samples.insert(0, Ok(present(bound_snapshot_for(local_index))));
    let replicator = bound_replicator_for(local_index, samples, times);
    assert_eq!(
        replicator.open().await.unwrap(),
        format!("replica-{}.example:5022", local_index + 1)
    );
    replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    replicator
}

fn testing_runtime(
    service: Arc<SqlServerService>,
    resource: ResourceUid,
) -> (tempfile::TempDir, PodRuntime) {
    let identity = KubericReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("contract-instance"),
        agent_generation: AgentGeneration::new("contract-generation"),
    };
    testing_runtime_with_identity(
        service,
        resource,
        identity,
        EffectivePolicy::fixed(1, 0).expect("valid singleton policy"),
    )
}

fn testing_runtime_with_identity(
    service: Arc<SqlServerService>,
    resource: ResourceUid,
    identity: KubericReplicaIdentity,
    effective_policy: EffectivePolicy,
) -> (tempfile::TempDir, PodRuntime) {
    let directory = tempfile::tempdir().expect("create isolated Kuberic testing directory");
    let state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: resource,
        pod_uid: PodUid::new("contract-pod"),
        pvc_uid: PvcUid::new("contract-pvc"),
        initialization_id: InitializationId::new("contract-initialization"),
        local_identity: identity.clone(),
        effective_policy,
    });
    let database = SqliteStore::metadata_database_path(directory.path());
    drop(SqliteStore::create_authorized(&database, state).expect("create testing store"));
    let store = Arc::new(
        SqliteStore::open_existing(&database, None).expect("reopen Kuberic testing store"),
    );
    (directory, PodRuntime::new(identity, service, store))
}

async fn opened_replicator(
    samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    times: Vec<u64>,
) -> Arc<SqlServerReplicator> {
    let replicator = replicator_with(samples, times);
    assert_eq!(
        replicator.open().await.unwrap(),
        "sql-replication.example:5022"
    );
    replicator
}

fn application_error(error: KubericRuntimeError) -> String {
    match error {
        KubericRuntimeError::Application(message) => message,
        other => panic!("expected an application error, got {other}"),
    }
}

#[tokio::test]
async fn publishes_only_the_present_configuration_sequence() {
    for (sequence, expected) in [("0", 0), ("42", 42), ("9223372036854775807", i64::MAX)] {
        let mut sample = snapshot(NativeRole::Primary);
        if let Observation::Present { value, .. } = &mut sample.availability_group {
            value.configuration_sequence = ConfigurationSequence::parse(sequence).unwrap();
        }
        let replicator = opened_replicator(vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
        assert_eq!(replicator.current_progress().await.unwrap(), expected);
    }

    let replicator = opened_replicator(Vec::new(), Vec::new()).await;
    let interfaces = SqlServerReplicatorFactory::new(replicator).interfaces();
    assert!(interfaces.state_replicator().is_none());
    assert!(interfaces.primary_replicator().is_some());
}

#[tokio::test]
async fn native_progress_above_signed_range_never_becomes_generic_progress() {
    let mut sample = snapshot(NativeRole::Primary);
    let group = match &mut sample.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    group.databases.push(DatabaseSnapshot {
        identity: DatabaseIdentity {
            name: SqlIdentifier::new("db").unwrap(),
            group_database_id: guid("55555555-5555-4555-8555-555555555555"),
        },
        local: None,
        replicas: vec![DatabaseReplicaSnapshot {
            group_database_id: guid("55555555-5555-4555-8555-555555555555"),
            replica_id: guid(LOCAL_ID),
            database_id: 5,
            provenance: NativeProvenance::Local,
            lineage: RecoveryLineageObservation::RemoteUnavailable,
            is_primary_replica: Some(true),
            synchronization_state: Some("SYNCHRONIZED".into()),
            synchronization_health: Some("HEALTHY".into()),
            database_state: Some("ONLINE".into()),
            is_suspended: Some(false),
            suspend_reason: None,
            is_commit_participant: Some(true),
            progress: NativeProgress {
                hardened_block: Some(DecimalProgress::parse("9223372036854775808").unwrap()),
                redone_record: Some(DecimalProgress::parse("9999999999999999999999999").unwrap()),
                committed_record: None,
            },
        }],
    });
    let replicator = opened_replicator(vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
    assert_eq!(replicator.current_progress().await.unwrap(), 42);
}

#[tokio::test]
async fn absent_failed_and_transport_error_observations_fail_explicitly() {
    let mut absent_group = snapshot(NativeRole::Primary);
    absent_group.availability_group = Observation::Absent {
        observed_at_unix_millis: OBSERVED_AT,
    };
    let mut failed_group = snapshot(NativeRole::Primary);
    failed_group.availability_group = Observation::Failed(ObservationFailure {
        kind: ObservationFailureKind::PermissionDenied,
        message: "redacted".into(),
        observed_at_unix_millis: OBSERVED_AT,
    });
    let failures = [
        (Ok(present(absent_group)), "availability group is absent"),
        (Ok(present(failed_group)), "PermissionDenied"),
        (
            Err(RuntimeError::new(
                ObservationFailureKind::Tls,
                "transport",
                "TLS validation failed",
            )),
            "Tls",
        ),
        (
            Err(RuntimeError::new(
                ObservationFailureKind::Unreachable,
                "transport",
                "connection failed",
            )),
            "Unreachable",
        ),
    ];
    for (sample, expected) in failures {
        let replicator = opened_replicator(vec![sample], vec![OBSERVED_AT]).await;
        let message = application_error(replicator.current_progress().await.unwrap_err());
        assert!(message.contains(expected), "{message}");
    }
}

#[tokio::test]
async fn freshness_uses_request_time_and_accepts_the_exact_age_boundary() {
    for (now, expected) in [
        (OBSERVED_AT + MAX_AGE, Ok(42)),
        (OBSERVED_AT + MAX_AGE + 1, Err("stale")),
        (OBSERVED_AT - 1, Err("future-dated")),
    ] {
        let replicator =
            opened_replicator(vec![Ok(present(snapshot(NativeRole::Primary)))], vec![now]).await;
        match expected {
            Ok(progress) => assert_eq!(replicator.current_progress().await.unwrap(), progress),
            Err(message) => assert!(
                application_error(replicator.current_progress().await.unwrap_err())
                    .contains(message)
            ),
        }
    }
}

#[tokio::test]
async fn progress_role_and_epoch_recheck_freshness_immediately_before_success() {
    let stale_times = vec![OBSERVED_AT, OBSERVED_AT, OBSERVED_AT + MAX_AGE + 1];

    let progress =
        opened_admitted_replicator(0, vec![Ok(present(bound_snapshot()))], stale_times.clone())
            .await;
    assert!(application_error(progress.current_progress().await.unwrap_err()).contains("stale"));

    let role =
        opened_admitted_replicator(0, vec![Ok(present(bound_snapshot()))], stale_times.clone())
            .await;
    assert!(
        application_error(
            role.change_role(configuration().epoch, ReplicaRole::Primary)
                .await
                .unwrap_err()
        )
        .contains("stale")
    );

    let epoch =
        opened_admitted_replicator(0, vec![Ok(present(bound_snapshot()))], stale_times).await;
    assert!(
        application_error(epoch.update_epoch(configuration().epoch).await.unwrap_err())
            .contains("stale")
    );
}

#[tokio::test]
async fn inconsistent_observation_timestamps_are_rejected() {
    let mut mismatched_snapshot = snapshot(NativeRole::Primary);
    mismatched_snapshot.observed_at_unix_millis += 1;

    let mut mismatched_group = snapshot(NativeRole::Primary);
    if let Observation::Present {
        observed_at_unix_millis,
        ..
    } = &mut mismatched_group.availability_group
    {
        *observed_at_unix_millis += 1;
    }

    for sample in [mismatched_snapshot, mismatched_group] {
        let replicator = opened_replicator(vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
        assert!(
            application_error(replicator.current_progress().await.unwrap_err())
                .contains("timestamps differ")
        );
    }
}

#[tokio::test]
async fn adapter_rejects_identity_and_strict_profile_mismatches() {
    let mut cases = Vec::new();

    let mut wrong_server = snapshot(NativeRole::Primary);
    wrong_server.instance.server_name = ServerName::new("other").unwrap();
    cases.push(wrong_server);

    let mut wrong_group = snapshot(NativeRole::Primary);
    if let Observation::Present { value, .. } = &mut wrong_group.availability_group {
        value.identity.name = AvailabilityGroupName::new("other-ag").unwrap();
    }
    cases.push(wrong_group);

    for count in [1, 2] {
        let mut wrong_count = snapshot(NativeRole::Primary);
        if let Observation::Present { value, .. } = &mut wrong_count.availability_group {
            value.replicas.truncate(count);
        }
        cases.push(wrong_count);
    }

    for (field, value) in [
        ("availability", "ASYNCHRONOUS_COMMIT"),
        ("failover", "MANUAL"),
        ("seeding", "MANUAL"),
    ] {
        let mut wrong_mode = snapshot(NativeRole::Primary);
        if let Observation::Present { value: group, .. } = &mut wrong_mode.availability_group {
            match field {
                "availability" => group.replicas[1].availability_mode = value.into(),
                "failover" => group.replicas[1].failover_mode = value.into(),
                "seeding" => group.replicas[1].seeding_mode = value.into(),
                _ => unreachable!(),
            }
        }
        cases.push(wrong_mode);
    }

    let mut wrong_required = snapshot(NativeRole::Primary);
    if let Observation::Present { value, .. } = &mut wrong_required.availability_group {
        value.required_synchronized_secondaries_to_commit = 0;
    }
    cases.push(wrong_required);

    let mut wrong_cluster = snapshot(NativeRole::Primary);
    if let Observation::Present { value, .. } = &mut wrong_cluster.availability_group {
        value.cluster_type = "NONE".into();
    }
    cases.push(wrong_cluster);

    let mut basic = snapshot(NativeRole::Primary);
    if let Observation::Present { value, .. } = &mut basic.availability_group {
        value.basic_features = true;
    }
    cases.push(basic);

    let mut distributed = snapshot(NativeRole::Primary);
    if let Observation::Present { value, .. } = &mut distributed.availability_group {
        value.is_distributed = true;
    }
    cases.push(distributed);

    let mut wrong_engine = snapshot(NativeRole::Primary);
    wrong_engine.instance.product_major_version = 16;
    wrong_engine.instance.product_version = "16.0.1000.1".into();
    cases.push(wrong_engine);

    let mut non_linux = snapshot(NativeRole::Primary);
    non_linux.instance.host_platform = "Windows".into();
    cases.push(non_linux);

    let mut wrong_architecture = snapshot(NativeRole::Primary);
    wrong_architecture.instance.architecture = "aarch64".into();
    cases.push(wrong_architecture);

    let mut hadr_disabled = snapshot(NativeRole::Primary);
    hadr_disabled.instance.hadr_enabled = false;
    cases.push(hadr_disabled);

    let mut two_databases = snapshot(NativeRole::Primary);
    if let Observation::Present { value, .. } = &mut two_databases.availability_group {
        value.databases = vec![
            database("55555555-5555-4555-8555-555555555555", "db-one"),
            database("66666666-6666-4666-8666-666666666666", "db-two"),
        ];
    }
    cases.push(two_databases);

    let mut unavailable_local = snapshot(NativeRole::Primary);
    if let Observation::Present { value, .. } = &mut unavailable_local.availability_group {
        value.local_replica.state_available = false;
    }
    cases.push(unavailable_local);

    for sample in cases {
        let replicator = opened_replicator(vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
        assert!(matches!(
            replicator.current_progress().await,
            Err(KubericRuntimeError::Application(_))
        ));
    }
}

#[tokio::test]
async fn resource_identity_and_native_roles_are_exact() {
    let source = Arc::new(ScriptedSource {
        config: observer_config(),
        samples: Mutex::new(VecDeque::new()),
    });
    let service = SqlServerService::with_observation_source(
        SqlServerServiceConfig::new(
            ResourceUid::new("resource-a"),
            "sql-replication.example:5022",
        )
        .unwrap(),
        source,
        Arc::new(ScriptedClock {
            times: Mutex::new(VecDeque::new()),
        }),
    );
    assert!(
        service
            .validate_resource_identity(&PartitionId::new("resource-a"))
            .is_ok()
    );
    assert!(
        service
            .validate_resource_identity(&PartitionId::new("resource-b"))
            .is_err()
    );

    for (native, requested, succeeds) in [
        (NativeRole::Primary, ReplicaRole::Primary, true),
        (NativeRole::Secondary, ReplicaRole::ActiveSecondary, true),
        (NativeRole::Secondary, ReplicaRole::IdleSecondary, true),
        (NativeRole::NotJoined, ReplicaRole::None, true),
        (NativeRole::Secondary, ReplicaRole::Primary, false),
        (NativeRole::Resolving, ReplicaRole::Primary, false),
    ] {
        let replicator =
            opened_replicator(vec![Ok(present(snapshot(native)))], vec![OBSERVED_AT]).await;
        let result = replicator.change_role(Epoch::new(1, 1), requested).await;
        assert_eq!(result.is_ok(), succeeds);
    }
}

#[test]
fn topology_constructors_reject_local_observer_and_runtime_identity_drift() {
    let source = || {
        Arc::new(ScriptedSource {
            config: observer_config(),
            samples: Mutex::new(VecDeque::new()),
        }) as Arc<dyn SqlServerObservationSource>
    };
    let clock = || {
        Arc::new(ScriptedClock {
            times: Mutex::new(VecDeque::new()),
        }) as Arc<dyn ObservationClock>
    };
    assert!(
        SqlServerService::with_observation_source_and_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022",)
                .unwrap(),
            source(),
            clock(),
            topology_expectation(),
        )
        .is_ok()
    );
    let wrong_source = Arc::new(ScriptedSource {
        config: observer_config_for(1),
        samples: Mutex::new(VecDeque::new()),
    }) as Arc<dyn SqlServerObservationSource>;
    assert!(
        SqlServerService::with_observation_source_and_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022",)
                .unwrap(),
            wrong_source,
            clock(),
            topology_expectation(),
        )
        .is_err()
    );
    let wrong_logical_config = ObserverConfig::from_json(
        br#"{
            "host":"sql.example",
            "port":1433,
            "availability_group":"test-ag",
            "expected_server_name":"sql-0",
            "replica_id":"wrong-logical",
            "incarnation":"pod-0",
            "observer_username_file":"/secrets/username",
            "observer_password_file":"/secrets/password",
            "sample_timeout_ms":1000,
            "connect_timeout_ms":1000,
            "query_timeout_ms":1000,
            "poll_interval_ms":1000,
            "max_age_ms":100
        }"#,
    )
    .unwrap();
    assert!(
        SqlServerService::with_observation_source_and_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022",)
                .unwrap(),
            Arc::new(ScriptedSource {
                config: wrong_logical_config,
                samples: Mutex::new(VecDeque::new()),
            }),
            clock(),
            topology_expectation(),
        )
        .is_err()
    );
    assert!(
        SqlServerReplicator::new_with_topology(
            "replica-1.example:5022".into(),
            source(),
            clock(),
            kuberic_identity(9),
            topology_expectation(),
        )
        .is_err()
    );
}

#[tokio::test]
async fn bound_service_open_rejects_a_different_runtime_local_identity() {
    let resource = ResourceUid::new("partition-generation-9");
    let service = Arc::new(
        SqlServerService::with_observation_source_and_topology(
            SqlServerServiceConfig::new(resource.clone(), "replica-1.example:5022").unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config(),
                samples: Mutex::new(VecDeque::new()),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(VecDeque::new()),
            }),
            topology_expectation(),
        )
        .unwrap(),
    );
    let (_directory, runtime) = testing_runtime_with_identity(
        service,
        resource,
        kuberic_identity(9),
        EffectivePolicy::fixed(3, 30).unwrap(),
    );
    let result = runtime
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::Primary,
            AccessStatus::NotPrimary,
            AccessStatus::NotPrimary,
            None,
        )
        .await;
    assert!(application_error(result.unwrap_err()).contains("runtime identity"));
}

#[tokio::test]
async fn service_roles_never_publish_routing_and_lifecycle_errors_are_distinct() {
    let resource = ResourceUid::new("partition-contract-generation");
    let source = Arc::new(ScriptedSource {
        config: observer_config(),
        samples: Mutex::new(
            (0..8)
                .map(|_| Ok(present(snapshot(NativeRole::Primary))))
                .collect(),
        ),
    });
    let clock = Arc::new(ScriptedClock {
        times: Mutex::new(vec![OBSERVED_AT; 8].into()),
    });
    let service = Arc::new(SqlServerService::with_observation_source(
        SqlServerServiceConfig::new(resource.clone(), "sql-replication.example:5022").unwrap(),
        source.clone(),
        clock.clone(),
    ));
    assert!(matches!(
        service.change_role(ReplicaRole::Primary).await,
        Err(KubericRuntimeError::NotOpen)
    ));

    let (_directory, runtime) = testing_runtime(service.clone(), resource);
    runtime
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::None,
            AccessStatus::NotPrimary,
            AccessStatus::NotPrimary,
            None,
        )
        .await
        .unwrap();
    assert!(service.replicator().is_some());
    *source.samples.lock().unwrap() = vec![
        Ok(present(snapshot(NativeRole::Primary))),
        Ok(present(snapshot(NativeRole::Secondary))),
        Ok(present(snapshot(NativeRole::Secondary))),
        Ok(present(snapshot(NativeRole::NotJoined))),
    ]
    .into();
    *clock.times.lock().unwrap() = vec![OBSERVED_AT; 4].into();

    for role in [
        ReplicaRole::Primary,
        ReplicaRole::ActiveSecondary,
        ReplicaRole::IdleSecondary,
        ReplicaRole::None,
    ] {
        assert_eq!(
            service.change_role(role).await.unwrap().service_address,
            None
        );
    }

    service.close().await.unwrap();
    assert!(service.replicator().is_none());
    assert!(matches!(
        service.change_role(ReplicaRole::None).await,
        Err(KubericRuntimeError::Closed)
    ));
}

#[tokio::test]
async fn service_close_and_abort_before_open_prevent_registration() {
    for abort in [false, true] {
        let resource = ResourceUid::new("partition-contract-generation");
        let service = Arc::new(SqlServerService::with_observation_source(
            SqlServerServiceConfig::new(resource.clone(), "sql-replication.example:5022").unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config(),
                samples: Mutex::new(VecDeque::new()),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(VecDeque::new()),
            }),
        ));
        if abort {
            service.abort();
        } else {
            service.close().await.unwrap();
        }
        let (_directory, runtime) = testing_runtime(service.clone(), resource);
        assert!(matches!(
            runtime
                .reconstruct(
                    OpenMode::Existing,
                    ReplicaRole::None,
                    AccessStatus::NotPrimary,
                    AccessStatus::NotPrimary,
                    None,
                )
                .await,
            Err(KubericRuntimeError::Closed)
        ));
        assert!(service.replicator().is_none());
    }
}

#[tokio::test]
async fn unopened_closed_and_aborted_progress_have_distinct_errors() {
    let unopened = replicator_with(Vec::new(), Vec::new());
    assert!(matches!(
        unopened.current_progress().await,
        Err(KubericRuntimeError::NotOpen)
    ));

    let closed = opened_replicator(Vec::new(), Vec::new()).await;
    closed.close().await.unwrap();
    assert!(matches!(
        closed.current_progress().await,
        Err(KubericRuntimeError::Closed)
    ));

    let aborted = opened_replicator(Vec::new(), Vec::new()).await;
    aborted.abort();
    assert!(matches!(
        aborted.current_progress().await,
        Err(KubericRuntimeError::Closed)
    ));
}

#[tokio::test]
async fn bound_topology_admits_exact_current_replays_and_reports_fresh_capability() {
    let current = bound_replica_set();
    let replicator = opened_bound_replicator(
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 4],
    )
    .await;

    assert!(
        application_error(replicator.catch_up_capability().await.unwrap_err())
            .contains("not been admitted")
    );
    replicator
        .update_current_replica_set_configuration(current.clone())
        .await
        .unwrap();
    assert!(
        application_error(replicator.catch_up_capability().await.unwrap_err())
            .contains("stable local role")
    );
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    assert_eq!(replicator.catch_up_capability().await.unwrap(), 42);
    replicator
        .update_current_replica_set_configuration(current)
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_current_admission_does_not_poison_an_exact_retry() {
    let current = bound_replica_set();
    let mut mismatched = bound_snapshot();
    if let Observation::Present { value, .. } = &mut mismatched.availability_group {
        value.configuration_sequence = ConfigurationSequence::parse("43").unwrap();
    }
    let replicator = opened_bound_replicator(
        vec![
            Ok(present(mismatched)),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 3],
    )
    .await;

    let error = replicator
        .update_current_replica_set_configuration(current.clone())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("configuration sequence"));
    assert!(
        application_error(replicator.catch_up_capability().await.unwrap_err())
            .contains("not been admitted")
    );

    replicator
        .update_current_replica_set_configuration(current)
        .await
        .unwrap();
}

#[tokio::test]
async fn first_admission_rejects_duplicate_and_wrong_local_native_replica_ids() {
    let mut duplicate = bound_snapshot();
    if let Observation::Present { value, .. } = &mut duplicate.availability_group {
        value.replicas[1].replica_id = value.replicas[0].replica_id.clone();
    }
    let mut wrong_local = bound_snapshot();
    if let Observation::Present { value, .. } = &mut wrong_local.availability_group {
        value.local_replica.identity =
            ReplicaIdentity::observed("logical-0", guid(SECOND_ID), "pod-0").unwrap();
    }

    for sample in [duplicate, wrong_local] {
        let replicator =
            opened_bound_replicator(vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
        let message = application_error(
            replicator
                .update_current_replica_set_configuration(bound_replica_set())
                .await
                .unwrap_err(),
        );
        assert!(
            message.contains("binding mismatch")
                || message.contains("identities differ")
                || message.contains("configuration or state"),
            "{message}"
        );
    }
}

#[test]
fn runtime_adapter_contains_missing_peer_authority_failure_before_persistence() {
    run_runtime_effect_test(async {
        let expectation = topology_expectation();
        let source = Arc::new(CountingSource {
            inner: ScriptedSource {
                config: observer_config(),
                samples: Mutex::new((0..10).map(|_| Ok(present(bound_snapshot()))).collect()),
            },
            observations: AtomicUsize::new(0),
        });
        let (_directory, store, _service, runtime, adapter) = open_effect_runtime(
            0,
            expectation,
            EffectivePolicy::fixed(3, 30).unwrap(),
            source.clone(),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
        )
        .await;

        let error = adapter
            .execute(runtime_effect(
                "exact-admission",
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(admitted_authority(0))),
            ))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("session, address, or role"),
            "{error}"
        );
        assert!(runtime.snapshot().await.authority.is_none());
        let failed = store.load_state().await.unwrap();
        assert!(failed.current_configuration.is_none());
        assert!(failed.admitted_policy.is_none());

        assert!(
            describe_peer(
                &runtime,
                ReplicaInformation::new(
                    OperationId::new("post-failure-peer-description"),
                    kuberic_identity(2),
                    "replica-2.example:5022".into(),
                ),
            )
            .await
            .is_err(),
            "contained authority failure must close the runtime before later peer mutation"
        );
        assert!(failed.pending_effect.is_some());
        assert_eq!(source.observations.load(Ordering::SeqCst), 2);
    });
}

#[tokio::test]
async fn both_bound_active_secondaries_admit_the_same_frozen_topology() {
    for local_index in [1, 2] {
        let current = bound_replica_set();
        let replicator = bound_replicator_for(
            local_index,
            vec![
                Ok(present(bound_snapshot_for(local_index))),
                Ok(present(bound_snapshot_for(local_index))),
                Ok(present(bound_snapshot_for(local_index))),
            ],
            vec![OBSERVED_AT; 3],
        );
        replicator.open().await.unwrap();
        replicator
            .update_current_replica_set_configuration(current)
            .await
            .unwrap();
        replicator
            .change_role(configuration().epoch, ReplicaRole::ActiveSecondary)
            .await
            .unwrap();
        assert_eq!(replicator.catch_up_capability().await.unwrap(), 42);
    }
}

#[tokio::test]
async fn secondary_commit_participation_is_not_required_from_direct_observation() {
    for commit_participant in [Some(false), None] {
        let mut sample = bound_snapshot_for(1);
        if let Observation::Present { value: group, .. } = &mut sample.availability_group {
            group.databases[0].replicas[0].is_commit_participant = commit_participant;
        }
        let replicator =
            opened_admitted_replicator(1, vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
        assert_eq!(replicator.current_progress().await.unwrap(), 42);
    }
}

#[tokio::test]
async fn primary_commit_participation_remains_exact() {
    for commit_participant in [Some(false), None] {
        let mut sample = bound_snapshot();
        if let Observation::Present { value: group, .. } = &mut sample.availability_group {
            group.databases[0].replicas[0].is_commit_participant = commit_participant;
        }
        let replicator =
            opened_admitted_replicator(0, vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
        let message = application_error(replicator.current_progress().await.unwrap_err());
        assert!(message.contains("binding mismatch"), "{message}");
    }
}

#[tokio::test]
async fn connected_native_replica_accepts_absent_last_connect_error() {
    let mut sample = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut sample.availability_group {
        group.replicas[0]
            .state
            .as_mut()
            .unwrap()
            .last_connect_error_number = None;
    }
    let replicator =
        opened_admitted_replicator(0, vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
    assert_eq!(replicator.current_progress().await.unwrap(), 42);
}

#[tokio::test]
async fn bound_current_configuration_rejects_every_frozen_value_drift() {
    let exact = bound_replica_set();
    let mut cases = Vec::new();

    let mut changed = exact.clone();
    changed.configuration.configuration_id = ConfigurationId::new("changed");
    cases.push(("configuration ID", changed));

    let mut changed = exact.clone();
    changed.configuration.epoch = Epoch::new(7, 12);
    cases.push(("epoch", changed));

    let mut changed = exact.clone();
    changed.configuration.primary_id = ReplicaId::new(2);
    cases.push(("primary", changed));

    let mut changed = exact.clone();
    changed.configuration.write_quorum = 1;
    cases.push(("quorum", changed));

    let mut changed = exact.clone();
    changed.configuration.members.pop();
    cases.push(("configuration members", changed));

    let mut changed = exact.clone();
    changed.configuration.members[1].role = ReplicaRole::IdleSecondary;
    cases.push(("configuration role", changed));

    let mut changed = exact.clone();
    changed.replicas[2].role = ReplicaRole::IdleSecondary;
    cases.push(("replica role", changed));

    let mut changed = exact.clone();
    changed.replicas[0].identity = kuberic_identity(9);
    cases.push(("replica identity", changed));

    let mut changed = exact.clone();
    changed.replicas[0].process_session_id = ProcessSessionId::default();
    cases.push(("empty session", changed));

    let mut changed = exact.clone();
    changed.replicas[0].replication_address.clear();
    cases.push(("empty address", changed));

    let mut changed = exact.clone();
    changed.replicas[1].process_session_id = changed.replicas[0].process_session_id.clone();
    cases.push(("duplicate session", changed));

    let mut changed = exact.clone();
    changed.replicas[1].replication_address = changed.replicas[0].replication_address.clone();
    cases.push(("duplicate address", changed));

    for (name, current) in cases {
        let replicator =
            opened_bound_replicator(vec![Ok(present(bound_snapshot()))], vec![OBSERVED_AT]).await;
        let message = application_error(
            replicator
                .update_current_replica_set_configuration(current)
                .await
                .unwrap_err(),
        );
        assert!(message.contains("binding mismatch"), "{name}: {message}");
    }
}

#[tokio::test]
async fn admitted_current_configuration_allows_only_value_identical_replay() {
    let current = bound_replica_set();
    let replicator = opened_bound_replicator(
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 4],
    )
    .await;
    replicator
        .update_current_replica_set_configuration(current.clone())
        .await
        .unwrap();

    let mut changed_progress = current.clone();
    changed_progress.replicas[0].current_progress += 1;
    let mut changed_session = current.clone();
    changed_session.replicas[0].process_session_id = ProcessSessionId::new("other-session");
    let mut changed_address = current;
    changed_address.replicas[1].replication_address = "other.example:5022".into();
    for changed_replay in [changed_progress, changed_session, changed_address] {
        let message = application_error(
            replicator
                .update_current_replica_set_configuration(changed_replay)
                .await
                .unwrap_err(),
        );
        assert!(message.contains("replay differs"), "{message}");
    }
}

#[tokio::test]
async fn a_new_process_admits_fresh_sessions_for_the_same_stable_topology() {
    let first =
        opened_bound_replicator(vec![Ok(present(bound_snapshot()))], vec![OBSERVED_AT]).await;
    first
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();

    let restarted = opened_bound_replicator(
        vec![Ok(present(bound_snapshot())), Ok(present(bound_snapshot()))],
        vec![OBSERVED_AT; 2],
    )
    .await;
    let restarted_configuration = replica_set_with_session_prefix("restart-session");
    restarted
        .update_current_replica_set_configuration(restarted_configuration.clone())
        .await
        .unwrap();
    let admitted = restarted.admitted_topology().unwrap();
    assert_eq!(
        admitted
            .members()
            .iter()
            .map(|member| member.process_session_id().as_str())
            .collect::<Vec<_>>(),
        [
            "restart-session-1",
            "restart-session-2",
            "restart-session-3"
        ]
    );

    let error = restarted
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("replay differs"));
}

#[tokio::test]
async fn bound_evidence_rejects_identity_incarnation_lineage_role_and_health_drift() {
    let mut cases = Vec::new();

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.identity.group_id = guid("99999999-9999-4999-8999-999999999999");
    }
    cases.push(("AG recreation", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.local_replica.identity =
            ReplicaIdentity::observed("logical-0", guid(SECOND_ID), "pod-0").unwrap();
    }
    cases.push(("native replica replacement", changed));

    let mut changed = bound_snapshot();
    changed.instance.server_name = ServerName::new("other-server").unwrap();
    changed.instance.property_server_name = ServerName::new("other-server").unwrap();
    cases.push(("server identity", changed));

    let mut changed = bound_snapshot();
    changed.instance.sqlserver_start_time = "2026-10-06T13:00:00".into();
    cases.push(("process restart", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.local_replica.identity =
            ReplicaIdentity::observed("logical-0", guid(LOCAL_ID), "other-pod").unwrap();
    }
    cases.push(("container incarnation", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.replicas[0].endpoint_url = Some("TCP://replacement:5022".into());
    }
    cases.push(("endpoint identity", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.databases[0]
            .local
            .as_mut()
            .unwrap()
            .recovery
            .as_mut()
            .unwrap()
            .recovery_fork_guid = Some(guid("99999999-9999-4999-8999-999999999999"));
    }
    cases.push(("database lineage", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.databases[0].replicas[0].synchronization_state = Some("SYNCHRONIZING".into());
    }
    cases.push(("synchronization", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.databases[0].replicas[0].synchronization_health = Some("PARTIALLY_HEALTHY".into());
    }
    cases.push(("database health", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.replicas[0].state.as_mut().unwrap().connected_state = Some("DISCONNECTED".into());
    }
    cases.push(("replica health", changed));

    let mut changed = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut changed.availability_group {
        group.local_replica.role = Some(NativeRole::Secondary);
    }
    cases.push(("native role", changed));

    for (name, sample) in cases {
        let replicator =
            opened_admitted_replicator(0, vec![Ok(present(sample))], vec![OBSERVED_AT]).await;
        let message = application_error(replicator.current_progress().await.unwrap_err());
        assert!(
            message.contains("binding mismatch")
                || message.contains("identities differ")
                || message.contains("configuration or state"),
            "{name}: {message}"
        );
    }
}

#[tokio::test]
async fn every_bound_callback_rejects_frozen_configuration_sequence_drift() {
    let mut drifted = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut drifted.availability_group {
        group.configuration_sequence = ConfigurationSequence::parse("43").unwrap();
    }

    for operation in ["admission", "progress", "role", "epoch"] {
        let replicator = if operation == "admission" {
            opened_bound_replicator(vec![Ok(present(drifted.clone()))], vec![OBSERVED_AT]).await
        } else {
            opened_admitted_replicator(0, vec![Ok(present(drifted.clone()))], vec![OBSERVED_AT])
                .await
        };
        let error = match operation {
            "admission" => replicator
                .update_current_replica_set_configuration(bound_replica_set())
                .await
                .unwrap_err(),
            "progress" => replicator.current_progress().await.unwrap_err(),
            "role" => replicator
                .change_role(configuration().epoch, ReplicaRole::Primary)
                .await
                .unwrap_err(),
            "epoch" => replicator
                .update_epoch(configuration().epoch)
                .await
                .unwrap_err(),
            _ => unreachable!(),
        };
        assert!(
            application_error(error).contains("configuration sequence"),
            "{operation}"
        );
    }

    let replicator = opened_bound_replicator(
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(drifted)),
        ],
        vec![OBSERVED_AT; 3],
    )
    .await;
    replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    assert!(
        application_error(replicator.catch_up_capability().await.unwrap_err())
            .contains("configuration sequence")
    );
}

#[tokio::test]
async fn bound_evidence_rejects_stale_future_failed_closed_and_aborted_calls() {
    for (now, expected) in [
        (OBSERVED_AT + MAX_AGE + 1, "stale"),
        (OBSERVED_AT - 1, "future-dated"),
    ] {
        let replicator =
            opened_bound_replicator(vec![Ok(present(bound_snapshot()))], vec![now]).await;
        assert!(
            application_error(
                replicator
                    .update_current_replica_set_configuration(bound_replica_set())
                    .await
                    .unwrap_err()
            )
            .contains(expected)
        );
    }

    let failed = opened_bound_replicator(
        vec![Ok(Observation::Failed(ObservationFailure {
            kind: ObservationFailureKind::PermissionDenied,
            message: "redacted".into(),
            observed_at_unix_millis: OBSERVED_AT,
        }))],
        Vec::new(),
    )
    .await;
    assert!(
        application_error(
            failed
                .update_current_replica_set_configuration(bound_replica_set())
                .await
                .unwrap_err()
        )
        .contains("PermissionDenied")
    );

    let closed = opened_bound_replicator(Vec::new(), Vec::new()).await;
    closed.close().await.unwrap();
    assert!(matches!(
        closed.current_progress().await,
        Err(KubericRuntimeError::Closed)
    ));

    let aborted = opened_bound_replicator(Vec::new(), Vec::new()).await;
    aborted.abort();
    assert!(matches!(
        aborted.current_progress().await,
        Err(KubericRuntimeError::Closed)
    ));
}

#[tokio::test]
async fn close_during_observation_cannot_publish_current_admission() {
    let current = bound_replica_set();
    let (replicator, entered, release) =
        gated_bound_replicator(vec![Ok(present(bound_snapshot()))], vec![OBSERVED_AT], 0);
    replicator.open().await.unwrap();
    let task = tokio::spawn({
        let replicator = replicator.clone();
        async move {
            replicator
                .update_current_replica_set_configuration(current)
                .await
        }
    });
    entered.await.unwrap();
    replicator.close().await.unwrap();
    release.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    assert!(matches!(
        replicator.catch_up_capability().await,
        Err(KubericRuntimeError::Closed)
    ));
}

#[tokio::test]
async fn abort_during_fresh_observation_fences_progress_role_and_epoch_results() {
    for operation in ["progress", "role", "epoch"] {
        let (replicator, entered, release) = gated_bound_replicator(
            vec![Ok(present(bound_snapshot())), Ok(present(bound_snapshot()))],
            vec![OBSERVED_AT; 2],
            1,
        );
        replicator.open().await.unwrap();
        replicator
            .update_current_replica_set_configuration(bound_replica_set())
            .await
            .unwrap();
        let task = tokio::spawn({
            let replicator = replicator.clone();
            let epoch = configuration().epoch;
            async move {
                match operation {
                    "progress" => replicator.current_progress().await.map(|_| ()),
                    "role" => replicator.change_role(epoch, ReplicaRole::Primary).await,
                    "epoch" => replicator.update_epoch(epoch).await,
                    _ => unreachable!(),
                }
            }
        });
        entered.await.unwrap();
        replicator.abort();
        release.send(()).unwrap();
        assert!(
            matches!(task.await.unwrap(), Err(KubericRuntimeError::Closed)),
            "{operation}"
        );
    }
}

#[tokio::test]
async fn abort_during_capability_observation_cannot_return_fresh_progress() {
    let current = bound_replica_set();
    let (replicator, entered, release) = gated_bound_replicator(
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 3],
        2,
    );
    replicator.open().await.unwrap();
    replicator
        .update_current_replica_set_configuration(current)
        .await
        .unwrap();
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    let task = tokio::spawn({
        let replicator = replicator.clone();
        async move { replicator.catch_up_capability().await }
    });
    entered.await.unwrap();
    replicator.abort();
    release.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
}

#[tokio::test]
async fn bound_role_and_epoch_callbacks_require_fresh_evidence_and_the_frozen_values() {
    let replicator = opened_admitted_replicator(
        0,
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 5],
    )
    .await;

    assert!(
        application_error(
            replicator
                .change_role(configuration().epoch, ReplicaRole::ActiveSecondary)
                .await
                .unwrap_err()
        )
        .contains("requested role")
    );
    assert!(
        application_error(
            replicator
                .change_role(Epoch::new(7, 12), ReplicaRole::Primary)
                .await
                .unwrap_err()
        )
        .contains("frozen epoch")
    );
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    assert!(
        application_error(
            replicator
                .update_epoch(Epoch::new(7, 12))
                .await
                .unwrap_err()
        )
        .contains("frozen epoch")
    );
    replicator
        .update_epoch(configuration().epoch)
        .await
        .unwrap();
}

#[tokio::test]
async fn capability_revalidates_instead_of_caching_success() {
    let current = bound_replica_set();
    let mut drifted = bound_snapshot();
    if let Observation::Present { value: group, .. } = &mut drifted.availability_group {
        group.databases[0].replicas[0].synchronization_health = Some("NOT_HEALTHY".into());
    }
    let replicator = opened_bound_replicator(
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(drifted)),
        ],
        vec![OBSERVED_AT; 4],
    )
    .await;
    replicator
        .update_current_replica_set_configuration(current)
        .await
        .unwrap();
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    assert_eq!(replicator.catch_up_capability().await.unwrap(), 42);
    assert!(
        application_error(replicator.catch_up_capability().await.unwrap_err())
            .contains("binding mismatch")
    );
}

#[tokio::test]
async fn supported_bound_callbacks_use_one_fresh_observation_and_no_mutation_interface() {
    let current = bound_replica_set();
    let source = Arc::new(CountingSource {
        inner: ScriptedSource {
            config: observer_config(),
            samples: Mutex::new(
                vec![
                    Ok(present(bound_snapshot())),
                    Ok(present(bound_snapshot())),
                    Ok(present(bound_snapshot())),
                    Ok(present(bound_snapshot())),
                ]
                .into(),
            ),
        },
        observations: AtomicUsize::new(0),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_with_topology(
            "replica-1.example:5022".into(),
            source.clone(),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT; 4].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
        )
        .unwrap(),
    );
    replicator.open().await.unwrap();

    replicator
        .update_current_replica_set_configuration(current)
        .await
        .unwrap();
    assert_eq!(replicator.current_progress().await.unwrap(), 42);
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    assert_eq!(replicator.catch_up_capability().await.unwrap(), 42);
    assert_eq!(source.observations.load(Ordering::SeqCst), 4);
    assert!(source.inner.samples.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bound_admission_never_enables_mutating_or_recovery_callbacks() {
    let current = bound_replica_set();
    let replicator = opened_bound_replicator(
        vec![Ok(present(bound_snapshot())), Ok(present(bound_snapshot()))],
        vec![OBSERVED_AT; 2],
    )
    .await;
    replicator
        .update_current_replica_set_configuration(current)
        .await
        .unwrap();
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();

    let failures = [
        replicator.on_data_loss().await.unwrap_err(),
        replicator
            .update_catch_up_replica_set_configuration(replica_set(), replica_set())
            .await
            .unwrap_err(),
        replicator
            .wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum)
            .await
            .unwrap_err(),
        replicator
            .build_replica(replica_information())
            .await
            .unwrap_err(),
        replicator
            .remove_replica(ReplicaId::new(2))
            .await
            .unwrap_err(),
    ];
    for error in failures {
        assert!(application_error(error).contains("observe-only"));
    }
}

#[test]
fn exact_current_capability_matches_the_locked_controller_comparison_boundary() {
    fn requires_full_repair(member_progress: i64, retained_from: i64) -> bool {
        member_progress.saturating_add(1) < retained_from
    }

    let retained_from = 42;
    assert!(!requires_full_repair(42, retained_from));
    assert!(!requires_full_repair(41, retained_from));
    assert!(requires_full_repair(40, retained_from));
}

fn replica_set() -> ReplicaSetConfiguration {
    ConfigurationDescriptor::new(Epoch::new(1, 1), ReplicaId::new(1), Vec::new(), 1).into()
}

fn replica_information() -> ReplicaInformation {
    ReplicaInformation::new(
        OperationId::new("build"),
        KubericReplicaIdentity {
            replica_id: ReplicaId::new(2),
            instance_id: ReplicaInstanceId::new("pod-2"),
            agent_generation: AgentGeneration::new("generation-2"),
        },
        "replica-2.example:5022".into(),
    )
}

#[tokio::test]
async fn every_topology_callback_is_an_explicit_observe_only_error() {
    let replicator = opened_replicator(Vec::new(), Vec::new()).await;
    let mut failures = Vec::new();
    failures.push(replicator.catch_up_capability().await.unwrap_err());
    failures.push(replicator.on_data_loss().await.unwrap_err());
    failures.push(
        replicator
            .update_catch_up_replica_set_configuration(replica_set(), replica_set())
            .await
            .unwrap_err(),
    );
    failures.push(
        replicator
            .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
            .await
            .unwrap_err(),
    );
    failures.push(
        replicator
            .update_current_replica_set_configuration(replica_set())
            .await
            .unwrap_err(),
    );
    failures.push(
        replicator
            .build_replica(replica_information())
            .await
            .unwrap_err(),
    );
    failures.push(
        replicator
            .remove_replica(ReplicaId::new(2))
            .await
            .unwrap_err(),
    );

    let expected = [
        "catch_up_capability",
        "on_data_loss",
        "update_catch_up_replica_set_configuration",
        "wait_for_catch_up_quorum",
        "update_current_replica_set_configuration",
        "build_replica",
        "remove_replica",
    ];
    for (error, callback) in failures.into_iter().zip(expected) {
        let message = application_error(error);
        assert!(message.contains(callback), "{message}");
        assert!(message.contains("observe-only"), "{message}");
    }
}
