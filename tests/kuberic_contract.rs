use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_runtime::RuntimeError as KubericRuntimeError;
use kuberic_runtime::application::{OpenMode, StatefulServiceReplica};
use kuberic_runtime::protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, EffectivePolicy, Epoch,
    InitializationId, OperationId, PartitionId, PodUid, PvcUid, ReplicaId,
    ReplicaIdentity as KubericReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, ReplicaSetQuorumMode,
    Replicator,
};
use kuberic_runtime::testing::hosting::PodRuntime;
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use sqlserver_replicated::kuberic::{
    ObservationClock, SqlServerObservationSource, SqlServerReplicator, SqlServerReplicatorFactory,
    SqlServerService, SqlServerServiceConfig,
};
use sqlserver_replicated::observation::{
    AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, DatabaseSnapshot, InstanceMetadata,
    InstanceSnapshot, LocalReplicaSnapshot, NativeProvenance, RecoveryLineageObservation,
    ReplicaSnapshot,
};
use sqlserver_replicated::runtime_config::ObserverConfig;
use sqlserver_replicated::runtime_error::RuntimeError;
use sqlserver_replicated::{
    AvailabilityGroupIdentity, AvailabilityGroupName, ConfigurationSequence, DatabaseIdentity,
    DecimalProgress, Guid, NativeProgress, NativeRole, Observation, ObservationFailure,
    ObservationFailureKind, ReplicaIdentity, ServerName, SqlIdentifier,
};

const OBSERVED_AT: u64 = 1_000;
const MAX_AGE: u64 = 100;
const AG_ID: &str = "11111111-1111-4111-8111-111111111111";
const LOCAL_ID: &str = "22222222-2222-4222-8222-222222222222";
const SECOND_ID: &str = "33333333-3333-4333-8333-333333333333";
const THIRD_ID: &str = "44444444-4444-4444-8444-444444444444";

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

struct ScriptedClock {
    times: Mutex<VecDeque<u64>>,
}

impl ObservationClock for ScriptedClock {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
        Ok(self
            .times
            .lock()
            .unwrap()
            .pop_front()
            .expect("test must provide one request-time clock value"))
    }
}

fn observer_config() -> ObserverConfig {
    ObserverConfig::from_json(
        format!(
            r#"{{
                "host":"sql.example",
                "port":1433,
                "availability_group":"test-ag",
                "expected_server_name":"sql-0",
                "replica_id":"logical-0",
                "incarnation":"pod-0",
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

fn testing_runtime(
    service: Arc<SqlServerService>,
    resource: ResourceUid,
) -> (tempfile::TempDir, PodRuntime) {
    let directory = tempfile::tempdir().expect("create isolated Kuberic testing directory");
    let identity = KubericReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: ReplicaInstanceId::new("contract-instance"),
        agent_generation: AgentGeneration::new("contract-generation"),
    };
    let state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: resource,
        pod_uid: PodUid::new("contract-pod"),
        pvc_uid: PvcUid::new("contract-pvc"),
        initialization_id: InitializationId::new("contract-initialization"),
        local_identity: identity.clone(),
        effective_policy: EffectivePolicy::fixed(1, 0).expect("valid singleton policy"),
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
