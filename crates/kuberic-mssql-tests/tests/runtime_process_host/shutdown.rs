use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use kuberic_mssql::kuberic::{
    SqlServerBootstrapConfig, SqlServerReplicator, TestingSeedStoreGate, TestingSeedStoreOperation,
    TestingSeedStoreStage,
};
use kuberic_mssql::runtime_host::{
    RuntimeProcessError, SeedDispatchPhase, SeedStateError, TestingRuntimeCompletion,
    testing_start_runtime_with_application,
};
use kuberic_mssql::{
    AvailabilityGroupOwnership, MutationEffect, SeedDispatchEvidence, SeedDispatchRequest,
    SqlMutationExecutor, StandaloneDatabaseIdentity, StandaloneDatabaseOwnership,
    StandaloneDatabaseSnapshot, source_database_creation_token,
};
use kuberic_runtime::protocol::types::ProcessSessionId;
use kuberic_runtime::replicator::{PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use super::*;

// All native behavior is scripted. The host, application, replicator, owned
// spawn_blocking store workers and owned submission tasks are the real ones.
struct SeedExecutor {
    executions: AtomicUsize,
    completed: AtomicBool,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait]
impl SqlMutationExecutor for SeedExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        let identity = StandaloneDatabaseIdentity {
            name: SqlIdentifier::new("app-db").unwrap(),
            database_guid: guid("77777777-7777-4777-8777-777777777777"),
            family_guid: guid("88888888-8888-4888-8888-888888888888"),
            recovery_fork_guid: guid("55555555-5555-4555-8555-555555555555"),
        };
        let local = identities()[0].clone();
        let mut digest = Sha256::new();
        for value in [
            "kuberic-mssql-public-v1-activation",
            local.replica_id.to_string().as_str(),
            local.instance_id.as_str(),
            local.agent_generation.as_str(),
        ] {
            digest.update(value.as_bytes());
            digest.update(b"\0");
        }
        let activation: String = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let ownership = StandaloneDatabaseOwnership {
            resource_id: "runtime-host-test".into(),
            configuration_id: configuration().configuration_id.to_string(),
            activation_id: activation.clone(),
        };
        Ok(Some(StandaloneDatabaseSnapshot {
            creation_token: Some(source_database_creation_token(&identity.name, &ownership)),
            identity: Some(identity),
            ownership: Some(ownership),
            availability_group_ownership: Some(AvailabilityGroupOwnership {
                resource_id: "runtime-host-test".into(),
                configuration_id: configuration().configuration_id.to_string(),
                activation_id: activation,
                name: AvailabilityGroupName::new("app-ag").unwrap(),
                creation_group_id: Some(guid("66666666-6666-4666-8666-666666666666")),
                group_id: Some(guid("66666666-6666-4666-8666-666666666666")),
            }),
            state: "ONLINE".into(),
            recovery_model: "FULL".into(),
            accessible: true,
            marker_table_ready: true,
            full_backup_ready: true,
            log_chain_ready: true,
            backup_verified: true,
        }))
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError> {
        assert_eq!(effect.label(), "add database to availability group");
        assert_eq!(self.executions.fetch_add(1, Ordering::SeqCst), 0);
        self.entered
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let release = self.release.lock().unwrap().take().unwrap();
        release
            .await
            .expect("explicitly release the entered executor");
        self.completed.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        Ok(effect.label() == "grant create any database")
    }

    async fn reconcile_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        Ok(if self.executions.load(Ordering::SeqCst) == 0 {
            SeedDispatchEvidence::Absent
        } else {
            // Local completion cannot reconcile an already retired callback.
            SeedDispatchEvidence::Ambiguous
        })
    }
}

struct ConcreteAttempt {
    config: RuntimeHostConfig,
    application: Arc<RuntimeHostApplication>,
    executor: Arc<SeedExecutor>,
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    stop: oneshot::Sender<Result<(), RuntimeProcessError>>,
    waiter: tokio::task::JoinHandle<Result<(), RuntimeProcessError>>,
    completion: TestingRuntimeCompletion,
}

async fn start_concrete(root: &Path) -> ConcreteAttempt {
    let (controls, replications) = free_host_addresses();
    let config = host_config(root, 0, &controls, &replications).await;
    let mut sample = snapshot(0);
    let Observation::Present { value: group, .. } = &mut sample.availability_group else {
        unreachable!()
    };
    group.databases.clear();
    for (index, replica) in group.replicas.iter_mut().enumerate() {
        replica.state = Some(ReplicaState {
            provenance: if index == 0 {
                NativeProvenance::Local
            } else {
                NativeProvenance::PrimaryRemote
            },
            role: Some(if index == 0 {
                NativeRole::Primary
            } else {
                NativeRole::Secondary
            }),
            operational_state: Some("ONLINE".into()),
            connected_state: Some("CONNECTED".into()),
            recovery_health: Some("ONLINE".into()),
            synchronization_health: Some("HEALTHY".into()),
            last_connect_error_number: Some(0),
        });
    }
    let source = Arc::new(RepeatingSource {
        config: config.observer().clone(),
        samples: Mutex::new(VecDeque::new()),
        fallback: sample,
        gate: Mutex::new(None),
    });
    let (entered, held) = oneshot::channel();
    let (release, resumed) = oneshot::channel();
    let executor = Arc::new(SeedExecutor {
        executions: AtomicUsize::new(0),
        completed: AtomicBool::new(false),
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(Some(resumed)),
    });
    let application = Arc::new(
        RuntimeHostApplication::testing_with_bootstrap_sources(
            &config,
            source,
            Arc::new(FixedClock),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
        )
        .unwrap(),
    );
    let (stop, stopped) = oneshot::channel();
    let (waiter, completion) =
        testing_start_runtime_with_application(&config, application.clone(), async move {
            stopped
                .await
                .expect("explicit process stop or cancelled waiter")
        })
        .unwrap();
    let waiter = tokio::spawn(waiter);
    let (mut client, report) = status(controls[0], 1).await;
    client
        .execute(initialization(&report.process_session_id, 0))
        .await
        .unwrap();
    ConcreteAttempt {
        config,
        application,
        executor,
        entered: held,
        release,
        stop,
        waiter,
        completion,
    }
}

async fn open_request(attempt: &ConcreteAttempt) -> wire::ExecuteCommandRequest {
    let (_, report) = status(attempt.config.control_address(), 1).await;
    let report = public_report(&report);
    let program = PublicOperationProgram::Open {
        replica: report.replica.clone(),
        existing: false,
    };
    public_operation_request(
        0,
        &report,
        program_intent(&report, "concrete-open", 1, program),
    )
}

async fn open_concrete(attempt: &ConcreteAttempt) -> Arc<SqlServerReplicator> {
    let request = open_request(attempt).await;
    let (mut client, _) = status(attempt.config.control_address(), 1).await;
    // Deliberately no retry: retain any standalone Open failure as evidence.
    client.execute(authorized(request)).await.unwrap();
    let (_, report) = status(attempt.config.control_address(), 1).await;
    assert_unsupported_queries(&public_report(&report));
    let session = public_report(&report).process_session_id;
    let replicator = attempt.application.testing_replicator().unwrap();
    let mut replicas = Vec::new();
    for identity in identities() {
        let mut replica = ReplicaInformation::new(
            OperationId::new(format!("current-{}", identity.replica_id)),
            identity.clone(),
            format!("replica-{}.example:5022", identity.replica_id),
        );
        replica.process_session_id = if identity.replica_id == ReplicaId::new(1) {
            replica.replication_address = attempt.config.replication_endpoint().to_owned();
            session.clone()
        } else {
            ProcessSessionId::new(format!("peer-session-{}", identity.replica_id))
        };
        replica.role = if identity.replica_id == ReplicaId::new(1) {
            ReplicaRole::Primary
        } else {
            ReplicaRole::ActiveSecondary
        };
        replicas.push(replica);
    }
    // Full native topology admission only; the API's unused progress fields
    // retain their defaults rather than simulating successful public queries.
    replicator
        .update_current_replica_set_configuration(ReplicaSetConfiguration {
            configuration: configuration(),
            replicas,
        })
        .await
        .unwrap();
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    let (_, report) = status(attempt.config.control_address(), 1).await;
    let report = public_report(&report);
    assert_unsupported_queries(&report);
    assert!(report.configuration.is_none());
    assert_ne!(report.read_status, AccessStatus::Granted);
    assert_ne!(report.write_status, AccessStatus::Granted);
    replicator
}

fn target_build() -> ReplicaInformation {
    let identity = identities()[1].clone();
    let mut target = ReplicaInformation::new(
        OperationId::new("concrete-held-build"),
        identity.clone(),
        "replica-2.example:5022".into(),
    );
    target.process_session_id = ProcessSessionId::new("peer-session-2");
    target.role = ReplicaRole::IdleSecondary;
    target.storage_incarnation = Some(ReplicaStorageIncarnationContext {
        replica: identity,
        storage: ReplicaStorageIdentity::Volatile {
            identity: "target-storage".into(),
        },
        storage_generation: StorageGeneration::new("target-storage-generation"),
    });
    target
}

async fn hold_store(replicator: &Arc<SqlServerReplicator>) -> TestingSeedStoreGate {
    let gate = replicator
        .testing_gate_seed_store(
            TestingSeedStoreOperation::Create,
            TestingSeedStoreStage::Started,
        )
        .unwrap();
    let mut callback = tokio::spawn({
        let replicator = replicator.clone();
        async move { replicator.build_replica(target_build()).await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = gate.entered.notified() => {},
            result = &mut callback => panic!("build finished before store gate: {result:?}"),
        }
    })
    .await
    .unwrap();
    callback.abort();
    assert!(callback.await.unwrap_err().is_cancelled());
    gate
}

fn assert_same_root_owned(root: &Path) {
    assert!(matches!(
        SeedDispatchStore::open(root),
        Err(SeedStateError::AlreadyOwned)
    ));
}

async fn assert_terminal(
    application: &Arc<RuntimeHostApplication>,
    replicator: &SqlServerReplicator,
) {
    assert!(application.testing_replicator().is_none());
    assert!(matches!(
        application.change_role(ReplicaRole::Primary).await,
        Err(KubericRuntimeError::Closed)
    ));
    assert!(matches!(
        replicator.open().await,
        Err(KubericRuntimeError::Closed)
    ));
    assert!(matches!(
        replicator.current_progress().await,
        Err(KubericRuntimeError::Closed)
    ));
    assert!(matches!(
        replicator.build_replica(target_build()).await,
        Err(KubericRuntimeError::Closed)
    ));
}

async fn assert_held(
    completion: &TestingRuntimeCompletion,
    application: &RuntimeHostApplication,
    root: &Path,
) {
    assert!(!completion.is_finished());
    // Cancel an actual local-drain waiter too. It must not release the cache.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(25),
            application.wait_for_containment()
        )
        .await
        .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), completion.wait())
            .await
            .is_err()
    );
    assert_same_root_owned(root);
}

async fn eventual(completion: &TestingRuntimeCompletion) -> Result<(), RuntimeProcessError> {
    tokio::time::timeout(Duration::from_secs(5), completion.wait())
        .await
        .unwrap()
}

async fn diagnostic(completion: &TestingRuntimeCompletion) -> RuntimeProcessError {
    tokio::time::timeout(Duration::from_secs(5), completion.diagnostic())
        .await
        .unwrap()
}

async fn cancelled_close_keeps_real_started_store_case() {
    let root = tempfile::tempdir_in(".").unwrap();
    let attempt = start_concrete(root.path()).await;
    let replicator = open_concrete(&attempt).await;
    let gate = hold_store(&replicator).await;
    let seed_root = attempt.config.application_root();
    assert_same_root_owned(seed_root);
    // A real bind failure enters the owner's Close path on this same actual
    // application. Its original public host remains explicitly owned below.
    let (waiter, completion) = testing_start_runtime_with_application(
        &attempt.config,
        attempt.application.clone(),
        std::future::pending(),
    )
    .unwrap();
    let waiter = tokio::spawn(waiter);
    tokio::time::timeout(Duration::from_secs(5), async {
        while attempt.application.testing_replicator().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !waiter.is_finished(),
        "Close must wait for the started worker"
    );
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    for _ in 0..3 {
        attempt.application.abort();
    }
    assert_terminal(&attempt.application, &replicator).await;
    assert_held(&completion, &attempt.application, seed_root).await;
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 0);
    gate.release();
    let error = eventual(&completion).await.unwrap_err().to_string();
    assert!(error.contains("Address already in use"), "{error}");
    assert!(error.contains("Close interrupted by Abort"), "{error}");
    assert!(error.contains("waiter cancelled"), "{error}");
    assert_eq!(diagnostic(&completion).await.to_string(), error);
    attempt.application.wait_for_containment().await.unwrap();
    assert_terminal(&attempt.application, &replicator).await;
    drop(SeedDispatchStore::open(seed_root).unwrap());
    attempt.stop.send(Ok(())).unwrap();
    attempt.waiter.await.unwrap().unwrap();
    eventual(&attempt.completion).await.unwrap();
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 0);
    // These executor gates were never entered; explicitly dispose both ends.
    drop((attempt.release, attempt.entered));
}

async fn deadline_keeps_real_started_store_case() {
    let root = tempfile::tempdir_in(".").unwrap();
    let attempt = start_concrete(root.path()).await;
    let replicator = open_concrete(&attempt).await;
    let gate = hold_store(&replicator).await;
    attempt
        .stop
        .send(Err(RuntimeProcessError::new("trigger evidence")))
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(35), attempt.waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err()
        .to_string();
    assert!(error.contains("trigger evidence"), "{error}");
    assert!(
        error.contains("local drainage timed out; containment unconfirmed"),
        "{error}"
    );
    for _ in 0..3 {
        attempt.application.abort();
    }
    assert_terminal(&attempt.application, &replicator).await;
    assert_held(
        &attempt.completion,
        &attempt.application,
        attempt.config.application_root(),
    )
    .await;
    gate.release();
    let final_error = eventual(&attempt.completion).await.unwrap_err().to_string();
    assert_eq!(
        final_error, error,
        "a successful late join must not erase timeout"
    );
    assert_eq!(diagnostic(&attempt.completion).await.to_string(), error);
    for _ in 0..2 {
        attempt.application.wait_for_containment().await.unwrap();
    }
    drop(SeedDispatchStore::open(attempt.config.application_root()).unwrap());
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 0);
    drop((attempt.release, attempt.entered));
}

async fn successful_shutdown_requires_real_store_join_case() {
    let root = tempfile::tempdir_in(".").unwrap();
    let attempt = start_concrete(root.path()).await;
    let replicator = open_concrete(&attempt).await;
    let gate = hold_store(&replicator).await;
    attempt.stop.send(Ok(())).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while attempt.application.testing_replicator().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_terminal(&attempt.application, &replicator).await;
    assert_held(
        &attempt.completion,
        &attempt.application,
        attempt.config.application_root(),
    )
    .await;
    assert!(!attempt.waiter.is_finished());
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), attempt.waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    eventual(&attempt.completion).await.unwrap();
    for _ in 0..3 {
        attempt.application.abort();
        attempt.application.wait_for_containment().await.unwrap();
        assert_terminal(&attempt.application, &replicator).await;
    }
    drop(SeedDispatchStore::open(attempt.config.application_root()).unwrap());
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 0);
    drop((attempt.release, attempt.entered));
}

async fn abort_repolls_inflight_application_close_without_self_deadlock_case() {
    let root = tempfile::tempdir_in(".").unwrap();
    let attempt = start_concrete(root.path()).await;
    let replicator = open_concrete(&attempt).await;
    let gate = hold_store(&replicator).await;
    let mut close = Box::pin(attempt.application.close());
    // The real inner Close now owns the service's Open-attempt mutex.
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut close)
            .await
            .is_err()
    );
    attempt.waiter.abort();
    assert!(attempt.waiter.await.unwrap_err().is_cancelled());
    assert_terminal(&attempt.application, &replicator).await;
    assert_held(
        &attempt.completion,
        &attempt.application,
        attempt.config.application_root(),
    )
    .await;
    gate.release();
    // Repoll the *same* Close after Abort, without a new graceful invocation.
    // Drop a failed waiter and join the owner before asserting, even on failure.
    let closed = tokio::time::timeout(Duration::from_secs(5), close).await;
    let error = eventual(&attempt.completion).await.unwrap_err().to_string();
    assert!(error.contains("waiter cancelled"), "{error}");
    assert_eq!(diagnostic(&attempt.completion).await.to_string(), error);
    attempt.application.wait_for_containment().await.unwrap();
    drop(SeedDispatchStore::open(attempt.config.application_root()).unwrap());
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 0);
    drop((attempt.stop, attempt.release, attempt.entered));
    closed
        .expect("post-Abort Close must drop its old mutex-holding future before local drain")
        .unwrap();
}

async fn cancellation_joins_entered_submission_without_reconciliation_case() {
    let root = tempfile::tempdir_in(".").unwrap();
    let attempt = start_concrete(root.path()).await;
    let replicator = open_concrete(&attempt).await;
    let callback = tokio::spawn({
        let replicator = replicator.clone();
        async move { replicator.build_replica(target_build()).await }
    });
    tokio::time::timeout(Duration::from_secs(5), attempt.entered)
        .await
        .unwrap()
        .unwrap();
    let path = attempt
        .config
        .application_root()
        .join("seed-dispatch-v1.json");
    let before = std::fs::read(&path).unwrap();
    let record: SeedDispatchRecord = serde_json::from_slice(&before).unwrap();
    assert_eq!(record.phase(), SeedDispatchPhase::Submitting);
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 1);
    callback.abort();
    assert!(callback.await.unwrap_err().is_cancelled());
    attempt.waiter.abort();
    assert!(attempt.waiter.await.unwrap_err().is_cancelled());
    for _ in 0..3 {
        attempt.application.abort();
    }
    assert_terminal(&attempt.application, &replicator).await;
    assert_held(
        &attempt.completion,
        &attempt.application,
        attempt.config.application_root(),
    )
    .await;
    assert!(!attempt.executor.completed.load(Ordering::SeqCst));
    attempt.release.send(()).unwrap();
    let error = eventual(&attempt.completion).await.unwrap_err().to_string();
    assert!(error.contains("waiter cancelled"), "{error}");
    assert_eq!(diagnostic(&attempt.completion).await.to_string(), error);
    assert!(attempt.executor.completed.load(Ordering::SeqCst));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "local join is not SQL reconciliation"
    );
    for _ in 0..3 {
        attempt.application.abort();
        attempt.application.wait_for_containment().await.unwrap();
        assert_terminal(&attempt.application, &replicator).await;
    }
    let replacement_store = SeedDispatchStore::open(attempt.config.application_root()).unwrap();
    assert_eq!(
        replacement_store.load().unwrap().unwrap().phase(),
        SeedDispatchPhase::Submitting
    );
    drop(replacement_store);
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 1);
    drop(attempt.stop);
}

async fn real_worker_late_failure_is_reported_case() {
    for bounded in [false, true] {
        let root = tempfile::tempdir_in(".").unwrap();
        let attempt = start_concrete(root.path()).await;
        let replicator = open_concrete(&attempt).await;
        let gate = hold_store(&replicator).await;
        assert_same_root_owned(attempt.config.application_root());
        if bounded {
            attempt
                .stop
                .send(Err(RuntimeProcessError::new("late failure trigger")))
                .unwrap();
            let error = tokio::time::timeout(Duration::from_secs(35), attempt.waiter)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err()
                .to_string();
            assert!(error.contains("late failure trigger"), "{error}");
            assert!(error.contains("local drainage timed out"), "{error}");
        } else {
            attempt.waiter.abort();
            assert!(attempt.waiter.await.unwrap_err().is_cancelled());
            drop(attempt.stop);
        }
        assert_held(
            &attempt.completion,
            &attempt.application,
            attempt.config.application_root(),
        )
        .await;
        // The held, genuinely started Create will encounter an unsafe state
        // file only after the public waiter has gone. No fake drain error.
        let path = attempt
            .config
            .application_root()
            .join("seed-dispatch-v1.json");
        std::fs::create_dir(&path).unwrap();
        gate.release();
        let error = eventual(&attempt.completion).await.unwrap_err().to_string();
        assert!(
            error.contains(&SeedStateError::UnsafeState.to_string()),
            "{error}"
        );
        if bounded {
            assert!(error.contains("late failure trigger"), "{error}");
            assert!(error.contains("local drainage timed out"), "{error}");
            assert!(error.contains("late local drainage"), "{error}");
        } else {
            assert!(error.contains("waiter cancelled"), "{error}");
        }
        assert_eq!(diagnostic(&attempt.completion).await.to_string(), error);
        for _ in 0..2 {
            attempt.application.abort();
            assert!(attempt.application.wait_for_containment().await.is_err());
            assert_terminal(&attempt.application, &replicator).await;
        }
        assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 0);
        std::fs::remove_dir(path).unwrap();
        assert_same_root_owned(attempt.config.application_root());
        // Failed drainage deliberately retains the cache. Release every actual
        // application/replicator owner before checking the physical flock.
        drop(replicator);
        drop(attempt.application);
        drop(SeedDispatchStore::open(attempt.config.application_root()).unwrap());
        drop((attempt.release, attempt.entered));
    }
}

async fn abort_during_actual_open_retains_attempt_until_terminal_refusal_case() {
    let root = tempfile::tempdir_in(".").unwrap();
    let attempt = start_concrete(root.path()).await;
    let (entered, held) = std::sync::mpsc::channel();
    let (release, resumed) = std::sync::mpsc::channel();
    attempt
        .application
        .testing_gate_open_publication(entered, resumed);
    let request = open_request(&attempt).await;
    let (mut client, _) = status(attempt.config.control_address(), 1).await;
    let open = tokio::spawn(async move { client.execute(authorized(request)).await });
    let replicator = held.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(attempt.application.testing_replicator().is_none());
    assert!(
        !open.is_finished(),
        "gate precedes actual Open success publication"
    );
    attempt.waiter.abort();
    assert!(attempt.waiter.await.unwrap_err().is_cancelled());
    for _ in 0..3 {
        attempt.application.abort();
    }
    assert_terminal(&attempt.application, &replicator).await;
    assert!(!attempt.completion.is_finished());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), attempt.completion.wait())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(25),
            attempt.application.wait_for_containment()
        )
        .await
        .is_err()
    );
    release.send(()).unwrap();
    let open_error = tokio::time::timeout(Duration::from_secs(5), open)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(open_error.message().contains("closed"), "{open_error}");
    let error = eventual(&attempt.completion).await.unwrap_err().to_string();
    assert!(error.contains("waiter cancelled"), "{error}");
    assert_eq!(diagnostic(&attempt.completion).await.to_string(), error);
    assert_terminal(&attempt.application, &replicator).await;
    attempt.application.wait_for_containment().await.unwrap();
    assert_eq!(attempt.executor.executions.load(Ordering::SeqCst), 0);
    drop((attempt.stop, attempt.release, attempt.entered));
}

// Reuse the public-host scaffold's serialization, runtime and stack geometry,
// but not its narrowly classified upstream startup retries.
fn run_concrete_test<F, Fut>(factory: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let _guard = HOST_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let result = std::thread::Builder::new()
        .name("mssql-concrete-shutdown-test".into())
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
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[test]
fn concrete_cancelled_close_keeps_real_started_store() {
    run_concrete_test(cancelled_close_keeps_real_started_store_case);
}

#[test]
fn concrete_deadline_keeps_real_started_store() {
    run_concrete_test(deadline_keeps_real_started_store_case);
}

#[test]
fn concrete_successful_shutdown_requires_real_store_join() {
    run_concrete_test(successful_shutdown_requires_real_store_join_case);
}

#[test]
fn concrete_abort_repolls_inflight_application_close_without_self_deadlock() {
    run_concrete_test(abort_repolls_inflight_application_close_without_self_deadlock_case);
}

#[test]
fn concrete_cancellation_joins_entered_submission_without_reconciliation() {
    run_concrete_test(cancellation_joins_entered_submission_without_reconciliation_case);
}

#[test]
fn concrete_real_worker_late_failure_is_reported() {
    run_concrete_test(real_worker_late_failure_is_reported_case);
}

#[test]
fn concrete_abort_during_actual_open_retains_attempt_until_terminal_refusal() {
    run_concrete_test(abort_during_actual_open_retains_attempt_until_terminal_refusal_case);
}
