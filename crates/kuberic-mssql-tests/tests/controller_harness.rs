use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kuberic_controller::test_util::{
    ApplicationFactory, CreatedApplication, ErrorKind,
    ObservationFailureKind as HarnessFailureKind, ReplicaContext, Result as HarnessResult,
    TestCluster, TestError,
};
use kuberic_mssql::kuberic::{
    ObservationClock, SqlServerObservationSource, SqlServerReplicator, SqlServerService,
    SqlServerServiceConfig,
};
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::runtime_error::RuntimeError;
use kuberic_mssql::{Observation, ObservationFailure, ObservationFailureKind};
use kuberic_runtime::StatefulServiceReplica;
use kuberic_runtime::application::INVALID_LSN;
use kuberic_runtime::host::ApplicationStorageState;
use kuberic_runtime::protocol::public_operations::ProgressQueryResult;
use kuberic_runtime::protocol::types::{AccessStatus, ReplicaIdentity, ReplicaRole, ResourceUid};
use kuberic_runtime::replicator::Replicator;
use tokio::sync::oneshot;

const OBSERVED_AT: u64 = 1_000;
const DRAIN_BUDGET: Duration = Duration::from_secs(45);

fn application_error(error: impl std::error::Error + Send + Sync + 'static) -> TestError {
    TestError::with_source(
        ErrorKind::Application,
        "MSSQL pilot application failed",
        error,
    )
}

fn require(condition: bool, message: &str) -> HarnessResult<()> {
    if condition {
        Ok(())
    } else {
        Err(TestError::new(ErrorKind::Application, message))
    }
}

struct CallbackGate {
    entered: oneshot::Sender<()>,
    dropped: oneshot::Sender<()>,
}

struct CallbackDrop(Option<oneshot::Sender<()>>);

impl Drop for CallbackDrop {
    fn drop(&mut self) {
        if let Some(dropped) = self.0.take() {
            let _ = dropped.send(());
        }
    }
}

struct UnavailableSource {
    config: ObserverConfig,
    gate: Mutex<Option<CallbackGate>>,
    observations: AtomicUsize,
}

#[async_trait]
impl SqlServerObservationSource for UnavailableSource {
    fn observer_config(&self) -> &ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        self.observations.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            let _dropped = CallbackDrop(Some(gate.dropped));
            gate.entered
                .send(())
                .expect("callback entry receiver retained");
            std::future::pending::<()>().await;
        }
        Ok(Observation::Failed(ObservationFailure {
            kind: ObservationFailureKind::Unreachable,
            message: "deliberately unavailable server-free native input".into(),
            observed_at_unix_millis: OBSERVED_AT,
        }))
    }
}

struct FixedClock;

impl ObservationClock for FixedClock {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
        Ok(OBSERVED_AT)
    }
}

struct CreatedReplica {
    identity: ReplicaIdentity,
    service: Arc<SqlServerService>,
    source: Arc<UnavailableSource>,
    drained: AtomicBool,
}

#[derive(Clone, Default)]
struct MssqlFactory {
    replicas: Arc<Mutex<Vec<Arc<CreatedReplica>>>>,
}

#[async_trait]
impl ApplicationFactory for MssqlFactory {
    type Service = SqlServerService;

    async fn create(
        &self,
        context: ReplicaContext,
    ) -> HarnessResult<CreatedApplication<Self::Service>> {
        require(
            std::fs::read_dir(context.application_root())?
                .next()
                .transpose()?
                .is_none(),
            "a fresh observer application must have an empty owned root",
        )?;
        let resource_uid = context
            .environment()
            .get("KUBERIC_RESOURCE_UID")
            .ok_or_else(|| {
                TestError::new(ErrorKind::Application, "missing controller resource UID")
            })?;
        let config = SqlServerServiceConfig::new(
            ResourceUid::new(resource_uid.clone()),
            context.replication_address(),
        )
        .map_err(application_error)?;
        let observer = serde_json::to_vec(&serde_json::json!({
            "host": "unused.invalid",
            "port": 1433,
            "availability_group": "pilot-ag",
            "expected_server_name": format!("sql-{}", context.identity().replica_id),
            "replica_id": context.identity().replica_id.to_string(),
            "incarnation": context.identity().instance_id.to_string(),
            "observer_username_file": "/unused/username",
            "observer_password_file": "/unused/password",
            "max_age_ms": 100
        }))
        .map_err(application_error)?;
        let source = Arc::new(UnavailableSource {
            config: ObserverConfig::from_json(&observer).map_err(application_error)?,
            gate: Mutex::new(None),
            observations: AtomicUsize::new(0),
        });
        let service = Arc::new(SqlServerService::with_observation_source(
            config,
            source.clone(),
            Arc::new(FixedClock),
        ));
        self.replicas.lock().unwrap().push(Arc::new(CreatedReplica {
            identity: context.identity().clone(),
            service: service.clone(),
            source,
            drained: AtomicBool::new(false),
        }));
        Ok(CreatedApplication::new(
            service,
            ApplicationStorageState::FreshEmpty,
        ))
    }

    async fn shutdown_and_drain(&self, service: Arc<Self::Service>) -> HarnessResult<()> {
        let replica = self
            .replicas
            .lock()
            .unwrap()
            .iter()
            .find(|replica| Arc::ptr_eq(&replica.service, &service))
            .cloned()
            .ok_or_else(|| TestError::new(ErrorKind::Application, "unowned MSSQL service"))?;
        service.abort();
        service
            .wait_for_containment()
            .await
            .map_err(application_error)?;
        replica.drained.store(true, Ordering::Release);
        Ok(())
    }
}

async fn open_observers(
    cluster: &mut TestCluster<MssqlFactory>,
    factory: &MssqlFactory,
) -> HarnessResult<(Arc<CreatedReplica>, Arc<SqlServerReplicator>)> {
    loop {
        cluster.step().await?;
        let observation = cluster.observe().await?;
        require(
            observation
                .failures()
                .iter()
                .all(|failure| failure.kind() == HarnessFailureKind::PendingStartup),
            "report acquisition failed, rather than a native query",
        )?;
        if observation.reports().count() != 3
            || !observation
                .reports()
                .all(|(_, report)| report.has_current_open_endpoint())
        {
            continue;
        }
        let reconcile_errors_before = observation.reconcile_errors();
        cluster.step().await?;
        let observation = cluster.observe().await?;
        require(
            observation.reconcile_errors() == reconcile_errors_before,
            "post-Open reconciliation failed instead of evaluating unavailable native evidence",
        )?;
        require(
            observation.reports().count() == 3
                && observation
                    .reports()
                    .all(|(_, report)| report.has_current_open_endpoint()),
            "reconciliation must retain all three exact open observers",
        )?;
        require(
            observation.failures().is_empty(),
            "an open host is unavailable",
        )?;
        for (reference, report) in observation.reports() {
            report
                .validate()
                .map_err(|message| TestError::new(ErrorKind::Application, message))?;
            require(
                &report.replica == reference.identity()
                    && report.process_session_id == *reference.process_session_id()
                    && report.storage_incarnation == *reference.storage_incarnation(),
                "Open evidence must describe the exact observed host",
            )?;
            require(
                matches!(&report.progress.current_progress, ProgressQueryResult::Failed(message)
                    if message.contains("current_progress is disabled by the observe-only SQL Server adapter")),
                "current progress must retain the explicit unsupported MSSQL result",
            )?;
            require(
                matches!(&report.progress.catch_up_capability, ProgressQueryResult::Value(value)
                    if *value == INVALID_LSN),
                "capability must remain explicitly unknown, not a retained floor",
            )?;
            require(
                report.role == ReplicaRole::None
                    && report.configuration.is_none()
                    && report.read_status != AccessStatus::Granted
                    && report.write_status != AccessStatus::Granted,
                "unavailable native evidence must not admit configuration or serving access",
            )?;
        }
        let reference = observation.replicas().next().unwrap();
        let service = cluster.service(reference).await?;
        let replica = factory
            .replicas
            .lock()
            .unwrap()
            .iter()
            .find(|replica| {
                replica.identity == *reference.identity() && Arc::ptr_eq(&replica.service, &service)
            })
            .cloned()
            .ok_or_else(|| {
                TestError::new(
                    ErrorKind::Application,
                    "host did not retain the factory service",
                )
            })?;
        let child = service.replicator().ok_or_else(|| {
            TestError::new(ErrorKind::Application, "successful Open has no MSSQL child")
        })?;
        return Ok((replica, child));
    }
}

#[test]
fn controller_opens_unsupported_mssql_observers_and_drains_native_work() {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(exercise_controller());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn exercise_controller() {
    let factory = MssqlFactory::default();
    let mut cluster = TestCluster::builder(factory.clone())
        .name("mssql-observer-pilot")
        .start()
        .await
        .unwrap();
    let mut callback = None;
    let mut cancelled = None;
    let mut retained_child = None;
    let scenario = async {
        let (replica, child) = open_observers(&mut cluster, &factory).await?;
        require(
            matches!(child.current_progress().await,
                Err(kuberic_runtime::RuntimeError::Application(message))
                    if message == "current_progress is disabled by the observe-only SQL Server adapter"),
            "open current progress must retain its unsupported operation",
        )?;
        require(
            child.catch_up_capability().await.map_err(application_error)? == INVALID_LSN,
            "open capability must report unknown without a retained-range claim",
        )?;
        require(
            factory
                .replicas
                .lock()
                .unwrap()
                .iter()
                .all(|replica| replica.source.observations.load(Ordering::SeqCst) == 0),
            "public reporting and queries must not observe SQL",
        )?;
        let (entered, entry) = oneshot::channel();
        let (dropped, cancellation) = oneshot::channel();
        *replica.source.gate.lock().unwrap() = Some(CallbackGate { entered, dropped });
        let querying = child.clone();
        // Independently native work, not a successful public query or admission.
        callback = Some(tokio::spawn(async move {
            querying.native_configuration_sequence().await
        }));
        retained_child = Some(child);
        cancelled = Some(cancellation);
        cluster
            .probe(async { entry.await.map_err(application_error) })
            .await?;
        Ok::<(), TestError>(())
    }
    .await;

    let finished = cluster.finish(scenario).await;
    let cancelled_at_finish = cancelled.as_mut().map(oneshot::Receiver::try_recv);
    let callback_result = if let Some(mut callback) = callback {
        match tokio::time::timeout(DRAIN_BUDGET, &mut callback).await {
            Ok(result) => Some(result),
            Err(error) => {
                callback.abort();
                let joined = callback.await;
                panic!(
                    "callback owner did not join: {error}; finish: {finished:?}; abort: {joined:?}"
                );
            }
        }
    } else {
        None
    };
    finished.expect("scenario and owned harness/factory cleanup must both succeed");
    assert_eq!(cancelled_at_finish, Some(Ok(())));
    assert!(matches!(
        callback_result,
        Some(Ok(Err(kuberic_mssql::kuberic::KubericAdapterError::Closed)))
    ));

    let replicas = factory.replicas.lock().unwrap().clone();
    assert_eq!(replicas.len(), 3);
    assert_eq!(
        replicas
            .iter()
            .map(|replica| replica.source.observations.load(Ordering::SeqCst))
            .sum::<usize>(),
        1,
        "only the separately owned native callback may observe SQL"
    );
    for replica in replicas {
        assert!(replica.drained.load(Ordering::Acquire));
        tokio::time::timeout(DRAIN_BUDGET, replica.service.wait_for_containment())
            .await
            .unwrap()
            .unwrap();
    }
    let child = retained_child.unwrap();
    tokio::time::timeout(DRAIN_BUDGET, child.wait_for_containment())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        child.current_progress().await,
        Err(kuberic_runtime::RuntimeError::Closed)
    ));
    assert!(matches!(
        child.catch_up_capability().await,
        Err(kuberic_runtime::RuntimeError::Closed)
    ));
}
