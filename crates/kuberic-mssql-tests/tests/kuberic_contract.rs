use std::collections::VecDeque;
use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kuberic_mssql::kuberic::{
    KubericAdapterError, ObservationClock, SqlServerBootstrapConfig, SqlServerObservationSource,
    SqlServerReplicator, SqlServerReplicatorFactory, SqlServerService, SqlServerServiceConfig,
    SqlServerStartIncarnation, TargetHostEvidenceExchange, TargetHostEvidenceReport,
    TargetHostEvidenceRequest,
};
use kuberic_mssql::observation::{
    AutomaticSeedingSnapshot, AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, DatabaseSnapshot,
    InstanceMetadata, InstanceSnapshot, LocalDatabaseSnapshot, LocalRecoveryMetadata,
    LocalReplicaSnapshot, NativeProvenance, RecoveryLineageObservation, ReplicaSnapshot,
    ReplicaState,
};
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::runtime_error::RuntimeError;
use kuberic_mssql::runtime_host::{
    SeedDispatchPhase, SeedDispatchRecord, SeedDispatchStore, SeedDispatchTarget,
};
use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, AvailabilityGroupOwnership,
    BootstrapOwnership, BootstrapOwnershipError, ConfigurationSequence, DatabaseIdentity,
    DatabaseLineage, DecimalProgress, Endpoint, Guid, MutationEffect, NativeConvergenceState,
    NativeProgress, NativeRole, Observation, ObservationFailure, ObservationFailureKind,
    ReplicaDescriptor, ReplicaIdentity, SeedDispatchEvidence, SeedDispatchRequest, ServerName,
    SqlIdentifier, SqlMutationExecutor, SqlServerTopologyExpectation,
    SqlServerTopologyMemberExpectation, StandaloneDatabaseIdentity, StandaloneDatabaseOwnership,
    StandaloneDatabaseSnapshot, classify_availability_group, classify_bootstrap_ownership,
    classify_owned_availability_group, classify_replica_join, classify_replica_seed,
    source_database_creation_token,
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

fn private_seed_root() -> std::path::PathBuf {
    let root = tempfile::tempdir().unwrap().keep();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    root
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

struct ScriptedMutationExecutor {
    databases: Mutex<VecDeque<Result<Option<StandaloneDatabaseSnapshot>, RuntimeError>>>,
    effects: Mutex<Vec<&'static str>>,
    executions: Mutex<VecDeque<Result<(), RuntimeError>>>,
}

struct SeedMutationExecutor {
    complete: Arc<AtomicBool>,
    executions: AtomicUsize,
}

struct GatedSeedMutationExecutor {
    complete: Arc<AtomicBool>,
    executions: AtomicUsize,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

struct SubmittedThenFailedSeedExecutor {
    executions: AtomicUsize,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

struct AcceptedReplyLostSeedExecutor {
    complete: Arc<AtomicBool>,
    executions: AtomicUsize,
}

struct RejectOnceSeedExecutor {
    complete: Arc<AtomicBool>,
    executions: AtomicUsize,
}

struct TerminalSeedMutationExecutor {
    associated: Arc<AtomicBool>,
    terminal: Arc<AtomicBool>,
    executions: AtomicUsize,
}

struct TerminalSeedSource {
    config: ObserverConfig,
    associated: Arc<AtomicBool>,
    replaced_group: Arc<AtomicBool>,
}

struct SeedTargetEvidenceExchange {
    complete: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy)]
enum ControlledTargetEvidenceMode {
    Healthy,
    ReplayPreviousAuthority,
    Stale,
    SqlRestarted,
    LineageMismatch,
}

struct ControlledTargetEvidenceExchange {
    modes: Mutex<VecDeque<ControlledTargetEvidenceMode>>,
    previous_request: Mutex<Option<TargetHostEvidenceRequest>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl ControlledTargetEvidenceExchange {
    fn new(modes: impl IntoIterator<Item = ControlledTargetEvidenceMode>) -> Self {
        Self {
            modes: Mutex::new(modes.into_iter().collect()),
            previous_request: Mutex::new(None),
            entered: Mutex::new(None),
            release: Mutex::new(None),
        }
    }

    fn with_gate(
        modes: impl IntoIterator<Item = ControlledTargetEvidenceMode>,
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            modes: Mutex::new(modes.into_iter().collect()),
            previous_request: Mutex::new(None),
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(Some(release)),
        }
    }
}

#[async_trait]
impl TargetHostEvidenceExchange for ControlledTargetEvidenceExchange {
    async fn collect(
        &self,
        request: &TargetHostEvidenceRequest,
    ) -> Result<TargetHostEvidenceReport, KubericAdapterError> {
        let entered = self.entered.lock().unwrap().take();
        let release = self.release.lock().unwrap().take();
        if let Some(entered) = entered {
            let _ = entered.send(());
        }
        if let Some(release) = release {
            release.await.map_err(|_| KubericAdapterError::Closed)?;
        }
        let mode = self
            .modes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ControlledTargetEvidenceMode::Healthy);
        let report_request =
            if matches!(mode, ControlledTargetEvidenceMode::ReplayPreviousAuthority) {
                self.previous_request.lock().unwrap().clone().ok_or(
                    KubericAdapterError::NativeConvergenceUnsafe(
                        "server-free prior target evidence",
                    ),
                )?
            } else {
                let current = request.clone();
                *self.previous_request.lock().unwrap() = Some(current.clone());
                current
            };
        let target_index = match request.target_identity().replica_id.value() {
            2 => 1,
            3 => 2,
            _ => {
                return Err(KubericAdapterError::NativeConvergenceConflict(
                    "server-free target evidence identity",
                ));
            }
        };
        let mut snapshot = bound_snapshot_for(target_index);
        let observed_at = if matches!(mode, ControlledTargetEvidenceMode::Stale) {
            request.requested_at_unix_millis().saturating_sub(1)
        } else {
            request.requested_at_unix_millis()
        };
        snapshot.observed_at_unix_millis = observed_at;
        if let Observation::Present {
            value: group,
            observed_at_unix_millis,
        } = &mut snapshot.availability_group
        {
            *observed_at_unix_millis = observed_at;
            if matches!(mode, ControlledTargetEvidenceMode::LineageMismatch) {
                let alternate = guid("99999999-9999-4999-8999-999999999999");
                let database = group.databases.first_mut().unwrap();
                database
                    .local
                    .as_mut()
                    .unwrap()
                    .recovery
                    .as_mut()
                    .unwrap()
                    .recovery_fork_guid = Some(alternate.clone());
                let local_native = group
                    .local_replica
                    .identity
                    .native_replica_id()
                    .unwrap()
                    .clone();
                let local = database
                    .replicas
                    .iter_mut()
                    .find(|state| state.replica_id == local_native)
                    .unwrap();
                if let RecoveryLineageObservation::Local { value } = &mut local.lineage {
                    value.recovery_fork_id = alternate;
                }
            }
        }
        let frozen =
            SqlServerStartIncarnation::new(snapshot.instance.sqlserver_start_time.clone()).unwrap();
        if matches!(mode, ControlledTargetEvidenceMode::SqlRestarted) {
            snapshot.instance.sqlserver_start_time = "2099-01-01T00:00:00".into();
        }
        Ok(TargetHostEvidenceReport::new(
            report_request,
            frozen,
            snapshot,
        ))
    }
}

#[async_trait]
impl TargetHostEvidenceExchange for SeedTargetEvidenceExchange {
    async fn collect(
        &self,
        request: &TargetHostEvidenceRequest,
    ) -> Result<TargetHostEvidenceReport, KubericAdapterError> {
        if !self.complete.load(Ordering::SeqCst) {
            return Err(KubericAdapterError::NativeConvergenceUnsafe(
                "server-free target evidence requested before native completion",
            ));
        }
        let target_index = match request.target_identity().replica_id.value() {
            2 => 1,
            3 => 2,
            _ => {
                return Err(KubericAdapterError::NativeConvergenceConflict(
                    "server-free target evidence identity",
                ));
            }
        };
        let mut snapshot = bound_snapshot_for(target_index);
        snapshot.observed_at_unix_millis = request.requested_at_unix_millis();
        if let Observation::Present {
            observed_at_unix_millis,
            ..
        } = &mut snapshot.availability_group
        {
            *observed_at_unix_millis = request.requested_at_unix_millis();
        }
        let frozen =
            SqlServerStartIncarnation::new(snapshot.instance.sqlserver_start_time.clone()).unwrap();
        Ok(TargetHostEvidenceReport::new(
            request.clone(),
            frozen,
            snapshot,
        ))
    }
}

fn seed_terminal_evidence(complete: &AtomicBool) -> SeedDispatchEvidence {
    if complete.load(Ordering::SeqCst) {
        SeedDispatchEvidence::Terminal
    } else {
        SeedDispatchEvidence::Absent
    }
}

#[async_trait]
impl SqlMutationExecutor for TerminalSeedMutationExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        Ok(Some(owned_standalone_database()))
    }

    async fn reconcile_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        Ok(if self.terminal.load(Ordering::SeqCst) {
            SeedDispatchEvidence::Terminal
        } else {
            SeedDispatchEvidence::Absent
        })
    }

    async fn submit_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.terminal.store(true, Ordering::SeqCst);
        self.associated.store(true, Ordering::SeqCst);
        Ok(SeedDispatchEvidence::Terminal)
    }

    async fn execute(&self, _effect: &MutationEffect) -> Result<(), RuntimeError> {
        unreachable!("terminal seed executor uses submit_seed_dispatch")
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        Ok(effect.label() == "grant create any database")
    }
}

#[async_trait]
impl SqlServerObservationSource for TerminalSeedSource {
    fn observer_config(&self) -> &ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        let mut snapshot = if self.associated.load(Ordering::SeqCst) {
            seeded_source_snapshot()
        } else {
            bootstrap_membership_snapshot(0)
        };
        if self.replaced_group.load(Ordering::SeqCst)
            && let Observation::Present { value: group, .. } = &mut snapshot.availability_group
        {
            group.identity.group_id = guid("99999999-9999-4999-8999-999999999999");
        }
        Ok(present(snapshot))
    }
}

#[async_trait]
impl SqlMutationExecutor for GatedSeedMutationExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        Ok(Some(owned_standalone_database()))
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError> {
        assert_eq!(effect.label(), "add database to availability group");
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.entered
            .lock()
            .unwrap()
            .take()
            .expect("seed dispatch gate must be entered once")
            .send(())
            .expect("seed dispatch test must await the gate");
        let release = self
            .release
            .lock()
            .unwrap()
            .take()
            .expect("seed dispatch gate must be released once");
        release
            .await
            .expect("seed dispatch test must release the gate");
        self.complete.store(true, Ordering::SeqCst);
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
        Ok(seed_terminal_evidence(&self.complete))
    }
}

#[async_trait]
impl SqlMutationExecutor for SubmittedThenFailedSeedExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        Ok(Some(owned_standalone_database()))
    }

    async fn submit_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.entered
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let release = self.release.lock().unwrap().take().unwrap();
        let _ = release.await;
        Err(RuntimeError::new(
            ObservationFailureKind::Unreachable,
            "seed ADD DATABASE submission",
            "SQL Server transport failed",
        ))
    }

    async fn execute(&self, _effect: &MutationEffect) -> Result<(), RuntimeError> {
        unreachable!("seed protocol executor uses submit_seed_dispatch")
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        Ok(effect.label() == "grant create any database")
    }
}

#[async_trait]
impl SqlMutationExecutor for AcceptedReplyLostSeedExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        Ok(Some(owned_standalone_database()))
    }

    async fn submit_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.complete.store(true, Ordering::SeqCst);
        Err(RuntimeError::new(
            ObservationFailureKind::Unreachable,
            "seed ADD DATABASE submission",
            "SQL Server transport failed",
        ))
    }

    async fn execute(&self, _effect: &MutationEffect) -> Result<(), RuntimeError> {
        unreachable!("seed protocol executor uses submit_seed_dispatch")
    }

    async fn reconcile_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        Ok(seed_terminal_evidence(&self.complete))
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        Ok(effect.label() == "grant create any database")
    }
}

#[async_trait]
impl SqlMutationExecutor for RejectOnceSeedExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        Ok(Some(owned_standalone_database()))
    }

    async fn submit_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        let attempt = self.executions.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            let mut error = RuntimeError::new(
                ObservationFailureKind::Unsupported,
                "seed ADD DATABASE submission",
                "SQL Server rejected the observation query",
            );
            error.server_code = Some(51001);
            Err(error)
        } else {
            self.complete.store(true, Ordering::SeqCst);
            Ok(SeedDispatchEvidence::Terminal)
        }
    }

    async fn reconcile_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        Ok(seed_terminal_evidence(&self.complete))
    }

    async fn execute(&self, _effect: &MutationEffect) -> Result<(), RuntimeError> {
        unreachable!("seed protocol executor uses submit_seed_dispatch")
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        Ok(effect.label() == "grant create any database")
    }
}

#[async_trait]
impl SqlMutationExecutor for SeedMutationExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        Ok(Some(owned_standalone_database()))
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError> {
        assert_eq!(effect.label(), "add database to availability group");
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.complete.store(true, Ordering::SeqCst);
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
        Ok(seed_terminal_evidence(&self.complete))
    }
}

struct SeedSource {
    config: ObserverConfig,
    complete: Arc<AtomicBool>,
}

struct ControlledSeedSource {
    config: ObserverConfig,
    associated: Arc<AtomicBool>,
    associated_snapshot: InstanceSnapshot,
}

#[async_trait]
impl SqlServerObservationSource for SeedSource {
    fn observer_config(&self) -> &ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        let snapshot = if self.complete.load(Ordering::SeqCst) {
            seeded_source_snapshot()
        } else {
            bootstrap_membership_snapshot(0)
        };
        Ok(present(snapshot))
    }
}

#[async_trait]
impl SqlServerObservationSource for ControlledSeedSource {
    fn observer_config(&self) -> &ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        Ok(present(if self.associated.load(Ordering::SeqCst) {
            self.associated_snapshot.clone()
        } else {
            bootstrap_membership_snapshot(0)
        }))
    }
}

#[async_trait]
impl SqlMutationExecutor for ScriptedMutationExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        self.databases
            .lock()
            .unwrap()
            .pop_front()
            .expect("test must provide one standalone database sample")
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError> {
        self.effects.lock().unwrap().push(effect.label());
        self.executions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(()))
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
        Ok(SeedDispatchEvidence::Terminal)
    }
}

struct GatedMutationExecutor {
    databases: Mutex<VecDeque<Result<Option<StandaloneDatabaseSnapshot>, RuntimeError>>>,
    effects: Mutex<Vec<&'static str>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait]
impl SqlMutationExecutor for GatedMutationExecutor {
    async fn observe_standalone_database(
        &self,
        _database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        self.databases
            .lock()
            .unwrap()
            .pop_front()
            .expect("test must provide one standalone database sample")
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError> {
        self.effects.lock().unwrap().push(effect.label());
        self.entered
            .lock()
            .unwrap()
            .take()
            .expect("mutation gate must be entered once")
            .send(())
            .expect("test must wait for mutation gate");
        let release = self
            .release
            .lock()
            .unwrap()
            .take()
            .expect("mutation gate must be released once");
        release.await.expect("test must release mutation gate");
        Ok(())
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        Ok(effect.label() == "grant create any database")
    }
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
    observer_config_for_timeout(index, 1000)
}

fn observer_config_for_timeout(index: usize, sample_timeout_ms: u64) -> ObserverConfig {
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
                "sample_timeout_ms":{sample_timeout_ms},
                "connect_timeout_ms":{sample_timeout_ms},
                "query_timeout_ms":{sample_timeout_ms},
                "poll_interval_ms":1,
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

fn desired_descriptors() -> Vec<ReplicaDescriptor> {
    (0..3)
        .map(|index| ReplicaDescriptor {
            identity: ReplicaIdentity::desired(format!("logical-{index}"), format!("pod-{index}"))
                .unwrap(),
            server_name: ServerName::new(format!("sql-{index}")).unwrap(),
            endpoint: Endpoint::new(format!("sql-{index}"), 5022).unwrap(),
        })
        .collect()
}

#[test]
fn bootstrap_ownership_is_derived_from_callback_configuration() {
    let current = bound_replica_set();
    assert_eq!(
        classify_bootstrap_ownership(&kuberic_identity(1), &current),
        Ok(BootstrapOwnership::PrimaryTopology)
    );
    assert_eq!(
        classify_bootstrap_ownership(&kuberic_identity(2), &current),
        Ok(BootstrapOwnership::LocalJoin)
    );
    assert_eq!(
        classify_bootstrap_ownership(&kuberic_identity(9), &current),
        Err(BootstrapOwnershipError::LocalMemberMissing)
    );
}

#[test]
fn native_bootstrap_classifiers_cover_every_operation_state() {
    let descriptors = desired_descriptors();
    let database_name = SqlIdentifier::new("app-db").unwrap();
    let database_identity = owned_standalone_database().identity.unwrap();
    assert_eq!(
        classify_availability_group(
            None,
            &AvailabilityGroupName::new("test-ag").unwrap(),
            None,
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Absent
    );
    let complete = bound_snapshot();
    let complete_group = match &complete.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    assert_eq!(
        classify_availability_group(
            Some(complete_group),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Complete
    );
    let mut no_database = complete_group.clone();
    no_database.databases.clear();
    assert_eq!(
        classify_availability_group(
            Some(&no_database),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            None,
            false,
        ),
        NativeConvergenceState::Complete
    );
    assert_eq!(
        classify_availability_group(
            Some(complete_group),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            None,
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    let mut unrelated_database = complete_group.clone();
    unrelated_database.databases[0].identity.name = SqlIdentifier::new("other-db").unwrap();
    assert_eq!(
        classify_availability_group(
            Some(&unrelated_database),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    let mut foreign_database = complete_group.clone();
    foreign_database.databases[0]
        .local
        .as_mut()
        .unwrap()
        .recovery
        .as_mut()
        .unwrap()
        .family_guid = Some(guid(AG_ID));
    assert_eq!(
        classify_availability_group(
            Some(&foreign_database),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    let mut multiple_databases = complete_group.clone();
    multiple_databases
        .databases
        .push(multiple_databases.databases[0].clone());
    assert_eq!(
        classify_availability_group(
            Some(&multiple_databases),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    let mut incomplete = complete_group.clone();
    incomplete.replicas.pop();
    assert_eq!(
        classify_availability_group(
            Some(&incomplete),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Incomplete
    );
    let mut conflicting = complete_group.clone();
    conflicting.replicas[0].seeding_mode = "MANUAL".into();
    assert_eq!(
        classify_availability_group(
            Some(&conflicting),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    let mut foreign_member = complete_group.clone();
    foreign_member.replicas[2] = replica(THIRD_ID, "foreign-sql");
    assert_eq!(
        classify_availability_group(
            Some(&foreign_member),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    assert_eq!(
        classify_availability_group(
            Some(complete_group),
            &AvailabilityGroupName::new("test-ag").unwrap(),
            Some(&guid(AG_ID)),
            &descriptors,
            &database_name,
            Some(&database_identity),
            true,
        ),
        NativeConvergenceState::Unsafe
    );

    let secondary = bound_snapshot_for(1);
    let secondary_group = match &secondary.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    assert_eq!(
        classify_replica_join(
            None,
            &complete_group.identity,
            &sql_identity(1),
            &ServerName::new("sql-1").unwrap(),
            "TCP://sql-1:5022",
            false,
        ),
        NativeConvergenceState::Absent
    );
    assert_eq!(
        classify_replica_join(
            Some(secondary_group),
            &complete_group.identity,
            &sql_identity(1),
            &ServerName::new("sql-1").unwrap(),
            "TCP://sql-1:5022",
            false,
        ),
        NativeConvergenceState::Complete
    );
    let mut join_incomplete = secondary_group.clone();
    join_incomplete.local_replica.state_available = false;
    join_incomplete.local_replica.role = None;
    assert_eq!(
        classify_replica_join(
            Some(&join_incomplete),
            &complete_group.identity,
            &sql_identity(1),
            &ServerName::new("sql-1").unwrap(),
            "TCP://sql-1:5022",
            false,
        ),
        NativeConvergenceState::Incomplete
    );
    let mut join_conflict = secondary_group.clone();
    join_conflict.replicas[1].endpoint_url = Some("TCP://other:5022".into());
    assert_eq!(
        classify_replica_join(
            Some(&join_conflict),
            &complete_group.identity,
            &sql_identity(1),
            &ServerName::new("sql-1").unwrap(),
            "TCP://sql-1:5022",
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    assert_eq!(
        classify_replica_join(
            Some(secondary_group),
            &complete_group.identity,
            &sql_identity(1),
            &ServerName::new("sql-1").unwrap(),
            "TCP://sql-1:5022",
            true,
        ),
        NativeConvergenceState::Unsafe
    );
    for wrong_group in [
        AvailabilityGroupIdentity {
            name: AvailabilityGroupName::new("other-ag").unwrap(),
            group_id: complete_group.identity.group_id.clone(),
        },
        AvailabilityGroupIdentity {
            name: complete_group.identity.name.clone(),
            group_id: guid(FORK_ID),
        },
    ] {
        assert_eq!(
            classify_replica_join(
                Some(secondary_group),
                &wrong_group,
                &sql_identity(1),
                &ServerName::new("sql-1").unwrap(),
                "TCP://sql-1:5022",
                false,
            ),
            NativeConvergenceState::Conflicting
        );
    }

    let lineage = expected_lineage();
    assert_eq!(
        classify_replica_seed(
            Some(secondary_group),
            &complete_group.identity,
            &lineage.database,
            &lineage,
            &guid(SECOND_ID),
            false,
        ),
        NativeConvergenceState::Complete
    );
    let mut seed_incomplete = secondary_group.clone();
    seed_incomplete.databases[0].replicas[0].synchronization_state = None;
    assert_eq!(
        classify_replica_seed(
            Some(&seed_incomplete),
            &complete_group.identity,
            &lineage.database,
            &lineage,
            &guid(SECOND_ID),
            false,
        ),
        NativeConvergenceState::Incomplete
    );
    let mut seed_conflict = secondary_group.clone();
    seed_conflict.databases[0].replicas[0].lineage = RecoveryLineageObservation::Local {
        value: DatabaseLineage {
            database: lineage.database.clone(),
            recovery_fork_id: guid(AG_ID),
        },
    };
    assert_eq!(
        classify_replica_seed(
            Some(&seed_conflict),
            &complete_group.identity,
            &lineage.database,
            &lineage,
            &guid(SECOND_ID),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    let mut seed_unsafe = secondary_group.clone();
    seed_unsafe
        .automatic_seeding
        .push(AutomaticSeedingSnapshot {
            group_database_id: lineage.database.group_database_id.clone(),
            remote_replica_id: guid(SECOND_ID),
            operation_id: guid(AG_ID),
            is_source: true,
            current_state: Some("FAILED".into()),
            performed_seeding: Some(false),
            failure_state: Some(1),
            error_code: Some(1),
            number_of_attempts: Some(1),
            start_time: None,
            completion_time: None,
        });
    assert_eq!(
        classify_replica_seed(
            Some(&seed_unsafe),
            &complete_group.identity,
            &lineage.database,
            &lineage,
            &guid(SECOND_ID),
            false,
        ),
        NativeConvergenceState::Unsafe
    );
    for wrong_group in [
        AvailabilityGroupIdentity {
            name: AvailabilityGroupName::new("other-ag").unwrap(),
            group_id: complete_group.identity.group_id.clone(),
        },
        AvailabilityGroupIdentity {
            name: complete_group.identity.name.clone(),
            group_id: guid(FORK_ID),
        },
    ] {
        assert_eq!(
            classify_replica_seed(
                Some(secondary_group),
                &wrong_group,
                &lineage.database,
                &lineage,
                &guid(SECOND_ID),
                false,
            ),
            NativeConvergenceState::Conflicting
        );
    }
    assert_eq!(
        classify_replica_seed(
            None,
            &complete_group.identity,
            &lineage.database,
            &lineage,
            &guid(SECOND_ID),
            false,
        ),
        NativeConvergenceState::Absent
    );
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

fn bootstrap_absent_snapshot(local_index: usize) -> InstanceSnapshot {
    let mut sample = bound_snapshot_for(local_index);
    sample.availability_group = Observation::Absent {
        observed_at_unix_millis: OBSERVED_AT,
    };
    sample
}

fn bootstrap_membership_snapshot(local_index: usize) -> InstanceSnapshot {
    let mut sample = bound_snapshot_for(local_index);
    let group = match &mut sample.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    group.databases.clear();
    if local_index == 0 {
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
    }
    sample
}

fn bootstrap_membership_snapshot_with_sequence(
    local_index: usize,
    sequence: &str,
) -> InstanceSnapshot {
    let mut sample = bootstrap_membership_snapshot(local_index);
    match &mut sample.availability_group {
        Observation::Present { value, .. } => {
            value.configuration_sequence = ConfigurationSequence::parse(sequence).unwrap();
        }
        _ => unreachable!(),
    }
    sample
}

fn seeded_source_snapshot() -> InstanceSnapshot {
    let mut sample = bootstrap_membership_snapshot(0);
    let source_database = match bound_snapshot_for(0).availability_group {
        Observation::Present { value, .. } => value.databases[0].clone(),
        _ => unreachable!(),
    };
    let group = match &mut sample.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    group.databases.push(source_database);
    let database = group.databases.first_mut().unwrap();
    for (index, native_id) in [SECOND_ID, THIRD_ID].into_iter().enumerate() {
        database.replicas.push(DatabaseReplicaSnapshot {
            group_database_id: guid(DATABASE_ID),
            replica_id: guid(native_id),
            database_id: 5,
            provenance: NativeProvenance::PrimaryRemote,
            lineage: RecoveryLineageObservation::RemoteUnavailable,
            is_primary_replica: Some(false),
            synchronization_state: Some("SYNCHRONIZED".into()),
            synchronization_health: Some("HEALTHY".into()),
            database_state: None,
            is_suspended: Some(false),
            suspend_reason: None,
            is_commit_participant: Some(true),
            progress: NativeProgress {
                hardened_block: Some(DecimalProgress::parse("100").unwrap()),
                redone_record: Some(DecimalProgress::parse("100").unwrap()),
                committed_record: Some(DecimalProgress::parse("100").unwrap()),
            },
        });
        group.automatic_seeding.push(AutomaticSeedingSnapshot {
            group_database_id: guid(DATABASE_ID),
            remote_replica_id: guid(native_id),
            operation_id: guid(
                [
                    "99999999-9999-4999-8999-999999999999",
                    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                ][index],
            ),
            is_source: true,
            current_state: Some("COMPLETED".into()),
            performed_seeding: Some(true),
            failure_state: Some(0),
            error_code: Some(0),
            number_of_attempts: Some(1),
            start_time: Some("2026-10-08T23:00:00".into()),
            completion_time: Some("2026-10-08T23:00:01".into()),
        });
    }
    sample
}

fn owned_standalone_database() -> StandaloneDatabaseSnapshot {
    let identity = StandaloneDatabaseIdentity {
        name: SqlIdentifier::new("app-db").unwrap(),
        database_guid: guid(DATABASE_GUID),
        family_guid: guid(FAMILY_GUID),
        recovery_fork_guid: guid(FORK_ID),
    };
    let ownership = StandaloneDatabaseOwnership {
        resource_id: "resource-a".into(),
        configuration_id: configuration().configuration_id.to_string(),
    };
    StandaloneDatabaseSnapshot {
        identity: Some(identity.clone()),
        creation_token: Some(source_database_creation_token(&identity.name, &ownership)),
        ownership: Some(ownership),
        availability_group_ownership: Some(AvailabilityGroupOwnership {
            resource_id: "resource-a".into(),
            configuration_id: configuration().configuration_id.to_string(),
            name: AvailabilityGroupName::new("test-ag").unwrap(),
            creation_group_id: Some(guid(AG_ID)),
            group_id: Some(guid(AG_ID)),
        }),
        state: "ONLINE".into(),
        recovery_model: "FULL".into(),
        accessible: true,
        marker_table_ready: true,
        full_backup_ready: true,
        log_chain_ready: true,
        backup_verified: true,
    }
}

fn owned_standalone_database_without_group_intent() -> StandaloneDatabaseSnapshot {
    let mut snapshot = owned_standalone_database();
    snapshot.availability_group_ownership = None;
    snapshot
}

fn owned_standalone_database_with_group_intent() -> StandaloneDatabaseSnapshot {
    let mut snapshot = owned_standalone_database();
    snapshot
        .availability_group_ownership
        .as_mut()
        .unwrap()
        .group_id = None;
    snapshot
        .availability_group_ownership
        .as_mut()
        .unwrap()
        .creation_group_id = None;
    snapshot
}

#[test]
fn availability_group_ownership_classifier_rejects_unmarked_recreated_and_secondary_forks() {
    let descriptors = desired_descriptors();
    let database_name = SqlIdentifier::new("app-db").unwrap();
    let database_identity = owned_standalone_database().identity.unwrap();
    let expected_group_ownership = AvailabilityGroupOwnership {
        resource_id: "resource-a".into(),
        configuration_id: configuration().configuration_id.to_string(),
        name: AvailabilityGroupName::new("test-ag").unwrap(),
        creation_group_id: None,
        group_id: None,
    };
    let complete = bound_snapshot();
    let complete_group = match &complete.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    assert_eq!(
        classify_owned_availability_group(
            Some(complete_group),
            None,
            &expected_group_ownership,
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );

    let intent = AvailabilityGroupOwnership {
        resource_id: "resource-a".into(),
        configuration_id: configuration().configuration_id.to_string(),
        name: AvailabilityGroupName::new("test-ag").unwrap(),
        creation_group_id: None,
        group_id: None,
    };
    assert_eq!(
        classify_owned_availability_group(
            Some(complete_group),
            Some(&intent),
            &expected_group_ownership,
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );

    let frozen = owned_standalone_database()
        .availability_group_ownership
        .unwrap();
    let mut recreated = complete_group.clone();
    recreated.identity.group_id = guid("99999999-9999-4999-8999-999999999999");
    assert_eq!(
        classify_owned_availability_group(
            Some(&recreated),
            Some(&frozen),
            &expected_group_ownership,
            &descriptors,
            &database_name,
            Some(&database_identity),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
    assert_eq!(
        classify_replica_join(
            Some(&recreated),
            &complete_group.identity,
            &recreated.local_replica.identity,
            &recreated.replicas[0].server_name,
            recreated.replicas[0].endpoint_url.as_deref().unwrap(),
            false,
        ),
        NativeConvergenceState::Conflicting
    );
}

#[tokio::test]
async fn explicit_bootstrap_path_assigns_exact_effect_ownership_and_replays_from_native_state() {
    let (primary, primary_mutation) = bootstrap_replicator_for(
        0,
        vec![
            Ok(present(bootstrap_absent_snapshot(0))),
            Ok(present(bootstrap_absent_snapshot(0))),
            Ok(present(bootstrap_membership_snapshot(0))),
            Ok(present(bootstrap_membership_snapshot_with_sequence(
                0, "43",
            ))),
            Ok(present(bootstrap_membership_snapshot_with_sequence(
                0, "43",
            ))),
        ],
        vec![
            Ok(None),
            Ok(Some(owned_standalone_database_without_group_intent())),
            Ok(Some(owned_standalone_database_with_group_intent())),
            Ok(Some(owned_standalone_database())),
            Ok(Some(owned_standalone_database())),
            Ok(Some(owned_standalone_database())),
        ],
    );
    primary.open().await.unwrap();
    primary
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    primary
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    assert_eq!(
        *primary_mutation.effects.lock().unwrap(),
        vec![
            "retain source database creation intent",
            "retain availability group intent",
            "retain availability group creation intent"
        ]
    );
    assert!(
        primary
            .admitted_topology()
            .unwrap()
            .database_lineage()
            .is_none()
    );
    assert_eq!(primary.current_progress().await.unwrap(), 42);

    for local_index in [1, 2] {
        let (secondary, mutation) = bootstrap_replicator_for(
            local_index,
            vec![
                Ok(present(bootstrap_absent_snapshot(local_index))),
                Ok(present(pending_secondary_join_snapshot(local_index))),
                Ok(present(bootstrap_membership_snapshot(local_index))),
                Ok(present(bootstrap_membership_snapshot(local_index))),
            ],
            vec![
                Ok(None),
                Ok(Some(owned_standalone_database())),
                Ok(None),
                Ok(Some(owned_standalone_database())),
                Ok(None),
                Ok(Some(owned_standalone_database())),
                Ok(Some(owned_standalone_database())),
            ],
        );
        secondary.open().await.unwrap();
        secondary
            .update_current_replica_set_configuration(bound_replica_set())
            .await
            .unwrap();
        secondary
            .update_current_replica_set_configuration(bound_replica_set())
            .await
            .unwrap();
        assert_eq!(
            *mutation.effects.lock().unwrap(),
            vec!["join availability group"]
        );
        assert!(secondary.admitted_topology().is_some());
    }
}

#[tokio::test]
async fn bootstrap_path_fails_closed_on_foreign_standalone_database() {
    let mut foreign = owned_standalone_database();
    foreign.ownership = Some(StandaloneDatabaseOwnership {
        resource_id: "foreign-resource".into(),
        configuration_id: configuration().configuration_id.to_string(),
    });
    let (replicator, mutation) = bootstrap_replicator_for(0, Vec::new(), vec![Ok(Some(foreign))]);
    replicator.open().await.unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("conflicts"));
    assert!(mutation.effects.lock().unwrap().is_empty());
    assert!(replicator.admitted_topology().is_none());
}

#[tokio::test]
async fn bootstrap_restart_requires_the_frozen_availability_group_marker() {
    let mut unmarked = owned_standalone_database();
    unmarked.availability_group_ownership = None;
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![
            Ok(present(bootstrap_membership_snapshot(0))),
            Ok(present(bootstrap_membership_snapshot(0))),
        ],
        vec![Ok(Some(unmarked))],
    );
    replicator.open().await.unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("conflicts"));
    assert!(mutation.effects.lock().unwrap().is_empty());

    let mut recreated = bootstrap_membership_snapshot(0);
    if let Observation::Present { value: group, .. } = &mut recreated.availability_group {
        group.identity.group_id = guid("99999999-9999-4999-8999-999999999999");
    }
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![Ok(present(recreated))],
        vec![Ok(Some(owned_standalone_database()))],
    );
    replicator.open().await.unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("conflicts"));
    assert!(mutation.effects.lock().unwrap().is_empty());
}

#[tokio::test]
async fn secondary_restart_rejects_a_local_same_name_group_with_a_different_primary_guid() {
    let mut fork = bootstrap_membership_snapshot(1);
    if let Observation::Present { value: group, .. } = &mut fork.availability_group {
        group.identity.group_id = guid("99999999-9999-4999-8999-999999999999");
    }
    let (replicator, mutation) = bootstrap_replicator_for(
        1,
        vec![Ok(present(fork))],
        vec![Ok(None), Ok(Some(owned_standalone_database()))],
    );
    replicator.open().await.unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("conflicts"));
    assert!(mutation.effects.lock().unwrap().is_empty());
}

#[tokio::test]
async fn current_configuration_replay_revalidates_availability_group_ownership() {
    let mut unmarked = owned_standalone_database();
    unmarked.availability_group_ownership = None;
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![
            Ok(present(bootstrap_membership_snapshot(0))),
            Ok(present(bootstrap_membership_snapshot(0))),
        ],
        vec![Ok(Some(owned_standalone_database())), Ok(Some(unmarked))],
    );
    replicator.open().await.unwrap();
    replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("conflicts"));
    assert!(mutation.effects.lock().unwrap().is_empty());
}

#[tokio::test]
async fn seed_build_revalidates_the_exact_availability_group_guid_marker() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut unmarked = owned_standalone_database();
    unmarked.availability_group_ownership = None;
    let executor = Arc::new(ScriptedMutationExecutor {
        databases: Mutex::new(
            vec![Ok(Some(owned_standalone_database())), Ok(Some(unmarked))].into(),
        ),
        effects: Mutex::new(Vec::new()),
        executions: Mutex::new(VecDeque::new()),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config(),
                samples: Mutex::new(
                    vec![
                        Ok(present(bootstrap_membership_snapshot(0))),
                        Ok(present(bootstrap_membership_snapshot(0))),
                    ]
                    .into(),
                ),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
            root.path().to_owned(),
        )
        .unwrap(),
    );
    admit_seed_replicator(&replicator).await;
    let error = replicator
        .build_replica(target_build(1, "unmarked-seed-build"))
        .await
        .unwrap_err();
    assert!(application_error(error).contains("ownership marker"));
    assert!(executor.effects.lock().unwrap().is_empty());
}

fn mutation_error(code: u32) -> RuntimeError {
    let mut error = RuntimeError::new(
        ObservationFailureKind::Unsupported,
        "scripted mutation",
        "scripted SQL Server mutation failure",
    );
    error.server_code = Some(code);
    error
}

fn pending_secondary_join_snapshot(local_index: usize) -> InstanceSnapshot {
    let mut sample = bootstrap_membership_snapshot(local_index);
    let group = match &mut sample.availability_group {
        Observation::Present { value, .. } => value,
        _ => unreachable!(),
    };
    group.local_replica.state_available = false;
    group.local_replica.role = Some(NativeRole::NotJoined);
    let local_id = group.local_replica.identity.native_replica_id().unwrap();
    group
        .replicas
        .iter_mut()
        .find(|replica| replica.replica_id == *local_id)
        .unwrap()
        .state = None;
    sample
}

#[tokio::test]
async fn secondary_waits_for_primary_group_and_retries_only_expected_join_propagation() {
    let executor = Arc::new(ScriptedMutationExecutor {
        databases: Mutex::new(
            vec![
                Ok(None),
                Ok(Some(owned_standalone_database())),
                Ok(None),
                Ok(Some(owned_standalone_database())),
                Ok(None),
                Ok(Some(owned_standalone_database())),
            ]
            .into(),
        ),
        effects: Mutex::new(Vec::new()),
        executions: Mutex::new(vec![Err(mutation_error(35250))].into()),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-2.example:5022")
                .unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config_for(1),
                samples: Mutex::new(
                    vec![
                        Ok(present(bootstrap_absent_snapshot(1))),
                        Ok(present(bootstrap_absent_snapshot(1))),
                        Ok(present(bootstrap_membership_snapshot(1))),
                    ]
                    .into(),
                ),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(2),
            topology_expectation_for(1),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
            private_seed_root(),
        )
        .unwrap(),
    );
    replicator.open().await.unwrap();
    replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    assert_eq!(
        *executor.effects.lock().unwrap(),
        vec!["join availability group"]
    );
    assert!(replicator.admitted_topology().is_some());
}

#[tokio::test]
async fn secondary_join_does_not_retry_arbitrary_sql_errors() {
    let executor = Arc::new(ScriptedMutationExecutor {
        databases: Mutex::new(
            vec![
                Ok(Some(owned_standalone_database())),
                Ok(Some(owned_standalone_database())),
            ]
            .into(),
        ),
        effects: Mutex::new(Vec::new()),
        executions: Mutex::new(vec![Err(mutation_error(50000))].into()),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-2.example:5022")
                .unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config_for(1),
                samples: Mutex::new(vec![Ok(present(pending_secondary_join_snapshot(1)))].into()),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(2),
            topology_expectation_for(1),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
            private_seed_root(),
        )
        .unwrap(),
    );
    replicator.open().await.unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("SQL Server error 50000"));
    assert_eq!(
        *executor.effects.lock().unwrap(),
        vec!["join availability group"]
    );
    assert!(replicator.admitted_topology().is_none());
}

#[tokio::test]
async fn bootstrap_retries_partial_membership_and_rejects_wrong_member_or_profile() {
    let mut partial = bootstrap_membership_snapshot(0);
    match &mut partial.availability_group {
        Observation::Present { value, .. } => {
            value.replicas.pop();
        }
        _ => unreachable!(),
    }
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![
            Ok(present(partial)),
            Ok(present(bootstrap_membership_snapshot(0))),
        ],
        vec![
            Ok(Some(owned_standalone_database())),
            Ok(Some(owned_standalone_database())),
        ],
    );
    replicator.open().await.unwrap();
    replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    assert!(mutation.effects.lock().unwrap().is_empty());

    let mut wrong_member = bootstrap_membership_snapshot(0);
    match &mut wrong_member.availability_group {
        Observation::Present { value, .. } => {
            value.replicas[2] = replica(THIRD_ID, "foreign-sql");
        }
        _ => unreachable!(),
    }
    let mut wrong_profile = bootstrap_membership_snapshot(0);
    match &mut wrong_profile.availability_group {
        Observation::Present { value, .. } => {
            value.replicas[1].seeding_mode = "MANUAL".into();
        }
        _ => unreachable!(),
    }
    for sample in [wrong_member, wrong_profile] {
        let (replicator, mutation) = bootstrap_replicator_for(
            0,
            vec![Ok(present(sample))],
            vec![Ok(Some(owned_standalone_database()))],
        );
        replicator.open().await.unwrap();
        let error = replicator
            .update_current_replica_set_configuration(bound_replica_set())
            .await
            .unwrap_err();
        assert!(application_error(error).contains("conflicts"));
        assert!(mutation.effects.lock().unwrap().is_empty());
        assert!(replicator.admitted_topology().is_none());
    }
}

#[tokio::test]
async fn bootstrap_retries_only_inconsistent_observation_propagation() {
    let propagation = RuntimeError::new(
        ObservationFailureKind::Inconsistent,
        "scripted observation",
        "native state changed during the observation bracket",
    );
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![
            Err(propagation),
            Ok(present(bootstrap_membership_snapshot(0))),
        ],
        vec![
            Ok(Some(owned_standalone_database())),
            Ok(Some(owned_standalone_database())),
        ],
    );
    replicator.open().await.unwrap();
    replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    assert!(mutation.effects.lock().unwrap().is_empty());

    let unavailable = RuntimeError::new(
        ObservationFailureKind::Unreachable,
        "scripted observation",
        "native endpoint is unavailable",
    );
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![Err(unavailable)],
        vec![Ok(Some(owned_standalone_database()))],
    );
    replicator.open().await.unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("Unreachable"));
    assert!(mutation.effects.lock().unwrap().is_empty());
    assert!(replicator.admitted_topology().is_none());
}

#[tokio::test]
async fn bootstrap_admission_accepts_only_exact_owned_database_association() {
    let mut exact = bootstrap_membership_snapshot(0);
    let exact_database = match bound_snapshot().availability_group {
        Observation::Present { value, .. } => value.databases[0].clone(),
        _ => unreachable!(),
    };
    match &mut exact.availability_group {
        Observation::Present { value, .. } => {
            value.databases.push(exact_database);
        }
        _ => unreachable!(),
    }
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![Ok(present(exact.clone()))],
        vec![Ok(Some(owned_standalone_database()))],
    );
    replicator.open().await.unwrap();
    replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap();
    assert!(mutation.effects.lock().unwrap().is_empty());
    assert!(
        replicator
            .admitted_topology()
            .unwrap()
            .database_lineage()
            .is_some()
    );

    let mut foreign = exact;
    match &mut foreign.availability_group {
        Observation::Present { value, .. } => {
            value.databases[0]
                .local
                .as_mut()
                .unwrap()
                .recovery
                .as_mut()
                .unwrap()
                .family_guid = Some(guid(AG_ID));
        }
        _ => unreachable!(),
    }
    let (replicator, mutation) = bootstrap_replicator_for(
        0,
        vec![Ok(present(foreign))],
        vec![Ok(Some(owned_standalone_database()))],
    );
    replicator.open().await.unwrap();
    let error = replicator
        .update_current_replica_set_configuration(bound_replica_set())
        .await
        .unwrap_err();
    assert!(application_error(error).contains("conflicts"));
    assert!(mutation.effects.lock().unwrap().is_empty());
    assert!(replicator.admitted_topology().is_none());
}

#[tokio::test]
async fn callback_deadline_cancels_gated_observation_before_it_can_publish() {
    let executor = Arc::new(ScriptedMutationExecutor {
        databases: Mutex::new(
            vec![
                Ok(Some(owned_standalone_database())),
                Ok(Some(owned_standalone_database())),
            ]
            .into(),
        ),
        effects: Mutex::new(Vec::new()),
        executions: Mutex::new(VecDeque::new()),
    });
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(GatedSource {
                config: observer_config_for_timeout(0, 20),
                samples: Mutex::new(vec![Ok(present(bootstrap_membership_snapshot(0)))].into()),
                observations: AtomicUsize::new(0),
                gate_at: 0,
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(Some(release_rx)),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor,
            private_seed_root(),
        )
        .unwrap(),
    );
    replicator.open().await.unwrap();
    let task = tokio::spawn({
        let replicator = replicator.clone();
        async move {
            replicator
                .update_current_replica_set_configuration(bound_replica_set())
                .await
        }
    });
    entered_rx.await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("callback must finish at its parent deadline")
        .unwrap()
        .unwrap_err();
    assert!(application_error(error).contains("bounded callback budget"));
    assert!(release_tx.send(()).is_err());
    assert!(replicator.admitted_topology().is_none());
}

#[tokio::test]
async fn callback_deadline_cancels_gated_mutation_before_it_can_publish() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let executor = Arc::new(GatedMutationExecutor {
        databases: Mutex::new(vec![Ok(None)].into()),
        effects: Mutex::new(Vec::new()),
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(Some(release_rx)),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config_for_timeout(0, 20),
                samples: Mutex::new(VecDeque::new()),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
            private_seed_root(),
        )
        .unwrap(),
    );
    replicator.open().await.unwrap();
    let task = tokio::spawn({
        let replicator = replicator.clone();
        async move {
            replicator
                .update_current_replica_set_configuration(bound_replica_set())
                .await
        }
    });
    entered_rx.await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("callback must finish at its parent deadline")
        .unwrap()
        .unwrap_err();
    assert!(application_error(error).contains("bounded callback budget"));
    assert!(release_tx.send(()).is_err());
    assert_eq!(
        *executor.effects.lock().unwrap(),
        vec!["retain source database creation intent"]
    );
    assert!(replicator.admitted_topology().is_none());
}

#[tokio::test]
async fn lifecycle_supersession_fences_a_late_mutation_completion() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let executor = Arc::new(GatedMutationExecutor {
        databases: Mutex::new(vec![Ok(None)].into()),
        effects: Mutex::new(Vec::new()),
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(Some(release_rx)),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config(),
                samples: Mutex::new(VecDeque::new()),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor,
            private_seed_root(),
        )
        .unwrap(),
    );
    replicator.open().await.unwrap();
    let task = tokio::spawn({
        let replicator = replicator.clone();
        async move {
            replicator
                .update_current_replica_set_configuration(bound_replica_set())
                .await
        }
    });
    entered_rx.await.unwrap();
    replicator.abort();
    release_tx.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    assert!(replicator.admitted_topology().is_none());
}

#[tokio::test]
async fn close_during_bootstrap_reobservation_fences_configuration_success() {
    let executor = Arc::new(ScriptedMutationExecutor {
        databases: Mutex::new(vec![Ok(Some(owned_standalone_database()))].into()),
        effects: Mutex::new(Vec::new()),
        executions: Mutex::new(VecDeque::new()),
    });
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(GatedSource {
                config: observer_config(),
                samples: Mutex::new(vec![Ok(present(bootstrap_membership_snapshot(0)))].into()),
                observations: AtomicUsize::new(0),
                gate_at: 0,
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(Some(release_rx)),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor,
            private_seed_root(),
        )
        .unwrap(),
    );
    replicator.open().await.unwrap();
    let task = tokio::spawn({
        let replicator = replicator.clone();
        async move {
            replicator
                .update_current_replica_set_configuration(bound_replica_set())
                .await
        }
    });
    entered_rx.await.unwrap();
    replicator.close().await.unwrap();
    release_tx.send(()).unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    assert!(replicator.admitted_topology().is_none());
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

fn bootstrap_replicator_for(
    local_index: usize,
    samples: Vec<Result<Observation<InstanceSnapshot>, RuntimeError>>,
    databases: Vec<Result<Option<StandaloneDatabaseSnapshot>, RuntimeError>>,
) -> (Arc<SqlServerReplicator>, Arc<ScriptedMutationExecutor>) {
    let executor = Arc::new(ScriptedMutationExecutor {
        databases: Mutex::new(databases.into()),
        effects: Mutex::new(Vec::new()),
        executions: Mutex::new(VecDeque::new()),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(
                ResourceUid::new("resource-a"),
                format!("replica-{}.example:5022", local_index + 1),
            )
            .unwrap(),
            Arc::new(ScriptedSource {
                config: observer_config_for(local_index),
                samples: Mutex::new(samples.into()),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(local_index as i64 + 1),
            topology_expectation_for(local_index),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
            private_seed_root(),
        )
        .unwrap(),
    );
    (replicator, executor)
}

fn seed_replicator(
    root: std::path::PathBuf,
) -> (
    Arc<SqlServerReplicator>,
    Arc<SeedMutationExecutor>,
    Arc<AtomicBool>,
) {
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replicator = seed_replicator_with_executor(
        root,
        complete.clone(),
        executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    (replicator, executor, complete)
}

fn seed_replicator_with_executor(
    root: std::path::PathBuf,
    complete: Arc<AtomicBool>,
    executor: Arc<dyn SqlMutationExecutor>,
) -> Arc<SqlServerReplicator> {
    seed_replicator_with_executor_and_target_evidence(
        root,
        complete.clone(),
        executor,
        Arc::new(SeedTargetEvidenceExchange { complete }),
    )
}

fn seed_replicator_with_executor_and_target_evidence(
    root: std::path::PathBuf,
    complete: Arc<AtomicBool>,
    executor: Arc<dyn SqlMutationExecutor>,
    target_evidence: Arc<dyn TargetHostEvidenceExchange>,
) -> Arc<SqlServerReplicator> {
    Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology_and_target_evidence(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(SeedSource {
                config: observer_config(),
                complete,
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor,
            target_evidence,
            root,
        )
        .unwrap(),
    )
}

fn terminal_seed_replicator(
    root: std::path::PathBuf,
) -> (
    Arc<SqlServerReplicator>,
    Arc<TerminalSeedMutationExecutor>,
    Arc<AtomicBool>,
    Arc<AtomicBool>,
) {
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let associated = Arc::new(AtomicBool::new(false));
    let replaced_group = Arc::new(AtomicBool::new(false));
    let executor = Arc::new(TerminalSeedMutationExecutor {
        associated: associated.clone(),
        terminal: Arc::new(AtomicBool::new(false)),
        executions: AtomicUsize::new(0),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology_and_target_evidence(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(TerminalSeedSource {
                config: observer_config(),
                associated: associated.clone(),
                replaced_group: replaced_group.clone(),
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
            Arc::new(SeedTargetEvidenceExchange {
                complete: associated.clone(),
            }),
            root,
        )
        .unwrap(),
    );
    (replicator, executor, associated, replaced_group)
}

async fn admit_seed_replicator(replicator: &SqlServerReplicator) {
    admit_seed_replicator_with_sessions(replicator, "session").await;
}

async fn admit_seed_replicator_with_sessions(
    replicator: &SqlServerReplicator,
    session_prefix: &str,
) {
    replicator.open().await.unwrap();
    replicator
        .update_current_replica_set_configuration(replica_set_with_session_prefix(session_prefix))
        .await
        .unwrap();
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
}

fn target_build(target_index: usize, build_id: &str) -> ReplicaInformation {
    target_build_with_sessions(target_index, build_id, "session")
}

fn target_build_with_sessions(
    target_index: usize,
    build_id: &str,
    session_prefix: &str,
) -> ReplicaInformation {
    let current = replica_set_with_session_prefix(session_prefix);
    let target = &current.replicas[target_index];
    let mut build = ReplicaInformation::new(
        OperationId::new(build_id),
        target.identity.clone(),
        target.replication_address.clone(),
    );
    build.process_session_id = target.process_session_id.clone();
    build
}

fn exact_seed_record(winning_target: usize, build_id: &str) -> SeedDispatchRecord {
    exact_seed_record_with_sessions(winning_target, build_id, "session")
}

fn exact_seed_record_with_sessions(
    winning_target: usize,
    build_id: &str,
    session_prefix: &str,
) -> SeedDispatchRecord {
    let current = replica_set_with_session_prefix(session_prefix);
    let member = |index: usize| {
        let kuberic = &current.replicas[index];
        SeedDispatchTarget::new(
            kuberic.identity.replica_id.to_string(),
            kuberic.identity.instance_id.to_string(),
            kuberic.identity.agent_generation.to_string(),
            kuberic.process_session_id.to_string(),
            [LOCAL_ID, SECOND_ID, THIRD_ID][index],
        )
        .unwrap()
    };
    SeedDispatchRecord::new(
        "resource-a",
        configuration().configuration_id.to_string(),
        "test-ag",
        AG_ID,
        "app-db",
        DATABASE_GUID,
        FAMILY_GUID,
        FORK_ID,
        member(0),
        [member(1), member(2)],
        current.replicas[winning_target]
            .identity
            .replica_id
            .to_string(),
        build_id,
        "2026-10-06T12:00:00",
    )
    .unwrap()
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
    assert!(
        SqlServerReplicator::new_with_topology(
            "replica-1.example:5022".into(),
            Arc::new(ScriptedSource {
                config: observer_config_for(1),
                samples: Mutex::new(VecDeque::new()),
            }),
            clock(),
            kuberic_identity(1),
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
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 5],
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
fn runtime_adapter_admits_authority_before_peer_session_enrichment() {
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

        adapter
            .execute(runtime_effect(
                "exact-admission",
                2,
                RuntimeEffectAction::AdmitAuthority(Box::new(admitted_authority(0))),
            ))
            .await
            .unwrap();
        assert!(runtime.snapshot().await.authority.is_some());
        let admitted = store.load_state().await.unwrap();
        assert_eq!(admitted.current_configuration, Some(configuration()));
        assert!(admitted.admitted_policy.is_some());
        assert!(admitted.pending_effect.is_none());
        assert!(
            _service
                .replicator()
                .and_then(|replicator| replicator.admitted_topology())
                .is_some()
        );
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
    changed.configuration.members[0].identity.instance_id = ReplicaInstanceId::new("");
    changed.replicas[0].identity = changed.configuration.members[0].identity.clone();
    cases.push(("empty instance identity", changed));

    let mut changed = exact.clone();
    changed.configuration.members[0].identity.agent_generation = AgentGeneration::new("");
    changed.replicas[0].identity = changed.configuration.members[0].identity.clone();
    cases.push(("empty generation identity", changed));

    let mut changed = exact.clone();
    changed.replicas[0].replication_address = "not-an-address".into();
    cases.push(("invalid address", changed));

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
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 5],
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
    let mut reordered = bound_replica_set();
    reordered.replicas.reverse();
    for changed_replay in [
        changed_progress,
        changed_session,
        changed_address,
        reordered,
    ] {
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
async fn incomplete_bootstrap_reports_native_capability_and_allows_one_way_peer_enrichment() {
    let complete = bound_replica_set();
    let mut incomplete = complete.clone();
    for replica in &mut incomplete.replicas[1..] {
        replica.process_session_id = ProcessSessionId::default();
        replica.replication_address.clear();
        replica.role = ReplicaRole::None;
    }
    let replicator = opened_bound_replicator(
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 6],
    )
    .await;
    replicator
        .update_current_replica_set_configuration(incomplete.clone())
        .await
        .unwrap();
    replicator
        .change_role(configuration().epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    assert_eq!(replicator.catch_up_capability().await.unwrap(), 42);
    replicator
        .update_current_replica_set_configuration(complete)
        .await
        .unwrap();
    assert_eq!(replicator.catch_up_capability().await.unwrap(), 42);
    assert!(
        application_error(
            replicator
                .update_current_replica_set_configuration(incomplete)
                .await
                .unwrap_err()
        )
        .contains("replay differs")
    );
}

#[tokio::test]
async fn concurrent_peer_enrichment_allows_exactly_one_completed_session_set() {
    let mut incomplete = bound_replica_set();
    for replica in &mut incomplete.replicas[1..] {
        replica.process_session_id = ProcessSessionId::default();
        replica.replication_address.clear();
        replica.role = ReplicaRole::None;
    }
    let replicator = opened_bound_replicator(
        vec![
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
            Ok(present(bound_snapshot())),
        ],
        vec![OBSERVED_AT; 3],
    )
    .await;
    replicator
        .update_current_replica_set_configuration(incomplete)
        .await
        .unwrap();

    let first = bound_replica_set();
    let second = replica_set_with_session_prefix("competing-session");
    let (first, second) = tokio::join!(
        replicator.update_current_replica_set_configuration(first),
        replicator.update_current_replica_set_configuration(second)
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
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
                || message.contains("configuration or state")
                || (name == "process restart" && message.contains("unsafe to continue")),
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
async fn concurrent_target_builds_select_one_dispatch_and_replay_from_native_evidence() {
    let root = tempfile::tempdir().unwrap();
    let (replicator, executor, complete) = seed_replicator(root.path().to_owned());
    admit_seed_replicator(&replicator).await;

    let first = target_build(1, "build-target-2");
    let second = target_build(2, "build-target-3");
    let (first_result, second_result) = tokio::join!(
        replicator.build_replica(first.clone()),
        replicator.build_replica(second.clone())
    );
    first_result.unwrap();
    second_result.unwrap();

    assert!(complete.load(Ordering::SeqCst));
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());

    replicator.build_replica(first).await.unwrap();
    replicator.build_replica(second).await.unwrap();
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn target_host_evidence_gates_completion_and_rejects_stale_build_session_replay() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let exchange = Arc::new(ControlledTargetEvidenceExchange::with_gate(
        [
            ControlledTargetEvidenceMode::Healthy,
            ControlledTargetEvidenceMode::ReplayPreviousAuthority,
        ],
        entered_tx,
        release_rx,
    ));
    let replicator = seed_replicator_with_executor_and_target_evidence(
        root.path().to_owned(),
        complete.clone(),
        executor.clone(),
        exchange.clone(),
    );
    admit_seed_replicator(&replicator).await;
    let callback = tokio::spawn({
        let replicator = replicator.clone();
        async move {
            replicator
                .build_replica(target_build(1, "evidence-gated-build"))
                .await
        }
    });
    entered_rx.await.unwrap();
    assert!(
        !callback.is_finished(),
        "primary-side native completion published before target-local evidence"
    );
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    release_tx.send(()).unwrap();
    callback.await.unwrap().unwrap();
    drop(replicator);

    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor_and_target_evidence(
        root.path().to_owned(),
        complete,
        replacement_executor.clone(),
        exchange,
    );
    admit_seed_replicator_with_sessions(&replacement, "replacement-session").await;
    let message = application_error(
        replacement
            .build_replica(target_build_with_sessions(
                1,
                "replacement-build",
                "replacement-session",
            ))
            .await
            .unwrap_err(),
    );
    assert!(
        message.contains("target-host evidence authority"),
        "{message}"
    );
    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn target_host_evidence_rejects_stale_restart_and_lineage_reports() {
    for (mode, expected) in [
        (ControlledTargetEvidenceMode::Stale, "stale"),
        (ControlledTargetEvidenceMode::SqlRestarted, "is unsafe"),
        (ControlledTargetEvidenceMode::LineageMismatch, "conflicts"),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let complete = Arc::new(AtomicBool::new(false));
        let executor = Arc::new(SeedMutationExecutor {
            complete: complete.clone(),
            executions: AtomicUsize::new(0),
        });
        let replicator = seed_replicator_with_executor_and_target_evidence(
            root.path().to_owned(),
            complete,
            executor.clone(),
            Arc::new(ControlledTargetEvidenceExchange::new([mode])),
        );
        admit_seed_replicator(&replicator).await;
        let message = application_error(
            replicator
                .build_replica(target_build(1, "invalid-target-evidence"))
                .await
                .unwrap_err(),
        );
        assert!(message.contains(expected), "{mode:?}: {message}");
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn either_single_target_can_own_group_dispatch_without_a_peer_build() {
    for (target_index, build_id) in [(1, "only-target-2"), (2, "only-target-3")] {
        let root = tempfile::tempdir().unwrap();
        let (replicator, executor, _) = seed_replicator(root.path().to_owned());
        admit_seed_replicator(&replicator).await;

        replicator
            .build_replica(target_build(target_index, build_id))
            .await
            .unwrap();

        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert!(!root.path().join("seed-dispatch-v1.json").exists());
    }
}

#[tokio::test]
async fn build_rejects_changed_session_before_dispatch_and_corrupt_seed_state_on_replay() {
    let root = tempfile::tempdir().unwrap();
    let (replicator, executor, _) = seed_replicator(root.path().to_owned());
    admit_seed_replicator(&replicator).await;

    let mut stale = target_build(1, "stale-session-build");
    stale.process_session_id = ProcessSessionId::new("stale-session");
    let message = application_error(replicator.build_replica(stale).await.unwrap_err());
    assert!(message.contains("session"), "{message}");
    assert_eq!(executor.executions.load(Ordering::SeqCst), 0);

    std::fs::write(root.path().join("seed-dispatch-v1.json"), b"{corrupt").unwrap();
    std::fs::set_permissions(
        root.path().join("seed-dispatch-v1.json"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let message = application_error(
        replicator
            .build_replica(target_build(1, "corrupt-state-build"))
            .await
            .unwrap_err(),
    );
    assert!(message.contains("malformed"), "{message}");
    assert_eq!(executor.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn winner_cancellation_before_durable_dispatch_allows_safe_rearbitration() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let first_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first = seed_replicator_with_executor(
        root.path().to_owned(),
        complete.clone(),
        first_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&first).await;
    first
        .testing_gate_seed_intent(entered_tx, release_rx)
        .unwrap();

    let callback = tokio::spawn({
        let first = first.clone();
        async move {
            first
                .build_replica(target_build(1, "cancel-before-dispatch"))
                .await
        }
    });
    entered_rx.await.unwrap();
    let retained: SeedDispatchRecord =
        serde_json::from_slice(&std::fs::read(root.path().join("seed-dispatch-v1.json")).unwrap())
            .unwrap();
    assert_eq!(retained.phase(), SeedDispatchPhase::Intent);
    first.abort();
    release_tx.send(()).unwrap();
    assert!(matches!(
        callback.await.unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    assert_eq!(first_executor.executions.load(Ordering::SeqCst), 0);
    drop(first);

    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        replacement_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator_with_sessions(&replacement, "replacement-session").await;
    replacement
        .build_replica(target_build_with_sessions(
            2,
            "rearbitrated-winner",
            "replacement-session",
        ))
        .await
        .unwrap();
    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn crash_immediately_before_request_submission_retries_from_submitting_without_false_acceptance()
 {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let first_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let first = seed_replicator_with_executor(
        root.path().to_owned(),
        complete.clone(),
        first_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&first).await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    first
        .testing_gate_seed_submission(entered_tx, release_rx)
        .unwrap();
    let callback = tokio::spawn({
        let first = first.clone();
        async move {
            first
                .build_replica(target_build(1, "crash-before-request"))
                .await
        }
    });
    entered_rx.await.unwrap();
    let retained: SeedDispatchRecord =
        serde_json::from_slice(&std::fs::read(root.path().join("seed-dispatch-v1.json")).unwrap())
            .unwrap();
    assert_eq!(retained.phase(), SeedDispatchPhase::Submitting);
    assert_eq!(first_executor.executions.load(Ordering::SeqCst), 0);
    first.abort();
    release_tx.send(()).unwrap();
    assert!(matches!(
        callback.await.unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    drop(first);

    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        replacement_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator_with_sessions(&replacement, "replacement-session").await;
    replacement
        .build_replica(target_build_with_sessions(
            2,
            "retry-after-pre-submit-crash",
            "replacement-session",
        ))
        .await
        .unwrap();
    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn crash_immediately_after_request_submission_retries_after_proven_pre_effect_failure() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first_executor = Arc::new(SubmittedThenFailedSeedExecutor {
        executions: AtomicUsize::new(0),
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(Some(release_rx)),
    });
    let first = seed_replicator_with_executor(
        root.path().to_owned(),
        complete.clone(),
        first_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&first).await;
    let callback = tokio::spawn({
        let first = first.clone();
        async move {
            first
                .build_replica(target_build(1, "crash-after-request"))
                .await
        }
    });
    entered_rx.await.unwrap();
    let retained: SeedDispatchRecord =
        serde_json::from_slice(&std::fs::read(root.path().join("seed-dispatch-v1.json")).unwrap())
            .unwrap();
    assert_eq!(retained.phase(), SeedDispatchPhase::Submitting);
    callback.abort();
    assert!(callback.await.unwrap_err().is_cancelled());
    drop(release_tx);
    tokio::task::yield_now().await;
    first.close().await.unwrap();
    drop(first);

    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        replacement_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator_with_sessions(&replacement, "replacement-session").await;
    replacement
        .build_replica(target_build_with_sessions(
            1,
            "retry-after-post-submit-crash",
            "replacement-session",
        ))
        .await
        .unwrap();
    assert_eq!(first_executor.executions.load(Ordering::SeqCst), 1);
    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn accepted_sql_with_lost_reply_replays_from_native_evidence_without_redispatch() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let executor = Arc::new(AcceptedReplyLostSeedExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replicator = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&replicator).await;
    replicator
        .build_replica(target_build(1, "accepted-reply-lost"))
        .await
        .unwrap();
    replicator
        .build_replica(target_build(1, "accepted-reply-lost"))
        .await
        .unwrap();
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn definite_sql_rejection_is_retryable_and_does_not_degrade_to_deadline() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let executor = Arc::new(RejectOnceSeedExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replicator = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&replicator).await;
    let message = application_error(
        replicator
            .build_replica(target_build(1, "definite-rejection"))
            .await
            .unwrap_err(),
    );
    assert!(message.contains("SQL Server error 51001"), "{message}");
    assert!(!message.contains("bounded callback budget"), "{message}");
    let retained: SeedDispatchRecord =
        serde_json::from_slice(&std::fs::read(root.path().join("seed-dispatch-v1.json")).unwrap())
            .unwrap();
    assert_eq!(retained.phase(), SeedDispatchPhase::FailedRetryable);

    replicator
        .build_replica(target_build(1, "definite-rejection"))
        .await
        .unwrap();
    assert_eq!(executor.executions.load(Ordering::SeqCst), 2);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn cancellation_after_durable_dispatch_restart_compacts_without_redispatch() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first_executor = Arc::new(GatedSeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(Some(release_rx)),
    });
    let first = seed_replicator_with_executor(
        root.path().to_owned(),
        complete.clone(),
        first_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&first).await;
    let callback = tokio::spawn({
        let first = first.clone();
        async move {
            first
                .build_replica(target_build(1, "cancel-after-dispatch"))
                .await
        }
    });
    entered_rx.await.unwrap();
    let retained: SeedDispatchRecord =
        serde_json::from_slice(&std::fs::read(root.path().join("seed-dispatch-v1.json")).unwrap())
            .unwrap();
    assert_eq!(retained.phase(), SeedDispatchPhase::Submitting);
    callback.abort();
    assert!(callback.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !complete.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.close().await.unwrap();
    drop(first);

    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        replacement_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&replacement).await;
    replacement
        .build_replica(target_build(2, "restart-native-only"))
        .await
        .unwrap();

    assert_eq!(first_executor.executions.load(Ordering::SeqCst), 1);
    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 0);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn compaction_checkpoint_fences_stale_callback_and_replacement_redispatch() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let complete = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let stale_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let stale = seed_replicator_with_executor(
        root.path().to_owned(),
        complete.clone(),
        stale_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator(&stale).await;
    stale
        .testing_gate_seed_compaction(entered_tx, release_rx)
        .unwrap();
    let callback = tokio::spawn({
        let stale = stale.clone();
        async move {
            stale
                .build_replica(target_build(1, "superseded-dispatch"))
                .await
        }
    });
    entered_rx.await.unwrap();
    let retained: SeedDispatchRecord =
        serde_json::from_slice(&std::fs::read(root.path().join("seed-dispatch-v1.json")).unwrap())
            .unwrap();
    assert_eq!(retained.phase(), SeedDispatchPhase::Accepted);
    stale.abort();
    release_tx.send(()).unwrap();
    assert!(matches!(
        callback.await.unwrap(),
        Err(KubericRuntimeError::Closed)
    ));
    assert_eq!(stale_executor.executions.load(Ordering::SeqCst), 1);
    drop(stale);

    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        replacement_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator_with_sessions(&replacement, "replacement-session").await;
    replacement
        .build_replica(target_build_with_sessions(
            2,
            "replacement-build",
            "replacement-session",
        ))
        .await
        .unwrap();
    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 0);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn terminal_marker_rejects_post_compaction_source_association_loss_without_redispatch() {
    let root = tempfile::tempdir().unwrap();
    let (replicator, executor, associated, _) = terminal_seed_replicator(root.path().to_owned());
    admit_seed_replicator(&replicator).await;
    replicator
        .build_replica(target_build(1, "terminal-association-owner"))
        .await
        .unwrap();
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());

    associated.store(false, Ordering::SeqCst);
    let message = application_error(
        replicator
            .build_replica(target_build(2, "terminal-association-lost"))
            .await
            .unwrap_err(),
    );
    assert!(
        message.contains("terminal seed dispatch source association"),
        "{message}"
    );
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn terminal_marker_rejects_post_compaction_availability_group_recreation_without_redispatch()
{
    let root = tempfile::tempdir().unwrap();
    let (replicator, executor, _, replaced_group) =
        terminal_seed_replicator(root.path().to_owned());
    admit_seed_replicator(&replicator).await;
    replicator
        .build_replica(target_build(1, "terminal-group-owner"))
        .await
        .unwrap();
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());

    replaced_group.store(true, Ordering::SeqCst);
    let message = application_error(
        replicator
            .build_replica(target_build(2, "terminal-group-recreated"))
            .await
            .unwrap_err(),
    );
    assert!(
        message.contains("availability group differs")
            || message.contains("availability-group ownership"),
        "{message}"
    );
    assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn restart_compacts_a_dispatch_crashed_after_source_association_before_removal() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    {
        let store = SeedDispatchStore::open(root.path()).unwrap();
        let intent = exact_seed_record_with_sessions(1, "historical-build", "historical-session");
        store.create(&intent).unwrap();
        let mut accepted = intent.clone();
        accepted.mark_accepted().unwrap();
        let mut associated = store.replace(intent.revision(), accepted).unwrap();
        associated.associate_group_database(DATABASE_ID).unwrap();
        store.replace(associated.revision(), associated).unwrap();
    }

    let complete = Arc::new(AtomicBool::new(true));
    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        replacement_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator_with_sessions(&replacement, "replacement-session").await;
    replacement
        .build_replica(target_build_with_sessions(
            1,
            "replacement-build",
            "replacement-session",
        ))
        .await
        .unwrap();

    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 0);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn later_incomplete_target_with_terminal_marker_waits_without_redispatch() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut partial = seeded_source_snapshot();
    if let Observation::Present { value: group, .. } = &mut partial.availability_group {
        let database = group.databases.first_mut().unwrap();
        database
            .replicas
            .retain(|replica| replica.replica_id != guid(THIRD_ID));
        group
            .automatic_seeding
            .retain(|seed| seed.remote_replica_id != guid(THIRD_ID));
    }
    let associated = Arc::new(AtomicBool::new(false));
    let executor = Arc::new(ScriptedMutationExecutor {
        databases: Mutex::new(
            vec![
                Ok(Some(owned_standalone_database())),
                Ok(Some(owned_standalone_database())),
            ]
            .into(),
        ),
        effects: Mutex::new(Vec::new()),
        executions: Mutex::new(VecDeque::new()),
    });
    let replicator = Arc::new(
        SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
                .unwrap(),
            Arc::new(ControlledSeedSource {
                config: observer_config_for_timeout(0, 100),
                associated: associated.clone(),
                associated_snapshot: partial,
            }),
            Arc::new(ScriptedClock {
                times: Mutex::new(vec![OBSERVED_AT].into()),
            }),
            kuberic_identity(1),
            topology_expectation(),
            SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
            executor.clone(),
            root.path().to_owned(),
        )
        .unwrap(),
    );
    admit_seed_replicator(&replicator).await;
    associated.store(true, Ordering::SeqCst);
    let message = application_error(
        replicator
            .build_replica(target_build(2, "later-incomplete-target"))
            .await
            .unwrap_err(),
    );
    assert!(message.contains("bounded callback budget"), "{message}");
    assert!(executor.effects.lock().unwrap().is_empty());
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn intent_only_restart_replays_or_atomically_replaces_the_abandoned_owner() {
    for (target_index, build_id) in [
        (1, "intent-winner"),
        (1, "superseding-intent-winner"),
        (2, "replacement-target-winner"),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        {
            let store = SeedDispatchStore::open(root.path()).unwrap();
            store
                .create(&exact_seed_record(1, "intent-winner"))
                .unwrap();
        }
        let (replicator, executor, _) = seed_replicator(root.path().to_owned());
        admit_seed_replicator(&replicator).await;
        replicator
            .build_replica(target_build(target_index, build_id))
            .await
            .unwrap();
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        assert!(!root.path().join("seed-dispatch-v1.json").exists());
    }
}

#[tokio::test]
async fn retained_dispatch_accepts_fresh_source_and_target_sessions_but_rejects_stale_callback() {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    {
        let store = SeedDispatchStore::open(root.path()).unwrap();
        let record = exact_seed_record_with_sessions(1, "historical-build", "historical-session");
        store.create(&record).unwrap();
        let mut ambiguous = record.clone();
        ambiguous.mark_submitting().unwrap();
        let mut ambiguous = store.replace(record.revision(), ambiguous).unwrap();
        ambiguous.mark_ambiguous().unwrap();
        store.replace(ambiguous.revision(), ambiguous).unwrap();
    }

    let complete = Arc::new(AtomicBool::new(true));
    let replacement_executor = Arc::new(SeedMutationExecutor {
        complete: complete.clone(),
        executions: AtomicUsize::new(0),
    });
    let replacement = seed_replicator_with_executor(
        root.path().to_owned(),
        complete,
        replacement_executor.clone() as Arc<dyn SqlMutationExecutor>,
    );
    admit_seed_replicator_with_sessions(&replacement, "replacement-session").await;

    let stale = target_build_with_sessions(1, "historical-build", "historical-session");
    let message = application_error(replacement.build_replica(stale).await.unwrap_err());
    assert!(message.contains("session"), "{message}");
    assert!(root.path().join("seed-dispatch-v1.json").exists());

    replacement
        .build_replica(target_build_with_sessions(
            2,
            "replacement-build",
            "replacement-session",
        ))
        .await
        .unwrap();
    assert_eq!(replacement_executor.executions.load(Ordering::SeqCst), 0);
    assert!(!root.path().join("seed-dispatch-v1.json").exists());
}

#[tokio::test]
async fn build_rejects_changed_sql_incarnation_and_conflicting_source_association() {
    let mut changed_incarnation = bootstrap_membership_snapshot(0);
    changed_incarnation.instance.sqlserver_start_time = "2026-10-09T00:00:00".into();
    let mut conflicting_association = seeded_source_snapshot();
    if let Observation::Present { value: group, .. } =
        &mut conflicting_association.availability_group
    {
        group.databases[0]
            .local
            .as_mut()
            .unwrap()
            .recovery
            .as_mut()
            .unwrap()
            .family_guid = Some(guid(AG_ID));
    }

    for (name, sample, expected) in [
        ("SQL incarnation", changed_incarnation, "incarnation"),
        (
            "source association",
            conflicting_association,
            "source database association",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let executor = Arc::new(ScriptedMutationExecutor {
            databases: Mutex::new(
                vec![
                    Ok(Some(owned_standalone_database())),
                    Ok(Some(owned_standalone_database())),
                ]
                .into(),
            ),
            effects: Mutex::new(Vec::new()),
            executions: Mutex::new(VecDeque::new()),
        });
        let replicator = Arc::new(
            SqlServerReplicator::new_bootstrap_with_topology(
                SqlServerServiceConfig::new(
                    ResourceUid::new("resource-a"),
                    "replica-1.example:5022",
                )
                .unwrap(),
                Arc::new(ScriptedSource {
                    config: observer_config(),
                    samples: Mutex::new(
                        vec![
                            Ok(present(bootstrap_membership_snapshot(0))),
                            Ok(present(bootstrap_membership_snapshot(0))),
                            Ok(present(sample)),
                        ]
                        .into(),
                    ),
                }),
                Arc::new(ScriptedClock {
                    times: Mutex::new(vec![OBSERVED_AT].into()),
                }),
                kuberic_identity(1),
                topology_expectation(),
                SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
                executor.clone(),
                root.path().to_owned(),
            )
            .unwrap(),
        );
        admit_seed_replicator(&replicator).await;
        let message = application_error(
            replicator
                .build_replica(target_build(1, "conflicting-build"))
                .await
                .unwrap_err(),
        );
        assert!(message.contains(expected), "{name}: {message}");
        assert!(executor.effects.lock().unwrap().is_empty(), "{name}");
    }
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
