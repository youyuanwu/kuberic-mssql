use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Duration;

use async_trait::async_trait;
use kuberic_runtime::application::{
    OpenContext, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, Epoch, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIdentity as KubericReplicaIdentity, ReplicaRole, ResourceUid,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, ReplicaSetQuorumMode,
    Replicator, ReplicatorFactory, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorSettings,
};
use kuberic_runtime::{Result as KubericResult, RuntimeError as KubericRuntimeError};

use crate::bootstrap::{
    BootstrapOwnership, NativeConvergenceState, StandaloneDatabaseIdentity,
    StandaloneDatabaseOwnership, classify_availability_group, classify_bootstrap_ownership,
    classify_replica_join, classify_standalone_database,
};
use crate::executor::SqlExecutor;
use crate::instance::{SqlServerInstanceManager, unix_millis};
use crate::mutation::{MutationEffect, SqlMutationExecutor};
use crate::observation::{
    AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, InstanceSnapshot, NativeProvenance,
    RecoveryLineageObservation,
};
use crate::runtime_config::ObserverConfig;
use crate::runtime_error::RuntimeError;
use crate::runtime_host::seed_state::{
    SeedDispatchPhase, SeedDispatchRecord, SeedDispatchStore, SeedDispatchTarget, SeedStateError,
};
use crate::topology_config::SqlServerTopologyExpectation;
use crate::{
    AvailabilityGroupIdentity, ConfigurationSequence, DatabaseIdentity, DatabaseLineage, Endpoint,
    Guid, NativeRole, Observation, ObservationFailureKind, ReplicaDescriptor,
    ReplicaIdentity as SqlReplicaIdentity, SUPPORTED_DATABASE_COUNT, SUPPORTED_ENGINE_MAJOR,
    SUPPORTED_REPLICA_COUNT, SUPPORTED_REQUIRED_SECONDARIES, ServerName, SqlIdentifier,
};

const CREATED: u8 = 0;
const OPEN: u8 = 1;
const CLOSED: u8 = 2;
const ABORTED: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserveOnlyOperation {
    CatchUpCapability,
    DataLoss,
    CatchUpConfiguration,
    CatchUpQuorum,
    CurrentConfiguration,
    BuildReplica,
    RemoveReplica,
}

impl fmt::Display for ObserveOnlyOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CatchUpCapability => "catch_up_capability",
            Self::DataLoss => "on_data_loss",
            Self::CatchUpConfiguration => "update_catch_up_replica_set_configuration",
            Self::CatchUpQuorum => "wait_for_catch_up_quorum",
            Self::CurrentConfiguration => "update_current_replica_set_configuration",
            Self::BuildReplica => "build_replica",
            Self::RemoveReplica => "remove_replica",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KubericAdapterError {
    InvalidConfiguration(&'static str),
    ResourceIdentityMismatch,
    ObservationUnavailable(ObservationFailureKind),
    AvailabilityGroupAbsent,
    ObservationStale,
    ObservationFromFuture,
    ObservationInconsistent(&'static str),
    UnsupportedProfile(&'static str),
    NativeRoleMismatch {
        requested: ReplicaRole,
        observed: Option<NativeRole>,
    },
    TopologyBindingMismatch(&'static str),
    TopologyNotAdmitted,
    NativeConvergenceConflict(&'static str),
    NativeConvergenceUnsafe(&'static str),
    NativeConvergenceDeadline,
    NativeMutationFailed {
        effect: &'static str,
        kind: ObservationFailureKind,
        server_code: Option<u32>,
    },
    SeedState(SeedStateError),
    UnsupportedOperation(ObserveOnlyOperation),
    NotOpen,
    Closed,
}

impl fmt::Display for KubericAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(
                    formatter,
                    "invalid SQL Server adapter configuration: {message}"
                )
            }
            Self::ResourceIdentityMismatch => {
                formatter.write_str("SQL Server resource identity does not match the partition")
            }
            Self::ObservationUnavailable(kind) => {
                write!(formatter, "SQL Server observation is unavailable: {kind:?}")
            }
            Self::AvailabilityGroupAbsent => {
                formatter.write_str("configured SQL Server availability group is absent")
            }
            Self::ObservationStale => formatter.write_str("SQL Server observation is stale"),
            Self::ObservationFromFuture => {
                formatter.write_str("SQL Server observation is future-dated")
            }
            Self::ObservationInconsistent(message) => {
                write!(
                    formatter,
                    "SQL Server observation is inconsistent: {message}"
                )
            }
            Self::UnsupportedProfile(message) => {
                write!(formatter, "SQL Server profile is not eligible: {message}")
            }
            Self::NativeRoleMismatch {
                requested,
                observed,
            } => write!(
                formatter,
                "requested Kuberic role {requested:?} does not match observed SQL Server role {observed:?}"
            ),
            Self::TopologyBindingMismatch(message) => {
                write!(formatter, "healthy topology binding mismatch: {message}")
            }
            Self::TopologyNotAdmitted => {
                formatter.write_str("healthy topology has not been admitted")
            }
            Self::NativeConvergenceConflict(effect) => {
                write!(
                    formatter,
                    "{effect} conflicts with the requested bootstrap topology"
                )
            }
            Self::NativeConvergenceUnsafe(effect) => {
                write!(formatter, "{effect} is unsafe to continue")
            }
            Self::NativeConvergenceDeadline => {
                formatter.write_str("bootstrap convergence exceeded its bounded callback budget")
            }
            Self::NativeMutationFailed {
                effect,
                kind,
                server_code,
            } => {
                write!(formatter, "{effect} failed: {kind:?}")?;
                if let Some(code) = server_code {
                    write!(formatter, " (SQL Server error {code})")?;
                }
                Ok(())
            }
            Self::SeedState(error) => write!(formatter, "seed dispatch state failed: {error}"),
            Self::UnsupportedOperation(operation) => write!(
                formatter,
                "{operation} is disabled by the observe-only SQL Server adapter"
            ),
            Self::NotOpen => formatter.write_str("SQL Server adapter is not open"),
            Self::Closed => formatter.write_str("SQL Server adapter is closed"),
        }
    }
}

impl std::error::Error for KubericAdapterError {}

impl From<KubericAdapterError> for KubericRuntimeError {
    fn from(error: KubericAdapterError) -> Self {
        match error {
            KubericAdapterError::NotOpen => Self::NotOpen,
            KubericAdapterError::Closed => Self::Closed,
            other => Self::Application(other.to_string()),
        }
    }
}

#[async_trait]
pub trait SqlServerObservationSource: Send + Sync {
    fn observer_config(&self) -> &ObserverConfig;

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError>;
}

#[async_trait]
impl<E: SqlExecutor> SqlServerObservationSource for SqlServerInstanceManager<E> {
    fn observer_config(&self) -> &ObserverConfig {
        self.config()
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        SqlServerInstanceManager::observe(self).await
    }
}

pub trait ObservationClock: Send + Sync {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError>;
}

#[derive(Debug, Default)]
pub struct SystemObservationClock;

impl ObservationClock for SystemObservationClock {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
        unix_millis()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerServiceConfig {
    pub resource_uid: ResourceUid,
    pub replication_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerBootstrapConfig {
    database: SqlIdentifier,
}

impl SqlServerBootstrapConfig {
    pub fn new(database: SqlIdentifier) -> Self {
        Self { database }
    }

    pub fn database(&self) -> &SqlIdentifier {
        &self.database
    }
}

impl SqlServerServiceConfig {
    pub fn new(
        resource_uid: ResourceUid,
        replication_address: impl Into<String>,
    ) -> Result<Self, KubericAdapterError> {
        let replication_address = replication_address.into();
        if resource_uid.is_empty() {
            return Err(KubericAdapterError::InvalidConfiguration(
                "resource UID must not be empty",
            ));
        }
        if replication_address.is_empty()
            || replication_address.chars().any(char::is_control)
            || replication_address.trim() != replication_address
        {
            return Err(KubericAdapterError::InvalidConfiguration(
                "replication address must be nonempty and contain no control or surrounding whitespace",
            ));
        }
        Ok(Self {
            resource_uid,
            replication_address,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SqlServerStartIncarnation(String);

impl SqlServerStartIncarnation {
    pub fn new(value: impl Into<String>) -> Result<Self, KubericAdapterError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 256
            || value.chars().any(char::is_control)
            || value.trim() != value
        {
            return Err(KubericAdapterError::InvalidConfiguration(
                "SQL Server start incarnation must be nonempty, bounded, and contain no control or surrounding whitespace",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerMemberEndpoint {
    server_name: ServerName,
    endpoint_url: String,
}

impl SqlServerMemberEndpoint {
    pub fn new(
        server_name: ServerName,
        endpoint_url: impl Into<String>,
    ) -> Result<Self, KubericAdapterError> {
        let endpoint_url = endpoint_url.into();
        validate_replication_address(&endpoint_url)?;
        Ok(Self {
            server_name,
            endpoint_url,
        })
    }

    pub fn server_name(&self) -> &ServerName {
        &self.server_name
    }

    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthyTopologyMemberBinding {
    kuberic_identity: KubericReplicaIdentity,
    process_session_id: ProcessSessionId,
    replication_address: String,
    native_replica_id: Guid,
    sql_endpoint: SqlServerMemberEndpoint,
    stable_role: ReplicaRole,
}

impl HealthyTopologyMemberBinding {
    pub fn kuberic_identity(&self) -> &KubericReplicaIdentity {
        &self.kuberic_identity
    }

    pub fn process_session_id(&self) -> &ProcessSessionId {
        &self.process_session_id
    }

    pub fn replication_address(&self) -> &str {
        &self.replication_address
    }

    pub fn native_replica_id(&self) -> &Guid {
        &self.native_replica_id
    }

    pub fn server_name(&self) -> &ServerName {
        self.sql_endpoint.server_name()
    }

    pub fn sql_endpoint_url(&self) -> &str {
        self.sql_endpoint.endpoint_url()
    }

    pub fn stable_role(&self) -> ReplicaRole {
        self.stable_role
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthyTopologyBinding {
    local_identity: KubericReplicaIdentity,
    configuration: ConfigurationDescriptor,
    native_configuration_sequence: ConfigurationSequence,
    availability_group: AvailabilityGroupIdentity,
    database_lineage: Option<DatabaseLineage>,
    bootstrap_database_name: Option<SqlIdentifier>,
    bootstrap_database: Option<StandaloneDatabaseIdentity>,
    local_sql_replica_identity: SqlReplicaIdentity,
    local_sql_server_start_incarnation: SqlServerStartIncarnation,
    members: Vec<HealthyTopologyMemberBinding>,
}

impl HealthyTopologyBinding {
    pub fn local_identity(&self) -> &KubericReplicaIdentity {
        &self.local_identity
    }

    pub fn configuration(&self) -> &ConfigurationDescriptor {
        &self.configuration
    }

    pub fn availability_group(&self) -> &AvailabilityGroupIdentity {
        &self.availability_group
    }

    pub fn database_lineage(&self) -> Option<&DatabaseLineage> {
        self.database_lineage.as_ref()
    }

    pub fn members(&self) -> &[HealthyTopologyMemberBinding] {
        &self.members
    }

    pub fn local_sql_replica_identity(&self) -> &SqlReplicaIdentity {
        &self.local_sql_replica_identity
    }

    pub fn local_sql_server_start_incarnation(&self) -> &SqlServerStartIncarnation {
        &self.local_sql_server_start_incarnation
    }

    pub fn validate_snapshot(
        &self,
        config: &ObserverConfig,
        snapshot: &InstanceSnapshot,
        now_unix_millis: u64,
        require_progress_role: bool,
        requested_role: Option<ReplicaRole>,
    ) -> Result<(), KubericAdapterError> {
        let observed_at = snapshot.observed_at_unix_millis;
        let Some(age) = now_unix_millis.checked_sub(observed_at) else {
            return Err(KubericAdapterError::ObservationFromFuture);
        };
        if age > config.max_age_millis() {
            return Err(KubericAdapterError::ObservationStale);
        }
        let group = match &snapshot.availability_group {
            Observation::Present {
                value,
                observed_at_unix_millis,
            } if *observed_at_unix_millis == observed_at => value,
            Observation::Present { .. } => {
                return Err(KubericAdapterError::ObservationInconsistent(
                    "availability-group and instance timestamps differ",
                ));
            }
            Observation::Absent { .. } => {
                return Err(KubericAdapterError::AvailabilityGroupAbsent);
            }
            Observation::Failed(failure) => {
                return Err(KubericAdapterError::ObservationUnavailable(failure.kind));
            }
        };
        validate_eligible_snapshot(config, &snapshot.instance, group, require_progress_role)?;
        validate_bound_evidence(self, config, &snapshot.instance, group, requested_role)?;
        Ok(())
    }

    fn local_member(&self) -> &HealthyTopologyMemberBinding {
        self.members
            .iter()
            .find(|member| member.kuberic_identity == self.local_identity)
            .expect("binding constructor requires one local member")
    }
}

pub struct SqlServerService {
    config: SqlServerServiceConfig,
    source: Arc<dyn SqlServerObservationSource>,
    clock: Arc<dyn ObservationClock>,
    expectation: Option<Arc<SqlServerTopologyExpectation>>,
    bootstrap: Option<BootstrapCapability>,
    open_attempt: tokio::sync::Mutex<()>,
    lifecycle: Mutex<ServiceLifecycle>,
}

#[derive(Clone)]
struct BootstrapCapability {
    config: SqlServerBootstrapConfig,
    executor: Arc<dyn SqlMutationExecutor>,
    seed_root: PathBuf,
}

enum ServiceLifecycle {
    Created,
    Open(Arc<SqlServerReplicator>),
    Closed,
    Aborted,
}

impl SqlServerService {
    pub fn new<E: SqlExecutor + 'static>(
        config: SqlServerServiceConfig,
        manager: SqlServerInstanceManager<E>,
    ) -> Self {
        Self::with_observation_source(config, Arc::new(manager), Arc::new(SystemObservationClock))
    }

    pub fn new_with_topology<E: SqlExecutor + 'static>(
        config: SqlServerServiceConfig,
        manager: SqlServerInstanceManager<E>,
        expectation: SqlServerTopologyExpectation,
    ) -> Result<Self, KubericAdapterError> {
        Self::with_observation_source_and_topology(
            config,
            Arc::new(manager),
            Arc::new(SystemObservationClock),
            expectation,
        )
    }

    pub fn with_observation_source(
        config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
    ) -> Self {
        Self::with_optional_topology(config, source, clock, None, None)
    }

    pub fn with_observation_source_and_topology(
        config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        expectation: SqlServerTopologyExpectation,
    ) -> Result<Self, KubericAdapterError> {
        validate_observer_expectation(source.observer_config(), &expectation)?;
        Ok(Self::with_optional_topology(
            config,
            source,
            clock,
            Some(Arc::new(expectation)),
            None,
        ))
    }

    pub fn with_bootstrap_source_and_topology(
        config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        expectation: SqlServerTopologyExpectation,
        bootstrap: SqlServerBootstrapConfig,
        executor: Arc<dyn SqlMutationExecutor>,
        seed_root: PathBuf,
    ) -> Result<Self, KubericAdapterError> {
        validate_observer_expectation(source.observer_config(), &expectation)?;
        Ok(Self::with_optional_topology(
            config,
            source,
            clock,
            Some(Arc::new(expectation)),
            Some(BootstrapCapability {
                config: bootstrap,
                executor,
                seed_root,
            }),
        ))
    }

    fn with_optional_topology(
        config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        expectation: Option<Arc<SqlServerTopologyExpectation>>,
        bootstrap: Option<BootstrapCapability>,
    ) -> Self {
        Self {
            config,
            source,
            clock,
            expectation,
            bootstrap,
            open_attempt: tokio::sync::Mutex::new(()),
            lifecycle: Mutex::new(ServiceLifecycle::Created),
        }
    }

    pub fn validate_resource_identity(
        &self,
        partition_id: &PartitionId,
    ) -> Result<(), KubericAdapterError> {
        if partition_id.as_str() == self.config.resource_uid.as_str() {
            Ok(())
        } else {
            Err(KubericAdapterError::ResourceIdentityMismatch)
        }
    }

    pub fn replicator(&self) -> Option<Arc<SqlServerReplicator>> {
        match &*self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            ServiceLifecycle::Open(replicator) => Some(replicator.clone()),
            _ => None,
        }
    }
}

#[async_trait]
impl StatefulServiceReplica for SqlServerService {
    async fn open(self: Arc<Self>, context: OpenContext) -> KubericResult<Arc<dyn Replicator>> {
        self.validate_resource_identity(
            &context.partition.get_partition_information().partition_id,
        )?;
        let _attempt = self.open_attempt.lock().await;
        if !matches!(
            *self
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ServiceLifecycle::Created
        ) {
            return Err(KubericRuntimeError::Closed);
        }
        let replicator = Arc::new(if let Some(expectation) = &self.expectation {
            if let Some(bootstrap) = &self.bootstrap {
                SqlServerReplicator::new_bootstrap_with_topology(
                    self.config.clone(),
                    self.source.clone(),
                    self.clock.clone(),
                    context.identity.clone(),
                    expectation.as_ref().clone(),
                    bootstrap.config.clone(),
                    bootstrap.executor.clone(),
                    bootstrap.seed_root.clone(),
                )
                .map_err(KubericRuntimeError::from)?
            } else {
                SqlServerReplicator::new_with_topology(
                    self.config.replication_address.clone(),
                    self.source.clone(),
                    self.clock.clone(),
                    context.identity.clone(),
                    expectation.as_ref().clone(),
                )
                .map_err(KubericRuntimeError::from)?
            }
        } else {
            SqlServerReplicator::new(
                self.config.replication_address.clone(),
                self.source.clone(),
                self.clock.clone(),
            )
        });
        let interfaces = context
            .partition
            .with_factory(Arc::new(SqlServerReplicatorFactory::new(
                replicator.clone(),
            )))
            .create_replicator(None, None)
            .await?;
        debug_assert!(interfaces.state_replicator().is_none());
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*lifecycle, ServiceLifecycle::Created) {
            replicator.abort();
            return Err(KubericRuntimeError::Closed);
        }
        *lifecycle = ServiceLifecycle::Open(replicator);
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> KubericResult<RoleChange> {
        let replicator = match &*self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            ServiceLifecycle::Created => return Err(KubericRuntimeError::NotOpen),
            ServiceLifecycle::Open(replicator) => replicator.clone(),
            ServiceLifecycle::Closed | ServiceLifecycle::Aborted => {
                return Err(KubericRuntimeError::Closed);
            }
        };
        replicator.validate_and_set_role(role).await?;
        Ok(RoleChange {
            service_address: None,
        })
    }

    async fn close(&self) -> KubericResult<()> {
        let replicator = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let replicator = match &*lifecycle {
                ServiceLifecycle::Open(replicator) => Some(replicator.clone()),
                _ => None,
            };
            *lifecycle = ServiceLifecycle::Closed;
            replicator
        };
        let _attempt = self.open_attempt.lock().await;
        if let Some(replicator) = replicator {
            replicator.close().await?;
        }
        Ok(())
    }

    fn abort(&self) {
        let replicator = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let replicator = match &*lifecycle {
                ServiceLifecycle::Open(replicator) => Some(replicator.clone()),
                _ => None,
            };
            *lifecycle = ServiceLifecycle::Aborted;
            replicator
        };
        if let Some(replicator) = replicator {
            replicator.abort();
        }
    }
}

pub struct SqlServerReplicatorFactory {
    replicator: Arc<SqlServerReplicator>,
}

impl SqlServerReplicatorFactory {
    pub fn new(replicator: Arc<SqlServerReplicator>) -> Self {
        Self { replicator }
    }

    pub fn interfaces(&self) -> ReplicatorInterfaces {
        ReplicatorInterfaces::primary(self.replicator.clone(), None)
    }
}

#[async_trait]
impl ReplicatorFactory for SqlServerReplicatorFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        state_provider: Option<Arc<dyn StateProvider>>,
        _settings: ReplicatorSettings,
    ) -> KubericResult<ReplicatorInterfaces> {
        if state_provider.is_some() {
            return Err(KubericRuntimeError::Application(
                "SQL Server owns its native replication state".into(),
            ));
        }
        if let Some(runtime_identity) = &self.replicator.runtime_identity {
            if context.identity()? != *runtime_identity {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "runtime local identity differs from the frozen local member",
                )
                .into());
            }
        }
        Ok(self.interfaces())
    }
}

pub struct SqlServerReplicator {
    replication_address: String,
    source: Arc<dyn SqlServerObservationSource>,
    clock: Arc<dyn ObservationClock>,
    bootstrap: Option<ReplicatorBootstrapCapability>,
    runtime_identity: Option<KubericReplicaIdentity>,
    expectation: Option<Arc<SqlServerTopologyExpectation>>,
    admitted_topology: Mutex<Option<Arc<HealthyTopologyBinding>>>,
    admitted_configuration: Mutex<Option<ReplicaSetConfiguration>>,
    admitted_bootstrap_database: Mutex<Option<StandaloneDatabaseIdentity>>,
    lifecycle: RwLock<ReplicatorLifecycle>,
    role: Mutex<Option<ReplicaRole>>,
    epoch: Mutex<Option<Epoch>>,
    #[cfg(feature = "kuberic-testing")]
    observation_injection: Mutex<Option<TestingObservationInjection>>,
}

#[cfg(feature = "kuberic-testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum TestingBootstrapObservation {
    AvailabilityGroupIdentityOrProfile,
    LogicalReplicaNativeMismatch,
    EndpointOrMemberProfile,
    AssociatedDatabaseIdentityOrRecovery,
    IncompatibleLineage,
    SqlProcessIncarnationReplacement,
}

#[cfg(feature = "kuberic-testing")]
struct TestingObservationInjection {
    scenario: TestingBootstrapObservation,
    snapshot: Option<InstanceSnapshot>,
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Clone)]
struct ReplicatorBootstrapCapability {
    resource_uid: ResourceUid,
    config: SqlServerBootstrapConfig,
    executor: Arc<dyn SqlMutationExecutor>,
    seed: Arc<SeedCoordinator>,
}

struct SeedCoordinator {
    root: PathBuf,
    store: Mutex<Option<Arc<SeedDispatchStore>>>,
    arbiter: tokio::sync::Mutex<()>,
    #[cfg(feature = "kuberic-testing")]
    intent_gate: Mutex<Option<SeedGate>>,
    #[cfg(feature = "kuberic-testing")]
    compaction_gate: Mutex<Option<SeedGate>>,
    #[cfg(feature = "kuberic-testing")]
    completion_gate: Mutex<Option<SeedGate>>,
}

#[cfg(feature = "kuberic-testing")]
struct SeedGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl SeedCoordinator {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            store: Mutex::new(None),
            arbiter: tokio::sync::Mutex::new(()),
            #[cfg(feature = "kuberic-testing")]
            intent_gate: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            compaction_gate: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            completion_gate: Mutex::new(None),
        }
    }

    fn store(&self) -> Result<Arc<SeedDispatchStore>, KubericAdapterError> {
        let mut slot = self
            .store
            .lock()
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?;
        if let Some(store) = slot.as_ref() {
            return Ok(store.clone());
        }
        let store =
            Arc::new(SeedDispatchStore::open(&self.root).map_err(KubericAdapterError::SeedState)?);
        *slot = Some(store.clone());
        Ok(store)
    }

    #[cfg(feature = "kuberic-testing")]
    async fn pause_after_intent_if_requested(&self) -> Result<(), KubericAdapterError> {
        self.pause_if_requested(&self.intent_gate).await
    }

    #[cfg(feature = "kuberic-testing")]
    async fn pause_before_compaction_if_requested(&self) -> Result<(), KubericAdapterError> {
        self.pause_if_requested(&self.compaction_gate).await
    }

    #[cfg(feature = "kuberic-testing")]
    async fn pause_before_completion_if_requested(&self) -> Result<(), KubericAdapterError> {
        self.pause_if_requested(&self.completion_gate).await
    }

    #[cfg(feature = "kuberic-testing")]
    async fn pause_if_requested(
        &self,
        slot: &Mutex<Option<SeedGate>>,
    ) -> Result<(), KubericAdapterError> {
        let gate = slot
            .lock()
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?
            .take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release
                .await
                .map_err(|_| KubericAdapterError::Closed)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct ReplicatorLifecycle {
    state: u8,
    generation: u64,
}

struct SeedBuildContext {
    binding: Arc<HealthyTopologyBinding>,
    database: StandaloneDatabaseIdentity,
    record: SeedDispatchRecord,
    target_native_replica_id: Guid,
}

enum SourceAssociation {
    Absent,
    Incomplete,
    Exact(DatabaseIdentity),
}

impl SqlServerReplicator {
    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_gate_seed_intent(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), KubericAdapterError> {
        let seed = &self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?
            .seed;
        let mut gate = seed
            .intent_gate
            .lock()
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?;
        if gate.is_some() {
            return Err(KubericAdapterError::SeedState(SeedStateError::AlreadyOwned));
        }
        *gate = Some(SeedGate { entered, release });
        Ok(())
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_gate_seed_compaction(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), KubericAdapterError> {
        let seed = &self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?
            .seed;
        let mut gate = seed
            .compaction_gate
            .lock()
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?;
        if gate.is_some() {
            return Err(KubericAdapterError::SeedState(SeedStateError::AlreadyOwned));
        }
        *gate = Some(SeedGate { entered, release });
        Ok(())
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_gate_seed_completion(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), KubericAdapterError> {
        let seed = &self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?
            .seed;
        let mut gate = seed
            .completion_gate
            .lock()
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?;
        if gate.is_some() {
            return Err(KubericAdapterError::SeedState(SeedStateError::AlreadyOwned));
        }
        *gate = Some(SeedGate { entered, release });
        Ok(())
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_inject_bootstrap_observation(
        &self,
        scenario: TestingBootstrapObservation,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), KubericAdapterError> {
        let mut injection = self
            .observation_injection
            .lock()
            .map_err(|_| KubericAdapterError::NativeConvergenceUnsafe("test observation seam"))?;
        if injection.is_some() {
            return Err(KubericAdapterError::SeedState(SeedStateError::AlreadyOwned));
        }
        *injection = Some(TestingObservationInjection {
            scenario,
            snapshot: None,
            entered,
            release,
        });
        Ok(())
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_inject_incarnation_observation(
        &self,
        snapshot: InstanceSnapshot,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), KubericAdapterError> {
        let mut injection = self
            .observation_injection
            .lock()
            .map_err(|_| KubericAdapterError::NativeConvergenceUnsafe("test observation seam"))?;
        if injection.is_some() {
            return Err(KubericAdapterError::SeedState(SeedStateError::AlreadyOwned));
        }
        *injection = Some(TestingObservationInjection {
            scenario: TestingBootstrapObservation::SqlProcessIncarnationReplacement,
            snapshot: Some(snapshot),
            entered,
            release,
        });
        Ok(())
    }

    pub fn new(
        replication_address: String,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
    ) -> Self {
        Self::with_parts(replication_address, source, clock, None, None, None)
    }

    pub fn new_with_topology(
        replication_address: String,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        runtime_identity: KubericReplicaIdentity,
        expectation: SqlServerTopologyExpectation,
    ) -> Result<Self, KubericAdapterError> {
        if runtime_identity.replica_id != expectation.local_replica_id() {
            return Err(KubericAdapterError::InvalidConfiguration(
                "runtime identity must match the local topology replica",
            ));
        }
        validate_observer_expectation(source.observer_config(), &expectation)?;
        Ok(Self::with_parts(
            replication_address,
            source,
            clock,
            Some(runtime_identity),
            Some(Arc::new(expectation)),
            None,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_bootstrap_with_topology(
        service_config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        runtime_identity: KubericReplicaIdentity,
        expectation: SqlServerTopologyExpectation,
        bootstrap: SqlServerBootstrapConfig,
        executor: Arc<dyn SqlMutationExecutor>,
        seed_root: PathBuf,
    ) -> Result<Self, KubericAdapterError> {
        if runtime_identity.replica_id != expectation.local_replica_id() {
            return Err(KubericAdapterError::InvalidConfiguration(
                "runtime identity must match the local topology replica",
            ));
        }
        validate_observer_expectation(source.observer_config(), &expectation)?;
        Ok(Self::with_parts(
            service_config.replication_address,
            source,
            clock,
            Some(runtime_identity),
            Some(Arc::new(expectation)),
            Some(ReplicatorBootstrapCapability {
                resource_uid: service_config.resource_uid,
                config: bootstrap,
                executor,
                seed: Arc::new(SeedCoordinator::new(seed_root)),
            }),
        ))
    }

    fn with_parts(
        replication_address: String,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        runtime_identity: Option<KubericReplicaIdentity>,
        expectation: Option<Arc<SqlServerTopologyExpectation>>,
        bootstrap: Option<ReplicatorBootstrapCapability>,
    ) -> Self {
        Self {
            replication_address,
            source,
            clock,
            bootstrap,
            runtime_identity,
            expectation,
            admitted_topology: Mutex::new(None),
            admitted_configuration: Mutex::new(None),
            admitted_bootstrap_database: Mutex::new(None),
            lifecycle: RwLock::new(ReplicatorLifecycle {
                state: CREATED,
                generation: 0,
            }),
            role: Mutex::new(None),
            epoch: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            observation_injection: Mutex::new(None),
        }
    }

    pub fn admitted_topology(&self) -> Option<Arc<HealthyTopologyBinding>> {
        self.admitted_topology
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn begin_operation(&self) -> Result<u64, KubericAdapterError> {
        let lifecycle = self
            .lifecycle
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match lifecycle.state {
            OPEN => Ok(lifecycle.generation),
            CREATED => Err(KubericAdapterError::NotOpen),
            CLOSED | ABORTED => Err(KubericAdapterError::Closed),
            _ => unreachable!("SQL Server replicator lifecycle is invalid"),
        }
    }

    fn finish_operation(
        &self,
        generation: u64,
    ) -> Result<RwLockReadGuard<'_, ReplicatorLifecycle>, KubericAdapterError> {
        let lifecycle = self
            .lifecycle
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.state != OPEN || lifecycle.generation != generation {
            return Err(KubericAdapterError::Closed);
        }
        Ok(lifecycle)
    }

    async fn observed_group(
        &self,
        generation: u64,
        require_progress_role: bool,
        requested_role: Option<ReplicaRole>,
    ) -> Result<(AvailabilityGroupSnapshot, u64), KubericAdapterError> {
        #[cfg(feature = "kuberic-testing")]
        let injected = self.take_testing_incarnation_observation().await?;
        #[cfg(feature = "kuberic-testing")]
        let observation = match injected {
            Some(snapshot) => Observation::Present {
                observed_at_unix_millis: snapshot.observed_at_unix_millis,
                value: snapshot,
            },
            None => self
                .source
                .observe()
                .await
                .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?,
        };
        #[cfg(not(feature = "kuberic-testing"))]
        let observation = self
            .source
            .observe()
            .await
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
        drop(self.finish_operation(generation)?);
        let (snapshot, observed_at) = match observation {
            Observation::Present {
                value,
                observed_at_unix_millis,
            } => (value, observed_at_unix_millis),
            Observation::Absent { .. } => {
                return Err(KubericAdapterError::AvailabilityGroupAbsent);
            }
            Observation::Failed(failure) => {
                return Err(KubericAdapterError::ObservationUnavailable(failure.kind));
            }
        };
        if snapshot.observed_at_unix_millis != observed_at {
            return Err(KubericAdapterError::ObservationInconsistent(
                "snapshot and attempt timestamps differ",
            ));
        }
        self.ensure_observation_fresh(observed_at)?;
        let group = match snapshot.availability_group {
            Observation::Present {
                value,
                observed_at_unix_millis,
            } if observed_at_unix_millis == observed_at => value,
            Observation::Present { .. } => {
                return Err(KubericAdapterError::ObservationInconsistent(
                    "availability-group and instance timestamps differ",
                ));
            }
            Observation::Absent { .. } => {
                return Err(KubericAdapterError::AvailabilityGroupAbsent);
            }
            Observation::Failed(failure) => {
                return Err(KubericAdapterError::ObservationUnavailable(failure.kind));
            }
        };
        validate_eligible_snapshot(
            self.source.observer_config(),
            &snapshot.instance,
            &group,
            require_progress_role,
        )?;
        let admitted = self.admitted_topology();
        if let Some(binding) = &admitted {
            validate_bound_evidence(
                binding,
                self.source.observer_config(),
                &snapshot.instance,
                &group,
                requested_role,
            )?;
            if require_progress_role {
                if let Some(role) = *self
                    .role
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                {
                    if !published_role_matches(binding, role) {
                        return Err(KubericAdapterError::TopologyBindingMismatch(
                            "published local role differs from the frozen stable role",
                        ));
                    }
                }
            }
        } else if require_progress_role {
            if let Some(role) = *self
                .role
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                validate_native_role(role, group.local_replica.role.clone())?;
            }
        }
        Ok((group, observed_at))
    }

    fn ensure_observation_fresh(&self, observed_at: u64) -> Result<(), KubericAdapterError> {
        let now = self
            .clock
            .now_unix_millis()
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
        let Some(age) = now.checked_sub(observed_at) else {
            return Err(KubericAdapterError::ObservationFromFuture);
        };
        if age > self.source.observer_config().max_age_millis() {
            return Err(KubericAdapterError::ObservationStale);
        }
        Ok(())
    }

    async fn bootstrap_observation(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            crate::observation::InstanceMetadata,
            Option<AvailabilityGroupSnapshot>,
            u64,
        ),
        KubericAdapterError,
    > {
        let remaining = bootstrap_remaining(deadline)?;
        let observation = tokio::time::timeout(remaining, self.source.observe()).await;
        drop(self.finish_operation(generation)?);
        let observation = observation
            .map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
        let (snapshot, observed_at) = present_snapshot(observation)?;
        self.ensure_observation_fresh(observed_at)?;
        validate_bootstrap_instance(self.source.observer_config(), &snapshot.instance)?;
        let group = match snapshot.availability_group {
            Observation::Present {
                value,
                observed_at_unix_millis,
            } if observed_at_unix_millis == observed_at => Some(value),
            Observation::Present { .. } => {
                return Err(KubericAdapterError::ObservationInconsistent(
                    "availability-group and instance timestamps differ",
                ));
            }
            Observation::Absent { .. } => None,
            Observation::Failed(failure) => {
                return Err(KubericAdapterError::ObservationUnavailable(failure.kind));
            }
        };
        Ok((snapshot.instance, group, observed_at))
    }

    #[cfg(feature = "kuberic-testing")]
    async fn inject_testing_observation(
        &self,
        group: &mut AvailabilityGroupSnapshot,
    ) -> Result<(), KubericAdapterError> {
        let injection = self
            .observation_injection
            .lock()
            .map_err(|_| KubericAdapterError::NativeConvergenceUnsafe("test observation seam"))?
            .take();
        let Some(injection) = injection else {
            return Ok(());
        };
        inject_testing_bootstrap_snapshot(group, injection.scenario)?;
        let _ = injection.entered.send(());
        injection
            .release
            .await
            .map_err(|_| KubericAdapterError::Closed)
    }

    #[cfg(feature = "kuberic-testing")]
    async fn take_testing_incarnation_observation(
        &self,
    ) -> Result<Option<InstanceSnapshot>, KubericAdapterError> {
        let injection = {
            let mut slot = self.observation_injection.lock().map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("test observation seam")
            })?;
            if slot.as_ref().is_some_and(|injection| {
                injection.scenario == TestingBootstrapObservation::SqlProcessIncarnationReplacement
                    && injection.snapshot.is_some()
            }) {
                slot.take()
            } else {
                None
            }
        };
        let Some(mut injection) = injection else {
            return Ok(None);
        };
        let snapshot =
            injection
                .snapshot
                .take()
                .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                    "test observation seam",
                ))?;
        let _ = injection.entered.send(());
        injection
            .release
            .await
            .map_err(|_| KubericAdapterError::Closed)?;
        Ok(Some(snapshot))
    }

    async fn execute_bootstrap_effect(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        executor: &dyn SqlMutationExecutor,
        effect: &MutationEffect,
    ) -> Result<bool, KubericAdapterError> {
        let remaining = bootstrap_remaining(deadline)?;
        let result = tokio::time::timeout(remaining, executor.execute(effect)).await;
        drop(self.finish_operation(generation)?);
        let result = result.map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?;
        match result {
            Ok(()) => Ok(true),
            Err(error) if effect.is_retryable_propagation_error(&error) => Ok(false),
            Err(error) => Err(KubericAdapterError::NativeMutationFailed {
                effect: effect.label(),
                kind: error.kind,
                server_code: error.server_code,
            }),
        }
    }

    async fn observe_bootstrap_database(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        executor: &dyn SqlMutationExecutor,
        database: &SqlIdentifier,
    ) -> Result<Option<crate::bootstrap::StandaloneDatabaseSnapshot>, KubericAdapterError> {
        let remaining = bootstrap_remaining(deadline)?;
        let result =
            tokio::time::timeout(remaining, executor.observe_standalone_database(database)).await;
        drop(self.finish_operation(generation)?);
        result
            .map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))
    }

    async fn converge_bootstrap_configuration(
        &self,
        generation: u64,
        current: &ReplicaSetConfiguration,
    ) -> Result<
        (
            crate::observation::InstanceMetadata,
            AvailabilityGroupSnapshot,
            u64,
            Option<crate::bootstrap::StandaloneDatabaseIdentity>,
            tokio::time::Instant,
        ),
        KubericAdapterError,
    > {
        let runtime_identity = self
            .runtime_identity
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let expectation = self
            .expectation
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let bootstrap = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        validate_configuration_shape(
            runtime_identity,
            expectation,
            &self.replication_address,
            current,
        )?;
        let ownership = classify_bootstrap_ownership(runtime_identity, current).map_err(|_| {
            KubericAdapterError::TopologyBindingMismatch(
                "local bootstrap ownership is not represented by the current configuration",
            )
        })?;
        let descriptors = bootstrap_descriptors(expectation, current)?;
        let expected_ownership = StandaloneDatabaseOwnership {
            resource_id: bootstrap.resource_uid.to_string(),
            configuration_id: current.configuration.configuration_id.to_string(),
        };
        let create_database = MutationEffect::create_source_database(
            bootstrap.config.database.clone(),
            &expected_ownership.resource_id,
            &expected_ownership.configuration_id,
        )
        .map_err(|_| {
            KubericAdapterError::InvalidConfiguration(
                "bootstrap database ownership markers are invalid",
            )
        })?;
        let create_group = MutationEffect::create_availability_group(
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
            descriptors.clone(),
        )
        .map_err(|_| {
            KubericAdapterError::InvalidConfiguration("bootstrap replica descriptors are invalid")
        })?;
        let join_group = MutationEffect::join_availability_group(
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
        );
        let deadline = tokio::time::Instant::now() + self.source.observer_config().sample_timeout();
        let mut join_absence_observed = false;
        loop {
            drop(self.finish_operation(generation)?);
            bootstrap_remaining(deadline)?;
            let database = self
                .observe_bootstrap_database(
                    generation,
                    deadline,
                    bootstrap.executor.as_ref(),
                    bootstrap.config.database(),
                )
                .await?;
            let database_state =
                classify_standalone_database(database.as_ref(), None, &expected_ownership, false);
            let database_identity = database
                .as_ref()
                .and_then(|database| database.identity.clone());
            match ownership {
                BootstrapOwnership::PrimaryTopology => {
                    match database_state {
                        NativeConvergenceState::Absent | NativeConvergenceState::Incomplete => {
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                &create_database,
                            )
                            .await?;
                            continue;
                        }
                        NativeConvergenceState::Complete => {}
                        NativeConvergenceState::Conflicting => {
                            return Err(KubericAdapterError::NativeConvergenceConflict(
                                "standalone source database",
                            ));
                        }
                        NativeConvergenceState::Unsafe => {
                            return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                "standalone source database",
                            ));
                        }
                    }
                    let (instance, group, observed_at) =
                        match self.bootstrap_observation(generation, deadline).await {
                            Ok(observation) => observation,
                            Err(error) if is_retryable_bootstrap_observation(&error) => {
                                self.wait_for_bootstrap_retry(generation, deadline).await?;
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                    let Some(group) = group else {
                        self.execute_bootstrap_effect(
                            generation,
                            deadline,
                            bootstrap.executor.as_ref(),
                            &create_group,
                        )
                        .await?;
                        continue;
                    };
                    match classify_availability_group(
                        Some(&group),
                        &self.source.observer_config().target().availability_group,
                        None,
                        &descriptors,
                        bootstrap.config.database(),
                        database_identity.as_ref(),
                        false,
                    ) {
                        NativeConvergenceState::Complete
                            if bootstrap_membership_ready(&group, ownership, &descriptors) =>
                        {
                            drop(self.finish_operation(generation)?);
                            bootstrap_remaining(deadline)?;
                            return Ok((instance, group, observed_at, database_identity, deadline));
                        }
                        NativeConvergenceState::Absent
                        | NativeConvergenceState::Incomplete
                        | NativeConvergenceState::Complete => {}
                        NativeConvergenceState::Conflicting => {
                            return Err(KubericAdapterError::NativeConvergenceConflict(
                                "availability group",
                            ));
                        }
                        NativeConvergenceState::Unsafe => {
                            return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                "availability group",
                            ));
                        }
                    }
                }
                BootstrapOwnership::LocalJoin => {
                    match database_state {
                        NativeConvergenceState::Absent
                        | NativeConvergenceState::Incomplete
                        | NativeConvergenceState::Complete => {}
                        NativeConvergenceState::Conflicting => {
                            return Err(KubericAdapterError::NativeConvergenceConflict(
                                "managed database ownership",
                            ));
                        }
                        NativeConvergenceState::Unsafe => {
                            return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                "managed database ownership",
                            ));
                        }
                    }
                    let (instance, group, observed_at) =
                        match self.bootstrap_observation(generation, deadline).await {
                            Ok(observation) => observation,
                            Err(error) if is_retryable_bootstrap_observation(&error) => {
                                self.wait_for_bootstrap_retry(generation, deadline).await?;
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                    let Some(group) = group else {
                        if join_absence_observed {
                            let dispatched = self
                                .execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &join_group,
                                )
                                .await?;
                            if dispatched {
                                continue;
                            }
                        } else {
                            join_absence_observed = true;
                        }
                        self.wait_for_bootstrap_retry(generation, deadline).await?;
                        continue;
                    };
                    match classify_availability_group(
                        Some(&group),
                        &self.source.observer_config().target().availability_group,
                        None,
                        &descriptors,
                        bootstrap.config.database(),
                        database_identity.as_ref(),
                        false,
                    ) {
                        NativeConvergenceState::Conflicting => {
                            return Err(KubericAdapterError::NativeConvergenceConflict(
                                "availability group",
                            ));
                        }
                        NativeConvergenceState::Unsafe => {
                            return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                "availability group",
                            ));
                        }
                        NativeConvergenceState::Absent
                        | NativeConvergenceState::Incomplete
                        | NativeConvergenceState::Complete => {}
                    }
                    let local = expectation.local_member();
                    match classify_replica_join(
                        Some(&group),
                        &group.identity,
                        &group.local_replica.identity,
                        local.server_name(),
                        local.endpoint_url(),
                        false,
                    ) {
                        NativeConvergenceState::Complete
                            if bootstrap_membership_ready(&group, ownership, &descriptors) =>
                        {
                            drop(self.finish_operation(generation)?);
                            bootstrap_remaining(deadline)?;
                            return Ok((instance, group, observed_at, database_identity, deadline));
                        }
                        NativeConvergenceState::Absent | NativeConvergenceState::Incomplete => {
                            let dispatched = self
                                .execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &join_group,
                                )
                                .await?;
                            if !dispatched {
                                self.wait_for_bootstrap_retry(generation, deadline).await?;
                            }
                            continue;
                        }
                        NativeConvergenceState::Complete => {}
                        NativeConvergenceState::Conflicting => {
                            return Err(KubericAdapterError::NativeConvergenceConflict(
                                "local replica join",
                            ));
                        }
                        NativeConvergenceState::Unsafe => {
                            return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                "local replica join",
                            ));
                        }
                    }
                }
            }
            self.wait_for_bootstrap_retry(generation, deadline).await?;
        }
    }

    async fn wait_for_bootstrap_retry(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), KubericAdapterError> {
        let remaining = bootstrap_remaining(deadline)?;
        tokio::time::timeout(
            remaining,
            tokio::time::sleep(
                self.source
                    .observer_config()
                    .poll_interval()
                    .min(Duration::from_millis(100))
                    .min(remaining),
            ),
        )
        .await
        .map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?;
        drop(self.finish_operation(generation)?);
        bootstrap_remaining(deadline)?;
        Ok(())
    }

    fn seed_build_context(
        &self,
        replica: &ReplicaInformation,
    ) -> Result<SeedBuildContext, KubericAdapterError> {
        let bootstrap =
            self.bootstrap
                .as_ref()
                .ok_or(KubericAdapterError::UnsupportedOperation(
                    ObserveOnlyOperation::BuildReplica,
                ))?;
        if replica.build_id.is_empty() {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "target build identity is empty",
            ));
        }
        let binding = self
            .admitted_topology()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let admitted_configuration = self
            .admitted_configuration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        if admitted_configuration.configuration != binding.configuration {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "admitted build configuration differs from the topology binding",
            ));
        }
        let database = self
            .admitted_bootstrap_database
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let source = binding.local_member();
        if source.kuberic_identity.replica_id != binding.configuration.primary_id
            || source.stable_role != ReplicaRole::Primary
            || *self
                .role
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                != Some(ReplicaRole::Primary)
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "build-owned seeding requires the exact configured primary",
            ));
        }
        let target = binding
            .members
            .iter()
            .find(|member| member.kuberic_identity == replica.identity)
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "target build identity is not admitted",
            ))?;
        if target.stable_role != ReplicaRole::ActiveSecondary
            || target.process_session_id.is_empty()
            || replica.process_session_id != target.process_session_id
            || replica.replication_address != target.replication_address
            || !matches!(
                replica.role,
                ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary
            )
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "target build session, address, or role differs from the admitted configuration",
            ));
        }
        let source_record = seed_member(source)?;
        let targets: [SeedDispatchTarget; 2] = binding
            .members
            .iter()
            .filter(|member| member.stable_role == ReplicaRole::ActiveSecondary)
            .map(seed_member)
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| {
                KubericAdapterError::TopologyBindingMismatch(
                    "seed dispatch requires exactly two configured targets",
                )
            })?;
        if targets
            .iter()
            .any(|target| target.process_session_id().is_empty())
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "both configured target sessions must be current before seed dispatch",
            ));
        }
        let target_native_replica_id = target.native_replica_id.clone();
        let record = SeedDispatchRecord::new(
            bootstrap.resource_uid.to_string(),
            binding.configuration.configuration_id.to_string(),
            binding.availability_group.name.as_str(),
            binding.availability_group.group_id.as_str(),
            database.name.as_str(),
            database.database_guid.as_str(),
            database.family_guid.as_str(),
            database.recovery_fork_guid.as_str(),
            source_record,
            targets,
            replica.identity.replica_id.to_string(),
            replica.build_id.to_string(),
            binding.local_sql_server_start_incarnation.as_str(),
        )
        .map_err(KubericAdapterError::SeedState)?;
        Ok(SeedBuildContext {
            binding,
            database,
            record,
            target_native_replica_id,
        })
    }

    async fn observe_seed_group(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        context: &SeedBuildContext,
    ) -> Result<AvailabilityGroupSnapshot, KubericAdapterError> {
        let (instance, group, _) = self.bootstrap_observation(generation, deadline).await?;
        #[allow(unused_mut)]
        let mut group = group.ok_or(KubericAdapterError::AvailabilityGroupAbsent)?;
        #[cfg(feature = "kuberic-testing")]
        self.inject_testing_observation(&mut group).await?;
        if let Err(error) =
            validate_eligible_snapshot(self.source.observer_config(), &instance, &group, false)
        {
            return Err(match error {
                KubericAdapterError::UnsupportedProfile(_) => {
                    KubericAdapterError::NativeConvergenceConflict(
                        "seed build native availability-group profile",
                    )
                }
                other => other,
            });
        }
        validate_seed_binding_evidence(&context.binding, &instance, &group)?;
        Ok(group)
    }

    fn validate_seed_record(
        &self,
        actual: &SeedDispatchRecord,
        expected: &SeedDispatchRecord,
    ) -> Result<(), KubericAdapterError> {
        if actual.same_dispatch_context(expected) {
            Ok(())
        } else {
            Err(KubericAdapterError::SeedState(
                SeedStateError::IdentityMismatch,
            ))
        }
    }

    fn compact_seed_record(
        &self,
        store: &SeedDispatchStore,
        mut record: SeedDispatchRecord,
        database: &DatabaseIdentity,
    ) -> Result<(), KubericAdapterError> {
        if record.phase() != SeedDispatchPhase::Dispatched {
            return Err(KubericAdapterError::SeedState(
                SeedStateError::InvalidTransition,
            ));
        }
        if record.group_database_id() != Some(database.group_database_id.as_str()) {
            record
                .associate_group_database(database.group_database_id.as_str())
                .map_err(KubericAdapterError::SeedState)?;
            record = store
                .replace(record.revision(), record)
                .map_err(KubericAdapterError::SeedState)?;
        }
        store
            .remove(record.revision())
            .map_err(KubericAdapterError::SeedState)
    }

    async fn arbitrate_seed_dispatch(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        context: &SeedBuildContext,
    ) -> Result<Option<tokio::task::JoinHandle<Result<(), RuntimeError>>>, KubericAdapterError>
    {
        let bootstrap = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let _arbiter = bootstrap.seed.arbiter.lock().await;
        drop(self.finish_operation(generation)?);
        let group = self
            .observe_seed_group(generation, deadline, context)
            .await?;
        if !matches!(
            source_association(&group, &context.database)?,
            SourceAssociation::Absent
        ) {
            return Ok(None);
        }
        let store = bootstrap.seed.store()?;
        let existing = store.load().map_err(KubericAdapterError::SeedState)?;
        let mut record = match existing {
            Some(record) => {
                self.validate_seed_record(&record, &context.record)?;
                match record.phase() {
                    SeedDispatchPhase::Dispatched => return Ok(None),
                    SeedDispatchPhase::Intent if record.same_intent_owner(&context.record) => {
                        record
                    }
                    SeedDispatchPhase::Intent => store
                        .replace_intent(record.revision(), context.record.clone())
                        .map_err(KubericAdapterError::SeedState)?,
                }
            }
            None => {
                store
                    .create(&context.record)
                    .map_err(KubericAdapterError::SeedState)?;
                context.record.clone()
            }
        };
        #[cfg(feature = "kuberic-testing")]
        bootstrap.seed.pause_after_intent_if_requested().await?;
        drop(self.finish_operation(generation)?);
        record
            .mark_dispatched()
            .map_err(KubericAdapterError::SeedState)?;
        store
            .replace(record.revision(), record)
            .map_err(KubericAdapterError::SeedState)?;
        let executor = bootstrap.executor.clone();
        let effect = MutationEffect::add_database(
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
            bootstrap.config.database.clone(),
        );
        Ok(Some(tokio::spawn(async move {
            executor.execute(&effect).await
        })))
    }

    async fn converge_seed_build(
        &self,
        generation: u64,
        replica: &ReplicaInformation,
    ) -> Result<(), KubericAdapterError> {
        let context = self.seed_build_context(replica)?;
        let bootstrap = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let deadline = tokio::time::Instant::now() + self.source.observer_config().sample_timeout();
        let mut dispatch_checked = false;
        loop {
            drop(self.finish_operation(generation)?);
            bootstrap_remaining(deadline)?;
            let group = match self
                .observe_seed_group(generation, deadline, &context)
                .await
            {
                Ok(group) => group,
                Err(error) if is_retryable_bootstrap_observation(&error) => {
                    self.wait_for_bootstrap_retry(generation, deadline).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let association = source_association(&group, &context.database)?;
            let store = bootstrap.seed.store()?;
            let record = store.load().map_err(KubericAdapterError::SeedState)?;
            if let Some(record) = record.as_ref() {
                self.validate_seed_record(record, &context.record)?;
            }
            match association {
                SourceAssociation::Exact(database) => {
                    #[cfg(feature = "kuberic-testing")]
                    if record.is_some() {
                        bootstrap
                            .seed
                            .pause_before_compaction_if_requested()
                            .await?;
                        drop(self.finish_operation(generation)?);
                    }
                    if let Some(record) = record {
                        self.compact_seed_record(store.as_ref(), record, &database)?;
                    }
                    match classify_source_seed_completion(
                        &group,
                        &database,
                        &context.database,
                        &context.target_native_replica_id,
                    ) {
                        NativeConvergenceState::Complete => {
                            #[cfg(feature = "kuberic-testing")]
                            bootstrap
                                .seed
                                .pause_before_completion_if_requested()
                                .await?;
                            drop(self.finish_operation(generation)?);
                            return Ok(());
                        }
                        NativeConvergenceState::Conflicting => {
                            return Err(KubericAdapterError::NativeConvergenceConflict(
                                "target replica seed",
                            ));
                        }
                        NativeConvergenceState::Unsafe => {
                            return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                "target replica seed",
                            ));
                        }
                        NativeConvergenceState::Absent | NativeConvergenceState::Incomplete => {}
                    }
                }
                SourceAssociation::Incomplete => {}
                SourceAssociation::Absent if !dispatch_checked => {
                    dispatch_checked = true;
                    let dispatch = match self
                        .arbitrate_seed_dispatch(generation, deadline, &context)
                        .await
                    {
                        Ok(dispatch) => dispatch,
                        Err(error) if is_retryable_bootstrap_observation(&error) => {
                            dispatch_checked = false;
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if let Some(mut dispatch) = dispatch {
                        let result =
                            tokio::time::timeout(bootstrap_remaining(deadline)?, &mut dispatch)
                                .await
                                .map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?
                                .map_err(|_| {
                                    KubericAdapterError::NativeConvergenceUnsafe(
                                        "seed dispatch task terminated",
                                    )
                                })?;
                        drop(self.finish_operation(generation)?);
                        result.map_err(|error| KubericAdapterError::NativeMutationFailed {
                            effect: "add database to availability group",
                            kind: error.kind,
                            server_code: error.server_code,
                        })?;
                    }
                    continue;
                }
                SourceAssociation::Absent => {}
            }
            self.wait_for_bootstrap_retry(generation, deadline).await?;
        }
    }

    async fn validate_and_set_role(&self, role: ReplicaRole) -> KubericResult<()> {
        let generation = self.begin_operation()?;
        if self.bootstrap.is_some() && self.admitted_topology().is_none() {
            if !matches!(role, ReplicaRole::Primary | ReplicaRole::ActiveSecondary) {
                return Err(KubericAdapterError::NativeRoleMismatch {
                    requested: role,
                    observed: None,
                }
                .into());
            }
            drop(self.finish_operation(generation)?);
            self.set_role(role);
            return Ok(());
        }
        let (group, observed_at) = self.observed_group(generation, false, Some(role)).await?;
        let _lifecycle = self.finish_operation(generation)?;
        if self.expectation.is_some() && self.admitted_topology().is_none() {
            return Err(KubericAdapterError::TopologyNotAdmitted.into());
        }
        if self.expectation.is_none() {
            validate_native_role(role, group.local_replica.role)?;
        }
        self.ensure_observation_fresh(observed_at)?;
        self.set_role(role);
        Ok(())
    }

    fn set_role(&self, role: ReplicaRole) {
        *self
            .role
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(role);
    }

    fn unsupported<T>(&self, operation: ObserveOnlyOperation) -> KubericResult<T> {
        Err(KubericAdapterError::UnsupportedOperation(operation).into())
    }
}

#[async_trait]
impl Replicator for SqlServerReplicator {
    async fn open(&self) -> KubericResult<String> {
        let mut lifecycle = self
            .lifecycle
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.state != CREATED {
            return Err(KubericRuntimeError::Closed);
        }
        lifecycle.state = OPEN;
        lifecycle.generation = lifecycle.generation.wrapping_add(1);
        Ok(self.replication_address.clone())
    }

    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> KubericResult<()> {
        let generation = self.begin_operation()?;
        let admitted = self.admitted_topology();
        let observed_at = if let Some(binding) = &admitted {
            let (_, observed_at) = self.observed_group(generation, false, Some(role)).await?;
            if epoch != binding.configuration.epoch {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "role-change epoch differs from the frozen epoch",
                )
                .into());
            }
            observed_at
        } else if self.expectation.is_some() {
            return Err(KubericAdapterError::TopologyNotAdmitted.into());
        } else {
            let (group, observed_at) = self.observed_group(generation, false, Some(role)).await?;
            validate_native_role(role, group.local_replica.role)?;
            observed_at
        };
        let _lifecycle = self.finish_operation(generation)?;
        self.ensure_observation_fresh(observed_at)?;
        self.set_role(role);
        *self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(epoch);
        Ok(())
    }

    async fn update_epoch(&self, epoch: Epoch) -> KubericResult<()> {
        let generation = self.begin_operation()?;
        let admitted = self.admitted_topology();
        let observed_at = if let Some(binding) = &admitted {
            let (_, observed_at) = self.observed_group(generation, false, None).await?;
            if epoch != binding.configuration.epoch {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "updated epoch differs from the frozen epoch",
                )
                .into());
            }
            Some(observed_at)
        } else if self.expectation.is_some() {
            return Err(KubericAdapterError::TopologyNotAdmitted.into());
        } else {
            None
        };
        let _lifecycle = self.finish_operation(generation)?;
        if let Some(observed_at) = observed_at {
            self.ensure_observation_fresh(observed_at)?;
        }
        *self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(epoch);
        Ok(())
    }

    async fn close(&self) -> KubericResult<()> {
        let mut lifecycle = self
            .lifecycle
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lifecycle.state = CLOSED;
        lifecycle.generation = lifecycle.generation.wrapping_add(1);
        Ok(())
    }

    fn abort(&self) {
        let mut lifecycle = self
            .lifecycle
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lifecycle.state = ABORTED;
        lifecycle.generation = lifecycle.generation.wrapping_add(1);
    }

    async fn current_progress(&self) -> KubericResult<i64> {
        let generation = self.begin_operation()?;
        if self.bootstrap.is_some() && self.admitted_topology().is_none() {
            drop(self.finish_operation(generation)?);
            return Ok(0);
        }
        let (group, observed_at) = self.observed_group(generation, true, None).await?;
        let value = self.admitted_topology().map_or_else(
            || group.configuration_sequence.value(),
            |binding| {
                if binding.database_lineage().is_none() {
                    binding.configuration().epoch.configuration_number
                } else {
                    group.configuration_sequence.value()
                }
            },
        );
        let _lifecycle = self.finish_operation(generation)?;
        self.ensure_observation_fresh(observed_at)?;
        Ok(value)
    }

    async fn catch_up_capability(&self) -> KubericResult<i64> {
        let generation = self.begin_operation()?;
        let Some(binding) = self.admitted_topology() else {
            if self.expectation.is_some() {
                return Err(KubericAdapterError::TopologyNotAdmitted.into());
            }
            return self.unsupported(ObserveOnlyOperation::CatchUpCapability);
        };

        {
            let _lifecycle = self.finish_operation(generation)?;
            if self
                .role
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none_or(|role| !published_role_matches(&binding, role))
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "capability requires the frozen stable local role to be published",
                )
                .into());
            }
        }
        let (group, observed_at) = self.observed_group(generation, true, None).await?;
        let value = if binding.database_lineage().is_none() {
            binding.configuration().epoch.configuration_number
        } else {
            group.configuration_sequence.value()
        };
        self.ensure_observation_fresh(observed_at)?;
        let _lifecycle = self.finish_operation(generation)?;
        if self
            .role
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none_or(|role| !published_role_matches(&binding, role))
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "capability requires the frozen stable local role to be published",
            )
            .into());
        }
        self.ensure_observation_fresh(observed_at)?;
        Ok(value)
    }
}

#[async_trait]
impl PrimaryReplicator for SqlServerReplicator {
    async fn on_data_loss(&self) -> KubericResult<bool> {
        self.unsupported(ObserveOnlyOperation::DataLoss)
    }

    async fn update_catch_up_replica_set_configuration(
        &self,
        _current: ReplicaSetConfiguration,
        _previous: ReplicaSetConfiguration,
    ) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::CatchUpConfiguration)
    }

    async fn wait_for_catch_up_quorum(&self, _mode: ReplicaSetQuorumMode) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::CatchUpQuorum)
    }

    async fn update_current_replica_set_configuration(
        &self,
        current: ReplicaSetConfiguration,
    ) -> KubericResult<()> {
        let Some(expectation) = &self.expectation else {
            return self.unsupported(ObserveOnlyOperation::CurrentConfiguration);
        };
        let runtime_identity = self
            .runtime_identity
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let current = if self.bootstrap.is_some() {
            normalize_bootstrap_configuration(current)?
        } else {
            current
        };
        let generation = self.begin_operation()?;
        if self.bootstrap.is_some() && current.replicas.len() < usize::from(SUPPORTED_REPLICA_COUNT)
        {
            let mut normalized = current.clone();
            for member in &current.configuration.members {
                if normalized
                    .replicas
                    .iter()
                    .any(|replica| replica.identity == member.identity)
                {
                    continue;
                }
                let mut replica = ReplicaInformation::new(
                    kuberic_runtime::protocol::types::OperationId::default(),
                    member.identity.clone(),
                    if member.identity == *runtime_identity {
                        self.replication_address.clone()
                    } else {
                        String::new()
                    },
                );
                replica.role = member.role;
                normalized.replicas.push(replica);
            }
            validate_configuration_shape(
                runtime_identity,
                expectation,
                &self.replication_address,
                &normalized,
            )?;
            drop(self.finish_operation(generation)?);
            return Ok(());
        }
        if self.bootstrap.is_some()
            && let Some(binding) = self.admitted_topology()
            && build_enriched_configuration(&binding, &current)?
        {
            let (_, observed_at) = self.observed_group(generation, false, None).await?;
            drop(self.finish_operation(generation)?);
            self.ensure_observation_fresh(observed_at)?;
            return Ok(());
        }

        let (instance, group, observed_at, bootstrap_database, bootstrap_deadline) =
            if self.bootstrap.is_some() {
                self.converge_bootstrap_configuration(generation, &current)
                    .await?
            } else {
                let observation = self
                    .source
                    .observe()
                    .await
                    .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
                drop(self.finish_operation(generation)?);
                let (snapshot, observed_at) = present_snapshot(observation)?;
                let (instance, group) = present_group(snapshot, observed_at)?;
                (
                    instance,
                    group,
                    observed_at,
                    None,
                    tokio::time::Instant::now(),
                )
            };
        validate_eligible_snapshot(self.source.observer_config(), &instance, &group, false)?;
        let candidate = admit_topology(
            runtime_identity,
            expectation,
            &self.replication_address,
            &instance,
            &group,
            &current,
            TopologyAdmissionOptions {
                observer_config: self.source.observer_config(),
                allow_unassociated_database: self.bootstrap.is_some(),
                bootstrap_database_name: self
                    .bootstrap
                    .as_ref()
                    .map(|bootstrap| bootstrap.config.database()),
                bootstrap_database: bootstrap_database.as_ref(),
            },
        )?;
        self.ensure_observation_fresh(observed_at)?;
        let _lifecycle = self.finish_operation(generation)?;
        let mut admitted_topology = self
            .admitted_topology
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut admitted_configuration = self
            .admitted_configuration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut admitted_bootstrap_database = self
            .admitted_bootstrap_database
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.bootstrap.is_some() {
            bootstrap_remaining(bootstrap_deadline)?;
        }
        if let (Some(previous_topology), Some(previous_configuration)) =
            (&*admitted_topology, &*admitted_configuration)
        {
            if (previous_topology.as_ref() != &candidate
                && !topology_enriches(previous_topology, &candidate))
                || (previous_configuration != &current
                    && (configuration_is_complete(previous_configuration)
                        || !configuration_enriches(previous_configuration, &current)))
                || admitted_bootstrap_database
                    .as_ref()
                    .zip(bootstrap_database.as_ref())
                    .is_some_and(|(previous, current)| previous != current)
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "current configuration replay differs from the admitted value",
                )
                .into());
            }
            self.ensure_observation_fresh(observed_at)?;
            *admitted_topology = Some(Arc::new(candidate));
            *admitted_configuration = Some(current);
            if bootstrap_database.is_some() {
                *admitted_bootstrap_database = bootstrap_database;
            }
        } else {
            self.ensure_observation_fresh(observed_at)?;
            *admitted_topology = Some(Arc::new(candidate));
            *admitted_configuration = Some(current);
            *admitted_bootstrap_database = bootstrap_database;
        }
        Ok(())
    }

    async fn build_replica(&self, replica: ReplicaInformation) -> KubericResult<()> {
        if self.bootstrap.is_none() {
            return self.unsupported(ObserveOnlyOperation::BuildReplica);
        }
        let generation = self.begin_operation()?;
        self.converge_seed_build(generation, &replica)
            .await
            .map_err(KubericRuntimeError::from)
    }

    async fn remove_replica(&self, _replica_id: ReplicaId) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::RemoveReplica)
    }
}

fn seed_member(
    member: &HealthyTopologyMemberBinding,
) -> Result<SeedDispatchTarget, KubericAdapterError> {
    SeedDispatchTarget::new(
        member.kuberic_identity.replica_id.to_string(),
        member.kuberic_identity.instance_id.to_string(),
        member.kuberic_identity.agent_generation.to_string(),
        member.process_session_id.to_string(),
        member.native_replica_id.as_str(),
    )
    .map_err(KubericAdapterError::SeedState)
}

#[cfg(feature = "kuberic-testing")]
fn inject_testing_bootstrap_snapshot(
    group: &mut AvailabilityGroupSnapshot,
    scenario: TestingBootstrapObservation,
) -> Result<(), KubericAdapterError> {
    let alternate = |value: &str| {
        Guid::parse("test bootstrap observation GUID", value)
            .map_err(|_| KubericAdapterError::NativeConvergenceUnsafe("test observation seam"))
    };
    match scenario {
        TestingBootstrapObservation::AvailabilityGroupIdentityOrProfile => {
            group.identity.group_id = alternate("ffffffff-ffff-ffff-ffff-000000000201")?;
            group.required_synchronized_secondaries_to_commit = 0;
        }
        TestingBootstrapObservation::LogicalReplicaNativeMismatch => {
            let expected = group.local_replica.identity.clone();
            let expected_native = expected.native_replica_id().cloned();
            let conflicting_native = alternate("ffffffff-ffff-ffff-ffff-000000000202")?;
            group.local_replica.identity = SqlReplicaIdentity::observed(
                expected.logical_id(),
                conflicting_native.clone(),
                expected.incarnation(),
            )
            .map_err(|_| KubericAdapterError::NativeConvergenceUnsafe("test observation seam"))?;
            if let Some(replica) = group
                .replicas
                .iter_mut()
                .find(|replica| Some(&replica.replica_id) == expected_native.as_ref())
            {
                replica.replica_id = conflicting_native;
            }
        }
        TestingBootstrapObservation::EndpointOrMemberProfile => {
            let replica =
                group
                    .replicas
                    .first_mut()
                    .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                        "test observation seam",
                    ))?;
            replica.endpoint_url = Some("tcp://conflicting.invalid:5022".to_owned());
            replica.availability_mode = "ASYNCHRONOUS_COMMIT".to_owned();
        }
        TestingBootstrapObservation::AssociatedDatabaseIdentityOrRecovery => {
            let recovery = group
                .databases
                .first_mut()
                .and_then(|database| database.local.as_mut())
                .and_then(|local| local.recovery.as_mut())
                .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                    "test observation seam",
                ))?;
            recovery.database_guid = Some(alternate("ffffffff-ffff-ffff-ffff-000000000203")?);
        }
        TestingBootstrapObservation::IncompatibleLineage => {
            let local_replica_id = group
                .local_replica
                .identity
                .native_replica_id()
                .cloned()
                .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                    "test observation seam",
                ))?;
            let database =
                group
                    .databases
                    .first_mut()
                    .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                        "test observation seam",
                    ))?;
            let database_identity = database.identity.clone();
            let mut injected = false;
            for target in database
                .replicas
                .iter_mut()
                .filter(|replica| replica.replica_id != local_replica_id)
            {
                target.lineage = RecoveryLineageObservation::Local {
                    value: DatabaseLineage {
                        database: database_identity.clone(),
                        recovery_fork_id: alternate("ffffffff-ffff-ffff-ffff-000000000204")?,
                    },
                };
                injected = true;
            }
            if !injected {
                return Err(KubericAdapterError::NativeConvergenceUnsafe(
                    "test observation seam",
                ));
            }
        }
        TestingBootstrapObservation::SqlProcessIncarnationReplacement => {}
    }
    Ok(())
}

fn validate_seed_binding_evidence(
    binding: &HealthyTopologyBinding,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
) -> Result<(), KubericAdapterError> {
    let local = binding.local_member();
    if instance.sqlserver_start_time != binding.local_sql_server_start_incarnation.as_str() {
        return Err(KubericAdapterError::NativeConvergenceUnsafe(
            "SQL Server process incarnation",
        ));
    }
    if local.stable_role != ReplicaRole::Primary
        || instance.server_name != *local.server_name()
        || instance.property_server_name != *local.server_name()
        || group.identity != binding.availability_group
        || group.local_replica.identity != binding.local_sql_replica_identity
        || group.local_replica.role != Some(NativeRole::Primary)
        || group.replicas.len() != binding.members.len()
    {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "seed build native source or availability group differs",
        ));
    }
    for member in &binding.members {
        let matches = group
            .replicas
            .iter()
            .filter(|replica| {
                replica.replica_id == member.native_replica_id
                    && replica.server_name == *member.server_name()
                    && replica.endpoint_url.as_deref() == Some(member.sql_endpoint_url())
            })
            .count();
        if matches != 1 {
            return Err(KubericAdapterError::NativeConvergenceConflict(
                "seed build native replica membership differs",
            ));
        }
    }
    Ok(())
}

fn source_association(
    group: &AvailabilityGroupSnapshot,
    expected: &StandaloneDatabaseIdentity,
) -> Result<SourceAssociation, KubericAdapterError> {
    let Some(database) = group.databases.first() else {
        return Ok(SourceAssociation::Absent);
    };
    if group.databases.len() != 1 || database.identity.name != expected.name {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "source database association",
        ));
    }
    let Some(local) = database.local.as_ref() else {
        return Ok(SourceAssociation::Incomplete);
    };
    let Some(recovery) = local.recovery.as_ref() else {
        return Ok(SourceAssociation::Incomplete);
    };
    if recovery.database_guid.as_ref() != Some(&expected.database_guid)
        || recovery.family_guid.as_ref() != Some(&expected.family_guid)
        || recovery.recovery_fork_guid.as_ref() != Some(&expected.recovery_fork_guid)
    {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "source database association",
        ));
    }
    if local.state.as_deref() != Some("ONLINE") || local.recovery_model.as_deref() != Some("FULL") {
        return Ok(SourceAssociation::Incomplete);
    }
    Ok(SourceAssociation::Exact(database.identity.clone()))
}

fn classify_source_seed_completion(
    group: &AvailabilityGroupSnapshot,
    database: &DatabaseIdentity,
    source: &StandaloneDatabaseIdentity,
    target_replica_id: &Guid,
) -> NativeConvergenceState {
    if group.automatic_seeding.iter().any(|seed| {
        seed.group_database_id == database.group_database_id
            && seed.remote_replica_id == *target_replica_id
            && (seed.current_state.as_deref() == Some("FAILED")
                || seed.performed_seeding == Some(false)
                || seed.failure_state.is_some_and(|value| value != 0)
                || seed.error_code.is_some_and(|value| value != 0))
    }) {
        return NativeConvergenceState::Unsafe;
    }
    let Some(snapshot) = group
        .databases
        .iter()
        .find(|candidate| candidate.identity == *database)
    else {
        return NativeConvergenceState::Incomplete;
    };
    let Some(target) = snapshot
        .replicas
        .iter()
        .find(|replica| &replica.replica_id == target_replica_id)
    else {
        return NativeConvergenceState::Incomplete;
    };
    match &target.lineage {
        RecoveryLineageObservation::Local { value }
            if value.database != *database
                || value.recovery_fork_id != source.recovery_fork_guid =>
        {
            return NativeConvergenceState::Conflicting;
        }
        RecoveryLineageObservation::Local { .. }
        | RecoveryLineageObservation::RemoteUnavailable => {}
        RecoveryLineageObservation::LocalUnavailable => {
            return NativeConvergenceState::Incomplete;
        }
    }
    let successful_dispatch = group.automatic_seeding.iter().any(|seed| {
        seed.group_database_id == database.group_database_id
            && seed.remote_replica_id == *target_replica_id
            && seed.current_state.as_deref() == Some("COMPLETED")
            && seed.performed_seeding == Some(true)
            && seed.failure_state.is_none_or(|value| value == 0)
            && seed.error_code.is_none_or(|value| value == 0)
    });
    let database_state_matches = match target.provenance {
        NativeProvenance::Local => target.database_state.as_deref() == Some("ONLINE"),
        NativeProvenance::PrimaryRemote => target
            .database_state
            .as_deref()
            .is_none_or(|state| state == "ONLINE"),
    };
    if !successful_dispatch
        || !database_state_matches
        || target.synchronization_state.as_deref() != Some("SYNCHRONIZED")
        || target.synchronization_health.as_deref() != Some("HEALTHY")
        || target.is_suspended != Some(false)
        || target.suspend_reason.is_some()
    {
        NativeConvergenceState::Incomplete
    } else {
        NativeConvergenceState::Complete
    }
}

fn validate_replication_address(address: &str) -> Result<(), KubericAdapterError> {
    if address.is_empty()
        || address.len() > 1024
        || address.chars().any(char::is_control)
        || address.trim() != address
    {
        return Err(KubericAdapterError::InvalidConfiguration(
            "replication address must be nonempty, bounded, and contain no control or surrounding whitespace",
        ));
    }
    Ok(())
}

fn present_snapshot(
    observation: Observation<InstanceSnapshot>,
) -> Result<(InstanceSnapshot, u64), KubericAdapterError> {
    match observation {
        Observation::Present {
            value,
            observed_at_unix_millis,
        } => {
            if value.observed_at_unix_millis != observed_at_unix_millis {
                return Err(KubericAdapterError::ObservationInconsistent(
                    "snapshot and attempt timestamps differ",
                ));
            }
            Ok((value, observed_at_unix_millis))
        }
        Observation::Absent { .. } => Err(KubericAdapterError::AvailabilityGroupAbsent),
        Observation::Failed(failure) => {
            Err(KubericAdapterError::ObservationUnavailable(failure.kind))
        }
    }
}

fn bootstrap_remaining(deadline: tokio::time::Instant) -> Result<Duration, KubericAdapterError> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        Err(KubericAdapterError::NativeConvergenceDeadline)
    } else {
        Ok(remaining)
    }
}

fn is_retryable_bootstrap_observation(error: &KubericAdapterError) -> bool {
    matches!(
        error,
        KubericAdapterError::ObservationUnavailable(ObservationFailureKind::Inconsistent)
    )
}

fn present_group(
    snapshot: InstanceSnapshot,
    observed_at: u64,
) -> Result<
    (
        crate::observation::InstanceMetadata,
        AvailabilityGroupSnapshot,
    ),
    KubericAdapterError,
> {
    let group = match snapshot.availability_group {
        Observation::Present {
            value,
            observed_at_unix_millis,
        } if observed_at_unix_millis == observed_at => value,
        Observation::Present { .. } => {
            return Err(KubericAdapterError::ObservationInconsistent(
                "availability-group and instance timestamps differ",
            ));
        }
        Observation::Absent { .. } => return Err(KubericAdapterError::AvailabilityGroupAbsent),
        Observation::Failed(failure) => {
            return Err(KubericAdapterError::ObservationUnavailable(failure.kind));
        }
    };
    Ok((snapshot.instance, group))
}

struct TopologyAdmissionOptions<'a> {
    observer_config: &'a ObserverConfig,
    allow_unassociated_database: bool,
    bootstrap_database_name: Option<&'a SqlIdentifier>,
    bootstrap_database: Option<&'a crate::bootstrap::StandaloneDatabaseIdentity>,
}

fn admit_topology(
    runtime_identity: &KubericReplicaIdentity,
    expectation: &SqlServerTopologyExpectation,
    replication_address: &str,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    current: &ReplicaSetConfiguration,
    options: TopologyAdmissionOptions<'_>,
) -> Result<HealthyTopologyBinding, KubericAdapterError> {
    validate_configuration_shape(runtime_identity, expectation, replication_address, current)?;
    if !options.allow_unassociated_database
        && group.configuration_sequence.value() != current.configuration.epoch.configuration_number
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "configuration sequence differs from the current configuration",
        ));
    }
    if instance.server_name != *expectation.local_member().server_name()
        || instance.property_server_name != *expectation.local_member().server_name()
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local SQL server differs from the topology expectation",
        ));
    }

    if group.databases.len() > usize::from(SUPPORTED_DATABASE_COUNT)
        || (!options.allow_unassociated_database && group.databases.is_empty())
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "exactly one managed database is required",
        ));
    }
    if options.allow_unassociated_database {
        match group.databases.as_slice() {
            [] => {}
            [database] => {
                let expected_name = options.bootstrap_database_name.ok_or(
                    KubericAdapterError::TopologyBindingMismatch(
                        "bootstrap managed database name is unavailable",
                    ),
                )?;
                let expected = options.bootstrap_database.ok_or(
                    KubericAdapterError::TopologyBindingMismatch(
                        "associated managed database ownership is unproven",
                    ),
                )?;
                let local =
                    database
                        .local
                        .as_ref()
                        .ok_or(KubericAdapterError::TopologyBindingMismatch(
                            "associated managed database has no local native identity",
                        ))?;
                let recovery =
                    local
                        .recovery
                        .as_ref()
                        .ok_or(KubericAdapterError::TopologyBindingMismatch(
                            "associated managed database has no native recovery identity",
                        ))?;
                if database.identity.name != *expected_name
                    || recovery.database_guid.as_ref() != Some(&expected.database_guid)
                    || recovery.family_guid.as_ref() != Some(&expected.family_guid)
                    || recovery.recovery_fork_guid.as_ref() != Some(&expected.recovery_fork_guid)
                {
                    return Err(KubericAdapterError::TopologyBindingMismatch(
                        "associated managed database differs from the configured native ownership",
                    ));
                }
            }
            _ => {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "unrelated or multiple managed databases are not admissible",
                ));
            }
        }
    }
    let database_lineage = if let Some(database) = group.databases.first() {
        let recovery_fork_id = database
            .local
            .as_ref()
            .and_then(|local| local.recovery.as_ref())
            .and_then(|recovery| recovery.recovery_fork_guid.clone())
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "local database recovery lineage is unavailable",
            ))?;
        Some(DatabaseLineage {
            database: database.identity.clone(),
            recovery_fork_id,
        })
    } else {
        None
    };

    let mut members = Vec::with_capacity(usize::from(SUPPORTED_REPLICA_COUNT));
    for configuration_member in &current.configuration.members {
        let expected = expectation
            .member(configuration_member.identity.replica_id)
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "current member has no SQL topology association",
            ))?;
        let descriptions = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == configuration_member.identity)
            .collect::<Vec<_>>();
        if descriptions.len() != 1 {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica identities differ from the current descriptor",
            ));
        }
        let native = group
            .replicas
            .iter()
            .filter(|replica| {
                replica.server_name == *expected.server_name()
                    && replica.endpoint_url.as_deref() == Some(expected.endpoint_url())
            })
            .collect::<Vec<_>>();
        if native.len() != 1 {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "SQL topology association does not identify one native replica",
            ));
        }
        members.push(HealthyTopologyMemberBinding {
            kuberic_identity: configuration_member.identity.clone(),
            process_session_id: descriptions[0].process_session_id.clone(),
            replication_address: descriptions[0].replication_address.clone(),
            native_replica_id: native[0].replica_id.clone(),
            sql_endpoint: SqlServerMemberEndpoint::new(
                expected.server_name().clone(),
                expected.endpoint_url(),
            )?,
            stable_role: configuration_member.role,
        });
    }
    members.sort_by_key(|member| member.kuberic_identity.replica_id);
    if members.iter().enumerate().any(|(index, member)| {
        members[..index]
            .iter()
            .any(|other| other.native_replica_id == member.native_replica_id)
    }) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "native replica identities must be unique",
        ));
    }

    let candidate = HealthyTopologyBinding {
        local_identity: runtime_identity.clone(),
        configuration: current.configuration.clone(),
        native_configuration_sequence: group.configuration_sequence,
        availability_group: group.identity.clone(),
        database_lineage,
        bootstrap_database_name: options.bootstrap_database_name.cloned(),
        bootstrap_database: options.bootstrap_database.cloned(),
        local_sql_replica_identity: group.local_replica.identity.clone(),
        local_sql_server_start_incarnation: SqlServerStartIncarnation::new(
            instance.sqlserver_start_time.clone(),
        )?,
        members,
    };
    if candidate.local_member().native_replica_id()
        != candidate
            .local_sql_replica_identity
            .native_replica_id()
            .expect("eligible local SQL identity requires a native ID")
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local SQL association differs from the observed native replica",
        ));
    }
    validate_current_configuration(&candidate, current)?;
    validate_bound_evidence(&candidate, options.observer_config, instance, group, None)?;
    Ok(candidate)
}

fn validate_configuration_shape(
    runtime_identity: &KubericReplicaIdentity,
    expectation: &SqlServerTopologyExpectation,
    replication_address: &str,
    current: &ReplicaSetConfiguration,
) -> Result<(), KubericAdapterError> {
    let configuration = &current.configuration;
    if configuration.configuration_id != configuration.expected_id()
        || configuration.members.len() != usize::from(SUPPORTED_REPLICA_COUNT)
        || current.replicas.len() != usize::from(SUPPORTED_REPLICA_COUNT)
        || configuration.write_quorum != 2
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current configuration requires exactly three members and quorum two",
        ));
    }
    let primary_count = configuration
        .members
        .iter()
        .filter(|member| member.role == ReplicaRole::Primary)
        .count();
    let secondary_count = configuration
        .members
        .iter()
        .filter(|member| member.role == ReplicaRole::ActiveSecondary)
        .count();
    if primary_count != 1
        || secondary_count != 2
        || configuration.members.iter().any(|member| {
            !matches!(
                member.role,
                ReplicaRole::Primary | ReplicaRole::ActiveSecondary
            )
        })
        || configuration
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
            .is_none_or(|member| member.identity.replica_id != configuration.primary_id)
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current configuration requires one Primary and two ActiveSecondary members",
        ));
    }
    if configuration
        .members
        .iter()
        .enumerate()
        .any(|(index, member)| {
            configuration.members[..index].iter().any(|other| {
                other.identity == member.identity
                    || other.identity.replica_id == member.identity.replica_id
            })
        })
        || expectation.members().iter().any(|expected| {
            !configuration
                .members
                .iter()
                .any(|member| member.identity.replica_id == expected.replica_id())
        })
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current member identities differ from the topology expectation",
        ));
    }
    let local = configuration
        .members
        .iter()
        .find(|member| member.identity == *runtime_identity)
        .ok_or(KubericAdapterError::TopologyBindingMismatch(
            "current configuration does not include the runtime identity",
        ))?;
    if local.identity.replica_id != expectation.local_replica_id() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "runtime identity differs from the local topology association",
        ));
    }

    for configuration_member in &configuration.members {
        if configuration_member.identity.instance_id.is_empty()
            || configuration_member.identity.agent_generation.is_empty()
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current member identities must be complete",
            ));
        }
        let descriptions = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == configuration_member.identity)
            .collect::<Vec<_>>();
        if descriptions.len() != 1 {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica identity differs from the descriptor",
            ));
        }
        if !descriptions[0].replication_address.is_empty()
            && validate_replication_address(&descriptions[0].replication_address).is_err()
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica address is invalid",
            ));
        }
        if !matches!(descriptions[0].role, ReplicaRole::None)
            && descriptions[0].role != configuration_member.role
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica role differs from the descriptor",
            ));
        }
        if configuration_member.identity == *runtime_identity
            && descriptions[0].replication_address != replication_address
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "local replication address differs from the runtime",
            ));
        }
    }
    if current.replicas.iter().enumerate().any(|(index, replica)| {
        current.replicas[..index].iter().any(|other| {
            other.identity == replica.identity
                || (!replica.process_session_id.is_empty()
                    && other.process_session_id == replica.process_session_id)
                || (!replica.replication_address.is_empty()
                    && other.replication_address == replica.replication_address)
        })
    }) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current identities, sessions, and addresses must be unique",
        ));
    }
    Ok(())
}

fn validate_observer_expectation(
    config: &ObserverConfig,
    expectation: &SqlServerTopologyExpectation,
) -> Result<(), KubericAdapterError> {
    if config.target().expected_server_name != *expectation.local_member().server_name()
        || config.target().replica.logical_id() != expectation.local_logical_replica_id()
    {
        Err(KubericAdapterError::InvalidConfiguration(
            "observer target must match the local SQL topology member and logical replica",
        ))
    } else {
        Ok(())
    }
}

fn bootstrap_descriptors(
    expectation: &SqlServerTopologyExpectation,
    current: &ReplicaSetConfiguration,
) -> Result<Vec<ReplicaDescriptor>, KubericAdapterError> {
    expectation
        .members()
        .iter()
        .map(|member| {
            let configured = current
                .configuration
                .members
                .iter()
                .find(|configured| configured.identity.replica_id == member.replica_id())
                .ok_or(KubericAdapterError::TopologyBindingMismatch(
                    "bootstrap member has no current configuration identity",
                ))?;
            let endpoint = member
                .endpoint_url()
                .get(6..)
                .and_then(|value| value.rsplit_once(':'))
                .ok_or(KubericAdapterError::InvalidConfiguration(
                    "bootstrap SQL endpoint is invalid",
                ))?;
            if !member.endpoint_url()[..6].eq_ignore_ascii_case("tcp://")
                || !endpoint
                    .0
                    .eq_ignore_ascii_case(member.server_name().as_str())
            {
                return Err(KubericAdapterError::InvalidConfiguration(
                    "bootstrap SQL endpoint must name the configured SQL member",
                ));
            }
            let port = endpoint
                .1
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or(KubericAdapterError::InvalidConfiguration(
                    "bootstrap SQL endpoint port is invalid",
                ))?;
            Ok(ReplicaDescriptor {
                identity: SqlReplicaIdentity::desired(
                    configured.identity.replica_id.to_string(),
                    configured.identity.agent_generation.to_string(),
                )
                .map_err(|_| {
                    KubericAdapterError::InvalidConfiguration(
                        "bootstrap SQL replica identity is invalid",
                    )
                })?,
                server_name: member.server_name().clone(),
                endpoint: Endpoint::new(member.server_name().as_str(), port).map_err(|_| {
                    KubericAdapterError::InvalidConfiguration("bootstrap SQL endpoint is invalid")
                })?,
            })
        })
        .collect()
}

fn bootstrap_membership_ready(
    group: &AvailabilityGroupSnapshot,
    ownership: BootstrapOwnership,
    descriptors: &[ReplicaDescriptor],
) -> bool {
    let expected_local_role = match ownership {
        BootstrapOwnership::PrimaryTopology => NativeRole::Primary,
        BootstrapOwnership::LocalJoin => NativeRole::Secondary,
    };
    if !group.local_replica.state_available
        || group.local_replica.role.as_ref() != Some(&expected_local_role)
    {
        return false;
    }
    let states = match ownership {
        BootstrapOwnership::PrimaryTopology => group.replicas.iter().collect::<Vec<_>>(),
        BootstrapOwnership::LocalJoin => {
            let Some(local_id) = group.local_replica.identity.native_replica_id() else {
                return false;
            };
            group
                .replicas
                .iter()
                .filter(|replica| &replica.replica_id == local_id)
                .collect::<Vec<_>>()
        }
    };
    states.len()
        == match ownership {
            BootstrapOwnership::PrimaryTopology => descriptors.len(),
            BootstrapOwnership::LocalJoin => 1,
        }
        && states.iter().all(|replica| {
            replica.state.as_ref().is_some_and(|state| {
                state.connected_state.as_deref() == Some("CONNECTED")
                    && (state.provenance != NativeProvenance::Local
                        || state.operational_state.as_deref() == Some("ONLINE"))
                    && matches!(
                        state.role,
                        Some(NativeRole::Primary | NativeRole::Secondary)
                    )
                    && state
                        .last_connect_error_number
                        .is_none_or(|number| number == 0)
            })
        })
        && (ownership != BootstrapOwnership::PrimaryTopology
            || states
                .iter()
                .filter(|replica| {
                    replica
                        .state
                        .as_ref()
                        .is_some_and(|state| state.role == Some(NativeRole::Primary))
                })
                .count()
                == 1)
}

fn validate_bootstrap_instance(
    config: &ObserverConfig,
    instance: &crate::observation::InstanceMetadata,
) -> Result<(), KubericAdapterError> {
    if instance.server_name != config.target().expected_server_name
        || instance.property_server_name != config.target().expected_server_name
    {
        return Err(KubericAdapterError::ObservationInconsistent(
            "configured and observed SQL Server identities differ",
        ));
    }
    validate_supported_instance(instance)
}

fn validate_current_configuration(
    binding: &HealthyTopologyBinding,
    current: &ReplicaSetConfiguration,
) -> Result<(), KubericAdapterError> {
    if current.configuration != binding.configuration {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current descriptor differs from the frozen descriptor",
        ));
    }
    if current.replicas.len() != binding.members.len() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current replica descriptions are incomplete",
        ));
    }
    for member in &binding.members {
        let matching = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == member.kuberic_identity)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica identities differ from the frozen members",
            ));
        }
        let replica = matching[0];
        if replica.process_session_id != member.process_session_id
            || replica.replication_address != member.replication_address
            || (!matches!(replica.role, ReplicaRole::None) && replica.role != member.stable_role)
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica session, address, or role differs from the frozen member",
            ));
        }
    }
    Ok(())
}

fn normalize_bootstrap_configuration(
    current: ReplicaSetConfiguration,
) -> Result<ReplicaSetConfiguration, KubericAdapterError> {
    if current.replicas.len() <= usize::from(SUPPORTED_REPLICA_COUNT) {
        return Ok(current);
    }
    let mut base = Vec::with_capacity(current.configuration.members.len());
    let mut described = 0;
    for member in &current.configuration.members {
        let matches = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == member.identity)
            .collect::<Vec<_>>();
        described += matches.len();
        let base_descriptions = matches
            .iter()
            .filter(|replica| replica.build_id.is_empty())
            .collect::<Vec<_>>();
        if base_descriptions.len() != 1
            || matches.iter().any(|replica| {
                !replica.build_id.is_empty()
                    && (member.role != ReplicaRole::ActiveSecondary
                        || !matches!(
                            replica.role,
                            ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary
                        ))
            })
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "bootstrap build-enriched configuration is ambiguous",
            ));
        }
        base.push((**base_descriptions.first().expect("one base description")).clone());
    }
    if described != current.replicas.len()
        || current.replicas.len() > current.configuration.members.len() + 2
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "bootstrap build-enriched configuration has unknown descriptions",
        ));
    }
    Ok(ReplicaSetConfiguration {
        configuration: current.configuration,
        replicas: base,
    })
}

fn build_enriched_configuration(
    binding: &HealthyTopologyBinding,
    current: &ReplicaSetConfiguration,
) -> Result<bool, KubericAdapterError> {
    if current.configuration != binding.configuration {
        return Ok(false);
    }
    if current.replicas.len() <= binding.members.len() {
        return Ok(false);
    }
    if current.replicas.len() > binding.members.len() + 2 {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "build-enriched current configuration has an invalid description count",
        ));
    }
    let mut base = Vec::with_capacity(binding.members.len());
    let mut builds = Vec::new();
    for member in &binding.members {
        let matches = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == member.kuberic_identity)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [only] => base.push((*only).clone()),
            [first, second] => {
                let (base_replica, build) =
                    match (first.build_id.is_empty(), second.build_id.is_empty()) {
                        (true, false) => (*first, *second),
                        (false, true) => (*second, *first),
                        _ => {
                            return Err(KubericAdapterError::TopologyBindingMismatch(
                                "build-enriched target descriptions are ambiguous",
                            ));
                        }
                    };
                base.push(base_replica.clone());
                builds.push(build.clone());
            }
            _ => {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "build-enriched current configuration has invalid member descriptions",
                ));
            }
        }
    }
    if builds.is_empty() {
        return Ok(false);
    }
    if base.len() + builds.len() != current.replicas.len() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "build-enriched current configuration contains an unknown replica",
        ));
    }
    validate_current_configuration(
        binding,
        &ReplicaSetConfiguration {
            configuration: current.configuration.clone(),
            replicas: base,
        },
    )?;
    for build in builds {
        let target = binding
            .members
            .iter()
            .find(|member| member.kuberic_identity == build.identity)
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "build-enriched current configuration has an unknown target",
            ))?;
        if target.stable_role != ReplicaRole::ActiveSecondary
            || build.process_session_id != target.process_session_id
            || (!build.replication_address.is_empty()
                && build.replication_address != target.replication_address)
            || !matches!(
                build.role,
                ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary
            )
            || build.current_progress < 0
            || build.catch_up_capability < 0
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "build-enriched current configuration differs from the admitted target",
            ));
        }
    }
    Ok(true)
}

fn configuration_enriches(
    previous: &ReplicaSetConfiguration,
    current: &ReplicaSetConfiguration,
) -> bool {
    if previous.configuration != current.configuration
        || previous.replicas.len() != current.replicas.len()
    {
        return false;
    }

    previous.replicas.iter().all(|old| {
        let matching = current
            .replicas
            .iter()
            .filter(|new| new.identity == old.identity)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return false;
        }
        let new = matching[0];
        (old.process_session_id.is_empty() || old.process_session_id == new.process_session_id)
            && (old.replication_address.is_empty()
                || old.replication_address == new.replication_address)
            && (matches!(old.role, ReplicaRole::None) || old.role == new.role)
            && old.current_progress == new.current_progress
            && old.catch_up_capability == new.catch_up_capability
    })
}

fn configuration_is_complete(configuration: &ReplicaSetConfiguration) -> bool {
    configuration.replicas.iter().all(|replica| {
        !replica.process_session_id.is_empty()
            && validate_replication_address(&replica.replication_address).is_ok()
            && !matches!(replica.role, ReplicaRole::None)
    })
}

fn topology_enriches(previous: &HealthyTopologyBinding, current: &HealthyTopologyBinding) -> bool {
    if previous.local_identity != current.local_identity
        || previous.configuration != current.configuration
        || (previous.database_lineage.is_some()
            && previous.native_configuration_sequence != current.native_configuration_sequence)
        || previous.availability_group != current.availability_group
        || previous
            .database_lineage
            .as_ref()
            .is_some_and(|lineage| current.database_lineage.as_ref() != Some(lineage))
        || previous.bootstrap_database_name != current.bootstrap_database_name
        || previous
            .bootstrap_database
            .as_ref()
            .is_some_and(|database| current.bootstrap_database.as_ref() != Some(database))
        || previous.local_sql_replica_identity != current.local_sql_replica_identity
        || previous.local_sql_server_start_incarnation != current.local_sql_server_start_incarnation
        || previous.members.len() != current.members.len()
    {
        return false;
    }
    previous.members.iter().all(|old| {
        let matching = current
            .members
            .iter()
            .filter(|new| new.kuberic_identity == old.kuberic_identity)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return false;
        }
        let new = matching[0];
        old.native_replica_id == new.native_replica_id
            && old.sql_endpoint == new.sql_endpoint
            && old.stable_role == new.stable_role
            && (old.process_session_id.is_empty()
                || old.process_session_id == new.process_session_id)
            && (old.replication_address.is_empty()
                || old.replication_address == new.replication_address)
    })
}

fn validate_bound_evidence(
    binding: &HealthyTopologyBinding,
    config: &ObserverConfig,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    requested_role: Option<ReplicaRole>,
) -> Result<(), KubericAdapterError> {
    let local = binding.local_member();
    let target = config.target();
    if instance.sqlserver_start_time != binding.local_sql_server_start_incarnation.as_str() {
        return Err(KubericAdapterError::NativeConvergenceUnsafe(
            "SQL Server process incarnation",
        ));
    }
    if binding.database_lineage.is_some()
        && group.configuration_sequence != binding.native_configuration_sequence
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "configuration sequence differs from the frozen configuration",
        ));
    }
    if target.replica.logical_id() != binding.local_sql_replica_identity.logical_id()
        || target.replica.incarnation() != binding.local_sql_replica_identity.incarnation()
        || instance.server_name != *local.server_name()
        || instance.property_server_name != *local.server_name()
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local SQL server or incarnation evidence differs",
        ));
    }
    if group.identity != binding.availability_group {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "availability-group identity differs",
        ));
    }
    if group.local_replica.identity != binding.local_sql_replica_identity {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local SQL replica identity differs",
        ));
    }
    if requested_role.is_some_and(|role| !published_role_matches(binding, role)) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "requested role differs from the frozen stable role",
        ));
    }
    let expected_native_role = native_role_for(local.stable_role);
    if group.local_replica.role.as_ref() != Some(&expected_native_role) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local native role differs from the frozen stable role",
        ));
    }
    if group.replicas.len() != binding.members.len() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "native replica membership differs",
        ));
    }
    for member in &binding.members {
        let matching = group
            .replicas
            .iter()
            .filter(|replica| {
                &replica.replica_id == member.native_replica_id()
                    && replica.server_name == *member.server_name()
                    && replica.endpoint_url.as_deref() == Some(member.sql_endpoint_url())
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "native replica GUID, SQL server, or endpoint membership differs",
            ));
        }
    }
    let local_native_id = binding
        .local_sql_replica_identity
        .native_replica_id()
        .expect("admitted local SQL identity requires a native ID");
    let local_replica = group
        .replicas
        .iter()
        .find(|replica| {
            &replica.replica_id == local_native_id && replica.server_name == *local.server_name()
        })
        .expect("native membership was validated");
    let Some(state) = &local_replica.state else {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local native replica state is unavailable",
        ));
    };
    if state.provenance != NativeProvenance::Local
        || state.role.as_ref() != Some(&expected_native_role)
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local native replica role differs",
        ));
    }
    if state.operational_state.as_deref() != Some("ONLINE") {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local native replica operational state differs",
        ));
    }
    if state.connected_state.as_deref() != Some("CONNECTED") {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local native replica connected state differs",
        ));
    }
    if binding.database_lineage.is_some() {
        if state.recovery_health.as_deref() != Some("ONLINE") {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "local native replica recovery health differs",
            ));
        }
        if state.synchronization_health.as_deref() != Some("HEALTHY") {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "local native replica synchronization health differs",
            ));
        }
    }
    if state
        .last_connect_error_number
        .is_some_and(|number| number != 0)
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local native replica last-connect evidence differs",
        ));
    }
    let Some(database_lineage) = &binding.database_lineage else {
        if group.databases.is_empty() {
            return Ok(());
        }
        let expected_name = binding.bootstrap_database_name.as_ref().ok_or(
            KubericAdapterError::TopologyBindingMismatch(
                "build-owned database identity is unavailable",
            ),
        )?;
        if group.databases.len() != 1 || group.databases[0].identity.name != *expected_name {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "build-owned database association differs",
            ));
        }
        if let Some(local_database) = group.databases[0].local.as_ref() {
            let recovery = local_database.recovery.as_ref().ok_or(
                KubericAdapterError::TopologyBindingMismatch(
                    "build-owned local database recovery identity is unavailable",
                ),
            )?;
            if local_database.state.as_deref() != Some("ONLINE")
                || local_database.recovery_model.as_deref() != Some("FULL")
                || binding.bootstrap_database.as_ref().is_some_and(|expected| {
                    recovery.database_guid.as_ref() != Some(&expected.database_guid)
                        || recovery.family_guid.as_ref() != Some(&expected.family_guid)
                        || recovery.recovery_fork_guid.as_ref()
                            != Some(&expected.recovery_fork_guid)
                })
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "build-owned local database identity or lineage differs",
                ));
            }
        }
        return Ok(());
    };
    if group.databases.len() != usize::from(SUPPORTED_DATABASE_COUNT) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "exactly one managed database is required",
        ));
    }
    let database = &group.databases[0];
    if database.identity != database_lineage.database {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "managed database identity differs",
        ));
    }
    let Some(local_database) = &database.local else {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local managed database evidence is unavailable",
        ));
    };
    if &local_database.replica_id != local_native_id
        || local_database.state.as_deref() != Some("ONLINE")
        || local_database.recovery_model.as_deref() != Some("FULL")
        || local_database
            .recovery
            .as_ref()
            .and_then(|recovery| recovery.recovery_fork_guid.as_ref())
            != Some(&database_lineage.recovery_fork_id)
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local database identity, state, recovery model, or lineage differs",
        ));
    }
    let matching_states = database
        .replicas
        .iter()
        .filter(|state| {
            &state.replica_id == local_native_id && state.provenance == NativeProvenance::Local
        })
        .collect::<Vec<_>>();
    if matching_states.len() != 1 {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local database replica evidence is incomplete",
        ));
    }
    validate_bound_database_state(matching_states[0], database_lineage, local.stable_role)
}

fn validate_bound_database_state(
    state: &DatabaseReplicaSnapshot,
    lineage: &DatabaseLineage,
    role: ReplicaRole,
) -> Result<(), KubericAdapterError> {
    let lineage_matches = matches!(
        &state.lineage,
        RecoveryLineageObservation::Local { value } if value == lineage
    );
    let commit_participation_matches =
        role != ReplicaRole::Primary || state.is_commit_participant == Some(true);
    if state.group_database_id != lineage.database.group_database_id
        || !lineage_matches
        || state.is_primary_replica != Some(role == ReplicaRole::Primary)
        || state.synchronization_state.as_deref() != Some("SYNCHRONIZED")
        || state.synchronization_health.as_deref() != Some("HEALTHY")
        || state.database_state.as_deref() != Some("ONLINE")
        || state.is_suspended != Some(false)
        || state.suspend_reason.is_some()
        || !commit_participation_matches
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local database lineage, synchronization, health, or role differs",
        ));
    }
    Ok(())
}

fn native_role_for(role: ReplicaRole) -> NativeRole {
    match role {
        ReplicaRole::Primary => NativeRole::Primary,
        ReplicaRole::ActiveSecondary => NativeRole::Secondary,
        ReplicaRole::IdleSecondary | ReplicaRole::None => {
            unreachable!("binding constructor rejects unstable roles")
        }
    }
}

fn published_role_matches(binding: &HealthyTopologyBinding, role: ReplicaRole) -> bool {
    role == binding.local_member().stable_role
        || (binding.bootstrap_database_name.is_some()
            && binding.local_member().stable_role == ReplicaRole::ActiveSecondary
            && role == ReplicaRole::IdleSecondary)
}

fn validate_eligible_snapshot(
    config: &ObserverConfig,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    require_progress_role: bool,
) -> Result<(), KubericAdapterError> {
    let target = config.target();
    if instance.server_name != target.expected_server_name
        || instance.property_server_name != target.expected_server_name
        || group.identity.name != target.availability_group
        || group.local_replica.identity.logical_id() != target.replica.logical_id()
        || group.local_replica.identity.incarnation() != target.replica.incarnation()
    {
        return Err(KubericAdapterError::ObservationInconsistent(
            "configured and observed SQL Server identities differ",
        ));
    }
    validate_supported_instance(instance)?;
    if !group.cluster_type.eq_ignore_ascii_case("EXTERNAL")
        || group.basic_features
        || group.is_distributed
    {
        return Err(KubericAdapterError::UnsupportedProfile(
            "a non-basic, non-distributed EXTERNAL availability group is required",
        ));
    }
    if group.required_synchronized_secondaries_to_commit
        != u32::from(SUPPORTED_REQUIRED_SECONDARIES)
    {
        return Err(KubericAdapterError::UnsupportedProfile(
            "required synchronized secondaries must equal one",
        ));
    }
    if group.replicas.len() != usize::from(SUPPORTED_REPLICA_COUNT) {
        return Err(KubericAdapterError::UnsupportedProfile(
            "exactly three replicas are required",
        ));
    }
    if group.replicas.iter().any(|replica| {
        replica.availability_mode != "SYNCHRONOUS_COMMIT"
            || replica.failover_mode != "EXTERNAL"
            || replica.seeding_mode != "AUTOMATIC"
    }) {
        return Err(KubericAdapterError::UnsupportedProfile(
            "all replicas must use synchronous commit, EXTERNAL failover, and automatic seeding",
        ));
    }
    if group.databases.len() > usize::from(SUPPORTED_DATABASE_COUNT) {
        return Err(KubericAdapterError::UnsupportedProfile(
            "at most one managed database is supported",
        ));
    }
    let Some(native_id) = group.local_replica.identity.native_replica_id() else {
        return Err(KubericAdapterError::ObservationInconsistent(
            "local native replica identity is unavailable",
        ));
    };
    let matching_local = group
        .replicas
        .iter()
        .filter(|replica| {
            &replica.replica_id == native_id && replica.server_name == instance.server_name
        })
        .count();
    if matching_local != 1 || !group.local_replica.state_available {
        return Err(KubericAdapterError::ObservationInconsistent(
            "local replica configuration or state is unavailable",
        ));
    }
    let eligible_role = matches!(
        group.local_replica.role,
        Some(NativeRole::Primary | NativeRole::Secondary)
    ) || (!require_progress_role
        && matches!(group.local_replica.role, Some(NativeRole::NotJoined)));
    if !eligible_role {
        return Err(KubericAdapterError::UnsupportedProfile(
            "local replica must have an eligible stable native role",
        ));
    }
    Ok(())
}

fn validate_supported_instance(
    instance: &crate::observation::InstanceMetadata,
) -> Result<(), KubericAdapterError> {
    if instance.product_major_version != SUPPORTED_ENGINE_MAJOR
        || instance.product_version.split('.').next() != Some("17")
        || instance.engine_edition != 3
        || !instance.hadr_enabled
        || !instance.host_platform.eq_ignore_ascii_case("Linux")
        || instance.architecture != "x86_64"
        || !supported_edition(&instance.edition)
    {
        return Err(KubericAdapterError::UnsupportedProfile(
            "SQL Server 2025 Enterprise-feature Linux x86_64 with HADR enabled is required",
        ));
    }
    Ok(())
}

fn supported_edition(edition: &str) -> bool {
    let edition = edition.strip_suffix(" (64-bit)").unwrap_or(edition);
    matches!(
        edition,
        "Developer Enterprise"
            | "Developer Enterprise Edition"
            | "Enterprise Developer"
            | "Enterprise Developer Edition"
            | "Enterprise"
            | "Enterprise Edition"
            | "Enterprise Edition: Core-based Licensing"
    )
}

fn validate_native_role(
    requested: ReplicaRole,
    observed: Option<NativeRole>,
) -> Result<(), KubericAdapterError> {
    let matches = matches!(
        (requested, &observed),
        (ReplicaRole::Primary, Some(NativeRole::Primary))
            | (
                ReplicaRole::ActiveSecondary | ReplicaRole::IdleSecondary,
                Some(NativeRole::Secondary)
            )
            | (ReplicaRole::None, Some(NativeRole::NotJoined))
    );
    if matches {
        Ok(())
    } else {
        Err(KubericAdapterError::NativeRoleMismatch {
            requested,
            observed,
        })
    }
}
