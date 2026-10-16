use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Duration;

#[cfg(feature = "kuberic-testing")]
use std::sync::Condvar;

mod compatibility;

use async_trait::async_trait;
use kuberic_runtime::application::{
    INVALID_LSN, OpenContext, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, Epoch, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIdentity as KubericReplicaIdentity, ReplicaRole, ResourceUid,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, ReplicaSetQuorumMode,
    Replicator, ReplicatorCapabilities, ReplicatorFactory, ReplicatorFactoryContext,
    ReplicatorInterfaces, ReplicatorSettings,
};
use kuberic_runtime::{Result as KubericResult, RuntimeError as KubericRuntimeError};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::bootstrap::{
    AvailabilityGroupOwnership, NativeConvergenceState, StandaloneDatabaseIdentity,
    StandaloneDatabaseOwnership, classify_owned_availability_group, classify_replica_join,
    classify_standalone_database,
};
use crate::executor::SqlExecutor;
use crate::instance::{SqlServerInstanceManager, unix_millis};
use crate::mutation::{
    MutationEffect, SeedDispatchEvidence, SeedDispatchRequest, SqlMutationExecutor,
};
use crate::observation::{
    AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, InstanceSnapshot, NativeProvenance,
    RecoveryLineageObservation,
};
use crate::runtime_config::ObserverConfig;
use crate::runtime_error::RuntimeError;
use crate::runtime_host::seed_state::{
    SeedDispatchPhase, SeedDispatchRecord, SeedDispatchStore, SeedStateError,
};
use crate::topology_config::SqlServerTopologyExpectation;
use crate::{
    AvailabilityGroupIdentity, ConfigurationSequence, DatabaseIdentity, DatabaseLineage, Guid,
    NativeRole, Observation, ObservationFailureKind, ReplicaDescriptor,
    ReplicaIdentity as SqlReplicaIdentity, SUPPORTED_DATABASE_COUNT, SUPPORTED_ENGINE_MAJOR,
    SUPPORTED_REPLICA_COUNT, SUPPORTED_REQUIRED_SECONDARIES, ServerName, SqlIdentifier,
};

use compatibility::{
    BootstrapConvergenceScope, CurrentBuildFacts, CurrentTopologyFacts,
    bootstrap_convergence_scope, build_enriched_configuration, configuration_enriches,
    configuration_is_complete, normalize_bootstrap_configuration, topology_enriches,
    validate_admitted_build_configuration, validate_bootstrap_configuration_replay,
    validate_build_id, validate_replication_address,
};

#[cfg(not(feature = "kuberic-testing"))]
use compatibility::BootstrapOwnership;
#[cfg(feature = "kuberic-testing")]
pub use compatibility::{
    BootstrapOwnership, BootstrapOwnershipError, classify_bootstrap_ownership,
    testing_current_pin_bootstrap_descriptors,
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
    NativeMutationAmbiguous(&'static str),
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
            Self::NativeMutationAmbiguous(effect) => {
                write!(
                    formatter,
                    "{effect} has a genuinely ambiguous SQL submission outcome"
                )
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetHostEvidenceRequest {
    resource_uid: String,
    configuration_id: String,
    build_id: String,
    target_identity: KubericReplicaIdentity,
    target_process_session_id: ProcessSessionId,
    target_native_replica_id: Guid,
    target_server_name: ServerName,
    target_endpoint_url: String,
    availability_group: AvailabilityGroupIdentity,
    database: StandaloneDatabaseIdentity,
    group_database_id: Guid,
    requested_at_unix_millis: u64,
    challenge: String,
}

impl TargetHostEvidenceRequest {
    pub fn resource_uid(&self) -> &str {
        &self.resource_uid
    }

    pub fn configuration_id(&self) -> &str {
        &self.configuration_id
    }

    pub fn build_id(&self) -> &str {
        &self.build_id
    }

    pub fn target_identity(&self) -> &KubericReplicaIdentity {
        &self.target_identity
    }

    pub fn target_process_session_id(&self) -> &ProcessSessionId {
        &self.target_process_session_id
    }

    pub fn target_native_replica_id(&self) -> &Guid {
        &self.target_native_replica_id
    }

    pub fn requested_at_unix_millis(&self) -> u64 {
        self.requested_at_unix_millis
    }

    pub fn challenge(&self) -> &str {
        &self.challenge
    }
}

#[derive(Debug, Clone)]
pub struct TargetHostEvidenceReport {
    request: TargetHostEvidenceRequest,
    frozen_sqlserver_start_time: SqlServerStartIncarnation,
    snapshot: InstanceSnapshot,
}

impl TargetHostEvidenceReport {
    pub fn new(
        request: TargetHostEvidenceRequest,
        frozen_sqlserver_start_time: SqlServerStartIncarnation,
        snapshot: InstanceSnapshot,
    ) -> Self {
        Self {
            request,
            frozen_sqlserver_start_time,
            snapshot,
        }
    }

    pub fn request(&self) -> &TargetHostEvidenceRequest {
        &self.request
    }

    pub fn frozen_sqlserver_start_time(&self) -> &SqlServerStartIncarnation {
        &self.frozen_sqlserver_start_time
    }

    pub fn snapshot(&self) -> &InstanceSnapshot {
        &self.snapshot
    }
}

#[async_trait]
pub trait TargetHostEvidenceExchange: Send + Sync {
    async fn collect(
        &self,
        request: &TargetHostEvidenceRequest,
    ) -> Result<TargetHostEvidenceReport, KubericAdapterError>;
}

#[derive(Debug, Default)]
struct UnavailableTargetHostEvidenceExchange;

#[async_trait]
impl TargetHostEvidenceExchange for UnavailableTargetHostEvidenceExchange {
    async fn collect(
        &self,
        _request: &TargetHostEvidenceRequest,
    ) -> Result<TargetHostEvidenceReport, KubericAdapterError> {
        Err(KubericAdapterError::NativeConvergenceUnsafe(
            "target-host evidence exchange",
        ))
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeTopologyMemberBinding {
    native_replica_id: Guid,
    sql_endpoint: SqlServerMemberEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeTopologyBinding {
    availability_group: AvailabilityGroupIdentity,
    database_lineage: Option<DatabaseLineage>,
    bootstrap_database_name: Option<SqlIdentifier>,
    bootstrap_database: Option<StandaloneDatabaseIdentity>,
    local_sql_replica_identity: SqlReplicaIdentity,
    local_sql_server_start_incarnation: SqlServerStartIncarnation,
    local_stable_role: ReplicaRole,
    members: Vec<NativeTopologyMemberBinding>,
}

#[derive(Debug, Clone)]
struct NativeProgressBinding {
    topology: NativeTopologyBinding,
    latest_sequence: ConfigurationSequence,
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

    fn native_topology(&self) -> NativeTopologyBinding {
        NativeTopologyBinding {
            availability_group: self.availability_group.clone(),
            database_lineage: self.database_lineage.clone(),
            bootstrap_database_name: self.bootstrap_database_name.clone(),
            bootstrap_database: self.bootstrap_database.clone(),
            local_sql_replica_identity: self.local_sql_replica_identity.clone(),
            local_sql_server_start_incarnation: self.local_sql_server_start_incarnation.clone(),
            local_stable_role: self.local_member().stable_role,
            members: self
                .members
                .iter()
                .map(|member| NativeTopologyMemberBinding {
                    native_replica_id: member.native_replica_id.clone(),
                    sql_endpoint: member.sql_endpoint.clone(),
                })
                .collect(),
        }
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
    target_evidence: Arc<dyn TargetHostEvidenceExchange>,
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
        Self::with_bootstrap_source_and_topology_and_target_evidence(
            config,
            source,
            clock,
            expectation,
            bootstrap,
            executor,
            Arc::new(UnavailableTargetHostEvidenceExchange),
            seed_root,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_bootstrap_source_and_topology_and_target_evidence(
        config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        expectation: SqlServerTopologyExpectation,
        bootstrap: SqlServerBootstrapConfig,
        executor: Arc<dyn SqlMutationExecutor>,
        target_evidence: Arc<dyn TargetHostEvidenceExchange>,
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
                target_evidence,
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
                SqlServerReplicator::new_bootstrap_with_topology_and_target_evidence(
                    self.config.clone(),
                    self.source.clone(),
                    self.clock.clone(),
                    context.identity.clone(),
                    expectation.as_ref().clone(),
                    bootstrap.config.clone(),
                    bootstrap.executor.clone(),
                    bootstrap.target_evidence.clone(),
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
        ReplicatorInterfaces::primary(
            self.replicator.clone(),
            None,
            ReplicatorCapabilities::conservative(),
        )
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
        self.replicator
            .bind_process_session(context.process_session_id()?)?;
        Ok(self.interfaces())
    }
}

pub struct SqlServerReplicator {
    replication_address: String,
    source: Arc<dyn SqlServerObservationSource>,
    clock: Arc<dyn ObservationClock>,
    bootstrap: Option<ReplicatorBootstrapCapability>,
    runtime_identity: Option<KubericReplicaIdentity>,
    process_session_id: Mutex<Option<ProcessSessionId>>,
    expectation: Option<Arc<SqlServerTopologyExpectation>>,
    admitted_topology: Mutex<Option<Arc<HealthyTopologyBinding>>>,
    admitted_configuration: Mutex<Option<ReplicaSetConfiguration>>,
    admitted_bootstrap_database: Mutex<Option<StandaloneDatabaseIdentity>>,
    native_progress: Mutex<Option<NativeProgressBinding>>,
    lifecycle: RwLock<ReplicatorLifecycle>,
    lifecycle_cancel: CancellationToken,
    role: Mutex<Option<ReplicaRole>>,
    epoch: Mutex<Option<Epoch>>,
    #[cfg(feature = "kuberic-testing")]
    observation_injection: Mutex<Option<TestingObservationInjection>>,
    #[cfg(feature = "kuberic-testing")]
    lifecycle_publication_gate: Mutex<Option<TestingLifecyclePublicationGate>>,
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

#[cfg(feature = "kuberic-testing")]
struct TestingLifecyclePublicationGate {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[derive(Clone)]
struct ReplicatorBootstrapCapability {
    resource_uid: ResourceUid,
    config: SqlServerBootstrapConfig,
    executor: Arc<dyn SqlMutationExecutor>,
    target_evidence: Arc<dyn TargetHostEvidenceExchange>,
    seed: Arc<SeedCoordinator>,
}

struct SeedCoordinator {
    root: PathBuf,
    store: Mutex<Option<Arc<SeedDispatchStore>>>,
    arbiter: tokio::sync::Mutex<()>,
    store_worker: Arc<tokio::sync::Mutex<()>>,
    store_controls: Mutex<Vec<std::sync::Weak<SeedStoreWorkControl>>>,
    submission_active: std::sync::atomic::AtomicBool,
    #[cfg(feature = "kuberic-testing")]
    intent_gate: Mutex<Option<SeedGate>>,
    #[cfg(feature = "kuberic-testing")]
    submission_gate: Mutex<Option<SeedGate>>,
    #[cfg(feature = "kuberic-testing")]
    compaction_gate: Mutex<Option<SeedGate>>,
    #[cfg(feature = "kuberic-testing")]
    completion_gate: Mutex<Option<SeedGate>>,
    #[cfg(feature = "kuberic-testing")]
    store_gate: Mutex<Option<SeedStoreHook>>,
    target_evidence_challenge: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "kuberic-testing")]
struct SeedGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeedStoreOperation {
    Open,
    Load,
    Create,
    Replace,
    Remove,
}

#[cfg(feature = "kuberic-testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum TestingSeedStoreOperation {
    Open,
    Load,
    Create,
    Replace,
    Remove,
}

#[cfg(feature = "kuberic-testing")]
impl From<TestingSeedStoreOperation> for SeedStoreOperation {
    fn from(value: TestingSeedStoreOperation) -> Self {
        match value {
            TestingSeedStoreOperation::Open => Self::Open,
            TestingSeedStoreOperation::Load => Self::Load,
            TestingSeedStoreOperation::Create => Self::Create,
            TestingSeedStoreOperation::Replace => Self::Replace,
            TestingSeedStoreOperation::Remove => Self::Remove,
        }
    }
}

#[cfg(feature = "kuberic-testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum TestingSeedStoreStage {
    BeforeStart,
    Started,
    Completed,
}

#[cfg(feature = "kuberic-testing")]
struct SeedStoreGateState {
    released: Mutex<bool>,
    changed: Condvar,
}

#[cfg(feature = "kuberic-testing")]
struct SeedStoreHook {
    operation: SeedStoreOperation,
    stage: TestingSeedStoreStage,
    entered: Arc<tokio::sync::Notify>,
    state: Arc<SeedStoreGateState>,
}

#[cfg(feature = "kuberic-testing")]
impl SeedStoreHook {
    fn checkpoint(&self, stage: TestingSeedStoreStage, control: &SeedStoreWorkControl) {
        if self.stage != stage {
            return;
        }
        self.entered.notify_one();
        let mut released = self
            .state
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*released
            && (stage == TestingSeedStoreStage::Started
                || !control
                    .cancel_requested
                    .load(std::sync::atomic::Ordering::Acquire))
        {
            released = self
                .state
                .changed
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(feature = "kuberic-testing")]
#[doc(hidden)]
pub struct TestingSeedStoreGate {
    pub entered: Arc<tokio::sync::Notify>,
    state: Arc<SeedStoreGateState>,
}

#[cfg(feature = "kuberic-testing")]
impl TestingSeedStoreGate {
    pub fn release(&self) {
        *self
            .state
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.state.changed.notify_all();
    }
}

#[cfg(feature = "kuberic-testing")]
impl Drop for TestingSeedStoreGate {
    fn drop(&mut self) {
        self.release();
    }
}

struct SeedStoreWorkControl {
    // Cancellation and a blocking filesystem operation arbitrate once:
    // 0=pending, 1=started/non-cancellable, 2=cancelled.
    phase: std::sync::atomic::AtomicU8,
    cancel_requested: std::sync::atomic::AtomicBool,
    #[cfg(feature = "kuberic-testing")]
    gate: Option<Arc<SeedStoreGateState>>,
}

impl SeedStoreWorkControl {
    fn cancel(&self) {
        self.cancel_requested
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.phase.compare_exchange(
            0,
            2,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
        #[cfg(feature = "kuberic-testing")]
        if let Some(gate) = &self.gate {
            let _released = gate
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gate.changed.notify_all();
        }
    }

    fn begin(&self) -> bool {
        self.phase
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }
}

struct SeedStoreCancellation(Arc<SeedStoreWorkControl>);

impl Drop for SeedStoreCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

enum SeedStoreWorkResult<T> {
    Cancelled,
    Complete(Result<T, SeedStateError>),
}

impl SeedCoordinator {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            store: Mutex::new(None),
            arbiter: tokio::sync::Mutex::new(()),
            store_worker: Arc::new(tokio::sync::Mutex::new(())),
            store_controls: Mutex::new(Vec::new()),
            submission_active: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "kuberic-testing")]
            intent_gate: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            submission_gate: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            compaction_gate: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            completion_gate: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            store_gate: Mutex::new(None),
            target_evidence_challenge: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn register_store_control(
        &self,
        control: &Arc<SeedStoreWorkControl>,
    ) -> Result<(), SeedStateError> {
        let mut controls = self
            .store_controls
            .lock()
            .map_err(|_| SeedStateError::UnsafeState)?;
        controls.retain(|control| control.strong_count() != 0);
        controls.push(Arc::downgrade(control));
        Ok(())
    }

    fn cancel_store_work(&self) {
        let Ok(mut controls) = self.store_controls.lock() else {
            return;
        };
        controls.retain(|control| {
            if let Some(control) = control.upgrade() {
                control.cancel();
                true
            } else {
                false
            }
        });
    }

    #[cfg(feature = "kuberic-testing")]
    async fn pause_after_intent_if_requested(&self) -> Result<(), KubericAdapterError> {
        self.pause_if_requested(&self.intent_gate).await
    }

    #[cfg(feature = "kuberic-testing")]
    async fn pause_before_submission_if_requested(&self) -> Result<(), KubericAdapterError> {
        self.pause_if_requested(&self.submission_gate).await
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

struct ActiveSeedSubmission(Arc<SeedCoordinator>);

impl Drop for ActiveSeedSubmission {
    fn drop(&mut self) {
        self.0
            .submission_active
            .store(false, std::sync::atomic::Ordering::SeqCst);
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
    target: HealthyTopologyMemberBinding,
    build_id: String,
}

struct SeedDispatchAttempt {
    request: SeedDispatchRequest,
    task: tokio::task::JoinHandle<Result<SeedDispatchEvidence, RuntimeError>>,
}

enum SeedDispatchAction {
    ObserveAmbiguous,
    Submitted(SeedDispatchAttempt),
}

enum SourceAssociation {
    Absent,
    Incomplete,
    Exact(DatabaseIdentity),
}

enum BootstrapConvergenceResult {
    SourceFoundation,
    FullTopology(Box<BootstrapConvergedTopology>),
}

struct BootstrapConvergedTopology {
    instance: crate::observation::InstanceMetadata,
    group: AvailabilityGroupSnapshot,
    observed_at: u64,
    database: Option<StandaloneDatabaseIdentity>,
    deadline: tokio::time::Instant,
}

impl SqlServerReplicator {
    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_bind_process_session(
        &self,
        process_session_id: ProcessSessionId,
    ) -> Result<(), KubericAdapterError> {
        self.bind_process_session(process_session_id)
    }

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
    pub fn testing_gate_seed_submission(
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
            .submission_gate
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
    pub fn testing_gate_seed_store(
        &self,
        operation: TestingSeedStoreOperation,
        stage: TestingSeedStoreStage,
    ) -> Result<TestingSeedStoreGate, KubericAdapterError> {
        let seed = &self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?
            .seed;
        let entered = Arc::new(tokio::sync::Notify::new());
        let state = Arc::new(SeedStoreGateState {
            released: Mutex::new(false),
            changed: Condvar::new(),
        });
        let mut slot = seed
            .store_gate
            .lock()
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?;
        if slot.is_some() {
            return Err(KubericAdapterError::SeedState(SeedStateError::AlreadyOwned));
        }
        *slot = Some(SeedStoreHook {
            operation: operation.into(),
            stage,
            entered: entered.clone(),
            state: state.clone(),
        });
        Ok(TestingSeedStoreGate { entered, state })
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

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_gate_lifecycle_publication(
        &self,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<(), KubericAdapterError> {
        let mut gate = self.lifecycle_publication_gate.lock().map_err(|_| {
            KubericAdapterError::NativeConvergenceUnsafe("test lifecycle publication seam")
        })?;
        if gate.is_some() {
            return Err(KubericAdapterError::SeedState(SeedStateError::AlreadyOwned));
        }
        *gate = Some(TestingLifecyclePublicationGate { entered, release });
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
        Self::new_bootstrap_with_topology_and_target_evidence(
            service_config,
            source,
            clock,
            runtime_identity,
            expectation,
            bootstrap,
            executor,
            Arc::new(UnavailableTargetHostEvidenceExchange),
            seed_root,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_bootstrap_with_topology_and_target_evidence(
        service_config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        runtime_identity: KubericReplicaIdentity,
        expectation: SqlServerTopologyExpectation,
        bootstrap: SqlServerBootstrapConfig,
        executor: Arc<dyn SqlMutationExecutor>,
        target_evidence: Arc<dyn TargetHostEvidenceExchange>,
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
                target_evidence,
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
            process_session_id: Mutex::new(None),
            expectation,
            admitted_topology: Mutex::new(None),
            admitted_configuration: Mutex::new(None),
            admitted_bootstrap_database: Mutex::new(None),
            native_progress: Mutex::new(None),
            lifecycle: RwLock::new(ReplicatorLifecycle {
                state: CREATED,
                generation: 0,
            }),
            lifecycle_cancel: CancellationToken::new(),
            role: Mutex::new(None),
            epoch: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            observation_injection: Mutex::new(None),
            #[cfg(feature = "kuberic-testing")]
            lifecycle_publication_gate: Mutex::new(None),
        }
    }

    pub fn admitted_topology(&self) -> Option<Arc<HealthyTopologyBinding>> {
        self.admitted_topology
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn native_progress_binding(&self) -> Option<NativeProgressBinding> {
        self.native_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn publish_native_progress(
        &self,
        topology: NativeTopologyBinding,
        sequence: ConfigurationSequence,
    ) -> Result<(), KubericAdapterError> {
        let mut accepted = self
            .native_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = accepted.as_ref() {
            if previous.topology != topology {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "native topology differs from the bound association",
                ));
            }
            if sequence < previous.latest_sequence {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "configuration sequence regressed",
                ));
            }
        }
        *accepted = Some(NativeProgressBinding {
            topology,
            latest_sequence: sequence,
        });
        Ok(())
    }

    fn validate_native_progress(
        &self,
        topology: &NativeTopologyBinding,
        sequence: ConfigurationSequence,
    ) -> Result<(), KubericAdapterError> {
        if let Some(previous) = self.native_progress_binding() {
            if previous.topology != *topology {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "native topology differs from the bound association",
                ));
            }
            if sequence < previous.latest_sequence {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "configuration sequence regressed",
                ));
            }
        }
        Ok(())
    }

    fn published_role(&self) -> Option<ReplicaRole> {
        *self
            .role
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn bind_process_session(
        &self,
        process_session_id: ProcessSessionId,
    ) -> Result<(), KubericAdapterError> {
        let mut accepted = self
            .process_session_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if accepted
            .as_ref()
            .is_some_and(|accepted| accepted != &process_session_id)
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "runtime process session changed after Replicator construction",
            ));
        }
        *accepted = Some(process_session_id);
        Ok(())
    }

    fn process_session_id(
        &self,
        current: &ReplicaSetConfiguration,
    ) -> Result<ProcessSessionId, KubericAdapterError> {
        let accepted = self
            .process_session_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        accepted
            .or_else(|| {
                self.runtime_identity.as_ref().and_then(|identity| {
                    current
                        .replicas
                        .iter()
                        .find(|replica| replica.identity == *identity)
                        .map(|replica| replica.process_session_id.clone())
                        .filter(|session| !session.is_empty())
                })
            })
            .or_else(|| {
                self.bootstrap.as_ref().and_then(|_| {
                    self.admitted_topology()
                        .map(|binding| binding.local_member().process_session_id.clone())
                        .filter(|session| !session.is_empty())
                })
            })
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "runtime process session is unavailable",
            ))
    }

    fn activation_id(
        &self,
        configuration: &ConfigurationDescriptor,
    ) -> Result<String, KubericAdapterError> {
        let identity = configuration
            .members
            .iter()
            .find(|member| member.identity.replica_id == configuration.primary_id)
            .map(|member| &member.identity)
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "configuration primary identity is unavailable",
            ))?;
        let mut digest = Sha256::new();
        let replica_id = identity.replica_id.to_string();
        for value in [
            "kuberic-mssql-public-v1-activation",
            replica_id.as_str(),
            identity.instance_id.as_str(),
            identity.agent_generation.as_str(),
        ] {
            digest.update(value.as_bytes());
            digest.update(b"\0");
        }
        Ok(digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
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

    #[cfg(feature = "kuberic-testing")]
    fn wait_for_testing_lifecycle_publication(&self) -> Result<(), KubericAdapterError> {
        let gate = self
            .lifecycle_publication_gate
            .lock()
            .map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("test lifecycle publication seam")
            })?
            .take();
        if let Some(gate) = gate {
            gate.entered.send(()).map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("test lifecycle publication seam")
            })?;
            gate.release.recv().map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("test lifecycle publication seam")
            })?;
        }
        Ok(())
    }

    async fn seed_store_work<T, F>(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        operation: SeedStoreOperation,
        work: F,
    ) -> Result<T, KubericAdapterError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, SeedStateError> + Send + 'static,
    {
        let seed = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?
            .seed
            .clone();
        #[cfg(not(feature = "kuberic-testing"))]
        let _ = operation;
        drop(self.finish_operation(generation)?);
        let permit = tokio::select! {
            biased;
            _ = self.lifecycle_cancel.cancelled() => {
                return Err(KubericAdapterError::Closed);
            }
            permit = tokio::time::timeout(
                bootstrap_remaining(deadline)?,
                seed.store_worker.clone().lock_owned(),
            ) => permit.map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?,
        };
        drop(self.finish_operation(generation)?);
        bootstrap_remaining(deadline)?;

        #[cfg(feature = "kuberic-testing")]
        let hook = {
            let mut pending = seed
                .store_gate
                .lock()
                .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?;
            if pending
                .as_ref()
                .is_some_and(|hook| hook.operation == operation)
            {
                pending.take()
            } else {
                None
            }
        };
        let control = Arc::new(SeedStoreWorkControl {
            phase: std::sync::atomic::AtomicU8::new(0),
            cancel_requested: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "kuberic-testing")]
            gate: hook.as_ref().map(|hook| hook.state.clone()),
        });
        seed.register_store_control(&control)
            .map_err(KubericAdapterError::SeedState)?;
        if self.lifecycle_cancel.is_cancelled() {
            control.cancel();
        }
        let cancellation = SeedStoreCancellation(control.clone());
        let (completed, mut result) = tokio::sync::oneshot::channel();
        let thread_control = control.clone();
        std::thread::Builder::new()
            .name("mssql-seed-store".into())
            .spawn(move || {
                let _permit = permit;
                #[cfg(feature = "kuberic-testing")]
                if let Some(hook) = &hook {
                    hook.checkpoint(TestingSeedStoreStage::BeforeStart, &thread_control);
                }
                if !thread_control.begin() {
                    let _ = completed.send(SeedStoreWorkResult::Cancelled);
                    return;
                }
                #[cfg(feature = "kuberic-testing")]
                if let Some(hook) = &hook {
                    hook.checkpoint(TestingSeedStoreStage::Started, &thread_control);
                }
                let outcome = work();
                #[cfg(feature = "kuberic-testing")]
                if let Some(hook) = &hook {
                    hook.checkpoint(TestingSeedStoreStage::Completed, &thread_control);
                }
                let _ = completed.send(SeedStoreWorkResult::Complete(outcome));
            })
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::Io))?;

        enum Stop {
            Deadline,
            Closed,
        }
        let mut stop = None;
        let outcome = match tokio::select! {
            biased;
            response = &mut result => Some(response),
            _ = tokio::time::sleep_until(deadline) => {
                stop = Some(Stop::Deadline);
                None
            }
            _ = self.lifecycle_cancel.cancelled() => {
                stop = Some(Stop::Closed);
                None
            }
        } {
            Some(response) => response,
            None => {
                control.cancel();
                result.await
            }
        }
        .map_err(|_| {
            KubericAdapterError::NativeConvergenceUnsafe("seed dispatch store worker terminated")
        })?;
        drop(cancellation);

        let value = match outcome {
            SeedStoreWorkResult::Cancelled => {
                let stop =
                    stop.or_else(|| self.lifecycle_cancel.is_cancelled().then_some(Stop::Closed));
                return Err(match stop {
                    Some(Stop::Closed) => KubericAdapterError::Closed,
                    Some(Stop::Deadline) | None => KubericAdapterError::NativeConvergenceDeadline,
                });
            }
            SeedStoreWorkResult::Complete(result) => {
                result.map_err(KubericAdapterError::SeedState)?
            }
        };
        if let Some(stop) = stop {
            return Err(match stop {
                Stop::Deadline => KubericAdapterError::NativeConvergenceDeadline,
                Stop::Closed => KubericAdapterError::Closed,
            });
        }
        drop(self.finish_operation(generation)?);
        bootstrap_remaining(deadline)?;
        Ok(value)
    }

    async fn seed_store(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<SeedDispatchStore>, KubericAdapterError> {
        let seed = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?
            .seed
            .clone();
        if let Some(store) = seed
            .store
            .lock()
            .map_err(|_| KubericAdapterError::SeedState(SeedStateError::UnsafeState))?
            .as_ref()
            .cloned()
        {
            return Ok(store);
        }
        let root = seed.root.clone();
        let slot = seed.clone();
        self.seed_store_work(generation, deadline, SeedStoreOperation::Open, move || {
            if let Some(existing) = slot
                .store
                .lock()
                .map_err(|_| SeedStateError::UnsafeState)?
                .as_ref()
                .cloned()
            {
                return Ok(existing);
            }
            let store = Arc::new(SeedDispatchStore::open(&root)?);
            let mut current = slot.store.lock().map_err(|_| SeedStateError::UnsafeState)?;
            if let Some(existing) = current.as_ref() {
                return Ok(existing.clone());
            }
            *current = Some(store.clone());
            Ok(store)
        })
        .await
    }

    async fn seed_store_load(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Option<SeedDispatchRecord>, KubericAdapterError> {
        let store = self.seed_store(generation, deadline).await?;
        self.seed_store_work(generation, deadline, SeedStoreOperation::Load, move || {
            store.load()
        })
        .await
    }

    async fn seed_store_create(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        record: SeedDispatchRecord,
    ) -> Result<(), KubericAdapterError> {
        let store = self.seed_store(generation, deadline).await?;
        self.seed_store_work(
            generation,
            deadline,
            SeedStoreOperation::Create,
            move || store.create(&record),
        )
        .await
    }

    async fn seed_store_replace(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        expected_revision: u64,
        record: SeedDispatchRecord,
    ) -> Result<SeedDispatchRecord, KubericAdapterError> {
        let store = self.seed_store(generation, deadline).await?;
        self.seed_store_work(
            generation,
            deadline,
            SeedStoreOperation::Replace,
            move || store.replace(expected_revision, record),
        )
        .await
    }

    async fn seed_store_replace_intent(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        expected_revision: u64,
        record: SeedDispatchRecord,
    ) -> Result<SeedDispatchRecord, KubericAdapterError> {
        let store = self.seed_store(generation, deadline).await?;
        self.seed_store_work(
            generation,
            deadline,
            SeedStoreOperation::Replace,
            move || store.replace_intent(expected_revision, record),
        )
        .await
    }

    async fn seed_store_remove(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        expected_revision: u64,
    ) -> Result<(), KubericAdapterError> {
        let store = self.seed_store(generation, deadline).await?;
        self.seed_store_work(
            generation,
            deadline,
            SeedStoreOperation::Remove,
            move || store.remove(expected_revision),
        )
        .await
    }

    async fn observe_group_snapshot(
        &self,
        generation: u64,
        require_progress_role: bool,
        allow_absent: bool,
    ) -> Result<
        Option<(
            crate::observation::InstanceMetadata,
            AvailabilityGroupSnapshot,
            u64,
        )>,
        KubericAdapterError,
    > {
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
            Observation::Absent { .. } if allow_absent => return Ok(None),
            Observation::Absent { .. } => return Err(KubericAdapterError::AvailabilityGroupAbsent),
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
        Ok(Some((snapshot.instance, group, observed_at)))
    }

    async fn observed_group(
        &self,
        generation: u64,
        require_progress_role: bool,
        requested_role: Option<ReplicaRole>,
    ) -> Result<(AvailabilityGroupSnapshot, u64), KubericAdapterError> {
        let (instance, group, observed_at) = self
            .observe_group_snapshot(generation, require_progress_role, false)
            .await?
            .expect("absence is rejected");
        let admitted = self.admitted_topology();
        if let Some(binding) = &admitted {
            validate_bound_evidence(
                binding,
                self.source.observer_config(),
                &instance,
                &group,
                requested_role,
            )?;
            self.validate_native_progress(
                &binding.native_topology(),
                group.configuration_sequence,
            )?;
            if require_progress_role
                && self
                    .published_role()
                    .is_some_and(|role| !published_role_matches(binding, role))
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "published local role differs from the frozen stable role",
                ));
            }
        } else if let Some(binding) = self.native_progress_binding() {
            validate_native_bound_evidence(
                &binding.topology,
                self.source.observer_config(),
                &instance,
                &group,
                requested_role,
            )?;
            self.validate_native_progress(&binding.topology, group.configuration_sequence)?;
            if require_progress_role
                && self
                    .published_role()
                    .is_none_or(|role| role != binding.topology.local_stable_role)
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "published local role differs from the bound stable role",
                ));
            }
        } else if require_progress_role {
            if let Some(role) = self.published_role() {
                validate_native_role(role, group.local_replica.role.clone())?;
            }
        }
        Ok((group, observed_at))
    }

    async fn observe_secondary_association(
        &self,
        generation: u64,
    ) -> Result<Option<(NativeTopologyBinding, ConfigurationSequence, u64)>, KubericAdapterError>
    {
        let expectation = self
            .expectation
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let runtime_identity = self
            .runtime_identity
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let Some((instance, group, observed_at)) =
            self.observe_group_snapshot(generation, true, true).await?
        else {
            return Ok(None);
        };
        let binding = associate_native_topology(
            runtime_identity,
            expectation,
            &instance,
            &group,
            ReplicaRole::ActiveSecondary,
        )?;
        validate_native_bound_evidence(
            &binding,
            self.source.observer_config(),
            &instance,
            &group,
            Some(ReplicaRole::ActiveSecondary),
        )?;
        self.validate_native_progress(&binding, group.configuration_sequence)?;
        Ok(Some((binding, group.configuration_sequence, observed_at)))
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

    async fn observe_availability_group_ownership(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        executor: &dyn SqlMutationExecutor,
        database: &SqlIdentifier,
    ) -> Result<Option<crate::bootstrap::StandaloneDatabaseSnapshot>, KubericAdapterError> {
        let remaining = bootstrap_remaining(deadline)?;
        let result = tokio::time::timeout(
            remaining,
            executor.observe_availability_group_ownership(database),
        )
        .await;
        drop(self.finish_operation(generation)?);
        result
            .map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))
    }

    async fn observe_effect_completion(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        executor: &dyn SqlMutationExecutor,
        effect: &MutationEffect,
    ) -> Result<bool, KubericAdapterError> {
        let remaining = bootstrap_remaining(deadline)?;
        let result =
            tokio::time::timeout(remaining, executor.observe_effect_completion(effect)).await;
        drop(self.finish_operation(generation)?);
        result
            .map_err(|_| KubericAdapterError::NativeConvergenceDeadline)?
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))
    }

    async fn require_availability_group_ownership(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        configuration_id: &str,
        expected_database: Option<&StandaloneDatabaseIdentity>,
        expected_group: &AvailabilityGroupIdentity,
    ) -> Result<(), KubericAdapterError> {
        let bootstrap = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let expected_database_ownership = StandaloneDatabaseOwnership {
            resource_id: bootstrap.resource_uid.to_string(),
            configuration_id: configuration_id.to_owned(),
            activation_id: self.activation_id(
                &self
                    .admitted_topology()
                    .ok_or(KubericAdapterError::TopologyNotAdmitted)?
                    .configuration,
            )?,
        };
        let snapshot = self
            .observe_availability_group_ownership(
                generation,
                deadline,
                bootstrap.executor.as_ref(),
                bootstrap.config.database(),
            )
            .await?;
        match classify_standalone_database(
            snapshot.as_ref(),
            expected_database,
            &expected_database_ownership,
            false,
        ) {
            NativeConvergenceState::Complete => {}
            NativeConvergenceState::Unsafe => {
                return Err(KubericAdapterError::NativeConvergenceUnsafe(
                    "availability-group ownership database",
                ));
            }
            NativeConvergenceState::Absent
            | NativeConvergenceState::Incomplete
            | NativeConvergenceState::Conflicting => {
                return Err(KubericAdapterError::NativeConvergenceConflict(
                    "availability-group ownership database",
                ));
            }
        }
        let expected_marker = AvailabilityGroupOwnership {
            resource_id: expected_database_ownership.resource_id,
            configuration_id: expected_database_ownership.configuration_id,
            activation_id: expected_database_ownership.activation_id,
            name: expected_group.name.clone(),
            creation_group_id: Some(expected_group.group_id.clone()),
            group_id: Some(expected_group.group_id.clone()),
        };
        if snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.availability_group_ownership.as_ref())
            != Some(&expected_marker)
        {
            return Err(KubericAdapterError::NativeConvergenceConflict(
                "availability-group ownership marker",
            ));
        }
        Ok(())
    }

    async fn converge_bootstrap_configuration(
        &self,
        generation: u64,
        current: &ReplicaSetConfiguration,
        scope: BootstrapConvergenceScope,
    ) -> Result<BootstrapConvergenceResult, KubericAdapterError> {
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
        let compatibility = CurrentTopologyFacts::translate(
            runtime_identity,
            &self.process_session_id(current)?,
            expectation,
            &self.replication_address,
            current,
        )?;
        let ownership = compatibility
            .bootstrap_ownership(runtime_identity)
            .map_err(|_| {
                KubericAdapterError::TopologyBindingMismatch(
                    "local bootstrap ownership is not represented by the current configuration",
                )
            })?;
        let descriptors = compatibility.bootstrap_descriptors(expectation)?;
        let expected_ownership = StandaloneDatabaseOwnership {
            resource_id: bootstrap.resource_uid.to_string(),
            configuration_id: compatibility.configuration.configuration_id.to_string(),
            activation_id: self.activation_id(&compatibility.configuration)?,
        };
        let expected_group_ownership = AvailabilityGroupOwnership {
            resource_id: expected_ownership.resource_id.clone(),
            configuration_id: expected_ownership.configuration_id.clone(),
            activation_id: expected_ownership.activation_id.clone(),
            name: self
                .source
                .observer_config()
                .target()
                .availability_group
                .clone(),
            creation_group_id: None,
            group_id: None,
        };
        let retain_database_creation_intent =
            MutationEffect::retain_source_database_creation_intent(
                bootstrap.config.database.clone(),
                &expected_ownership.resource_id,
                &expected_ownership.configuration_id,
                &expected_ownership.activation_id,
            )
            .map_err(|_| {
                KubericAdapterError::InvalidConfiguration(
                    "bootstrap database creation intent is invalid",
                )
            })?;
        let create_database = MutationEffect::create_source_database(
            bootstrap.config.database.clone(),
            &expected_ownership.resource_id,
            &expected_ownership.configuration_id,
            &expected_ownership.activation_id,
        )
        .map_err(|_| {
            KubericAdapterError::InvalidConfiguration(
                "bootstrap database ownership markers are invalid",
            )
        })?;
        let remove_database_creation_intent =
            MutationEffect::remove_source_database_creation_intent(
                bootstrap.config.database(),
                &expected_ownership.resource_id,
                &expected_ownership.configuration_id,
                &expected_ownership.activation_id,
            )
            .map_err(|_| {
                KubericAdapterError::InvalidConfiguration(
                    "bootstrap database creation intent is invalid",
                )
            })?;
        let configure_database_recovery =
            MutationEffect::configure_source_database_recovery(bootstrap.config.database.clone());
        let retain_database_ownership = MutationEffect::retain_source_database_ownership(
            bootstrap.config.database.clone(),
            &expected_ownership.resource_id,
            &expected_ownership.configuration_id,
            &expected_ownership.activation_id,
        )
        .map_err(|_| {
            KubericAdapterError::InvalidConfiguration(
                "bootstrap database ownership markers are invalid",
            )
        })?;
        let create_marker_table =
            MutationEffect::create_source_marker_table(bootstrap.config.database.clone());
        let backup_database =
            MutationEffect::backup_source_database(bootstrap.config.database.clone());
        let backup_log = MutationEffect::backup_source_log(bootstrap.config.database.clone());
        let verify_backup = MutationEffect::verify_source_backup(bootstrap.config.database.clone());
        let retain_group_intent = MutationEffect::retain_availability_group_intent(
            bootstrap.config.database.clone(),
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
            &expected_ownership.resource_id,
            &expected_ownership.configuration_id,
            &expected_ownership.activation_id,
        )
        .map_err(|_| {
            KubericAdapterError::InvalidConfiguration(
                "availability-group ownership intent is invalid",
            )
        })?;
        let create_group = MutationEffect::create_availability_group(
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
            &expected_ownership.resource_id,
            &expected_ownership.configuration_id,
            &expected_ownership.activation_id,
            descriptors.clone(),
        )
        .map_err(|_| {
            KubericAdapterError::InvalidConfiguration("bootstrap replica descriptors are invalid")
        })?;
        let retain_group_creation_intent =
            MutationEffect::retain_availability_group_creation_intent(
                bootstrap.config.database.clone(),
                self.source
                    .observer_config()
                    .target()
                    .availability_group
                    .clone(),
                &expected_ownership.resource_id,
                &expected_ownership.configuration_id,
                &expected_ownership.activation_id,
            )
            .map_err(|_| {
                KubericAdapterError::InvalidConfiguration(
                    "availability-group creation intent is invalid",
                )
            })?;
        let bind_group = MutationEffect::bind_availability_group_ownership(
            bootstrap.config.database.clone(),
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
        );
        let remove_group_creation_intent =
            MutationEffect::remove_availability_group_creation_intent(
                &self.source.observer_config().target().availability_group,
                &expected_ownership.resource_id,
                &expected_ownership.configuration_id,
                &expected_ownership.activation_id,
            )
            .map_err(|_| {
                KubericAdapterError::InvalidConfiguration(
                    "availability-group creation intent is invalid",
                )
            })?;
        let join_group = MutationEffect::join_availability_group(
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
        );
        let grant_create_any_database = MutationEffect::grant_create_any_database(
            self.source
                .observer_config()
                .target()
                .availability_group
                .clone(),
        );
        let retain_create_any_database_evidence =
            MutationEffect::retain_create_any_database_evidence(
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
            let ownership_database = match ownership {
                BootstrapOwnership::PrimaryTopology => database.clone(),
                BootstrapOwnership::LocalJoin => {
                    self.observe_availability_group_ownership(
                        generation,
                        deadline,
                        bootstrap.executor.as_ref(),
                        bootstrap.config.database(),
                    )
                    .await?
                }
            };
            let database_identity = database
                .as_ref()
                .and_then(|database| database.identity.clone());
            let availability_group_ownership = ownership_database
                .as_ref()
                .and_then(|database| database.availability_group_ownership.as_ref());
            match ownership {
                BootstrapOwnership::PrimaryTopology => {
                    match database_state {
                        NativeConvergenceState::Absent => {
                            let intent_ready = self
                                .observe_effect_completion(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &retain_database_creation_intent,
                                )
                                .await?;
                            let effect = if intent_ready {
                                &create_database
                            } else {
                                &retain_database_creation_intent
                            };
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                effect,
                            )
                            .await?;
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
                            continue;
                        }
                        NativeConvergenceState::Incomplete => {
                            let snapshot = database.as_ref().ok_or(
                                KubericAdapterError::NativeConvergenceUnsafe(
                                    "standalone source database",
                                ),
                            )?;
                            let expected_token = crate::bootstrap::source_database_creation_token(
                                bootstrap.config.database(),
                                &expected_ownership,
                            );
                            if snapshot.creation_token.as_deref() != Some(&expected_token) {
                                return Err(KubericAdapterError::NativeConvergenceConflict(
                                    "standalone source database creation",
                                ));
                            }
                            if self
                                .observe_effect_completion(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &retain_database_creation_intent,
                                )
                                .await?
                            {
                                self.execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &remove_database_creation_intent,
                                )
                                .await?;
                                self.wait_for_bootstrap_retry(generation, deadline).await?;
                                continue;
                            }
                            let effect = if snapshot.recovery_model != "FULL" {
                                &configure_database_recovery
                            } else if snapshot.ownership.is_none() {
                                &retain_database_ownership
                            } else if !snapshot.marker_table_ready {
                                &create_marker_table
                            } else if !snapshot.full_backup_ready {
                                &backup_database
                            } else if !snapshot.log_chain_ready {
                                &backup_log
                            } else if !snapshot.backup_verified {
                                &verify_backup
                            } else {
                                return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                    "standalone source database classification",
                                ));
                            };
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                effect,
                            )
                            .await?;
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
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
                    match classify_owned_availability_group(
                        group.as_ref(),
                        availability_group_ownership,
                        &expected_group_ownership,
                        &descriptors,
                        bootstrap.config.database(),
                        database_identity.as_ref(),
                        false,
                    ) {
                        NativeConvergenceState::Complete
                            if scope == BootstrapConvergenceScope::SourceFoundation
                                && group
                                    .as_ref()
                                    .is_some_and(bootstrap_source_foundation_ready) =>
                        {
                            drop(self.finish_operation(generation)?);
                            bootstrap_remaining(deadline)?;
                            return Ok(BootstrapConvergenceResult::SourceFoundation);
                        }
                        NativeConvergenceState::Complete
                            if group.as_ref().is_some_and(|group| {
                                bootstrap_membership_ready(group, ownership, &descriptors)
                            }) =>
                        {
                            if self
                                .observe_effect_completion(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &retain_group_creation_intent,
                                )
                                .await?
                            {
                                self.execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &remove_group_creation_intent,
                                )
                                .await?;
                                self.wait_for_bootstrap_retry(generation, deadline).await?;
                                continue;
                            }
                            if !self
                                .observe_effect_completion(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &grant_create_any_database,
                                )
                                .await?
                            {
                                self.execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &grant_create_any_database,
                                )
                                .await?;
                                self.execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &retain_create_any_database_evidence,
                                )
                                .await?;
                                self.wait_for_bootstrap_retry(generation, deadline).await?;
                                continue;
                            }
                            let group = group.expect("complete classifier requires a group");
                            drop(self.finish_operation(generation)?);
                            bootstrap_remaining(deadline)?;
                            return Ok(BootstrapConvergenceResult::FullTopology(Box::new(
                                BootstrapConvergedTopology {
                                    instance,
                                    group,
                                    observed_at,
                                    database: database_identity,
                                    deadline,
                                },
                            )));
                        }
                        NativeConvergenceState::Absent => {
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                &retain_group_intent,
                            )
                            .await?;
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
                            continue;
                        }
                        NativeConvergenceState::Incomplete if group.is_none() => {
                            let effect = if self
                                .observe_effect_completion(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &retain_group_creation_intent,
                                )
                                .await?
                            {
                                &create_group
                            } else {
                                &retain_group_creation_intent
                            };
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                effect,
                            )
                            .await?;
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
                            continue;
                        }
                        NativeConvergenceState::Incomplete
                            if group.as_ref().is_some_and(|group| {
                                availability_group_ownership.is_some_and(|marker| {
                                    marker.group_id.is_none()
                                        && marker.creation_group_id.as_ref()
                                            == Some(&group.identity.group_id)
                                })
                            }) =>
                        {
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                &bind_group,
                            )
                            .await?;
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
                            continue;
                        }
                        NativeConvergenceState::Incomplete | NativeConvergenceState::Complete => {}
                        NativeConvergenceState::Conflicting => {
                            let label = if group.as_ref().is_some_and(|group| {
                                availability_group_ownership
                                    != Some(&AvailabilityGroupOwnership {
                                        resource_id: expected_group_ownership.resource_id.clone(),
                                        configuration_id: expected_group_ownership
                                            .configuration_id
                                            .clone(),
                                        activation_id: expected_group_ownership
                                            .activation_id
                                            .clone(),
                                        name: expected_group_ownership.name.clone(),
                                        creation_group_id: Some(group.identity.group_id.clone()),
                                        group_id: Some(group.identity.group_id.clone()),
                                    })
                            }) {
                                "availability-group ownership marker"
                            } else {
                                "availability group"
                            };
                            return Err(KubericAdapterError::NativeConvergenceConflict(label));
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
                    let expected_group = match availability_group_ownership {
                        Some(AvailabilityGroupOwnership {
                            resource_id,
                            configuration_id,
                            activation_id,
                            name,
                            creation_group_id: Some(creation_group_id),
                            group_id: Some(group_id),
                        }) if resource_id == &expected_ownership.resource_id
                            && configuration_id == &expected_ownership.configuration_id
                            && activation_id == &expected_ownership.activation_id
                            && creation_group_id == group_id
                            && name
                                == &self.source.observer_config().target().availability_group =>
                        {
                            AvailabilityGroupIdentity {
                                name: name.clone(),
                                group_id: group_id.clone(),
                            }
                        }
                        Some(AvailabilityGroupOwnership { group_id: None, .. }) | None => {
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
                            continue;
                        }
                        Some(_) => {
                            return Err(KubericAdapterError::NativeConvergenceConflict(
                                "availability-group ownership",
                            ));
                        }
                    };
                    let Some(group) = group else {
                        if join_absence_observed {
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                &join_group,
                            )
                            .await?;
                        } else {
                            join_absence_observed = true;
                        }
                        self.wait_for_bootstrap_retry(generation, deadline).await?;
                        continue;
                    };
                    match classify_owned_availability_group(
                        Some(&group),
                        availability_group_ownership,
                        &expected_group_ownership,
                        &descriptors,
                        bootstrap.config.database(),
                        database_identity.as_ref(),
                        false,
                    ) {
                        NativeConvergenceState::Conflicting => {
                            let label = if availability_group_ownership
                                != Some(&AvailabilityGroupOwnership {
                                    resource_id: expected_group_ownership.resource_id.clone(),
                                    configuration_id: expected_group_ownership
                                        .configuration_id
                                        .clone(),
                                    activation_id: expected_group_ownership.activation_id.clone(),
                                    name: expected_group_ownership.name.clone(),
                                    creation_group_id: Some(group.identity.group_id.clone()),
                                    group_id: Some(group.identity.group_id.clone()),
                                }) {
                                "availability-group ownership marker"
                            } else {
                                "availability group"
                            };
                            return Err(KubericAdapterError::NativeConvergenceConflict(label));
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
                        &expected_group,
                        &group.local_replica.identity,
                        local.server_name(),
                        local.endpoint_url(),
                        false,
                    ) {
                        NativeConvergenceState::Complete
                            if bootstrap_membership_ready(&group, ownership, &descriptors) =>
                        {
                            if !self
                                .observe_effect_completion(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &grant_create_any_database,
                                )
                                .await?
                            {
                                self.execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &grant_create_any_database,
                                )
                                .await?;
                                self.execute_bootstrap_effect(
                                    generation,
                                    deadline,
                                    bootstrap.executor.as_ref(),
                                    &retain_create_any_database_evidence,
                                )
                                .await?;
                                self.wait_for_bootstrap_retry(generation, deadline).await?;
                                continue;
                            }
                            drop(self.finish_operation(generation)?);
                            bootstrap_remaining(deadline)?;
                            return Ok(BootstrapConvergenceResult::FullTopology(Box::new(
                                BootstrapConvergedTopology {
                                    instance,
                                    group,
                                    observed_at,
                                    database: database_identity,
                                    deadline,
                                },
                            )));
                        }
                        NativeConvergenceState::Absent | NativeConvergenceState::Incomplete => {
                            self.execute_bootstrap_effect(
                                generation,
                                deadline,
                                bootstrap.executor.as_ref(),
                                &join_group,
                            )
                            .await?;
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
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
        let retry_delay = self
            .source
            .observer_config()
            .poll_interval()
            .max(Duration::from_millis(100))
            .min(remaining);
        tokio::time::sleep(retry_delay).await;
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
        validate_build_id(replica)?;
        let binding = self
            .admitted_topology()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let admitted_configuration = self
            .admitted_configuration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        validate_admitted_build_configuration(&binding, &admitted_configuration)?;
        let database = self
            .admitted_bootstrap_database
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let published_role = *self
            .role
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let compatibility = CurrentBuildFacts::translate(&binding, replica, published_role)?;
        let record = SeedDispatchRecord::new(
            bootstrap.resource_uid.to_string(),
            binding.local_identity.clone(),
            binding.configuration.configuration_id.to_string(),
            binding.availability_group.name.as_str(),
            binding.availability_group.group_id.as_str(),
            database.name.as_str(),
            database.database_guid.as_str(),
            database.family_guid.as_str(),
            database.recovery_fork_guid.as_str(),
            compatibility.source,
            compatibility.targets,
            compatibility.winning_target_replica_id,
            compatibility.build_id.clone(),
            compatibility.target_storage,
            binding.local_sql_server_start_incarnation.as_str(),
        )
        .map_err(KubericAdapterError::SeedState)?;
        Ok(SeedBuildContext {
            binding,
            database,
            record,
            target: compatibility.target,
            build_id: compatibility.build_id,
        })
    }

    async fn collect_target_host_evidence(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        context: &SeedBuildContext,
        database: &DatabaseIdentity,
    ) -> Result<NativeConvergenceState, KubericAdapterError> {
        let bootstrap = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let requested_at_unix_millis = self
            .clock
            .now_unix_millis()
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
        let challenge_sequence = bootstrap
            .seed
            .target_evidence_challenge
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let request = TargetHostEvidenceRequest {
            resource_uid: bootstrap.resource_uid.to_string(),
            configuration_id: context.binding.configuration.configuration_id.to_string(),
            build_id: context.build_id.clone(),
            target_identity: context.target.kuberic_identity.clone(),
            target_process_session_id: context.target.process_session_id.clone(),
            target_native_replica_id: context.target.native_replica_id.clone(),
            target_server_name: context.target.server_name().clone(),
            target_endpoint_url: context.target.sql_endpoint_url().to_owned(),
            availability_group: context.binding.availability_group.clone(),
            database: context.database.clone(),
            group_database_id: database.group_database_id.clone(),
            requested_at_unix_millis,
            challenge: format!(
                "{}:{}:{challenge_sequence}",
                context.build_id, context.target.process_session_id
            ),
        };
        let report = tokio::time::timeout(
            bootstrap_remaining(deadline)?,
            bootstrap.target_evidence.collect(&request),
        )
        .await
        .map_err(|_| KubericAdapterError::NativeConvergenceDeadline)??;
        drop(self.finish_operation(generation)?);
        let now_unix_millis = self
            .clock
            .now_unix_millis()
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
        validate_target_host_evidence(
            &request,
            &report,
            now_unix_millis,
            self.source.observer_config().max_age_millis(),
        )
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

    async fn compact_seed_record(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        mut record: SeedDispatchRecord,
        database: &DatabaseIdentity,
    ) -> Result<(), KubericAdapterError> {
        loop {
            if record.phase() != SeedDispatchPhase::Accepted {
                return Err(KubericAdapterError::SeedState(
                    SeedStateError::InvalidTransition,
                ));
            }
            if record.group_database_id() != Some(database.group_database_id.as_str()) {
                record
                    .associate_group_database(database.group_database_id.as_str())
                    .map_err(KubericAdapterError::SeedState)?;
                let revision = record.revision();
                match self
                    .seed_store_replace(generation, deadline, revision, record)
                    .await
                {
                    Ok(updated) => record = updated,
                    Err(KubericAdapterError::SeedState(SeedStateError::RevisionMismatch)) => {
                        let Some(current) = self.seed_store_load(generation, deadline).await?
                        else {
                            return Ok(());
                        };
                        record = current;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            match self
                .seed_store_remove(generation, deadline, record.revision())
                .await
            {
                Ok(()) | Err(KubericAdapterError::SeedState(SeedStateError::Missing)) => {
                    return Ok(());
                }
                Err(KubericAdapterError::SeedState(SeedStateError::RevisionMismatch)) => {
                    let Some(current) = self.seed_store_load(generation, deadline).await? else {
                        return Ok(());
                    };
                    record = current;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn compact_legacy_seed_record(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        mut record: SeedDispatchRecord,
        database: &DatabaseIdentity,
    ) -> Result<(), KubericAdapterError> {
        loop {
            if record.phase() != SeedDispatchPhase::LegacyAmbiguous {
                return Err(KubericAdapterError::SeedState(
                    SeedStateError::InvalidTransition,
                ));
            }
            if record.group_database_id() == Some(database.group_database_id.as_str()) {
                return Ok(());
            }
            record
                .associate_legacy_group_database(database.group_database_id.as_str())
                .map_err(KubericAdapterError::SeedState)?;
            let revision = record.revision();
            match self
                .seed_store_replace(generation, deadline, revision, record)
                .await
            {
                Ok(_) => return Ok(()),
                Err(KubericAdapterError::SeedState(SeedStateError::RevisionMismatch)) => {
                    let current = self.seed_store_load(generation, deadline).await?.ok_or(
                        KubericAdapterError::NativeConvergenceUnsafe(
                            "legacy seed dispatch evidence disappeared during compaction",
                        ),
                    )?;
                    record = current;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn persist_seed_dispatch_evidence(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        context: &SeedBuildContext,
        evidence: SeedDispatchEvidence,
    ) -> Result<SeedDispatchRecord, KubericAdapterError> {
        loop {
            let Some(mut record) = self.seed_store_load(generation, deadline).await? else {
                if matches!(
                    evidence,
                    SeedDispatchEvidence::Accepted | SeedDispatchEvidence::Terminal
                ) {
                    let mut accepted = context.record.clone();
                    accepted
                        .mark_accepted()
                        .map_err(KubericAdapterError::SeedState)?;
                    return Ok(accepted);
                }
                return Err(KubericAdapterError::SeedState(SeedStateError::Missing));
            };
            self.validate_seed_record(&record, &context.record)?;
            let previous_phase = record.phase();
            match evidence {
                SeedDispatchEvidence::Accepted | SeedDispatchEvidence::Terminal => {
                    if record.phase() != SeedDispatchPhase::Accepted {
                        record
                            .mark_accepted()
                            .map_err(KubericAdapterError::SeedState)?;
                    }
                }
                SeedDispatchEvidence::Submitting | SeedDispatchEvidence::Ambiguous => {
                    if record.phase() == SeedDispatchPhase::Submitting {
                        record
                            .mark_ambiguous()
                            .map_err(KubericAdapterError::SeedState)?;
                    } else if record.phase() != SeedDispatchPhase::Ambiguous {
                        return Err(KubericAdapterError::SeedState(
                            SeedStateError::InvalidTransition,
                        ));
                    }
                }
                SeedDispatchEvidence::Absent | SeedDispatchEvidence::FailedRetryable => {
                    if matches!(
                        record.phase(),
                        SeedDispatchPhase::Submitting | SeedDispatchPhase::Ambiguous
                    ) {
                        record
                            .mark_failed_retryable()
                            .map_err(KubericAdapterError::SeedState)?;
                    } else if !matches!(
                        record.phase(),
                        SeedDispatchPhase::Intent | SeedDispatchPhase::FailedRetryable
                    ) {
                        return Err(KubericAdapterError::NativeConvergenceConflict(
                            "accepted seed dispatch SQL evidence",
                        ));
                    }
                }
                SeedDispatchEvidence::Conflict => {
                    return Err(KubericAdapterError::NativeConvergenceConflict(
                        "seed dispatch SQL metadata",
                    ));
                }
            }
            if record.phase() == previous_phase {
                return Ok(record);
            }
            let revision = record.revision();
            match self
                .seed_store_replace(generation, deadline, revision, record)
                .await
            {
                Ok(record) => return Ok(record),
                Err(KubericAdapterError::SeedState(SeedStateError::RevisionMismatch)) => {}
                Err(error) => return Err(error),
            }
        }
    }

    async fn reconcile_seed_dispatch_evidence(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, KubericAdapterError> {
        let executor = &self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?
            .executor;
        let evidence = tokio::time::timeout(
            bootstrap_remaining(deadline)?,
            executor.reconcile_seed_dispatch(request),
        )
        .await
        .map_err(|_| {
            KubericAdapterError::NativeMutationAmbiguous("add database to availability group")
        })?
        .map_err(|error| KubericAdapterError::NativeMutationFailed {
            effect: "reconcile add database submission",
            kind: error.kind,
            server_code: error.server_code,
        })?;
        drop(self.finish_operation(generation)?);
        Ok(evidence)
    }

    async fn arbitrate_seed_dispatch(
        &self,
        generation: u64,
        deadline: tokio::time::Instant,
        context: &SeedBuildContext,
    ) -> Result<SeedDispatchAction, KubericAdapterError> {
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
            return Ok(SeedDispatchAction::ObserveAmbiguous);
        }
        let existing = self.seed_store_load(generation, deadline).await?;
        let mut record = match existing {
            Some(record) => {
                self.validate_seed_record(&record, &context.record)?;
                match record.phase() {
                    SeedDispatchPhase::Intent if record.same_intent_owner(&context.record) => {
                        record
                    }
                    SeedDispatchPhase::Intent | SeedDispatchPhase::FailedRetryable
                        if !record.same_intent_owner(&context.record) =>
                    {
                        self.seed_store_replace_intent(
                            generation,
                            deadline,
                            record.revision(),
                            context.record.clone(),
                        )
                        .await?
                    }
                    SeedDispatchPhase::Intent
                    | SeedDispatchPhase::Submitting
                    | SeedDispatchPhase::Accepted
                    | SeedDispatchPhase::Ambiguous
                    | SeedDispatchPhase::FailedRetryable => record,
                    SeedDispatchPhase::LegacyAmbiguous => {
                        return Err(KubericAdapterError::NativeConvergenceUnsafe(
                            "legacy seed dispatch recovery requires exact native association; operator recovery is required",
                        ));
                    }
                }
            }
            None => {
                let request = SeedDispatchRequest::new(
                    context.binding.availability_group.name.clone(),
                    context.binding.availability_group.group_id.clone(),
                    context.database.name.clone(),
                    context.database.database_guid.clone(),
                    context.record.operation_token(),
                    context.record.terminal_token(),
                )
                .map_err(|_| {
                    KubericAdapterError::NativeConvergenceUnsafe("seed dispatch operation token")
                })?;
                match self
                    .reconcile_seed_dispatch_evidence(generation, deadline, &request)
                    .await?
                {
                    SeedDispatchEvidence::Absent => {}
                    SeedDispatchEvidence::Accepted | SeedDispatchEvidence::Terminal => {
                        return Err(KubericAdapterError::NativeConvergenceConflict(
                            "terminal seed dispatch source association",
                        ));
                    }
                    SeedDispatchEvidence::Conflict => {
                        return Err(KubericAdapterError::NativeConvergenceConflict(
                            "seed dispatch SQL metadata",
                        ));
                    }
                    SeedDispatchEvidence::Submitting
                    | SeedDispatchEvidence::Ambiguous
                    | SeedDispatchEvidence::FailedRetryable => {
                        return Err(KubericAdapterError::NativeConvergenceUnsafe(
                            "seed dispatch SQL metadata exists without temporary application state",
                        ));
                    }
                }
                self.seed_store_create(generation, deadline, context.record.clone())
                    .await?;
                context.record.clone()
            }
        };
        let request = SeedDispatchRequest::new(
            context.binding.availability_group.name.clone(),
            context.binding.availability_group.group_id.clone(),
            context.database.name.clone(),
            context.database.database_guid.clone(),
            record.operation_token(),
            context.record.terminal_token(),
        )
        .map_err(|_| {
            KubericAdapterError::NativeConvergenceUnsafe("seed dispatch operation token")
        })?;
        if record.phase() == SeedDispatchPhase::Submitting
            && bootstrap
                .seed
                .submission_active
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(SeedDispatchAction::ObserveAmbiguous);
        }
        let evidence = self
            .reconcile_seed_dispatch_evidence(generation, deadline, &request)
            .await?;
        record = self
            .persist_seed_dispatch_evidence(generation, deadline, context, evidence)
            .await?;
        match record.phase() {
            SeedDispatchPhase::Accepted | SeedDispatchPhase::Ambiguous => {
                return Ok(SeedDispatchAction::ObserveAmbiguous);
            }
            SeedDispatchPhase::Submitting => {
                return Err(KubericAdapterError::SeedState(
                    SeedStateError::InvalidTransition,
                ));
            }
            SeedDispatchPhase::Intent | SeedDispatchPhase::FailedRetryable => {}
            SeedDispatchPhase::LegacyAmbiguous => {
                return Err(KubericAdapterError::NativeConvergenceUnsafe(
                    "legacy seed dispatch recovery requires exact native association; operator recovery is required",
                ));
            }
        }
        #[cfg(feature = "kuberic-testing")]
        bootstrap.seed.pause_after_intent_if_requested().await?;
        drop(self.finish_operation(generation)?);
        record
            .mark_submitting()
            .map_err(KubericAdapterError::SeedState)?;
        let record = self
            .seed_store_replace(generation, deadline, record.revision(), record)
            .await?;
        #[cfg(feature = "kuberic-testing")]
        bootstrap
            .seed
            .pause_before_submission_if_requested()
            .await?;
        drop(self.finish_operation(generation)?);
        let executor = bootstrap.executor.clone();
        let request = SeedDispatchRequest::new(
            context.binding.availability_group.name.clone(),
            context.binding.availability_group.group_id.clone(),
            context.database.name.clone(),
            context.database.database_guid.clone(),
            record.operation_token(),
            context.record.terminal_token(),
        )
        .map_err(|_| {
            KubericAdapterError::NativeConvergenceUnsafe("seed dispatch operation token")
        })?;
        let submitted_request = request.clone();
        bootstrap
            .seed
            .submission_active
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let seed = bootstrap.seed.clone();
        Ok(SeedDispatchAction::Submitted(SeedDispatchAttempt {
            request,
            task: tokio::spawn(async move {
                let _active = ActiveSeedSubmission(seed);
                executor.submit_seed_dispatch(&submitted_request).await
            }),
        }))
    }

    async fn converge_seed_build(
        &self,
        generation: u64,
        replica: &ReplicaInformation,
    ) -> Result<(), KubericAdapterError> {
        let context = self.seed_build_context(replica)?;
        #[cfg(feature = "kuberic-testing")]
        let bootstrap = self
            .bootstrap
            .as_ref()
            .ok_or(KubericAdapterError::TopologyNotAdmitted)?;
        let deadline = tokio::time::Instant::now() + self.source.observer_config().sample_timeout();
        self.require_availability_group_ownership(
            generation,
            deadline,
            &context.binding.configuration.configuration_id.to_string(),
            Some(&context.database),
            &context.binding.availability_group,
        )
        .await?;
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
            let record = self.seed_store_load(generation, deadline).await?;
            if let Some(record) = record.as_ref() {
                self.validate_seed_record(record, &context.record)?;
            }
            let association = source_association(&group, &context.database)?;
            match association {
                SourceAssociation::Exact(database) => {
                    if let Some(legacy) = record
                        .as_ref()
                        .filter(|record| record.phase() == SeedDispatchPhase::LegacyAmbiguous)
                    {
                        self.compact_legacy_seed_record(
                            generation,
                            deadline,
                            legacy.clone(),
                            &database,
                        )
                        .await?;
                    } else {
                        let operation_token = record.as_ref().map_or(
                            context.record.operation_token(),
                            SeedDispatchRecord::operation_token,
                        );
                        let request = SeedDispatchRequest::new(
                            context.binding.availability_group.name.clone(),
                            context.binding.availability_group.group_id.clone(),
                            context.database.name.clone(),
                            context.database.database_guid.clone(),
                            operation_token,
                            context.record.terminal_token(),
                        )
                        .map_err(|_| {
                            KubericAdapterError::NativeConvergenceUnsafe(
                                "seed dispatch operation token",
                            )
                        })?;
                        match self
                            .reconcile_seed_dispatch_evidence(generation, deadline, &request)
                            .await?
                        {
                            SeedDispatchEvidence::Terminal => {}
                            SeedDispatchEvidence::Conflict => {
                                return Err(KubericAdapterError::NativeConvergenceConflict(
                                    "terminal seed dispatch SQL metadata",
                                ));
                            }
                            SeedDispatchEvidence::Absent
                            | SeedDispatchEvidence::Submitting
                            | SeedDispatchEvidence::Accepted
                            | SeedDispatchEvidence::Ambiguous
                            | SeedDispatchEvidence::FailedRetryable => {
                                return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                    "terminal seed dispatch marker is not proven",
                                ));
                            }
                        }
                        #[cfg(feature = "kuberic-testing")]
                        if record.is_some() {
                            bootstrap
                                .seed
                                .pause_before_compaction_if_requested()
                                .await?;
                            drop(self.finish_operation(generation)?);
                        }
                        if let Some(mut record) = record {
                            if record.phase() != SeedDispatchPhase::Accepted {
                                record
                                    .mark_accepted()
                                    .map_err(KubericAdapterError::SeedState)?;
                                record = self
                                    .seed_store_replace(
                                        generation,
                                        deadline,
                                        record.revision(),
                                        record,
                                    )
                                    .await?;
                            }
                            self.compact_seed_record(generation, deadline, record, &database)
                                .await?;
                        }
                    }
                    match classify_source_seed_completion(
                        &group,
                        &database,
                        &context.database,
                        &context.target.native_replica_id,
                    ) {
                        NativeConvergenceState::Complete => {
                            match self
                                .collect_target_host_evidence(
                                    generation, deadline, &context, &database,
                                )
                                .await?
                            {
                                NativeConvergenceState::Complete => {}
                                NativeConvergenceState::Conflicting => {
                                    return Err(KubericAdapterError::NativeConvergenceConflict(
                                        "target-host seed evidence",
                                    ));
                                }
                                NativeConvergenceState::Unsafe => {
                                    return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                        "target-host seed evidence",
                                    ));
                                }
                                NativeConvergenceState::Absent
                                | NativeConvergenceState::Incomplete => {
                                    self.wait_for_bootstrap_retry(generation, deadline).await?;
                                    continue;
                                }
                            }
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
                SourceAssociation::Incomplete => {
                    if record
                        .as_ref()
                        .is_some_and(|record| record.phase() == SeedDispatchPhase::LegacyAmbiguous)
                    {
                        return Err(KubericAdapterError::NativeConvergenceUnsafe(
                            "legacy seed dispatch recovery requires exact native association; operator recovery is required",
                        ));
                    }
                    if record
                        .as_ref()
                        .is_none_or(|record| record.phase() == SeedDispatchPhase::Accepted)
                    {
                        let operation_token = record.as_ref().map_or(
                            context.record.operation_token(),
                            SeedDispatchRecord::operation_token,
                        );
                        let request = SeedDispatchRequest::new(
                            context.binding.availability_group.name.clone(),
                            context.binding.availability_group.group_id.clone(),
                            context.database.name.clone(),
                            context.database.database_guid.clone(),
                            operation_token,
                            context.record.terminal_token(),
                        )
                        .map_err(|_| {
                            KubericAdapterError::NativeConvergenceUnsafe(
                                "seed dispatch operation token",
                            )
                        })?;
                        match self
                            .reconcile_seed_dispatch_evidence(generation, deadline, &request)
                            .await?
                        {
                            SeedDispatchEvidence::Accepted | SeedDispatchEvidence::Terminal => {
                                return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                    "terminal seed dispatch source association is incomplete",
                                ));
                            }
                            SeedDispatchEvidence::Conflict => {
                                return Err(KubericAdapterError::NativeConvergenceConflict(
                                    "terminal seed dispatch SQL metadata",
                                ));
                            }
                            SeedDispatchEvidence::Absent if record.is_none() => {}
                            SeedDispatchEvidence::Absent
                            | SeedDispatchEvidence::Submitting
                            | SeedDispatchEvidence::Ambiguous
                            | SeedDispatchEvidence::FailedRetryable => {
                                return Err(KubericAdapterError::NativeConvergenceUnsafe(
                                    "seed dispatch SQL metadata differs from temporary application state",
                                ));
                            }
                        }
                    }
                }
                SourceAssociation::Absent => {
                    if record
                        .as_ref()
                        .is_some_and(|record| record.phase() == SeedDispatchPhase::LegacyAmbiguous)
                    {
                        return Err(KubericAdapterError::NativeConvergenceUnsafe(
                            "legacy seed dispatch recovery requires exact native association; operator recovery is required",
                        ));
                    }
                    let dispatch = match self
                        .arbitrate_seed_dispatch(generation, deadline, &context)
                        .await
                    {
                        Ok(dispatch) => dispatch,
                        Err(error) if is_retryable_bootstrap_observation(&error) => {
                            self.wait_for_bootstrap_retry(generation, deadline).await?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    match dispatch {
                        SeedDispatchAction::ObserveAmbiguous => {}
                        SeedDispatchAction::Submitted(mut dispatch) => {
                            let result = tokio::time::timeout(
                                bootstrap_remaining(deadline)?,
                                &mut dispatch.task,
                            )
                            .await
                            .map_err(|_| {
                                KubericAdapterError::NativeMutationAmbiguous(
                                    "add database to availability group",
                                )
                            })?
                            .map_err(|_| {
                                KubericAdapterError::NativeConvergenceUnsafe(
                                    "seed dispatch task terminated",
                                )
                            })?;
                            drop(self.finish_operation(generation)?);
                            match result {
                                Ok(evidence) => {
                                    let record = self
                                        .persist_seed_dispatch_evidence(
                                            generation, deadline, &context, evidence,
                                        )
                                        .await?;
                                    if record.phase() == SeedDispatchPhase::FailedRetryable {
                                        return Err(KubericAdapterError::NativeMutationFailed {
                                            effect: "add database to availability group",
                                            kind: ObservationFailureKind::Unsupported,
                                            server_code: None,
                                        });
                                    }
                                }
                                Err(error) => {
                                    let evidence = self
                                        .reconcile_seed_dispatch_evidence(
                                            generation,
                                            deadline,
                                            &dispatch.request,
                                        )
                                        .await?;
                                    let record = self
                                        .persist_seed_dispatch_evidence(
                                            generation, deadline, &context, evidence,
                                        )
                                        .await?;
                                    match record.phase() {
                                        SeedDispatchPhase::Accepted => {}
                                        SeedDispatchPhase::FailedRetryable
                                        | SeedDispatchPhase::Intent => {
                                            return Err(
                                                KubericAdapterError::NativeMutationFailed {
                                                    effect: "add database to availability group",
                                                    kind: error.kind,
                                                    server_code: error.server_code,
                                                },
                                            );
                                        }
                                        SeedDispatchPhase::Submitting
                                        | SeedDispatchPhase::Ambiguous => {
                                            return Err(
                                                KubericAdapterError::NativeMutationAmbiguous(
                                                    "add database to availability group",
                                                ),
                                            );
                                        }
                                        SeedDispatchPhase::LegacyAmbiguous => {
                                            return Err(
                                                KubericAdapterError::NativeConvergenceUnsafe(
                                                    "legacy seed dispatch recovery requires exact native association; operator recovery is required",
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    self.wait_for_bootstrap_retry(generation, deadline).await?;
                    continue;
                }
            }
            self.wait_for_bootstrap_retry(generation, deadline).await?;
        }
    }

    async fn validate_and_set_role(&self, role: ReplicaRole) -> KubericResult<()> {
        let generation = self.begin_operation()?;
        if self.expectation.is_some() && self.admitted_topology().is_none() {
            if !matches!(role, ReplicaRole::Primary | ReplicaRole::ActiveSecondary) {
                return Err(KubericAdapterError::NativeRoleMismatch {
                    requested: role,
                    observed: None,
                }
                .into());
            }
            let association = if self.bootstrap.is_none()
                && role == ReplicaRole::ActiveSecondary
                && self.native_progress_binding().is_none()
            {
                self.observe_secondary_association(generation).await?
            } else if let Some(binding) = self.native_progress_binding() {
                let (group, observed_at) =
                    self.observed_group(generation, false, Some(role)).await?;
                Some((binding.topology, group.configuration_sequence, observed_at))
            } else {
                None
            };
            let _lifecycle = self.finish_operation(generation)?;
            #[cfg(feature = "kuberic-testing")]
            self.wait_for_testing_lifecycle_publication()?;
            if let Some((topology, sequence, observed_at)) = association {
                self.ensure_observation_fresh(observed_at)?;
                self.publish_native_progress(topology, sequence)?;
            }
            self.set_role(role);
            return Ok(());
        }
        let (group, observed_at) = self.observed_group(generation, false, Some(role)).await?;
        let _lifecycle = self.finish_operation(generation)?;
        #[cfg(feature = "kuberic-testing")]
        self.wait_for_testing_lifecycle_publication()?;
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

    fn set_epoch(&self, epoch: Epoch) -> Result<(), KubericAdapterError> {
        let mut accepted = self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if accepted.as_ref().is_some_and(|accepted| epoch < *accepted) {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "runtime epoch regressed",
            ));
        }
        *accepted = Some(epoch);
        Ok(())
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
        let mut unbound_observed_at = None;
        let native = if let Some(binding) = &admitted {
            let (group, observed_at) = self.observed_group(generation, false, Some(role)).await?;
            if epoch.data_loss_number != binding.configuration.epoch.data_loss_number
                || epoch.configuration_number < binding.configuration.epoch.configuration_number
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "role-change epoch precedes or changes the admitted history",
                )
                .into());
            }
            Some((
                binding.native_topology(),
                group.configuration_sequence,
                observed_at,
            ))
        } else if self.expectation.is_some() {
            if !matches!(role, ReplicaRole::Primary | ReplicaRole::ActiveSecondary) {
                return Err(KubericAdapterError::NativeRoleMismatch {
                    requested: role,
                    observed: None,
                }
                .into());
            }
            if let Some(binding) = self.native_progress_binding() {
                let (group, observed_at) =
                    self.observed_group(generation, false, Some(role)).await?;
                Some((binding.topology, group.configuration_sequence, observed_at))
            } else if self.bootstrap.is_none() && role == ReplicaRole::ActiveSecondary {
                self.observe_secondary_association(generation).await?
            } else {
                None
            }
        } else {
            let (group, observed_at) = self.observed_group(generation, false, Some(role)).await?;
            validate_native_role(role, group.local_replica.role)?;
            unbound_observed_at = Some(observed_at);
            None
        };
        let _lifecycle = self.finish_operation(generation)?;
        #[cfg(feature = "kuberic-testing")]
        self.wait_for_testing_lifecycle_publication()?;
        if let Some((topology, sequence, observed_at)) = native {
            self.ensure_observation_fresh(observed_at)?;
            self.publish_native_progress(topology, sequence)?;
        }
        if let Some(observed_at) = unbound_observed_at {
            self.ensure_observation_fresh(observed_at)?;
        }
        self.set_epoch(epoch)?;
        self.set_role(role);
        Ok(())
    }

    async fn update_epoch(&self, epoch: Epoch) -> KubericResult<()> {
        let generation = self.begin_operation()?;
        let admitted = self.admitted_topology();
        let native = if let Some(binding) = &admitted {
            let (group, observed_at) = self.observed_group(generation, false, None).await?;
            if epoch.data_loss_number != binding.configuration.epoch.data_loss_number
                || epoch.configuration_number < binding.configuration.epoch.configuration_number
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "updated epoch precedes or changes the admitted history",
                )
                .into());
            }
            Some((
                binding.native_topology(),
                group.configuration_sequence,
                observed_at,
            ))
        } else if let Some(binding) = self.native_progress_binding() {
            let (group, observed_at) = self.observed_group(generation, false, None).await?;
            Some((binding.topology, group.configuration_sequence, observed_at))
        } else {
            None
        };
        let _lifecycle = self.finish_operation(generation)?;
        #[cfg(feature = "kuberic-testing")]
        self.wait_for_testing_lifecycle_publication()?;
        if let Some((topology, sequence, observed_at)) = native {
            self.ensure_observation_fresh(observed_at)?;
            self.publish_native_progress(topology, sequence)?;
        }
        self.set_epoch(epoch)?;
        Ok(())
    }

    async fn close(&self) -> KubericResult<()> {
        let mut lifecycle = self
            .lifecycle
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lifecycle.state = CLOSED;
        lifecycle.generation = lifecycle.generation.wrapping_add(1);
        self.lifecycle_cancel.cancel();
        if let Some(bootstrap) = &self.bootstrap {
            bootstrap.seed.cancel_store_work();
        }
        Ok(())
    }

    fn abort(&self) {
        let mut lifecycle = self
            .lifecycle
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lifecycle.state = ABORTED;
        lifecycle.generation = lifecycle.generation.wrapping_add(1);
        self.lifecycle_cancel.cancel();
        if let Some(bootstrap) = &self.bootstrap {
            bootstrap.seed.cancel_store_work();
        }
    }

    async fn current_progress(&self) -> KubericResult<i64> {
        let generation = self.begin_operation()?;
        let admitted = self.admitted_topology();
        let native = self.native_progress_binding();
        if self.expectation.is_some() && admitted.is_none() && native.is_none() {
            if self.bootstrap.is_none()
                && self.published_role() == Some(ReplicaRole::ActiveSecondary)
                && let Some((topology, sequence, observed_at)) =
                    self.observe_secondary_association(generation).await?
            {
                let _lifecycle = self.finish_operation(generation)?;
                #[cfg(feature = "kuberic-testing")]
                self.wait_for_testing_lifecycle_publication()?;
                self.ensure_observation_fresh(observed_at)?;
                self.publish_native_progress(topology, sequence)?;
                return Ok(sequence.value());
            }
            let _lifecycle = self.finish_operation(generation)?;
            #[cfg(feature = "kuberic-testing")]
            self.wait_for_testing_lifecycle_publication()?;
            return Ok(0);
        }
        let (group, observed_at) = self.observed_group(generation, true, None).await?;
        let value = group.configuration_sequence.value();
        let _lifecycle = self.finish_operation(generation)?;
        #[cfg(feature = "kuberic-testing")]
        self.wait_for_testing_lifecycle_publication()?;
        self.ensure_observation_fresh(observed_at)?;
        if let Some(binding) = admitted {
            self.publish_native_progress(binding.native_topology(), group.configuration_sequence)?;
        } else if let Some(binding) = native {
            self.publish_native_progress(binding.topology, group.configuration_sequence)?;
        }
        Ok(value)
    }

    async fn catch_up_capability(&self) -> KubericResult<i64> {
        let generation = self.begin_operation()?;
        let admitted = self.admitted_topology();
        let native = self.native_progress_binding();
        if admitted.is_none() && native.is_none() {
            if self.expectation.is_some() {
                if self.bootstrap.is_none()
                    && self.published_role() == Some(ReplicaRole::ActiveSecondary)
                    && let Some((topology, sequence, observed_at)) =
                        self.observe_secondary_association(generation).await?
                {
                    let _lifecycle = self.finish_operation(generation)?;
                    #[cfg(feature = "kuberic-testing")]
                    self.wait_for_testing_lifecycle_publication()?;
                    self.ensure_observation_fresh(observed_at)?;
                    self.publish_native_progress(topology, sequence)?;
                    return Ok(sequence.value());
                }
                let _lifecycle = self.finish_operation(generation)?;
                #[cfg(feature = "kuberic-testing")]
                self.wait_for_testing_lifecycle_publication()?;
                return Ok(INVALID_LSN);
            }
            return self.unsupported(ObserveOnlyOperation::CatchUpCapability);
        }

        {
            let _lifecycle = self.finish_operation(generation)?;
            let role_matches = if let Some(binding) = &admitted {
                self.published_role()
                    .is_some_and(|role| published_role_matches(binding, role))
            } else {
                self.published_role().is_some_and(|role| {
                    native
                        .as_ref()
                        .is_some_and(|binding| role == binding.topology.local_stable_role)
                })
            };
            if !role_matches {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "capability requires the bound stable local role to be published",
                )
                .into());
            }
        }
        let (group, observed_at) = self.observed_group(generation, true, None).await?;
        let value = group.configuration_sequence.value();
        self.ensure_observation_fresh(observed_at)?;
        let _lifecycle = self.finish_operation(generation)?;
        #[cfg(feature = "kuberic-testing")]
        self.wait_for_testing_lifecycle_publication()?;
        let role_matches = if let Some(binding) = &admitted {
            self.published_role()
                .is_some_and(|role| published_role_matches(binding, role))
        } else {
            self.published_role().is_some_and(|role| {
                native
                    .as_ref()
                    .is_some_and(|binding| role == binding.topology.local_stable_role)
            })
        };
        if !role_matches {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "capability requires the bound stable local role to be published",
            )
            .into());
        }
        self.ensure_observation_fresh(observed_at)?;
        if let Some(binding) = admitted {
            self.publish_native_progress(binding.native_topology(), group.configuration_sequence)?;
        } else if let Some(binding) = native {
            self.publish_native_progress(binding.topology, group.configuration_sequence)?;
        }
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
        let process_session_id = self.process_session_id(&current)?;
        let bootstrap_scope = if self.bootstrap.is_some() {
            bootstrap_convergence_scope(
                runtime_identity,
                &process_session_id,
                expectation,
                &self.replication_address,
                &current,
            )?
        } else {
            BootstrapConvergenceScope::FullTopology
        };
        let generation = self.begin_operation()?;
        if bootstrap_scope == BootstrapConvergenceScope::Deferred {
            drop(self.finish_operation(generation)?);
            return Ok(());
        }
        if self.bootstrap.is_some()
            && let Some(binding) = self.admitted_topology()
        {
            let accepted_replay = if build_enriched_configuration(&binding, &current)? {
                true
            } else {
                let admitted = self
                    .admitted_configuration
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if admitted.as_ref().is_some_and(configuration_is_complete) {
                    validate_bootstrap_configuration_replay(&binding, &current)?;
                    true
                } else {
                    false
                }
            };
            if !accepted_replay {
                drop(binding);
            } else {
                let ownership_deadline =
                    tokio::time::Instant::now() + self.source.observer_config().sample_timeout();
                self.require_availability_group_ownership(
                    generation,
                    ownership_deadline,
                    &binding.configuration.configuration_id.to_string(),
                    None,
                    &binding.availability_group,
                )
                .await?;
                let (_, observed_at) = self.observed_group(generation, false, None).await?;
                drop(self.finish_operation(generation)?);
                self.ensure_observation_fresh(observed_at)?;
                return Ok(());
            }
        }

        let (instance, group, observed_at, bootstrap_database, bootstrap_deadline) =
            if self.bootstrap.is_some() {
                match self
                    .converge_bootstrap_configuration(generation, &current, bootstrap_scope)
                    .await?
                {
                    BootstrapConvergenceResult::SourceFoundation => return Ok(()),
                    BootstrapConvergenceResult::FullTopology(converged) => {
                        let BootstrapConvergedTopology {
                            instance,
                            group,
                            observed_at,
                            database,
                            deadline,
                        } = *converged;
                        (instance, group, observed_at, database, deadline)
                    }
                }
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
        let compatibility = CurrentTopologyFacts::translate(
            runtime_identity,
            &process_session_id,
            expectation,
            &self.replication_address,
            &current,
        )?;
        let candidate = admit_topology(
            runtime_identity,
            expectation,
            &instance,
            &group,
            &compatibility,
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
        let native_topology = candidate.native_topology();
        let native_sequence = candidate.native_configuration_sequence;
        let mut native_progress = self
            .native_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = native_progress.as_ref() {
            if previous.topology != native_topology {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "native topology differs from the bound association",
                )
                .into());
            }
            if native_sequence < previous.latest_sequence {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "configuration sequence regressed",
                )
                .into());
            }
        }
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
        *native_progress = Some(NativeProgressBinding {
            topology: native_topology,
            latest_sequence: native_sequence,
        });
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

fn validate_target_host_evidence(
    expected: &TargetHostEvidenceRequest,
    report: &TargetHostEvidenceReport,
    now_unix_millis: u64,
    max_age_millis: u64,
) -> Result<NativeConvergenceState, KubericAdapterError> {
    if report.request() != expected {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target-host evidence authority",
        ));
    }
    let snapshot = report.snapshot();
    if snapshot.observed_at_unix_millis < expected.requested_at_unix_millis {
        return Err(KubericAdapterError::ObservationStale);
    }
    let age = now_unix_millis
        .checked_sub(snapshot.observed_at_unix_millis)
        .ok_or(KubericAdapterError::ObservationFromFuture)?;
    if age > max_age_millis {
        return Err(KubericAdapterError::ObservationStale);
    }
    if snapshot.instance.sqlserver_start_time != report.frozen_sqlserver_start_time().as_str() {
        return Err(KubericAdapterError::NativeConvergenceUnsafe(
            "target SQL Server start identity",
        ));
    }
    if snapshot.instance.server_name != expected.target_server_name
        || snapshot.instance.property_server_name != expected.target_server_name
    {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target SQL Server identity",
        ));
    }
    let group = match &snapshot.availability_group {
        Observation::Present {
            value,
            observed_at_unix_millis,
        } if *observed_at_unix_millis == snapshot.observed_at_unix_millis => value,
        Observation::Present { .. } => {
            return Err(KubericAdapterError::ObservationInconsistent(
                "target-host availability-group and instance timestamps differ",
            ));
        }
        Observation::Absent { .. } => return Ok(NativeConvergenceState::Incomplete),
        Observation::Failed(failure) => {
            return Err(KubericAdapterError::ObservationUnavailable(failure.kind));
        }
    };
    if group.identity != expected.availability_group {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target availability-group identity",
        ));
    }
    if group.local_replica.identity.native_replica_id() != Some(&expected.target_native_replica_id)
        || group.local_replica.role != Some(NativeRole::Secondary)
    {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target native replica identity or role",
        ));
    }
    if !group.local_replica.state_available {
        return Ok(NativeConvergenceState::Incomplete);
    }
    let Some(replica) = group
        .replicas
        .iter()
        .find(|replica| replica.replica_id == expected.target_native_replica_id)
    else {
        return Ok(NativeConvergenceState::Incomplete);
    };
    if replica.server_name != expected.target_server_name
        || replica.endpoint_url.as_deref() != Some(expected.target_endpoint_url.as_str())
        || replica.availability_mode != "SYNCHRONOUS_COMMIT"
        || replica.failover_mode != "EXTERNAL"
        || replica.seeding_mode != "AUTOMATIC"
    {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target native replica profile",
        ));
    }
    let Some(replica_state) = replica.state.as_ref() else {
        return Ok(NativeConvergenceState::Incomplete);
    };
    if replica_state.provenance != NativeProvenance::Local
        || replica_state.role != Some(NativeRole::Secondary)
        || replica_state.operational_state.as_deref() != Some("ONLINE")
        || replica_state.connected_state.as_deref() != Some("CONNECTED")
        || replica_state.synchronization_health.as_deref() != Some("HEALTHY")
        || replica_state
            .last_connect_error_number
            .is_some_and(|value| value != 0)
    {
        return Ok(NativeConvergenceState::Incomplete);
    }
    let Some(database) = group
        .databases
        .iter()
        .find(|database| database.identity.name == expected.database.name)
    else {
        return Ok(NativeConvergenceState::Incomplete);
    };
    if group.databases.len() != 1
        || database.identity.group_database_id != expected.group_database_id
    {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target group-database identity",
        ));
    }
    let Some(local) = database.local.as_ref() else {
        return Ok(NativeConvergenceState::Incomplete);
    };
    if local.replica_id != expected.target_native_replica_id {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target local database replica identity",
        ));
    }
    let Some(recovery) = local.recovery.as_ref() else {
        return Ok(NativeConvergenceState::Incomplete);
    };
    if recovery.family_guid.as_ref() != Some(&expected.database.family_guid)
        || recovery.recovery_fork_guid.as_ref() != Some(&expected.database.recovery_fork_guid)
    {
        return Err(KubericAdapterError::NativeConvergenceConflict(
            "target database family or recovery-fork lineage",
        ));
    }
    if recovery.database_guid.is_none()
        || local.state.as_deref() != Some("ONLINE")
        || local.recovery_model.as_deref() != Some("FULL")
    {
        return Ok(NativeConvergenceState::Incomplete);
    }
    let Some(target) = database
        .replicas
        .iter()
        .find(|state| state.replica_id == expected.target_native_replica_id)
    else {
        return Ok(NativeConvergenceState::Incomplete);
    };
    match &target.lineage {
        RecoveryLineageObservation::Local { value }
            if value.database != database.identity
                || value.recovery_fork_id != expected.database.recovery_fork_guid =>
        {
            return Err(KubericAdapterError::NativeConvergenceConflict(
                "target local database recovery lineage",
            ));
        }
        RecoveryLineageObservation::Local { .. } => {}
        RecoveryLineageObservation::LocalUnavailable
        | RecoveryLineageObservation::RemoteUnavailable => {
            return Ok(NativeConvergenceState::Incomplete);
        }
    }
    if target.provenance != NativeProvenance::Local
        || target.is_primary_replica != Some(false)
        || target.synchronization_state.as_deref() != Some("SYNCHRONIZED")
        || target.synchronization_health.as_deref() != Some("HEALTHY")
        || target.database_state.as_deref() != Some("ONLINE")
        || target.is_suspended != Some(false)
        || target.suspend_reason.is_some()
    {
        Ok(NativeConvergenceState::Incomplete)
    } else {
        Ok(NativeConvergenceState::Complete)
    }
}

fn classify_source_seed_completion(
    group: &AvailabilityGroupSnapshot,
    database: &DatabaseIdentity,
    source: &StandaloneDatabaseIdentity,
    target_replica_id: &Guid,
) -> NativeConvergenceState {
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
    // A failed DMV process is not terminal because SQL Server may retry it.
    // Completion requires a successful process plus the current native target
    // postcondition, so historical failures cannot create false success.
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
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    current: &CurrentTopologyFacts,
    options: TopologyAdmissionOptions<'_>,
) -> Result<HealthyTopologyBinding, KubericAdapterError> {
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
    for configuration_member in &current.members {
        let expected = expectation
            .member(configuration_member.identity.replica_id)
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "current member has no SQL topology association",
            ))?;
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
            process_session_id: configuration_member.process_session_id.clone(),
            replication_address: configuration_member.replication_address.clone(),
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
    current.validate_binding(&candidate)?;
    validate_bound_evidence(&candidate, options.observer_config, instance, group, None)?;
    Ok(candidate)
}

fn associate_native_topology(
    runtime_identity: &KubericReplicaIdentity,
    expectation: &SqlServerTopologyExpectation,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    stable_role: ReplicaRole,
) -> Result<NativeTopologyBinding, KubericAdapterError> {
    if runtime_identity.replica_id != expectation.local_replica_id()
        || instance.server_name != *expectation.local_member().server_name()
        || instance.property_server_name != *expectation.local_member().server_name()
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local runtime and SQL topology identities differ",
        ));
    }
    if group.databases.len() != usize::from(SUPPORTED_DATABASE_COUNT) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "exactly one managed database is required",
        ));
    }
    let database = &group.databases[0];
    let recovery_fork_id = database
        .local
        .as_ref()
        .and_then(|local| local.recovery.as_ref())
        .and_then(|recovery| recovery.recovery_fork_guid.clone())
        .ok_or(KubericAdapterError::TopologyBindingMismatch(
            "local database recovery lineage is unavailable",
        ))?;
    let mut members = Vec::with_capacity(usize::from(SUPPORTED_REPLICA_COUNT));
    for expected in expectation.members() {
        let matching = group
            .replicas
            .iter()
            .filter(|replica| {
                replica.server_name == *expected.server_name()
                    && replica.endpoint_url.as_deref() == Some(expected.endpoint_url())
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "SQL topology association does not identify one native replica",
            ));
        }
        members.push(NativeTopologyMemberBinding {
            native_replica_id: matching[0].replica_id.clone(),
            sql_endpoint: SqlServerMemberEndpoint::new(
                expected.server_name().clone(),
                expected.endpoint_url(),
            )?,
        });
    }
    if members.iter().enumerate().any(|(index, member)| {
        members[..index]
            .iter()
            .any(|other| other.native_replica_id == member.native_replica_id)
    }) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "native replica identities must be unique",
        ));
    }
    let local_native_id = group.local_replica.identity.native_replica_id().ok_or(
        KubericAdapterError::TopologyBindingMismatch(
            "local native replica identity is unavailable",
        ),
    )?;
    let local_member = members
        .iter()
        .find(|member| {
            member.sql_endpoint.server_name() == expectation.local_member().server_name()
        })
        .expect("topology expectation includes the local member");
    if local_member.native_replica_id != *local_native_id {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local SQL association differs from the observed native replica",
        ));
    }
    Ok(NativeTopologyBinding {
        availability_group: group.identity.clone(),
        database_lineage: Some(DatabaseLineage {
            database: database.identity.clone(),
            recovery_fork_id,
        }),
        bootstrap_database_name: None,
        bootstrap_database: None,
        local_sql_replica_identity: group.local_replica.identity.clone(),
        local_sql_server_start_incarnation: SqlServerStartIncarnation::new(
            instance.sqlserver_start_time.clone(),
        )?,
        local_stable_role: stable_role,
        members,
    })
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

fn bootstrap_source_foundation_ready(group: &AvailabilityGroupSnapshot) -> bool {
    if !group.local_replica.state_available
        || group.local_replica.role.as_ref() != Some(&NativeRole::Primary)
    {
        return false;
    }
    let Some(local_id) = group.local_replica.identity.native_replica_id() else {
        return false;
    };
    group
        .replicas
        .iter()
        .find(|replica| &replica.replica_id == local_id)
        .and_then(|replica| replica.state.as_ref())
        .is_some_and(|state| {
            state.provenance == NativeProvenance::Local
                && state.role == Some(NativeRole::Primary)
                && state.operational_state.as_deref() == Some("ONLINE")
                && state.connected_state.as_deref() == Some("CONNECTED")
                && state
                    .last_connect_error_number
                    .is_none_or(|number| number == 0)
        })
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

fn validate_bound_evidence(
    binding: &HealthyTopologyBinding,
    config: &ObserverConfig,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    requested_role: Option<ReplicaRole>,
) -> Result<(), KubericAdapterError> {
    validate_native_bound_evidence(
        &binding.native_topology(),
        config,
        instance,
        group,
        requested_role,
    )
}

fn validate_native_bound_evidence(
    binding: &NativeTopologyBinding,
    config: &ObserverConfig,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    requested_role: Option<ReplicaRole>,
) -> Result<(), KubericAdapterError> {
    let target = config.target();
    if instance.sqlserver_start_time != binding.local_sql_server_start_incarnation.as_str() {
        return Err(KubericAdapterError::NativeConvergenceUnsafe(
            "SQL Server process incarnation",
        ));
    }
    if target.replica.logical_id() != binding.local_sql_replica_identity.logical_id()
        || target.replica.incarnation() != binding.local_sql_replica_identity.incarnation()
        || instance.server_name != target.expected_server_name
        || instance.property_server_name != target.expected_server_name
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
    if requested_role.is_some_and(|role| role != binding.local_stable_role) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "requested role differs from the frozen stable role",
        ));
    }
    let expected_native_role = native_role_for(binding.local_stable_role);
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
                replica.replica_id == member.native_replica_id
                    && replica.server_name == *member.sql_endpoint.server_name()
                    && replica.endpoint_url.as_deref() == Some(member.sql_endpoint.endpoint_url())
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
        .find(|replica| &replica.replica_id == local_native_id)
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
    validate_bound_database_state(
        matching_states[0],
        database_lineage,
        binding.local_stable_role,
    )
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
