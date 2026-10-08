use std::fmt;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};

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

use crate::executor::SqlExecutor;
use crate::instance::{SqlServerInstanceManager, unix_millis};
use crate::observation::{
    AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, InstanceSnapshot, NativeProvenance,
    RecoveryLineageObservation,
};
use crate::runtime_config::ObserverConfig;
use crate::runtime_error::RuntimeError;
use crate::topology_config::SqlServerTopologyExpectation;
use crate::{
    AvailabilityGroupIdentity, DatabaseLineage, Guid, NativeRole, Observation,
    ObservationFailureKind, ReplicaIdentity as SqlReplicaIdentity, SUPPORTED_DATABASE_COUNT,
    SUPPORTED_ENGINE_MAJOR, SUPPORTED_REPLICA_COUNT, SUPPORTED_REQUIRED_SECONDARIES, ServerName,
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
    availability_group: AvailabilityGroupIdentity,
    database_lineage: DatabaseLineage,
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

    pub fn database_lineage(&self) -> &DatabaseLineage {
        &self.database_lineage
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
    open_attempt: tokio::sync::Mutex<()>,
    lifecycle: Mutex<ServiceLifecycle>,
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
        Self::with_optional_topology(config, source, clock, None)
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
        ))
    }

    fn with_optional_topology(
        config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        expectation: Option<Arc<SqlServerTopologyExpectation>>,
    ) -> Self {
        Self {
            config,
            source,
            clock,
            expectation,
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
            SqlServerReplicator::new_with_topology(
                self.config.replication_address.clone(),
                self.source.clone(),
                self.clock.clone(),
                context.identity.clone(),
                expectation.as_ref().clone(),
            )
            .map_err(KubericRuntimeError::from)?
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
    runtime_identity: Option<KubericReplicaIdentity>,
    expectation: Option<Arc<SqlServerTopologyExpectation>>,
    admitted_topology: Mutex<Option<Arc<HealthyTopologyBinding>>>,
    admitted_configuration: Mutex<Option<ReplicaSetConfiguration>>,
    lifecycle: RwLock<ReplicatorLifecycle>,
    role: Mutex<Option<ReplicaRole>>,
    epoch: Mutex<Option<Epoch>>,
}

#[derive(Debug, Clone, Copy)]
struct ReplicatorLifecycle {
    state: u8,
    generation: u64,
}

impl SqlServerReplicator {
    pub fn new(
        replication_address: String,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
    ) -> Self {
        Self::with_parts(replication_address, source, clock, None, None)
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
        ))
    }

    fn with_parts(
        replication_address: String,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        runtime_identity: Option<KubericReplicaIdentity>,
        expectation: Option<Arc<SqlServerTopologyExpectation>>,
    ) -> Self {
        Self {
            replication_address,
            source,
            clock,
            runtime_identity,
            expectation,
            admitted_topology: Mutex::new(None),
            admitted_configuration: Mutex::new(None),
            lifecycle: RwLock::new(ReplicatorLifecycle {
                state: CREATED,
                generation: 0,
            }),
            role: Mutex::new(None),
            epoch: Mutex::new(None),
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
                    if role != binding.local_member().stable_role {
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

    async fn validate_and_set_role(&self, role: ReplicaRole) -> KubericResult<()> {
        let generation = self.begin_operation()?;
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
        let (group, observed_at) = self.observed_group(generation, true, None).await?;
        let value = group.configuration_sequence.value();
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
            if *self
                .role
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                != Some(binding.local_member().stable_role)
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "capability requires the frozen stable local role to be published",
                )
                .into());
            }
        }
        let (group, observed_at) = self.observed_group(generation, true, None).await?;
        let value = group.configuration_sequence.value();
        self.ensure_observation_fresh(observed_at)?;
        let _lifecycle = self.finish_operation(generation)?;
        if *self
            .role
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            != Some(binding.local_member().stable_role)
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
        let generation = self.begin_operation()?;
        let observation = self
            .source
            .observe()
            .await
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
        drop(self.finish_operation(generation)?);
        let (snapshot, observed_at) = present_snapshot(observation)?;
        let (instance, group) = present_group(snapshot, observed_at)?;
        validate_eligible_snapshot(self.source.observer_config(), &instance, &group, false)?;
        let candidate = admit_topology(
            runtime_identity,
            expectation,
            &self.replication_address,
            self.source.observer_config(),
            &instance,
            &group,
            &current,
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
        if let (Some(previous_topology), Some(previous_configuration)) =
            (&*admitted_topology, &*admitted_configuration)
        {
            if (previous_topology.as_ref() != &candidate
                && !topology_enriches(previous_topology, &candidate))
                || (previous_configuration != &current
                    && (configuration_is_complete(previous_configuration)
                        || !configuration_enriches(previous_configuration, &current)))
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "current configuration replay differs from the admitted value",
                )
                .into());
            }
            self.ensure_observation_fresh(observed_at)?;
            *admitted_topology = Some(Arc::new(candidate));
            *admitted_configuration = Some(current);
        } else {
            self.ensure_observation_fresh(observed_at)?;
            *admitted_topology = Some(Arc::new(candidate));
            *admitted_configuration = Some(current);
        }
        Ok(())
    }

    async fn build_replica(&self, _replica: ReplicaInformation) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::BuildReplica)
    }

    async fn remove_replica(&self, _replica_id: ReplicaId) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::RemoveReplica)
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

fn admit_topology(
    runtime_identity: &KubericReplicaIdentity,
    expectation: &SqlServerTopologyExpectation,
    replication_address: &str,
    config: &ObserverConfig,
    instance: &crate::observation::InstanceMetadata,
    group: &AvailabilityGroupSnapshot,
    current: &ReplicaSetConfiguration,
) -> Result<HealthyTopologyBinding, KubericAdapterError> {
    validate_configuration_shape(runtime_identity, expectation, replication_address, current)?;
    if group.configuration_sequence.value() != current.configuration.epoch.configuration_number {
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

    let database = group
        .databases
        .first()
        .ok_or(KubericAdapterError::TopologyBindingMismatch(
            "exactly one managed database is required",
        ))?;
    if group.databases.len() != usize::from(SUPPORTED_DATABASE_COUNT) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "exactly one managed database is required",
        ));
    }
    let recovery_fork_id = database
        .local
        .as_ref()
        .and_then(|local| local.recovery.as_ref())
        .and_then(|recovery| recovery.recovery_fork_guid.clone())
        .ok_or(KubericAdapterError::TopologyBindingMismatch(
            "local database recovery lineage is unavailable",
        ))?;
    let database_lineage = DatabaseLineage {
        database: database.identity.clone(),
        recovery_fork_id,
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
        availability_group: group.identity.clone(),
        database_lineage,
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
    validate_bound_evidence(&candidate, config, instance, group, None)?;
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
        || previous.availability_group != current.availability_group
        || previous.database_lineage != current.database_lineage
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
    if group.configuration_sequence.value() != binding.configuration.epoch.configuration_number {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "configuration sequence differs from the frozen configuration",
        ));
    }
    if target.replica.logical_id() != binding.local_sql_replica_identity.logical_id()
        || target.replica.incarnation() != binding.local_sql_replica_identity.incarnation()
        || instance.server_name != *local.server_name()
        || instance.property_server_name != *local.server_name()
        || instance.sqlserver_start_time != binding.local_sql_server_start_incarnation.as_str()
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
    if requested_role.is_some_and(|role| role != local.stable_role) {
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
    if state
        .last_connect_error_number
        .is_some_and(|number| number != 0)
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "local native replica last-connect evidence differs",
        ));
    }
    if group.databases.len() != usize::from(SUPPORTED_DATABASE_COUNT) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "exactly one managed database is required",
        ));
    }
    let database = &group.databases[0];
    if database.identity != binding.database_lineage.database {
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
            != Some(&binding.database_lineage.recovery_fork_id)
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
        &binding.database_lineage,
        local.stable_role,
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
