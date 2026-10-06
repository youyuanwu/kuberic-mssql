use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_runtime::application::{
    OpenContext, RoleChange, StateProvider, StatefulServiceReplica,
};
use kuberic_runtime::protocol::types::{Epoch, PartitionId, ReplicaId, ReplicaRole, ResourceUid};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, ReplicaSetQuorumMode,
    Replicator, ReplicatorFactory, ReplicatorFactoryContext, ReplicatorInterfaces,
    ReplicatorSettings,
};
use kuberic_runtime::{Result as KubericResult, RuntimeError as KubericRuntimeError};

use crate::executor::SqlExecutor;
use crate::instance::{SqlServerInstanceManager, unix_millis};
use crate::observation::{AvailabilityGroupSnapshot, InstanceSnapshot};
use crate::runtime_config::ObserverConfig;
use crate::runtime_error::RuntimeError;
use crate::{
    NativeRole, Observation, ObservationFailureKind, SUPPORTED_DATABASE_COUNT,
    SUPPORTED_ENGINE_MAJOR, SUPPORTED_REPLICA_COUNT, SUPPORTED_REQUIRED_SECONDARIES,
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

pub struct SqlServerService {
    config: SqlServerServiceConfig,
    source: Arc<dyn SqlServerObservationSource>,
    clock: Arc<dyn ObservationClock>,
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

    pub fn with_observation_source(
        config: SqlServerServiceConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
    ) -> Self {
        Self {
            config,
            source,
            clock,
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
        let replicator = Arc::new(SqlServerReplicator::new(
            self.config.replication_address.clone(),
            self.source.clone(),
            self.clock.clone(),
        ));
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
        _context: ReplicatorFactoryContext,
        state_provider: Option<Arc<dyn StateProvider>>,
        _settings: ReplicatorSettings,
    ) -> KubericResult<ReplicatorInterfaces> {
        if state_provider.is_some() {
            return Err(KubericRuntimeError::Application(
                "SQL Server owns its native replication state".into(),
            ));
        }
        Ok(self.interfaces())
    }
}

pub struct SqlServerReplicator {
    replication_address: String,
    source: Arc<dyn SqlServerObservationSource>,
    clock: Arc<dyn ObservationClock>,
    lifecycle: AtomicU8,
    role: Mutex<Option<ReplicaRole>>,
    epoch: Mutex<Option<Epoch>>,
}

impl SqlServerReplicator {
    pub fn new(
        replication_address: String,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
    ) -> Self {
        Self {
            replication_address,
            source,
            clock,
            lifecycle: AtomicU8::new(CREATED),
            role: Mutex::new(None),
            epoch: Mutex::new(None),
        }
    }

    fn require_open(&self) -> Result<(), KubericAdapterError> {
        match self.lifecycle.load(Ordering::Acquire) {
            OPEN => Ok(()),
            CREATED => Err(KubericAdapterError::NotOpen),
            CLOSED | ABORTED => Err(KubericAdapterError::Closed),
            _ => unreachable!("SQL Server replicator lifecycle is invalid"),
        }
    }

    async fn observed_group(
        &self,
        require_progress_role: bool,
    ) -> Result<AvailabilityGroupSnapshot, KubericAdapterError> {
        self.require_open()?;
        let observation = self
            .source
            .observe()
            .await
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?;
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
        if require_progress_role {
            if let Some(role) = *self
                .role
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                validate_native_role(role, group.local_replica.role.clone())?;
            }
        }
        Ok(group)
    }

    async fn validate_and_set_role(&self, role: ReplicaRole) -> KubericResult<()> {
        let group = self.observed_group(false).await?;
        validate_native_role(role, group.local_replica.role)?;
        *self
            .role
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(role);
        Ok(())
    }

    fn unsupported<T>(&self, operation: ObserveOnlyOperation) -> KubericResult<T> {
        Err(KubericAdapterError::UnsupportedOperation(operation).into())
    }
}

#[async_trait]
impl Replicator for SqlServerReplicator {
    async fn open(&self) -> KubericResult<String> {
        self.lifecycle
            .compare_exchange(CREATED, OPEN, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| KubericRuntimeError::Closed)?;
        Ok(self.replication_address.clone())
    }

    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> KubericResult<()> {
        self.validate_and_set_role(role).await?;
        *self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(epoch);
        Ok(())
    }

    async fn update_epoch(&self, epoch: Epoch) -> KubericResult<()> {
        self.require_open()?;
        *self
            .epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(epoch);
        Ok(())
    }

    async fn close(&self) -> KubericResult<()> {
        self.lifecycle.store(CLOSED, Ordering::Release);
        Ok(())
    }

    fn abort(&self) {
        self.lifecycle.store(ABORTED, Ordering::Release);
    }

    async fn current_progress(&self) -> KubericResult<i64> {
        Ok(self
            .observed_group(true)
            .await?
            .configuration_sequence
            .value())
    }

    async fn catch_up_capability(&self) -> KubericResult<i64> {
        self.unsupported(ObserveOnlyOperation::CatchUpCapability)
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
        _current: ReplicaSetConfiguration,
    ) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::CurrentConfiguration)
    }

    async fn build_replica(&self, _replica: ReplicaInformation) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::BuildReplica)
    }

    async fn remove_replica(&self, _replica_id: ReplicaId) -> KubericResult<()> {
        self.unsupported(ObserveOnlyOperation::RemoveReplica)
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
