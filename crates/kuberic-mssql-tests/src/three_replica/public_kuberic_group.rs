use std::collections::BTreeSet;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::join_all;
use kuberic_mssql::instance::SqlServerInstanceManager;
use kuberic_mssql::kuberic::{
    KubericAdapterError, SqlServerBootstrapConfig, SqlServerObservationSource,
    SqlServerStartIncarnation, SystemObservationClock, TargetHostEvidenceExchange,
    TargetHostEvidenceReport, TargetHostEvidenceRequest, TestingBootstrapObservation,
};
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::runtime_error::RuntimeError;
use kuberic_mssql::runtime_host::{
    RuntimeEndpointResolver, RuntimeHostApplication, RuntimeHostArgs, RuntimeHostConfig,
    SeedDispatchPhase, SeedDispatchRecord,
};
use kuberic_mssql::tds::{
    SeedSubmissionAction, SeedSubmissionCheckpoint, SeedSubmissionProbe, TdsExecutor,
    TdsMutationExecutor,
};
use kuberic_mssql::{
    Guid, MutationEffect, NativeRole, Observation, ObservationFailureKind, SeedDispatchEvidence,
    SeedDispatchRequest, SqlIdentifier, SqlMutationExecutor, StandaloneDatabaseSnapshot,
};
use kuberic_runtime::RuntimeError as KubericRuntimeError;
use kuberic_runtime::application::StatefulServiceReplica;
use kuberic_runtime::control::proto::{self as wire, agent_control_client::AgentControlClient};
use kuberic_runtime::control::{
    normalize_public_v1_agent_status_report, public_v1_operation_command_to_proto,
};
use kuberic_runtime::host::{HostError, ReplicaHost, RunningReplica};
use kuberic_runtime::protocol::command::PublicV1OperationCommand;
use kuberic_runtime::protocol::public_operations::{
    PossibleDataLossIntent, PublicLifecycleInput, PublicLifecycleRecipe, PublicOperationClass,
    PublicOperationIntent, PublicOperationProgram, PublicOperationReplayDisposition,
    PublicReplicaDescriptor, PublicV1AgentReport, PublicV1CallKind, PublicV1OperationResult,
};
use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch, OperationId, PodUid,
    ProcessSessionId, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole,
    ReplicaStorageIdentity, ReplicaStorageIncarnationContext, ResourceUid, StorageGeneration,
    derive_agent_generation, derive_initialization_id,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, Replicator,
};
use tonic::{Request, transport::Channel};

use super::availability_group::PreparedAvailabilityGroup;
use super::cleanup::{CleanupClock, CleanupCoordinator};
use super::kuberic_group::{MSSQL_FAILOVER_DELAY_SECONDS, MssqlGroupError};
use super::member::ReadyMember;
use super::model::{NativeTopologyBinding, TopologyRun};

const AGENT_TOKEN: &str = "mssql-three-replica-agent";

type Source = Arc<dyn SqlServerObservationSource>;

pub struct PublicMssqlPod {
    pub ordinal: u8,
    pub identity: ReplicaIdentity,
    pub session: kuberic_runtime::protocol::types::ProcessSessionId,
    pub stable_role: ReplicaRole,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    resource_uid: ResourceUid,
    control_address: SocketAddr,
    replication_address: SocketAddr,
    running: Option<RunningReplica>,
    source: Source,
    application: Arc<RuntimeHostApplication>,
    application_root: PathBuf,
    observer_config_path: PathBuf,
    mutation: Option<Arc<PublicMutationExecutor>>,
    conflict_injection: PublicConflictInjection,
}

pub struct PublicMssqlGroup {
    pub resource_uid: ResourceUid,
    pub effective_policy: EffectivePolicy,
    pub configuration: ConfigurationDescriptor,
    pub pods: [PublicMssqlPod; 3],
    native_binding: Option<NativeTopologyBinding>,
    operation_timeout: Duration,
    complete_deadline: Instant,
    faults: PublicFaultInjection,
    target_evidence: Arc<PublicTargetHostEvidenceExchange>,
}

#[derive(Debug, Clone)]
pub struct PublicBuildCommand {
    pub operation_id: OperationId,
    pub build_id: OperationId,
    pub target: ReplicaIdentity,
    pub retire: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicensedConflictScenario {
    AvailabilityGroupIdentityOrProfile,
    LogicalReplicaNativeMismatch,
    EndpointOrMemberProfile,
    StandaloneDatabaseIdentityOrRecovery,
    AssociatedDatabaseIdentityOrRecovery,
    IncompatibleLineage,
    SqlProcessIncarnationReplacement,
}

struct HostAttempt {
    shutdown: tokio::sync::watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<kuberic_runtime::host::Result<Option<RunningReplica>>>>,
}

struct PublicMutationExecutor {
    mutation: TdsMutationExecutor,
    ownership: TdsMutationExecutor,
    ordinal: u8,
    seed_dispatches: AtomicUsize,
    faults: PublicFaultInjection,
    conflicts: PublicConflictInjection,
}

#[derive(Clone)]
struct PublicTargetHostEvidenceExchange {
    resource_uid: ResourceUid,
    configuration_id: String,
    identities: [ReplicaIdentity; 3],
    frozen_sql_starts: [SqlServerStartIncarnation; 3],
    sources: Arc<Mutex<Vec<Option<Source>>>>,
    authority: Arc<Mutex<PublicTargetEvidenceAuthorityState>>,
    injection: Arc<Mutex<Option<PublicTargetEvidenceInjection>>>,
    gate: Arc<Mutex<Option<SeedGate>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicTargetEvidenceAuthority {
    build_id: String,
    identity: ReplicaIdentity,
    process_session_id: ProcessSessionId,
}

struct PublicTargetEvidenceAuthorityState {
    current: Vec<Option<PublicTargetEvidenceAuthority>>,
    retained_dispatch: Option<PublicTargetEvidenceAuthority>,
    retired_builds: BTreeSet<String>,
}

impl PublicTargetEvidenceAuthorityState {
    fn new(target_count: usize) -> Self {
        Self {
            current: vec![None; target_count],
            retained_dispatch: None,
            retired_builds: BTreeSet::new(),
        }
    }

    fn authorize(
        &mut self,
        index: usize,
        authority: PublicTargetEvidenceAuthority,
    ) -> Result<(), MssqlGroupError> {
        let current = self
            .current
            .get_mut(index)
            .ok_or_else(|| MssqlGroupError::new("target evidence authority index is invalid"))?;
        if current.as_ref() == Some(&authority) {
            return Ok(());
        }
        if self.retired_builds.contains(&authority.build_id)
            || current
                .as_ref()
                .is_some_and(|active| active.build_id == authority.build_id)
        {
            return Err(MssqlGroupError::new(
                "target build authority has been retired, superseded, or rebound",
            ));
        }
        if let Some(previous) = current.replace(authority) {
            self.retired_builds.insert(previous.build_id);
        }
        Ok(())
    }

    fn revoke(&mut self, index: usize, build_id: &str) -> Result<(), MssqlGroupError> {
        let current = self
            .current
            .get_mut(index)
            .ok_or_else(|| MssqlGroupError::new("target evidence authority index is invalid"))?;
        if current
            .as_ref()
            .is_some_and(|authority| authority.build_id == build_id)
        {
            *current = None;
        }
        self.retired_builds.insert(build_id.to_owned());
        Ok(())
    }

    fn retain_dispatch(&mut self, authority: PublicTargetEvidenceAuthority) {
        self.retained_dispatch = Some(authority);
    }

    fn exact(&self, index: usize, authority: &PublicTargetEvidenceAuthority) -> bool {
        self.current.get(index).and_then(Option::as_ref) == Some(authority)
            || self.retained_dispatch.as_ref() == Some(authority)
    }

    fn exact_request(
        &self,
        index: usize,
        build_id: &str,
        identity: &ReplicaIdentity,
        process_session_id: &ProcessSessionId,
    ) -> Option<PublicTargetEvidenceAuthority> {
        self.current
            .get(index)
            .and_then(Option::as_ref)
            .into_iter()
            .chain(self.retained_dispatch.as_ref())
            .find(|authority| {
                authority.build_id == build_id
                    && authority.identity == *identity
                    && authority.process_session_id == *process_session_id
            })
            .cloned()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicTargetEvidenceInjection {
    IncompatibleLineage,
}

impl PublicTargetHostEvidenceExchange {
    fn new(
        resource_uid: ResourceUid,
        configuration: &ConfigurationDescriptor,
        identities: [ReplicaIdentity; 3],
        members: &[ReadyMember; 3],
    ) -> Result<Self, MssqlGroupError> {
        let frozen_sql_starts = members
            .iter()
            .map(|member| SqlServerStartIncarnation::new(member.sql_start_time.clone()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(display_error)?
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three target SQL starts required"))?;
        Ok(Self {
            resource_uid,
            configuration_id: configuration.configuration_id.to_string(),
            identities,
            frozen_sql_starts,
            sources: Arc::new(Mutex::new(vec![None, None, None])),
            authority: Arc::new(Mutex::new(PublicTargetEvidenceAuthorityState::new(3))),
            injection: Arc::new(Mutex::new(None)),
            gate: Arc::new(Mutex::new(None)),
        })
    }

    fn register_source(&self, index: usize, source: Source) -> Result<(), MssqlGroupError> {
        let mut sources = self
            .sources
            .lock()
            .map_err(|_| MssqlGroupError::new("target evidence source lock is poisoned"))?;
        let slot = sources
            .get_mut(index)
            .ok_or_else(|| MssqlGroupError::new("target evidence source index is invalid"))?;
        if slot.is_some() {
            return Err(MssqlGroupError::new(
                "target evidence source is already registered",
            ));
        }
        *slot = Some(source);
        Ok(())
    }

    fn replace_source(&self, index: usize, source: Source) -> Result<(), MssqlGroupError> {
        let mut sources = self
            .sources
            .lock()
            .map_err(|_| MssqlGroupError::new("target evidence source lock is poisoned"))?;
        *sources
            .get_mut(index)
            .ok_or_else(|| MssqlGroupError::new("target evidence source index is invalid"))? =
            Some(source);
        Ok(())
    }

    fn arm_incompatible_lineage(&self) -> Result<(), MssqlGroupError> {
        let mut injection = self
            .injection
            .lock()
            .map_err(|_| MssqlGroupError::new("target evidence injection lock is poisoned"))?;
        if injection.is_some() {
            return Err(MssqlGroupError::new(
                "target evidence injection is already armed",
            ));
        }
        *injection = Some(PublicTargetEvidenceInjection::IncompatibleLineage);
        Ok(())
    }

    fn gate_next(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| MssqlGroupError::new("target evidence gate lock is poisoned"))?;
        if gate.is_some() {
            return Err(MssqlGroupError::new(
                "target evidence gate is already armed",
            ));
        }
        *gate = Some(SeedGate { entered, release });
        Ok(())
    }

    fn authorize(
        &self,
        command: &PublicBuildCommand,
        process_session_id: ProcessSessionId,
    ) -> Result<(), MssqlGroupError> {
        let index = self
            .identities
            .iter()
            .position(|identity| identity == &command.target)
            .ok_or_else(|| MssqlGroupError::new("target evidence identity is unknown"))?;
        self.authority
            .lock()
            .map_err(|_| MssqlGroupError::new("target evidence authority lock is poisoned"))?
            .authorize(
                index,
                PublicTargetEvidenceAuthority {
                    build_id: command.build_id.to_string(),
                    identity: command.target.clone(),
                    process_session_id,
                },
            )
    }

    fn retain_dispatched(&self, record: &SeedDispatchRecord) -> Result<(), MssqlGroupError> {
        let Some(authority) = dispatched_authority(record, &self.identities)? else {
            return Ok(());
        };
        self.authority
            .lock()
            .map_err(|_| MssqlGroupError::new("target evidence authority lock is poisoned"))?
            .retain_dispatch(authority);
        Ok(())
    }

    fn exact_authority(
        &self,
        index: usize,
        build_id: &str,
        identity: &ReplicaIdentity,
        process_session_id: &ProcessSessionId,
    ) -> Result<PublicTargetEvidenceAuthority, KubericAdapterError> {
        self.authority
            .lock()
            .map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("target-host test authority")
            })?
            .exact_request(index, build_id, identity, process_session_id)
            .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                "target-host test authority",
            ))
    }

    fn revalidate_authority(
        &self,
        index: usize,
        authority: &PublicTargetEvidenceAuthority,
    ) -> Result<(), KubericAdapterError> {
        if self
            .authority
            .lock()
            .map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("target-host test authority")
            })?
            .exact(index, authority)
        {
            Ok(())
        } else {
            Err(KubericAdapterError::NativeConvergenceUnsafe(
                "target-host test authority",
            ))
        }
    }

    fn revoke(&self, command: &PublicBuildCommand) -> Result<(), MssqlGroupError> {
        let Some(index) = self
            .identities
            .iter()
            .position(|identity| identity == &command.target)
        else {
            return Ok(());
        };
        self.authority
            .lock()
            .map_err(|_| MssqlGroupError::new("target evidence authority lock is poisoned"))?
            .revoke(index, command.build_id.as_str())
    }
}

fn dispatched_authority(
    record: &SeedDispatchRecord,
    identities: &[ReplicaIdentity; 3],
) -> Result<Option<PublicTargetEvidenceAuthority>, MssqlGroupError> {
    if !matches!(
        record.phase(),
        SeedDispatchPhase::Submitting | SeedDispatchPhase::Accepted | SeedDispatchPhase::Ambiguous
    ) {
        return Ok(None);
    }
    let target = record
        .targets()
        .iter()
        .find(|target| target.replica_id() == record.winning_target_replica_id())
        .ok_or_else(|| MssqlGroupError::new("retained seed winner target is unavailable"))?;
    let identity = identities
        .iter()
        .find(|identity| {
            identity.replica_id.to_string() == target.replica_id()
                && identity.instance_id.as_str() == target.instance_id()
                && identity.agent_generation.as_str() == target.agent_generation()
        })
        .cloned()
        .ok_or_else(|| MssqlGroupError::new("retained seed winner identity is unavailable"))?;
    Ok(Some(PublicTargetEvidenceAuthority {
        build_id: record.winning_build_id().to_owned(),
        identity,
        process_session_id: ProcessSessionId::new(target.process_session_id().to_owned()),
    }))
}

#[async_trait]
impl TargetHostEvidenceExchange for PublicTargetHostEvidenceExchange {
    async fn collect(
        &self,
        request: &TargetHostEvidenceRequest,
    ) -> Result<TargetHostEvidenceReport, KubericAdapterError> {
        if request.resource_uid() != self.resource_uid.as_str()
            || request.configuration_id() != self.configuration_id
        {
            return Err(KubericAdapterError::NativeConvergenceConflict(
                "target-host test authority",
            ));
        }
        let index = self
            .identities
            .iter()
            .position(|identity| identity == request.target_identity())
            .ok_or(KubericAdapterError::NativeConvergenceConflict(
                "target-host test authority",
            ))?;
        let authority = self.exact_authority(
            index,
            request.build_id(),
            request.target_identity(),
            request.target_process_session_id(),
        )?;
        let source = self
            .sources
            .lock()
            .map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("target-host evidence source")
            })?
            .get(index)
            .and_then(Clone::clone)
            .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                "target-host evidence source",
            ))?;
        let gate = self
            .gate
            .lock()
            .map_err(|_| KubericAdapterError::NativeConvergenceUnsafe("target-host evidence gate"))?
            .take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release.await.map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("target-host evidence gate")
            })?;
            self.revalidate_authority(index, &authority)?;
        }
        let mut snapshot = match source
            .observe()
            .await
            .map_err(|error| KubericAdapterError::ObservationUnavailable(error.kind))?
        {
            Observation::Present { value, .. } => value,
            Observation::Absent { .. } => {
                return Err(KubericAdapterError::AvailabilityGroupAbsent);
            }
            Observation::Failed(failure) => {
                return Err(KubericAdapterError::ObservationUnavailable(failure.kind));
            }
        };
        if self
            .injection
            .lock()
            .map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("target-host evidence injection")
            })?
            .take()
            == Some(PublicTargetEvidenceInjection::IncompatibleLineage)
        {
            let alternate = alternate_guid(501).map_err(|_| {
                KubericAdapterError::NativeConvergenceUnsafe("target-host evidence injection")
            })?;
            let group = match &mut snapshot.availability_group {
                Observation::Present { value, .. } => value,
                Observation::Absent { .. } | Observation::Failed(_) => {
                    return Err(KubericAdapterError::NativeConvergenceUnsafe(
                        "target-host evidence injection",
                    ));
                }
            };
            let local_native = group
                .local_replica
                .identity
                .native_replica_id()
                .cloned()
                .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                    "target-host evidence injection",
                ))?;
            let database =
                group
                    .databases
                    .first_mut()
                    .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                        "target-host evidence injection",
                    ))?;
            database
                .local
                .as_mut()
                .and_then(|local| local.recovery.as_mut())
                .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                    "target-host evidence injection",
                ))?
                .recovery_fork_guid = Some(alternate.clone());
            let local = database
                .replicas
                .iter_mut()
                .find(|state| state.replica_id == local_native)
                .ok_or(KubericAdapterError::NativeConvergenceUnsafe(
                    "target-host evidence injection",
                ))?;
            if let kuberic_mssql::observation::RecoveryLineageObservation::Local { value } =
                &mut local.lineage
            {
                value.recovery_fork_id = alternate;
            }
        }
        self.revalidate_authority(index, &authority)?;
        Ok(TargetHostEvidenceReport::new(
            request.clone(),
            self.frozen_sql_starts[index].clone(),
            snapshot,
        ))
    }
}

impl PublicMutationExecutor {
    fn new(
        mutation: TdsMutationExecutor,
        ownership: TdsMutationExecutor,
        ordinal: u8,
        faults: PublicFaultInjection,
        conflicts: PublicConflictInjection,
    ) -> Self {
        Self {
            mutation,
            ownership,
            ordinal,
            seed_dispatches: AtomicUsize::new(0),
            faults,
            conflicts,
        }
    }
}

#[derive(Default)]
struct PublicConflictState {
    scenario: Option<LicensedConflictScenario>,
    gate: Option<SeedGate>,
}

#[derive(Clone, Default)]
struct PublicConflictInjection {
    state: Arc<Mutex<PublicConflictState>>,
}

struct SeedGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl PublicConflictInjection {
    fn arm(
        &self,
        scenario: LicensedConflictScenario,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MssqlGroupError::new("licensed conflict injection lock is poisoned"))?;
        if state.scenario.is_some() || state.gate.is_some() {
            return Err(MssqlGroupError::new(
                "licensed conflict injection is already armed",
            ));
        }
        state.scenario = Some(scenario);
        state.gate = Some(SeedGate { entered, release });
        Ok(())
    }

    fn scenario(&self) -> Option<LicensedConflictScenario> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .scenario
    }

    async fn pause_once(&self) -> Result<(), RuntimeError> {
        let gate = self.state.lock().map_err(|_| injected_fault())?.gate.take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            gate.release.await.map_err(|_| injected_fault())?;
        }
        Ok(())
    }

    fn clear(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.scenario = None;
        state.gate = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicMutationFault {
    SourceDatabaseCreationIntentCompletion,
    SourceDatabaseCompletion,
    SourceDatabaseCreationIntentRemovalCompletion,
    SourceRecoveryCompletion,
    SourceOwnershipCompletion,
    SourceMarkerTableCompletion,
    SourceFullBackupCompletion,
    SourceLogBackupCompletion,
    SourceBackupVerificationCompletion,
    AvailabilityGroupIntentCompletion,
    AvailabilityGroupCreationIntentCompletion,
    AvailabilityGroupCompletion,
    AvailabilityGroupBindingCompletion,
    AvailabilityGroupCreationIntentRemovalCompletion,
    PrimarySeedingGrantCompletion,
    PrimarySeedingGrantEvidenceCompletion,
    FirstSecondaryJoinCompletion,
    FirstSecondarySeedingGrantCompletion,
    FirstSecondarySeedingGrantEvidenceCompletion,
    SecondSecondaryJoinCompletion,
    SecondSecondarySeedingGrantCompletion,
    SecondSecondarySeedingGrantEvidenceCompletion,
    SeedBeforeRequestSubmission,
    SeedAfterRequestSubmission,
    SeedAcceptedReplyLost,
    SeedDefiniteSqlRejection,
}

impl PublicMutationFault {
    pub const fn effect_label(self) -> &'static str {
        match self {
            Self::SourceDatabaseCreationIntentCompletion => {
                "retain source database creation intent"
            }
            Self::SourceDatabaseCompletion => "create source database",
            Self::SourceDatabaseCreationIntentRemovalCompletion => {
                "remove source database creation intent"
            }
            Self::SourceRecoveryCompletion => "configure source database recovery",
            Self::SourceOwnershipCompletion => "retain source database ownership",
            Self::SourceMarkerTableCompletion => "create source marker table",
            Self::SourceFullBackupCompletion => "backup source database",
            Self::SourceLogBackupCompletion => "backup source log",
            Self::SourceBackupVerificationCompletion => "verify source backup",
            Self::AvailabilityGroupIntentCompletion => "retain availability group intent",
            Self::AvailabilityGroupCreationIntentCompletion => {
                "retain availability group creation intent"
            }
            Self::AvailabilityGroupCompletion => "create availability group",
            Self::AvailabilityGroupBindingCompletion => "bind availability group ownership",
            Self::AvailabilityGroupCreationIntentRemovalCompletion => {
                "remove availability group creation intent"
            }
            Self::PrimarySeedingGrantCompletion
            | Self::FirstSecondarySeedingGrantCompletion
            | Self::SecondSecondarySeedingGrantCompletion => "grant create any database",
            Self::PrimarySeedingGrantEvidenceCompletion
            | Self::FirstSecondarySeedingGrantEvidenceCompletion
            | Self::SecondSecondarySeedingGrantEvidenceCompletion => {
                "retain create any database evidence"
            }
            Self::FirstSecondaryJoinCompletion | Self::SecondSecondaryJoinCompletion => {
                "join availability group"
            }
            Self::SeedBeforeRequestSubmission
            | Self::SeedAfterRequestSubmission
            | Self::SeedAcceptedReplyLost
            | Self::SeedDefiniteSqlRejection => "add database to availability group",
        }
    }

    const fn ordinal(self) -> u8 {
        match self {
            Self::FirstSecondaryJoinCompletion
            | Self::FirstSecondarySeedingGrantCompletion
            | Self::FirstSecondarySeedingGrantEvidenceCompletion => 2,
            Self::SecondSecondaryJoinCompletion
            | Self::SecondSecondarySeedingGrantCompletion
            | Self::SecondSecondarySeedingGrantEvidenceCompletion => 3,
            _ => 1,
        }
    }
}

#[derive(Default)]
struct PublicFaultState {
    armed: Mutex<Option<PublicMutationFault>>,
    hits: AtomicUsize,
    seed_after_request_gate: Mutex<Option<SeedGate>>,
}

#[derive(Clone, Default)]
pub struct PublicFaultInjection {
    state: Arc<PublicFaultState>,
}

impl PublicFaultInjection {
    pub fn new(fault: PublicMutationFault) -> Self {
        let injection = Self::default();
        injection.arm(fault);
        injection
    }

    pub fn arm(&self, fault: PublicMutationFault) {
        *self
            .state
            .armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(fault);
    }

    pub fn hit_count(&self) -> usize {
        self.state.hits.load(Ordering::SeqCst)
    }

    pub fn gate_seed_after_request(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        let mut gate = self
            .state
            .seed_after_request_gate
            .lock()
            .map_err(|_| MssqlGroupError::new("seed request gate lock is poisoned"))?;
        if gate.is_some() {
            return Err(MssqlGroupError::new("seed request gate is already armed"));
        }
        *gate = Some(SeedGate { entered, release });
        Ok(())
    }

    fn take_if(&self, ordinal: u8, effect: &MutationEffect, before: bool) -> bool {
        let mut armed = self
            .state
            .armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(fault) = *armed else {
            return false;
        };
        let matches =
            !before && ordinal == fault.ordinal() && effect.label() == fault.effect_label();
        if matches {
            *armed = None;
            self.state.hits.fetch_add(1, Ordering::SeqCst);
        }
        matches
    }
}

#[async_trait]
impl SeedSubmissionProbe for PublicFaultInjection {
    async fn checkpoint(
        &self,
        checkpoint: SeedSubmissionCheckpoint,
    ) -> Result<SeedSubmissionAction, RuntimeError> {
        if checkpoint == SeedSubmissionCheckpoint::AfterRequestSubmission {
            let gate = self
                .state
                .seed_after_request_gate
                .lock()
                .map_err(|_| injected_fault())?
                .take();
            if let Some(gate) = gate {
                let _ = gate.entered.send(());
                gate.release.await.map_err(|_| injected_fault())?;
            }
        }
        let mut armed = self.state.armed.lock().map_err(|_| injected_fault())?;
        let matches = matches!(
            (*armed, checkpoint),
            (
                Some(PublicMutationFault::SeedBeforeRequestSubmission),
                SeedSubmissionCheckpoint::BeforeRequestSubmission
            ) | (
                Some(PublicMutationFault::SeedAfterRequestSubmission),
                SeedSubmissionCheckpoint::AfterRequestSubmission
            ) | (
                Some(PublicMutationFault::SeedAcceptedReplyLost),
                SeedSubmissionCheckpoint::AfterAcceptedBeforeReply
            ) | (
                Some(PublicMutationFault::SeedDefiniteSqlRejection),
                SeedSubmissionCheckpoint::BeforeRequestSubmission
            )
        );
        if !matches {
            return Ok(SeedSubmissionAction::Continue);
        }
        let fault = armed.take().expect("matched armed seed fault");
        self.state.hits.fetch_add(1, Ordering::SeqCst);
        if fault == PublicMutationFault::SeedDefiniteSqlRejection {
            Ok(SeedSubmissionAction::InjectDefiniteRejection)
        } else {
            Err(injected_fault())
        }
    }
}

#[async_trait]
impl SqlMutationExecutor for PublicMutationExecutor {
    async fn observe_standalone_database(
        &self,
        database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        self.mutation.observe_standalone_database(database).await
    }

    async fn observe_availability_group_ownership(
        &self,
        database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        let mut observation = self.ownership.observe_standalone_database(database).await?;
        if self.ordinal == 1
            && self.conflicts.scenario()
                == Some(LicensedConflictScenario::StandaloneDatabaseIdentityOrRecovery)
        {
            let snapshot = observation.as_mut().ok_or_else(injected_fault)?;
            snapshot.recovery_model = "SIMPLE".to_owned();
            if let Some(identity) = snapshot.identity.as_mut() {
                identity.database_guid = alternate_guid(205).map_err(|_| injected_fault())?;
            }
            self.conflicts.pause_once().await?;
        }
        Ok(observation)
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        self.mutation.observe_effect_completion(effect).await
    }

    async fn reconcile_seed_dispatch(
        &self,
        request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        self.mutation.reconcile_seed_dispatch(request).await
    }

    async fn submit_seed_dispatch(
        &self,
        request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        self.seed_dispatches.fetch_add(1, Ordering::SeqCst);
        self.mutation.submit_seed_dispatch(request).await
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError> {
        if self.faults.take_if(self.ordinal, effect, true) {
            return Err(injected_fault());
        }
        let is_seed_dispatch = effect.label() == "add database to availability group";
        if is_seed_dispatch {
            self.seed_dispatches.fetch_add(1, Ordering::SeqCst);
        }
        self.mutation.execute(effect).await?;
        if self.faults.take_if(self.ordinal, effect, false) {
            return Err(injected_fault());
        }
        Ok(())
    }
}

fn injected_fault() -> RuntimeError {
    RuntimeError::new(
        ObservationFailureKind::Unreachable,
        "licensed fault injection",
        "reply interrupted at an exact mutation boundary",
    )
}

struct RuntimeConfigContext<'a> {
    root: &'a Path,
    run: &'a TopologyRun,
    native: Option<&'a NativeTopologyBinding>,
    prepared: Option<&'a PreparedAvailabilityGroup>,
    members: &'a [ReadyMember; 3],
    controls: &'a [SocketAddr; 3],
    replications: &'a [SocketAddr; 3],
    identities: &'a [ReplicaIdentity; 3],
}

enum AssemblyTopology<'a> {
    Existing(&'a NativeTopologyBinding),
    Prepared(&'a PreparedAvailabilityGroup),
}

struct AssemblyOptions<'a> {
    convergence_timeout: Duration,
    complete_deadline: Instant,
    faults: PublicFaultInjection,
    operation_namespace: &'a str,
}

impl PublicMssqlGroup {
    pub async fn from_live_with_coordinator(
        root: &Path,
        run: &TopologyRun,
        native_binding: &NativeTopologyBinding,
        members: &[ReadyMember; 3],
        convergence_timeout: Duration,
        complete_deadline: Instant,
        cleanup: &CleanupCoordinator<impl CleanupClock>,
    ) -> Result<Self, MssqlGroupError> {
        Self::assemble(
            root,
            run,
            AssemblyTopology::Existing(native_binding),
            members,
            cleanup,
            AssemblyOptions {
                convergence_timeout,
                complete_deadline,
                faults: PublicFaultInjection::default(),
                operation_namespace: "bootstrap",
            },
        )
        .await
    }

    pub async fn from_prepared_with_coordinator(
        root: &Path,
        run: &TopologyRun,
        prepared: &PreparedAvailabilityGroup,
        members: &[ReadyMember; 3],
        convergence_timeout: Duration,
        complete_deadline: Instant,
        cleanup: &CleanupCoordinator<impl CleanupClock>,
    ) -> Result<Self, MssqlGroupError> {
        Self::assemble(
            root,
            run,
            AssemblyTopology::Prepared(prepared),
            members,
            cleanup,
            AssemblyOptions {
                convergence_timeout,
                complete_deadline,
                faults: PublicFaultInjection::default(),
                operation_namespace: "bootstrap",
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn from_prepared_with_fault(
        root: &Path,
        run: &TopologyRun,
        prepared: &PreparedAvailabilityGroup,
        members: &[ReadyMember; 3],
        convergence_timeout: Duration,
        complete_deadline: Instant,
        cleanup: &CleanupCoordinator<impl CleanupClock>,
        faults: PublicFaultInjection,
        operation_namespace: &str,
    ) -> Result<Self, MssqlGroupError> {
        Self::assemble(
            root,
            run,
            AssemblyTopology::Prepared(prepared),
            members,
            cleanup,
            AssemblyOptions {
                convergence_timeout,
                complete_deadline,
                faults,
                operation_namespace,
            },
        )
        .await
    }

    async fn assemble(
        root: &Path,
        run: &TopologyRun,
        topology: AssemblyTopology<'_>,
        members: &[ReadyMember; 3],
        cleanup: &CleanupCoordinator<impl CleanupClock>,
        options: AssemblyOptions<'_>,
    ) -> Result<Self, MssqlGroupError> {
        let AssemblyOptions {
            convergence_timeout,
            complete_deadline,
            faults,
            operation_namespace,
        } = options;
        check_deadline(complete_deadline, "public Kuberic assembly")?;
        if operation_namespace.is_empty()
            || operation_namespace.chars().any(char::is_control)
            || operation_namespace.trim() != operation_namespace
        {
            return Err(MssqlGroupError::new(
                "public operation namespace is invalid",
            ));
        }
        let (native_binding, prepared) = match topology {
            AssemblyTopology::Existing(native) => (Some(native), None),
            AssemblyTopology::Prepared(prepared) => (None, Some(prepared)),
        };
        let resource_uid = ResourceUid::new(run.resource_uid.clone());
        let identities = identities(run, &resource_uid);
        let roles = match native_binding {
            Some(native) => stable_roles(native)?,
            None => [
                ReplicaRole::Primary,
                ReplicaRole::ActiveSecondary,
                ReplicaRole::ActiveSecondary,
            ],
        };
        let configuration = configuration(native_binding, &identities, roles)?;
        let effective_policy = EffectivePolicy::fixed(3, MSSQL_FAILOVER_DELAY_SECONDS)
            .ok_or_else(|| MssqlGroupError::new("three-member policy is invalid"))?;
        let controls: [SocketAddr; 3] = (0..3)
            .map(|_| free_address())
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three control addresses required"))?;
        let replications: [SocketAddr; 3] = (0..3)
            .map(|_| free_address())
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three replication addresses required"))?;
        let operation_timeout =
            convergence_timeout.min(remaining(complete_deadline, "public Kuberic assembly")?);
        let target_evidence = Arc::new(PublicTargetHostEvidenceExchange::new(
            resource_uid.clone(),
            &configuration,
            identities.clone(),
            members,
        )?);

        let mut attempts = Vec::new();
        let mut pods = Vec::new();
        let primary_mutation_settings = if native_binding.is_none() {
            let observer = ObserverConfig::read(&members[0].observer_config)
                .await
                .map_err(display_error)?;
            Some(observer.connection().testing_with_secret_files(
                members[0].admin_username.clone(),
                members[0].admin_password.clone(),
            ))
        } else {
            None
        };
        let config_context = RuntimeConfigContext {
            root,
            run,
            native: native_binding,
            prepared,
            members,
            controls: &controls,
            replications: &replications,
            identities: &identities,
        };
        for index in 0..3 {
            let conflict_injection = PublicConflictInjection::default();
            let config = runtime_config(&config_context, index).await?;
            let application_root = config.application_root().to_owned();
            let observer = ObserverConfig::read(&members[index].observer_config)
                .await
                .map_err(display_error)?;
            let mutation_settings = native_binding.is_none().then(|| {
                observer.connection().testing_with_secret_files(
                    members[index].admin_username.clone(),
                    members[index].admin_password.clone(),
                )
            });
            let mutation = mutation_settings.map(|settings| {
                Arc::new(PublicMutationExecutor::new(
                    TdsMutationExecutor::testing_new_with_seed_probe(
                        settings,
                        Arc::new(faults.clone()),
                    ),
                    TdsMutationExecutor::testing_new(
                        primary_mutation_settings
                            .clone()
                            .expect("bootstrap construction requires primary ownership settings"),
                    ),
                    index as u8 + 1,
                    faults.clone(),
                    conflict_injection.clone(),
                ))
            });
            let source = Arc::new(SqlServerInstanceManager::new(
                TdsExecutor::new(observer.connection().clone()),
                observer,
            )) as Source;
            target_evidence.register_source(index, source.clone())?;
            let application = Arc::new(if native_binding.is_some() {
                RuntimeHostApplication::with_observation_source(
                    &config,
                    source.clone(),
                    Arc::new(SystemObservationClock),
                )
                .map_err(display_error)?
            } else {
                RuntimeHostApplication::testing_with_bootstrap_sources_and_target_evidence(
                    &config,
                    source.clone(),
                    Arc::new(SystemObservationClock),
                    SqlServerBootstrapConfig::new(
                        SqlIdentifier::new(format!("km_db_{}", run.run_id))
                            .map_err(display_error)?,
                    ),
                    mutation
                        .clone()
                        .expect("bootstrap construction requires a mutation executor"),
                    target_evidence.clone(),
                )
                .map_err(display_error)?
            });
            let host = ReplicaHost::new(
                config.replica_process_config(),
                application.clone(),
                application.storage_state().map_err(display_error)?,
                Arc::new(RuntimeEndpointResolver::new(&config).map_err(display_error)?),
            )
            .with_application_storage_paths(application.storage_paths());
            let (shutdown, receiver) = tokio::sync::watch::channel(false);
            attempts.push(HostAttempt {
                shutdown,
                task: Some(tokio::spawn(host.start_with_shutdown(receiver))),
            });
            pods.push(PublicMssqlPod {
                ordinal: index as u8 + 1,
                identity: identities[index].clone(),
                session: Default::default(),
                stable_role: roles[index],
                pod_uid: PodUid::new(run.kuberic_members[index].pod_uid.clone()),
                pvc_uid: PvcUid::new(run.kuberic_members[index].pvc_uid.clone()),
                resource_uid: resource_uid.clone(),
                control_address: controls[index],
                replication_address: replications[index],
                running: None,
                source,
                application,
                application_root,
                observer_config_path: members[index].observer_config.clone(),
                mutation,
                conflict_injection,
            });
        }
        let mut pods: [PublicMssqlPod; 3] = pods
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three public Kuberic pods required"))?;

        let result = async {
            if let Some(native_binding) = native_binding {
                validate_observations(&pods, native_binding, operation_timeout, complete_deadline)
                    .await?;
            }
            let startup_reports =
                startup_reports(&mut pods, &mut attempts, operation_timeout).await?;
            let uninitialized = startup_reports
                .iter()
                .filter(|report| {
                    report.public_v1.is_none()
                        && report.storage_state == wire::AgentStorageState::Uninitialized as i32
                })
                .count();
            let initialized = startup_reports
                .iter()
                .filter(|report| report.public_v1.is_some())
                .count();
            if uninitialized != 0 && initialized != 0 {
                return Err(MssqlGroupError::new(
                    "public Kuberic stores have mixed fresh and established state",
                ));
            }
            let existing = if uninitialized == pods.len() {
                let initialization_sessions = startup_reports
                    .iter()
                    .map(|report| report.process_session_id.clone())
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| {
                        MssqlGroupError::new("exactly three initialization sessions required")
                    })?;
                initialize(
                    &pods,
                    run,
                    &identities,
                    &configuration,
                    &effective_policy,
                    &initialization_sessions,
                    operation_timeout,
                )
                .await?;
                false
            } else if initialized != pods.len() {
                return Err(MssqlGroupError::new(format!(
                    "public Kuberic storage state is neither fresh nor established: {:?}",
                    startup_reports
                        .iter()
                        .enumerate()
                        .map(|(index, report)| (
                            index + 1,
                            report.storage_state,
                            report.storage_error.clone(),
                            report.public_v1.is_some(),
                        ))
                        .collect::<Vec<_>>()
                )));
            } else {
                true
            };
            for (index, attempt) in attempts.iter_mut().enumerate() {
                if pods[index].running.is_some() {
                    continue;
                }
                let mut task = attempt
                    .task
                    .take()
                    .ok_or_else(|| MssqlGroupError::new("public host task is missing"))?;
                let joined =
                    timeout(operation_timeout, "public Kuberic readiness", &mut task).await;
                let joined = match joined {
                    Ok(joined) => joined,
                    Err(error) => {
                        attempt.task = Some(task);
                        return Err(error);
                    }
                };
                let started = joined.map_err(display_error)?;
                let running = started
                    .map_err(display_error)?
                    .ok_or_else(|| MssqlGroupError::new("public Kuberic startup was cancelled"))?;
                pods[index].running = Some(running);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            let sessions = ensure_configuration(
                &pods,
                &resource_uid,
                &configuration,
                operation_timeout,
                operation_namespace,
                existing,
            )
            .await?;
            for (pod, session) in pods.iter_mut().zip(sessions) {
                pod.session = session;
            }
            if prepared.is_some() {
                admit_bootstrap_replicators(&pods, &configuration, &replications).await?;
            }
            refresh_current_progress(
                &pods,
                &resource_uid,
                &configuration,
                operation_timeout,
                complete_deadline,
                operation_namespace,
            )
            .await?;
            Ok::<(), MssqlGroupError>(())
        }
        .await;
        if let Err(error) = result {
            let shutdown = cancel_attempts(&mut attempts, &mut pods, cleanup).await;
            return Err(match shutdown {
                Ok(()) => error,
                Err(shutdown) => MssqlGroupError::termination_unconfirmed(format!(
                    "{error}; public Kuberic startup cleanup failed: {shutdown}"
                )),
            });
        }
        Ok(Self {
            resource_uid,
            effective_policy,
            configuration,
            pods,
            native_binding: native_binding.cloned(),
            operation_timeout,
            complete_deadline,
            faults,
            target_evidence,
        })
    }

    pub async fn reports_bracketed(&self) -> Result<[wire::AgentStatusReport; 3], MssqlGroupError> {
        let before =
            observe_sources(&self.pods, self.operation_timeout, self.complete_deadline).await?;
        if let Some(native_binding) = &self.native_binding {
            validate_snapshots(&before, native_binding)?;
        } else {
            validate_bootstrap_snapshots(&before)?;
        }
        let mut reports = Vec::new();
        for pod in &self.pods {
            reports.push(status(pod, self.operation_timeout).await?.1);
        }
        let after =
            observe_sources(&self.pods, self.operation_timeout, self.complete_deadline).await?;
        if let Some(native_binding) = &self.native_binding {
            validate_snapshots(&after, native_binding)?;
        } else {
            validate_bootstrap_snapshots(&after)?;
        }
        reports
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three public Kuberic reports required"))
    }

    pub async fn bootstrap_configuration_sequence(&self) -> Result<i64, MssqlGroupError> {
        if self.native_binding.is_some() {
            return Err(MssqlGroupError::new(
                "bootstrap sequence is only available for prepared topology hosts",
            ));
        }
        let deadline = tokio::time::Instant::now()
            + self.operation_timeout.min(remaining(
                self.complete_deadline,
                "bootstrap configuration sequence",
            )?);
        let mut previous = None;
        loop {
            let snapshots = match observe_sources(
                &self.pods,
                rpc_remaining(deadline, "bootstrap configuration sequence observation")?,
                self.complete_deadline,
            )
            .await
            {
                Ok(snapshots) => snapshots,
                Err(error)
                    if error
                        .to_string()
                        .contains("direct SQL observation failed: Inconsistent") =>
                {
                    tokio::time::sleep(
                        rpc_remaining(deadline, "bootstrap inconsistent observation retry")?
                            .min(Duration::from_millis(100)),
                    )
                    .await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            validate_bootstrap_snapshots(&snapshots)?;
            let sequences = snapshots
                .iter()
                .map(|snapshot| match &snapshot.availability_group {
                    Observation::Present { value, .. } => Ok(value.configuration_sequence.value()),
                    _ => Err(MssqlGroupError::new(
                        "bootstrap validation requires a present availability group",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let common = sequences
                .first()
                .copied()
                .filter(|sequence| sequences.iter().all(|candidate| candidate == sequence));
            if let Some(common) = common
                && Some(common) == previous
            {
                refresh_current_progress(
                    &self.pods,
                    &self.resource_uid,
                    &self.configuration,
                    rpc_remaining(deadline, "bootstrap progress refresh")?,
                    self.complete_deadline,
                    "bootstrap-observed",
                )
                .await?;
                return Ok(common);
            }
            previous = common;
            tokio::time::sleep(
                rpc_remaining(deadline, "bootstrap configuration sequence retry")?
                    .min(Duration::from_millis(100)),
            )
            .await;
        }
    }

    pub async fn assert_superseded_sessions_rejected(
        &self,
        superseded: &[kuberic_runtime::protocol::types::ProcessSessionId; 3],
    ) -> Result<(), MssqlGroupError> {
        let deadline = tokio::time::Instant::now() + self.operation_timeout;
        for (index, (pod, session)) in self.pods.iter().zip(superseded).enumerate() {
            if &pod.session == session {
                return Err(MssqlGroupError::new(format!(
                    "member {} did not replace its process session",
                    pod.ordinal
                )));
            }
            let reports = public_reports(
                &self.pods,
                deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .ok_or_else(|| {
                        MssqlGroupError::new("stale-session validation exceeded its deadline")
                    })?,
            )
            .await?;
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or_else(|| {
                    MssqlGroupError::new("stale-session validation exceeded its deadline")
                })?;
            let (mut client, _) = status(pod, remaining).await?;
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or_else(|| {
                    MssqlGroupError::new("stale-session validation exceeded its deadline")
                })?;
            let request = program_request(
                &self.pods,
                &reports,
                index,
                &self.resource_uid,
                session,
                &format!("stale-restart-progress-{}", index + 1),
                PublicOperationProgram::Progress { capability: false },
            )
            .await?;
            let error = tokio::time::timeout(remaining, client.execute(request))
                .await
                .map_err(|_| MssqlGroupError::new("stale-session execution exceeded its deadline"))?
                .err()
                .ok_or_else(|| {
                    MssqlGroupError::new(format!(
                        "member {} accepted a superseded process session",
                        pod.ordinal
                    ))
                })?;
            if error.code() != tonic::Code::FailedPrecondition
                || !error.message().contains("stale agent process session")
            {
                return Err(MssqlGroupError::new(format!(
                    "member {} returned an unexpected stale-session result: {error}",
                    pod.ordinal
                )));
            }
        }
        Ok(())
    }

    pub fn target_build_command(
        &self,
        target_index: usize,
        operation_id: impl Into<String>,
        build_id: impl Into<String>,
    ) -> Result<PublicBuildCommand, MssqlGroupError> {
        if !matches!(target_index, 1 | 2) {
            return Err(MssqlGroupError::new(
                "public bootstrap builds target secondary indexes 1 or 2",
            ));
        }
        let operation_id = OperationId::new(operation_id.into());
        let build_id = OperationId::new(build_id.into());
        if operation_id != build_id {
            return Err(MssqlGroupError::new(
                "public target build operation ID must equal its durable build ID",
            ));
        }
        Ok(PublicBuildCommand {
            operation_id,
            build_id,
            target: self.pods[target_index].identity.clone(),
            retire: false,
        })
    }

    pub fn replay_build_command(&self, command: &PublicBuildCommand) -> PublicBuildCommand {
        command.clone()
    }

    pub fn retire_build_command(&self, command: &PublicBuildCommand) -> PublicBuildCommand {
        PublicBuildCommand {
            operation_id: OperationId::new(format!("remove-{}", command.operation_id)),
            build_id: command.build_id.clone(),
            target: command.target.clone(),
            retire: true,
        }
    }

    pub fn supersede_build_command(
        &self,
        command: &PublicBuildCommand,
        build_id: impl Into<String>,
    ) -> PublicBuildCommand {
        let build_id = OperationId::new(build_id.into());
        PublicBuildCommand {
            operation_id: build_id.clone(),
            build_id,
            target: command.target.clone(),
            retire: false,
        }
    }

    pub async fn execute_build_commands_sequential(
        &self,
        commands: &[PublicBuildCommand],
    ) -> Vec<Result<(), tonic::Status>> {
        let mut outcomes = Vec::with_capacity(commands.len());
        for command in commands {
            outcomes.push(self.execute_build_command_with_retry(command).await);
        }
        outcomes
    }

    pub async fn execute_build_commands_concurrently(
        &self,
        commands: &[PublicBuildCommand],
    ) -> Vec<Result<(), tonic::Status>> {
        join_all(
            commands
                .iter()
                .map(|command| self.execute_build_command_with_retry(command)),
        )
        .await
    }

    pub fn testing_gate_next_seed_intent(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        let replicator = self.pods[0]
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap source replicator is unavailable"))?;
        replicator
            .testing_gate_seed_intent(entered, release)
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    pub fn testing_gate_next_seed_compaction(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        self.pods[0]
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap source replicator is unavailable"))?
            .testing_gate_seed_compaction(entered, release)
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    pub fn testing_gate_next_seed_completion(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        self.pods[0]
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap source replicator is unavailable"))?
            .testing_gate_seed_completion(entered, release)
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    pub fn testing_gate_next_target_evidence(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        self.target_evidence.gate_next(entered, release)
    }

    pub fn testing_inject_target_lineage_mismatch(&self) -> Result<(), MssqlGroupError> {
        self.target_evidence.arm_incompatible_lineage()
    }

    pub fn testing_seed_dispatch_count(&self) -> Result<usize, MssqlGroupError> {
        Ok(self.pods[0]
            .mutation
            .as_ref()
            .ok_or_else(|| MssqlGroupError::new("bootstrap mutation executor is unavailable"))?
            .seed_dispatches
            .load(Ordering::SeqCst))
    }

    pub fn testing_arm_mutation_fault(&self, fault: PublicMutationFault) {
        self.faults.arm(fault);
    }

    pub fn testing_gate_seed_after_request(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        self.faults.gate_seed_after_request(entered, release)
    }

    pub fn testing_mutation_fault_hits(&self) -> usize {
        self.faults.hit_count()
    }

    pub fn testing_seed_dispatch_record(
        &self,
    ) -> Result<Option<SeedDispatchRecord>, MssqlGroupError> {
        let path = self.pods[0].application_root.join("seed-dispatch-v1.json");
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(MssqlGroupError::new(error.to_string())),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    pub fn testing_abort_seed_source(&self) {
        self.pods[0].application.abort();
    }

    pub async fn testing_execute_admitted_seed_callback(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        self.execute_admitted_seed_callback(command).await
    }

    pub async fn testing_execute_admitted_seed_callback_with_retry(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        let deadline = tokio::time::Instant::now() + self.operation_timeout;
        loop {
            let result = self.execute_admitted_seed_callback(command).await;
            if result.as_ref().is_err_and(retryable_public_build_error)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            return result;
        }
    }

    pub fn testing_spawn_admitted_seed_callback(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<tokio::task::JoinHandle<Result<(), tonic::Status>>, tonic::Status> {
        let (replicator, build) = self.admitted_seed_callback(command)?;
        Ok(tokio::spawn(async move {
            replicator
                .build_replica(build)
                .await
                .map_err(|error| tonic::Status::internal(error.to_string()))
        }))
    }

    pub async fn testing_execute_kuberic_build_command(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        let deadline = tokio::time::Instant::now() + self.operation_timeout;
        loop {
            let result = self.update_testing_build_authority(command).await;
            if result.as_ref().is_err_and(retryable_public_build_error)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            return result;
        }
    }

    pub async fn testing_admit_target_build_authority_only(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        let deadline = tokio::time::Instant::now() + self.operation_timeout;
        loop {
            let result = self.update_testing_build_authority(command).await;
            if result.as_ref().is_err_and(retryable_public_build_error)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            return result;
        }
    }

    pub async fn testing_assert_build_success_not_published(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), MssqlGroupError> {
        for pod in &self.pods {
            self.assert_build_success_not_published_by(pod, command)
                .await?;
        }
        Ok(())
    }

    pub async fn testing_assert_source_build_success_not_published(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), MssqlGroupError> {
        self.assert_build_success_not_published_by(&self.pods[0], command)
            .await
    }

    pub fn testing_arm_licensed_conflict(
        &self,
        scenario: LicensedConflictScenario,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        if scenario == LicensedConflictScenario::StandaloneDatabaseIdentityOrRecovery {
            return self.pods[0]
                .conflict_injection
                .arm(scenario, entered, release);
        }
        let scenario = match scenario {
            LicensedConflictScenario::AvailabilityGroupIdentityOrProfile => {
                TestingBootstrapObservation::AvailabilityGroupIdentityOrProfile
            }
            LicensedConflictScenario::LogicalReplicaNativeMismatch => {
                TestingBootstrapObservation::LogicalReplicaNativeMismatch
            }
            LicensedConflictScenario::EndpointOrMemberProfile => {
                TestingBootstrapObservation::EndpointOrMemberProfile
            }
            LicensedConflictScenario::AssociatedDatabaseIdentityOrRecovery => {
                TestingBootstrapObservation::AssociatedDatabaseIdentityOrRecovery
            }
            LicensedConflictScenario::IncompatibleLineage => {
                TestingBootstrapObservation::IncompatibleLineage
            }
            LicensedConflictScenario::SqlProcessIncarnationReplacement => {
                TestingBootstrapObservation::SqlProcessIncarnationReplacement
            }
            LicensedConflictScenario::StandaloneDatabaseIdentityOrRecovery => unreachable!(),
        };
        let pod_index =
            usize::from(scenario == TestingBootstrapObservation::SqlProcessIncarnationReplacement);
        self.pods[pod_index]
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap source replicator is unavailable"))?
            .testing_inject_bootstrap_observation(scenario, entered, release)
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    pub fn testing_clear_licensed_conflict(&self) {
        self.pods[0].conflict_injection.clear();
    }

    pub fn testing_arm_incarnation_conflict(
        &self,
        snapshot: InstanceSnapshot,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), MssqlGroupError> {
        self.pods[1]
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap secondary replicator is unavailable"))?
            .testing_inject_incarnation_observation(snapshot, entered, release)
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    pub async fn testing_execute_primary_configuration_callback(
        &self,
    ) -> Result<(), MssqlGroupError> {
        self.testing_execute_configuration_callback(0).await
    }

    pub async fn testing_execute_secondary_admitted_validator(
        &self,
    ) -> Result<(), MssqlGroupError> {
        self.pods[1]
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap secondary replicator is unavailable"))?
            .current_progress()
            .await
            .map(|_| ())
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    async fn testing_execute_configuration_callback(
        &self,
        pod_index: usize,
    ) -> Result<(), MssqlGroupError> {
        let replicator = self.pods[pod_index]
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap source replicator is unavailable"))?;
        let replicas = self
            .pods
            .iter()
            .map(|member| {
                let mut replica = ReplicaInformation::new(
                    OperationId::default(),
                    member.identity.clone(),
                    replicator
                        .admitted_topology()
                        .and_then(|topology| {
                            topology
                                .members()
                                .iter()
                                .find(|candidate| candidate.kuberic_identity() == &member.identity)
                                .map(|candidate| candidate.replication_address().to_owned())
                        })
                        .unwrap_or_default(),
                );
                replica.process_session_id = member.session.clone();
                replica.role = member.stable_role;
                replica
            })
            .collect();
        replicator
            .update_current_replica_set_configuration(ReplicaSetConfiguration {
                configuration: self.configuration.clone(),
                replicas,
            })
            .await
            .map_err(|error| MssqlGroupError::new(error.to_string()))
    }

    pub fn testing_frozen_secondary_start_identity(&self) -> Result<String, MssqlGroupError> {
        self.testing_frozen_start_identity(1)
    }

    fn testing_frozen_start_identity(&self, pod_index: usize) -> Result<String, MssqlGroupError> {
        self.pods[pod_index]
            .application
            .testing_replicator()
            .and_then(|replicator| replicator.admitted_topology())
            .map(|topology| {
                topology
                    .local_sql_server_start_incarnation()
                    .as_str()
                    .to_owned()
            })
            .ok_or_else(|| MssqlGroupError::new("licensed admitted topology is unavailable"))
    }

    pub async fn testing_wait_for_fresh_secondary_observation(
        &self,
        frozen: &str,
        host_port: u16,
        timeout: Duration,
    ) -> Result<InstanceSnapshot, MssqlGroupError> {
        let bytes = fs::read(&self.pods[1].observer_config_path).map_err(display_error)?;
        let mut document: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(display_error)?;
        document["port"] = serde_json::Value::from(host_port);
        let observer =
            ObserverConfig::from_json(&serde_json::to_vec(&document).map_err(display_error)?)
                .map_err(display_error)?;
        let source = Arc::new(SqlServerInstanceManager::new(
            TdsExecutor::new(observer.connection().clone()),
            observer,
        )) as Source;
        let deadline = tokio::time::Instant::now() + timeout;
        let mut last_retryable = "no observation attempted".to_owned();
        loop {
            match source.observe().await {
                Ok(Observation::Present { value, .. })
                    if value.instance.sqlserver_start_time != frozen =>
                {
                    self.target_evidence.replace_source(1, source.clone())?;
                    return Ok(value);
                }
                Ok(Observation::Present { .. }) if tokio::time::Instant::now() >= deadline => {
                    return Err(MssqlGroupError::new(format!(
                        "SQL Server restart retained the frozen start identity; last setup observation: {last_retryable}"
                    )));
                }
                Ok(Observation::Absent { .. } | Observation::Failed(_)) | Err(_)
                    if tokio::time::Instant::now() >= deadline =>
                {
                    return Err(MssqlGroupError::new(format!(
                        "fresh present SQL Server observation was unavailable after restart; last setup observation: {last_retryable}"
                    )));
                }
                Ok(Observation::Present { value, .. }) => {
                    last_retryable = format!(
                        "present with unchanged start identity {}",
                        value.instance.sqlserver_start_time
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Ok(Observation::Absent { .. }) => {
                    last_retryable = "absent".to_owned();
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Ok(Observation::Failed(failure)) => {
                    last_retryable = format!("failed: {:?}", failure.kind);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(error) => {
                    last_retryable = format!("unavailable: {error}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    pub async fn testing_assert_source_build_success_never_published(
        &self,
        command: &PublicBuildCommand,
        duration: Duration,
    ) -> Result<(), MssqlGroupError> {
        let deadline = tokio::time::Instant::now() + duration;
        loop {
            self.testing_assert_source_build_success_not_published(command)
                .await?;
            if tokio::time::Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn assert_build_success_not_published_by(
        &self,
        pod: &PublicMssqlPod,
        command: &PublicBuildCommand,
    ) -> Result<(), MssqlGroupError> {
        let (_, report) = status(pod, self.operation_timeout).await?;
        exact_public_report(pod, &report)?;
        let public = report
            .public_v1
            .as_ref()
            .ok_or_else(|| MssqlGroupError::new("public build completion report is unavailable"))?;
        if public.completed_operations.iter().any(|completion| {
            completion.operation_id == command.operation_id.as_str()
                && completion.result
                    == wire::PublicV1OperationResultKind::PublicV1OperationResultSucceeded as i32
        }) {
            return Err(MssqlGroupError::new(format!(
                "stale build {} published completion from member {}",
                command.build_id, pod.ordinal
            )));
        }
        Ok(())
    }

    async fn update_testing_build_authority(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        if let Some(record) = self
            .testing_seed_dispatch_record()
            .map_err(|error| tonic::Status::internal(error.to_string()))?
        {
            self.target_evidence
                .retain_dispatched(&record)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
        }
        if command.retire {
            return self
                .target_evidence
                .revoke(command)
                .map_err(|error| tonic::Status::internal(error.to_string()));
        }
        let target = self
            .pods
            .iter()
            .find(|pod| pod.identity == command.target)
            .ok_or_else(|| tonic::Status::failed_precondition("target pod is unavailable"))?;
        let (_, wire_report) = status(target, self.operation_timeout)
            .await
            .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
        let report = exact_public_report(target, &wire_report)
            .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
        self.target_evidence
            .authorize(command, report.process_session_id)
            .map_err(|error| tonic::Status::failed_precondition(error.to_string()))
    }

    async fn execute_build_command(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        self.update_testing_build_authority(command).await?;
        if command.retire {
            Ok(())
        } else {
            self.execute_admitted_seed_callback(command).await
        }
    }

    async fn execute_admitted_seed_callback(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        let timeout_duration = self.operation_timeout.min(
            remaining(self.complete_deadline, "public target build")
                .map_err(|error| tonic::Status::deadline_exceeded(error.to_string()))?,
        );
        let (replicator, build) = self.admitted_seed_callback(command)?;
        tokio::time::timeout(timeout_duration, replicator.build_replica(build))
            .await
            .map_err(|_| tonic::Status::deadline_exceeded("bootstrap seed callback timed out"))?
            .map_err(|error| tonic::Status::internal(error.to_string()))
    }

    fn admitted_seed_callback(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<
        (
            Arc<kuberic_mssql::kuberic::SqlServerReplicator>,
            ReplicaInformation,
        ),
        tonic::Status,
    > {
        let source = &self.pods[0];
        let replicator = source.application.testing_replicator().ok_or_else(|| {
            tonic::Status::failed_precondition("bootstrap source replicator is unavailable")
        })?;
        let topology = replicator.admitted_topology().ok_or_else(|| {
            tonic::Status::failed_precondition("bootstrap source topology is not admitted")
        })?;
        let target = topology
            .members()
            .iter()
            .find(|member| member.kuberic_identity() == &command.target)
            .ok_or_else(|| {
                tonic::Status::failed_precondition("bootstrap build target is not admitted")
            })?;
        let target_pod = self
            .pods
            .iter()
            .find(|pod| pod.identity == command.target)
            .ok_or_else(|| tonic::Status::failed_precondition("target pod is unavailable"))?;
        let initialization = derive_initialization_id(
            &self.resource_uid,
            target_pod.identity.replica_id,
            &target_pod.pod_uid,
            &target_pod.pvc_uid,
        );
        let mut build = ReplicaInformation::new(
            command.build_id.clone(),
            command.target.clone(),
            target.replication_address().to_owned(),
        );
        build.process_session_id = target.process_session_id().clone();
        build.storage_incarnation = Some(ReplicaStorageIncarnationContext {
            replica: target_pod.identity.clone(),
            storage: ReplicaStorageIdentity::Persistent {
                pod_uid: target_pod.pod_uid.clone(),
                pvc_uid: target_pod.pvc_uid.clone(),
            },
            storage_generation: StorageGeneration::new(format!("storage-{initialization}")),
        });
        Ok((replicator, build))
    }

    async fn execute_build_command_with_retry(
        &self,
        command: &PublicBuildCommand,
    ) -> Result<(), tonic::Status> {
        let deadline = tokio::time::Instant::now() + self.operation_timeout;
        loop {
            let result = self.execute_build_command(command).await;
            if result.as_ref().is_err_and(retryable_public_build_error)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            return result;
        }
    }

    pub async fn shutdown_with_coordinator(
        mut self,
        cleanup: &CleanupCoordinator<impl CleanupClock>,
    ) -> Result<(), MssqlGroupError> {
        for pod in &self.pods {
            if let Some(running) = &pod.running {
                running.shutdown();
            }
        }
        let mut errors = Vec::new();
        for pod in &mut self.pods {
            if let Some(mut running) = pod.running.take() {
                let deadline = cleanup.remaining().min(Duration::from_secs(10));
                match tokio::time::timeout(deadline, running.wait()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(error.to_string()),
                    Err(_) => {
                        pod.application.abort();
                        match tokio::time::timeout(
                            cleanup.remaining().min(Duration::from_secs(10)),
                            running.wait(),
                        )
                        .await
                        {
                            Ok(Ok(())) => errors.push(format!(
                                "member {} shutdown exceeded its deadline",
                                pod.ordinal
                            )),
                            Ok(Err(error)) => errors.push(format!(
                                "member {} shutdown timed out and abort failed: {error}",
                                pod.ordinal
                            )),
                            Err(_) => errors.push(format!(
                                "member {} termination remains unconfirmed after abort",
                                pod.ordinal
                            )),
                        }
                    }
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(MssqlGroupError::termination_unconfirmed(format!(
                "public Kuberic shutdown failed: {errors:?}"
            )))
        }
    }
}

fn alternate_guid(index: u16) -> Result<Guid, MssqlGroupError> {
    Guid::parse(
        "licensed conflict GUID",
        format!("ffffffff-ffff-ffff-ffff-{index:012x}"),
    )
    .map_err(display_error)
}

fn retryable_public_build_error(error: &tonic::Status) -> bool {
    error.code() == tonic::Code::Cancelled
        || (error.code() == tonic::Code::Unavailable
            && error.message().contains("reconfiguration is still pending"))
        || (error.code() == tonic::Code::Internal
            && error
                .message()
                .contains("SQL Server observation is unavailable: Malformed"))
        || (error.code() == tonic::Code::Internal
            && error
                .message()
                .contains("SQL Server observation is unavailable: Unsupported"))
        || (error.code() == tonic::Code::Internal
            && error
                .message()
                .contains("current configuration replay differs from the admitted value"))
        || (error.code() == tonic::Code::Internal
            && error.message().contains(
                "current replica session, address, or role differs from the frozen member",
            ))
        || (error.code() == tonic::Code::Internal
            && error
                .message()
                .contains("genuinely ambiguous SQL submission outcome"))
        || (error.code() == tonic::Code::Internal
            && error.message().contains("seed dispatch state is missing"))
}

async fn admit_bootstrap_replicators(
    pods: &[PublicMssqlPod; 3],
    configuration: &ConfigurationDescriptor,
    replications: &[SocketAddr; 3],
) -> Result<(), MssqlGroupError> {
    let results = join_all(pods.iter().map(|pod| async move {
        let replicator = pod
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("bootstrap replicator is unavailable"))?;
        let replicas = pods
            .iter()
            .enumerate()
            .map(|(index, member)| {
                let mut replica = ReplicaInformation::new(
                    OperationId::default(),
                    member.identity.clone(),
                    format!("http://{}", replications[index]),
                );
                replica.process_session_id = member.session.clone();
                replica.role = member.stable_role;
                replica
            })
            .collect();
        let current = ReplicaSetConfiguration {
            configuration: configuration.clone(),
            replicas,
        };
        let role = configuration
            .members
            .iter()
            .find(|member| member.identity == pod.identity)
            .map(|member| member.role)
            .ok_or_else(|| {
                MssqlGroupError::new(format!(
                    "member {} is absent from bootstrap configuration",
                    pod.ordinal
                ))
            })?;
        for attempt in 1..=3 {
            match replicator
                .update_current_replica_set_configuration(current.clone())
                .await
            {
                Ok(()) => {
                    replicator
                        .change_role(configuration.epoch, role)
                        .await
                        .map_err(|error| {
                            MssqlGroupError::new(format!(
                                "member {} bootstrap role validation failed: {error}",
                                pod.ordinal
                            ))
                        })?;
                    return Ok(());
                }
                Err(error)
                    if error.to_string().contains("operation was cancelled") && attempt < 3 =>
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => {
                    return Err(MssqlGroupError::new(format!(
                        "member {} bootstrap configuration failed: {error}",
                        pod.ordinal
                    )));
                }
            }
        }
        unreachable!("bounded bootstrap configuration retry always returns")
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn runtime_config(
    context: &RuntimeConfigContext<'_>,
    local_index: usize,
) -> Result<RuntimeHostConfig, MssqlGroupError> {
    let runtime_root = context.run.members[local_index]
        .data_directory
        .join("kuberic-public-runtime");
    create_private_directory(&runtime_root)?;
    create_private_directory(&runtime_root.join("state"))?;
    let token = runtime_root.join("agent-token");
    write_private(&token, AGENT_TOKEN)?;
    let topology = runtime_root.join("topology.json");
    let topology_members = (0..3)
        .map(|index| {
            let endpoint_url = context.native.map_or_else(
                || {
                    let prepared = context
                        .prepared
                        .expect("prepared bootstrap requires endpoint evidence");
                    format!(
                        "TCP://{}:{}",
                        context.run.members[index].server_name,
                        prepared.endpoint_evidence[index].port
                    )
                },
                |native| native.members[index].endpoint_url.clone(),
            );
            serde_json::json!({
                "replica_id": context.run.kuberic_members[index].replica_id,
                "server_name": context.run.members[index].server_name,
                "endpoint_url": endpoint_url
            })
        })
        .collect::<Vec<_>>();
    write_private(
        &topology,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "members": topology_members
        }))
        .map_err(display_error)?,
    )?;
    let routes = runtime_root.join("routes.json");
    let route_values = (0..3)
        .filter(|index| *index != local_index)
        .map(|index| {
            serde_json::json!({
                "replica_id": context.identities[index].replica_id.value(),
                "instance_id": context.identities[index].instance_id.as_str(),
                "agent_generation": context.identities[index].agent_generation.as_str(),
                "control_endpoint": format!("http://{}", context.controls[index]),
                "replication_endpoint": format!("http://{}", context.replications[index])
            })
        })
        .collect::<Vec<_>>();
    write_private(
        &routes,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "routes": route_values
        }))
        .map_err(display_error)?,
    )?;
    RuntimeHostConfig::load(
        RuntimeHostArgs {
            resource_uid: context.run.resource_uid.clone(),
            replica_id: context.run.kuberic_members[local_index].replica_id,
            pod_uid: context.run.kuberic_members[local_index].pod_uid.clone(),
            pvc_uid: context.run.kuberic_members[local_index].pvc_uid.clone(),
            data_root: runtime_root.join("state"),
            application_root: None,
            control_address: context.controls[local_index],
            replication_address: context.replications[local_index],
            control_endpoint: format!("http://{}", context.controls[local_index]),
            replication_endpoint: format!("http://{}", context.replications[local_index]),
            namespace: None,
            peer_routes: Some(routes),
            observer_config: context.members[local_index].observer_config.clone(),
            topology_config: topology,
            bearer_token_file: token,
            rpc_deadline_ms: 5_000,
            transport_window_capacity: 256,
            shutdown_deadline_ms: 10_000,
        },
        context.root,
    )
    .await
    .map_err(display_error)
}

fn identities(run: &TopologyRun, resource: &ResourceUid) -> [ReplicaIdentity; 3] {
    std::array::from_fn(|index| {
        let replica_id = ReplicaId::new(run.kuberic_members[index].replica_id);
        let pod_uid = PodUid::new(run.kuberic_members[index].pod_uid.clone());
        let pvc_uid = PvcUid::new(run.kuberic_members[index].pvc_uid.clone());
        let initialization = derive_initialization_id(resource, replica_id, &pod_uid, &pvc_uid);
        ReplicaIdentity {
            replica_id,
            instance_id: ReplicaInstanceId::new(pod_uid.as_str()),
            agent_generation: derive_agent_generation(&initialization),
        }
    })
}

fn stable_roles(native: &NativeTopologyBinding) -> Result<[ReplicaRole; 3], MssqlGroupError> {
    native
        .members
        .iter()
        .map(|member| match member.role.as_str() {
            "PRIMARY" => Ok(ReplicaRole::Primary),
            "SECONDARY" => Ok(ReplicaRole::ActiveSecondary),
            _ => Err(MssqlGroupError::new("native role is not stable")),
        })
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three native roles required"))
}

fn configuration(
    native: Option<&NativeTopologyBinding>,
    identities: &[ReplicaIdentity; 3],
    roles: [ReplicaRole; 3],
) -> Result<ConfigurationDescriptor, MssqlGroupError> {
    let primary = roles
        .iter()
        .position(|role| *role == ReplicaRole::Primary)
        .ok_or_else(|| MssqlGroupError::new("native topology has no primary"))?;
    Ok(ConfigurationDescriptor::new(
        Epoch::new(
            0,
            native.map_or(1, |binding| binding.configuration_sequence),
        ),
        identities[primary].replica_id,
        identities
            .iter()
            .cloned()
            .zip(roles)
            .map(|(identity, role)| ConfigurationMember { identity, role })
            .collect(),
        2,
    ))
}

async fn startup_reports(
    pods: &mut [PublicMssqlPod; 3],
    attempts: &mut [HostAttempt],
    timeout_duration: Duration,
) -> Result<[wire::AgentStatusReport; 3], MssqlGroupError> {
    let mut reports = Vec::new();
    for index in 0..3 {
        enum StartupOutcome {
            Report(Box<wire::AgentStatusReport>),
            Task(
                Result<
                    kuberic_runtime::host::Result<Option<RunningReplica>>,
                    tokio::task::JoinError,
                >,
            ),
        }
        let outcome = {
            let task = attempts[index]
                .task
                .as_mut()
                .ok_or_else(|| MssqlGroupError::new("public host task is missing"))?;
            tokio::select! {
                report = async {
                    status(&pods[index], timeout_duration)
                        .await
                        .map(|(_, report)| Box::new(report))
                } => {
                    StartupOutcome::Report(report?)
                }
                result = task => StartupOutcome::Task(result),
            }
        };
        match outcome {
            StartupOutcome::Report(report) => reports.push(*report),
            StartupOutcome::Task(result) => {
                attempts[index].task.take();
                let running = result
                    .map_err(display_error)?
                    .map_err(display_error)?
                    .ok_or_else(|| MssqlGroupError::new("public host startup was cancelled"))?;
                pods[index].running = Some(running);
                reports.push(status(&pods[index], timeout_duration).await?.1);
            }
        }
    }
    reports
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three startup reports required"))
}

async fn initialize(
    pods: &[PublicMssqlPod; 3],
    run: &TopologyRun,
    identities: &[ReplicaIdentity; 3],
    configuration: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
    sessions: &[String; 3],
    timeout_duration: Duration,
) -> Result<(), MssqlGroupError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let results = join_all((0..3).map(|index| async move {
        let (mut client, _) = status(
            &pods[index],
            rpc_remaining(deadline, "public initialization status")?,
        )
        .await?;
        let initialization = derive_initialization_id(
            &ResourceUid::new(run.resource_uid.clone()),
            identities[index].replica_id,
            &PodUid::new(run.kuberic_members[index].pod_uid.clone()),
            &PvcUid::new(run.kuberic_members[index].pvc_uid.clone()),
        );
        tokio::time::timeout(
            rpc_remaining(deadline, "public initialization execution")?,
            client.execute(authorized(wire::ExecuteCommandRequest {
                protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                resource_uid: run.resource_uid.clone(),
                target: Some(identities[index].clone().into()),
                expected_process_session_id: sessions[index].clone(),
                command: Some(
                    wire::execute_command_request::Command::InitializeAgentStore(
                        wire::InitializeAgentStoreCommand {
                            initialization_id: initialization.to_string(),
                            resource_uid: run.resource_uid.clone(),
                            local_replica_id: identities[index].replica_id.value(),
                            expected_instance_id: identities[index].instance_id.to_string(),
                            expected_pod_uid: run.kuberic_members[index].pod_uid.clone(),
                            expected_pvc_uid: run.kuberic_members[index].pvc_uid.clone(),
                            assigned_agent_generation: identities[index]
                                .agent_generation
                                .to_string(),
                            effective_policy: Some(policy.clone().into()),
                            bootstrap_configuration: Some(configuration.clone().into()),
                            provisioning: None,
                        },
                    ),
                ),
            })),
        )
        .await
        .map_err(|_| MssqlGroupError::new("public initialization exceeded its deadline"))?
        .map_err(display_error)?;
        Ok::<(), MssqlGroupError>(())
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn ensure_configuration(
    pods: &[PublicMssqlPod; 3],
    resource_uid: &ResourceUid,
    configuration: &ConfigurationDescriptor,
    timeout_duration: Duration,
    operation_namespace: &str,
    existing: bool,
) -> Result<[kuberic_runtime::protocol::types::ProcessSessionId; 3], MssqlGroupError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let results = join_all((0..3).map(|index| {
        let operation_id = format!("{operation_namespace}-open-{}", index + 1);
        async move {
            execute_configuration_operation(
                pods,
                resource_uid,
                configuration,
                deadline,
                index,
                &operation_id,
                ConfigurationOperation::Open { existing },
            )
            .await
        }
    }))
    .await;
    for result in results {
        result?;
    }
    if existing {
        return public_reports(
            pods,
            rpc_remaining(deadline, "public contained reopen completion")?,
        )
        .await?
        .map(|report| report.process_session_id)
        .into_iter()
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three agent sessions required"));
    }

    let results = join_all((1..3).map(|index| {
        let operation_id = format!("{operation_namespace}-secondary-role-{}", index + 1);
        async move {
            execute_configuration_operation(
                pods,
                resource_uid,
                configuration,
                deadline,
                index,
                &operation_id,
                ConfigurationOperation::ActiveSecondaryRole,
            )
            .await
        }
    }))
    .await;
    for result in results {
        result?;
    }
    wait_for_active_secondaries(pods, deadline, operation_namespace, configuration.epoch).await?;

    execute_configuration_operation(
        pods,
        resource_uid,
        configuration,
        deadline,
        0,
        &format!("{operation_namespace}-initial-primary"),
        ConfigurationOperation::Lifecycle {
            secondary_role_namespace: Some(operation_namespace),
        },
    )
    .await?;

    let results = join_all((1..3).map(|index| {
        let operation_id = format!("{operation_namespace}-secondary-lifecycle-{}", index + 1);
        async move {
            execute_configuration_operation(
                pods,
                resource_uid,
                configuration,
                deadline,
                index,
                &operation_id,
                ConfigurationOperation::Lifecycle {
                    secondary_role_namespace: Some(operation_namespace),
                },
            )
            .await
        }
    }))
    .await;
    for result in results {
        result?;
    }

    wait_for_configured_secondaries(pods, deadline)
        .await?
        .map(|report| report.process_session_id)
        .into_iter()
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three agent sessions required"))
}

#[derive(Clone, Copy)]
enum ConfigurationOperation<'a> {
    Open {
        existing: bool,
    },
    ActiveSecondaryRole,
    CurrentProgress,
    Lifecycle {
        secondary_role_namespace: Option<&'a str>,
    },
}

async fn execute_configuration_operation(
    pods: &[PublicMssqlPod; 3],
    resource_uid: &ResourceUid,
    configuration: &ConfigurationDescriptor,
    deadline: tokio::time::Instant,
    local_index: usize,
    operation_id: &str,
    operation: ConfigurationOperation<'_>,
) -> Result<(), MssqlGroupError> {
    loop {
        let mut client = None;
        let mut reports = Vec::new();
        for (index, pod) in pods.iter().enumerate() {
            let (candidate, wire_report) =
                status(pod, rpc_remaining(deadline, "public configuration status")?).await?;
            if index == local_index {
                client = Some(candidate);
            }
            reports.push(exact_public_report(pod, &wire_report)?);
        }
        let reports: [PublicV1AgentReport; 3] = reports
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three public reports required"))?;
        let session = reports[local_index].process_session_id.clone();
        let request = match operation {
            ConfigurationOperation::Open { existing } => {
                program_request(
                    pods,
                    &reports,
                    local_index,
                    resource_uid,
                    &session,
                    operation_id,
                    PublicOperationProgram::Open {
                        replica: pods[local_index].identity.clone(),
                        existing,
                    },
                )
                .await?
            }
            ConfigurationOperation::ActiveSecondaryRole => {
                program_request(
                    pods,
                    &reports,
                    local_index,
                    resource_uid,
                    &session,
                    operation_id,
                    PublicOperationProgram::Role {
                        epoch: configuration.epoch,
                        role: ReplicaRole::ActiveSecondary,
                    },
                )
                .await?
            }
            ConfigurationOperation::CurrentProgress => {
                program_request(
                    pods,
                    &reports,
                    local_index,
                    resource_uid,
                    &session,
                    operation_id,
                    PublicOperationProgram::Progress { capability: false },
                )
                .await?
            }
            ConfigurationOperation::Lifecycle {
                secondary_role_namespace,
            } => {
                configuration_request(
                    pods,
                    &reports,
                    local_index,
                    resource_uid,
                    configuration,
                    LifecycleRequestContext {
                        session: &session,
                        operation_id,
                        secondary_role_namespace,
                    },
                )
                .await?
            }
        };
        let mut client = client
            .ok_or_else(|| MssqlGroupError::new("public configuration client is unavailable"))?;
        match tokio::time::timeout(
            rpc_remaining(deadline, "public configuration execution")?,
            client.execute(request),
        )
        .await
        {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(error)) if error.code() == tonic::Code::Cancelled => {}
            Ok(Err(error)) => {
                return Err(MssqlGroupError::new(format!(
                    "public configuration operation {operation_id} failed: {error}"
                )));
            }

            Err(_) => {
                return Err(MssqlGroupError::new(
                    "public configuration execution exceeded its deadline",
                ));
            }
        }
        tokio::time::sleep(
            rpc_remaining(deadline, "public configuration retry")?.min(Duration::from_millis(20)),
        )
        .await;
    }
}

async fn refresh_current_progress(
    pods: &[PublicMssqlPod; 3],
    resource_uid: &ResourceUid,
    configuration: &ConfigurationDescriptor,
    operation_timeout: Duration,
    complete_deadline: Instant,
    operation_namespace: &str,
) -> Result<(), MssqlGroupError> {
    for pod in pods {
        let replicator = pod
            .application
            .testing_replicator()
            .ok_or_else(|| MssqlGroupError::new("public progress Replicator is unavailable"))?;
        let progress = replicator.current_progress().await.map_err(display_error)?;
        let observed = pod.source.observe().await.map_err(display_error)?;
        let observed = match observed {
            Observation::Present { value, .. } => value,
            Observation::Absent { .. } => {
                return Err(MssqlGroupError::new(
                    "public progress observation lacks the availability group",
                ));
            }
            Observation::Failed(failure) => {
                return Err(MssqlGroupError::new(format!(
                    "public progress observation failed: {:?}",
                    failure.kind
                )));
            }
        };
        let group = match observed.availability_group {
            Observation::Present { value, .. } => value,
            Observation::Absent { .. } => {
                return Err(MssqlGroupError::new(
                    "public progress observation lacks native group state",
                ));
            }
            Observation::Failed(failure) => {
                return Err(MssqlGroupError::new(format!(
                    "public progress native group observation failed: {:?}",
                    failure.kind
                )));
            }
        };
        if progress != group.configuration_sequence.value() {
            return Err(MssqlGroupError::new(format!(
                "member {} Public V1 progress {progress} differs from native AG sequence {}",
                pod.ordinal,
                group.configuration_sequence.value()
            )));
        }
    }
    let deadline = tokio::time::Instant::now()
        + operation_timeout.min(remaining(complete_deadline, "public progress refresh")?);
    let results = join_all((0..3).map(|index| {
        let operation_id = format!("{operation_namespace}-current-progress-{}", index + 1);
        async move {
            execute_configuration_operation(
                pods,
                resource_uid,
                configuration,
                deadline,
                index,
                &operation_id,
                ConfigurationOperation::CurrentProgress,
            )
            .await
        }
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn wait_for_active_secondaries(
    pods: &[PublicMssqlPod; 3],
    deadline: tokio::time::Instant,
    operation_namespace: &str,
    epoch: Epoch,
) -> Result<[PublicV1AgentReport; 3], MssqlGroupError> {
    loop {
        let reports = public_reports(
            pods,
            rpc_remaining(deadline, "public secondary role publication")?,
        )
        .await?;
        if reports[1..].iter().enumerate().all(|(offset, report)| {
            active_secondary_role_visible(
                report,
                &format!("{operation_namespace}-secondary-role-{}", offset + 2),
                epoch,
            )
        }) {
            return Ok(reports);
        }
        tokio::time::sleep(
            rpc_remaining(deadline, "public secondary role retry")?.min(Duration::from_millis(20)),
        )
        .await;
    }
}

async fn wait_for_configured_secondaries(
    pods: &[PublicMssqlPod; 3],
    deadline: tokio::time::Instant,
) -> Result<[PublicV1AgentReport; 3], MssqlGroupError> {
    loop {
        let reports = public_reports(
            pods,
            rpc_remaining(deadline, "public configuration completion")?,
        )
        .await?;
        if reports[1..]
            .iter()
            .all(|report| report.role == ReplicaRole::ActiveSecondary)
        {
            return Ok(reports);
        }
        tokio::time::sleep(
            rpc_remaining(deadline, "public configuration completion retry")?
                .min(Duration::from_millis(20)),
        )
        .await;
    }
}

async fn program_request(
    pods: &[PublicMssqlPod; 3],
    reports: &[PublicV1AgentReport; 3],
    local_index: usize,
    resource_uid: &ResourceUid,
    session: &ProcessSessionId,
    operation_id: &str,
    program: PublicOperationProgram,
) -> Result<Request<wire::ExecuteCommandRequest>, MssqlGroupError> {
    let report = reports
        .get(local_index)
        .ok_or_else(|| MssqlGroupError::new("public operation target index is invalid"))?;
    let local = pods
        .get(local_index)
        .ok_or_else(|| MssqlGroupError::new("public operation pod index is invalid"))?;
    let input_digest = program.digest();
    let replay_disposition = program.replay_disposition();
    let operation_id = OperationId::new(format!("{operation_id}-session-{session}"));
    let revision = operation_revision(
        local,
        report,
        &operation_id,
        &input_digest,
        replay_disposition,
        session,
    )
    .await?;
    let command = PublicV1OperationCommand {
        target: local.identity.clone(),
        intent: PublicOperationIntent {
            identity: report.identity.clone(),
            operation_id,
            revision,
            process_session_id: session.clone(),
            class: PublicOperationClass::Authority,
            input_digest,
            replay_disposition,
            lifecycle: None,
            program: Some(program),
        },
    };
    public_operation_request(resource_uid, local, session, command)
}

struct LifecycleRequestContext<'a> {
    session: &'a ProcessSessionId,
    operation_id: &'a str,
    secondary_role_namespace: Option<&'a str>,
}

async fn configuration_request(
    pods: &[PublicMssqlPod; 3],
    reports: &[PublicV1AgentReport; 3],
    local_index: usize,
    resource_uid: &ResourceUid,
    configuration: &ConfigurationDescriptor,
    context: LifecycleRequestContext<'_>,
) -> Result<Request<wire::ExecuteCommandRequest>, MssqlGroupError> {
    let LifecycleRequestContext {
        session,
        operation_id,
        secondary_role_namespace,
    } = context;
    let report = reports
        .get(local_index)
        .ok_or_else(|| MssqlGroupError::new("public configuration target index is invalid"))?;
    let local = pods
        .get(local_index)
        .ok_or_else(|| MssqlGroupError::new("public configuration pod index is invalid"))?;
    let role = configuration
        .members
        .iter()
        .find(|member| member.identity == local.identity)
        .map(|member| member.role)
        .ok_or_else(|| {
            MssqlGroupError::new("public configuration lacks the exact local replica")
        })?;
    let recipe = match role {
        ReplicaRole::Primary => PublicLifecycleRecipe::InitialPrimary,
        ReplicaRole::ActiveSecondary => PublicLifecycleRecipe::SecondaryEpochAdvance,
        _ => {
            return Err(MssqlGroupError::new(
                "public lifecycle target is not a ready primary or secondary",
            ));
        }
    };
    let lifecycle = PublicLifecycleInput {
        recipe,
        replica: local.identity.clone(),
        epoch: configuration.epoch,
        possible_data_loss: PossibleDataLossIntent::NotPossible,
        current: configuration.clone(),
        previous: None,
        replicas: ready_remote_secondaries(
            pods,
            reports,
            local_index,
            configuration,
            secondary_role_namespace,
        )?,
        data_loss_recovery: None,
    };
    let input_digest = lifecycle.digest();
    let replay_disposition = PublicOperationReplayDisposition::Convergent;
    let operation_id = OperationId::new(format!("{operation_id}-session-{session}"));
    let revision = operation_revision(
        local,
        report,
        &operation_id,
        &input_digest,
        replay_disposition,
        session,
    )
    .await?;
    let command = PublicV1OperationCommand {
        target: local.identity.clone(),
        intent: PublicOperationIntent {
            identity: report.identity.clone(),
            operation_id,
            revision,
            process_session_id: session.clone(),
            class: PublicOperationClass::Authority,
            input_digest,
            replay_disposition,
            lifecycle: Some(lifecycle),
            program: None,
        },
    };
    public_operation_request(resource_uid, local, session, command)
}

fn public_operation_request(
    resource_uid: &ResourceUid,
    pod: &PublicMssqlPod,
    session: &ProcessSessionId,
    command: PublicV1OperationCommand,
) -> Result<Request<wire::ExecuteCommandRequest>, MssqlGroupError> {
    let command = public_v1_operation_command_to_proto(&command).map_err(display_error)?;
    Ok(authorized(wire::ExecuteCommandRequest {
        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        resource_uid: resource_uid.to_string(),
        target: Some(pod.identity.clone().into()),
        expected_process_session_id: session.to_string(),
        command: Some(wire::execute_command_request::Command::PublicV1Operation(
            command,
        )),
    }))
}

async fn operation_revision(
    pod: &PublicMssqlPod,
    report: &PublicV1AgentReport,
    operation_id: &OperationId,
    input_digest: &kuberic_runtime::protocol::public_operations::CanonicalOperationInputDigest,
    replay_disposition: PublicOperationReplayDisposition,
    session: &ProcessSessionId,
) -> Result<u64, MssqlGroupError> {
    if let Some(completion) = report.completed_operations.iter().find(|completion| {
        completion.operation_id == *operation_id
            && completion.process_session_id == *session
            && completion.input_digest == *input_digest
            && completion.replay_disposition == replay_disposition
            && completion.history == report.history
            && completion.storage_incarnation == report.storage_incarnation
            && completion.result == PublicV1OperationResult::Succeeded
    }) {
        return Ok(completion.revision);
    }
    let handle = pod
        .running
        .as_ref()
        .ok_or_else(|| MssqlGroupError::new("public operation runtime is unavailable"))?
        .handle();
    if let Some(intent) = handle
        .public_v1_operation_intent(operation_id)
        .await
        .map_err(display_error)?
    {
        if intent.process_session_id != *session
            || intent.input_digest != *input_digest
            || intent.replay_disposition != replay_disposition
            || intent.class != PublicOperationClass::Authority
        {
            return Err(MssqlGroupError::new(
                "durable public operation changed exact authority",
            ));
        }
        return Ok(intent.revision);
    }
    handle
        .public_v1_next_operation_revision()
        .await
        .map_err(display_error)
}

fn ready_remote_secondaries(
    pods: &[PublicMssqlPod; 3],
    reports: &[PublicV1AgentReport; 3],
    local_index: usize,
    configuration: &ConfigurationDescriptor,
    secondary_role_namespace: Option<&str>,
) -> Result<Vec<PublicReplicaDescriptor>, MssqlGroupError> {
    configuration
        .members
        .iter()
        .filter(|member| {
            member.role == ReplicaRole::ActiveSecondary
                && member.identity.replica_id != pods[local_index].identity.replica_id
        })
        .map(|member| {
            let index = pods
                .iter()
                .position(|pod| pod.identity == member.identity)
                .ok_or_else(|| {
                    MssqlGroupError::new(
                        "public configuration secondary has no exact local fixture pod",
                    )
                })?;
            let pod = &pods[index];
            let report = &reports[index];
            let role_visible = report.role == ReplicaRole::ActiveSecondary
                || secondary_role_namespace.is_some_and(|namespace| {
                    active_secondary_role_visible(
                        report,
                        &format!("{namespace}-secondary-role-{}", index + 1),
                        configuration.epoch,
                    )
                });
            if !role_visible {
                return Err(MssqlGroupError::new(format!(
                    "member {} has no visible active-secondary authority",
                    pod.ordinal
                )));
            }
            public_replica_descriptor(pod, report, ReplicaRole::ActiveSecondary)
        })
        .collect()
}

fn public_replica_descriptor(
    pod: &PublicMssqlPod,
    report: &PublicV1AgentReport,
    role: ReplicaRole,
) -> Result<PublicReplicaDescriptor, MssqlGroupError> {
    Ok(PublicReplicaDescriptor {
        build_id: OperationId::default(),
        identity: pod.identity.clone(),
        replication_address: format!("http://{}", pod.replication_address),
        process_session_id: report.process_session_id.clone(),
        storage_incarnation: Some(exact_storage_incarnation(pod, report)?),
        role,
        current_progress: report.progress.last,
        catch_up_capability: report.progress.first,
        must_catchup: false,
    })
}

fn active_secondary_role_visible(
    report: &PublicV1AgentReport,
    operation_id: &str,
    epoch: Epoch,
) -> bool {
    let operation_id = format!("{operation_id}-session-{}", report.process_session_id);
    let program = PublicOperationProgram::Role {
        epoch,
        role: ReplicaRole::ActiveSecondary,
    };
    let input_digest = program.digest();
    let replay_disposition = program.replay_disposition();
    report.completed_operations.iter().any(|completion| {
        completion.operation_id.as_str() == operation_id
            && completion.process_session_id == report.process_session_id
            && completion.input_digest == input_digest
            && completion.replay_disposition == replay_disposition
            && completion.history == report.history
            && completion.storage_incarnation == report.storage_incarnation
            && completion.call_kind == PublicV1CallKind::Role
            && completion.result == PublicV1OperationResult::Succeeded
    })
}

async fn public_reports(
    pods: &[PublicMssqlPod; 3],
    timeout_duration: Duration,
) -> Result<[PublicV1AgentReport; 3], MssqlGroupError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut reports = Vec::new();
    for pod in pods {
        let (_, report) = status(pod, rpc_remaining(deadline, "public report collection")?).await?;
        reports.push(exact_public_report(pod, &report)?);
    }
    reports
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three public reports required"))
}

fn exact_public_report(
    pod: &PublicMssqlPod,
    report: &wire::AgentStatusReport,
) -> Result<PublicV1AgentReport, MssqlGroupError> {
    let mut public =
        normalize_public_v1_agent_status_report(report.clone()).map_err(display_error)?;
    if public.resource_uid != pod.resource_uid || public.replica != pod.identity {
        return Err(MssqlGroupError::new(
            "public report differs from the exact fixture identity",
        ));
    }
    public.storage_incarnation = exact_storage_incarnation(pod, &public)?;
    Ok(public)
}

fn exact_storage_incarnation(
    pod: &PublicMssqlPod,
    report: &PublicV1AgentReport,
) -> Result<ReplicaStorageIncarnationContext, MssqlGroupError> {
    if report.storage_incarnation.replica != pod.identity {
        return Err(MssqlGroupError::new(
            "public report storage differs from the exact replica identity",
        ));
    }
    match &report.storage_incarnation.storage {
        ReplicaStorageIdentity::Persistent { pod_uid, pvc_uid }
            if pod_uid == &pod.pod_uid && pvc_uid == &pod.pvc_uid => {}
        _ => {
            return Err(MssqlGroupError::new(
                "public report storage differs from the known Pod/PVC identity",
            ));
        }
    }
    Ok(ReplicaStorageIncarnationContext {
        replica: pod.identity.clone(),
        storage: ReplicaStorageIdentity::Persistent {
            pod_uid: pod.pod_uid.clone(),
            pvc_uid: pod.pvc_uid.clone(),
        },
        storage_generation: report.storage_incarnation.storage_generation.clone(),
    })
}

async fn status(
    pod: &PublicMssqlPod,
    timeout_duration: Duration,
) -> Result<(AgentControlClient<Channel>, wire::AgentStatusReport), MssqlGroupError> {
    status_at(
        pod.control_address,
        &pod.resource_uid,
        &pod.identity,
        timeout_duration,
    )
    .await
}

async fn status_at(
    control_address: SocketAddr,
    resource_uid: &ResourceUid,
    identity: &ReplicaIdentity,
    timeout_duration: Duration,
) -> Result<(AgentControlClient<Channel>, wire::AgentStatusReport), MssqlGroupError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = rpc_remaining(deadline, "public agent connection")?;
        match tokio::time::timeout(
            remaining,
            AgentControlClient::connect(format!("http://{control_address}")),
        )
        .await
        {
            Ok(Ok(mut client)) => {
                let request = authorized(wire::GetAgentStatusRequest {
                    protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                    resource_uid: resource_uid.to_string(),
                    replica_id: identity.replica_id.value(),
                    expected_instance_id: identity.instance_id.to_string(),
                });
                match tokio::time::timeout(
                    rpc_remaining(deadline, "public agent status")?,
                    client.get_status(request),
                )
                .await
                {
                    Ok(Ok(report)) => return Ok((client, report.into_inner())),
                    Ok(Err(_)) => {}
                    Err(_) => {
                        return Err(MssqlGroupError::new(
                            "public agent status exceeded its deadline",
                        ));
                    }
                }
            }
            Ok(Err(_)) => {}
            Err(_) => {
                return Err(MssqlGroupError::new(
                    "public agent connection exceeded its deadline",
                ));
            }
        }
        tokio::time::sleep(
            rpc_remaining(deadline, "public agent status retry")?.min(Duration::from_millis(20)),
        )
        .await;
    }
}

fn rpc_remaining(deadline: tokio::time::Instant, stage: &str) -> Result<Duration, MssqlGroupError> {
    deadline
        .checked_duration_since(tokio::time::Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| MssqlGroupError::new(format!("{stage} exceeded its deadline")))
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {AGENT_TOKEN}")
            .parse()
            .expect("token metadata"),
    );
    request
}

async fn observe_sources(
    pods: &[PublicMssqlPod; 3],
    operation_timeout: Duration,
    complete_deadline: Instant,
) -> Result<[InstanceSnapshot; 3], MssqlGroupError> {
    let remaining = remaining(complete_deadline, "direct observation")?;
    let values = join_all(pods.iter().map(|pod| async move {
        timeout(
            operation_timeout.min(remaining),
            "direct SQL observation",
            pod.source.observe(),
        )
        .await?
        .map_err(display_error)
        .and_then(|observation| match observation {
            Observation::Present { value, .. } => Ok(value),
            Observation::Absent { .. } => Err(MssqlGroupError::new("availability group is absent")),
            Observation::Failed(failure) => Err(MssqlGroupError::new(format!(
                "direct SQL observation failed: {:?}",
                failure.kind
            ))),
        })
    }))
    .await;
    values
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three direct observations required"))
}

async fn validate_observations(
    pods: &[PublicMssqlPod; 3],
    native: &NativeTopologyBinding,
    operation_timeout: Duration,
    complete_deadline: Instant,
) -> Result<(), MssqlGroupError> {
    let snapshots = observe_sources(pods, operation_timeout, complete_deadline).await?;
    validate_snapshots(&snapshots, native)
}

fn validate_snapshots(
    snapshots: &[InstanceSnapshot; 3],
    native: &NativeTopologyBinding,
) -> Result<(), MssqlGroupError> {
    for (index, snapshot) in snapshots.iter().enumerate() {
        let group = match &snapshot.availability_group {
            Observation::Present { value, .. } => value,
            _ => return Err(MssqlGroupError::new("native snapshot is unavailable")),
        };
        let member = &native.members[index];
        let expected_role = if member.role == "PRIMARY" {
            NativeRole::Primary
        } else {
            NativeRole::Secondary
        };
        if snapshot.instance.server_name.as_str() != member.server_name
            || snapshot.instance.sqlserver_start_time != member.sql_start_time
            || group.identity.group_id.as_str() != native.availability_group_id
            || group.configuration_sequence.value() != native.configuration_sequence
            || group
                .local_replica
                .identity
                .native_replica_id()
                .map(|id| id.as_str())
                != Some(member.native_replica_id.as_str())
            || group.local_replica.role.as_ref() != Some(&expected_role)
        {
            return Err(MssqlGroupError::new(
                "direct observation differs from the frozen native topology",
            ));
        }
    }
    Ok(())
}

fn validate_bootstrap_snapshots(snapshots: &[InstanceSnapshot; 3]) -> Result<(), MssqlGroupError> {
    let mut group_id = None;
    let mut primary_count = 0;
    for snapshot in snapshots {
        let group = match &snapshot.availability_group {
            Observation::Present { value, .. } => value,
            _ => {
                return Err(MssqlGroupError::new(
                    "bootstrap availability group is unavailable",
                ));
            }
        };
        if group.replicas.len() != 3
            || group.databases.len() > 1
            || !group.cluster_type.eq_ignore_ascii_case("EXTERNAL")
            || group.required_synchronized_secondaries_to_commit != 1
            || group.basic_features
            || group.is_distributed
            || group.replicas.iter().any(|replica| {
                replica.availability_mode != "SYNCHRONOUS_COMMIT"
                    || replica.failover_mode != "EXTERNAL"
                    || replica.seeding_mode != "AUTOMATIC"
            })
        {
            return Err(MssqlGroupError::new(
                "bootstrap availability-group profile differs",
            ));
        }
        if group_id.get_or_insert_with(|| group.identity.group_id.clone())
            != &group.identity.group_id
        {
            return Err(MssqlGroupError::new(
                "bootstrap members do not report one native group identity",
            ));
        }
        let expected_role = match group.local_replica.role {
            Some(NativeRole::Primary) => {
                primary_count += 1;
                NativeRole::Primary
            }
            Some(NativeRole::Secondary) => NativeRole::Secondary,
            _ => {
                return Err(MssqlGroupError::new(
                    "bootstrap local replica role is not stable",
                ));
            }
        };
        let local_id = group
            .local_replica
            .identity
            .native_replica_id()
            .ok_or_else(|| MssqlGroupError::new("bootstrap local replica ID is unavailable"))?;
        let local = group
            .replicas
            .iter()
            .find(|replica| &replica.replica_id == local_id)
            .ok_or_else(|| MssqlGroupError::new("bootstrap local member is absent"))?;
        if local.state.as_ref().is_none_or(|state| {
            state.role.as_ref() != Some(&expected_role)
                || state.operational_state.as_deref() != Some("ONLINE")
                || state.connected_state.as_deref() != Some("CONNECTED")
        }) {
            return Err(MssqlGroupError::new(
                "bootstrap local member is not online and connected",
            ));
        }
    }
    if primary_count != 1 {
        return Err(MssqlGroupError::new(
            "bootstrap topology does not contain exactly one primary",
        ));
    }
    Ok(())
}

async fn cancel_attempts(
    attempts: &mut [HostAttempt],
    pods: &mut [PublicMssqlPod; 3],
    cleanup: &CleanupCoordinator<impl CleanupClock>,
) -> Result<(), MssqlGroupError> {
    for attempt in attempts.iter() {
        attempt.shutdown.send_replace(true);
    }
    for (index, attempt) in attempts.iter_mut().enumerate() {
        let Some(mut task) = attempt.task.take() else {
            continue;
        };
        let deadline = cleanup.remaining().min(Duration::from_secs(10));
        match tokio::time::timeout(deadline, &mut task).await {
            Ok(Ok(Ok(Some(mut running)))) => {
                running.shutdown();
                tokio::time::timeout(deadline, running.wait())
                    .await
                    .map_err(|_| {
                        MssqlGroupError::termination_unconfirmed("public host shutdown timed out")
                    })?
                    .map_err(display_error)?;
            }
            Ok(Ok(Ok(None))) => {}
            Ok(Ok(Err(HostError::Runtime(KubericRuntimeError::OperationCancelled)))) => {}
            Ok(Ok(Err(error))) => return Err(display_error(error)),
            Ok(Err(error)) => return Err(display_error(error)),
            Err(_) => {
                pods[index].application.abort();
                attempt.task = Some(task);
                return Err(MssqlGroupError::termination_unconfirmed(format!(
                    "member {} startup cancellation timed out",
                    index + 1
                )));
            }
        }
        pods[index].running = None;
    }
    Ok(())
}

fn free_address() -> Result<SocketAddr, MssqlGroupError> {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map_err(display_error)
}

fn create_private_directory(path: &Path) -> Result<(), MssqlGroupError> {
    if !path.exists() {
        fs::create_dir_all(path).map_err(display_error)?;
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(display_error)
}

fn write_private(path: &Path, bytes: impl AsRef<[u8]>) -> Result<(), MssqlGroupError> {
    fs::write(path, bytes).map_err(display_error)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(display_error)
}

fn remaining(deadline: Instant, stage: &str) -> Result<Duration, MssqlGroupError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| MssqlGroupError::new(format!("{stage} exceeded its deadline")))
}

fn check_deadline(deadline: Instant, stage: &str) -> Result<(), MssqlGroupError> {
    remaining(deadline, stage).map(|_| ())
}

async fn timeout<T>(
    duration: Duration,
    stage: &str,
    future: impl std::future::Future<Output = T>,
) -> Result<T, MssqlGroupError> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| MssqlGroupError::new(format!("{stage} timed out")))
}

fn display_error(error: impl std::fmt::Display) -> MssqlGroupError {
    MssqlGroupError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use kuberic_mssql::runtime_host::SeedDispatchTarget;
    use kuberic_runtime::protocol::types::AgentGeneration;

    use super::*;

    fn testing_identities() -> [ReplicaIdentity; 3] {
        std::array::from_fn(|index| ReplicaIdentity {
            replica_id: ReplicaId::new(index as i64 + 1),
            instance_id: ReplicaInstanceId::new(format!("pod-{}", index + 1)),
            agent_generation: AgentGeneration::new(format!("generation-{}", index + 1)),
        })
    }

    fn testing_authority(
        identity: &ReplicaIdentity,
        build_id: &str,
        session: &str,
    ) -> PublicTargetEvidenceAuthority {
        PublicTargetEvidenceAuthority {
            build_id: build_id.to_owned(),
            identity: identity.clone(),
            process_session_id: ProcessSessionId::new(session),
        }
    }

    fn dispatched_record(
        identities: &[ReplicaIdentity; 3],
        winning_target: usize,
        build_id: &str,
        sessions: [&str; 3],
    ) -> SeedDispatchRecord {
        let member = |index: usize| {
            SeedDispatchTarget::new(
                identities[index].replica_id.to_string(),
                identities[index].instance_id.to_string(),
                identities[index].agent_generation.to_string(),
                sessions[index],
                format!("00000000-0000-4000-8000-00000000000{}", index + 1),
            )
            .unwrap()
        };
        let mut record = SeedDispatchRecord::new(
            "resource",
            identities[0].clone(),
            "configuration",
            "availability-group",
            "00000000-0000-4000-8000-000000000010",
            "database",
            "00000000-0000-4000-8000-000000000011",
            "00000000-0000-4000-8000-000000000012",
            "00000000-0000-4000-8000-000000000013",
            member(0),
            [member(1), member(2)],
            identities[winning_target].replica_id.to_string(),
            build_id,
            ReplicaStorageIncarnationContext {
                replica: identities[winning_target].clone(),
                storage: ReplicaStorageIdentity::Volatile {
                    identity: format!("storage-{winning_target}"),
                },
                storage_generation: StorageGeneration::new(format!(
                    "storage-generation-{winning_target}"
                )),
            },
            "2026-10-16T09:49:30Z",
        )
        .unwrap();
        record.mark_submitting().unwrap();
        record
    }

    #[test]
    fn identical_current_target_authorization_is_idempotent() {
        let identities = testing_identities();
        let authority = testing_authority(&identities[1], "build-a", "session-a");
        let mut state = PublicTargetEvidenceAuthorityState::new(3);

        state.authorize(1, authority.clone()).unwrap();
        state.authorize(1, authority.clone()).unwrap();

        assert_eq!(state.current[1].as_ref(), Some(&authority));
        assert!(state.retired_builds.is_empty());
        assert_eq!(
            state.exact_request(
                1,
                "build-a",
                &identities[1],
                &ProcessSessionId::new("session-a"),
            ),
            Some(authority)
        );
        assert!(
            state
                .exact_request(
                    1,
                    "build-a",
                    &identities[1],
                    &ProcessSessionId::new("different-session"),
                )
                .is_none()
        );
    }

    #[test]
    fn target_authority_supersession_retires_the_previous_exact_build() {
        let identities = testing_identities();
        let authority_a = testing_authority(&identities[1], "build-a", "session-a");
        let authority_b = testing_authority(&identities[1], "build-b", "session-a");
        let mut state = PublicTargetEvidenceAuthorityState::new(3);

        state.authorize(1, authority_a.clone()).unwrap();
        state.authorize(1, authority_b.clone()).unwrap();

        assert_eq!(state.current[1].as_ref(), Some(&authority_b));
        assert!(!state.exact(1, &authority_a));
        assert!(state.retired_builds.contains("build-a"));
        assert!(state.authorize(1, authority_a).is_err());
    }

    #[tokio::test]
    async fn revoke_while_collection_is_blocked_invalidates_revalidation() {
        let identities = testing_identities();
        let authority = testing_authority(&identities[1], "build-a", "session-a");
        let state = Arc::new(Mutex::new(PublicTargetEvidenceAuthorityState::new(3)));
        state
            .lock()
            .unwrap()
            .authorize(1, authority.clone())
            .unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let blocked_state = state.clone();
        let blocked_authority = authority.clone();
        let collection = tokio::spawn(async move {
            assert!(blocked_state.lock().unwrap().exact(1, &blocked_authority));
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            blocked_state.lock().unwrap().exact(1, &blocked_authority)
        });

        entered_rx.await.unwrap();
        state.lock().unwrap().revoke(1, "build-a").unwrap();
        release_tx.send(()).unwrap();

        assert!(!collection.await.unwrap());
        assert!(state.lock().unwrap().retired_builds.contains("build-a"));
    }

    #[test]
    fn dispatched_winner_retention_comes_from_the_seed_dispatch_record() {
        let identities = testing_identities();
        let winner = testing_authority(&identities[1], "build-a", "session-a");
        let replacement = testing_authority(&identities[1], "build-b", "session-a");
        let record = dispatched_record(
            &identities,
            1,
            "build-a",
            ["session-source", "session-a", "session-peer"],
        );
        let retained = dispatched_authority(&record, &identities)
            .unwrap()
            .expect("submitting dispatch must retain exact winning authority");
        let mut state = PublicTargetEvidenceAuthorityState::new(3);
        state.authorize(1, winner.clone()).unwrap();
        state.retain_dispatch(retained);

        state.authorize(1, replacement.clone()).unwrap();

        assert_eq!(state.current[1].as_ref(), Some(&replacement));
        assert!(state.exact(1, &winner));
        assert!(state.retired_builds.contains("build-a"));
    }

    #[tokio::test]
    async fn stalled_public_control_connection_is_deadline_bounded() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stalled = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("pod-1"),
            agent_generation: kuberic_runtime::protocol::types::AgentGeneration::new(
                "generation-1",
            ),
        };
        let started = tokio::time::Instant::now();
        let error = status_at(
            address,
            &ResourceUid::new("resource"),
            &identity,
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("deadline"));
        assert!(started.elapsed() < Duration::from_millis(500));
        stalled.abort();
    }

    #[test]
    fn public_live_group_uses_no_kuberic_testing_or_private_host_api() {
        let source = include_str!("public_kuberic_group.rs");
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
}
