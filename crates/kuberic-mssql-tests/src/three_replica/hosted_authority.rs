//! Fixture-only association of native samples with an exclusively owned host command stream.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use kuberic_mssql::kuberic::{HealthyTopologyBinding, ObservationClock, SqlServerReplicator};
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::{Guid, Observation, SqlIdentifier};
use kuberic_runtime::control::{normalize_execute_request, proto};
use kuberic_runtime::protocol::command::ProtocolCommand;
use kuberic_runtime::protocol::public_operations::{
    PublicConfiguration, PublicLifecycleRecipe, PublicOperationIntent, PublicOperationProgram,
    PublicReplicaDescriptor, PublicV1AgentReport, PublicV1CallKind, PublicV1OperationCompletion,
    PublicV1OperationResult,
};
use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ConfigurationMember, Epoch, ReplicaIdentity, ReplicaRole,
};
use kuberic_runtime::replicator::{PrimaryReplicator, ReplicaSetConfiguration};
use serde::Serialize;

use super::ReadyMember;
use super::hosted_native_prefix::{NativePrefixDecision, NativePrefixEvidence};

pub type DiagnosticResult<T> = Result<T, String>;

#[derive(Clone, Debug, Serialize)]
pub struct AcceptedCommand {
    pub target_index: usize,
    pub input: PublicOperationIntent,
    pub completion: PublicV1OperationCompletion,
}

#[derive(Debug, Serialize)]
pub struct HostedAssociation {
    pub scope: &'static str,
    pub native_input: &'static str,
    pub authority_boundary: &'static str,
    pub secondary_native_binding: &'static str,
    pub accepted_configuration: Vec<AcceptedCommand>,
    pub accepted_advances: Vec<AcceptedCommand>,
    pub admitted_reports: Vec<PublicV1AgentReport>,
    pub final_reports: Vec<PublicV1AgentReport>,
    pub native_database_anchors: Vec<NativeDatabaseAnchor>,
    pub native: NativePrefixEvidence,
    pub decision: NativePrefixDecision,
    pub public_coverage: BlockedPublicCoverage,
}

#[derive(Clone, Copy)]
pub enum NativeInput {
    Scripted,
    LiveQuietFixture,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NativeDatabaseAnchor {
    pub replica: ReplicaIdentity,
    pub database_name: SqlIdentifier,
    pub group_database_id: Guid,
    pub local_database_id: u32,
    pub database_guid: Guid,
    pub family_guid: Guid,
    pub recovery_fork_id: Guid,
}

impl NativeDatabaseAnchor {
    pub fn from_snapshot(
        replica: ReplicaIdentity,
        snapshot: &InstanceSnapshot,
    ) -> DiagnosticResult<Self> {
        let Observation::Present { value: group, .. } = &snapshot.availability_group else {
            return Err("independent native database anchor lacks availability group".into());
        };
        let [database] = group.databases.as_slice() else {
            return Err("independent native database anchor requires one database".into());
        };
        let local = database
            .local
            .as_ref()
            .ok_or("independent local database unavailable")?;
        let recovery = local
            .recovery
            .as_ref()
            .ok_or("independent recovery metadata unavailable")?;
        Ok(Self {
            replica,
            database_name: database.identity.name.clone(),
            group_database_id: database.identity.group_database_id.clone(),
            local_database_id: local.database_id,
            database_guid: recovery
                .database_guid
                .clone()
                .ok_or("independent database GUID missing")?,
            family_guid: recovery
                .family_guid
                .clone()
                .ok_or("independent family GUID missing")?,
            recovery_fork_id: recovery
                .recovery_fork_guid
                .clone()
                .ok_or("independent recovery fork missing")?,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct BlockedPublicCoverage {
    pub status: &'static str,
    pub missing_evidence: Vec<&'static str>,
}

struct AuthorityState {
    generation: u64,
    epochs: [Epoch; 3],
    report_high_water: [u64; 3],
    pending: bool,
    retired: Option<&'static str>,
    advances: Vec<AcceptedCommand>,
}

pub struct CommandAdmission {
    state: Arc<Mutex<AuthorityState>>,
    finished: bool,
}

impl Drop for CommandAdmission {
    fn drop(&mut self) {
        if !self.finished {
            let mut state = self.state.lock().unwrap();
            state.pending = false;
            state
                .retired
                .get_or_insert("authority command outcome is unknown");
        }
    }
}

pub struct HostedAuthority {
    pub configuration: ConfigurationDescriptor,
    pub baseline: Vec<PublicV1AgentReport>,
    pub accepted: Vec<AcceptedCommand>,
    pub bindings: Vec<Arc<HealthyTopologyBinding>>,
    pub native_database_anchors: Vec<NativeDatabaseAnchor>,
    observers: Vec<ObserverConfig>,
    state: Arc<Mutex<AuthorityState>>,
    clock: Arc<dyn ObservationClock>,
}

pub fn operation_input(
    target: &ReplicaIdentity,
    resource: &str,
    request: &proto::ExecuteCommandRequest,
) -> DiagnosticResult<PublicOperationIntent> {
    let envelope = normalize_execute_request(request.clone())
        .map_err(|error| format!("public operation input: {error}"))?;
    let ProtocolCommand::PublicV1Operation(operation) = envelope.command else {
        return Err("diagnostic requires a public operation".into());
    };
    if &operation.target != target || request.resource_uid != resource {
        return Err("public operation target/resource differs".into());
    }
    Ok(operation.intent)
}

pub fn complete_remote_configuration(
    configuration: &ConfigurationDescriptor,
    reports: &[PublicV1AgentReport],
    local_index: usize,
) -> DiagnosticResult<PublicConfiguration> {
    if reports.len() != 3 || local_index >= 3 {
        return Err("three reports and an exact local member are required".into());
    }
    let peers = configuration
        .members
        .iter()
        .filter(|member| member.identity != reports[local_index].replica)
        .map(|member| {
            let report = reports
                .iter()
                .find(|report| report.replica == member.identity)
                .ok_or("configuration peer report missing")?;
            Ok(PublicReplicaDescriptor {
                build_id: Default::default(),
                identity: report.replica.clone(),
                replication_address: report
                    .replication_address
                    .clone()
                    .ok_or("configuration peer address missing")?,
                process_session_id: report.process_session_id.clone(),
                storage_incarnation: Some(report.storage_incarnation.clone()),
                role: member.role,
                is_up: report.has_current_open_endpoint(),
                is_ready: report.has_current_open_endpoint()
                    && report.progress.current().is_some()
                    && report.progress.capability().is_some(),
                current_progress: report
                    .progress
                    .current()
                    .ok_or("configuration peer current progress is unavailable")?,
                catch_up_capability: report
                    .progress
                    .capability()
                    .ok_or("configuration peer catch-up capability is unavailable")?,
                must_catchup: false,
            })
        })
        .collect::<DiagnosticResult<Vec<_>>>()?;
    let input = PublicConfiguration {
        current: configuration.clone(),
        previous: None,
        replicas: peers,
    };
    input.validate().map_err(str::to_owned)?;
    Ok(input)
}

pub async fn validate_secondary_native_configuration(
    target_index: usize,
    input: &PublicOperationIntent,
    report: &PublicV1AgentReport,
    replicator: &SqlServerReplicator,
) -> DiagnosticResult<Arc<HealthyTopologyBinding>> {
    accepted_command(target_index, input, report)?;
    let Some(PublicOperationProgram::Configuration(configuration)) = &input.program else {
        return Err("secondary native validation requires an accepted configuration".into());
    };
    configuration.validate().map_err(str::to_owned)?;
    if !configuration.current.members.iter().any(|member| {
        member.identity == report.replica && member.role == ReplicaRole::ActiveSecondary
    }) {
        return Err("secondary native validation requires the exact secondary member".into());
    }
    // Public secondary configuration persists host authority, not a primary callback.
    replicator
        .update_current_replica_set_configuration(ReplicaSetConfiguration {
            configuration: configuration.current.clone(),
            replicas: configuration
                .replicas
                .iter()
                .cloned()
                .map(Into::into)
                .collect(),
        })
        .await
        .map_err(|error| format!("explicit secondary native validation: {error}"))?;
    replicator
        .admitted_topology()
        .ok_or_else(|| "explicit secondary native validation did not admit a topology".into())
}

fn operation_kind(input: &PublicOperationIntent) -> DiagnosticResult<PublicV1CallKind> {
    if let Some(lifecycle) = &input.lifecycle {
        return match lifecycle.recipe {
            PublicLifecycleRecipe::InitialPrimary => Ok(PublicV1CallKind::InitialPrimary),
            PublicLifecycleRecipe::SecondaryEpochAdvance => {
                Ok(PublicV1CallKind::SecondaryEpochAdvance)
            }
            _ => Err("unsupported diagnostic lifecycle".into()),
        };
    }
    match input.program {
        Some(PublicOperationProgram::Epoch { .. }) => Ok(PublicV1CallKind::Epoch),
        Some(PublicOperationProgram::Role { .. }) => Ok(PublicV1CallKind::Role),
        Some(PublicOperationProgram::Close) => Ok(PublicV1CallKind::Close),
        Some(PublicOperationProgram::Configuration(_)) => Ok(PublicV1CallKind::Configuration),
        _ => Err("unsupported diagnostic operation".into()),
    }
}

pub fn accepted_command(
    target_index: usize,
    input: &PublicOperationIntent,
    report: &PublicV1AgentReport,
) -> DiagnosticResult<AcceptedCommand> {
    input.validate().map_err(str::to_owned)?;
    report.validate().map_err(str::to_owned)?;
    if input.identity != report.identity || input.process_session_id != report.process_session_id {
        return Err("operation input session/protocol differs from actual report".into());
    }
    let kind = operation_kind(input)?;
    let matches = report
        .completed_operations
        .iter()
        .filter(|completion| completion.operation_id == input.operation_id)
        .collect::<Vec<_>>();
    let [completion] = matches.as_slice() else {
        return Err("exact successful operation completion is missing or duplicated".into());
    };
    let (current, previous) = match (&input.lifecycle, &input.program) {
        (Some(lifecycle), None) => (
            Some(lifecycle.current.configuration_id.clone()),
            lifecycle
                .previous
                .as_ref()
                .map(|previous| previous.configuration_id.clone()),
        ),
        (None, Some(PublicOperationProgram::Configuration(configuration))) => (
            Some(configuration.current.configuration_id.clone()),
            configuration
                .previous
                .as_ref()
                .map(|previous| previous.configuration_id.clone()),
        ),
        _ => (None, None),
    };
    if completion.revision != input.revision
        || completion.call_kind != kind
        || completion.process_session_id != report.process_session_id
        || completion.input_digest != input.input_digest
        || completion.replay_disposition != input.replay_disposition
        || completion.history != report.history
        || completion.storage_incarnation != report.storage_incarnation
        || completion.current_configuration_id != current
        || completion.previous_configuration_id != previous
        || completion.result != PublicV1OperationResult::Succeeded
    {
        return Err("operation completion provenance differs from accepted input/report".into());
    }
    Ok(AcceptedCommand {
        target_index,
        input: input.clone(),
        completion: (*completion).clone(),
    })
}

pub fn validate_reports(
    baseline: &[PublicV1AgentReport],
    current: &[PublicV1AgentReport],
    high_water: &[u64; 3],
) -> DiagnosticResult<()> {
    if baseline.len() != 3 || current.len() != 3 {
        return Err("exactly three hosted member reports are required".into());
    }
    for (index, (expected, report)) in baseline.iter().zip(current).enumerate() {
        report.validate().map_err(str::to_owned)?;
        if report.identity != expected.identity
            || report.resource_uid != expected.resource_uid
            || report.replica != expected.replica
            || report.process_session_id != expected.process_session_id
            || report.replicator_creation_id != expected.replicator_creation_id
            || report.history != expected.history
            || report.storage_incarnation != expected.storage_incarnation
            || report.replication_address != expected.replication_address
        {
            return Err(format!(
                "hosted member {} identity/session/storage/history differs",
                index + 1
            ));
        }
        if report.report_sequence <= high_water[index] {
            return Err(format!(
                "hosted member {} report is delayed or replayed",
                index + 1
            ));
        }
    }
    Ok(())
}

pub fn validate_configuration(
    configuration: &ConfigurationDescriptor,
    accepted: &[AcceptedCommand],
    reports: &[PublicV1AgentReport],
) -> DiagnosticResult<()> {
    if accepted.len() != 3 || reports.len() != 3 || configuration.members.len() != 3 {
        return Err("three accepted configuration receipts are required".into());
    }
    for (index, command) in accepted.iter().enumerate() {
        let input = match (&command.input.lifecycle, &command.input.program) {
            (Some(lifecycle), None)
                if lifecycle.recipe == PublicLifecycleRecipe::InitialPrimary
                    && lifecycle.replica == reports[index].replica
                    && lifecycle.epoch == configuration.epoch =>
            {
                PublicConfiguration {
                    current: lifecycle.current.clone(),
                    previous: lifecycle.previous.clone(),
                    replicas: lifecycle.replicas.clone(),
                }
            }
            (None, Some(PublicOperationProgram::Configuration(input))) => input.clone(),
            _ => return Err("configuration receipt lacks exact supported input".into()),
        };
        if command.target_index != index
            || input.current != *configuration
            || input.previous.is_some()
        {
            return Err("accepted configuration descriptor/member/epoch differs".into());
        }
        let expected_peers = configuration
            .members
            .iter()
            .filter(|member| member.identity != reports[index].replica)
            .collect::<Vec<_>>();
        if expected_peers.len() != 2 || input.replicas.len() != expected_peers.len() {
            return Err("configuration peer membership differs".into());
        }
        validate_peer_descriptors(&expected_peers, &input.replicas, reports)?;
        let observed = accepted_command(index, &command.input, &reports[index])?;
        if observed.completion != command.completion {
            return Err("configuration accepted receipt was replaced".into());
        }
    }
    Ok(())
}

fn validate_peer_descriptors(
    peers: &[&ConfigurationMember],
    descriptors: &[PublicReplicaDescriptor],
    reports: &[PublicV1AgentReport],
) -> DiagnosticResult<()> {
    if descriptors.len() != peers.len() {
        return Err("configuration peer membership differs".into());
    }
    for peer in peers {
        let report = reports
            .iter()
            .find(|report| report.replica == peer.identity)
            .ok_or("configuration peer report missing")?;
        if descriptors
            .iter()
            .filter(|descriptor| {
                descriptor.identity == report.replica
                    && descriptor.process_session_id == report.process_session_id
                    && descriptor.storage_incarnation.as_ref() == Some(&report.storage_incarnation)
                    && Some(&descriptor.replication_address) == report.replication_address.as_ref()
                    && descriptor.role == peer.role
                    && descriptor.is_up == report.has_current_open_endpoint()
                    && descriptor.is_ready
                        == (report.has_current_open_endpoint()
                            && report.progress.current().is_some()
                            && report.progress.capability().is_some())
            })
            .count()
            != 1
        {
            return Err("configuration peer session/storage/address/availability differs".into());
        }
    }
    Ok(())
}

pub fn validate_startup_configuration(
    configuration: &ConfigurationDescriptor,
    baseline: &[PublicV1AgentReport; 3],
    inputs: &[(usize, PublicOperationIntent)],
    reports: &[PublicV1AgentReport; 3],
) -> DiagnosticResult<()> {
    validate_reports(
        baseline,
        reports,
        &std::array::from_fn(|index| baseline[index].report_sequence),
    )?;
    if configuration.members.len() != 3 || inputs.len() != 3 {
        return Err("three exact startup lifecycle inputs are required".into());
    }
    for (index, (target, input)) in inputs.iter().enumerate() {
        let report = &reports[index];
        let member = configuration
            .members
            .iter()
            .find(|member| member.identity == report.replica)
            .ok_or("startup report is not an expected configuration member")?;
        let recipe = match member.role {
            ReplicaRole::Primary => PublicLifecycleRecipe::InitialPrimary,
            ReplicaRole::ActiveSecondary => PublicLifecycleRecipe::SecondaryEpochAdvance,
            _ => return Err("startup configuration has an unsupported role".into()),
        };
        let lifecycle = input
            .lifecycle
            .as_ref()
            .ok_or("startup receipt lacks a lifecycle input")?;
        if *target != index
            || input.program.is_some()
            || lifecycle.recipe != recipe
            || lifecycle.replica != report.replica
            || lifecycle.epoch != configuration.epoch
            || lifecycle.current != *configuration
            || lifecycle.previous.is_some()
        {
            return Err("startup lifecycle target/recipe/configuration differs".into());
        }
        let peers = configuration
            .members
            .iter()
            .filter(|peer| peer.identity != member.identity)
            .collect::<Vec<_>>();
        validate_peer_descriptors(&peers, &lifecycle.replicas, reports)?;
        accepted_command(index, input, report)?;
    }
    Ok(())
}

fn validate_independent_native_anchor(
    anchor: &NativeDatabaseAnchor,
    replica: &ReplicaIdentity,
    native: &NativePrefixEvidence,
    index: usize,
) -> DiagnosticResult<()> {
    let expected = &native.initial_binding.members[index];
    if anchor.replica != *replica
        || anchor.database_name.as_str() != native.initial_binding.database_name
        || anchor.group_database_id.as_str() != native.initial_binding.group_database_id
        || anchor.local_database_id != expected.local_database_id
        || anchor.database_guid.as_str() != expected.database_guid
        || anchor.family_guid.as_str() != native.initial_binding.family_guid
        || anchor.recovery_fork_id.as_str() != native.initial_binding.recovery_fork_id
    {
        return Err("independent hosted native database anchor differs".into());
    }
    Ok(())
}

impl HostedAuthority {
    pub fn new(
        configuration: ConfigurationDescriptor,
        baseline: Vec<PublicV1AgentReport>,
        accepted: Vec<AcceptedCommand>,
        bindings: Vec<Arc<HealthyTopologyBinding>>,
        native_database_anchors: Vec<NativeDatabaseAnchor>,
        observers: Vec<ObserverConfig>,
        clock: Arc<dyn ObservationClock>,
    ) -> DiagnosticResult<Self> {
        validate_configuration(&configuration, &accepted, &baseline)?;
        if bindings.len() != 3 || native_database_anchors.len() != 3 || observers.len() != 3 {
            return Err("missing hosted/native member bridge".into());
        }
        let state = Arc::new(Mutex::new(AuthorityState {
            generation: 1,
            epochs: [configuration.epoch; 3],
            report_high_water: std::array::from_fn(|index| baseline[index].report_sequence),
            pending: false,
            retired: None,
            advances: Vec::new(),
        }));
        Ok(Self {
            configuration,
            baseline,
            accepted,
            bindings,
            native_database_anchors,
            observers,
            state,
            clock,
        })
    }

    pub fn evaluation_time(&self) -> DiagnosticResult<u64> {
        self.clock
            .now_unix_millis()
            .map_err(|error| format!("publication clock: {error}"))
    }

    fn check_lifetime(state: &AuthorityState, generation: u64) -> DiagnosticResult<()> {
        if let Some(reason) = state.retired {
            return Err(format!("diagnostic authority retired: {reason}"));
        }
        if state.pending || state.generation != generation {
            return Err("diagnostic authority is pending or superseded".into());
        }
        Ok(())
    }

    pub fn generation(&self) -> DiagnosticResult<u64> {
        let state = self.state.lock().unwrap();
        Self::check_lifetime(&state, state.generation)?;
        Ok(state.generation)
    }

    pub fn retire(&self) {
        self.state
            .lock()
            .unwrap()
            .retired
            .get_or_insert("host process stopping");
    }

    pub fn begin_command(&self, terminal: bool) -> DiagnosticResult<CommandAdmission> {
        let mut state = self.state.lock().unwrap();
        Self::check_lifetime(&state, state.generation)?;
        state.pending = true;
        if terminal {
            state.retired = Some("terminal dispatch");
        }
        Ok(CommandAdmission {
            state: self.state.clone(),
            finished: false,
        })
    }

    pub fn complete_command(
        &self,
        mut admission: CommandAdmission,
        index: usize,
        input: &PublicOperationIntent,
        reports: &[PublicV1AgentReport],
    ) -> DiagnosticResult<()> {
        if !Arc::ptr_eq(&admission.state, &self.state) || index >= 3 {
            return Err("authority admission owner/target differs".into());
        }
        let epoch = match input.program {
            Some(PublicOperationProgram::Epoch { epoch }) => epoch,
            Some(PublicOperationProgram::Role { epoch, role })
                if self.configuration.members.iter().any(|member| {
                    member.identity == self.baseline[index].replica && member.role == role
                }) =>
            {
                epoch
            }
            _ => return Err("unsupported owned diagnostic dispatch".into()),
        };
        let mut state = self.state.lock().unwrap();
        validate_reports(&self.baseline, reports, &state.report_high_water)?;
        validate_configuration(&self.configuration, &self.accepted, reports)?;
        let receipt = accepted_command(index, input, &reports[index])?;
        if epoch < state.epochs[index] {
            return Err("accepted diagnostic epoch regressed".into());
        }
        if epoch > state.epochs[index] {
            state.generation += 1;
            state.epochs[index] = epoch;
        }
        state.report_high_water = std::array::from_fn(|index| reports[index].report_sequence);
        state.advances.push(receipt);
        state.pending = false;
        admission.finished = true;
        Ok(())
    }

    pub async fn collect<F, R, RF>(
        &self,
        native: F,
        mut reports: R,
        members: &[ReadyMember; 3],
        source: NativeInput,
        budget: Duration,
    ) -> DiagnosticResult<HostedAssociation>
    where
        F: std::future::Future<Output = DiagnosticResult<NativePrefixEvidence>>,
        R: FnMut() -> RF,
        RF: std::future::Future<Output = DiagnosticResult<Vec<PublicV1AgentReport>>>,
    {
        tokio::time::timeout(budget, async {
            let generation = self.generation()?;
            let admitted_reports = reports().await?;
            {
                let mut state = self.state.lock().unwrap();
                Self::check_lifetime(&state, generation)?;
                validate_reports(&self.baseline, &admitted_reports, &state.report_high_water)?;
                validate_configuration(&self.configuration, &self.accepted, &admitted_reports)?;
                state.report_high_water =
                    std::array::from_fn(|index| admitted_reports[index].report_sequence);
            }
            let native = native.await?;
            self.validate_bridge(&native, &admitted_reports)?;
            self.generation().and_then(|current| {
                if current == generation {
                    Ok(())
                } else {
                    Err("diagnostic authority is pending or superseded".into())
                }
            })?;
            let final_reports = reports().await?;
            self.publish(
                generation,
                admitted_reports,
                native,
                final_reports,
                members,
                source,
            )
        })
        .await
        .map_err(|error| format!("hosted native association deadline: {error}"))?
    }

    pub fn publish(
        &self,
        generation: u64,
        admitted_reports: Vec<PublicV1AgentReport>,
        mut native: NativePrefixEvidence,
        final_reports: Vec<PublicV1AgentReport>,
        members: &[ReadyMember; 3],
        source: NativeInput,
    ) -> DiagnosticResult<HostedAssociation> {
        let mut state = self.state.lock().unwrap();
        Self::check_lifetime(&state, generation)?;
        if matches!(source, NativeInput::LiveQuietFixture) {
            native.evaluated_at_unix_millis = self.evaluation_time()?;
        }
        validate_reports(&self.baseline, &final_reports, &state.report_high_water)?;
        validate_configuration(&self.configuration, &self.accepted, &final_reports)?;
        self.validate_bridge(&native, &final_reports)?;
        let decision = native
            .evaluate(members)
            .map_err(|error| format!("native prefix evidence: {error}"))?;
        state.report_high_water = std::array::from_fn(|index| final_reports[index].report_sequence);
        let mut missing_evidence = vec![
            "role_specific_public_progress_and_history_mapping",
            "retained_transfer_capability",
            "durable_restart_continuity",
            "hardening_with_redo_lag",
        ];
        let (scope, native_input) = match source {
            NativeInput::Scripted => {
                missing_evidence.insert(0, "live_native_hosted_witness");
                ("offline_hosted_native_association", "scripted_no_live_sql")
            }
            NativeInput::LiveQuietFixture => {
                ("live_hosted_native_association", "live_owned_quiet_sql")
            }
        };
        Ok(HostedAssociation {
            scope,
            native_input,
            authority_boundary: "exclusively_owned_command_stream_at_publication",
            secondary_native_binding: "explicit_mssql_validation_after_public_configuration_receipt",
            accepted_configuration: self.accepted.clone(),
            accepted_advances: state.advances.clone(),
            admitted_reports,
            final_reports,
            native_database_anchors: self.native_database_anchors.clone(),
            native,
            decision,
            public_coverage: BlockedPublicCoverage {
                status: "blocked",
                missing_evidence,
            },
        })
    }

    fn validate_bridge(
        &self,
        native: &NativePrefixEvidence,
        reports: &[PublicV1AgentReport],
    ) -> DiagnosticResult<()> {
        for (index, binding) in self.bindings.iter().enumerate() {
            let report = &reports[index];
            let expected = &native.initial_binding.members[index];
            let anchor = &self.native_database_anchors[index];
            validate_independent_native_anchor(anchor, &report.replica, native, index)?;
            let local = binding
                .members()
                .iter()
                .find(|member| member.kuberic_identity() == &report.replica)
                .ok_or("missing adapter local member")?;
            if binding.local_identity() != &report.replica
                || binding.configuration() != &self.configuration
                || local.process_session_id() != &report.process_session_id
                || Some(local.replication_address()) != report.replication_address.as_deref()
                || local.native_replica_id().as_str() != expected.native_replica_id
                || local.server_name().as_str() != expected.server_name
                || local.sql_endpoint_url() != expected.endpoint_url
                || binding.local_sql_server_start_incarnation().as_str() != expected.sql_start_time
                || binding.availability_group().group_id.as_str()
                    != native.initial_binding.availability_group_id
                || binding.availability_group().name.as_str()
                    != native.initial_binding.availability_group_name
            {
                return Err(format!("hosted/native member {} bridge differs", index + 1));
            }
            let role = self
                .configuration
                .members
                .iter()
                .find(|member| member.identity == report.replica)
                .ok_or("local configured member missing")?
                .role;
            for samples in [&native.initial_observations, &native.observations] {
                let sample = samples
                    .members
                    .iter()
                    .find(|sample| sample.ordinal == expected.ordinal)
                    .ok_or("native member observation missing")?;
                binding
                    .validate_snapshot(
                        &self.observers[index],
                        &sample.snapshot,
                        native.evaluated_at_unix_millis,
                        true,
                        Some(role),
                    )
                    .map_err(|error| format!("adapter native history/member bridge: {error}"))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
