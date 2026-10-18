use super::*;

use kuberic_mssql::kuberic::HealthyTopologyBinding;
use kuberic_mssql_tests::three_replica::hosted_native_prefix::{
    NativePrefixDecision, NativePrefixEvidence, NativeSample, NativeSampleSet,
    TransactionAcknowledgement, TransactionRecordRow, TransactionSample,
};
use kuberic_mssql_tests::three_replica::{NativeMemberBinding, NativeTopologyBinding, ReadyMember};
use kuberic_runtime::control::normalize_execute_request;
use kuberic_runtime::protocol::command::ProtocolCommand;
use kuberic_runtime::protocol::public_operations::{
    PublicConfiguration, PublicV1CallKind, PublicV1OperationCompletion, PublicV1OperationResult,
};
use kuberic_runtime::replicator::{PrimaryReplicator, ReplicaSetConfiguration};
use serde::Serialize;

type DiagnosticResult<T> = Result<T, String>;

#[derive(Clone, Debug, Serialize)]
struct AcceptedCommand {
    target_index: usize,
    input: PublicOperationIntent,
    completion: PublicV1OperationCompletion,
}

#[derive(Debug, Serialize)]
struct HostedAssociation {
    scope: &'static str,
    native_input: &'static str,
    authority_boundary: &'static str,
    accepted_configuration: Vec<AcceptedCommand>,
    accepted_advances: Vec<AcceptedCommand>,
    admitted_reports: Vec<PublicV1AgentReport>,
    final_reports: Vec<PublicV1AgentReport>,
    native_database_anchors: Vec<NativeDatabaseAnchor>,
    native: NativePrefixEvidence,
    decision: NativePrefixDecision,
    public_coverage: BlockedPublicCoverage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct NativeDatabaseAnchor {
    replica: ReplicaIdentity,
    database_name: SqlIdentifier,
    group_database_id: Guid,
    local_database_id: u32,
    database_guid: Guid,
    family_guid: Guid,
    recovery_fork_id: Guid,
}

impl NativeDatabaseAnchor {
    fn from_snapshot(
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
struct BlockedPublicCoverage {
    status: &'static str,
    missing_evidence: [&'static str; 5],
}

struct AuthorityState {
    generation: u64,
    epochs: [Epoch; 3],
    report_high_water: [u64; 3],
    pending: bool,
    retired: Option<&'static str>,
    advances: Vec<AcceptedCommand>,
}

struct CommandAdmission {
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

struct OwnedDiagnostic {
    attempts: Vec<HostAttempt>,
    replicas: Vec<RunningReplica>,
    baseline: Vec<PublicV1AgentReport>,
    accepted: Vec<AcceptedCommand>,
    bindings: Vec<Arc<HealthyTopologyBinding>>,
    native_database_anchors: Vec<NativeDatabaseAnchor>,
    state: Arc<Mutex<AuthorityState>>,
}

fn command_input(command: &PublicCommand) -> DiagnosticResult<PublicOperationIntent> {
    let envelope = normalize_execute_request(command.request.clone())
        .map_err(|error| format!("public operation input: {error}"))?;
    let ProtocolCommand::PublicV1Operation(operation) = envelope.command else {
        return Err("diagnostic requires a public operation".into());
    };
    if operation.target != identities()[command.target_index]
        || command.request.resource_uid != "runtime-host-test"
    {
        return Err("public operation target differs".into());
    }
    Ok(operation.intent)
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

fn accepted_command(
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

fn validate_reports(
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

fn validate_configuration(
    accepted: &[AcceptedCommand],
    reports: &[PublicV1AgentReport],
) -> DiagnosticResult<()> {
    if accepted.len() != 3 || reports.len() != 3 {
        return Err("three accepted configuration receipts are required".into());
    }
    for (index, command) in accepted.iter().enumerate() {
        let input = match (&command.input.lifecycle, &command.input.program) {
            (Some(lifecycle), None)
                if index == 0
                    && lifecycle.recipe == PublicLifecycleRecipe::InitialPrimary
                    && lifecycle.replica == reports[index].replica
                    && lifecycle.epoch == configuration().epoch =>
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
            || input.current != configuration()
            || input.previous.is_some()
        {
            return Err("accepted configuration descriptor/member/epoch differs".into());
        }
        let expected_peers = configuration()
            .members
            .into_iter()
            .filter(|member| member.identity != reports[index].replica)
            .collect::<Vec<_>>();
        if input.replicas.len() != expected_peers.len() {
            return Err("configuration peer membership differs".into());
        }
        for peer in expected_peers {
            let report = reports
                .iter()
                .find(|report| report.replica == peer.identity)
                .ok_or("configuration peer report missing")?;
            if input
                .replicas
                .iter()
                .filter(|descriptor| {
                    descriptor.identity == report.replica
                        && descriptor.process_session_id == report.process_session_id
                        && descriptor.storage_incarnation.as_ref()
                            == Some(&report.storage_incarnation)
                        && Some(&descriptor.replication_address)
                            == report.replication_address.as_ref()
                        && descriptor.role == peer.role
                })
                .count()
                != 1
            {
                return Err("configuration peer session/storage/address differs".into());
            }
        }
        let observed = accepted_command(index, &command.input, &reports[index])?;
        if observed.completion != command.completion {
            return Err("configuration accepted receipt was replaced".into());
        }
    }
    Ok(())
}

impl OwnedDiagnostic {
    async fn start(root: &Path) -> Self {
        let snapshots = std::array::from_fn(|index| {
            let mut value = snapshot(index);
            let Observation::Present { value: group, .. } = &mut value.availability_group else {
                unreachable!()
            };
            let database = &mut group.databases[0];
            let local = database.local.as_mut().unwrap();
            local.database_id += index as u32;
            local.recovery.as_mut().unwrap().database_guid = Some(guid(NATIVE_IDS[index]));
            database.replicas[0].database_id = local.database_id;
            value
        });
        let mut attempts = start_attempts_with_snapshots(root, snapshots).await;
        let (replicas, _, bootstrap_commands) = initialize_attempts(&mut attempts).await;
        let (_, configured) = current_public_reports(&attempts).await;
        let commands = (0..3)
            .map(|index| {
                let replicas = configuration()
                    .members
                    .into_iter()
                    .filter(|member| member.identity != configured[index].replica)
                    .map(|member| {
                        let peer = usize::try_from(member.identity.replica_id.value() - 1).unwrap();
                        PublicReplicaDescriptor {
                            build_id: OperationId::default(),
                            identity: configured[peer].replica.clone(),
                            replication_address: format!("http://{}", attempts[peer].replication),
                            process_session_id: configured[peer].process_session_id.clone(),
                            storage_incarnation: Some(configured[peer].storage_incarnation.clone()),
                            role: member.role,
                            current_progress: configured[peer].progress.last,
                            catch_up_capability: configured[peer].progress.first,
                            must_catchup: false,
                        }
                    })
                    .collect();
                let program = PublicOperationProgram::Configuration(PublicConfiguration {
                    current: configuration(),
                    previous: None,
                    replicas,
                });
                PublicCommand {
                    target_index: index,
                    request: public_operation_request(
                        index,
                        &configured[index],
                        program_intent(
                            &configured[index],
                            format!("native-full-configuration-{}", index + 1),
                            4,
                            program,
                        ),
                    ),
                }
            })
            .collect::<Vec<_>>();
        let commands = commands
            .into_iter()
            .map(|command| {
                let index = command.target_index;
                if attempts[index]
                    .application
                    .testing_replicator()
                    .unwrap()
                    .admitted_topology()
                    .is_some()
                {
                    bootstrap_commands
                        .iter()
                        .find(|existing| {
                            existing.target_index == index
                                && command_input(existing).unwrap().lifecycle.is_some()
                        })
                        .unwrap()
                        .clone()
                } else {
                    command
                }
            })
            .collect::<Vec<_>>();
        for command in &commands {
            if matches!(
                command_input(command).unwrap().program,
                Some(PublicOperationProgram::Configuration(_))
            ) {
                execute_public_command(&attempts, command).await;
            }
        }
        let (_, baseline) = current_public_reports(&attempts).await;
        let accepted = (0..3)
            .map(|index| {
                let command = &commands[index];
                accepted_command(index, &command_input(command).unwrap(), &baseline[index]).unwrap()
            })
            .collect::<Vec<_>>();
        validate_configuration(&accepted, &baseline).unwrap();
        for (index, report) in baseline.iter().enumerate() {
            assert_eq!(report.replica, identities()[index]);
            assert_eq!(report.resource_uid.as_str(), "runtime-host-test");
            assert_eq!(
                report.storage_incarnation.storage,
                ReplicaStorageIdentity::Persistent {
                    pod_uid: PodUid::new(format!("pod-{}", index + 1)),
                    pvc_uid: PvcUid::new(format!("pvc-{}", index + 1)),
                }
            );
        }
        let bindings: Vec<_> = attempts
            .iter()
            .map(|attempt| {
                attempt
                    .application
                    .testing_replicator()
                    .unwrap()
                    .admitted_topology()
                    .unwrap()
            })
            .collect();
        let mut native_database_anchors = Vec::with_capacity(3);
        for (index, attempt) in attempts.iter().enumerate() {
            let Observation::Present {
                value: snapshot, ..
            } = attempt.source.observe().await.unwrap()
            else {
                panic!("initial native anchor unavailable")
            };
            bindings[index]
                .validate_snapshot(
                    attempt.source.observer_config(),
                    &snapshot,
                    OBSERVED_AT,
                    true,
                    Some(configuration().members[index].role),
                )
                .unwrap();
            native_database_anchors.push(
                NativeDatabaseAnchor::from_snapshot(baseline[index].replica.clone(), &snapshot)
                    .unwrap(),
            );
        }
        let state = Arc::new(Mutex::new(AuthorityState {
            generation: 1,
            epochs: [configuration().epoch; 3],
            report_high_water: std::array::from_fn(|index| baseline[index].report_sequence),
            pending: false,
            retired: None,
            advances: Vec::new(),
        }));
        Self {
            attempts,
            replicas,
            baseline,
            accepted,
            bindings,
            native_database_anchors,
            state,
        }
    }

    async fn fresh_reports(&self) -> DiagnosticResult<Vec<PublicV1AgentReport>> {
        let mut reports = Vec::with_capacity(3);
        for (index, attempt) in self.attempts.iter().enumerate() {
            let mut client = AgentControlClient::connect(format!("http://{}", attempt.control))
                .await
                .map_err(|error| format!("diagnostic report connect: {error}"))?;
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.get_status(authorized(wire::GetAgentStatusRequest {
                    protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                    resource_uid: "runtime-host-test".into(),
                    replica_id: index as i64 + 1,
                    expected_instance_id: format!("pod-{}", index + 1),
                })),
            )
            .await
            .map_err(|error| format!("diagnostic report deadline: {error}"))?
            .map_err(|error| format!("diagnostic report: {error}"))?;
            reports.push(
                normalize_public_v1_agent_status_report(result.into_inner())
                    .map_err(|error| format!("diagnostic report normalization: {error}"))?,
            );
        }
        Ok(reports)
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

    fn begin_command(&self, terminal: bool) -> DiagnosticResult<CommandAdmission> {
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

    async fn dispatch(&self, command: PublicCommand) -> DiagnosticResult<()> {
        let input = command_input(&command)?;
        let (epoch, terminal) = match &input.program {
            Some(PublicOperationProgram::Epoch { epoch }) => (*epoch, false),
            Some(PublicOperationProgram::Role { epoch, role })
                if *role == configuration().members[command.target_index].role =>
            {
                (*epoch, false)
            }
            Some(PublicOperationProgram::Close) => (configuration().epoch, true),
            _ => return Err("unsupported owned diagnostic dispatch".into()),
        };
        let mut admission = self.begin_command(terminal)?;
        let mut client = AgentControlClient::connect(format!(
            "http://{}",
            self.attempts[command.target_index].control
        ))
        .await
        .map_err(|error| format!("authority dispatch connect: {error}"))?;
        tokio::time::timeout(
            Duration::from_secs(5),
            client.execute(authorized(command.request)),
        )
        .await
        .map_err(|error| format!("authority dispatch deadline: {error}"))?
        .map_err(|error| format!("authority dispatch: {error}"))?;
        let reports = self.fresh_reports().await?;
        let receipt =
            accepted_command(command.target_index, &input, &reports[command.target_index])?;
        let mut state = self.state.lock().unwrap();
        validate_reports(&self.baseline, &reports, &state.report_high_water)?;
        validate_configuration(&self.accepted, &reports)?;
        if epoch < state.epochs[command.target_index] {
            return Err("accepted diagnostic epoch regressed".into());
        }
        if epoch > state.epochs[command.target_index] {
            state.generation += 1;
            state.epochs[command.target_index] = epoch;
        }
        state.report_high_water = std::array::from_fn(|index| reports[index].report_sequence);
        state.advances.push(receipt);
        state.pending = false;
        admission.finished = true;
        Ok(())
    }

    fn command(&self, revision: u64, program: PublicOperationProgram) -> PublicCommand {
        let report = &self.baseline[0];
        PublicCommand {
            target_index: 0,
            request: public_operation_request(
                0,
                report,
                program_intent(
                    report,
                    format!("native-authority-{revision}"),
                    revision,
                    program,
                ),
            ),
        }
    }

    async fn collect(
        &self,
        native: impl std::future::Future<Output = DiagnosticResult<NativePrefixEvidence>>,
    ) -> DiagnosticResult<HostedAssociation> {
        tokio::time::timeout(Duration::from_secs(15), self.collect_inner(native))
            .await
            .map_err(|error| format!("hosted native association deadline: {error}"))?
    }

    async fn collect_inner(
        &self,
        native: impl std::future::Future<Output = DiagnosticResult<NativePrefixEvidence>>,
    ) -> DiagnosticResult<HostedAssociation> {
        let generation = {
            let state = self.state.lock().unwrap();
            Self::check_lifetime(&state, state.generation)?;
            state.generation
        };
        let admitted_reports = self.fresh_reports().await?;
        {
            let mut state = self.state.lock().unwrap();
            Self::check_lifetime(&state, generation)?;
            validate_reports(&self.baseline, &admitted_reports, &state.report_high_water)?;
            validate_configuration(&self.accepted, &admitted_reports)?;
            state.report_high_water =
                std::array::from_fn(|index| admitted_reports[index].report_sequence);
        }
        let native = native.await?;
        self.validate_bridge(&native, &admitted_reports)?;
        let decision = native
            .evaluate(&ready_members(&native.initial_binding))
            .map_err(|error| format!("native prefix evidence: {error}"))?;
        {
            let state = self.state.lock().unwrap();
            Self::check_lifetime(&state, generation)?;
        }
        let final_reports = self.fresh_reports().await?;
        self.publish(
            generation,
            admitted_reports,
            native,
            decision,
            final_reports,
        )
    }

    fn publish(
        &self,
        generation: u64,
        admitted_reports: Vec<PublicV1AgentReport>,
        native: NativePrefixEvidence,
        decision: NativePrefixDecision,
        final_reports: Vec<PublicV1AgentReport>,
    ) -> DiagnosticResult<HostedAssociation> {
        let mut state = self.state.lock().unwrap();
        Self::check_lifetime(&state, generation)?;
        validate_reports(&self.baseline, &final_reports, &state.report_high_water)?;
        validate_configuration(&self.accepted, &final_reports)?;
        self.validate_bridge(&native, &final_reports)?;
        state.report_high_water = std::array::from_fn(|index| final_reports[index].report_sequence);
        Ok(HostedAssociation {
            scope: "offline_hosted_native_association",
            native_input: "scripted_no_live_sql",
            authority_boundary: "exclusively_owned_command_stream_at_publication",
            accepted_configuration: self.accepted.clone(),
            accepted_advances: state.advances.clone(),
            admitted_reports,
            final_reports,
            native_database_anchors: self.native_database_anchors.clone(),
            native,
            decision,
            public_coverage: BlockedPublicCoverage {
                status: "blocked",
                missing_evidence: [
                    "live_native_hosted_witness",
                    "role_specific_public_progress_and_history_mapping",
                    "retained_transfer_capability",
                    "durable_restart_continuity",
                    "hardening_with_redo_lag",
                ],
            },
        })
    }

    fn validate_bridge(
        &self,
        native: &NativePrefixEvidence,
        reports: &[PublicV1AgentReport],
    ) -> DiagnosticResult<()> {
        if self.bindings.len() != 3 || reports.len() != 3 {
            return Err("missing hosted/native member bridge".into());
        }
        for (index, binding) in self.bindings.iter().enumerate() {
            let report = &reports[index];
            let expected = &native.initial_binding.members[index];
            let anchor = &self.native_database_anchors[index];
            if anchor.replica != report.replica
                || anchor.database_name.as_str() != native.initial_binding.database_name
                || anchor.group_database_id.as_str() != native.initial_binding.group_database_id
                || anchor.local_database_id != expected.local_database_id
                || anchor.database_guid.as_str() != expected.database_guid
                || anchor.family_guid.as_str() != native.initial_binding.family_guid
                || anchor.recovery_fork_id.as_str() != native.initial_binding.recovery_fork_id
            {
                return Err("independent hosted native database anchor differs".into());
            }
            let local = binding
                .members()
                .iter()
                .find(|member| member.kuberic_identity() == &report.replica)
                .ok_or("missing adapter local member")?;
            if binding.local_identity() != &report.replica
                || binding.configuration() != &configuration()
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
            for samples in [&native.initial_observations, &native.observations] {
                let sample = samples
                    .members
                    .iter()
                    .find(|sample| sample.ordinal == expected.ordinal)
                    .ok_or("native member observation missing")?;
                binding
                    .validate_snapshot(
                        self.attempts[index].source.observer_config(),
                        &sample.snapshot,
                        native.evaluated_at_unix_millis,
                        true,
                        Some(configuration().members[index].role),
                    )
                    .map_err(|error| format!("adapter native history/member bridge: {error}"))?;
            }
        }
        Ok(())
    }

    async fn scripted_native(
        &self,
        record: Option<&str>,
    ) -> DiagnosticResult<NativePrefixEvidence> {
        let mut samples = Vec::new();
        for (index, attempt) in self.attempts.iter().enumerate() {
            let observed = attempt
                .source
                .observe()
                .await
                .map_err(|error| format!("scripted native observation: {error}"))?;
            let Observation::Present {
                value: snapshot, ..
            } = observed
            else {
                return Err("scripted native observation unavailable".into());
            };
            samples.push(NativeSample {
                ordinal: index as u8 + 1,
                collection_completed_at_unix_millis: OBSERVED_AT,
                snapshot,
            });
        }
        let observations = NativeSampleSet {
            requested_at_unix_millis: OBSERVED_AT,
            completed_at_unix_millis: OBSERVED_AT,
            members: samples,
        };
        let members = std::array::from_fn(|index| {
            let sample = &observations.members[index].snapshot;
            let Observation::Present { value: group, .. } = &sample.availability_group else {
                unreachable!()
            };
            let database = &group.databases[0];
            let local = database.local.as_ref().unwrap();
            NativeMemberBinding {
                ordinal: index as u8 + 1,
                server_name: sample.instance.server_name.to_string(),
                container_id: format!("scripted-not-a-container-{}", index + 1),
                sql_start_time: sample.instance.sqlserver_start_time.clone(),
                sql_start_unix_millis: 0,
                native_replica_id: local.replica_id.to_string(),
                local_database_id: local.database_id,
                database_guid: local
                    .recovery
                    .as_ref()
                    .unwrap()
                    .database_guid
                    .as_ref()
                    .unwrap()
                    .to_string(),
                role: if index == 0 { "PRIMARY" } else { "SECONDARY" }.into(),
                endpoint_url: format!("TCP://sql-{}:5022", index + 1),
                endpoint_name: "scripted-not-collected".into(),
                endpoint_port: 5022,
                endpoint_certificate_name: "scripted-not-collected".into(),
                endpoint_certificate_thumbprint: "scripted-not-collected".into(),
            }
        });
        let binding = NativeTopologyBinding {
            session_id: "scripted-native-run-not-public-session".into(),
            availability_group_name: "app-ag".into(),
            availability_group_id: "66666666-6666-4666-8666-666666666666".into(),
            configuration_sequence: 42,
            database_name: "app-db".into(),
            group_database_id: "44444444-4444-4444-8444-444444444444".into(),
            family_guid: "88888888-8888-4888-8888-888888888888".into(),
            recovery_fork_id: "55555555-5555-4555-8555-555555555555".into(),
            seeding_operation_ids: ["scripted".into(), "scripted".into()],
            members,
        };
        let acknowledgement = TransactionAcknowledgement {
            marker_id: "00000000-0000-4000-8000-000000000001".into(),
            marker_value: "scripted-no-insert".into(),
            transaction_id: "123".into(),
            database_id: 5,
            delayed_durability: "DISABLED".into(),
            commit_requested_at_unix_millis: 900,
            acknowledged_at_unix_millis: 950,
        };
        let transaction_samples = [
            (
                "before_commit",
                880,
                890,
                Some(TransactionRecordRow {
                    transaction_id: "123".into(),
                    database_id: 5,
                    state: 4,
                    commit_lsn: None,
                }),
            ),
            (
                "overlap",
                901,
                902,
                Some(TransactionRecordRow {
                    transaction_id: "123".into(),
                    database_id: 5,
                    state: 12,
                    commit_lsn: record
                        .map(DecimalProgress::parse)
                        .transpose()
                        .map_err(|error| format!("scripted record: {error}"))?,
                }),
            ),
            ("after_ack", 951, 952, None),
            ("after_ack", 953, 954, None),
        ]
        .into_iter()
        .map(|(phase, requested, completed, row)| TransactionSample {
            phase,
            requested_at_unix_millis: requested,
            completed_at_unix_millis: completed,
            row,
        })
        .collect();
        Ok(NativePrefixEvidence {
            initial_binding: binding.clone(),
            initial_observations: observations.clone(),
            record_binding: binding.clone(),
            transitions: Vec::new(),
            target_ordinal: 2,
            acknowledgement,
            transaction_samples,
            evaluation_binding: binding,
            observations,
            evaluated_at_unix_millis: OBSERVED_AT,
        })
    }

    async fn stop(&mut self) {
        self.state.lock().unwrap().retired = Some("host process stopping");
        stop_all(&mut self.replicas).await;
    }
}

fn ready_members(binding: &NativeTopologyBinding) -> [ReadyMember; 3] {
    std::array::from_fn(|index| {
        let member = &binding.members[index];
        ReadyMember {
            ordinal: member.ordinal,
            server_name: member.server_name.clone(),
            container_id: member.container_id.clone(),
            host_port: 0,
            sql_start_time: member.sql_start_time.clone(),
            sql_start_unix_millis: member.sql_start_unix_millis,
            observer_config: PathBuf::new(),
            admin_username: PathBuf::new(),
            admin_password: PathBuf::new(),
            ca_certificate: PathBuf::new(),
        }
    })
}

async fn pending_collection(group: &OwnedDiagnostic, command: PublicCommand, retired: bool) {
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    let mut collection = Box::pin(group.collect(async {
        entered.send(()).unwrap();
        held.await.unwrap();
        group.scripted_native(Some("99")).await
    }));
    tokio::select! {
        result = &mut collection => panic!("collection finished before its gate: {result:?}"),
        result = waiting => result.unwrap(),
    }
    group.dispatch(command).await.unwrap();
    release.send(()).unwrap();
    let result = collection.await;
    if retired {
        assert!(result.unwrap_err().contains("superseded"));
    } else {
        assert!(matches!(
            result.unwrap().decision,
            NativePrefixDecision::NativeSampledCoverage { .. }
        ));
    }
}

async fn association_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut group = OwnedDiagnostic::start(temporary.path()).await;
    for (record, expected) in [
        (Some("99"), "native_sampled_coverage"),
        (Some("100"), "outside_observed_prefix"),
        (None, "unavailable"),
    ] {
        let association = group.collect(group.scripted_native(record)).await.unwrap();
        let serialized = serde_json::to_value(&association).unwrap();
        assert_eq!(serialized["decision"]["status"], expected);
        assert_eq!(serialized["public_coverage"]["status"], "blocked");
        assert_eq!(serialized["native_input"], "scripted_no_live_sql");
        assert_eq!(
            association
                .native
                .record_binding
                .members
                .map(|member| member.local_database_id),
            [5, 6, 7]
        );
    }
    altered_evidence_cases(&group).await;
    for (revision, program, retired) in [
        (
            5,
            PublicOperationProgram::Epoch {
                epoch: Epoch::new(0, 42),
            },
            false,
        ),
        (
            6,
            PublicOperationProgram::Epoch {
                epoch: Epoch::new(0, 43),
            },
            true,
        ),
        (
            7,
            PublicOperationProgram::Role {
                epoch: Epoch::new(0, 44),
                role: ReplicaRole::Primary,
            },
            true,
        ),
    ] {
        pending_collection(&group, group.command(revision, program), retired).await;
        if !retired {
            pending_collection(
                &group,
                group.command(
                    revision,
                    PublicOperationProgram::Epoch {
                        epoch: Epoch::new(0, 42),
                    },
                ),
                false,
            )
            .await;
        }
    }
    let retained = group.baseline.clone();
    group.stop().await;
    let mut restarted_attempts = start_attempts_with_snapshots(
        temporary.path(),
        std::array::from_fn(|index| group.attempts[index].source.fallback.clone()),
    )
    .await;
    let mut restarted = Vec::new();
    for attempt in &mut restarted_attempts {
        restarted.push((&mut attempt.task).await.unwrap().unwrap());
    }
    let (_, reports) = current_public_reports(&restarted_attempts).await;
    assert!(
        retained
            .iter()
            .zip(&reports)
            .all(|(old, new)| old.process_session_id != new.process_session_id)
    );
    assert!(validate_reports(&retained, &reports, &[0; 3]).is_err());
    stop_all(&mut restarted).await;
}

async fn altered_evidence_cases(group: &OwnedDiagnostic) {
    let (_, current) = current_public_reports(&group.attempts).await;
    for mutation in 0..14 {
        let mut reports = current.clone();
        let report = &mut reports[1];
        match mutation {
            0 => report.process_session_id = Default::default(),
            1 => report.process_session_id = group.baseline[0].process_session_id.clone(),
            2 => report.replicator_creation_id = Default::default(),
            3 => {
                report.storage_incarnation.storage_generation =
                    StorageGeneration::new("replacement")
            }
            4 => {
                report.storage_incarnation.storage = ReplicaStorageIdentity::Persistent {
                    pod_uid: PodUid::new("replacement"),
                    pvc_uid: PvcUid::new("pvc-2"),
                }
            }
            5 => {
                report.storage_incarnation.storage = ReplicaStorageIdentity::Persistent {
                    pod_uid: PodUid::new("pod-2"),
                    pvc_uid: PvcUid::new("replacement"),
                }
            }
            6 => report.history.data_loss_number += 1,
            7 => report.resource_uid = ResourceUid::new("other"),
            8 => report.replica = identities()[0].clone(),
            9 => report.report_sequence = group.baseline[1].report_sequence,
            10 => report.replication_address = None,
            11 => report.storage_incarnation.storage_generation = Default::default(),
            12 => report.history.history_id = Default::default(),
            13 => report.replicator_creation_id = group.baseline[0].replicator_creation_id.clone(),
            _ => unreachable!(),
        }
        assert!(
            validate_reports(
                &group.baseline,
                &reports,
                &std::array::from_fn(|index| group.baseline[index].report_sequence)
            )
            .is_err(),
            "altered public evidence mutation {mutation}"
        );
    }
    for mutation in 0..12 {
        let mut reports = current.clone();
        let receipt = reports[0]
            .completed_operations
            .iter_mut()
            .find(|receipt| receipt.operation_id == group.accepted[0].input.operation_id)
            .unwrap();
        match mutation {
            0 => receipt.operation_id = OperationId::new("other"),
            1 => receipt.revision += 1,
            2 => receipt.call_kind = PublicV1CallKind::Epoch,
            3 => receipt.process_session_id = group.baseline[1].process_session_id.clone(),
            4 => receipt.input_digest = PublicOperationProgram::Close.digest(),
            5 => receipt.replay_disposition = PublicOperationReplayDisposition::Containment,
            6 => receipt.history.data_loss_number += 1,
            7 => receipt.storage_incarnation = group.baseline[1].storage_incarnation.clone(),
            8 => receipt.current_configuration_id = None,
            9 => receipt.previous_configuration_id = receipt.current_configuration_id.clone(),
            10 => receipt.result = PublicV1OperationResult::Cancelled,
            11 => receipt.result = PublicV1OperationResult::Ambiguous("unknown".into()),
            _ => unreachable!(),
        }
        assert!(
            validate_configuration(&group.accepted, &reports).is_err(),
            "altered completion mutation {mutation}"
        );
    }
    for mutation in 0..5 {
        let mut accepted = group.accepted.clone();
        let lifecycle = accepted[0].input.lifecycle.as_mut().unwrap();
        match mutation {
            0 => lifecycle.current.configuration_id = Default::default(),
            1 => lifecycle.current.members[1].identity = identities()[0].clone(),
            2 => lifecycle.replicas[0].process_session_id = Default::default(),
            3 => lifecycle.replicas[0].storage_incarnation = None,
            4 => lifecycle.replicas[0].identity = identities()[0].clone(),
            _ => unreachable!(),
        }
        assert!(
            validate_configuration(&accepted, &current).is_err(),
            "altered configuration mutation {mutation}"
        );
    }
    for mutation in 0..7 {
        let mut evidence = group.scripted_native(Some("99")).await.unwrap();
        match mutation {
            0 => evidence.initial_binding.members[1].sql_start_time.push('x'),
            1 => evidence.initial_binding.members[1].native_replica_id = NATIVE_IDS[0].into(),
            2 => evidence.record_binding.configuration_sequence += 1,
            3 => evidence.initial_binding.members[1].local_database_id += 1,
            4 => evidence.initial_binding.members[1].database_guid = NATIVE_IDS[0].into(),
            5 => evidence.initial_binding.family_guid = NATIVE_IDS[0].into(),
            6 => evidence.initial_binding.recovery_fork_id = NATIVE_IDS[0].into(),
            _ => unreachable!(),
        }
        assert!(
            group.collect(async { Ok(evidence) }).await.is_err(),
            "altered native evidence mutation {mutation}"
        );
    }
    for mutation in 0..4 {
        let mut evidence = group.scripted_native(Some("99")).await.unwrap();
        for binding in [
            &mut evidence.initial_binding,
            &mut evidence.record_binding,
            &mut evidence.evaluation_binding,
        ] {
            match mutation {
                0 => binding.members[1].local_database_id += 10,
                1 => binding.members[1].database_guid = NATIVE_IDS[0].into(),
                2 => binding.family_guid = NATIVE_IDS[0].into(),
                3 => binding.recovery_fork_id = NATIVE_IDS[0].into(),
                _ => unreachable!(),
            }
        }
        for samples in [
            &mut evidence.initial_observations,
            &mut evidence.observations,
        ] {
            for sample in &mut samples.members {
                let Observation::Present { value: group, .. } =
                    &mut sample.snapshot.availability_group
                else {
                    unreachable!()
                };
                let database = &mut group.databases[0];
                let local = database.local.as_mut().unwrap();
                let recovery = local.recovery.as_mut().unwrap();
                match mutation {
                    0 if sample.ordinal == 2 => {
                        local.database_id += 10;
                        database.replicas[0].database_id += 10;
                    }
                    1 if sample.ordinal == 2 => recovery.database_guid = Some(guid(NATIVE_IDS[0])),
                    2 => recovery.family_guid = Some(guid(NATIVE_IDS[0])),
                    3 => {
                        recovery.recovery_fork_guid = Some(guid(NATIVE_IDS[0]));
                        let RecoveryLineageObservation::Local { value: lineage } =
                            &mut database.replicas[0].lineage
                        else {
                            unreachable!()
                        };
                        lineage.recovery_fork_id = guid(NATIVE_IDS[0]);
                    }
                    _ => {}
                }
            }
        }
        evidence
            .evaluate(&ready_members(&evidence.initial_binding))
            .unwrap();
        let error = group.collect(async { Ok(evidence) }).await.unwrap_err();
        assert!(
            error.contains("independent hosted native database anchor"),
            "coherent native mutation {mutation}: {error}"
        );
    }
}

async fn terminal_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut group = OwnedDiagnostic::start(temporary.path()).await;
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    let mut collection = Box::pin(group.collect(async {
        entered.send(()).unwrap();
        held.await.unwrap();
        group.scripted_native(Some("99")).await
    }));
    tokio::select! {
        result = &mut collection => panic!("early collection: {result:?}"),
        result = waiting => result.unwrap(),
    }
    let error = group
        .dispatch(group.command(5, PublicOperationProgram::Close))
        .await
        .unwrap_err();
    assert!(error.contains("closed"), "{error}");
    release.send(()).unwrap();
    assert!(collection.await.unwrap_err().contains("retired"));
    group.attempts[0]
        .application
        .wait_for_containment()
        .await
        .unwrap();
    group.stop().await;
}

#[test]
fn public_native_authority_association_rejects_supersession_and_restart() {
    run_host_test(association_case);
}

#[test]
fn public_native_authority_association_rejects_terminal_publication() {
    run_host_test(terminal_case);
}

async fn pending_dispatch_case(await_unknown_result: bool) {
    let temporary = tempfile::tempdir().unwrap();
    let mut group = OwnedDiagnostic::start(temporary.path()).await;
    let admitted = group.fresh_reports().await.unwrap();
    let final_reports = group.fresh_reports().await.unwrap();
    let native = group.scripted_native(Some("99")).await.unwrap();
    let decision = native
        .evaluate(&ready_members(&native.initial_binding))
        .unwrap();
    let generation = group.state.lock().unwrap().generation;
    let command = group.command(
        5,
        PublicOperationProgram::Role {
            epoch: Epoch::new(0, 42),
            role: ReplicaRole::Primary,
        },
    );
    let input = command_input(&command).unwrap();
    let (entered, release) = group.attempts[0].source.gate_next_observation();
    let mut dispatch = Box::pin(group.dispatch(command));
    tokio::select! {
        result = &mut dispatch => panic!("dispatch finished before its observation gate: {result:?}"),
        result = entered => result.unwrap(),
    }
    assert!(
        group
            .publish(
                generation,
                admitted.clone(),
                native.clone(),
                decision.clone(),
                final_reports.clone(),
            )
            .unwrap_err()
            .contains("pending")
    );
    if await_unknown_result {
        let error = dispatch.await.unwrap_err();
        assert!(error.contains("authority dispatch deadline"), "{error}");
    } else {
        drop(dispatch);
    }
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let reports = group.fresh_reports().await.unwrap();
            if reports[0]
                .completed_operations
                .iter()
                .any(|receipt| receipt.operation_id == input.operation_id)
            {
                accepted_command(0, &input, &reports[0]).unwrap();
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        group
            .publish(generation, admitted, native, decision, final_reports,)
            .unwrap_err()
            .contains("unknown")
    );
    assert!(
        group
            .collect(group.scripted_native(Some("99")))
            .await
            .unwrap_err()
            .contains("unknown")
    );
    group.stop().await;
}

async fn pending_and_unknown_dispatch_cases() {
    pending_dispatch_case(false).await;
    pending_dispatch_case(true).await;
}

#[test]
fn public_native_authority_association_rejects_pending_and_unknown_dispatch() {
    run_host_test(pending_and_unknown_dispatch_cases);
}

async fn secondary_replay_case() {
    let temporary = tempfile::tempdir().unwrap();
    let mut group = OwnedDiagnostic::start(temporary.path()).await;
    for index in [1, 2] {
        let Some(PublicOperationProgram::Configuration(input)) =
            &group.accepted[index].input.program
        else {
            unreachable!()
        };
        let configuration = ReplicaSetConfiguration {
            configuration: input.current.clone(),
            replicas: input.replicas.iter().cloned().map(Into::into).collect(),
        };
        let replicator = group.attempts[index]
            .application
            .testing_replicator()
            .unwrap();
        let original = replicator.admitted_topology().unwrap();
        replicator
            .update_current_replica_set_configuration(configuration.clone())
            .await
            .unwrap();
        assert_eq!(replicator.admitted_topology().unwrap(), original);
        let local = original
            .members()
            .iter()
            .find(|member| member.kuberic_identity() == original.local_identity())
            .unwrap();
        assert_eq!(
            local.process_session_id(),
            &group.baseline[index].process_session_id
        );
        assert_eq!(
            Some(local.replication_address()),
            group.baseline[index].replication_address.as_deref()
        );
        for mutation in 0..2 {
            let mut changed = configuration.clone();
            match mutation {
                0 => {
                    changed.replicas[0].process_session_id =
                        kuberic_runtime::protocol::types::ProcessSessionId::new(
                            "replaced-remote-session",
                        )
                }
                1 => changed.replicas[0].replication_address = "http://127.0.0.1:32001".into(),
                _ => unreachable!(),
            }
            let error = replicator
                .update_current_replica_set_configuration(changed)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("replay differs"), "{error}");
            assert_eq!(replicator.admitted_topology().unwrap(), original);
        }
    }
    group.stop().await;
}

#[test]
fn remote_only_secondary_configuration_replay_keeps_factory_local_context() {
    run_host_test(secondary_replay_case);
}
