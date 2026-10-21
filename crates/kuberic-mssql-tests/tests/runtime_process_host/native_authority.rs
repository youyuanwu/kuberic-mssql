use super::*;

use kuberic_mssql_tests::three_replica::hosted_native_prefix::{
    NativePrefixDecision, NativePrefixEvidence, NativeSample, NativeSampleSet,
    TransactionAcknowledgement, TransactionRecordRow, TransactionSample,
};
use kuberic_mssql_tests::three_replica::{NativeMemberBinding, NativeTopologyBinding, ReadyMember};
use kuberic_runtime::protocol::public_operations::{
    ProgressQueryResult, PublicV1CallKind, PublicV1OperationResult,
};
use kuberic_runtime::replicator::{PrimaryReplicator, ReplicaSetConfiguration};

use kuberic_mssql_tests::three_replica::hosted_authority::{
    AcceptedCommand, DiagnosticResult, HostedAssociation, HostedAuthority, NativeDatabaseAnchor,
    NativeInput, accepted_command, complete_remote_configuration, operation_input,
    validate_reports, validate_secondary_native_configuration,
};

struct OwnedDiagnostic {
    attempts: Vec<HostAttempt>,
    replicas: Vec<RunningReplica>,
    authority: HostedAuthority,
    members: [ReadyMember; 3],
}

impl std::ops::Deref for OwnedDiagnostic {
    type Target = HostedAuthority;

    fn deref(&self) -> &Self::Target {
        &self.authority
    }
}

fn validate_configuration(
    accepted: &[AcceptedCommand],
    reports: &[PublicV1AgentReport],
) -> DiagnosticResult<()> {
    kuberic_mssql_tests::three_replica::hosted_authority::validate_configuration(
        &configuration(),
        accepted,
        reports,
    )
}

fn command_input(command: &PublicCommand) -> DiagnosticResult<PublicOperationIntent> {
    operation_input(
        &identities()[command.target_index],
        "runtime-host-test",
        &command.request,
    )
}

fn scripted_snapshot(index: usize, observed_at: u64) -> InstanceSnapshot {
    let mut value = snapshot(index);
    value.observed_at_unix_millis = observed_at;
    let Observation::Present {
        value: group,
        observed_at_unix_millis,
    } = &mut value.availability_group
    else {
        unreachable!()
    };
    *observed_at_unix_millis = observed_at;
    let database = &mut group.databases[0];
    let local = database.local.as_mut().unwrap();
    local.database_id += index as u32;
    local.recovery.as_mut().unwrap().database_guid = Some(guid(NATIVE_IDS[index]));
    database.replicas[0].database_id = local.database_id;
    value
}

impl OwnedDiagnostic {
    async fn start(root: &Path) -> Self {
        Self::start_with_clock(root, Arc::new(FixedClock)).await
    }

    async fn start_with_clock(root: &Path, clock: Arc<dyn ObservationClock>) -> Self {
        let snapshots = std::array::from_fn(|index| scripted_snapshot(index, OBSERVED_AT));
        let mut attempts = start_attempts_with_snapshots(root, snapshots).await;
        let (replicas, _, bootstrap_commands) = initialize_attempts(&mut attempts).await;
        let (_, configured) = current_public_reports(&attempts).await;
        let commands = (0..3)
            .map(|index| {
                let program = PublicOperationProgram::Configuration(
                    complete_remote_configuration(&configuration(), &configured, index).unwrap(),
                );
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
        let mut bindings = Vec::with_capacity(3);
        for (index, attempt) in attempts.iter().enumerate() {
            let replicator = attempt.application.testing_replicator().unwrap();
            let binding = if index == 0 {
                replicator.admitted_topology().unwrap()
            } else {
                assert!(
                    replicator.admitted_topology().is_none(),
                    "public secondary configuration must not invoke a primary-only callback"
                );
                validate_secondary_native_configuration(
                    index,
                    &accepted[index].input,
                    &baseline[index],
                    &replicator,
                )
                .await
                .unwrap()
            };
            bindings.push(binding);
        }
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
        let observers = attempts
            .iter()
            .map(|attempt| attempt.source.observer_config().clone())
            .collect();
        let authority = HostedAuthority::new(
            configuration(),
            baseline,
            accepted,
            bindings,
            native_database_anchors,
            observers,
            clock,
        )
        .unwrap();
        let members = std::array::from_fn(|index| ReadyMember {
            ordinal: index as u8 + 1,
            server_name: attempts[index]
                .source
                .fallback
                .instance
                .server_name
                .to_string(),
            container_id: format!("scripted-not-a-container-{}", index + 1),
            host_port: 0,
            sql_start_time: attempts[index]
                .source
                .fallback
                .instance
                .sqlserver_start_time
                .clone(),
            sql_start_unix_millis: 0,
            observer_config: PathBuf::new(),
            admin_username: PathBuf::new(),
            admin_password: PathBuf::new(),
            ca_certificate: PathBuf::new(),
        });
        Self {
            attempts,
            replicas,
            authority,
            members,
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

    async fn dispatch(&self, command: PublicCommand) -> DiagnosticResult<()> {
        let input = command_input(&command)?;
        let terminal = match &input.program {
            Some(PublicOperationProgram::Epoch { .. }) => false,
            Some(PublicOperationProgram::Role { role, .. })
                if *role == configuration().members[command.target_index].role =>
            {
                false
            }
            Some(PublicOperationProgram::Close) => true,
            _ => return Err("unsupported owned diagnostic dispatch".into()),
        };
        let admission = self.authority.begin_command(terminal)?;
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
        if terminal {
            accepted_command(command.target_index, &input, &reports[command.target_index])?;
            return Ok(());
        }
        self.authority
            .complete_command(admission, command.target_index, &input, &reports)
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
        self.authority
            .collect(
                native,
                || self.fresh_reports(),
                &self.members,
                NativeInput::Scripted,
                Duration::from_secs(15),
            )
            .await
    }

    fn publish(
        &self,
        generation: u64,
        admitted_reports: Vec<PublicV1AgentReport>,
        native: NativePrefixEvidence,
        final_reports: Vec<PublicV1AgentReport>,
    ) -> DiagnosticResult<HostedAssociation> {
        let members = ready_members(&native.initial_binding);
        self.authority.publish(
            generation,
            admitted_reports,
            native,
            final_reports,
            &members,
            NativeInput::Scripted,
        )
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
        let initial_observations = NativeSampleSet {
            requested_at_unix_millis: OBSERVED_AT,
            completed_at_unix_millis: OBSERVED_AT,
            members: samples,
        };
        let members = std::array::from_fn(|index| {
            let sample = &initial_observations.members[index].snapshot;
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
            commit_requested_at_unix_millis: 1100,
            acknowledged_at_unix_millis: 1150,
        };
        let transaction_samples = [
            (
                "before_commit",
                1080,
                1090,
                Some(TransactionRecordRow {
                    transaction_id: "123".into(),
                    database_id: 5,
                    state: 4,
                    commit_lsn: None,
                }),
            ),
            (
                "overlap",
                1101,
                1102,
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
            ("after_ack", 1151, 1152, None),
            ("after_ack", 1153, 1154, None),
        ]
        .into_iter()
        .map(|(phase, requested, completed, row)| TransactionSample {
            phase,
            requested_at_unix_millis: requested,
            completed_at_unix_millis: completed,
            row,
        })
        .collect();
        let final_at = 1200;
        let observations = NativeSampleSet {
            requested_at_unix_millis: final_at,
            completed_at_unix_millis: final_at,
            members: (0..3)
                .map(|index| NativeSample {
                    ordinal: index as u8 + 1,
                    collection_completed_at_unix_millis: final_at,
                    snapshot: scripted_snapshot(index, final_at),
                })
                .collect(),
        };
        Ok(NativePrefixEvidence {
            initial_binding: binding.clone(),
            initial_observations,
            record_binding: binding.clone(),
            transitions: Vec::new(),
            target_ordinal: 2,
            acknowledgement,
            transaction_samples,
            evaluation_binding: binding,
            observations,
            evaluated_at_unix_millis: final_at,
        })
    }

    async fn stop(&mut self) {
        self.authority.retire();
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
            serialized["secondary_native_binding"],
            "explicit_mssql_validation_after_public_configuration_receipt"
        );
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
    let replicator = group.attempts[0].application.testing_replicator().unwrap();
    group
        .dispatch(group.command(5, PublicOperationProgram::Close))
        .await
        .unwrap();
    assert!(replicator.current_progress().await.is_err());
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
    let generation = group.authority.generation().unwrap();
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
            .publish(generation, admitted, native, final_reports)
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

async fn partial_startup_ownership_cases() {
    use kuberic_mssql_tests::three_replica::{
        PublicStartupProbeHandle, PublicStartupProbeMember, PublicStartupProbeMode,
        exercise_partial_public_startup_cleanup,
    };
    for mode in 0..5 {
        let temporary = tempfile::tempdir().unwrap();
        let mut group = OwnedDiagnostic::start(temporary.path()).await;
        group.authority.retire();
        let reports = group.fresh_reports().await.unwrap();
        let replicators = group
            .attempts
            .iter()
            .map(|attempt| attempt.application.testing_replicator().unwrap())
            .collect::<Vec<_>>();
        let controls = group
            .attempts
            .iter()
            .map(|attempt| attempt.control)
            .collect::<Vec<_>>();
        let (release, held) = tokio::sync::oneshot::channel();
        let mut held = Some(held);
        let mut replicas = group.replicas.drain(..).map(Some).collect::<Vec<_>>();
        let mut failed = if mode >= 3 {
            let mut primary = replicas[0].take().unwrap();
            primary.shutdown();
            primary.wait().await.unwrap();
            let task = tokio::spawn(async { panic!("injected startup task panic") });
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            Some(task)
        } else {
            None
        };
        let members = replicas
            .into_iter()
            .zip(group.attempts.drain(..))
            .enumerate()
            .map(|(index, (running, attempt))| {
                let handle = if index == 0 && mode >= 3 {
                    PublicStartupProbeHandle::Pending(failed.take().unwrap())
                } else if index == 1 && mode < 3 {
                    let running = running.unwrap();
                    let held = held.take().unwrap();
                    PublicStartupProbeHandle::Pending(tokio::spawn(async move {
                        held.await.expect("release owned startup");
                        Ok(Some(running))
                    }))
                } else {
                    PublicStartupProbeHandle::Started(running.unwrap())
                };
                PublicStartupProbeMember {
                    config: attempt.config,
                    application: attempt.application.clone(),
                    source: attempt.source.clone(),
                    report: reports[index].clone(),
                    handle,
                }
            })
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|_| unreachable!());
        let (drained, mut completion) = tokio::sync::oneshot::channel();
        let budget = if mode == 2 {
            Duration::from_millis(20)
        } else {
            Duration::from_secs(5)
        };
        let probe_mode = match mode {
            3 => PublicStartupProbeMode::ReadinessFailure,
            4 => PublicStartupProbeMode::ReportFailure,
            _ => PublicStartupProbeMode::Drain,
        };
        let mut cleanup = Box::pin(exercise_partial_public_startup_cleanup(
            members, budget, drained, probe_mode,
        ));
        if mode >= 3 {
            let error = cleanup.await.unwrap_err();
            assert!(error.agent_termination_unconfirmed(), "{error}");
            assert!(error.to_string().contains("injected startup task panic"));
        } else if mode == 0 {
            release.send(()).unwrap();
            cleanup.await.unwrap();
        } else {
            tokio::select! {
                result = &mut cleanup => panic!("cleanup finished before held startup: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
            assert!(matches!(
                completion.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            if mode == 1 {
                drop(cleanup);
            } else {
                assert!(cleanup.await.unwrap_err().agent_termination_unconfirmed());
            }
            assert!(matches!(
                completion.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            release.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), completion)
            .await
            .unwrap()
            .unwrap();
        for (control, replicator) in controls.into_iter().zip(replicators) {
            assert!(tokio::net::TcpStream::connect(control).await.is_err());
            assert!(replicator.current_progress().await.is_err());
        }
    }
}

#[test]
fn partial_native_authority_startup_drains_transferred_and_late_hosts() {
    run_host_test(partial_startup_ownership_cases);
}

async fn quiet_assembly_case() {
    use kuberic_mssql_tests::three_replica::hosted_native_prefix::{
        CoverageConclusion, RecordProbeOutcome,
    };
    use kuberic_mssql_tests::three_replica::{
        CommitCoverageReport, associate_quiet_hosted_witness,
    };
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    struct PublicationClock(AtomicU64);
    impl ObservationClock for PublicationClock {
        fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }
    let temporary = tempfile::tempdir().unwrap();
    let clock = Arc::new(PublicationClock(AtomicU64::new(1200)));
    let mut group = OwnedDiagnostic::start_with_clock(temporary.path(), clock.clone()).await;
    let secondary = group.attempts[1].application.testing_replicator().unwrap();
    let binding = secondary.admitted_topology().unwrap();
    let mut unacknowledged = group.baseline[1].clone();
    unacknowledged
        .completed_operations
        .retain(|completion| completion.operation_id != group.accepted[1].input.operation_id);
    assert!(
        validate_secondary_native_configuration(
            1,
            &group.accepted[1].input,
            &unacknowledged,
            &secondary,
        )
        .await
        .is_err()
    );
    assert_eq!(secondary.admitted_topology().unwrap(), binding);
    let original = &group.accepted[0].input;
    for unavailable in [
        ProgressQueryResult::NotReady,
        ProgressQueryResult::NotQueried("copy pending".into()),
        ProgressQueryResult::Failed("query failed".into()),
    ] {
        let mut reports = group.baseline.clone();
        reports[1].progress.current_progress = unavailable.clone();
        assert_eq!(
            complete_remote_configuration(&configuration(), &reports, 0).unwrap_err(),
            "configuration peer current progress is unavailable"
        );
        reports[1].progress = group.baseline[1].progress.clone();
        reports[1].progress.catch_up_capability = unavailable;
        assert_eq!(
            complete_remote_configuration(&configuration(), &reports, 0).unwrap_err(),
            "configuration peer catch-up capability is unavailable"
        );
    }
    let mut later = group.baseline.clone();
    later[1].progress.current_progress =
        ProgressQueryResult::Value(later[1].progress.current().unwrap() + 1);
    let mut rebuilt = original.clone();
    let lifecycle = rebuilt.lifecycle.as_mut().unwrap();
    lifecycle.replicas = complete_remote_configuration(&configuration(), &later, 0)
        .unwrap()
        .replicas;
    rebuilt.input_digest = lifecycle.digest();
    assert_ne!(rebuilt.input_digest, original.input_digest);
    assert!(accepted_command(0, &rebuilt, &later[0]).is_err());
    accepted_command(0, original, &later[0]).unwrap();
    for index in [1, 2] {
        let Some(PublicOperationProgram::Configuration(input)) =
            &group.accepted[index].input.program
        else {
            unreachable!()
        };
        assert_eq!(input.replicas.len(), 2);
        assert!(
            input
                .replicas
                .iter()
                .any(|peer| peer.role == ReplicaRole::Primary)
        );
    }
    let evidence = group.scripted_native(Some("99")).await.unwrap();
    let raw = CommitCoverageReport {
        fixture_run_id: "server-free-live-assembly-schema-test".into(),
        fixture_resource_uid: "runtime-host-test".into(),
        native_binding: evidence.initial_binding.clone(),
        primary_ordinal: 1,
        selected_target_ordinal: 2,
        acknowledgement: evidence.acknowledgement.clone(),
        transaction_samples: evidence.transaction_samples.clone(),
        record_probe: RecordProbeOutcome::Observed {
            commit_lsn: DecimalProgress::parse("99").unwrap(),
        },
        before: evidence.initial_observations.clone(),
        after_ack: evidence.observations.clone(),
        quiet: evidence.observations.clone(),
        coverage: CoverageConclusion::Blocked {
            missing_evidence: vec!["server_free_synthetic_inputs_not_live_proof"],
            not_exercised: ["covered", "not_yet_covered", "hardening_with_redo_lag"],
        },
    };
    for stale in [false, true] {
        clock.0.store(1200, Ordering::SeqCst);
        let calls = AtomicUsize::new(0);
        let result = associate_quiet_hosted_witness(
            &group.authority,
            &group.members,
            async {
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                Ok(raw.clone())
            },
            || {
                let sequence = calls.fetch_add(1, Ordering::SeqCst);
                let group = &group;
                let clock = &clock;
                async move {
                    let reports = group.fresh_reports().await?;
                    if stale && sequence == 1 {
                        clock.0.store(40_000, Ordering::SeqCst);
                    }
                    Ok(reports)
                }
            },
            Duration::from_secs(15),
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        if stale {
            assert!(
                result.is_err(),
                "final reports cannot keep expired native samples fresh"
            );
        } else {
            let report = result.unwrap();
            assert_eq!(report.native_collection, raw);
            assert_eq!(report.association.scope, "live_hosted_native_association");
            assert_eq!(report.association.native_input, "live_owned_quiet_sql");
            assert_eq!(report.association.public_coverage.status, "blocked");
            assert!(
                !report
                    .association
                    .public_coverage
                    .missing_evidence
                    .contains(&"live_native_hosted_witness")
            );
            assert!(matches!(
                report.association.decision,
                NativePrefixDecision::NativeSampledCoverage { .. }
            ));
        }
    }
    group.stop().await;
}

#[test]
fn native_authority_quiet_assembly_preserves_frozen_inputs_and_publication_freshness() {
    run_host_test(quiet_assembly_case);
}

async fn contained_startup_case() {
    use kuberic_mssql_tests::three_replica::hosted_authority::validate_startup_configuration;
    use kuberic_mssql_tests::three_replica::{
        PublicStartupProbeHandle, PublicStartupProbeMember, exercise_public_configuration_startup,
    };
    use kuberic_runtime::protocol::public_operations::PublicProgressRole;
    use kuberic_runtime::protocol::types::{AccessStatus, ProcessSessionId};

    let temporary = tempfile::tempdir().unwrap();
    let mut attempts = start_attempts(temporary.path()).await;
    let replicas = initialize_stores(&mut attempts).await;
    let (_, initial) = current_public_reports(&attempts).await;
    let controls = attempts
        .iter()
        .map(|attempt| attempt.control)
        .collect::<Vec<_>>();
    let members = attempts
        .into_iter()
        .zip(replicas)
        .enumerate()
        .map(|(index, (attempt, running))| PublicStartupProbeMember {
            config: attempt.config,
            application: attempt.application,
            source: attempt.source,
            report: initial[index].clone(),
            handle: PublicStartupProbeHandle::Started(running),
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| unreachable!());
    let (drained, completion) = tokio::sync::oneshot::channel();
    let evidence = exercise_public_configuration_startup(
        members,
        &configuration(),
        Duration::from_secs(10),
        drained,
    )
    .await
    .unwrap();
    completion.await.unwrap();
    validate_startup_configuration(
        &configuration(),
        &evidence.baseline,
        &evidence.inputs,
        &evidence.reports,
    )
    .unwrap();
    for (index, (report, control)) in evidence.reports.iter().zip(controls).enumerate() {
        let (role, progress_role, access) = if index == 0 {
            (
                ReplicaRole::Primary,
                PublicProgressRole::Primary,
                AccessStatus::Granted,
            )
        } else {
            (
                ReplicaRole::ActiveSecondary,
                PublicProgressRole::Secondary,
                AccessStatus::NotPrimary,
            )
        };
        assert_eq!(report.role, role);
        assert_eq!(report.progress.role, progress_role);
        assert_eq!(report.progress.capability(), Some(42));
        assert_eq!(report.progress.current(), Some(42));
        assert_eq!(report.read_status, access);
        assert_eq!(report.write_status, access);
        assert!(tokio::net::TcpStream::connect(control).await.is_err());
    }

    for mutation in 0..16 {
        let mut reports = evidence.reports.clone();
        let mut inputs = evidence.inputs.clone();
        let report = &mut reports[1];
        let operation_id = &evidence.inputs[1].1.operation_id;
        let receipt_index = report
            .completed_operations
            .iter()
            .position(|receipt| &receipt.operation_id == operation_id)
            .unwrap();
        match mutation {
            0 => {
                report.completed_operations.remove(receipt_index);
            }
            1 => {
                report.completed_operations[receipt_index].result =
                    PublicV1OperationResult::Cancelled
            }
            2 => {
                report.completed_operations[receipt_index].input_digest =
                    PublicOperationProgram::Close.digest()
            }
            3 => report.completed_operations[receipt_index].revision += 1,
            4 => report.completed_operations[receipt_index].call_kind = PublicV1CallKind::Role,
            5 => {
                report.process_session_id = ProcessSessionId::new("replacement");
                for receipt in &mut report.completed_operations {
                    receipt.process_session_id = report.process_session_id.clone();
                }
            }
            6 => {
                report.history.data_loss_number += 1;
                for receipt in &mut report.completed_operations {
                    receipt.history = report.history.clone();
                }
            }
            7 => {
                report.storage_incarnation.storage_generation =
                    StorageGeneration::new("replacement");
                for receipt in &mut report.completed_operations {
                    receipt.storage_incarnation = report.storage_incarnation.clone();
                }
            }
            8 => report.report_sequence = evidence.baseline[1].report_sequence,
            9 => inputs[1].0 = 0,
            10 => {
                inputs[1].1.lifecycle.as_mut().unwrap().current = ConfigurationDescriptor::new(
                    Epoch::new(0, configuration().epoch.configuration_number + 1),
                    configuration().primary_id,
                    configuration().members,
                    2,
                )
            }
            11 => inputs[1].1.lifecycle.as_mut().unwrap().replica = identities()[0].clone(),
            12 => {
                inputs[1].1.lifecycle.as_mut().unwrap().recipe =
                    PublicLifecycleRecipe::InitialPrimary
            }
            13 => report.completed_operations[receipt_index].current_configuration_id = None,
            14 => {
                let lifecycle = inputs[1].1.lifecycle.as_mut().unwrap();
                lifecycle.replicas[0].process_session_id =
                    ProcessSessionId::new("replacement-peer");
                inputs[1].1.input_digest = lifecycle.digest();
                report.completed_operations[receipt_index].input_digest =
                    inputs[1].1.input_digest.clone();
            }
            15 => report.replication_address = Some("http://127.0.0.1:1".into()),
            _ => unreachable!(),
        }
        if matches!(mutation, 6 | 7) {
            accepted_command(1, &inputs[1].1, report).unwrap();
        }
        let error =
            validate_startup_configuration(&configuration(), &evidence.baseline, &inputs, &reports)
                .expect_err(&format!("altered startup evidence {mutation}"));
        if matches!(mutation, 6 | 7) {
            assert!(error.contains("identity/session/storage/history differs"));
        }
    }
}

#[test]
fn native_authority_contained_startup_uses_actual_gate_and_exact_receipts() {
    run_host_test(contained_startup_case);
}
