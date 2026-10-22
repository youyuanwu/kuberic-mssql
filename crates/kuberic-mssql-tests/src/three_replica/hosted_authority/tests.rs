//! Synthetic record-consistency tests, not real hosted operations or SQL observations.
//!
//! Succeeded receipts are validator input assumptions only. Reports retain failed
//! current progress and unknown capability; these tests prove neither public history
//! mapping nor successful startup, association, or full adapter/native bridging.

use kuberic_mssql::observation::RecoveryLineageObservation;
use kuberic_runtime::protocol::public_operations::{
    INVALID_LSN, PossibleDataLossIntent, ProgressQueryResult, PublicLifecycleInput,
    PublicOperationClass, PublicOperationReplayDisposition, PublicProgress, PublicProgressRole,
    PublicV1Identity,
};
use kuberic_runtime::protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationId, HistoryContext, HistoryId, OpenMode,
    OperationId, ProcessSessionId, PublicProtocolGeneration, ReplicaId, ReplicaInstanceId,
    ReplicaStorageIdentity, ReplicaStorageIncarnationContext, ReplicatorCreationId, ResourceUid,
    StorageGeneration,
};

use super::*;
use crate::three_replica::commit_coverage::prefix::tests::receipt;
use crate::three_replica::commit_coverage::tests::bound_prefix_fixture;

fn synthetic_report_records() -> (ConfigurationDescriptor, [PublicV1AgentReport; 3]) {
    let reports = std::array::from_fn(|index| {
        let replica = ReplicaIdentity {
            replica_id: ReplicaId::new(index as i64 + 1),
            instance_id: ReplicaInstanceId::new(format!("synthetic-instance-{index}")),
            agent_generation: AgentGeneration::new(format!("synthetic-generation-{index}")),
        };
        let resource_uid = ResourceUid::new("synthetic-validator-resource");
        let report = PublicV1AgentReport {
            identity: PublicV1Identity::new(1),
            resource_uid: resource_uid.clone(),
            replica: replica.clone(),
            process_session_id: ProcessSessionId::new(format!("synthetic-session-{index}")),
            replicator_creation_id: ReplicatorCreationId::new(format!(
                "synthetic-creation-{index}"
            )),
            report_sequence: 10,
            role: ReplicaRole::None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            history: HistoryContext {
                resource_uid,
                protocol_generation: PublicProtocolGeneration::new(1),
                history_id: HistoryId::new("synthetic-validator-history"),
                data_loss_number: 0,
            },
            storage_incarnation: ReplicaStorageIncarnationContext {
                replica,
                storage: ReplicaStorageIdentity::Volatile {
                    identity: format!("synthetic-storage-{index}"),
                },
                storage_generation: StorageGeneration::new("synthetic-storage-generation"),
            },
            progress: PublicProgress {
                role: PublicProgressRole::Recovery,
                current_progress: ProgressQueryResult::Failed(
                    "synthetic record has no public current-progress proof".into(),
                ),
                catch_up_capability: ProgressQueryResult::Value(INVALID_LSN),
            },
            configuration: None,
            active_operations: Vec::new(),
            open_mode: OpenMode::New,
            replication_address: Some(format!("synthetic://replica-{index}")),
            service_location: None,
            data_loss_recovery: None,
            completed_operations: Vec::new(),
            extensions: Default::default(),
        };
        report.validate().unwrap();
        report
    });
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 42),
        reports[0].replica.replica_id,
        reports
            .iter()
            .enumerate()
            .map(|(index, report)| ConfigurationMember {
                identity: report.replica.clone(),
                role: if index == 0 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    );
    (configuration, reports)
}

fn synthetic_configuration_record(
    configuration: &ConfigurationDescriptor,
    reports: &[PublicV1AgentReport; 3],
    index: usize,
) -> PublicConfiguration {
    let record = PublicConfiguration {
        current: configuration.clone(),
        previous: None,
        replicas: configuration
            .members
            .iter()
            .filter(|member| member.identity != reports[index].replica)
            .map(|member| {
                let report = reports
                    .iter()
                    .find(|report| report.replica == member.identity)
                    .unwrap();
                PublicReplicaDescriptor {
                    build_id: Default::default(),
                    identity: report.replica.clone(),
                    replication_address: report.replication_address.clone().unwrap(),
                    process_session_id: report.process_session_id.clone(),
                    storage_incarnation: Some(report.storage_incarnation.clone()),
                    role: member.role,
                    current_progress: INVALID_LSN,
                    catch_up_capability: INVALID_LSN,
                    must_catchup: false,
                }
            })
            .collect(),
    };
    record.validate().unwrap();
    record
}

fn synthetic_input_record(
    report: &PublicV1AgentReport,
    program: PublicOperationProgram,
) -> PublicOperationIntent {
    let input = PublicOperationIntent {
        identity: report.identity.clone(),
        operation_id: OperationId::new(format!(
            "synthetic-command-{}",
            report.replica.replica_id.value()
        )),
        revision: 1,
        process_session_id: report.process_session_id.clone(),
        class: PublicOperationClass::Authority,
        input_digest: program.digest(),
        replay_disposition: program.replay_disposition(),
        lifecycle: None,
        program: Some(program),
    };
    input.validate().unwrap();
    input
}

fn synthetic_succeeded_receipt_record(
    input: &PublicOperationIntent,
    report: &PublicV1AgentReport,
    kind: PublicV1CallKind,
    configuration: &PublicConfiguration,
) -> PublicV1OperationCompletion {
    PublicV1OperationCompletion {
        operation_id: input.operation_id.clone(),
        revision: input.revision,
        call_kind: kind,
        process_session_id: input.process_session_id.clone(),
        input_digest: input.input_digest.clone(),
        replay_disposition: input.replay_disposition,
        history: report.history.clone(),
        storage_incarnation: report.storage_incarnation.clone(),
        previous_configuration_id: configuration
            .previous
            .as_ref()
            .map(|previous| previous.configuration_id.clone()),
        current_configuration_id: Some(configuration.current.configuration_id.clone()),
        catch_up_mode: None,
        catch_up_specific_quorum: None,
        result: PublicV1OperationResult::Succeeded,
    }
}

fn synthetic_configuration_records() -> (
    ConfigurationDescriptor,
    [PublicV1AgentReport; 3],
    [AcceptedCommand; 3],
) {
    let (configuration, mut reports) = synthetic_report_records();
    let accepted = std::array::from_fn(|index| {
        let record = synthetic_configuration_record(&configuration, &reports, index);
        let input = synthetic_input_record(
            &reports[index],
            PublicOperationProgram::Configuration(record.clone()),
        );
        let completion = synthetic_succeeded_receipt_record(
            &input,
            &reports[index],
            PublicV1CallKind::Configuration,
            &record,
        );
        reports[index].completed_operations.push(completion);
        accepted_command(index, &input, &reports[index]).unwrap()
    });
    validate_configuration(&configuration, &accepted, &reports).unwrap();
    (configuration, reports, accepted)
}

fn rebind_synthetic_receipt_context(report: &mut PublicV1AgentReport) {
    for completion in &mut report.completed_operations {
        completion.history = report.history.clone();
        completion.storage_incarnation = report.storage_incarnation.clone();
        completion.process_session_id = report.process_session_id.clone();
    }
}

#[test]
fn synthetic_reports_preserve_frozen_history_storage_session_and_high_water() {
    let (_, baseline, _) = synthetic_configuration_records();
    let high_water = std::array::from_fn(|index| baseline[index].report_sequence);
    let mut current = baseline.clone();
    for report in &mut current {
        report.report_sequence += 1;
    }
    validate_reports(&baseline, &current, &high_water).unwrap();
    for change in 0..4 {
        let mut changed = current.clone();
        let report = &mut changed[1];
        match change {
            0 => report.history.history_id = HistoryId::new("synthetic-other-history"),
            1 => {
                report.storage_incarnation.storage_generation =
                    StorageGeneration::new("synthetic-other-storage-generation")
            }
            2 => report.process_session_id = ProcessSessionId::new("synthetic-other-session"),
            3 => report.report_sequence = high_water[1],
            _ => unreachable!(),
        }
        rebind_synthetic_receipt_context(report);
        report.validate().unwrap();
        let expected = if change == 3 {
            "hosted member 2 report is delayed or replayed"
        } else {
            "hosted member 2 identity/session/storage/history differs"
        };
        assert_eq!(
            validate_reports(&baseline, &changed, &high_water).unwrap_err(),
            expected,
            "mutation {change}"
        );
    }
}

#[test]
fn synthetic_receipts_require_exact_operation_and_input_report_provenance() {
    let (_, reports, accepted) = synthetic_configuration_records();
    let input = &accepted[0].input;
    accepted_command(0, input, &reports[0]).unwrap();
    for change in 0..11 {
        let mut report = reports[0].clone();
        let completion = &mut report.completed_operations[0];
        match change {
            0 => completion.operation_id = OperationId::new("synthetic-other-operation"),
            1 => completion.revision += 1,
            2 => completion.call_kind = PublicV1CallKind::Epoch,
            3 => completion.process_session_id = ProcessSessionId::new("synthetic-other-session"),
            4 => {
                completion.input_digest = PublicOperationProgram::Epoch {
                    epoch: Epoch::new(0, 43),
                }
                .digest()
            }
            5 => completion.history.history_id = HistoryId::new("synthetic-other-history"),
            6 => {
                completion.storage_incarnation.storage_generation =
                    StorageGeneration::new("synthetic-other-storage-generation")
            }
            7 => {
                completion.current_configuration_id = Some(ConfigurationId::new(
                    "synthetic-other-current-configuration",
                ))
            }
            8 => {
                completion.previous_configuration_id = Some(ConfigurationId::new(
                    "synthetic-unexpected-previous-configuration",
                ))
            }
            9 => completion.replay_disposition = PublicOperationReplayDisposition::RepeatableRead,
            10 => {
                completion.result =
                    PublicV1OperationResult::Ambiguous("synthetic uncertain outcome".into())
            }
            _ => unreachable!(),
        }
        input.validate().unwrap();
        report.validate().unwrap();
        let expected = if change == 0 {
            "exact successful operation completion is missing or duplicated"
        } else {
            "operation completion provenance differs from accepted input/report"
        };
        assert_eq!(
            accepted_command(0, input, &report).unwrap_err(),
            expected,
            "mutation {change}"
        );
    }
}

#[test]
fn synthetic_configuration_freezes_peer_descriptors_and_stored_receipts() {
    let (configuration, reports, accepted) = synthetic_configuration_records();
    for change in 0..3 {
        let mut changed_reports = reports.clone();
        let mut changed_accepted = accepted.clone();
        let input = &mut changed_accepted[0].input;
        let Some(PublicOperationProgram::Configuration(record)) = &mut input.program else {
            unreachable!()
        };
        match change {
            0 => {
                record.replicas[0].process_session_id =
                    ProcessSessionId::new("synthetic-replaced-peer-session")
            }
            1 => {
                record.replicas[0]
                    .storage_incarnation
                    .as_mut()
                    .unwrap()
                    .storage_generation = StorageGeneration::new("synthetic-replaced-peer-storage")
            }
            2 => record.replicas[0].replication_address = "synthetic://replaced-peer".into(),
            _ => unreachable!(),
        }
        let record = record.clone();
        input.input_digest = input.program.as_ref().unwrap().digest();
        input.validate().unwrap();
        changed_reports[0].completed_operations[0] = synthetic_succeeded_receipt_record(
            input,
            &changed_reports[0],
            PublicV1CallKind::Configuration,
            &record,
        );
        changed_reports[0].validate().unwrap();
        changed_accepted[0] = accepted_command(0, input, &changed_reports[0]).unwrap();
        assert_eq!(
            validate_configuration(&configuration, &changed_accepted, &changed_reports)
                .unwrap_err(),
            "configuration peer session/storage/address differs",
            "mutation {change}"
        );
    }
    let mut replaced = accepted.clone();
    replaced[0].completion.revision += 1;
    accepted_command(0, &replaced[0].input, &reports[0]).unwrap();
    assert_eq!(
        validate_configuration(&configuration, &replaced, &reports).unwrap_err(),
        "configuration accepted receipt was replaced"
    );
}

#[test]
fn synthetic_startup_requires_baseline_continuity_and_exact_target_recipe_configuration() {
    let (configuration, baseline) = synthetic_report_records();
    let mut reports = baseline.clone();
    let inputs: [_; 3] = std::array::from_fn(|index| {
        reports[index].report_sequence += 1;
        let record = synthetic_configuration_record(&configuration, &reports, index);
        let mut input = synthetic_input_record(
            &reports[index],
            PublicOperationProgram::Configuration(record.clone()),
        );
        let (recipe, kind) = if index == 0 {
            (
                PublicLifecycleRecipe::InitialPrimary,
                PublicV1CallKind::InitialPrimary,
            )
        } else {
            (
                PublicLifecycleRecipe::SecondaryEpochAdvance,
                PublicV1CallKind::SecondaryEpochAdvance,
            )
        };
        let lifecycle = PublicLifecycleInput {
            recipe,
            replica: reports[index].replica.clone(),
            epoch: configuration.epoch,
            possible_data_loss: PossibleDataLossIntent::NotPossible,
            current: configuration.clone(),
            previous: None,
            replicas: record.replicas.clone(),
            data_loss_recovery: None,
        };
        input.input_digest = lifecycle.digest();
        input.lifecycle = Some(lifecycle);
        input.program = None;
        input.validate().unwrap();
        let completion = synthetic_succeeded_receipt_record(&input, &reports[index], kind, &record);
        reports[index].completed_operations.push(completion);
        accepted_command(index, &input, &reports[index]).unwrap();
        (index, input)
    });
    validate_startup_configuration(&configuration, &baseline, &inputs, &reports).unwrap();
    for change in 0..5 {
        let mut changed_reports = reports.clone();
        let mut changed_inputs = inputs.clone();
        match change {
            0 => changed_reports[0].history.history_id = HistoryId::new("synthetic-other-history"),
            1 => {
                changed_reports[0].storage_incarnation.storage_generation =
                    StorageGeneration::new("synthetic-other-storage-generation")
            }
            2 => changed_inputs[0].0 = 1,
            3 | 4 => {
                let input = &mut changed_inputs[0].1;
                let lifecycle = input.lifecycle.as_mut().unwrap();
                if change == 3 {
                    let mut previous = configuration.clone();
                    previous.epoch = Epoch::new(0, 41);
                    previous.configuration_id = previous.expected_id();
                    lifecycle.recipe = PublicLifecycleRecipe::FailoverPromotion;
                    lifecycle.previous = Some(previous);
                } else {
                    lifecycle.epoch = Epoch::new(0, 43);
                    lifecycle.current.epoch = lifecycle.epoch;
                    lifecycle.current.configuration_id = lifecycle.current.expected_id();
                }
                let record = lifecycle.configuration();
                input.input_digest = lifecycle.digest();
                input.validate().unwrap();
                changed_reports[0].completed_operations[0] = synthetic_succeeded_receipt_record(
                    input,
                    &changed_reports[0],
                    if change == 3 {
                        PublicV1CallKind::FailoverPromotion
                    } else {
                        PublicV1CallKind::InitialPrimary
                    },
                    &record,
                );
            }
            _ => unreachable!(),
        }
        rebind_synthetic_receipt_context(&mut changed_reports[0]);
        changed_inputs[0].1.validate().unwrap();
        changed_reports[0].validate().unwrap();
        if change != 3 {
            accepted_command(0, &changed_inputs[0].1, &changed_reports[0]).unwrap();
        }
        assert_eq!(
            validate_startup_configuration(
                &configuration,
                &baseline,
                &changed_inputs,
                &changed_reports
            )
            .unwrap_err(),
            if change <= 1 {
                "hosted member 1 identity/session/storage/history differs"
            } else {
                "startup lifecycle target/recipe/configuration differs"
            },
            "mutation {change}"
        );
    }
}

fn synthetic_native_prefix_record() -> (NativePrefixEvidence, [ReadyMember; 3]) {
    let (samples, members, binding) = bound_prefix_fixture();
    let (acknowledgement, transaction_samples) = receipt(Some("99"));
    let mut initial = samples.clone();
    initial.requested_at_unix_millis = 850;
    initial.completed_at_unix_millis = 879;
    for sample in &mut initial.members {
        sample.collection_completed_at_unix_millis = 870;
        sample.snapshot.observed_at_unix_millis = 860;
        let Observation::Present {
            observed_at_unix_millis,
            ..
        } = &mut sample.snapshot.availability_group
        else {
            unreachable!()
        };
        *observed_at_unix_millis = 860;
    }
    let native = NativePrefixEvidence {
        initial_binding: binding.clone(),
        initial_observations: initial,
        record_binding: binding.clone(),
        transitions: Vec::new(),
        target_ordinal: 2,
        acknowledgement,
        transaction_samples,
        evaluation_binding: binding,
        observations: samples,
        evaluated_at_unix_millis: 1100,
    };
    native.evaluate(&members).unwrap();
    (native, members)
}

#[test]
fn synthetic_native_rewrites_pass_prefix_evaluation_but_fail_independent_anchors() {
    let (native, members) = synthetic_native_prefix_record();
    let (_, reports) = synthetic_report_records();
    let anchors: [_; 3] = std::array::from_fn(|index| {
        NativeDatabaseAnchor::from_snapshot(
            reports[index].replica.clone(),
            &native.initial_observations.members[index].snapshot,
        )
        .unwrap()
    });
    for (index, anchor) in anchors.iter().enumerate() {
        validate_independent_native_anchor(anchor, &reports[index].replica, &native, index)
            .unwrap();
    }
    let replacement = Guid::parse(
        "synthetic rewritten native identity",
        "00000063-0000-4000-8000-000000000000",
    )
    .unwrap();
    for change in 0..4 {
        let mut rewritten = native.clone();
        for binding in [
            &mut rewritten.initial_binding,
            &mut rewritten.record_binding,
            &mut rewritten.evaluation_binding,
        ] {
            match change {
                0 => binding.members[1].local_database_id += 100,
                1 => binding.members[1].database_guid = replacement.to_string(),
                2 => binding.family_guid = replacement.to_string(),
                3 => binding.recovery_fork_id = replacement.to_string(),
                _ => unreachable!(),
            }
        }
        for samples in [
            &mut rewritten.initial_observations,
            &mut rewritten.observations,
        ] {
            for sample in &mut samples.members {
                if change <= 1 && sample.ordinal != 2 {
                    continue;
                }
                let Observation::Present { value: group, .. } =
                    &mut sample.snapshot.availability_group
                else {
                    unreachable!()
                };
                let database = &mut group.databases[0];
                let local = database.local.as_mut().unwrap();
                match change {
                    0 => {
                        local.database_id += 100;
                        database.replicas[0].database_id += 100;
                    }
                    1 => local.recovery.as_mut().unwrap().database_guid = Some(replacement.clone()),
                    2 => local.recovery.as_mut().unwrap().family_guid = Some(replacement.clone()),
                    3 => {
                        let recovery = local.recovery.as_mut().unwrap();
                        recovery.recovery_fork_guid = Some(replacement.clone());
                        recovery.first_recovery_fork_guid = Some(replacement.clone());
                        let RecoveryLineageObservation::Local { value } =
                            &mut database.replicas[0].lineage
                        else {
                            unreachable!()
                        };
                        value.recovery_fork_id = replacement.clone();
                    }
                    _ => unreachable!(),
                }
            }
        }
        assert_eq!(
            rewritten.evaluate(&members).unwrap(),
            native.evaluate(&members).unwrap(),
            "coherent native mutation {change}"
        );
        assert_eq!(
            validate_independent_native_anchor(&anchors[1], &reports[1].replica, &rewritten, 1)
                .unwrap_err(),
            "independent hosted native database anchor differs",
            "mutation {change}"
        );
    }
}
