use kuberic_mssql::observation::{NativeProvenance, RecoveryLineageObservation};
use kuberic_mssql::{DatabaseLineage, Guid, NativeRole};

use super::*;
use crate::three_replica::commit_coverage::TransactionRecordRow;
use crate::three_replica::commit_coverage::tests::bound_prefix_fixture;

pub(in crate::three_replica) fn receipt(
    record: Option<&str>,
) -> (TransactionAcknowledgement, Vec<TransactionSample>) {
    let ack = TransactionAcknowledgement {
        marker_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        marker_value: "fixture".to_owned(),
        transaction_id: "123".to_owned(),
        database_id: 5,
        delayed_durability: "DISABLED".to_owned(),
        commit_requested_at_unix_millis: 900,
        acknowledged_at_unix_millis: 950,
    };
    let row = |state, value: Option<&str>| {
        Some(TransactionRecordRow {
            transaction_id: ack.transaction_id.clone(),
            database_id: ack.database_id,
            state,
            commit_lsn: value.map(|value| DecimalProgress::parse(value).unwrap()),
        })
    };
    let samples = vec![
        TransactionSample {
            phase: "before_commit",
            requested_at_unix_millis: 880,
            completed_at_unix_millis: 890,
            row: row(4, None),
        },
        TransactionSample {
            phase: "overlap",
            requested_at_unix_millis: 901,
            completed_at_unix_millis: 902,
            row: row(12, record),
        },
        TransactionSample {
            phase: "after_ack",
            requested_at_unix_millis: 951,
            completed_at_unix_millis: 952,
            row: None,
        },
        TransactionSample {
            phase: "after_ack",
            requested_at_unix_millis: 953,
            completed_at_unix_millis: 954,
            row: None,
        },
    ];
    (ack, samples)
}

fn target_mut(samples: &mut NativeSampleSet) -> &mut DatabaseReplicaSnapshot {
    let Observation::Present { value: group, .. } =
        &mut samples.members[1].snapshot.availability_group
    else {
        panic!()
    };
    &mut group.databases[0].replicas[0]
}

#[test]
fn native_prefix_is_strict_exact_and_redo_independent() {
    for (record, hardened, covered) in [
        ("100", "101", true),
        ("100", "100", false),
        ("100", "99", false),
        (
            "9999999999999999999999998",
            "9999999999999999999999999",
            true,
        ),
    ] {
        for redo in [None, Some("0"), Some("9999999999999999999999999")] {
            let (mut samples, members, binding) = bound_prefix_fixture();
            let row = target_mut(&mut samples);
            row.progress.hardened_block = Some(DecimalProgress::parse(hardened).unwrap());
            row.progress.redone_record = redo.map(|value| DecimalProgress::parse(value).unwrap());
            row.progress.committed_record = None;
            let (ack, rows) = receipt(Some(record));
            let decision =
                evaluate_prefix(&members, &binding, 2, &ack, &rows, &samples, 1200).unwrap();
            assert_eq!(
                matches!(decision, NativePrefixDecision::NativeSampledCoverage { .. }),
                covered
            );
            if !covered {
                assert!(matches!(
                    decision,
                    NativePrefixDecision::OutsideObservedPrefix { .. }
                ));
            }
        }
    }
}

#[test]
fn missing_record_or_local_boundary_remains_unavailable() {
    for record in [None, Some("0")] {
        let (samples, members, binding) = bound_prefix_fixture();
        let (ack, rows) = receipt(record);
        assert!(matches!(
            evaluate_prefix(&members, &binding, 2, &ack, &rows, &samples, 1200).unwrap(),
            NativePrefixDecision::Unavailable { .. },
        ));
    }
    for hardened in [None, Some("0")] {
        let (mut samples, members, binding) = bound_prefix_fixture();
        target_mut(&mut samples).progress.hardened_block =
            hardened.map(|value| DecimalProgress::parse(value).unwrap());
        let (ack, rows) = receipt(Some("100"));
        assert!(matches!(
            evaluate_prefix(&members, &binding, 2, &ack, &rows, &samples, 1200).unwrap(),
            NativePrefixDecision::Unavailable { .. },
        ));
    }
}

#[test]
fn primary_receipt_is_revalidated_and_target_dbid_is_member_local() {
    let (samples, members, binding) = bound_prefix_fixture();
    let (ack, rows) = receipt(Some("100"));
    assert_ne!(
        binding.members[0].local_database_id,
        binding.members[1].local_database_id
    );
    assert_ne!(
        binding.members[0].database_guid,
        binding.members[1].database_guid
    );
    evaluate_prefix(&members, &binding, 2, &ack, &rows, &samples, 1200).unwrap();
    for mutation in 0..6 {
        let mut ack = ack.clone();
        let mut rows = rows.clone();
        match mutation {
            0 => ack.database_id = binding.members[1].local_database_id as i32,
            1 => rows[1].row.as_mut().unwrap().transaction_id = "other".to_owned(),
            2 => rows[1].row.as_mut().unwrap().database_id += 1,
            3 => ack.delayed_durability = "ALLOWED".to_owned(),
            4 => rows[0].row = rows[1].row.clone(),
            5 => rows[3].requested_at_unix_millis = 949,
            _ => unreachable!(),
        }
        assert!(evaluate_prefix(&members, &binding, 2, &ack, &rows, &samples, 1200).is_err());
    }
}

#[test]
fn target_evidence_must_be_fresh_current_and_post_ack() {
    let (samples, members, binding) = bound_prefix_fixture();
    let (ack, rows) = receipt(Some("100"));
    for mutation in 0..5 {
        let mut samples = samples.clone();
        let evaluation = match mutation {
            0 => {
                samples.requested_at_unix_millis = 949;
                1200
            }
            1 => {
                samples.requested_at_unix_millis = 1001;
                1200
            }
            2 => 1099,
            3 => 31_001,
            4 => {
                samples.members[1].collection_completed_at_unix_millis = 999;
                1200
            }
            _ => unreachable!(),
        };
        assert!(evaluate_prefix(&members, &binding, 2, &ack, &rows, &samples, evaluation).is_err());
    }
}

#[test]
fn selected_control_policy_never_waives_native_identity_or_other_members_health() {
    let (base, members, binding) = bound_prefix_fixture();
    let (ack, rows) = receipt(Some("100"));
    let foreign = Guid::parse("test", "99999999-0000-4000-8000-000000000000").unwrap();
    for mutation in 0..8 {
        let mut samples = base.clone();
        let Observation::Present { value: group, .. } =
            &mut samples.members[1].snapshot.availability_group
        else {
            panic!()
        };
        match mutation {
            0 => group.local_replica.role = Some(NativeRole::Primary),
            1 => group.databases[0].replicas[0].provenance = NativeProvenance::PrimaryRemote,
            2 => {
                group.databases[0].replicas[0].lineage = RecoveryLineageObservation::Local {
                    value: DatabaseLineage {
                        database: group.databases[0].identity.clone(),
                        recovery_fork_id: foreign.clone(),
                    },
                }
            }
            3 => group.databases[0].local.as_mut().unwrap().database_id += 1,
            4 => {
                group.databases[0]
                    .local
                    .as_mut()
                    .unwrap()
                    .recovery
                    .as_mut()
                    .unwrap()
                    .database_guid = Some(foreign.clone())
            }
            5 => {
                group.configuration_sequence =
                    kuberic_mssql::ConfigurationSequence::parse("43").unwrap()
            }
            6 => samples.members[1]
                .snapshot
                .instance
                .sqlserver_start_time
                .push('1'),
            7 => {
                let Observation::Present { value: third, .. } =
                    &mut samples.members[2].snapshot.availability_group
                else {
                    panic!()
                };
                third.databases[0].replicas[0].is_suspended = Some(true);
            }
            _ => unreachable!(),
        }
        assert!(evaluate_prefix(&members, &binding, 2, &ack, &rows, &samples, 1200).is_err());
    }
    let mut paused = base;
    let row = target_mut(&mut paused);
    row.is_suspended = Some(true);
    row.synchronization_state = Some("SYNCHRONIZING".to_owned());
    row.synchronization_health = Some("NOT_HEALTHY".to_owned());
    evaluate_prefix(&members, &binding, 2, &ack, &rows, &paused, 1200).unwrap();
}
