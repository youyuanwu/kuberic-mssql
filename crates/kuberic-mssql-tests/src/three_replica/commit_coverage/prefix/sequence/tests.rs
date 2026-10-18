use kuberic_mssql::{Guid, Observation};

use super::super::tests::receipt;
use super::*;
use crate::three_replica::commit_coverage::tests::bound_prefix_fixture;

fn phase_samples(sequence: [i64; 3], stamp: u64, suspended: bool) -> NativeSampleSet {
    let (mut samples, _, _) = bound_prefix_fixture();
    samples.requested_at_unix_millis = stamp;
    samples.completed_at_unix_millis = stamp + 10;
    for (index, sample) in samples.members.iter_mut().enumerate() {
        sample.snapshot.observed_at_unix_millis = stamp + 1;
        sample.collection_completed_at_unix_millis = stamp + 9;
        let Observation::Present {
            value: group,
            observed_at_unix_millis,
        } = &mut sample.snapshot.availability_group
        else {
            panic!()
        };
        *observed_at_unix_millis = stamp + 1;
        group.configuration_sequence =
            kuberic_mssql::ConfigurationSequence::parse(&sequence[index].to_string()).unwrap();
        if sample.ordinal == 2 {
            group.databases[0].replicas[0].is_suspended = Some(suspended);
            if suspended {
                group.databases[0].replicas[0].synchronization_state =
                    Some("SYNCHRONIZING".to_owned());
            }
        }
    }
    samples
}

fn suspend_request() -> NativeControlRequest {
    NativeControlRequest {
        action: DataMovementAction::Suspend,
        requested_at_unix_millis: 1320,
        accepted_at_unix_millis: Some(1330),
        before_observations: phase_samples([42; 3], 1300, false),
    }
}

#[test]
fn post_control_sequence_is_not_admitted_until_all_members_and_native_state_agree() {
    let (initial, members, binding) = bound_prefix_fixture();
    let mut history =
        NativeSequenceHistory::new(&members, binding.clone(), 2, &initial, 1200).unwrap();
    for samples in [
        phase_samples([43, 42, 43], 1340, true),
        phase_samples([43; 3], 1340, false),
    ] {
        assert!(matches!(
            history
                .admit_control(
                    &members,
                    &suspend_request(),
                    UnadmittedNativeSurvey::scripted(samples),
                    1360,
                )
                .unwrap(),
            NativeControlAdmission::Pending
        ));
        assert_eq!(history.current(), &binding);
        assert!(history.transitions().is_empty());
    }
    assert!(matches!(
        history
            .admit_control(
                &members,
                &suspend_request(),
                UnadmittedNativeSurvey::scripted(phase_samples([47; 3], 1340, true)),
                1360,
            )
            .unwrap(),
        NativeControlAdmission::Admitted(_, _)
    ));
    assert_eq!(history.current().configuration_sequence, 47);
    assert_eq!(history.transitions()[0].before_binding, binding);
    assert_eq!(history.transitions()[0].after_binding, *history.current());
}

#[test]
fn missing_ack_regression_or_changed_anchors_never_refresh_a_native_phase() {
    let (initial, members, binding) = bound_prefix_fixture();
    for mutation in 0..8 {
        let mut history =
            NativeSequenceHistory::new(&members, binding.clone(), 2, &initial, 1200).unwrap();
        let mut request = suspend_request();
        let mut samples = phase_samples([43; 3], 1340, true);
        match mutation {
            0 => request.accepted_at_unix_millis = None,
            1 => samples = phase_samples([41; 3], 1340, true),
            2 => request.before_observations = phase_samples([43; 3], 1300, false),
            3 => samples.members[1]
                .snapshot
                .instance
                .sqlserver_start_time
                .push('1'),
            4 => {
                let Observation::Present { value: group, .. } =
                    &mut samples.members[1].snapshot.availability_group
                else {
                    panic!()
                };
                group.databases[0]
                    .local
                    .as_mut()
                    .unwrap()
                    .recovery
                    .as_mut()
                    .unwrap()
                    .family_guid =
                    Some(Guid::parse("test", "99999999-0000-4000-8000-000000000000").unwrap());
            }
            5 => request.action = DataMovementAction::Resume,
            6 => {
                let Observation::Present { value: group, .. } =
                    &mut samples.members[1].snapshot.availability_group
                else {
                    panic!()
                };
                group.required_synchronized_secondaries_to_commit = 2;
            }
            7 => {
                let Observation::Present { value: group, .. } =
                    &mut samples.members[1].snapshot.availability_group
                else {
                    panic!()
                };
                group.replicas[2].replica_id =
                    Guid::parse("test", "99999999-0000-4000-8000-000000000000").unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            history
                .admit_control(
                    &members,
                    &request,
                    UnadmittedNativeSurvey::scripted(samples),
                    1360
                )
                .is_err()
        );
        assert_eq!(history.current(), &binding);
        assert!(history.transitions().is_empty());
    }
}

#[test]
fn source_record_and_later_prefix_must_belong_to_the_known_control_chain() {
    let (initial, members, binding) = bound_prefix_fixture();
    let mut history = NativeSequenceHistory::new(&members, binding, 2, &initial, 1200).unwrap();
    history
        .admit_control(
            &members,
            &suspend_request(),
            UnadmittedNativeSurvey::scripted(phase_samples([43; 3], 1340, true)),
            1360,
        )
        .unwrap();
    let record_binding = history.current().clone();
    let (mut ack, mut rows) = receipt(Some("100"));
    ack.commit_requested_at_unix_millis += 1000;
    ack.acknowledged_at_unix_millis += 1000;
    for row in &mut rows {
        row.requested_at_unix_millis += 1000;
        row.completed_at_unix_millis += 1000;
    }
    history
        .admit_control(
            &members,
            &NativeControlRequest {
                action: DataMovementAction::Resume,
                requested_at_unix_millis: 2120,
                accepted_at_unix_millis: Some(2130),
                before_observations: phase_samples([43; 3], 2100, true),
            },
            UnadmittedNativeSurvey::scripted(phase_samples([44; 3], 2140, false)),
            2160,
        )
        .unwrap();
    let samples = phase_samples([44; 3], 3000, false);
    assert!(matches!(
        history
            .evaluate(&members, &record_binding, &ack, &rows, &samples, 3020,)
            .unwrap(),
        NativePrefixDecision::NativeSampledCoverage { .. }
    ));
    let mut forged = record_binding.clone();
    forged.configuration_sequence = 45;
    assert!(
        history
            .evaluate(&members, &forged, &ack, &rows, &samples, 3020)
            .is_err()
    );
    assert!(
        history
            .evaluate(&members, history.current(), &ack, &rows, &samples, 3020)
            .is_err()
    );
    ack.acknowledged_at_unix_millis = 2200;
    assert!(
        history
            .evaluate(&members, &record_binding, &ack, &rows, &samples, 3020)
            .is_err()
    );
}
