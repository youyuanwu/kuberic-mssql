use std::cell::Cell;

use kuberic_mssql::Observation;

use super::*;
use crate::three_replica::commit_coverage::tests::bound_prefix_fixture;

fn fixture(stamp: u64) -> (NativeSampleSet, [ReadyMember; 3], NativeTopologyBinding) {
    let (mut samples, members, binding) = bound_prefix_fixture();
    samples.requested_at_unix_millis = stamp;
    samples.completed_at_unix_millis = stamp;
    for sample in &mut samples.members {
        sample.snapshot.observed_at_unix_millis = stamp;
        sample.collection_completed_at_unix_millis = stamp;
        let Observation::Present {
            observed_at_unix_millis,
            ..
        } = &mut sample.snapshot.availability_group
        else {
            panic!()
        };
        *observed_at_unix_millis = stamp;
    }
    (samples, members, binding)
}

fn change_target(samples: &mut NativeSampleSet, suspended: bool, healthy: bool) {
    let Observation::Present { value: group, .. } =
        &mut samples.members[1].snapshot.availability_group
    else {
        panic!()
    };
    let row = &mut group.databases[0].replicas[0];
    row.is_suspended = Some(suspended);
    if !healthy {
        row.synchronization_state = Some("SYNCHRONIZING".to_owned());
    }
}

#[tokio::test(start_paused = true)]
async fn latest_paused_or_unhealthy_target_prevents_suspend_sql() {
    for suspended in [true, false] {
        let (initial, _, binding) = fixture(1000);
        assert!(healthy_target(&initial, &binding, 2).unwrap());
        let (mut latest, members, binding) = fixture(2000);
        change_target(&mut latest, suspended, false);
        let sent = Cell::new(false);
        let current = Instant::now();
        let result = dispatch_after_owner_check(
            FinalControlAdmission {
                members: &members,
                binding: &binding,
                target: 2,
                action: DataMovementAction::Suspend,
                samples: &latest,
                deadline: current + CONTROL_BUDGET,
                work_deadline: current + Duration::from_secs(60),
            },
            || Ok(()),
            || Ok((2000, current)),
            || async {
                sent.set(true);
                Ok(())
            },
        )
        .await;
        assert!(result.is_err());
        assert!(!sent.get());
    }
}

#[tokio::test(start_paused = true)]
async fn final_owner_inspection_cannot_make_resume_native_evidence_stale() {
    for seconds in [1, 31] {
        let (mut samples, members, binding) = fixture(1000);
        change_target(&mut samples, true, false);
        let current = Instant::now();
        let clock = Cell::new((1000, current));
        let sent = Cell::new(false);
        let result = dispatch_after_owner_check(
            FinalControlAdmission {
                members: &members,
                binding: &binding,
                target: 2,
                action: DataMovementAction::Resume,
                samples: &samples,
                deadline: current + RELEASE_BUDGET,
                work_deadline: current + Duration::from_secs(60),
            },
            || {
                clock.set((
                    1000 + seconds * 1000,
                    current + Duration::from_secs(seconds),
                ));
                Ok(())
            },
            || Ok(clock.get()),
            || async {
                sent.set(true);
                Ok(())
            },
        )
        .await;
        assert_eq!(result.is_ok(), seconds == 1);
        assert_eq!(sent.get(), seconds == 1);
    }
}

#[tokio::test(start_paused = true)]
async fn preparation_must_leave_minimum_work_before_the_separate_release_reserve() {
    for seconds in [5, 15] {
        let (samples, members, binding) = fixture(1000);
        let current = Instant::now();
        let clock = Cell::new((1000, current));
        let sent = Cell::new(false);
        let result = dispatch_after_owner_check(
            FinalControlAdmission {
                members: &members,
                binding: &binding,
                target: 2,
                action: DataMovementAction::Suspend,
                samples: &samples,
                deadline: current + CONTROL_BUDGET,
                work_deadline: current + Duration::from_secs(40),
            },
            || {
                clock.set((
                    1000 + seconds * 1000,
                    current + Duration::from_secs(seconds),
                ));
                Ok(())
            },
            || Ok(clock.get()),
            || async {
                sent.set(true);
                Ok(())
            },
        )
        .await;
        assert_eq!(result.is_ok(), seconds == 5);
        assert_eq!(sent.get(), seconds == 5);
    }
}
