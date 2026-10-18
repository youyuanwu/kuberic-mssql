use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::FutureExt;
use kuberic_mssql::Observation;

use super::*;
use crate::three_replica::commit_coverage::tests::bound_prefix_fixture;

enum Step {
    Observe { suspended: bool, healthy: bool },
    ObserveFailure,
    Restarted,
    Advanced,
    Disagreement,
    Versioned { sequence: i64, suspended: bool },
    Pending,
    Panic,
    Dispatch(DataMovementAction, bool),
    WorkFailure,
}

struct Scripted {
    history: NativeSequenceHistory,
    members: [ReadyMember; 3],
    target: u8,
    steps: VecDeque<Step>,
    dispatched: Vec<DataMovementAction>,
    dropped: Arc<AtomicUsize>,
}

impl Scripted {
    fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        let (initial, members, binding) = bound_prefix_fixture();
        let history = NativeSequenceHistory::new(&members, binding, 2, &initial, 1200).unwrap();
        Self {
            history,
            members,
            target: 2,
            steps: steps.into_iter().collect(),
            dispatched: vec![],
            dropped: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct PendingDrop(Arc<AtomicUsize>);

impl Drop for PendingDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait(?Send)]
impl PrefixBackend for Scripted {
    fn history(&self) -> &NativeSequenceHistory {
        &self.history
    }
    fn history_mut(&mut self) -> &mut NativeSequenceHistory {
        &mut self.history
    }
    fn members(&self) -> &[ReadyMember; 3] {
        &self.members
    }
    fn target(&self) -> u8 {
        self.target
    }

    async fn observe(&mut self, _: Instant) -> Result<NativeSampleSet, CommitCoverageError> {
        let step = self.steps.pop_front().expect("scripted observation");
        let (suspended, healthy, restarted, sequence) = match step {
            Step::Observe { suspended, healthy } => (suspended, healthy, false, [42; 3]),
            Step::Restarted => (false, true, true, [42; 3]),
            Step::Advanced => (false, true, false, [43; 3]),
            Step::Disagreement => (true, false, false, [43, 42, 43]),
            Step::Versioned {
                sequence,
                suspended,
            } => (suspended, !suspended, false, [sequence; 3]),
            Step::ObserveFailure => return Err(CommitCoverageError::Control("owner replaced")),
            Step::Pending => {
                let _guard = PendingDrop(self.dropped.clone());
                return std::future::pending().await;
            }
            Step::Panic => panic!("controlled observation panic"),
            _ => panic!("unexpected observation step"),
        };
        let (mut samples, _, _) = bound_prefix_fixture();
        let stamp = now()?;
        samples.requested_at_unix_millis = stamp;
        samples.completed_at_unix_millis = stamp;
        for sample in &mut samples.members {
            sample.collection_completed_at_unix_millis = stamp;
            sample.snapshot.observed_at_unix_millis = stamp;
            let Observation::Present {
                value: group,
                observed_at_unix_millis,
            } = &mut sample.snapshot.availability_group
            else {
                panic!()
            };
            *observed_at_unix_millis = stamp;
            group.configuration_sequence = kuberic_mssql::ConfigurationSequence::parse(
                &sequence[usize::from(sample.ordinal - 1)].to_string(),
            )
            .unwrap();
            if sample.ordinal == self.target {
                group.databases[0].replicas[0].is_suspended = Some(suspended);
                if !healthy {
                    group.databases[0].replicas[0].synchronization_state =
                        Some("SYNCHRONIZING".to_owned());
                }
                if restarted {
                    sample.snapshot.instance.sqlserver_start_time.push('1');
                }
            }
        }
        Ok(samples)
    }

    async fn survey(
        &mut self,
        deadline: Instant,
    ) -> Result<UnadmittedNativeSurvey, CommitCoverageError> {
        self.observe(deadline)
            .await
            .map(UnadmittedNativeSurvey::scripted)
    }

    async fn dispatch(
        &mut self,
        action: DataMovementAction,
        _: Instant,
    ) -> Result<(), CommitCoverageError> {
        let Step::Dispatch(expected, succeeds) = self.steps.pop_front().expect("scripted dispatch")
        else {
            panic!()
        };
        assert_eq!(action, expected);
        self.dispatched.push(action);
        if succeeds {
            Ok(())
        } else {
            Err(CommitCoverageError::Control("resume failed"))
        }
    }

    async fn work(&mut self, _: Instant) -> Result<PausedWork, CommitCoverageError> {
        assert!(matches!(self.steps.pop_front(), Some(Step::WorkFailure)));
        Err(CommitCoverageError::Control("work failed"))
    }
}

fn state(suspended: bool, healthy: bool) -> Step {
    Step::Observe { suspended, healthy }
}

#[tokio::test(start_paused = true)]
async fn command_acceptance_requires_native_completion_and_resume_health() {
    let mut backend = Scripted::new([
        state(false, true),
        Step::Dispatch(DataMovementAction::Suspend, true),
        state(false, true),
        state(true, false),
        state(true, false),
        Step::Dispatch(DataMovementAction::Resume, true),
        state(false, false),
        state(false, true),
    ]);
    let mut attempted = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    let (suspend, _) = change_data_movement(
        &mut backend,
        DataMovementAction::Suspend,
        deadline,
        &mut attempted,
    )
    .await
    .unwrap();
    let (resume, _) = change_data_movement(
        &mut backend,
        DataMovementAction::Resume,
        deadline,
        &mut attempted,
    )
    .await
    .unwrap();
    assert!(attempted);
    assert!(suspend.accepted_at_unix_millis.is_some());
    assert!(resume.accepted_at_unix_millis.is_some());
    assert!(backend.steps.is_empty());
    assert_eq!(
        backend.dispatched,
        [DataMovementAction::Suspend, DataMovementAction::Resume]
    );
}

#[tokio::test(start_paused = true)]
async fn real_control_seam_admits_only_post_ack_consensus_not_any_higher_sample() {
    let mut backend = Scripted::new([
        state(false, true),
        Step::Dispatch(DataMovementAction::Suspend, true),
        Step::Disagreement,
        Step::Versioned {
            sequence: 43,
            suspended: true,
        },
    ]);
    let mut attempted = false;
    change_data_movement(
        &mut backend,
        DataMovementAction::Suspend,
        Instant::now() + Duration::from_secs(5),
        &mut attempted,
    )
    .await
    .unwrap();
    assert_eq!(backend.history.current().configuration_sequence, 43);
    assert_eq!(backend.history.transitions().len(), 1);
    assert!(backend.steps.is_empty());
    assert!(attempted);
    assert_eq!(backend.dispatched, [DataMovementAction::Suspend]);
}

#[tokio::test(start_paused = true)]
async fn wrong_target_owner_or_incarnation_never_dispatches_control() {
    for kind in 0..4 {
        let mut backend = Scripted::new([match kind {
            0 => state(false, true),
            1 => Step::ObserveFailure,
            2 => Step::Restarted,
            _ => Step::Advanced,
        }]);
        if kind == 0 {
            backend.target = 1;
        }
        assert!(
            controlled_run(
                &mut backend,
                Instant::now() + Duration::from_secs(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .is_err()
        );
        assert!(backend.dispatched.is_empty());
    }
    let mut backend = Scripted::new([]);
    let mut attempted = false;
    assert!(
        change_data_movement(
            &mut backend,
            DataMovementAction::Suspend,
            Instant::now(),
            &mut attempted
        )
        .await
        .is_err()
    );
    assert!(!attempted);
    assert!(backend.dispatched.is_empty());
}

#[tokio::test(start_paused = true)]
async fn original_and_resume_errors_survive_real_control_orchestration() {
    let mut backend = Scripted::new([
        state(false, true),
        Step::Dispatch(DataMovementAction::Suspend, true),
        state(true, false),
        Step::WorkFailure,
        state(true, false),
        Step::Dispatch(DataMovementAction::Resume, false),
    ]);
    let error = controlled_run(
        &mut backend,
        Instant::now() + Duration::from_secs(5),
        Instant::now() + Duration::from_secs(10),
    )
    .await
    .err()
    .expect("combined control failure")
    .to_string();
    assert!(error.contains("work failed"));
    assert!(error.contains("resume failed"));
    assert!(backend.steps.is_empty());
    assert_eq!(backend.dispatched.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_suspend_cannot_grant_release_phase_or_erase_timeout() {
    let mut backend = Scripted::new([
        state(false, true),
        Step::Dispatch(DataMovementAction::Suspend, true),
        Step::Pending,
    ]);
    let result = controlled_run(
        &mut backend,
        Instant::now() + Duration::from_millis(10),
        Instant::now() + Duration::from_secs(5),
    )
    .await;
    assert!(matches!(result, Err(CommitCoverageError::Release {
        primary: Some(primary), release,
    }) if matches!(*primary, CommitCoverageError::Data(DataError::Deadline))
        && matches!(*release, CommitCoverageError::Control(_))));
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 1);
    assert!(backend.steps.is_empty());
    assert_eq!(backend.dispatched, [DataMovementAction::Suspend]);
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_panic_leave_no_escaped_control_future() {
    for panic in [false, true] {
        let mut backend = Scripted::new([
            state(false, true),
            Step::Dispatch(DataMovementAction::Suspend, true),
            if panic { Step::Panic } else { Step::Pending },
        ]);
        let future = controlled_run(
            &mut backend,
            Instant::now() + Duration::from_secs(5),
            Instant::now() + Duration::from_secs(10),
        );
        let result = tokio::time::timeout(
            Duration::from_millis(10),
            std::panic::AssertUnwindSafe(future).catch_unwind(),
        )
        .await;
        assert!(matches!(result, Err(_) | Ok(Err(_))));
        assert_eq!(backend.dispatched, [DataMovementAction::Suspend]);
        assert_eq!(backend.dropped.load(Ordering::SeqCst), usize::from(!panic));
    }
}
