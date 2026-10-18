use std::time::Duration;

use async_trait::async_trait;
use tokio::time::{Instant, sleep};

use super::LaunchedMembers;
use crate::fixture::admin::validated_identifier;
use crate::fixture::deadline::{complete_before, earlier};
use crate::three_replica::availability_group::{availability_group_name, database_name};
use crate::three_replica::commit_coverage::prefix::sequence::{
    NativeControlAdmission, NativeControlRequest, NativeSequenceHistory, UnadmittedNativeSurvey,
    control_complete,
};
use crate::three_replica::commit_coverage::prefix::{
    DataMovementAction, DataMovementReceipt, NativePrefixDecision, NativePrefixReport,
    NativePrefixScenario, public_coverage_blocker, target_row, validate_sample_set,
};
use crate::three_replica::commit_coverage::{
    CommitCoverageError, NativeSamplePolicy, NativeSampleSet, TransactionAcknowledgement,
    TransactionSample, collect_native_samples, collect_native_samples_for, now,
    probe_acknowledged_transaction, record_probe_outcome,
};
use crate::three_replica::data::{self, DataContext, DataError};
use crate::three_replica::{NativeTopologyBinding, ReadyMember};

const EXPERIMENT_BUDGET: Duration = Duration::from_secs(180);
const RELEASE_BUDGET: Duration = Duration::from_secs(60);
const CONTROL_BUDGET: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

struct PausedWork {
    record_binding: NativeTopologyBinding,
    acknowledgement: TransactionAcknowledgement,
    transaction_samples: Vec<TransactionSample>,
    lagging: NativePrefixScenario,
}

struct ControlledRun {
    suspend: DataMovementReceipt,
    suspended: NativeSampleSet,
    work: PausedWork,
    resume: DataMovementReceipt,
    resumed: NativeSampleSet,
}

#[async_trait(?Send)]
trait PrefixBackend {
    fn history(&self) -> &NativeSequenceHistory;
    fn history_mut(&mut self) -> &mut NativeSequenceHistory;
    fn binding(&self) -> &NativeTopologyBinding {
        self.history().current()
    }
    fn members(&self) -> &[ReadyMember; 3];
    fn target(&self) -> u8;
    async fn observe(&mut self, deadline: Instant) -> Result<NativeSampleSet, CommitCoverageError>;
    async fn survey(
        &mut self,
        deadline: Instant,
    ) -> Result<UnadmittedNativeSurvey, CommitCoverageError>;
    async fn dispatch(
        &mut self,
        action: DataMovementAction,
        deadline: Instant,
    ) -> Result<(), CommitCoverageError>;
    async fn work(&mut self, deadline: Instant) -> Result<PausedWork, CommitCoverageError>;
}

fn healthy_target(
    samples: &NativeSampleSet,
    binding: &NativeTopologyBinding,
    target: u8,
) -> Result<bool, CommitCoverageError> {
    control_complete(DataMovementAction::Resume, samples, binding, target)
}

async fn observed<B: PrefixBackend>(
    backend: &mut B,
    deadline: Instant,
) -> Result<NativeSampleSet, CommitCoverageError> {
    let samples = complete_before(deadline, CONTROL_BUDGET, backend.observe(deadline))
        .await
        .map_err(|_| DataError::Deadline)??;
    validate_sample_set(
        backend.members(),
        backend.binding(),
        backend.target(),
        &samples,
        now()?,
    )?;
    Ok(samples)
}

async fn change_data_movement<B: PrefixBackend>(
    backend: &mut B,
    action: DataMovementAction,
    deadline: Instant,
    suspend_attempted: &mut bool,
) -> Result<(DataMovementReceipt, NativeSampleSet), CommitCoverageError> {
    backend.history().require_next(action)?;
    let samples = observed(backend, deadline).await?;
    if action == DataMovementAction::Suspend
        && !healthy_target(&samples, backend.binding(), backend.target())?
    {
        return Err(CommitCoverageError::Control(
            "SUSPEND requires the exact healthy secondary",
        ));
    }
    let requested_at_unix_millis = now()?;
    let should_dispatch = action == DataMovementAction::Suspend
        || target_row(&samples, backend.binding(), backend.target())?.is_suspended == Some(true);
    if should_dispatch {
        if Instant::now() >= deadline {
            return Err(DataError::Deadline.into());
        }
        if action == DataMovementAction::Suspend {
            *suspend_attempted = true;
        }
        complete_before(deadline, CONTROL_BUDGET, backend.dispatch(action, deadline))
            .await
            .map_err(|_| DataError::Deadline)??;
        let accepted = now()?;
        if accepted < requested_at_unix_millis {
            return Err(CommitCoverageError::Clock);
        }
        let request = NativeControlRequest {
            action,
            requested_at_unix_millis,
            accepted_at_unix_millis: Some(accepted),
            before_observations: samples,
        };
        loop {
            let survey = complete_before(deadline, CONTROL_BUDGET, backend.survey(deadline))
                .await
                .map_err(|_| DataError::Deadline)??;
            let members = backend.members().clone();
            match backend
                .history_mut()
                .admit_control(&members, &request, survey, now()?)?
            {
                NativeControlAdmission::Admitted(receipt, samples) => {
                    return Ok((receipt, samples));
                }
                NativeControlAdmission::Pending => {}
            }
            if Instant::now() >= deadline {
                return Err(DataError::Deadline.into());
            }
            sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    } else {
        if !healthy_target(&samples, backend.binding(), backend.target())? {
            return Err(CommitCoverageError::Control(
                "unacknowledged resume cannot admit a healthy phase",
            ));
        }
        Ok((
            DataMovementReceipt {
                action,
                requested_at_unix_millis,
                accepted_at_unix_millis: None,
                confirmed_at_unix_millis: now()?,
            },
            samples,
        ))
    }
}

async fn controlled_run<B: PrefixBackend>(
    backend: &mut B,
    work_deadline: Instant,
    release_deadline: Instant,
) -> Result<ControlledRun, CommitCoverageError> {
    let mut attempted = false;
    let primary = async {
        let (suspend, suspended) = change_data_movement(
            backend,
            DataMovementAction::Suspend,
            earlier(work_deadline, Instant::now() + CONTROL_BUDGET),
            &mut attempted,
        )
        .await?;
        let work = complete_before(
            work_deadline,
            work_deadline.saturating_duration_since(Instant::now()),
            backend.work(work_deadline),
        )
        .await
        .map_err(|_| DataError::Deadline)??;
        Ok::<_, CommitCoverageError>((suspend, suspended, work))
    }
    .await;
    if !attempted {
        return match primary {
            Err(error) => Err(error),
            Ok(_) => Err(CommitCoverageError::Control(
                "missing SUSPEND dispatch authority",
            )),
        };
    }
    let release = change_data_movement(
        backend,
        DataMovementAction::Resume,
        earlier(release_deadline, Instant::now() + RELEASE_BUDGET),
        &mut attempted,
    )
    .await;
    match (primary, release) {
        (Ok((suspend, suspended, work)), Ok((resume, resumed))) => Ok(ControlledRun {
            suspend,
            suspended,
            work,
            resume,
            resumed,
        }),
        (Err(primary), Ok(_)) => Err(primary),
        (primary, Err(release)) => Err(CommitCoverageError::Release {
            primary: primary.err().map(Box::new),
            release: Box::new(release),
        }),
    }
}

struct LivePrefixBackend<'a> {
    owner: &'a LaunchedMembers,
    history: NativeSequenceHistory,
    primary_index: usize,
    target: u8,
    last_accepted_control: Option<DataMovementAction>,
}

impl LivePrefixBackend<'_> {
    fn context(&self, deadline: Instant) -> Result<DataContext<'_>, CommitCoverageError> {
        self.owner.native_probe_context(deadline.into_std())
    }

    fn verify_owner(&self, deadline: Instant) -> Result<(), CommitCoverageError> {
        self.owner
            .verify_native_probe_ownership(deadline.into_std())
    }

    fn scenario(
        &self,
        record_binding: &NativeTopologyBinding,
        ack: &TransactionAcknowledgement,
        transaction_samples: &[TransactionSample],
        observations: NativeSampleSet,
    ) -> Result<NativePrefixScenario, CommitCoverageError> {
        let evaluated_at_unix_millis = now()?;
        let decision = self.history.evaluate(
            &self.owner.members,
            record_binding,
            ack,
            transaction_samples,
            &observations,
            evaluated_at_unix_millis,
        )?;
        Ok(NativePrefixScenario {
            native_binding: self.history.current().clone(),
            evaluated_at_unix_millis,
            observations,
            decision,
        })
    }
}

#[async_trait(?Send)]
impl PrefixBackend for LivePrefixBackend<'_> {
    fn history(&self) -> &NativeSequenceHistory {
        &self.history
    }
    fn history_mut(&mut self) -> &mut NativeSequenceHistory {
        &mut self.history
    }
    fn members(&self) -> &[ReadyMember; 3] {
        &self.owner.members
    }
    fn target(&self) -> u8 {
        self.target
    }

    async fn observe(&mut self, deadline: Instant) -> Result<NativeSampleSet, CommitCoverageError> {
        self.verify_owner(deadline)?;
        let samples = collect_native_samples_for(
            &self.context(deadline)?,
            self.history.current(),
            NativeSamplePolicy::ControlledSecondary(self.target),
        )
        .await
        .map_err(|source| CommitCoverageError::Stage {
            name: match self.last_accepted_control {
                None => "observation before any acknowledged HADR control",
                Some(DataMovementAction::Suspend) => "observation after acknowledged SUSPEND",
                Some(DataMovementAction::Resume) => "observation after acknowledged RESUME",
            },
            source: Box::new(source),
        })?;
        self.verify_owner(deadline)?;
        Ok(samples)
    }

    async fn survey(
        &mut self,
        deadline: Instant,
    ) -> Result<UnadmittedNativeSurvey, CommitCoverageError> {
        self.verify_owner(deadline)?;
        let survey = UnadmittedNativeSurvey::collect(&self.context(deadline)?).await?;
        self.verify_owner(deadline)?;
        Ok(survey)
    }

    async fn dispatch(
        &mut self,
        action: DataMovementAction,
        deadline: Instant,
    ) -> Result<(), CommitCoverageError> {
        let target_index = self
            .history
            .current()
            .members
            .iter()
            .position(|member| member.ordinal == self.target && member.role == "SECONDARY")
            .ok_or(CommitCoverageError::Control(
                "control target is not the admitted secondary",
            ))?;
        validated_identifier(&self.history.current().database_name).map_err(DataError::Admin)?;
        let context = self.context(deadline)?;
        let mut session = data::connect(&context, target_index, "master", false, deadline).await?;
        self.verify_owner(deadline)?;
        let fresh = collect_native_samples_for(
            &context,
            self.history.current(),
            NativeSamplePolicy::ControlledSecondary(self.target),
        )
        .await
        .map_err(|source| CommitCoverageError::Stage {
            name: "HADR pre-dispatch native admission",
            source: Box::new(source),
        })?;
        validate_sample_set(
            &self.owner.members,
            self.history.current(),
            self.target,
            &fresh,
            now()?,
        )?;
        self.verify_owner(deadline)?;
        let option = match action {
            DataMovementAction::Suspend => "SUSPEND",
            DataMovementAction::Resume => "RESUME",
        };
        data::execute_batch(
            &context,
            &mut session,
            &format!(
                "ALTER DATABASE [{}] SET HADR {option};",
                self.history.current().database_name
            ),
            deadline,
        )
        .await?;
        self.last_accepted_control = Some(action);
        Ok(())
    }

    async fn work(&mut self, deadline: Instant) -> Result<PausedWork, CommitCoverageError> {
        self.verify_owner(deadline)?;
        let context = self.context(deadline)?;
        let record_binding = self.history.current().clone();
        let (acknowledgement, transaction_samples) =
            probe_acknowledged_transaction(&context, &record_binding, self.primary_index).await?;
        let samples = self.observe(deadline).await?;
        if target_row(&samples, self.history.current(), self.target)?.is_suspended != Some(true) {
            return Err(CommitCoverageError::Control(
                "selected secondary did not remain suspended",
            ));
        }
        let lagging = self.scenario(
            &record_binding,
            &acknowledgement,
            &transaction_samples,
            samples,
        )?;
        if matches!(
            lagging.decision,
            NativePrefixDecision::NativeSampledCoverage { .. }
        ) {
            return Err(CommitCoverageError::Control(
                "paused target unexpectedly covers the new record",
            ));
        }
        Ok(PausedWork {
            record_binding,
            acknowledgement,
            transaction_samples,
            lagging,
        })
    }
}

impl LaunchedMembers {
    pub async fn probe_native_prefix_coverage(
        &mut self,
        selected_target_ordinal: u8,
    ) -> Result<NativePrefixReport, CommitCoverageError> {
        let (binding, primary_index) = self.native_probe_binding(selected_target_ordinal)?;
        if binding.database_name != database_name(&self.run)
            || binding.availability_group_name != availability_group_name(&self.run)
        {
            return Err(CommitCoverageError::Control(
                "managed names do not belong to the fixture run",
            ));
        }

        let experiment_deadline = earlier(
            Instant::from_std(self.context.deadline),
            Instant::now() + EXPERIMENT_BUDGET,
        );
        if experiment_deadline.saturating_duration_since(Instant::now())
            < RELEASE_BUDGET + CONTROL_BUDGET
        {
            return Err(DataError::Deadline.into());
        }
        let work_deadline = experiment_deadline - RELEASE_BUDGET;
        self.verify_native_probe_ownership(work_deadline.into_std())?;
        let before = collect_native_samples(
            &self.native_probe_context(work_deadline.into_std())?,
            binding,
        )
        .await?;
        let history = NativeSequenceHistory::new(
            &self.members,
            binding.clone(),
            selected_target_ordinal,
            &before,
            now()?,
        )?;
        let mut backend = LivePrefixBackend {
            owner: self,
            history,
            primary_index,
            target: selected_target_ordinal,
            last_accepted_control: None,
        };
        let run = controlled_run(&mut backend, work_deadline, experiment_deadline).await?;
        if backend.history.transitions().len() != 2 {
            return Err(CommitCoverageError::Control(
                "successful experiment requires both acknowledged control phases",
            ));
        }
        let mut covered = backend.scenario(
            &run.work.record_binding,
            &run.work.acknowledgement,
            &run.work.transaction_samples,
            run.resumed,
        )?;
        while matches!(
            covered.decision,
            NativePrefixDecision::OutsideObservedPrefix { .. }
        ) {
            if Instant::now() >= experiment_deadline {
                return Err(DataError::Deadline.into());
            }
            sleep(POLL_INTERVAL.min(experiment_deadline.saturating_duration_since(Instant::now())))
                .await;
            let samples = backend.observe(experiment_deadline).await?;
            covered = backend.scenario(
                &run.work.record_binding,
                &run.work.acknowledgement,
                &run.work.transaction_samples,
                samples,
            )?;
        }
        let quiet_samples = collect_native_samples(
            &backend.context(experiment_deadline)?,
            backend.history.current(),
        )
        .await?;
        backend.verify_owner(experiment_deadline)?;
        let quiet = backend.scenario(
            &run.work.record_binding,
            &run.work.acknowledgement,
            &run.work.transaction_samples,
            quiet_samples,
        )?;
        if matches!(
            quiet.decision,
            NativePrefixDecision::OutsideObservedPrefix { .. }
        ) {
            return Err(CommitCoverageError::Control(
                "quiet sample does not cover the acknowledged record",
            ));
        }
        let record_probe = record_probe_outcome(&run.work.transaction_samples)?;
        Ok(NativePrefixReport {
            scope: "fixture_native_only",
            fixture_run_id: self.run.run_id.clone(),
            fixture_resource_uid: self.run.resource_uid.clone(),
            initial_native_binding: backend.history.initial().clone(),
            record_native_binding: run.work.record_binding,
            sequence_transitions: backend.history.transitions().to_vec(),
            primary_ordinal: binding.members[primary_index].ordinal,
            selected_target_ordinal,
            acknowledgement: run.work.acknowledgement,
            transaction_samples: run.work.transaction_samples,
            record_probe,
            before,
            suspended: run.suspended,
            controls: [run.suspend, run.resume],
            lagging: run.work.lagging,
            covered,
            quiet,
            public_coverage: public_coverage_blocker(),
        })
    }
}

#[cfg(test)]
mod tests;
