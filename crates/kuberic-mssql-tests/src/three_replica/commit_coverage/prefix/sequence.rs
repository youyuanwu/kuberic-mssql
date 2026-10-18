use serde::Serialize;

use super::{
    CommitCoverageError, DataMovementAction, DataMovementReceipt, NativePrefixDecision,
    NativeSampleSet, NativeTopologyBinding, ReadyMember, SequenceCheck, TransactionAcknowledgement,
    TransactionSample, evaluate_prefix, sample_sequence, target_row, validate_sample_set,
    validate_sample_set_inner,
};
use crate::three_replica::commit_coverage::{now, observe_native_member};
use crate::three_replica::data::DataContext;

pub(in crate::three_replica) struct UnadmittedNativeSurvey(NativeSampleSet);

impl UnadmittedNativeSurvey {
    pub(in crate::three_replica) async fn collect(
        context: &DataContext<'_>,
    ) -> Result<Self, CommitCoverageError> {
        let requested_at_unix_millis = now()?;
        let mut members = Vec::with_capacity(3);
        for member in context.members {
            members.push(observe_native_member(context, member).await?);
        }
        Ok(Self(NativeSampleSet {
            requested_at_unix_millis,
            completed_at_unix_millis: now()?,
            members,
        }))
    }

    #[cfg(test)]
    pub(in crate::three_replica) fn scripted(samples: NativeSampleSet) -> Self {
        Self(samples)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeSequenceTransition {
    pub receipt: DataMovementReceipt,
    pub before_binding: NativeTopologyBinding,
    pub before_observations: NativeSampleSet,
    pub after_binding: NativeTopologyBinding,
    pub after_observations: NativeSampleSet,
}

pub(in crate::three_replica) struct NativeControlRequest {
    pub action: DataMovementAction,
    pub requested_at_unix_millis: u64,
    pub accepted_at_unix_millis: Option<u64>,
    pub before_observations: NativeSampleSet,
}

pub(in crate::three_replica) enum NativeControlAdmission {
    Pending,
    Admitted(DataMovementReceipt, NativeSampleSet),
}

pub(in crate::three_replica) struct NativeSequenceHistory {
    initial: NativeTopologyBinding,
    current: NativeTopologyBinding,
    admitted_at_unix_millis: u64,
    target: u8,
    transitions: Vec<NativeSequenceTransition>,
}

impl NativeSequenceHistory {
    pub(in crate::three_replica) fn new(
        members: &[ReadyMember; 3],
        binding: NativeTopologyBinding,
        target: u8,
        initial: &NativeSampleSet,
        evaluated_at: u64,
    ) -> Result<Self, CommitCoverageError> {
        validate_sample_set(members, &binding, target, initial, evaluated_at)?;
        if binding.configuration_sequence <= 0
            || !control_complete(DataMovementAction::Resume, initial, &binding, target)?
        {
            return Err(CommitCoverageError::Control(
                "initial native phase is not healthy and configured",
            ));
        }
        Ok(Self {
            initial: binding.clone(),
            current: binding,
            admitted_at_unix_millis: evaluated_at,
            target,
            transitions: Vec::with_capacity(2),
        })
    }

    pub(in crate::three_replica) fn current(&self) -> &NativeTopologyBinding {
        &self.current
    }

    pub(in crate::three_replica) fn initial(&self) -> &NativeTopologyBinding {
        &self.initial
    }

    pub(in crate::three_replica) fn transitions(&self) -> &[NativeSequenceTransition] {
        &self.transitions
    }

    pub(in crate::three_replica) fn require_next(
        &self,
        action: DataMovementAction,
    ) -> Result<(), CommitCoverageError> {
        if matches!(
            (self.transitions.len(), action),
            (0, DataMovementAction::Suspend) | (1, DataMovementAction::Resume)
        ) {
            Ok(())
        } else {
            Err(CommitCoverageError::Control(
                "control has no admitted predecessor phase",
            ))
        }
    }

    pub(in crate::three_replica) fn admit_control(
        &mut self,
        members: &[ReadyMember; 3],
        request: &NativeControlRequest,
        survey: UnadmittedNativeSurvey,
        evaluated_at: u64,
    ) -> Result<NativeControlAdmission, CommitCoverageError> {
        self.require_next(request.action)?;
        let accepted = request
            .accepted_at_unix_millis
            .ok_or(CommitCoverageError::Control(
                "unacknowledged control cannot admit a native phase",
            ))?;
        if request.requested_at_unix_millis < self.admitted_at_unix_millis
            || accepted < request.requested_at_unix_millis
            || survey.0.requested_at_unix_millis < accepted
            || accepted > evaluated_at
        {
            return Err(CommitCoverageError::Clock);
        }
        validate_sample_set(
            members,
            &self.current,
            self.target,
            &request.before_observations,
            request.requested_at_unix_millis,
        )?;
        validate_sample_set_inner(
            members,
            &self.current,
            self.target,
            &survey.0,
            evaluated_at,
            SequenceCheck::UnadmittedSurvey,
        )?;
        let sequence = sample_sequence(&survey.0.members[0])?;
        for sample in &survey.0.members {
            if sample_sequence(sample)? != sequence {
                return Ok(NativeControlAdmission::Pending);
            }
        }
        let candidate = NativeTopologyBinding {
            configuration_sequence: sequence,
            ..self.current.clone()
        };
        validate_sample_set(members, &candidate, self.target, &survey.0, evaluated_at)?;
        if !control_complete(request.action, &survey.0, &candidate, self.target)? {
            return Ok(NativeControlAdmission::Pending);
        }
        let receipt = DataMovementReceipt {
            action: request.action,
            requested_at_unix_millis: request.requested_at_unix_millis,
            accepted_at_unix_millis: Some(accepted),
            confirmed_at_unix_millis: evaluated_at,
        };
        self.transitions.push(NativeSequenceTransition {
            receipt: receipt.clone(),
            before_binding: self.current.clone(),
            before_observations: request.before_observations.clone(),
            after_binding: candidate.clone(),
            after_observations: survey.0.clone(),
        });
        self.current = candidate;
        self.admitted_at_unix_millis = evaluated_at;
        Ok(NativeControlAdmission::Admitted(receipt, survey.0))
    }

    pub(in crate::three_replica) fn evaluate(
        &self,
        members: &[ReadyMember; 3],
        record_binding: &NativeTopologyBinding,
        ack: &TransactionAcknowledgement,
        rows: &[TransactionSample],
        samples: &NativeSampleSet,
        evaluated_at: u64,
    ) -> Result<NativePrefixDecision, CommitCoverageError> {
        let suspend = self
            .transitions
            .first()
            .ok_or(CommitCoverageError::Control(
                "record has no admitted suspended phase",
            ))?;
        if record_binding != &suspend.after_binding
            || ack.commit_requested_at_unix_millis < suspend.receipt.confirmed_at_unix_millis
            || self.transitions.get(1).is_some_and(|resume| {
                ack.acknowledged_at_unix_millis > resume.receipt.requested_at_unix_millis
            })
        {
            return Err(CommitCoverageError::Control(
                "workload does not belong to the admitted control phase",
            ));
        }
        evaluate_prefix(
            members,
            &self.current,
            self.target,
            ack,
            rows,
            samples,
            evaluated_at,
        )
    }
}

pub(in crate::three_replica) fn control_complete(
    action: DataMovementAction,
    samples: &NativeSampleSet,
    binding: &NativeTopologyBinding,
    target: u8,
) -> Result<bool, CommitCoverageError> {
    let row = target_row(samples, binding, target)?;
    Ok(match action {
        DataMovementAction::Suspend => row.is_suspended == Some(true),
        DataMovementAction::Resume => {
            row.is_suspended == Some(false)
                && row.synchronization_state.as_deref() == Some("SYNCHRONIZED")
                && row.synchronization_health.as_deref() == Some("HEALTHY")
        }
    })
}

#[cfg(test)]
mod tests;
