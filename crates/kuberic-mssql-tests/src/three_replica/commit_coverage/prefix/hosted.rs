//! In-memory native evidence for the offline hosted diagnostic, not SQL authority.

use serde::Serialize;

pub use super::sequence::NativeSequenceTransition;
pub use super::{DataMovementAction, DataMovementReceipt, NativePrefixDecision};
use super::{evaluate_prefix, sequence::NativeSequenceHistory};
pub use crate::three_replica::commit_coverage::{
    NativeSample, NativeSampleSet, TransactionAcknowledgement, TransactionRecordRow,
    TransactionSample,
};
use crate::three_replica::{CommitCoverageError, NativeTopologyBinding, ReadyMember};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativePrefixEvidence {
    pub initial_binding: NativeTopologyBinding,
    pub initial_observations: NativeSampleSet,
    pub record_binding: NativeTopologyBinding,
    pub transitions: Vec<NativeSequenceTransition>,
    pub target_ordinal: u8,
    pub acknowledgement: TransactionAcknowledgement,
    pub transaction_samples: Vec<TransactionSample>,
    pub evaluation_binding: NativeTopologyBinding,
    pub observations: NativeSampleSet,
    pub evaluated_at_unix_millis: u64,
}

impl NativePrefixEvidence {
    pub fn evaluate(
        &self,
        members: &[ReadyMember; 3],
    ) -> Result<NativePrefixDecision, CommitCoverageError> {
        if self.initial_observations.completed_at_unix_millis > self.evaluated_at_unix_millis
            || self.transitions.iter().any(|transition| {
                transition.receipt.confirmed_at_unix_millis > self.evaluated_at_unix_millis
            })
        {
            return Err(CommitCoverageError::Control(
                "native phase evidence is from after the evaluation",
            ));
        }
        let admitted_at = self.transitions.last().map_or(
            self.initial_observations.completed_at_unix_millis,
            |transition| transition.receipt.confirmed_at_unix_millis,
        );
        if self.observations.requested_at_unix_millis < admitted_at {
            return Err(CommitCoverageError::Control(
                "final native observation precedes its admitted phase",
            ));
        }
        let mut history = NativeSequenceHistory::new(
            members,
            self.initial_binding.clone(),
            self.target_ordinal,
            &self.initial_observations,
            self.initial_observations.completed_at_unix_millis,
        )?;
        for transition in &self.transitions {
            history.replay_transition(members, transition)?;
        }
        if history.current() != &self.evaluation_binding {
            return Err(CommitCoverageError::Control(
                "evaluation binding is not the evidenced native phase",
            ));
        }
        if self.transitions.is_empty() {
            if self.record_binding != self.initial_binding {
                return Err(CommitCoverageError::Control(
                    "same-phase record binding differs from initial evidence",
                ));
            }
            evaluate_prefix(
                members,
                &self.evaluation_binding,
                self.target_ordinal,
                &self.acknowledgement,
                &self.transaction_samples,
                &self.observations,
                self.evaluated_at_unix_millis,
            )
        } else {
            history.evaluate(
                members,
                &self.record_binding,
                &self.acknowledgement,
                &self.transaction_samples,
                &self.observations,
                self.evaluated_at_unix_millis,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::three_replica::commit_coverage::prefix::tests::receipt;
    use crate::three_replica::commit_coverage::tests::bound_prefix_fixture;

    #[test]
    fn hosted_facade_reuses_native_evaluation_and_rejects_phase_relabeling() {
        let (samples, members, binding) = bound_prefix_fixture();
        let (acknowledgement, transaction_samples) = receipt(Some("99"));
        let mut initial = samples.clone();
        initial.requested_at_unix_millis = 850;
        initial.completed_at_unix_millis = 879;
        for sample in &mut initial.members {
            sample.collection_completed_at_unix_millis = 870;
            sample.snapshot.observed_at_unix_millis = 860;
            let kuberic_mssql::Observation::Present {
                observed_at_unix_millis,
                ..
            } = &mut sample.snapshot.availability_group
            else {
                unreachable!()
            };
            *observed_at_unix_millis = 860;
        }
        let evidence = NativePrefixEvidence {
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
        evidence.evaluate(&members).unwrap();
        let mut changed = evidence.clone();
        changed.record_binding.configuration_sequence += 1;
        assert!(changed.evaluate(&members).is_err());
        changed = evidence;
        changed.evaluation_binding.configuration_sequence += 1;
        assert!(changed.evaluate(&members).is_err());
    }
}
