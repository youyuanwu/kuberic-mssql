use kuberic_mssql::observation::DatabaseReplicaSnapshot;
use kuberic_mssql::{DecimalProgress, Observation};
use serde::Serialize;

use super::{
    CommitCoverageError, CoverageConclusion, MAX_OVERLAP_SAMPLES, MAX_SAMPLE_AGE,
    NativeSamplePolicy, NativeSampleSet, RecordProbeOutcome, TransactionAcknowledgement,
    TransactionSample, parse_transaction_row, record_probe_outcome, validate_native_sample_for,
};
use crate::three_replica::{NativeTopologyBinding, ReadyMember};

pub mod hosted;
pub(in crate::three_replica) mod sequence;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativePrefixReport {
    pub scope: &'static str,
    pub fixture_run_id: String,
    pub fixture_resource_uid: String,
    pub initial_native_binding: NativeTopologyBinding,
    pub record_native_binding: NativeTopologyBinding,
    pub sequence_transitions: Vec<sequence::NativeSequenceTransition>,
    pub primary_ordinal: u8,
    pub selected_target_ordinal: u8,
    pub acknowledgement: TransactionAcknowledgement,
    pub transaction_samples: Vec<TransactionSample>,
    pub record_probe: RecordProbeOutcome,
    pub before: NativeSampleSet,
    pub suspended: NativeSampleSet,
    pub controls: [DataMovementReceipt; 2],
    pub lagging: NativePrefixScenario,
    pub covered: NativePrefixScenario,
    pub quiet: NativePrefixScenario,
    pub public_coverage: CoverageConclusion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativePrefixScenario {
    pub native_binding: NativeTopologyBinding,
    pub evaluated_at_unix_millis: u64,
    pub observations: NativeSampleSet,
    pub decision: NativePrefixDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum NativePrefixDecision {
    NativeSampledCoverage {
        commit_record: DecimalProgress,
        local_hardened_boundary: DecimalProgress,
    },
    OutsideObservedPrefix {
        commit_record: DecimalProgress,
        local_hardened_boundary: DecimalProgress,
    },
    Unavailable {
        reason: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataMovementAction {
    Suspend,
    Resume,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DataMovementReceipt {
    pub action: DataMovementAction,
    pub requested_at_unix_millis: u64,
    pub accepted_at_unix_millis: Option<u64>,
    pub confirmed_at_unix_millis: u64,
}

pub(in crate::three_replica) fn target_row<'a>(
    samples: &'a NativeSampleSet,
    binding: &NativeTopologyBinding,
    target: u8,
) -> Result<&'a DatabaseReplicaSnapshot, CommitCoverageError> {
    let expected = binding
        .members
        .iter()
        .find(|member| member.ordinal == target)
        .ok_or(CommitCoverageError::InvalidNativeSample)?;
    let sample = samples
        .members
        .iter()
        .find(|member| member.ordinal == target)
        .ok_or(CommitCoverageError::InvalidNativeSample)?;
    let Observation::Present { value: group, .. } = &sample.snapshot.availability_group else {
        return Err(CommitCoverageError::InvalidNativeSample);
    };
    group
        .databases
        .first()
        .and_then(|database| {
            database
                .replicas
                .iter()
                .find(|row| row.replica_id.as_str() == expected.native_replica_id)
        })
        .ok_or(CommitCoverageError::InvalidNativeSample)
}

pub(in crate::three_replica) fn validate_sample_set(
    members: &[ReadyMember; 3],
    binding: &NativeTopologyBinding,
    target: u8,
    samples: &NativeSampleSet,
    evaluated_at: u64,
) -> Result<(), CommitCoverageError> {
    validate_sample_set_inner(
        members,
        binding,
        target,
        samples,
        evaluated_at,
        SequenceCheck::Admitted,
    )
}

#[derive(Clone, Copy)]
enum SequenceCheck {
    Admitted,
    UnadmittedSurvey,
}

fn sample_sequence(sample: &super::NativeSample) -> Result<i64, CommitCoverageError> {
    let Observation::Present { value: group, .. } = &sample.snapshot.availability_group else {
        return Err(CommitCoverageError::InvalidNativeSample);
    };
    Ok(group.configuration_sequence.value())
}

fn validate_sample_set_inner(
    members: &[ReadyMember; 3],
    binding: &NativeTopologyBinding,
    target: u8,
    samples: &NativeSampleSet,
    evaluated_at: u64,
    sequence_check: SequenceCheck,
) -> Result<(), CommitCoverageError> {
    if samples.members.len() != 3
        || samples.requested_at_unix_millis > samples.completed_at_unix_millis
        || samples.completed_at_unix_millis > evaluated_at
        || binding
            .members
            .iter()
            .enumerate()
            .any(|(index, member)| usize::from(member.ordinal) != index + 1)
        || binding
            .members
            .iter()
            .filter(|member| member.role == "PRIMARY")
            .count()
            != 1
        || binding
            .members
            .iter()
            .filter(|member| member.role == "SECONDARY")
            .count()
            != 2
        || !binding
            .members
            .iter()
            .any(|member| member.ordinal == target && member.role == "SECONDARY")
    {
        return Err(CommitCoverageError::InvalidNativeSample);
    }
    for member in members {
        let matches = samples
            .members
            .iter()
            .filter(|sample| sample.ordinal == member.ordinal)
            .collect::<Vec<_>>();
        let [sample] = matches.as_slice() else {
            return Err(CommitCoverageError::InvalidNativeSample);
        };
        if sample.collection_completed_at_unix_millis > samples.completed_at_unix_millis
            || sample.snapshot.observed_at_unix_millis > sample.collection_completed_at_unix_millis
            || evaluated_at
                .checked_sub(sample.snapshot.observed_at_unix_millis)
                .is_none_or(|age| u128::from(age) > MAX_SAMPLE_AGE.as_millis())
        {
            return Err(CommitCoverageError::InvalidNativeSample);
        }
        let candidate;
        let expected = if matches!(sequence_check, SequenceCheck::UnadmittedSurvey) {
            let observed = sample_sequence(sample)?;
            if observed < binding.configuration_sequence {
                return Err(CommitCoverageError::Control(
                    "native sequence survey regressed",
                ));
            }
            candidate = NativeTopologyBinding {
                configuration_sequence: observed,
                ..binding.clone()
            };
            &candidate
        } else {
            binding
        };
        validate_native_sample_for(
            &sample.snapshot,
            member,
            expected,
            samples.requested_at_unix_millis,
            sample.collection_completed_at_unix_millis,
            NativeSamplePolicy::ControlledSecondary(target),
        )?;
    }
    Ok(())
}

fn evaluate_prefix(
    members: &[ReadyMember; 3],
    binding: &NativeTopologyBinding,
    target: u8,
    ack: &TransactionAcknowledgement,
    transaction_samples: &[TransactionSample],
    samples: &NativeSampleSet,
    evaluated_at: u64,
) -> Result<NativePrefixDecision, CommitCoverageError> {
    validate_sample_set(members, binding, target, samples, evaluated_at)?;
    let primary = binding
        .members
        .iter()
        .find(|member| member.role == "PRIMARY")
        .ok_or(CommitCoverageError::InvalidTransactionSample)?;
    if ack.delayed_durability != "DISABLED"
        || ack.commit_requested_at_unix_millis > ack.acknowledged_at_unix_millis
        || ack.acknowledged_at_unix_millis > evaluated_at
        || samples.requested_at_unix_millis < ack.acknowledged_at_unix_millis
        || u32::try_from(ack.database_id).ok() != Some(primary.local_database_id)
        || transaction_samples.is_empty()
    {
        return Err(CommitCoverageError::InvalidTransactionSample);
    }
    parse_transaction_row(
        Some(&ack.transaction_id),
        Some(ack.database_id),
        Some(0),
        None,
        &ack.transaction_id,
        ack.database_id,
    )?;
    let mut previous = 0;
    let mut before = 0;
    let mut after = 0;
    let mut overlap = 0;
    for sample in transaction_samples {
        if sample.requested_at_unix_millis < previous
            || sample.completed_at_unix_millis < sample.requested_at_unix_millis
            || sample.completed_at_unix_millis > evaluated_at
        {
            return Err(CommitCoverageError::InvalidTransactionSample);
        }
        previous = sample.completed_at_unix_millis;
        match sample.phase {
            "before_commit"
                if sample.completed_at_unix_millis <= ack.commit_requested_at_unix_millis =>
            {
                before += 1
            }
            "overlap" if sample.requested_at_unix_millis >= ack.commit_requested_at_unix_millis => {
                overlap += 1
            }
            "after_ack" if sample.requested_at_unix_millis >= ack.acknowledged_at_unix_millis => {
                after += 1
            }
            _ => return Err(CommitCoverageError::InvalidTransactionSample),
        }
        if let Some(row) = &sample.row {
            let lsn = row.commit_lsn.map(|value| value.to_string());
            parse_transaction_row(
                Some(&row.transaction_id),
                Some(row.database_id),
                Some(row.state),
                lsn.as_deref(),
                &ack.transaction_id,
                ack.database_id,
            )?;
            if sample.phase == "before_commit"
                && matches!(row.state, 10 | 12)
                && row.commit_lsn.is_some_and(|lsn| lsn.value() != 0)
            {
                return Err(CommitCoverageError::InvalidTransactionSample);
            }
        }
    }
    if before != 1 || after != 2 || overlap > MAX_OVERLAP_SAMPLES {
        return Err(CommitCoverageError::InvalidTransactionSample);
    }
    let RecordProbeOutcome::Observed { commit_lsn } = record_probe_outcome(transaction_samples)?
    else {
        return Ok(NativePrefixDecision::Unavailable {
            reason: "transaction-specific commit record not observed in bounded window",
        });
    };
    let Some(hardened) = target_row(samples, binding, target)?
        .progress
        .hardened_block
        .filter(|lsn| lsn.value() != 0)
    else {
        return Ok(NativePrefixDecision::Unavailable {
            reason: "target-local nonzero hardened boundary unavailable",
        });
    };
    // Microsoft's exclusive local flush prefix, not the generic same-field comparator.
    Ok(if commit_lsn < hardened {
        NativePrefixDecision::NativeSampledCoverage {
            commit_record: commit_lsn,
            local_hardened_boundary: hardened,
        }
    } else {
        NativePrefixDecision::OutsideObservedPrefix {
            commit_record: commit_lsn,
            local_hardened_boundary: hardened,
        }
    })
}

pub(in crate::three_replica) fn public_coverage_blocker() -> CoverageConclusion {
    CoverageConclusion::Blocked {
        missing_evidence: vec![
            "accepted public configuration/storage/session authority and native/public history mapping not collected",
        ],
        not_exercised: [
            "public_catch_up",
            "public_history_mapping",
            "hardening_with_redo_lag",
        ],
    }
}

#[cfg(test)]
pub(in crate::three_replica) mod tests;
