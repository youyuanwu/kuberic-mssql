use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

pub const PROFILE_VERSION: u32 = 1;
pub const EXIT_READY_TO_PIN: i32 = 0;
pub const EXIT_INVALID: i32 = 2;
pub const EXIT_BLOCKED: i32 = 3;
pub const EXIT_INCOMPLETE: i32 = 4;
pub const EXIT_BASELINE_CONFIRMED: i32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseIntakeMode {
    Baseline,
    Candidate,
}

impl fmt::Display for ReleaseIntakeMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Baseline => formatter.write_str("baseline"),
            Self::Candidate => formatter.write_str("candidate"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateId {
    DependencyIdentity,
    SemanticContractReview,
    PersistenceContractReview,
    FreshStateActivationReview,
    NativeHistoryContract,
    PinnedAdapterContract,
    SfReadinessContract,
    ReleaseIntakeContract,
    Formatting,
    StrictClippy,
    FullLockedTests,
    LiveOneReplica,
    LiveThreeReplica,
    LiveThreeReplicaRestart,
    ExternalProgressContract,
    RoleBeforePrimaryConfiguration,
    SameRoleSecondaryEpoch,
    RemoteActiveMembership,
    InvalidBuildProgress,
    CancelBeforeRemove,
    SignedAuthorizationContract,
}

impl fmt::Display for GateId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DependencyIdentity => "dependency_identity",
            Self::SemanticContractReview => "semantic_contract_review",
            Self::PersistenceContractReview => "persistence_contract_review",
            Self::FreshStateActivationReview => "fresh_state_activation_review",
            Self::NativeHistoryContract => "native_history_contract",
            Self::PinnedAdapterContract => "pinned_adapter_contract",
            Self::SfReadinessContract => "sf_readiness_contract",
            Self::ReleaseIntakeContract => "release_intake_contract",
            Self::Formatting => "formatting",
            Self::StrictClippy => "strict_clippy",
            Self::FullLockedTests => "full_locked_tests",
            Self::LiveOneReplica => "live_one_replica",
            Self::LiveThreeReplica => "live_three_replica",
            Self::LiveThreeReplicaRestart => "live_three_replica_restart",
            Self::ExternalProgressContract => "external_progress_contract",
            Self::RoleBeforePrimaryConfiguration => "role_before_primary_configuration",
            Self::SameRoleSecondaryEpoch => "same_role_secondary_epoch",
            Self::RemoteActiveMembership => "remote_active_membership",
            Self::InvalidBuildProgress => "invalid_build_progress",
            Self::CancelBeforeRemove => "cancel_before_remove",
            Self::SignedAuthorizationContract => "signed_authorization_contract",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Passed,
    Failed,
    Pending,
}

impl fmt::Display for GateStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Pending => "pending",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateRequirement {
    Required,
    Optional,
}

impl fmt::Display for GateRequirement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Required => "required",
            Self::Optional => "optional",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateDefinition {
    pub id: GateId,
    pub baseline: GateRequirement,
    pub candidate: GateRequirement,
}

impl GateDefinition {
    pub const fn requirement(self, mode: ReleaseIntakeMode) -> GateRequirement {
        match mode {
            ReleaseIntakeMode::Baseline => self.baseline,
            ReleaseIntakeMode::Candidate => self.candidate,
        }
    }
}

pub const PROFILE_V1: [GateDefinition; 21] = [
    required(GateId::DependencyIdentity),
    required(GateId::SemanticContractReview),
    required(GateId::PersistenceContractReview),
    required(GateId::FreshStateActivationReview),
    required(GateId::NativeHistoryContract),
    required(GateId::PinnedAdapterContract),
    required(GateId::SfReadinessContract),
    required(GateId::ReleaseIntakeContract),
    required(GateId::Formatting),
    required(GateId::StrictClippy),
    required(GateId::FullLockedTests),
    candidate_required(GateId::LiveOneReplica),
    candidate_required(GateId::LiveThreeReplica),
    candidate_required(GateId::LiveThreeReplicaRestart),
    candidate_required(GateId::ExternalProgressContract),
    candidate_required(GateId::RoleBeforePrimaryConfiguration),
    candidate_required(GateId::SameRoleSecondaryEpoch),
    candidate_required(GateId::RemoteActiveMembership),
    candidate_required(GateId::InvalidBuildProgress),
    candidate_required(GateId::CancelBeforeRemove),
    candidate_required(GateId::SignedAuthorizationContract),
];

const fn required(id: GateId) -> GateDefinition {
    GateDefinition {
        id,
        baseline: GateRequirement::Required,
        candidate: GateRequirement::Required,
    }
}

const fn candidate_required(id: GateId) -> GateDefinition {
    GateDefinition {
        id,
        baseline: GateRequirement::Optional,
        candidate: GateRequirement::Required,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateResult {
    pub gate_id: GateId,
    pub status: GateStatus,
    pub evidence: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntakeOutcome {
    Blocked,
    Incomplete,
    BaselineConfirmed,
    ReadyToPin,
}

impl fmt::Display for IntakeOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Blocked => "blocked",
            Self::Incomplete => "incomplete",
            Self::BaselineConfirmed => "baseline_confirmed",
            Self::ReadyToPin => "ready_to_pin",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseIntakeRecord {
    pub profile_version: u32,
    pub mode: ReleaseIntakeMode,
    pub current_dependency_identity: String,
    pub reviewed_dependency_identity: String,
    pub semantic_contract_finding: String,
    pub persistence_contract_finding: String,
    pub fresh_state_finding: String,
    pub gates: Vec<GateResult>,
    pub blockers: Vec<String>,
    pub outcome: IntakeOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntakeError {
    Malformed(String),
    UnknownProfileVersion(u32),
    MissingValue(&'static str),
    EmptyGateEvidence(GateId),
    DuplicateGate(GateId),
    MissingGate(GateId),
    ClaimedOutcome {
        claimed: IntakeOutcome,
        computed: IntakeOutcome,
    },
}

impl fmt::Display for IntakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(message) => write!(formatter, "malformed intake record: {message}"),
            Self::UnknownProfileVersion(version) => {
                write!(formatter, "unknown intake profile version {version}")
            }
            Self::MissingValue(field) => write!(formatter, "missing required value {field}"),
            Self::EmptyGateEvidence(gate) => {
                write!(formatter, "gate {gate} has empty evidence")
            }
            Self::DuplicateGate(gate) => write!(formatter, "duplicate gate {gate}"),
            Self::MissingGate(gate) => write!(formatter, "missing gate {gate}"),
            Self::ClaimedOutcome { claimed, computed } => write!(
                formatter,
                "claimed outcome {claimed} does not match computed outcome {computed}"
            ),
        }
    }
}

impl std::error::Error for IntakeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedIntake {
    outcome: IntakeOutcome,
    effective_statuses: BTreeMap<GateId, GateStatus>,
}

impl ValidatedIntake {
    pub fn outcome(&self) -> IntakeOutcome {
        self.outcome
    }

    pub fn effective_status(&self, gate: GateId) -> GateStatus {
        self.effective_statuses[&gate]
    }
}

pub fn parse_and_validate_json(
    input: &[u8],
) -> Result<(ReleaseIntakeRecord, ValidatedIntake), IntakeError> {
    let record =
        serde_json::from_slice(input).map_err(|error| IntakeError::Malformed(error.to_string()))?;
    let validated = validate_record(&record)?;
    Ok((record, validated))
}

pub fn validate_record(record: &ReleaseIntakeRecord) -> Result<ValidatedIntake, IntakeError> {
    if record.profile_version != PROFILE_VERSION {
        return Err(IntakeError::UnknownProfileVersion(record.profile_version));
    }
    require_value(
        &record.current_dependency_identity,
        "current_dependency_identity",
    )?;
    require_value(
        &record.reviewed_dependency_identity,
        "reviewed_dependency_identity",
    )?;
    require_value(
        &record.semantic_contract_finding,
        "semantic_contract_finding",
    )?;
    require_value(
        &record.persistence_contract_finding,
        "persistence_contract_finding",
    )?;
    require_value(&record.fresh_state_finding, "fresh_state_finding")?;
    for blocker in &record.blockers {
        require_value(blocker, "blockers[]")?;
    }

    let mut results = BTreeMap::new();
    for result in &record.gates {
        if result.evidence.trim().is_empty() {
            return Err(IntakeError::EmptyGateEvidence(result.gate_id));
        }
        if results.insert(result.gate_id, result.status).is_some() {
            return Err(IntakeError::DuplicateGate(result.gate_id));
        }
    }
    for definition in PROFILE_V1 {
        if !results.contains_key(&definition.id) {
            return Err(IntakeError::MissingGate(definition.id));
        }
    }

    match record.mode {
        ReleaseIntakeMode::Baseline
            if record.current_dependency_identity != record.reviewed_dependency_identity =>
        {
            results.insert(GateId::DependencyIdentity, GateStatus::Failed);
        }
        ReleaseIntakeMode::Candidate
            if record.current_dependency_identity == record.reviewed_dependency_identity =>
        {
            results.insert(GateId::DependencyIdentity, GateStatus::Failed);
        }
        _ => {}
    }

    let has_failed_required = PROFILE_V1
        .iter()
        .filter(|definition| definition.requirement(record.mode) == GateRequirement::Required)
        .any(|definition| results[&definition.id] == GateStatus::Failed);
    let has_pending_required = PROFILE_V1
        .iter()
        .filter(|definition| definition.requirement(record.mode) == GateRequirement::Required)
        .any(|definition| results[&definition.id] == GateStatus::Pending);
    let outcome = if !record.blockers.is_empty() || has_failed_required {
        IntakeOutcome::Blocked
    } else if has_pending_required {
        IntakeOutcome::Incomplete
    } else {
        match record.mode {
            ReleaseIntakeMode::Baseline => IntakeOutcome::BaselineConfirmed,
            ReleaseIntakeMode::Candidate => IntakeOutcome::ReadyToPin,
        }
    };

    if record.outcome != outcome {
        return Err(IntakeError::ClaimedOutcome {
            claimed: record.outcome,
            computed: outcome,
        });
    }
    Ok(ValidatedIntake {
        outcome,
        effective_statuses: results,
    })
}

fn require_value(value: &str, field: &'static str) -> Result<(), IntakeError> {
    if value.trim().is_empty() {
        Err(IntakeError::MissingValue(field))
    } else {
        Ok(())
    }
}

pub fn render_summary(record: &ReleaseIntakeRecord, validated: &ValidatedIntake) -> String {
    let mut summary = String::new();
    summary.push_str("Kuberic MSSQL release intake\n");
    summary.push_str(&format!("profile_version: {}\n", record.profile_version));
    summary.push_str(&format!("mode: {}\n", record.mode));
    summary.push_str(&format!(
        "current_dependency_identity: {}\n",
        record.current_dependency_identity
    ));
    summary.push_str(&format!(
        "reviewed_dependency_identity: {}\n",
        record.reviewed_dependency_identity
    ));
    summary.push_str(&format!(
        "semantic_contract_finding: {}\n",
        record.semantic_contract_finding
    ));
    summary.push_str(&format!(
        "persistence_contract_finding: {}\n",
        record.persistence_contract_finding
    ));
    summary.push_str(&format!(
        "fresh_state_finding: {}\n",
        record.fresh_state_finding
    ));
    summary.push_str("gates:\n");
    for definition in PROFILE_V1 {
        let recorded = record
            .gates
            .iter()
            .find(|result| result.gate_id == definition.id)
            .expect("validated intake has every canonical gate");
        let effective = validated.effective_status(definition.id);
        summary.push_str(&format!(
            "  - {}: {} ({})",
            definition.id,
            effective,
            definition.requirement(record.mode)
        ));
        if effective != recorded.status {
            summary.push_str(&format!(", recorded={}", recorded.status));
        }
        summary.push_str(&format!("; evidence={}\n", recorded.evidence));
    }
    summary.push_str("blockers:\n");
    let mut blockers = record.blockers.iter().collect::<Vec<_>>();
    blockers.sort();
    if blockers.is_empty() {
        summary.push_str("  - none\n");
    } else {
        for blocker in blockers {
            summary.push_str(&format!("  - {blocker}\n"));
        }
    }
    summary.push_str(&format!("outcome: {}\n", validated.outcome()));
    summary
}

pub const fn outcome_exit_code(outcome: IntakeOutcome) -> i32 {
    match outcome {
        IntakeOutcome::ReadyToPin => EXIT_READY_TO_PIN,
        IntakeOutcome::Blocked => EXIT_BLOCKED,
        IntakeOutcome::Incomplete => EXIT_INCOMPLETE,
        IntakeOutcome::BaselineConfirmed => EXIT_BASELINE_CONFIRMED,
    }
}
