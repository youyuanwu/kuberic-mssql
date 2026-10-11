use std::path::PathBuf;
use std::process::Command;

use kuberic_mssql_tests::release_intake::{
    EXIT_BASELINE_CONFIRMED, EXIT_BLOCKED, EXIT_INCOMPLETE, EXIT_INVALID, EXIT_READY_TO_PIN,
    GateId, GateRequirement, GateResult, GateStatus, IntakeError, IntakeOutcome, PROFILE_V1,
    PROFILE_VERSION, ReleaseIntakeMode, ReleaseIntakeRecord, parse_and_validate_json,
    validate_record,
};

const CURRENT_DEPENDENCY: &str = "kuberic-runtime@09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f";
const CANDIDATE_DEPENDENCY: &str = "kuberic-runtime@1111111111111111111111111111111111111111";

fn record(mode: ReleaseIntakeMode, status: GateStatus) -> ReleaseIntakeRecord {
    let reviewed_dependency_identity = match mode {
        ReleaseIntakeMode::Baseline => CURRENT_DEPENDENCY,
        ReleaseIntakeMode::Candidate => CANDIDATE_DEPENDENCY,
    };
    ReleaseIntakeRecord {
        profile_version: PROFILE_VERSION,
        mode,
        current_dependency_identity: CURRENT_DEPENDENCY.into(),
        reviewed_dependency_identity: reviewed_dependency_identity.into(),
        semantic_contract_finding: "semantic contract reviewed".into(),
        persistence_contract_finding: "persistence contract reviewed".into(),
        fresh_state_finding: "fresh-state activation reviewed".into(),
        gates: PROFILE_V1
            .iter()
            .map(|definition| GateResult {
                gate_id: definition.id,
                status,
                evidence: format!("{} evidence", definition.id),
            })
            .collect(),
        blockers: Vec::new(),
        outcome: match (mode, status) {
            (_, GateStatus::Failed) => IntakeOutcome::Blocked,
            (_, GateStatus::Pending) => IntakeOutcome::Incomplete,
            (ReleaseIntakeMode::Baseline, GateStatus::Passed) => IntakeOutcome::BaselineConfirmed,
            (ReleaseIntakeMode::Candidate, GateStatus::Passed) => IntakeOutcome::ReadyToPin,
        },
    }
}

fn set_status(record: &mut ReleaseIntakeRecord, gate: GateId, status: GateStatus) {
    record
        .gates
        .iter_mut()
        .find(|result| result.gate_id == gate)
        .unwrap()
        .status = status;
}

#[test]
fn profile_v1_has_the_exact_canonical_matrix() {
    let expected = [
        (GateId::DependencyIdentity, GateRequirement::Required),
        (GateId::SemanticContractReview, GateRequirement::Required),
        (GateId::PersistenceContractReview, GateRequirement::Required),
        (
            GateId::FreshStateActivationReview,
            GateRequirement::Required,
        ),
        (GateId::NativeHistoryContract, GateRequirement::Required),
        (GateId::PinnedAdapterContract, GateRequirement::Required),
        (GateId::SfReadinessContract, GateRequirement::Required),
        (GateId::ReleaseIntakeContract, GateRequirement::Required),
        (GateId::Formatting, GateRequirement::Required),
        (GateId::StrictClippy, GateRequirement::Required),
        (GateId::FullLockedTests, GateRequirement::Required),
        (GateId::LiveOneReplica, GateRequirement::Optional),
        (GateId::LiveThreeReplica, GateRequirement::Optional),
        (GateId::LiveThreeReplicaRestart, GateRequirement::Optional),
        (GateId::ExternalProgressContract, GateRequirement::Optional),
        (
            GateId::RoleBeforePrimaryConfiguration,
            GateRequirement::Optional,
        ),
        (GateId::SameRoleSecondaryEpoch, GateRequirement::Optional),
        (GateId::RemoteActiveMembership, GateRequirement::Optional),
        (GateId::InvalidBuildProgress, GateRequirement::Optional),
        (GateId::CancelBeforeRemove, GateRequirement::Optional),
        (
            GateId::SignedAuthorizationContract,
            GateRequirement::Optional,
        ),
    ];

    assert_eq!(PROFILE_V1.len(), expected.len());
    for (definition, (id, baseline)) in PROFILE_V1.iter().zip(expected) {
        assert_eq!(definition.id, id);
        assert_eq!(definition.baseline, baseline);
        assert_eq!(definition.candidate, GateRequirement::Required);
    }
}

#[test]
fn candidate_all_required_pass_is_ready_to_pin() {
    let record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    assert_eq!(
        validate_record(&record).unwrap().outcome(),
        IntakeOutcome::ReadyToPin
    );
}

#[test]
fn baseline_all_pass_is_confirmed_and_never_ready_to_pin() {
    let record = record(ReleaseIntakeMode::Baseline, GateStatus::Passed);
    assert_eq!(
        validate_record(&record).unwrap().outcome(),
        IntakeOutcome::BaselineConfirmed
    );
    assert_ne!(record.outcome, IntakeOutcome::ReadyToPin);
}

#[test]
fn failed_required_gate_blocks() {
    let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    set_status(
        &mut record,
        GateId::SemanticContractReview,
        GateStatus::Failed,
    );
    record.outcome = IntakeOutcome::Blocked;
    assert_eq!(
        validate_record(&record).unwrap().outcome(),
        IntakeOutcome::Blocked
    );
}

#[test]
fn explicit_blocker_and_failed_required_gate_outrank_pending_required_evidence() {
    let mut blocked = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    set_status(
        &mut blocked,
        GateId::SemanticContractReview,
        GateStatus::Pending,
    );
    blocked
        .blockers
        .push("unresolved persistence ambiguity".into());
    blocked.outcome = IntakeOutcome::Blocked;
    assert_eq!(
        validate_record(&blocked).unwrap().outcome(),
        IntakeOutcome::Blocked
    );

    blocked.blockers.clear();
    set_status(
        &mut blocked,
        GateId::PersistenceContractReview,
        GateStatus::Failed,
    );
    assert_eq!(
        validate_record(&blocked).unwrap().outcome(),
        IntakeOutcome::Blocked
    );
}

#[test]
fn pending_required_gate_is_incomplete() {
    let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    set_status(
        &mut record,
        GateId::FreshStateActivationReview,
        GateStatus::Pending,
    );
    record.outcome = IntakeOutcome::Incomplete;
    assert_eq!(
        validate_record(&record).unwrap().outcome(),
        IntakeOutcome::Incomplete
    );
}

#[test]
fn optional_pending_and_failed_statuses_are_outcome_neutral() {
    for status in [GateStatus::Pending, GateStatus::Failed] {
        let mut record = record(ReleaseIntakeMode::Baseline, GateStatus::Passed);
        set_status(&mut record, GateId::LiveOneReplica, status);
        assert_eq!(
            validate_record(&record).unwrap().outcome(),
            IntakeOutcome::BaselineConfirmed
        );
    }
}

#[test]
fn optional_failure_neither_blocks_nor_completes_required_pending_evidence() {
    let mut record = record(ReleaseIntakeMode::Baseline, GateStatus::Passed);
    set_status(&mut record, GateId::LiveThreeReplica, GateStatus::Failed);
    set_status(
        &mut record,
        GateId::PersistenceContractReview,
        GateStatus::Pending,
    );
    record.outcome = IntakeOutcome::Incomplete;
    assert_eq!(
        validate_record(&record).unwrap().outcome(),
        IntakeOutcome::Incomplete
    );
}

#[test]
fn omitted_required_and_optional_gates_are_invalid() {
    for gate in [GateId::Formatting, GateId::SignedAuthorizationContract] {
        let mut record = record(ReleaseIntakeMode::Baseline, GateStatus::Passed);
        record.gates.retain(|result| result.gate_id != gate);
        assert_eq!(
            validate_record(&record),
            Err(IntakeError::MissingGate(gate))
        );
    }
}

#[test]
fn duplicate_gate_is_invalid() {
    let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    record.gates.push(record.gates[0].clone());
    assert_eq!(
        validate_record(&record),
        Err(IntakeError::DuplicateGate(GateId::DependencyIdentity))
    );
}

#[test]
fn unknown_or_extra_gate_is_invalid_json() {
    let record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    let mut value = serde_json::to_value(record).unwrap();
    value["gates"][0]["gate_id"] = serde_json::json!("future_unknown_gate");
    assert!(matches!(
        parse_and_validate_json(&serde_json::to_vec(&value).unwrap()),
        Err(IntakeError::Malformed(_))
    ));
}

#[test]
fn record_cannot_promote_demote_or_reclassify_profile_gates() {
    let record = record(ReleaseIntakeMode::Baseline, GateStatus::Passed);
    for (gate_index, field, value) in [
        (11, "required", serde_json::json!(true)),
        (0, "required", serde_json::json!(false)),
        (0, "applicable", serde_json::json!(false)),
    ] {
        let mut document = serde_json::to_value(&record).unwrap();
        document["gates"][gate_index][field] = value;
        assert!(matches!(
            parse_and_validate_json(&serde_json::to_vec(&document).unwrap()),
            Err(IntakeError::Malformed(_))
        ));
    }
}

#[test]
fn missing_identities_are_invalid() {
    for current_missing in [true, false] {
        let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
        if current_missing {
            record.current_dependency_identity.clear();
        } else {
            record.reviewed_dependency_identity = " ".into();
        }
        assert!(matches!(
            validate_record(&record),
            Err(IntakeError::MissingValue(_))
        ));
    }
}

#[test]
fn dependency_identities_require_canonical_package_and_lowercase_git_revision() {
    for identity in [
        "kuberic-runtime@09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f ",
        " kuberic-runtime@09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f",
        "kuberic-runtime@09CE0A7BA9FDD9D6DCD207606FD0C9496A58C47F",
        "kuberic_runtime@09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f",
        "kuberic-runtime@09ce0a7b",
        "kuberic-runtime@09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47g",
    ] {
        let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
        record.reviewed_dependency_identity = identity.into();
        assert_eq!(
            validate_record(&record),
            Err(IntakeError::InvalidDependencyIdentity(
                "reviewed_dependency_identity"
            ))
        );
    }
}

#[test]
fn baseline_reviewed_identity_mismatch_fails_dependency_identity_and_blocks() {
    let mut record = record(ReleaseIntakeMode::Baseline, GateStatus::Passed);
    record.reviewed_dependency_identity = CANDIDATE_DEPENDENCY.into();
    record.outcome = IntakeOutcome::Blocked;
    let validated = validate_record(&record).unwrap();
    assert_eq!(validated.outcome(), IntakeOutcome::Blocked);
    assert_eq!(
        validated.effective_status(GateId::DependencyIdentity),
        GateStatus::Failed
    );
}

#[test]
fn candidate_current_identity_match_fails_dependency_identity_and_blocks() {
    let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    record.reviewed_dependency_identity = record.current_dependency_identity.clone();
    record.outcome = IntakeOutcome::Blocked;
    let validated = validate_record(&record).unwrap();
    assert_eq!(validated.outcome(), IntakeOutcome::Blocked);
    assert_eq!(
        validated.effective_status(GateId::DependencyIdentity),
        GateStatus::Failed
    );
}

#[test]
fn checked_in_current_pin_baseline_record_is_complete_and_valid() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/kuberic-release-intake-baseline.json");
    let input = std::fs::read(path).unwrap();
    let (record, validated) = parse_and_validate_json(&input).unwrap();
    assert_eq!(record.mode, ReleaseIntakeMode::Baseline);
    assert_eq!(record.current_dependency_identity, CURRENT_DEPENDENCY);
    assert_eq!(record.reviewed_dependency_identity, CURRENT_DEPENDENCY);
    assert_eq!(record.gates.len(), PROFILE_V1.len());
    assert_eq!(validated.outcome(), IntakeOutcome::BaselineConfirmed);
}

#[test]
fn missing_semantic_persistence_or_fresh_state_findings_are_invalid() {
    for field in [
        "semantic_contract_finding",
        "persistence_contract_finding",
        "fresh_state_finding",
    ] {
        let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
        match field {
            "semantic_contract_finding" => record.semantic_contract_finding.clear(),
            "persistence_contract_finding" => record.persistence_contract_finding.clear(),
            "fresh_state_finding" => record.fresh_state_finding.clear(),
            _ => unreachable!(),
        }
        assert_eq!(
            validate_record(&record),
            Err(IntakeError::MissingValue(field))
        );
    }
}

#[test]
fn unknown_profile_version_is_invalid() {
    let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    record.profile_version = PROFILE_VERSION + 1;
    assert_eq!(
        validate_record(&record),
        Err(IntakeError::UnknownProfileVersion(PROFILE_VERSION + 1))
    );
}

#[test]
fn claimed_outcome_must_match_computed_outcome() {
    let mut record = record(ReleaseIntakeMode::Candidate, GateStatus::Passed);
    record.outcome = IntakeOutcome::BaselineConfirmed;
    assert_eq!(
        validate_record(&record),
        Err(IntakeError::ClaimedOutcome {
            claimed: IntakeOutcome::BaselineConfirmed,
            computed: IntakeOutcome::ReadyToPin,
        })
    );
}

#[test]
fn malformed_input_is_invalid() {
    assert!(matches!(
        parse_and_validate_json(br#"{"profile_version":1"#),
        Err(IntakeError::Malformed(_))
    ));
}

#[test]
fn binary_parses_records_prints_canonical_summary_and_uses_distinct_exit_statuses() {
    let cases = [
        (
            "ready",
            record(ReleaseIntakeMode::Candidate, GateStatus::Passed),
            EXIT_READY_TO_PIN,
            "outcome: ready_to_pin",
        ),
        (
            "baseline",
            record(ReleaseIntakeMode::Baseline, GateStatus::Passed),
            EXIT_BASELINE_CONFIRMED,
            "outcome: baseline_confirmed",
        ),
        (
            "incomplete",
            record(ReleaseIntakeMode::Candidate, GateStatus::Pending),
            EXIT_INCOMPLETE,
            "outcome: incomplete",
        ),
        (
            "blocked",
            record(ReleaseIntakeMode::Candidate, GateStatus::Failed),
            EXIT_BLOCKED,
            "outcome: blocked",
        ),
    ];

    for (name, record, expected_exit, expected_outcome) in cases {
        let path = record_path(name);
        std::fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_kuberic-release-intake"))
            .arg(&path)
            .output()
            .unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            output.status.code(),
            Some(expected_exit),
            "{name}: {output:?}"
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.starts_with("Kuberic MSSQL release intake\n"));
        assert!(stdout.contains("  - dependency_identity:"));
        assert!(stdout.contains("  - signed_authorization_contract:"));
        assert!(stdout.ends_with(&format!("{expected_outcome}\n")));
        assert!(output.stderr.is_empty());
    }

    let invalid_path = record_path("invalid");
    std::fs::write(&invalid_path, b"{not-json").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kuberic-release-intake"))
        .arg(&invalid_path)
        .output()
        .unwrap();
    let _ = std::fs::remove_file(&invalid_path);
    assert_eq!(output.status.code(), Some(EXIT_INVALID));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("release intake invalid")
    );
}

fn record_path(name: &str) -> PathBuf {
    let directory =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/release-intake-contract");
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(format!("{}-{name}.json", std::process::id()))
}
