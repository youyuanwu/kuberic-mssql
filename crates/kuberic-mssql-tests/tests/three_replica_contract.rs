use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Mutex;

use kuberic_mssql_tests::three_replica::{
    ACKNOWLEDGEMENT_SCHEMA_VERSION, CombinedFixtureError, FailureCategory, FailureStage,
    FixtureConfig, FixtureConfigError, JOURNAL_SCHEMA_VERSION, JournalError, KubericMember,
    LaunchAuthorization, OwnershipJournal, PINNED_SQL_SERVER_IMAGE, ResourceBinding, ResourceKind,
    ResourcePolicy, ResourceRecord, ResourceState, RunState, SanitizedFailure, SqlMember,
    StageDeadlines, TopologyRun,
};

static ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

fn write_acknowledgement(path: &Path, accepted: bool) {
    fs::write(
        path,
        format!(
            "{{\"schema_version\":{ACKNOWLEDGEMENT_SCHEMA_VERSION},\
             \"sql_server_eula\":{{\"accepted\":{accepted}}}}}"
        ),
    )
    .unwrap();
}

fn sample_run(root: &Path) -> TopologyRun {
    TopologyRun {
        run_id: "run-123".into(),
        resource_uid: "resource-123".into(),
        members: std::array::from_fn(|index| SqlMember {
            ordinal: (index + 1) as u8,
            server_name: format!("sql-{}", index + 1),
            container_name: format!("container-{}", index + 1),
            data_directory: root.join(format!("member-{}", index + 1)),
        }),
        kuberic_members: std::array::from_fn(|index| KubericMember {
            ordinal: (index + 1) as u8,
            replica_id: (index + 1) as i64,
            instance_id: format!("instance-{}", index + 1),
            pod_uid: format!("pod-{}", index + 1),
            pvc_uid: format!("pvc-{}", index + 1),
        }),
    }
}

#[test]
fn affirmative_file_authorizes_exactly_one_acceptance_setting() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("acknowledgement.json");
    write_acknowledgement(&path, true);

    let authorization = LaunchAuthorization::load(&path).unwrap();
    assert_eq!(
        authorization.sql_server_environment(),
        [("ACCEPT_EULA", "Y")]
    );
    assert_eq!(authorization.source().path(), path.canonicalize().unwrap());
    assert_ne!(authorization.source().sha256(), &[0; 32]);
    authorization.revalidate().unwrap();
}

#[test]
fn denied_missing_malformed_or_unsupported_input_never_authorizes_launch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("acknowledgement.json");

    assert_eq!(
        LaunchAuthorization::load(&path).unwrap_err(),
        FixtureConfigError::AcknowledgementUnavailable
    );

    write_acknowledgement(&path, false);
    assert_eq!(
        LaunchAuthorization::load(&path).unwrap_err(),
        FixtureConfigError::AcknowledgementDenied
    );

    fs::write(&path, b"not json").unwrap();
    assert_eq!(
        LaunchAuthorization::load(&path).unwrap_err(),
        FixtureConfigError::AcknowledgementMalformed
    );

    fs::write(
        &path,
        br#"{"schema_version":2,"sql_server_eula":{"accepted":true}}"#,
    )
    .unwrap();
    assert_eq!(
        LaunchAuthorization::load(&path).unwrap_err(),
        FixtureConfigError::AcknowledgementSchema(2)
    );
}

#[test]
fn strict_document_rejects_missing_duplicate_and_unknown_fields() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("acknowledgement.json");

    for document in [
        r#"{"schema_version":1}"#,
        r#"{"schema_version":1,"sql_server_eula":{}}"#,
        r#"{"schema_version":1,"sql_server_eula":{"accepted":true,"extra":1}}"#,
        r#"{"schema_version":1,"schema_version":1,"sql_server_eula":{"accepted":true}}"#,
        r#"{"schema_version":1,"sql_server_eula":{"accepted":true},"extra":1}"#,
    ] {
        fs::write(&path, document).unwrap();
        assert_eq!(
            LaunchAuthorization::load(&path).unwrap_err(),
            FixtureConfigError::AcknowledgementMalformed,
            "{document}"
        );
    }
}

#[test]
fn symlinks_and_changed_files_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target.json");
    let link = directory.path().join("link.json");
    write_acknowledgement(&target, true);
    symlink(&target, &link).unwrap();
    assert_eq!(
        LaunchAuthorization::load(&link).unwrap_err(),
        FixtureConfigError::AcknowledgementNotRegular
    );

    let authorization = LaunchAuthorization::load(&target).unwrap();
    write_acknowledgement(&target, false);
    assert_eq!(
        authorization.revalidate().unwrap_err(),
        FixtureConfigError::AcknowledgementChanged
    );

    fs::remove_file(&target).unwrap();
    write_acknowledgement(&target, true);
    assert_eq!(
        authorization.revalidate().unwrap_err(),
        FixtureConfigError::AcknowledgementChanged
    );
}

#[test]
fn ambient_legacy_acceptance_cannot_replace_the_required_file() {
    let _guard = ENVIRONMENT_LOCK.lock().unwrap();
    let previous = std::env::var_os("SQLSERVER_TEST_EULA_ACCEPTED");
    unsafe {
        std::env::set_var("SQLSERVER_TEST_EULA_ACCEPTED", "true");
    }
    assert_eq!(
        FixtureConfig::new(
            Path::new("/tmp/kuberic-mssql-three-replica"),
            Path::new("/definitely/missing/acknowledgement.json"),
        )
        .unwrap_err(),
        FixtureConfigError::AcknowledgementUnavailable
    );
    unsafe {
        match previous {
            Some(value) => std::env::set_var("SQLSERVER_TEST_EULA_ACCEPTED", value),
            None => std::env::remove_var("SQLSERVER_TEST_EULA_ACCEPTED"),
        }
    }
}

#[test]
fn fifo_input_is_rejected_without_blocking() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("acknowledgement.fifo");
    let path_bytes = CString::new(path.as_os_str().as_bytes()).unwrap();
    let result = unsafe { libc::mkfifo(path_bytes.as_ptr(), 0o600) };
    assert_eq!(result, 0);
    assert_eq!(
        LaunchAuthorization::load(&path).unwrap_err(),
        FixtureConfigError::AcknowledgementNotRegular
    );
}

#[test]
fn fixture_policy_is_numeric_and_not_environment_derived() {
    let resources = ResourcePolicy::default();
    assert_eq!(resources.container_memory_bytes, 3 * 1024 * 1024 * 1024);
    assert_eq!(resources.sql_server_memory_mb, 2048);
    assert_eq!(resources.container_cpu_nanos, 2_000_000_000);
    assert_eq!(
        resources.minimum_available_memory_bytes,
        10 * 1024 * 1024 * 1024
    );
    assert_eq!(resources.minimum_fixture_bytes, 15 * 1024 * 1024 * 1024);
    assert_eq!(resources.minimum_effective_cpus, 2);

    let deadlines = StageDeadlines::default();
    assert_eq!(deadlines.complete_run.as_secs(), 1200);
    assert_eq!(deadlines.cleanup.as_secs(), 180);
}

#[test]
fn fixture_root_must_be_absolute() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("acknowledgement.json");
    write_acknowledgement(&path, true);
    assert_eq!(
        FixtureConfig::new("relative/root", &path).unwrap_err(),
        FixtureConfigError::InvalidFixtureRoot
    );
    let config = FixtureConfig::new(directory.path().join("missing/fixture"), &path).unwrap();
    assert!(config.root().is_absolute());
    assert!(config.root().ends_with("missing/fixture"));
    assert_eq!(config.image(), PINNED_SQL_SERVER_IMAGE);
    assert_eq!(config.resources(), ResourcePolicy::default());
    assert_eq!(config.deadlines(), StageDeadlines::default());
    assert_eq!(
        config.authorization().sql_server_environment(),
        [("ACCEPT_EULA", "Y")]
    );

    let target = directory.path().join("missing-target");
    let dangling = directory.path().join("dangling");
    symlink(&target, &dangling).unwrap();
    assert_eq!(
        FixtureConfig::new(dangling.join("fixture"), &path).unwrap_err(),
        FixtureConfigError::InvalidFixtureRoot
    );
}

#[test]
fn dedicated_just_recipes_have_exact_isolated_invocation_contracts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let justfile = fs::read_to_string(root.join("justfile")).unwrap();
    assert!(
        justfile.contains(
            "test-live-three-replica acknowledgement root=\"target/mssql-three-replica\":"
        )
    );
    assert!(justfile.contains("cleanup-live-three-replica root=\"target/mssql-three-replica\":"));
    assert!(justfile.contains("env -u SQLSERVER_TEST_EULA_ACCEPTED"));
    assert!(
        justfile.contains(
            "KUBERIC_MSSQL_EULA_ACKNOWLEDGEMENT=\"$(realpath {{quote(acknowledgement)}})\""
        )
    );
    assert!(
        justfile.contains("KUBERIC_MSSQL_THREE_REPLICA_ROOT=\"$(realpath -m {{quote(root)}})\"")
    );
    assert!(justfile.contains(
        "cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_mssql_happy_path -- --ignored --exact --test-threads=1"
    ));
    assert!(justfile.contains(
        "cargo run --locked -p kuberic-mssql-tests --bin mssql-three-replica-fixture -- cleanup --root"
    ));

    for recipe in [
        "default: check",
        "check: fmt-check clippy test test-ci-helpers",
    ] {
        let line = justfile.lines().find(|line| *line == recipe).unwrap();
        assert!(!line.contains("test-live-three-replica"));
    }
    let ci = justfile
        .lines()
        .find(|line| line.starts_with("ci fixture="))
        .unwrap();
    assert!(!ci.contains("test-live-three-replica"));
}

#[test]
fn journal_round_trips_without_secret_values() {
    let directory = tempfile::tempdir().unwrap();
    let mut journal = OwnershipJournal::new(sample_run(directory.path()));
    journal.resources.push(ResourceRecord {
        kind: ResourceKind::SecretFile,
        logical_name: "sa-password".into(),
        path: Some(directory.path().join("sa-password")),
        intent: None,
        binding: Some(ResourceBinding {
            immutable_id: "device:1/inode:2".into(),
            attributes_sha256: "a".repeat(64),
        }),
        state: ResourceState::Bound,
    });
    journal.state = RunState::Ready;

    let encoded = journal.to_json().unwrap();
    let text = String::from_utf8(encoded.clone()).unwrap();
    assert!(text.contains("sa-password"));
    assert!(!text.contains("\"secret_value\""));
    assert_eq!(OwnershipJournal::from_json(&encoded).unwrap(), journal);
}

#[test]
fn journal_rejects_unknown_or_unsupported_schemas() {
    let directory = tempfile::tempdir().unwrap();
    let journal = OwnershipJournal::new(sample_run(directory.path()));
    let mut value: serde_json::Value = serde_json::from_slice(&journal.to_json().unwrap()).unwrap();
    value["schema_version"] = serde_json::json!(JOURNAL_SCHEMA_VERSION + 1);
    assert_eq!(
        OwnershipJournal::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err(),
        JournalError::UnsupportedSchema(JOURNAL_SCHEMA_VERSION + 1)
    );

    value["schema_version"] = serde_json::json!(JOURNAL_SCHEMA_VERSION);
    value["unknown"] = serde_json::json!(true);
    assert_eq!(
        OwnershipJournal::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err(),
        JournalError::Malformed
    );
}

#[test]
fn nested_binding_unknown_fields_are_rejected_without_echoing_values() {
    let directory = tempfile::tempdir().unwrap();
    let mut journal = OwnershipJournal::new(sample_run(directory.path()));
    journal.resources.push(ResourceRecord {
        kind: ResourceKind::Container,
        logical_name: "sql-1".into(),
        path: None,
        intent: None,
        binding: Some(ResourceBinding {
            immutable_id: "container-id".into(),
            attributes_sha256: "b".repeat(64),
        }),
        state: ResourceState::Cleaning,
    });
    let mut value: serde_json::Value = serde_json::from_slice(&journal.to_json().unwrap()).unwrap();
    value["resources"][0]["binding"]["secret"] = serde_json::json!("actual-secret-value");
    let error = OwnershipJournal::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
    assert_eq!(error, JournalError::Malformed);
    assert!(!error.to_string().contains("actual-secret-value"));
}

#[test]
fn binding_survives_cleaning_and_combined_errors_are_sanitized() {
    let binding = ResourceBinding {
        immutable_id: "container-id".into(),
        attributes_sha256: "c".repeat(64),
    };
    let record = ResourceRecord {
        kind: ResourceKind::Container,
        logical_name: "sql-1".into(),
        path: None,
        intent: None,
        binding: Some(binding.clone()),
        state: ResourceState::Cleaning,
    };
    assert_eq!(record.binding, Some(binding));

    let error = CombinedFixtureError::new(
        SanitizedFailure::new(FailureStage::Setup, FailureCategory::ContainerCreation),
        vec![SanitizedFailure::new(
            FailureStage::Cleanup,
            FailureCategory::ContainerRemoval,
        )],
    );
    assert_eq!(
        error.to_string(),
        "setup: container creation failed; cleanup: container removal failed"
    );
}
