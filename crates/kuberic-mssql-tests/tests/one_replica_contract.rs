use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

use kuberic_mssql_tests::one_replica::{
    LEGACY_CLEANUP_SECTION, ONE_REPLICA_JOURNAL_SCHEMA_VERSION, ONE_REPLICA_ROOT_ENV,
    OneReplicaConfig, OneReplicaJournal, OneReplicaMember, OneReplicaRun,
    cleanup_one_replica_fixture_with, format_cleanup_summary, legacy_container_name,
};
use kuberic_mssql_tests::three_replica::{BoundedProcessRunner, RunState};

mod support;
use support::FakeDocker;

fn sample_run(root: PathBuf) -> OneReplicaRun {
    OneReplicaRun {
        run_id: "0123456789abcdef".to_owned(),
        resource_uid: "0123456789abcdef0123456789abcdef".to_owned(),
        image_id: "sha256:image".to_owned(),
        member: OneReplicaMember {
            ordinal: 1,
            server_name: "km-one-012345".to_owned(),
            container_name: "kuberic-mssql-one-0123456789abcdef".to_owned(),
            network_name: "km-one-0123456789abcdef".to_owned(),
            data_directory: root.join("member-1"),
            environment_file: root.join("credentials/container.env"),
        },
    }
}

#[test]
fn explicit_config_uses_exact_one_replica_root() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("fixture");
    let config = OneReplicaConfig::for_test_fixture(&root).unwrap();
    assert_eq!(config.root(), root);
    assert_eq!(ONE_REPLICA_ROOT_ENV, "SQLSERVER_ONE_REPLICA_ROOT");
    assert_eq!(
        config.resources().container_memory_bytes,
        3 * 1024 * 1024 * 1024
    );
    assert_eq!(
        config.resources().minimum_available_memory_bytes,
        5 * 1024 * 1024 * 1024
    );
    assert_eq!(config.deadlines().complete_run.as_secs(), 600);
    assert_eq!(config.sql_server_environment(), [("ACCEPT_EULA", "Y")]);
}

#[test]
fn journal_schema_is_strict_and_contains_exactly_one_member() {
    let root = PathBuf::from("/fixture");
    let mut journal = OneReplicaJournal::new(sample_run(root));
    journal.state = RunState::Ready;
    let bytes = journal.to_json().unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["schema_version"], ONE_REPLICA_JOURNAL_SCHEMA_VERSION);
    assert_eq!(value["run"]["member"]["ordinal"], 1);
    assert!(value["run"].get("members").is_none());
    assert_eq!(OneReplicaJournal::from_json(&bytes).unwrap(), journal);

    let mut unsupported = value.clone();
    unsupported["schema_version"] = serde_json::json!(2);
    assert!(OneReplicaJournal::from_json(&serde_json::to_vec(&unsupported).unwrap()).is_err());
    let mut unknown = value;
    unknown["unknown"] = serde_json::json!(true);
    assert!(OneReplicaJournal::from_json(&serde_json::to_vec(&unknown).unwrap()).is_err());
    assert!(OneReplicaJournal::from_json(b"{").is_err());
    let text = String::from_utf8(bytes).unwrap();
    for secret in [
        "ACCEPT_EULA=Y",
        "MSSQL_SA_PASSWORD",
        "actual-password",
        "PRIVATE KEY",
    ] {
        assert!(!text.contains(secret), "{secret}");
    }
}

#[test]
fn legacy_owner_marker_is_refused_before_docker_access() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        root.join("owner"),
        b"kuberic-sqlserver-observer-container-v1\n",
    )
    .unwrap();

    let error =
        cleanup_one_replica_fixture_with(&root, &FakeDocker::default(), BoundedProcessRunner)
            .unwrap_err()
            .to_string();
    assert!(error.contains(&root.display().to_string()));
    assert!(error.contains("not adopted or deleted"));
    assert!(error.contains(LEGACY_CLEANUP_SECTION));
}

#[test]
fn legacy_ownership_record_and_combined_indicators_are_refused() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(root.join("fixture-run.json"), b"{}").unwrap();
    let error =
        cleanup_one_replica_fixture_with(&root, &FakeDocker::default(), BoundedProcessRunner)
            .unwrap_err()
            .to_string();
    assert!(error.contains("fixture-run.json"));
    assert!(!error.contains("legacy owner marker"));

    fs::write(
        root.join("owner"),
        b"kuberic-sqlserver-observer-container-v1\n",
    )
    .unwrap();
    let legacy_container = legacy_container_name(&root).unwrap();
    let docker = FakeDocker::default().with_container(&legacy_container);
    let error = cleanup_one_replica_fixture_with(&root, &docker, BoundedProcessRunner)
        .unwrap_err()
        .to_string();
    assert!(error.contains("fixture-run.json"));
    assert!(error.contains("legacy owner marker"));
    assert!(error.contains(&legacy_container));
    assert!(error.contains("not adopted or deleted"));
}

#[test]
fn cleanup_binary_rejects_invalid_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_mssql-one-replica-fixture"))
        .arg("invalid")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("usage: mssql-one-replica-fixture cleanup --root <fixture-root>")
    );
}

#[test]
fn successful_cleanup_summary_is_stable_and_secret_free() {
    let summary = format_cleanup_summary(
        &cleanup_one_replica_fixture_with(
            tempfile::tempdir().unwrap().path().join("absent"),
            &FakeDocker::default(),
            BoundedProcessRunner,
        )
        .unwrap(),
    );
    assert_eq!(
        summary,
        "one-replica cleanup complete: removed=0, unresolved=0, journal=absent"
    );
    assert!(!summary.contains("password"));
    assert!(!summary.contains("ACCEPT_EULA"));
}

#[test]
fn repository_routes_one_replica_validation_without_python_fixture_helpers() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .unwrap();
    let justfile = fs::read_to_string(root.join("justfile")).unwrap();
    assert!(justfile.contains("test-live-one-replica root=\"target/mssql-one-replica\":"));
    assert!(
        justfile.contains("cargo test --locked -p kuberic-mssql-tests --test live_one_replica")
    );
    assert!(justfile.contains(
        "cargo run --locked -p kuberic-mssql-tests --bin mssql-one-replica-fixture -- cleanup --root"
    ));
    for removed in [
        "sqlserver_fixture.py",
        "test-ci-helpers",
        "SQLSERVER_FIXTURE_DIR",
        "SQLSERVER_LIVE_ABSENT_CONFIG",
        "test-live-kuberic",
        "test-live-shared",
        "verify-live-cli",
    ] {
        assert!(!justfile.contains(removed), "{removed}");
    }

    let workflow = fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert!(workflow.contains("Run Rust-owned one- and three-replica validation"));
    assert!(workflow.contains("just ci"));
    assert!(workflow.contains("just cleanup"));
}
