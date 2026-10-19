use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::sync::Mutex;

use kuberic_mssql::SqlServerEulaAcknowledgement;
use kuberic_mssql_tests::three_replica::{
    ACKNOWLEDGEMENT_SCHEMA_VERSION, CleanupError, CleanupReport, CombinedFixtureError,
    FailureCategory, FailureStage, FixtureConfig, FixtureConfigError, JOURNAL_SCHEMA_VERSION,
    JournalError, KubericMember, LaunchAuthorization, NativeTopologyPhase, OwnershipJournal,
    PINNED_SQL_SERVER_IMAGE, ResourceBinding, ResourceKind, ResourcePolicy, ResourceRecord,
    ResourceState, RunState, SanitizedFailure, SqlMember, StageDeadlines, TopologyRun,
    combine_with_cleanup,
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
    let source = authorization.source().expect("file-backed source");
    assert_eq!(source.path(), path.canonicalize().unwrap());
    assert_ne!(source.sha256(), &[0; 32]);
    authorization.revalidate().unwrap();
}

#[test]
fn test_fixture_authorization_is_affirmative_sourceless_and_idempotent() {
    let directory = tempfile::tempdir().unwrap();
    let authorization = LaunchAuthorization::for_test_fixture();
    assert_eq!(authorization.source(), None);
    assert_eq!(
        authorization.sql_server_environment(),
        [("ACCEPT_EULA", "Y")]
    );
    authorization.revalidate().unwrap();
    authorization.revalidate().unwrap();

    let config = FixtureConfig::for_test_fixture(directory.path().join("fixture")).unwrap();
    assert_eq!(config.authorization().source(), None);
    assert_eq!(
        config.authorization().sql_server_environment(),
        [("ACCEPT_EULA", "Y")]
    );
    config.authorization().revalidate().unwrap();
}

#[test]
fn production_and_file_negative_paths_remain_explicit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("acknowledgement.json");

    assert!(SqlServerEulaAcknowledgement::new(false).is_err());
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
fn shipped_false_acknowledgement_example_parses_but_cannot_authorize_launch() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/eula-acknowledgement.example.json");
    let document: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).expect("example is valid JSON");
    assert_eq!(document["schema_version"], 1);
    assert_eq!(document["sql_server_eula"]["accepted"], false);
    assert_eq!(
        LaunchAuthorization::load(&path).unwrap_err(),
        FixtureConfigError::AcknowledgementDenied
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
    assert!(justfile.contains("test-live-one-replica root=\"target/mssql-one-replica\":"));
    assert!(
        justfile
            .contains("test-live-one-replica-recovery root=\"target/mssql-one-replica-recovery\":")
    );
    assert!(justfile.contains("cleanup-live-one-replica root=\"target/mssql-one-replica\":"));
    assert!(justfile.contains("test-live-three-replica root=\"target/mssql-three-replica\":"));
    assert!(justfile.contains(
        "test-live-native-commit-coverage root=\"target/mssql-native-commit-coverage\":"
    ));
    assert!(justfile.contains(
        "test-live-native-prefix-coverage root=\"target/mssql-native-prefix-coverage\":"
    ));
    assert!(justfile.contains(
        "three_replica_mssql_native_prefix_coverage -- --ignored --exact --test-threads=1 --nocapture"
    ));
    assert!(justfile.contains(
        "three_replica_mssql_native_commit_coverage -- --ignored --exact --test-threads=1 --nocapture"
    ));
    assert!(
        justfile.contains(
            "test-live-three-replica-restart root=\"target/mssql-three-replica-restart\":"
        )
    );
    assert!(justfile.contains("cleanup-live-three-replica root=\"target/mssql-three-replica\":"));
    assert!(
        justfile
            .contains("test-live-three-replica-signal root=\"target/mssql-three-replica-signal\":")
    );
    assert!(justfile.contains(
        "test-live-three-replica-recovery root=\"target/mssql-three-replica-recovery\":"
    ));
    assert!(
        justfile
            .contains("test-live-three-replica-faults root=\"target/mssql-three-replica-faults\":")
    );
    assert!(justfile.contains(
        "test-live-three-replica-conflicts root=\"target/mssql-three-replica-conflicts\":"
    ));
    assert!(justfile.contains(
        "test-live-three-replica-terminal-seed root=\"target/mssql-three-replica-terminal-seed\":"
    ));
    assert!(justfile.contains(
        "test-live-three-replica-target-evidence root=\"target/mssql-three-replica-target-evidence\":"
    ));
    assert_eq!(
        justfile
            .lines()
            .filter(|line| line.contains("env -u SQLSERVER_TEST_EULA_ACCEPTED"))
            .count(),
        16
    );
    assert!(!justfile.contains("KUBERIC_MSSQL_EULA_ACKNOWLEDGEMENT"));
    assert!(!justfile.contains("acknowledgement root="));
    assert!(justfile.contains("test-live-lease-fence root=\"target/mssql-lease-fence\":"));
    assert!(justfile.contains("cleanup-live-lease-fence root=\"target/mssql-lease-fence\":"));
    assert!(
        justfile.contains("KUBERIC_MSSQL_THREE_REPLICA_ROOT=\"$(realpath -m {{quote(root)}})\"")
    );
    assert!(justfile.contains(
        "cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_mssql_happy_path -- --ignored --exact --test-threads=1"
    ));
    assert!(justfile.contains(
        "cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_mssql_same_root_restart -- --ignored --exact --test-threads=1 --nocapture"
    ));
    assert!(justfile.contains(
        "cargo run --locked -p kuberic-mssql-tests --bin mssql-three-replica-fixture -- cleanup --root"
    ));
    assert!(justfile.contains(
        "three_replica_sigterm_during_owned_launch_is_recoverable -- --ignored --exact --test-threads=1 --nocapture"
    ));
    assert!(justfile.contains(
        "three_replica_mssql_bootstrap_fault_boundaries -- --ignored --test-threads=1 --nocapture"
    ));
    assert!(justfile.contains(
        "three_replica_mssql_conflict_evidence -- --ignored --test-threads=1 --nocapture"
    ));
    assert!(justfile.contains(
        "three_replica_mssql_terminal_seed_conflicts -- --ignored --test-threads=1 --nocapture"
    ));

    for recipe in ["default: check", "check: fmt-check clippy test"] {
        let line = justfile.lines().find(|line| *line == recipe).unwrap();
        assert!(!line.contains("test-live-three-replica"));
        assert!(!line.contains("test-live-one-replica"));
    }
    assert!(justfile.contains(
        "cargo nextest run --locked --workspace --all-features --test-threads=1 --profile default"
    ));
    assert!(justfile.contains("cargo test --locked --workspace --all-features --doc"));
    let ci = justfile
        .lines()
        .find(|line| line.starts_with("ci:"))
        .unwrap();
    assert_eq!(ci, "ci: setup check test-live-nextest");
    assert!(justfile.contains(
        "cargo nextest run --locked --workspace --all-features --profile live --run-ignored only --test-threads=1"
    ));
    assert!(
        justfile
            .contains("bash crates/kuberic-mssql-tests/scripts/cleanup_nextest_fixtures.sh --all")
    );
    assert!(!justfile.contains("sqlserver_fixture.py"));
    assert!(!justfile.contains("test-ci-helpers"));
    assert!(!justfile.contains("validate-live"));
    assert!(!justfile.contains("test-live-kuberic"));
    assert!(!justfile.contains("test-live-shared"));

    let workflow = fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert!(workflow.contains("just cleanup\n"));
    assert!(!workflow.contains("just cleanup-live-three-replica\n"));
    assert!(!workflow.contains("upload-artifact"));
    let cargo_config = fs::read_to_string(root.join(".cargo/config.toml")).unwrap();
    assert!(cargo_config.contains("ACCEPT_EULA = { value = \"Y\", force = true }"));
}

fn nextest_cleanup_test_environment() -> (tempfile::TempDir, std::path::PathBuf) {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let directory = tempfile::tempdir_in(workspace.join("target")).unwrap();
    let script = workspace.join("crates/kuberic-mssql-tests/scripts/cleanup_nextest_fixtures.sh");
    (directory, script)
}

fn fake_cleanup_tool(directory: &Path, name: &str, failure: &str) {
    let tool = directory.join(name);
    fs::write(
        &tool,
        format!(
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"$DISPATCH_LOG\"\n\
             if [[ \"$*\" == *{failure}* ]]; then exit 1; fi\n"
        ),
    )
    .unwrap();
    fs::set_permissions(tool, fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn nextest_cleanup_visits_all_nested_family_journals_after_failure() {
    let (directory, script) = nextest_cleanup_test_environment();
    let root = directory.path().join("runner");
    let cases = [
        "one-replica",
        "one-replica/recovery-pid",
        "three-replica/fail-first",
        "three-replica/other-case",
        "lease-fence/expiry",
        "lease-fence/active-restart",
    ];
    for case in cases {
        let path = root.join(case);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("ownership.json"), "{}").unwrap();
    }
    let outside = directory.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("ownership.json"), "{}").unwrap();
    symlink(&outside, root.join("three-replica/foreign-link")).unwrap();
    fs::create_dir_all(root.join("unrelated")).unwrap();
    fs::write(root.join("unrelated/ownership.json"), "{}").unwrap();
    fake_cleanup_tool(directory.path(), "cargo", "fail-first");
    let log = directory.path().join("dispatch.log");
    let output = std::process::Command::new("bash")
        .arg(&script)
        .arg(&root)
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", directory.path().display()),
        )
        .env("DISPATCH_LOG", &log)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("nextest fixture cleanup failed"));
    let dispatched = fs::read_to_string(log).unwrap();
    assert_eq!(dispatched.lines().count(), cases.len());
    for case in cases {
        let binary = if case.starts_with("one-replica") {
            "mssql-one-replica-fixture"
        } else {
            "mssql-three-replica-fixture"
        };
        assert!(dispatched.contains(&format!(
            "--bin {binary} -- cleanup --root {}",
            root.join(case).display()
        )));
        assert!(root.join(case).join("ownership.json").exists());
    }
    assert!(!dispatched.contains("outside"));
    assert!(!dispatched.contains("unrelated"));
    assert!(!dispatched.contains("foreign-link"));
}

#[test]
fn nextest_cleanup_rejects_symlink_evidence_and_accepts_absent_root() {
    let (directory, script) = nextest_cleanup_test_environment();
    let root = directory.path().join("runner");
    let outside = directory.path().join("outside");
    fs::create_dir_all(root.join("three-replica")).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("ownership.json"), "{}").unwrap();
    symlink(&outside, root.join("one-replica")).unwrap();
    symlink(
        outside.join("ownership.json"),
        root.join("three-replica/ownership.json"),
    )
    .unwrap();
    let output = std::process::Command::new("bash")
        .arg(&script)
        .arg(&root)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("symlink fixture family"));
    assert!(error.contains("nonregular fixture journal"));
    assert!(outside.join("ownership.json").exists());
    assert!(
        std::process::Command::new("bash")
            .arg(script)
            .arg(directory.path().join("absent"))
            .status()
            .unwrap()
            .success()
    );
}

#[test]
fn aggregate_cleanup_attempts_nextest_after_legacy_failure() {
    let (directory, script) = nextest_cleanup_test_environment();
    fake_cleanup_tool(directory.path(), "just", "cleanup-live-one-replica");
    let log = directory.path().join("dispatch.log");
    let output = std::process::Command::new("bash")
        .arg(script)
        .arg("--all")
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", directory.path().display()),
        )
        .env("DISPATCH_LOG", &log)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("fixture cleanup recipe failed"));
    let dispatched = fs::read_to_string(log).unwrap();
    assert_eq!(
        dispatched.lines().collect::<Vec<_>>(),
        [
            "cleanup-live-one-replica",
            "cleanup-live-three-replica",
            "cleanup-live-lease-fence",
            "cleanup-live-nextest",
        ]
    );
}

#[test]
fn lease_cleanup_absence_is_noop_and_failure_does_not_suppress_second_root() {
    let (directory, _) = nextest_cleanup_test_environment();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let root = directory.path().join("absent-lease");
    fake_cleanup_tool(directory.path(), "cargo", "expiry");
    let log = directory.path().join("dispatch.log");
    let run = || {
        std::process::Command::new("just")
            .arg("--justfile")
            .arg(workspace.join("justfile"))
            .arg("cleanup-live-lease-fence")
            .arg(&root)
            .env("HOME", directory.path())
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    directory.path().display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("DISPATCH_LOG", &log)
            .output()
            .unwrap()
    };
    assert!(run().status.success());
    assert!(!log.exists());
    fs::create_dir(&root).unwrap();
    assert!(!run().status.success());
    let dispatched = fs::read_to_string(log).unwrap();
    assert_eq!(dispatched.lines().count(), 2);
    assert!(dispatched.contains(root.join("expiry").to_str().unwrap()));
    assert!(dispatched.contains(root.join("active-restart").to_str().unwrap()));
}

#[test]
fn nextest_cleanup_reports_discovery_failure_after_visiting_found_journals() {
    let (directory, script) = nextest_cleanup_test_environment();
    let root = directory.path().join("runner");
    fs::create_dir_all(root.join("three-replica")).unwrap();
    fs::write(root.join("three-replica/ownership.json"), "{}").unwrap();
    fake_cleanup_tool(directory.path(), "cargo", "never-matches");
    let find = directory.path().join("find");
    fs::write(
        &find,
        "#!/usr/bin/env bash\nprintf '%s\\0' \"$2/ownership.json\"\nexit 1\n",
    )
    .unwrap();
    fs::set_permissions(find, fs::Permissions::from_mode(0o700)).unwrap();
    let log = directory.path().join("dispatch.log");
    let output = std::process::Command::new("bash")
        .arg(script)
        .arg(&root)
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", directory.path().display()),
        )
        .env("DISPATCH_LOG", &log)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("journal discovery failed"));
    let dispatched = fs::read_to_string(log).unwrap();
    assert_eq!(dispatched.lines().count(), 1);
    assert!(dispatched.contains(&root.join("three-replica").display().to_string()));
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
fn schema_nine_preserves_previous_shapes_and_adds_explicit_lifecycle_exclusion() {
    let current_bytes = include_bytes!("../fixtures/ownership-journal-schema-8.json");
    let current = OwnershipJournal::from_json(current_bytes).unwrap();
    assert_eq!(current.schema_version, JOURNAL_SCHEMA_VERSION);
    assert_eq!(current.native_phase, NativeTopologyPhase::Absent);
    let mut expected: serde_json::Value = serde_json::from_slice(current_bytes).unwrap();
    expected["schema_version"] = serde_json::json!(JOURNAL_SCHEMA_VERSION);
    expected["lifecycle_exclusion"] = serde_json::Value::Null;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&current.to_json().unwrap()).unwrap(),
        expected
    );

    let previous_seven = OwnershipJournal::from_json(include_bytes!(
        "../fixtures/ownership-journal-schema-7.json"
    ))
    .unwrap();
    assert_eq!(previous_seven, current);

    let previous_six = OwnershipJournal::from_json(include_bytes!(
        "../fixtures/ownership-journal-schema-6.json"
    ))
    .unwrap();
    assert_eq!(previous_six, current);
}

#[test]
fn pending_native_lifecycle_exclusion_survives_owner_reentry() {
    use kuberic_mssql_tests::three_replica::{LifecycleExclusion, LifecyclePhase};
    let directory = tempfile::tempdir().unwrap();
    let mut journal = OwnershipJournal::new(sample_run(directory.path()));
    journal.lifecycle_exclusion = Some(LifecycleExclusion {
        container_id: "exact-container".to_owned(),
        phase: LifecyclePhase::StopPending,
    });
    let value = journal.to_json().unwrap();
    let restored = OwnershipJournal::from_json(&value).unwrap();
    assert!(
        !restored
            .lifecycle_exclusion
            .unwrap()
            .permits_restart("exact-container")
    );
    let mut missing: serde_json::Value = serde_json::from_slice(&value).unwrap();
    missing
        .as_object_mut()
        .unwrap()
        .remove("lifecycle_exclusion");
    assert_eq!(
        OwnershipJournal::from_json(&serde_json::to_vec(&missing).unwrap()),
        Err(JournalError::Malformed)
    );
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

    let mut missing_phase: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../fixtures/ownership-journal-schema-8.json"
    ))
    .unwrap();
    missing_phase
        .as_object_mut()
        .unwrap()
        .remove("native_phase");
    assert_eq!(
        OwnershipJournal::from_json(&serde_json::to_vec(&missing_phase).unwrap()).unwrap_err(),
        JournalError::Malformed
    );

    let mut inconsistent_phase: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../fixtures/ownership-journal-schema-8.json"
    ))
    .unwrap();
    inconsistent_phase["native_phase"] = serde_json::json!("converged");
    assert_eq!(
        OwnershipJournal::from_json(&serde_json::to_vec(&inconsistent_phase).unwrap()).unwrap_err(),
        JournalError::Malformed
    );
}

#[test]
fn schema_five_journal_migrates_without_inventing_a_blocked_owner() {
    let directory = tempfile::tempdir().unwrap();
    let journal = OwnershipJournal::new(sample_run(directory.path()));
    let mut value: serde_json::Value = serde_json::from_slice(&journal.to_json().unwrap()).unwrap();
    value["schema_version"] = serde_json::json!(5);
    value.as_object_mut().unwrap().remove("blocked_owner");
    let migrated = OwnershipJournal::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(migrated.schema_version, JOURNAL_SCHEMA_VERSION);
    assert!(migrated.blocked_owner.is_none());
    assert!(!migrated.blocked_owner_unknown);

    value["state"] = serde_json::json!("blocked");
    let blocked = OwnershipJournal::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(blocked.schema_version, JOURNAL_SCHEMA_VERSION);
    assert!(blocked.blocked_owner.is_none());
    assert!(blocked.blocked_owner_unknown);
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

#[test]
fn combined_cleanup_diagnostics_include_the_logical_resource_name() {
    let report = CleanupReport {
        removed: Vec::new(),
        unresolved: vec!["member-data-2".to_owned()],
        errors: vec![CleanupError {
            resource: "member-data-2".to_owned(),
            failure: SanitizedFailure::new(
                FailureStage::Cleanup,
                FailureCategory::OwnershipMismatch,
            ),
        }],
    };
    let error = combine_with_cleanup::<()>(
        Err(SanitizedFailure::new(
            FailureStage::Setup,
            FailureCategory::ContainerCreation,
        )),
        &report,
    )
    .unwrap_err();
    assert!(error.to_string().contains("resource member-data-2"));
    assert!(!error.to_string().contains("Password"));
}
