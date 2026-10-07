use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use kuberic_mssql_tests::one_replica::{
    OneReplicaJournal, OneReplicaJournalStore, OneReplicaMember, OneReplicaRun,
    acquire_one_replica_root_lock, cleanup_one_replica_fixture_with,
};
use kuberic_mssql_tests::three_replica::{
    BoundedProcessRunner, PrivateFile, ProcessIncarnation, ResourceBinding, ResourceKind,
    ResourceRecord, ResourceState, RunState, acquire_root_lock, current_process_incarnation,
};

mod support;
use support::FakeDocker;

const CHILD_ROOT_ENV: &str = "KUBERIC_MSSQL_ONE_REPLICA_CHILD_ROOT";
const CHILD_READY_ENV: &str = "KUBERIC_MSSQL_ONE_REPLICA_CHILD_READY";

fn cleanup(
    root: &Path,
) -> Result<
    kuberic_mssql_tests::one_replica::OneReplicaCleanupEvidence,
    kuberic_mssql_tests::one_replica::OneReplicaCleanupError,
> {
    cleanup_one_replica_fixture_with(root, &FakeDocker::default(), BoundedProcessRunner)
}

fn sample_run(root: &Path) -> OneReplicaRun {
    OneReplicaRun {
        run_id: "fedcba9876543210".to_owned(),
        resource_uid: "fedcba9876543210fedcba9876543210".to_owned(),
        image_id: "sha256:image".to_owned(),
        member: OneReplicaMember {
            ordinal: 1,
            server_name: "km-one-fedcba".to_owned(),
            container_name: "kuberic-mssql-one-fedcba9876543210".to_owned(),
            network_name: "km-one-fedcba9876543210".to_owned(),
            data_directory: root.join("member-1"),
            environment_file: root.join("credentials/container.env"),
        },
    }
}

#[test]
fn one_replica_blocked_owner_child() {
    let Some(root) = std::env::var_os(CHILD_ROOT_ENV).map(std::path::PathBuf::from) else {
        return;
    };
    let ready =
        std::path::PathBuf::from(std::env::var_os(CHILD_READY_ENV).expect("child ready path"));
    let store = OneReplicaJournalStore::initialize(&root).unwrap();
    let mut journal = store.create(sample_run(&root)).unwrap();
    store.block_for_current_process(&mut journal).unwrap();
    fs::write(ready, b"ready").unwrap();
    loop {
        thread::park();
    }
}

#[test]
fn separate_process_recovery_refuses_live_owner_then_recovers_after_death() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("separate-owner");
    let ready = parent.path().join("ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "one_replica_blocked_owner_child", "--nocapture"])
        .env(CHILD_ROOT_ENV, &root)
        .env(CHILD_READY_ENV, &ready)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.is_file() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(25));
    }
    assert!(ready.is_file());
    assert!(
        cleanup(&root)
            .unwrap_err()
            .to_string()
            .contains("still alive")
    );

    child.kill().unwrap();
    child.wait().unwrap();
    let evidence = cleanup(&root).unwrap();
    assert!(evidence.report.succeeded());
    let journal =
        OneReplicaJournal::from_json(&fs::read(root.join("ownership.json")).unwrap()).unwrap();
    assert_eq!(journal.state, RunState::Removed);
    assert!(journal.blocked_owner.is_none());
}

fn write_journal(root: &Path, journal: &OneReplicaJournal) {
    fs::create_dir_all(root).unwrap();
    fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("ownership.json");
    fs::write(&path, journal.to_json().unwrap()).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn cleanup_is_idempotent_when_root_is_absent() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("absent");
    let first = cleanup(&root).unwrap();
    let second = cleanup(&root).unwrap();
    assert!(first.report.succeeded());
    assert!(second.report.succeeded());
    assert!(first.journal_path.is_none());
    assert!(second.journal_path.is_none());
}

#[test]
fn one_and_three_replica_lifecycles_contend_on_the_same_root_lock() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("shared-root");
    let three_replica = acquire_root_lock(&root).unwrap();
    assert!(acquire_one_replica_root_lock(&root).is_err());
    drop(three_replica);
    assert!(acquire_one_replica_root_lock(&root).is_ok());
}

#[test]
fn cleanup_is_idempotent_after_all_resources_are_removed() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("removed");
    let mut journal = OneReplicaJournal::new(sample_run(&root));
    journal.state = RunState::Removed;
    journal.resources.extend([
        ResourceRecord {
            kind: ResourceKind::Network,
            logical_name: "docker-network".to_owned(),
            path: None,
            intent: None,
            binding: Some(ResourceBinding {
                immutable_id: "network-id".to_owned(),
                attributes_sha256: "a".repeat(64),
            }),
            state: ResourceState::Removed,
        },
        ResourceRecord {
            kind: ResourceKind::Container,
            logical_name: "container-1".to_owned(),
            path: None,
            intent: None,
            binding: Some(ResourceBinding {
                immutable_id: "container-id".to_owned(),
                attributes_sha256: "b".repeat(64),
            }),
            state: ResourceState::Removed,
        },
    ]);
    write_journal(&root, &journal);

    let evidence = cleanup(&root).unwrap();
    assert!(evidence.report.succeeded());
}

#[test]
fn absent_container_recovers_even_when_credentials_are_missing() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("missing-credentials");
    let mut journal = OneReplicaJournal::new(sample_run(&root));
    journal.resources.push(ResourceRecord {
        kind: ResourceKind::Container,
        logical_name: "container-1".to_owned(),
        path: None,
        intent: Some(ResourceBinding {
            immutable_id: journal.run.member.container_name.clone(),
            attributes_sha256: "a".repeat(64),
        }),
        binding: Some(ResourceBinding {
            immutable_id: "container-id".to_owned(),
            attributes_sha256: "b".repeat(64),
        }),
        state: ResourceState::Bound,
    });
    write_journal(&root, &journal);

    let evidence = cleanup(&root).unwrap();
    assert!(evidence.report.succeeded());
    let stored =
        OneReplicaJournal::from_json(&fs::read(root.join("ownership.json")).unwrap()).unwrap();
    assert_eq!(stored.resources[0].state, ResourceState::Removed);
}

#[test]
fn standalone_cleanup_reports_replacement_found_by_stable_name() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("replacement");
    fs::create_dir_all(root.join("credentials")).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(root.join("credentials"), fs::Permissions::from_mode(0o700)).unwrap();
    PrivateFile::create_text(root.join("credentials/sa-password"), "ValidPassword1!").unwrap();
    let mut journal = OneReplicaJournal::new(sample_run(&root));
    journal.resources.push(ResourceRecord {
        kind: ResourceKind::Container,
        logical_name: "container-1".to_owned(),
        path: None,
        intent: Some(ResourceBinding {
            immutable_id: journal.run.member.container_name.clone(),
            attributes_sha256: "a".repeat(64),
        }),
        binding: Some(ResourceBinding {
            immutable_id: "removed-original-id".to_owned(),
            attributes_sha256: "b".repeat(64),
        }),
        state: ResourceState::Bound,
    });
    write_journal(&root, &journal);
    let docker = FakeDocker::default().with_container(journal.run.member.container_name.clone());

    let error = cleanup_one_replica_fixture_with(&root, &docker, BoundedProcessRunner)
        .unwrap_err()
        .to_string();
    assert!(error.contains("container-1"));
    assert!(error.contains("container identity or immutable attributes changed"));
}

#[test]
fn standalone_cleanup_reports_replaced_path_binding() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("path-replacement");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("secret");
    let original = PrivateFile::create_text(&path, "original").unwrap();
    let mut journal = OneReplicaJournal::new(sample_run(&root));
    journal.resources.push(ResourceRecord {
        kind: ResourceKind::SecretFile,
        logical_name: "secret-file".to_owned(),
        path: Some(path.clone()),
        intent: None,
        binding: Some(original.binding().clone()),
        state: ResourceState::Bound,
    });
    write_journal(&root, &journal);
    fs::remove_file(&path).unwrap();
    PrivateFile::create_text(&path, "replacement").unwrap();

    let error = cleanup(&root).unwrap_err().to_string();
    assert!(error.contains("secret-file"));
    assert!(error.contains("path immutable binding changed"));
}

#[test]
fn cleanup_rejects_malformed_and_unsupported_journals() {
    let parent = tempfile::tempdir().unwrap();
    let malformed = parent.path().join("malformed");
    fs::create_dir(&malformed).unwrap();
    fs::set_permissions(&malformed, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(malformed.join("ownership.json"), b"{").unwrap();
    assert!(cleanup(&malformed).is_err());

    let unsupported = parent.path().join("unsupported");
    let mut value: serde_json::Value = serde_json::from_slice(
        &OneReplicaJournal::new(sample_run(&unsupported))
            .to_json()
            .unwrap(),
    )
    .unwrap();
    value["schema_version"] = serde_json::json!(99);
    fs::create_dir(&unsupported).unwrap();
    fs::set_permissions(&unsupported, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        unsupported.join("ownership.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    assert!(cleanup(&unsupported).is_err());
}

#[test]
fn journal_transitions_are_durable_and_fail_closed() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("transitions");
    let store = OneReplicaJournalStore::initialize(&root).unwrap();
    let mut journal = store.create(sample_run(&root)).unwrap();
    let index = store
        .record_intent(
            &mut journal,
            ResourceRecord {
                kind: ResourceKind::Container,
                logical_name: "container-1".to_owned(),
                path: None,
                intent: Some(ResourceBinding {
                    immutable_id: "container-name".to_owned(),
                    attributes_sha256: "a".repeat(64),
                }),
                binding: None,
                state: ResourceState::Intended,
            },
        )
        .unwrap();
    assert!(
        store
            .bind(
                &mut journal,
                index,
                ResourceBinding {
                    immutable_id: "container-id".to_owned(),
                    attributes_sha256: "b".repeat(64),
                },
            )
            .is_err()
    );
    store.mark_dispatched(&mut journal, index).unwrap();
    store
        .bind(
            &mut journal,
            index,
            ResourceBinding {
                immutable_id: "container-id".to_owned(),
                attributes_sha256: "b".repeat(64),
            },
        )
        .unwrap();

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(persisted.resources[index].state, ResourceState::Bound);
    assert_eq!(
        persisted.resources[index]
            .binding
            .as_ref()
            .unwrap()
            .immutable_id,
        "container-id"
    );
    assert!(
        !String::from_utf8(persisted.to_json().unwrap())
            .unwrap()
            .contains("secret_value")
    );
}

#[test]
fn cleanup_refuses_a_live_exact_owner() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("live-owner");
    let mut journal = OneReplicaJournal::new(sample_run(&root));
    journal.state = RunState::Blocked;
    journal.blocked_owner = Some(current_process_incarnation().unwrap());
    write_journal(&root, &journal);

    let error = cleanup(&root).unwrap_err().to_string();
    assert!(error.contains("owner process is still alive"));
}

#[test]
fn cleanup_accepts_stale_owner_and_pid_reuse_evidence() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("stale-owner");
    let current = current_process_incarnation().unwrap();
    let mut journal = OneReplicaJournal::new(sample_run(&root));
    journal.state = RunState::Blocked;
    journal.blocked_owner = Some(ProcessIncarnation {
        pid: current.pid,
        starttime_ticks: current.starttime_ticks.saturating_add(1),
    });
    write_journal(&root, &journal);

    let evidence = cleanup(&root).unwrap();
    assert!(evidence.report.succeeded());
    let stored =
        OneReplicaJournal::from_json(&fs::read(root.join("ownership.json")).unwrap()).unwrap();
    assert_eq!(stored.state, RunState::Removed);
    assert!(stored.blocked_owner.is_none());
}

#[test]
fn unknown_blocked_owner_remains_fail_closed() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("unknown-owner");
    let mut journal = OneReplicaJournal::new(sample_run(&root));
    journal.state = RunState::Blocked;
    journal.blocked_owner_unknown = true;
    write_journal(&root, &journal);

    let error = cleanup(&root).unwrap_err().to_string();
    assert!(error.contains("owner identity is unavailable"));
}
