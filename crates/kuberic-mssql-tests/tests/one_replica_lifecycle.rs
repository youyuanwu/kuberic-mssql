use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use kuberic_mssql_tests::one_replica::{
    OneReplicaJournal, OneReplicaJournalStore, OneReplicaMember, OneReplicaRun,
    cleanup_one_replica_fixture,
};
use kuberic_mssql_tests::three_replica::{
    ProcessIncarnation, ResourceBinding, ResourceKind, ResourceRecord, ResourceState, RunState,
    current_process_incarnation,
};

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
    let first = cleanup_one_replica_fixture(&root).unwrap();
    let second = cleanup_one_replica_fixture(&root).unwrap();
    assert!(first.report.succeeded());
    assert!(second.report.succeeded());
    assert!(first.journal_path.is_none());
    assert!(second.journal_path.is_none());
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

    let evidence = cleanup_one_replica_fixture(&root).unwrap();
    assert!(evidence.report.succeeded());
}

#[test]
fn cleanup_rejects_malformed_and_unsupported_journals() {
    let parent = tempfile::tempdir().unwrap();
    let malformed = parent.path().join("malformed");
    fs::create_dir(&malformed).unwrap();
    fs::set_permissions(&malformed, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(malformed.join("ownership.json"), b"{").unwrap();
    assert!(cleanup_one_replica_fixture(&malformed).is_err());

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
    assert!(cleanup_one_replica_fixture(&unsupported).is_err());
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

    let error = cleanup_one_replica_fixture(&root).unwrap_err().to_string();
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

    let evidence = cleanup_one_replica_fixture(&root).unwrap();
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

    let error = cleanup_one_replica_fixture(&root).unwrap_err().to_string();
    assert!(error.contains("owner identity is unavailable"));
}
