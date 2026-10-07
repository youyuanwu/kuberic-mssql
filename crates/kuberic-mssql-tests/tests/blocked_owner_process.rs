use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use kuberic_mssql_tests::three_replica::{
    JournalStore, OwnershipJournal, ProcessIncarnation, RunState, cleanup_three_replica_fixture,
    create_blocked_owner_regression_fixture, process_incarnation, process_incarnation_matches_stat,
    retry_owner_regression_fixture,
};

const CHILD_MODE: &str = "KUBERIC_MSSQL_BLOCKED_OWNER_CHILD";
const RETRY_MODE: &str = "KUBERIC_MSSQL_BLOCKED_OWNER_RETRY";
const ROOT_ENV: &str = "KUBERIC_MSSQL_BLOCKED_OWNER_ROOT";
const READY_ENV: &str = "KUBERIC_MSSQL_BLOCKED_OWNER_READY";
const REFUSED_ENV: &str = "KUBERIC_MSSQL_BLOCKED_OWNER_REFUSED";

#[test]
fn blocked_owner_child() {
    let Some(root) = std::env::var_os(ROOT_ENV).map(PathBuf::from) else {
        return;
    };
    if std::env::var_os(RETRY_MODE).is_some() {
        retry_owner_regression_fixture(&root).expect("retry exact owner fixture");
        return;
    }
    if std::env::var_os(CHILD_MODE).is_none() {
        return;
    }
    let ready = PathBuf::from(std::env::var_os(READY_ENV).expect("ready path"));
    let refused = PathBuf::from(std::env::var_os(REFUSED_ENV).expect("refused path"));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let resource = runtime
        .block_on(create_blocked_owner_regression_fixture(&root))
        .expect("create blocked owner fixture");
    fs::write(&ready, resource.as_os_str().as_encoded_bytes()).unwrap();
    let error = cleanup_three_replica_fixture(&root).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("owner process incarnation is still alive")
    );
    fs::write(refused, b"refused").unwrap();
    loop {
        thread::park();
    }
}

#[test]
fn live_owner_process_blocks_cleanup_until_death_then_allows_recovery_and_retry() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("fixture");
    let ready = temporary.path().join("ready");
    let refused = temporary.path().join("same-process-refused");
    let mut child = spawn_child(&root, &ready, &refused, false);
    wait_for(Duration::from_secs(20), || {
        ready.is_file() && refused.is_file()
    });

    let journal = load_journal(&root);
    let owner = journal.blocked_owner.expect("blocked owner incarnation");
    assert_eq!(journal.state, RunState::Blocked);
    assert_eq!(owner.pid, child.id());
    assert_eq!(process_incarnation(child.id()).unwrap(), owner);
    let resource = PathBuf::from(String::from_utf8(fs::read(&ready).unwrap()).unwrap());
    assert!(resource.is_dir());

    let refused_cleanup = Command::new(env!("CARGO_BIN_EXE_mssql-three-replica-fixture"))
        .args(["cleanup", "--root"])
        .arg(&root)
        .output()
        .unwrap();
    assert!(!refused_cleanup.status.success());
    assert!(
        resource.is_dir(),
        "live-owner cleanup removed an owned path"
    );
    let still_blocked = load_journal(&root);
    assert_eq!(still_blocked.state, RunState::Blocked);
    assert!(still_blocked.blocked_owner.is_some());
    assert!(still_blocked.resources.iter().all(
        |resource| resource.state != kuberic_mssql_tests::three_replica::ResourceState::Removed
    ));

    child.kill().unwrap();
    child.wait().unwrap();
    let recovered = Command::new(env!("CARGO_BIN_EXE_mssql-three-replica-fixture"))
        .args(["cleanup", "--root"])
        .arg(&root)
        .output()
        .unwrap();
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    assert!(!resource.exists());
    let removed = load_journal(&root);
    assert_eq!(removed.state, RunState::Removed);
    assert!(removed.blocked_owner.is_none());

    let mut retry = spawn_child(&root, &ready, &refused, true);
    assert!(retry.wait().unwrap().success());
    let retried = load_journal(&root);
    assert_eq!(retried.state, RunState::Removed);
}

#[test]
fn process_incarnation_matching_rejects_pid_reuse_starttime_mismatch() {
    let pid = 4242;
    let exact = proc_stat(pid, 123_456);
    let reused = proc_stat(pid, 123_457);
    let expected = ProcessIncarnation {
        pid,
        starttime_ticks: 123_456,
    };
    assert!(process_incarnation_matches_stat(expected, &exact).unwrap());
    assert!(!process_incarnation_matches_stat(expected, &reused).unwrap());
    assert!(process_incarnation_matches_stat(expected, &proc_stat(pid + 1, 123_456)).is_err());
}

fn proc_stat(pid: u32, starttime: u64) -> String {
    let mut fields = vec!["0".to_owned(); 20];
    fields[0] = "S".to_owned();
    fields[19] = starttime.to_string();
    format!("{pid} (owner process with spaces) {}", fields.join(" "))
}

fn spawn_child(root: &Path, ready: &Path, refused: &Path, retry: bool) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("blocked_owner_child")
        .arg("--exact")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(ROOT_ENV, root)
        .env(READY_ENV, ready)
        .env(REFUSED_ENV, refused)
        .env_remove(CHILD_MODE)
        .env_remove(RETRY_MODE)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if retry {
        command.env(RETRY_MODE, "1");
    } else {
        command.env(CHILD_MODE, "1");
    }
    command.spawn().unwrap()
}

fn wait_for(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("condition did not become ready within {timeout:?}");
}

fn load_journal(root: &Path) -> OwnershipJournal {
    JournalStore::initialize(root)
        .unwrap()
        .load()
        .unwrap()
        .unwrap()
}
