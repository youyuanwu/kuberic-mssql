use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use kuberic_mssql_tests::one_replica::{
    ONE_REPLICA_FAULT_ENV, OneReplicaConfig, OneReplicaJournal, cleanup_one_replica_fixture,
    launch_one_replica, run_unique_scenarios,
};
use kuberic_mssql_tests::three_replica::{FailureCategory, FailureStage, SanitizedFailure};

const CHILD_MODE_ENV: &str = "KUBERIC_MSSQL_ONE_REPLICA_CHILD_MODE";
const CHILD_ROOT_ENV: &str = "SQLSERVER_ONE_REPLICA_ROOT";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires licensed SQL Server container prerequisites"]
async fn one_replica_mssql_observation_and_cli() {
    let config = OneReplicaConfig::from_environment().expect("valid one-replica fixture config");
    let fixture = launch_one_replica(config)
        .await
        .unwrap_or_else(|error| panic!("one-replica launch failed: {error}"));
    let evidence = fixture
        .run_with_cleanup(std::time::Duration::from_secs(120), |member| async move {
            run_unique_scenarios(
                &member,
                std::path::Path::new(env!("CARGO_BIN_EXE_sqlserver-observer-test")),
            )
            .await
            .map_err(|error| {
                SanitizedFailure::with_detail(
                    FailureStage::Test,
                    FailureCategory::SqlUnavailable,
                    error.to_string(),
                )
            })
        })
        .await
        .unwrap_or_else(|error| panic!("one-replica lifecycle failed: {error}"));
    assert!(evidence.absent_observed_at_unix_millis > 0);
    assert!(evidence.cli_observed_at_unix_millis > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_replica_recovery_child() {
    let Ok(mode) = std::env::var(CHILD_MODE_ENV) else {
        return;
    };
    let config = OneReplicaConfig::from_environment().unwrap();
    match mode.as_str() {
        "launch-fault" => {
            assert!(launch_one_replica(config).await.is_err());
        }
        "panic" => {
            let fixture = launch_one_replica(config).await.unwrap();
            let result = fixture
                .run_with_cleanup(Duration::from_secs(120), |_| async move {
                    panic!("one-replica panic checkpoint");
                    #[allow(unreachable_code)]
                    Ok::<(), SanitizedFailure>(())
                })
                .await;
            assert!(result.is_err());
        }
        "timeout" => {
            let fixture = launch_one_replica(config).await.unwrap();
            let result = fixture
                .run_with_cleanup(Duration::from_millis(100), |_| async move {
                    std::future::pending::<Result<(), SanitizedFailure>>().await
                })
                .await;
            assert!(result.is_err());
        }
        "error" => {
            let fixture = launch_one_replica(config).await.unwrap();
            let result = fixture
                .run_with_cleanup(Duration::from_secs(120), |_| async move {
                    Err::<(), _>(SanitizedFailure::new(
                        FailureStage::Test,
                        FailureCategory::SqlUnavailable,
                    ))
                })
                .await;
            assert!(result.is_err());
        }
        "scenario-fault" => {
            let fixture = launch_one_replica(config).await.unwrap();
            let result = fixture
                .run_with_cleanup(Duration::from_secs(120), |member| async move {
                    run_unique_scenarios(
                        &member,
                        Path::new(env!("CARGO_BIN_EXE_sqlserver-observer-test")),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| {
                        SanitizedFailure::with_detail(
                            FailureStage::Test,
                            FailureCategory::SqlUnavailable,
                            error.to_string(),
                        )
                    })
                })
                .await;
            assert!(result.is_err());
        }
        "signal" => {
            let fixture = launch_one_replica(config).await.unwrap();
            let result = fixture
                .run_with_cleanup(Duration::from_secs(120), |_| async move {
                    std::future::pending::<Result<(), SanitizedFailure>>().await
                })
                .await;
            assert!(result.is_err());
        }
        other => panic!("unknown one-replica child mode {other}"),
    }
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT_ENV).unwrap());
    assert_removed(&root);
}

#[test]
#[ignore = "requires licensed SQL Server container prerequisites"]
fn one_replica_recovery_fault_and_completion_paths() {
    for (mode, fault) in [
        ("launch-fault", Some("after-container-create")),
        ("panic", None),
        ("timeout", None),
        ("error", Some("before-cleanup")),
        ("scenario-fault", Some("before-cli-validation")),
    ] {
        let root = recovery_root(mode);
        let mut child = spawn_child(mode, &root, fault);
        wait_for_exit(&mut child, Duration::from_secs(180));
        cleanup_one_replica_fixture(&root).unwrap();
        assert_removed(&root);
        assert_no_owned_docker_resources();
    }
}

#[test]
#[ignore = "requires licensed SQL Server container prerequisites"]
fn one_replica_recovery_sigint_and_sigterm_paths() {
    for signal in [libc::SIGINT, libc::SIGTERM] {
        let root = recovery_root(&format!("signal-{signal}"));
        let mut child = spawn_child("signal", &root, None);
        wait_for_blocked_owner(&root, child.id(), Duration::from_secs(120));
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        wait_for_exit(&mut child, Duration::from_secs(120));
        cleanup_one_replica_fixture(&root).unwrap();
        assert_removed(&root);
        assert_no_owned_docker_resources();
    }
}

fn recovery_root(name: &str) -> PathBuf {
    let base = std::env::var_os("SQLSERVER_ONE_REPLICA_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/mssql-one-replica-recovery"));
    std::fs::create_dir_all(&base).unwrap();
    std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700)).unwrap();
    base.join(format!("{name}-{}", std::process::id()))
}

fn spawn_child(mode: &str, root: &Path, fault: Option<&str>) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "one_replica_recovery_child", "--nocapture"])
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_ROOT_ENV, root)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if let Some(fault) = fault {
        command.env(ONE_REPLICA_FAULT_ENV, fault);
    } else {
        command.env_remove(ONE_REPLICA_FAULT_ENV);
    }
    command.spawn().unwrap()
}

fn wait_for_exit(child: &mut Child, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child exited with {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("one-replica recovery child exceeded {timeout:?}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_blocked_owner(root: &Path, pid: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(bytes) = std::fs::read(root.join("ownership.json"))
            && let Ok(journal) = OneReplicaJournal::from_json(&bytes)
            && journal.blocked_owner.is_some_and(|owner| owner.pid == pid)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "one-replica child did not publish blocked owner"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn assert_removed(root: &Path) {
    let journal =
        OneReplicaJournal::from_json(&std::fs::read(root.join("ownership.json")).unwrap()).unwrap();
    assert_eq!(
        journal.state,
        kuberic_mssql_tests::three_replica::RunState::Removed
    );
    assert!(journal.blocked_owner.is_none());
    assert!(journal.resources.iter().all(
        |resource| resource.state == kuberic_mssql_tests::three_replica::ResourceState::Removed
    ));
}

fn assert_no_owned_docker_resources() {
    for arguments in [
        [
            "ps",
            "-a",
            "--filter",
            "label=io.kuberic.mssql.fixture=one-replica",
            "--format",
            "{{.ID}}",
        ]
        .as_slice(),
        [
            "network",
            "ls",
            "--filter",
            "label=io.kuberic.mssql.fixture=one-replica",
            "--format",
            "{{.ID}}",
        ]
        .as_slice(),
    ] {
        let output = Command::new("docker").args(arguments).output().unwrap();
        assert!(output.status.success());
        assert!(output.stdout.iter().all(u8::is_ascii_whitespace));
    }
}
