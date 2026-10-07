//! Licensed, explicitly acknowledged three-member SQL Server/Kuberic lifecycle.

use std::error::Error;
use std::fs;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use futures::FutureExt;
use kuberic_mssql_tests::three_replica::{
    CancellationSignals, CombinedFixtureError, FixtureConfig, HandledCancellationSignal,
    JournalStore, MssqlGroup, MssqlGroupError, OwnershipJournal, ResourceState, RunState,
    cleanup_three_replica_fixture, launch_three_members,
};
use kuberic_runtime::control::proto;

type TestError = Box<dyn Error + Send + Sync>;

#[test]
#[ignore = "requires explicit SQL Server EULA acknowledgement and a qualified local Docker host"]
fn three_replica_mssql_happy_path() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("three-replica Tokio runtime")
                .block_on(run_three_replica_mssql_happy_path());
        })
        .expect("three-replica test thread")
        .join()
        .expect("three-replica test thread panicked");
}

#[test]
fn primary_and_shutdown_errors_are_preserved() {
    let primary: TestError = "report failed".into();
    let shutdown = std::io::Error::other("shutdown failed");
    let error = combine_primary_and_shutdown(Err(primary), Err(shutdown)).unwrap_err();
    assert_eq!(
        error.to_string(),
        "report failed; shutdown also failed: shutdown failed"
    );
}

#[test]
#[ignore = "requires explicit SQL Server EULA acknowledgement and a qualified local Docker host"]
fn three_replica_sigterm_during_owned_launch_is_recoverable() {
    let root = required_path("KUBERIC_MSSQL_THREE_REPLICA_ROOT");
    let acknowledgement = required_path("KUBERIC_MSSQL_EULA_ACKNOWLEDGEMENT");
    cleanup_three_replica_fixture(&root).expect("clean baseline before SIGTERM regression");

    let ready = root
        .parent()
        .expect("fixture root has a parent")
        .join(format!("signal-handlers-ready-{}", std::process::id()));
    if ready.exists() {
        fs::remove_file(&ready).expect("remove stale signal-ready evidence");
    }

    let mut interrupted = spawn_live_child(&root, &acknowledgement, Some(&ready));
    wait_until(Duration::from_secs(30), || ready.is_file())
        .expect("subprocess did not publish handler-ready evidence");
    wait_until(Duration::from_secs(180), || {
        load_journal_if_present(&root).is_some_and(|journal| {
            journal.resources.iter().any(|resource| {
                matches!(
                    resource.state,
                    ResourceState::Dispatched | ResourceState::Bound | ResourceState::Cleaning
                )
            })
        })
    })
    .expect("subprocess never entered a journaled owned-resource phase");

    let signal_result = unsafe { libc::kill(interrupted.id() as i32, libc::SIGTERM) };
    assert_eq!(signal_result, 0, "send SIGTERM to live subprocess");
    let status = wait_for_child(&mut interrupted, Duration::from_secs(240));
    assert!(
        !status.success(),
        "handled SIGTERM must make the interrupted validation fail explicitly"
    );

    let journal = load_journal_if_present(&root).expect("recoverable ownership journal");
    assert!(
        journal.state == RunState::Removed || !journal.resources.is_empty(),
        "SIGTERM must leave explicit cleanup or recoverable ownership evidence"
    );
    cleanup_three_replica_fixture(&root).expect("recover exact state after SIGTERM");
    assert_removed(&root);

    let mut retry = spawn_live_child(&root, &acknowledgement, None);
    let retry_status = wait_for_child(&mut retry, Duration::from_secs(1200));
    assert!(retry_status.success(), "retry after SIGTERM must succeed");
    cleanup_three_replica_fixture(&root).expect("idempotent cleanup after retry");
    assert_removed(&root);
    fs::remove_file(&ready).expect("remove signal-ready evidence");
}

async fn run_three_replica_mssql_happy_path() {
    let mut cancellation =
        CancellationSignals::register().expect("install SIGINT/SIGTERM cleanup handlers");
    write_signal_handler_ready_evidence();
    let root = PathBuf::from(
        std::env::var_os("KUBERIC_MSSQL_THREE_REPLICA_ROOT")
            .expect("KUBERIC_MSSQL_THREE_REPLICA_ROOT must be explicitly configured"),
    );
    let acknowledgement = PathBuf::from(
        std::env::var_os("KUBERIC_MSSQL_EULA_ACKNOWLEDGEMENT")
            .expect("KUBERIC_MSSQL_EULA_ACKNOWLEDGEMENT must be explicitly configured"),
    );
    let config = FixtureConfig::new(&root, acknowledgement).expect("validated fixture config");
    let execution = AssertUnwindSafe(execute_live_lifecycle(&root, config)).catch_unwind();
    let outcome = {
        tokio::pin!(execution);
        tokio::select! {
            outcome = &mut execution => ExecutionOutcome::Completed(outcome),
            signal = cancellation.recv() => ExecutionOutcome::Interrupted(signal),
        }
    };
    match outcome {
        ExecutionOutcome::Completed(Ok(Ok(()))) => {}
        ExecutionOutcome::Completed(Ok(Err(error))) => {
            panic!("three-replica lifecycle failed: {error}")
        }
        ExecutionOutcome::Completed(Err(panic)) => {
            cleanup_three_replica_fixture(&root).expect("exact cleanup after live lifecycle panic");
            std::panic::resume_unwind(panic);
        }
        ExecutionOutcome::Interrupted(signal) => {
            cleanup_three_replica_fixture(&root)
                .expect("exact cleanup after handled live lifecycle interruption");
            panic!("three-replica lifecycle interrupted by {signal:?}");
        }
    }
}

enum ExecutionOutcome {
    Completed(Result<Result<(), TestError>, Box<dyn std::any::Any + Send + 'static>>),
    Interrupted(HandledCancellationSignal),
}

async fn execute_live_lifecycle(root: &Path, config: FixtureConfig) -> Result<(), TestError> {
    let mut launched = launch_three_members(config)
        .await
        .map_err(|error| -> TestError { Box::new(error) })?;

    let lifecycle = AssertUnwindSafe(async {
        let topology = launched.provision_availability_group().await?;
        let native_binding = launched
            .journal()
            .native_binding
            .clone()
            .ok_or("exact native topology binding was not journaled")?;
        if native_binding != topology.binding {
            return Err::<(), TestError>("journaled native binding changed".into());
        }

        let group = MssqlGroup::from_live(
            root,
            &launched.run,
            &native_binding,
            &launched.members,
            launched.kuberic_convergence_timeout(),
            launched.complete_deadline(),
        )
        .await?;
        let runtime_result = AssertUnwindSafe(async {
            let reports = group.reports_bracketed().await?;
            assert_exact_reports(&group, &reports, native_binding.configuration_sequence)?;

            let marker = launched
                .commit_replicated_marker(&topology.evidence)
                .await?;
            if marker.primary_ordinal != topology.evidence.primary_ordinal
                || marker.readable_ordinals != [1, 2, 3]
            {
                return Err::<(), TestError>(
                    "post-runtime marker was not readable from all exact members".into(),
                );
            }
            Ok(())
        })
        .catch_unwind()
        .await;
        finish_runtime(runtime_result, group.shutdown().await)
    })
    .catch_unwind();
    let outcome = lifecycle.await;

    let cleanup = launched.cleanup();
    match (outcome, cleanup) {
        (Ok(Ok(())), Ok(cleanup)) => {
            assert_eq!(cleanup.removed_container_ids.len(), 3);
            assert!(!cleanup.removed_network_id.is_empty());
            assert_removed(root);
            Ok(())
        }
        (Ok(Err(error)), cleanup) => {
            if let Err(cleanup) = cleanup {
                Err(combined_error(error, cleanup))
            } else {
                Err(error)
            }
        }
        (Err(panic), cleanup) => {
            if let Err(cleanup) = cleanup {
                panic!("three-replica lifecycle panicked; cleanup also failed: {cleanup}");
            }
            std::panic::resume_unwind(panic);
        }
        (Ok(Ok(())), Err(error)) => Err(Box::new(error)),
    }
}

fn combined_error(primary: TestError, cleanup: CombinedFixtureError) -> TestError {
    format!("{primary}; cleanup also failed: {cleanup}").into()
}

fn finish_runtime(
    runtime_result: Result<Result<(), TestError>, Box<dyn std::any::Any + Send + 'static>>,
    shutdown: Result<(), MssqlGroupError>,
) -> Result<(), TestError> {
    match (runtime_result, shutdown) {
        (Ok(primary), shutdown) => combine_primary_and_shutdown(primary, shutdown),
        (Err(panic), Ok(())) => std::panic::resume_unwind(panic),
        (Err(panic), Err(shutdown)) => {
            let primary = panic_message(&panic);
            panic!("three-replica runtime panicked: {primary}; shutdown also failed: {shutdown}");
        }
    }
}

fn combine_primary_and_shutdown<E>(
    primary: Result<(), TestError>,
    shutdown: Result<(), E>,
) -> Result<(), TestError>
where
    E: Error + Send + Sync + 'static,
{
    match (primary, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(shutdown)) => Err(Box::new(shutdown)),
        (Err(primary), Err(shutdown)) => {
            Err(format!("{primary}; shutdown also failed: {shutdown}").into())
        }
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send + 'static>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            panic
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

fn write_signal_handler_ready_evidence() {
    let Some(path) = std::env::var_os("KUBERIC_MSSQL_SIGNAL_READY_FILE") else {
        return;
    };
    fs::write(PathBuf::from(path), b"ready\n")
        .expect("write SIGINT/SIGTERM handler-ready evidence");
}

fn required_path(variable: &str) -> PathBuf {
    PathBuf::from(
        std::env::var_os(variable)
            .unwrap_or_else(|| panic!("{variable} must be explicitly configured")),
    )
}

fn spawn_live_child(root: &Path, acknowledgement: &Path, ready: Option<&Path>) -> Child {
    let mut command = Command::new(std::env::current_exe().expect("current live test executable"));
    command
        .arg("three_replica_mssql_happy_path")
        .arg("--ignored")
        .arg("--exact")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("KUBERIC_MSSQL_THREE_REPLICA_ROOT", root)
        .env("KUBERIC_MSSQL_EULA_ACKNOWLEDGEMENT", acknowledgement)
        .env_remove("KUBERIC_MSSQL_SIGNAL_READY_FILE")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if let Some(ready) = ready {
        command.env("KUBERIC_MSSQL_SIGNAL_READY_FILE", ready);
    }
    command.spawn().expect("spawn exact live child")
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> Result<(), ()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(())
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll live child") {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill timed-out live child");
            let _ = child.wait();
            panic!("live child exceeded {timeout:?}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn load_journal_if_present(root: &Path) -> Option<OwnershipJournal> {
    let bytes = fs::read(root.join("ownership.json")).ok()?;
    OwnershipJournal::from_json(&bytes).ok()
}

fn assert_exact_reports(
    group: &MssqlGroup,
    reports: &[proto::AgentStatusReport; 3],
    configuration_sequence: i64,
) -> Result<(), TestError> {
    let configuration_id = group.configuration.configuration_id.as_str();
    for (pod, report) in group.pods.iter().zip(reports) {
        if report.resource_uid != group.resource_uid.as_str()
            || report.process_session_id != pod.session.as_str()
            || report.replica_id != pod.identity.replica_id.value()
            || report.previous_configuration.is_some()
            || report
                .current_configuration
                .as_ref()
                .map(|configuration| configuration.configuration_id.as_str())
                != Some(configuration_id)
            || report.current_progress != configuration_sequence
            || report.catch_up_capability != Some(configuration_sequence)
            || !report.healthy
            || report.write_status == proto::AccessStatus::Granted as i32
        {
            return Err("Kuberic report differs from the exact fenced topology".into());
        }
    }
    if reports
        .iter()
        .filter(|report| report.role == proto::ReplicaRole::Primary as i32)
        .count()
        != 1
        || reports
            .iter()
            .filter(|report| report.role == proto::ReplicaRole::ActiveSecondary as i32)
            .count()
            != 2
    {
        return Err("Kuberic reports do not expose one primary and two active secondaries".into());
    }
    Ok(())
}

fn assert_removed(root: &Path) {
    let store = JournalStore::initialize(root).expect("cleanup journal root");
    let journal = store
        .load()
        .expect("cleanup journal")
        .expect("cleanup journal exists");
    assert_eq!(journal.state, RunState::Removed);
    assert!(
        journal
            .resources
            .iter()
            .all(|resource| resource.state == ResourceState::Removed)
    );
    assert!(journal.run.members.iter().all(|member| {
        !member.data_directory.exists()
            && !root
                .join("kuberic-runtime")
                .join(format!("member-{}", member.ordinal))
                .exists()
    }));
}
