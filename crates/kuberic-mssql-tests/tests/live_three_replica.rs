//! Licensed, explicitly acknowledged three-member SQL Server/Kuberic lifecycle.

use std::error::Error;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};

use futures::FutureExt;
use kuberic_mssql_tests::three_replica::{
    CombinedFixtureError, FixtureConfig, HandledCancellationSignal, JournalStore, MssqlGroup,
    ResourceState, RunState, cleanup_three_replica_fixture, launch_three_members,
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

async fn run_three_replica_mssql_happy_path() {
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
            signal = cancellation_signal() => ExecutionOutcome::Interrupted(signal),
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

        let group =
            MssqlGroup::from_live(root, &launched.run, &native_binding, &launched.members).await?;
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
        let shutdown = group.shutdown().await;
        if let Err(error) = shutdown {
            return Err::<(), TestError>(error.into());
        }
        match runtime_result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
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

async fn cancellation_signal() -> HandledCancellationSignal {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install SIGINT cleanup handler");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM cleanup handler");
    tokio::select! {
        _ = interrupt.recv() => HandledCancellationSignal::Interrupt,
        _ = terminate.recv() => HandledCancellationSignal::Terminate,
    }
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
