//! Explicitly provisioned lease/fence experiments; never production authority.

use std::{
    error::Error,
    panic::AssertUnwindSafe,
    path::Path,
    time::{Duration, Instant},
};

use futures::FutureExt;
use kuberic_mssql::OpaqueId;
use kuberic_mssql_tests::three_replica::{
    AdminError, CancellationSignals, FixtureConfig, LaunchedMembers, LeaseObservation,
    launch_three_members,
};

type TestError = Box<dyn Error + Send + Sync>;
const CREATION_SECONDS: u32 = 60;
const RENEWAL_SECONDS: u32 = 120;
const MARGIN: Duration = Duration::from_secs(3);

#[test]
#[ignore = "requires explicitly provisioned licensed Rust-owned SQL Server containers"]
fn mssql_write_lease_and_exact_fence_experiments() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let root = std::env::var_os("KUBERIC_MSSQL_LEASE_FENCE_ROOT")
                        .expect("KUBERIC_MSSQL_LEASE_FENCE_ROOT must be explicitly configured");
                    std::fs::create_dir_all(&root)?;
                    let mut signals =
                        CancellationSignals::register().expect("SIGINT/SIGTERM handlers");
                    run_scenario(&Path::new(&root).join("expiry"), &mut signals, false).await?;
                    run_scenario(&Path::new(&root).join("active-restart"), &mut signals, true).await
                })
                .expect("licensed lease/fence experiments");
        })
        .unwrap()
        .join()
        .expect("lease/fence test thread");
}

async fn run_scenario(
    root: &Path,
    signals: &mut CancellationSignals,
    restart: bool,
) -> Result<(), TestError> {
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut fixture = tokio::select! {
        result = launch_three_members(config) => result?,
        signal = signals.recv() => return Err(format!("lease fixture launch cancelled: {signal:?}").into()),
    };
    let deadline = tokio::time::Instant::from_std(fixture.complete_deadline());
    let outcome = AssertUnwindSafe(async {
        tokio::select! {
            result = experiment(&mut fixture, restart) => result,
            signal = signals.recv() => Err(format!("lease experiment cancelled: {signal:?}").into()),
            _ = tokio::time::sleep_until(deadline) => Err("lease experiment total deadline exceeded".into()),
        }
    }).catch_unwind().await;
    let cleanup = fixture.cleanup();
    match (outcome, cleanup) {
        (Ok(Ok(())), Ok(_)) => Ok(()),
        (Ok(Err(error)), Ok(_)) => Err(error),
        (Ok(result), Err(cleanup)) => {
            Err(format!("lease experiment result: {result:?}; cleanup failed: {cleanup}").into())
        }
        (Err(panic), Ok(_)) => std::panic::resume_unwind(panic),
        (Err(_), Err(cleanup)) => {
            Err(format!("lease experiment panicked; cleanup failed: {cleanup}").into())
        }
    }
}

async fn experiment(fixture: &mut LaunchedMembers, restart: bool) -> Result<(), TestError> {
    let (group, creation_ack) = fixture
        .provision_write_lease_group_for_test(CREATION_SECONDS)
        .await?;
    let index = usize::from(group.evidence.primary_ordinal - 1);
    let baseline = fixture.observe_write_lease_for_test(index).await?;
    if baseline.role != "PRIMARY"
        || !baseline.remaining_ticks.is_some_and(|value| value > 0)
        || baseline.connected_replicas != 3
        || baseline.synchronized_databases != 3
    {
        return Err(
            format!("lease baseline is not a live three-replica primary: {baseline:?}").into(),
        );
    }
    fixture.probe_lease_write_for_test(index).await?;
    let submitted = Instant::now();
    fixture
        .renew_write_lease_for_test(index, RENEWAL_SECONDS)
        .await?;
    let acknowledged = Instant::now();
    eprintln!(
        "lease baseline={baseline:?}; creation_seconds={CREATION_SECONDS}; renewal_seconds={RENEWAL_SECONDS}"
    );

    let renewed_earliest = submitted + Duration::from_secs(u64::from(RENEWAL_SECONDS)) - MARGIN;
    if restart {
        return active_restart_and_fence(fixture, index, &baseline, renewed_earliest).await;
    }

    // Acknowledgement is later than native application; this is an upper,
    // not a submission-based lower, bound on the original expiry.
    let original_latest = creation_ack + Duration::from_secs(u64::from(CREATION_SECONDS)) + MARGIN;
    if original_latest >= renewed_earliest {
        return Err("no defensible window for proving native renewal extension".into());
    }
    tokio::time::sleep_until(tokio::time::Instant::from_std(original_latest)).await;
    fixture.probe_lease_write_for_test(index).await?;
    if Instant::now() >= renewed_earliest {
        return Err("renewal proof write missed the conservative renewed window".into());
    }
    eprintln!("lease renewal proved by direct write beyond original latest expiry");

    let expiry_deadline =
        acknowledged + Duration::from_secs(u64::from(RENEWAL_SECONDS)) + Duration::from_secs(30);
    loop {
        let observed = fixture.observe_write_lease_for_test(index).await?;
        if observed.group_id != baseline.group_id
            || observed.replica_id != baseline.replica_id
            || observed.sql_start != baseline.sql_start
        {
            return Err("native identity changed during lease-expiry experiment".into());
        }
        let write = fixture.probe_lease_write_for_test(index).await;
        eprintln!("lease expiry observation: native={observed:?}; direct_write={write:?}");
        lease_window_remaining(expiry_deadline, Instant::now())?;
        if observed.remaining_ticks == Some(0)
            && observed.connected_replicas == baseline.connected_replicas
            && observed.synchronized_databases == baseline.synchronized_databases
            && matches!(write, Err(AdminError::Sql(988)))
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn fresh_start(
    fixture: &LaunchedMembers,
    index: usize,
    old: &str,
    deadline: Instant,
) -> Result<LeaseObservation, TestError> {
    loop {
        lease_window_remaining(deadline, Instant::now())?;
        match fixture.observe_write_lease_for_test(index).await {
            Ok(observed) if observed.sql_start != old => return Ok(observed),
            result if Instant::now() >= deadline => {
                return Err(format!("fresh SQL incarnation not established: {result:?}").into());
            }
            result => eprintln!("waiting for fresh SQL start: {result:?}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn active_restart_and_fence(
    fixture: &mut LaunchedMembers,
    index: usize,
    baseline: &LeaseObservation,
    renewed_earliest: Instant,
) -> Result<(), TestError> {
    let cancelled = fixture
        .cancel_write_lease_request_for_test(index, RENEWAL_SECONDS)
        .await;
    if cancelled != Err(AdminError::QueryDeadline) {
        return Err(format!("expected cancelled response deadline, got {cancelled:?}").into());
    }
    // This does not assume that dropping the TDS session rolled back renewal.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let before_restart = fixture.observe_write_lease_for_test(index).await?;
    if !before_restart
        .remaining_ticks
        .is_some_and(|value| value > 0)
    {
        return Err("active-lease restart did not establish a live native lease".into());
    }
    fixture.probe_lease_write_for_test(index).await?;
    eprintln!("cancelled renewal outcome remains unknown; fresh native={before_restart:?}");

    let old = fixture
        .capture_fence_request_for_test(index, OpaqueId::new("operation", "before-restart")?)
        .await?;
    let container_id = fixture.members[index].container_id.clone();
    if Instant::now() >= renewed_earliest {
        return Err("active-lease restart missed the acknowledged lease window".into());
    }
    fixture.restart_sql_process_for_test(index)?;
    let fresh = fresh_start(fixture, index, &baseline.sql_start, renewed_earliest).await?;
    if fixture.members[index].container_id != container_id || fresh.group_id != baseline.group_id {
        return Err("active-lease restart replaced container or AG identity".into());
    }
    let write = fixture.probe_lease_write_for_test(index).await;
    let after_write = fixture.observe_write_lease_for_test(index).await?;
    let remaining = lease_window_remaining(renewed_earliest, Instant::now())?;
    if after_write.sql_start != fresh.sql_start {
        return Err("SQL process changed while probing post-restart writes".into());
    }
    eprintln!(
        "active-lease restart: before_write={fresh:?}; direct_write={write:?}; after_write={after_write:?}; old_lease_window_remaining={remaining:?}"
    );
    if !matches!(
        write,
        Ok(()) | Err(AdminError::Sql(_) | AdminError::WriteUnconfirmed | AdminError::QueryDeadline)
    ) {
        return Err(
            format!("active-lease restart write outcome is inconclusive: {write:?}").into(),
        );
    }

    if fixture.fence_owned_incarnation_for_test(&old).await.is_ok() {
        return Err("stale fence request stopped a replacement".into());
    }
    let still_running = fixture.observe_write_lease_for_test(index).await?;
    if still_running.sql_start != fresh.sql_start {
        return Err("stale request disturbed the replacement SQL process".into());
    }
    let current = fixture
        .capture_fence_request_for_test(index, OpaqueId::new("operation", "exact-stop")?)
        .await?;
    let receipt = fixture.fence_owned_incarnation_for_test(&current).await?;
    receipt.validate(current.request())?;
    fixture.restart_sql_process_for_test(index)?;
    let replacement = fresh_start(
        fixture,
        index,
        &fresh.sql_start,
        Instant::now() + Duration::from_secs(120),
    )
    .await?;
    let next = fixture
        .capture_fence_request_for_test(index, OpaqueId::new("operation", "exact-stop")?)
        .await?;
    if receipt.validate(next.request()).is_ok() {
        return Err("old exact-stop receipt transferred to restarted SQL".into());
    }
    eprintln!(
        "verified exact fixture fence and nontransferable receipt; replacement={replacement:?}"
    );
    Ok(())
}

fn lease_window_remaining(
    deadline: Instant,
    observed_at: Instant,
) -> Result<Duration, &'static str> {
    deadline
        .checked_duration_since(observed_at)
        .filter(|remaining| !remaining.is_zero())
        .ok_or("native lease evidence missed its conservative observation window")
}

#[test]
fn lease_window_rejects_deadline_and_late_observation() {
    let origin = Instant::now();
    let deadline = origin + Duration::from_secs(120);
    assert_eq!(
        lease_window_remaining(deadline, origin),
        Ok(Duration::from_secs(120))
    );
    assert!(lease_window_remaining(deadline, deadline).is_err());
    assert!(lease_window_remaining(deadline, deadline + Duration::from_secs(1)).is_err());
}
