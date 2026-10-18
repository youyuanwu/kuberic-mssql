use super::*;
use kuberic_mssql_tests::three_replica::hosted_native_prefix::NativePrefixDecision;

#[test]
#[ignore = "requires explicit licensed execution authorization and a qualified owned three-replica Docker fixture"]
fn three_replica_mssql_live_hosted_native_witness() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("live hosted witness runtime")
                .block_on(async {
                    let root = required_path("KUBERIC_MSSQL_THREE_REPLICA_ROOT");
                    let mut cancellation =
                        CancellationSignals::register().expect("cancellation signals");
                    execute(&root, &mut cancellation)
                        .await
                        .expect("live hosted witness and owned cleanup");
                });
        })
        .expect("live hosted witness thread")
        .join()
        .expect("live hosted witness thread panicked");
}

async fn execute(root: &Path, cancellation: &mut CancellationSignals) -> Result<(), TestError> {
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let provision = {
        let work = AssertUnwindSafe(async {
            launched.provision_availability_group().await?;
            Ok::<(), TestError>(())
        })
        .catch_unwind();
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => RuntimeOutcome::Completed(result),
            signal = cancellation.recv() => RuntimeOutcome::Interrupted(signal),
        }
    };
    if !matches!(&provision, RuntimeOutcome::Completed(Ok(Ok(())))) {
        let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
        return finish_runtime_outcome(root, launched, provision, &cleanup);
    }
    let Some(native_binding) = launched.journal().native_binding.clone() else {
        let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
        return finish_runtime_outcome(
            root,
            launched,
            RuntimeOutcome::Completed(Ok(Err("native binding missing".into()))),
            &cleanup,
        );
    };
    let startup_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let startup = {
        let work = AssertUnwindSafe(tokio::time::timeout_at(
            tokio::time::Instant::from_std(launched.complete_deadline()),
            PublicMssqlGroup::from_live_with_coordinator(
                root,
                &launched.run,
                &native_binding,
                &launched.members,
                launched.kuberic_convergence_timeout(),
                launched.complete_deadline(),
                &startup_cleanup,
            ),
        ))
        .catch_unwind();
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => Some(result),
            _signal = cancellation.recv() => None,
        }
    };
    let mut group = match startup {
        Some(Ok(Ok(Ok(group)))) => group,
        Some(Ok(Ok(Err(error)))) if !error.agent_termination_unconfirmed() => {
            return finish_runtime_outcome(
                root,
                launched,
                RuntimeOutcome::Completed(Ok(Err(Box::new(error)))),
                &startup_cleanup,
            );
        }
        other => {
            let diagnostic = match other {
                Some(Ok(Ok(Err(error)))) => format!("live hosted startup failed: {error}"),
                Some(Ok(Err(error))) => format!("live hosted startup deadline: {error}"),
                Some(Err(panic)) => format!("live hosted startup panic: {}", panic_message(&panic)),
                None => "live hosted startup interrupted".into(),
                Some(Ok(Ok(Ok(_)))) => unreachable!(),
            };
            return block_disposal(&mut launched, &diagnostic);
        }
    };
    let mut serialized = None;
    let outcome = {
        let work = AssertUnwindSafe(async {
            let report = group.collect_live_native_witness(&launched, 2).await?;
            let bytes = serde_json::to_vec(&report)?;
            if !matches!(
                report.association.decision,
                NativePrefixDecision::NativeSampledCoverage { .. }
            ) {
                return Err::<(), TestError>(
                    format!(
                        "live witness did not prove sampled native coverage: {}",
                        String::from_utf8(bytes)?
                    )
                    .into(),
                );
            }
            serialized = Some(bytes);
            Ok(())
        })
        .catch_unwind();
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => RuntimeOutcome::Completed(result),
            signal = cancellation.recv() => RuntimeOutcome::Interrupted(signal),
        }
    };
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let shutdown = AssertUnwindSafe(group.shutdown_with_coordinator(&cleanup))
        .catch_unwind()
        .await;
    if !matches!(&shutdown, Ok(Ok(()))) {
        let diagnostic = match shutdown {
            Ok(Err(error)) => error.to_string(),
            Err(panic) => format!("panic: {}", panic_message(&panic)),
            Ok(Ok(())) => unreachable!(),
        };
        return block_disposal(
            &mut launched,
            &format!(
                "{}; live hosted shutdown failed: {diagnostic}",
                outcome.description(),
            ),
        );
    }
    finish_native_commit_report(
        || finish_runtime_outcome(root, launched, outcome, &cleanup),
        serialized,
        |bytes| {
            let mut output = std::io::stdout().lock();
            output.write_all(bytes)?;
            writeln!(output)?;
            output.flush()?;
            Ok(())
        },
    )
}

pub(super) fn block_disposal(
    launched: &mut LaunchedMembers,
    primary: &str,
) -> Result<(), TestError> {
    launched.block_cleanup().map_err(|error| -> TestError {
        format!("{primary}; host termination unconfirmed; journal blocking also failed: {error}")
            .into()
    })?;
    Err(format!("{primary}; host termination unconfirmed; fixture disposal blocked").into())
}
