//! Licensed three-member SQL Server/Kuberic test lifecycle.

use std::error::Error;
use std::fs;
use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use futures::FutureExt;
use kuberic_mssql::runtime_host::SeedDispatchPhase;
use kuberic_mssql_tests::three_replica::{
    CLEANUP_BUDGET, CancellationSignals, CleanupClock, CleanupCoordinator, CombinedFixtureError,
    FixtureConfig, HandledCancellationSignal, JournalStore, LaunchedMembers,
    LicensedConflictScenario, MssqlGroupError, NativeCleanupEvidence, NativeTopologyBinding,
    NativeTopologyPhase, OwnershipJournal, PublicBuildCommand, PublicFaultInjection,
    PublicMssqlGroup, PublicMutationFault, ReadyMember, ResourceState, RunState,
    SystemCleanupClock, TopologyRun, cleanup_three_replica_fixture, launch_three_members,
};
use kuberic_runtime::control::{normalize_public_v1_agent_status_report, proto};
use kuberic_runtime::protocol::public_operations::{
    PublicProgressRole, PublicV1CallKind, PublicV1OperationResult,
};
use kuberic_runtime::protocol::types::{AccessStatus, ReplicaRole, ReplicaStorageIdentity};

type TestError = Box<dyn Error + Send + Sync>;
const LIFECYCLE_CHILD_TEST: &str = "three_replica_lifecycle_child";
const CHILD_MODE_ENV: &str = "KUBERIC_MSSQL_THREE_REPLICA_CHILD_MODE";
const FAULT_HIT_ENV: &str = "KUBERIC_MSSQL_FAULT_HIT_FILE";

#[path = "live_three_replica/hosted_witness.rs"]
mod hosted_witness;

struct PendingSeedCompaction {
    command: PublicBuildCommand,
    release: tokio::sync::oneshot::Sender<()>,
    callback: tokio::task::JoinHandle<Result<(), tonic::Status>>,
}

#[test]
fn three_replica_lifecycle_child() {
    let mode = match std::env::var(CHILD_MODE_ENV) {
        Ok(mode) => mode,
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => panic!("invalid live SQL child mode"),
    };
    match mode.as_str() {
        "happy-path" => run_three_replica_test(false),
        "same-root-restart" => run_three_replica_test(true),
        "native-commit" => run_native_experiment(NativeExperiment::CommitRecord),
        _ => panic!("invalid live SQL child mode"),
    }
}

#[test]
fn lifecycle_child_rejects_invalid_mode_before_fixture_admission() {
    let output = test_child_command(
        Path::new("/unused-live-sql-child-root"),
        "invalid",
        None,
        None,
    )
    .env_remove("KUBERIC_MSSQL_THREE_REPLICA_ROOT")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .output()
    .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid live SQL child mode"));
}

macro_rules! phase4_case {
    ($name:ident, $case:ident, $suffix:literal $(, $argument:expr)* $(,)?) => {
        #[test]
        #[ignore = "exercises a licensed SQL scenario on a qualified local Docker host"]
        fn $name() {
            run_phase4_test(|root| async move {
                let root = root.join($suffix);
                $case(&root $(, $argument)*).await
            });
        }
    };
}

mod three_replica_mssql_bootstrap_fault_boundaries {
    use super::*;

    phase4_case!(
        source_recovery,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceRecoveryCompletion,
        "source-recovery"
    );
    phase4_case!(
        source_database_creation_intent,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceDatabaseCreationIntentCompletion,
        "source-database-creation-intent"
    );
    phase4_case!(
        source_database_completion,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceDatabaseCompletion,
        "source-database-completion"
    );
    phase4_case!(
        source_database_creation_intent_removal,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceDatabaseCreationIntentRemovalCompletion,
        "source-database-creation-intent-removal"
    );
    phase4_case!(
        source_ownership,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceOwnershipCompletion,
        "source-ownership"
    );
    phase4_case!(
        source_marker_table,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceMarkerTableCompletion,
        "source-marker-table"
    );
    phase4_case!(
        source_full_backup,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceFullBackupCompletion,
        "source-full-backup"
    );
    phase4_case!(
        source_log_backup,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceLogBackupCompletion,
        "source-log-backup"
    );
    phase4_case!(
        source_backup_verification,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SourceBackupVerificationCompletion,
        "source-backup-verification"
    );
    phase4_case!(
        availability_group_ownership_intent,
        exercise_configuration_fault,
        "",
        PublicMutationFault::AvailabilityGroupIntentCompletion,
        "availability-group-ownership-intent"
    );
    phase4_case!(
        availability_group_creation_intent,
        exercise_configuration_fault,
        "",
        PublicMutationFault::AvailabilityGroupCreationIntentCompletion,
        "availability-group-creation-intent"
    );
    phase4_case!(
        availability_group_creation,
        exercise_configuration_fault,
        "",
        PublicMutationFault::AvailabilityGroupCompletion,
        "availability-group-creation"
    );
    phase4_case!(
        availability_group_binding,
        exercise_configuration_fault,
        "",
        PublicMutationFault::AvailabilityGroupBindingCompletion,
        "availability-group-binding"
    );
    phase4_case!(
        availability_group_creation_intent_removal,
        exercise_configuration_fault,
        "",
        PublicMutationFault::AvailabilityGroupCreationIntentRemovalCompletion,
        "availability-group-creation-intent-removal"
    );
    phase4_case!(
        primary_seeding_grant,
        exercise_configuration_fault,
        "",
        PublicMutationFault::PrimarySeedingGrantCompletion,
        "primary-seeding-grant"
    );
    phase4_case!(
        primary_seeding_grant_evidence,
        exercise_configuration_fault,
        "",
        PublicMutationFault::PrimarySeedingGrantEvidenceCompletion,
        "primary-seeding-grant-evidence"
    );
    phase4_case!(
        first_secondary_join,
        exercise_configuration_fault,
        "",
        PublicMutationFault::FirstSecondaryJoinCompletion,
        "first-secondary-join"
    );
    phase4_case!(
        first_secondary_seeding_grant,
        exercise_configuration_fault,
        "",
        PublicMutationFault::FirstSecondarySeedingGrantCompletion,
        "first-secondary-seeding-grant"
    );
    phase4_case!(
        first_secondary_seeding_grant_evidence,
        exercise_configuration_fault,
        "",
        PublicMutationFault::FirstSecondarySeedingGrantEvidenceCompletion,
        "first-secondary-seeding-grant-evidence"
    );
    phase4_case!(
        second_secondary_join,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SecondSecondaryJoinCompletion,
        "second-secondary-join"
    );
    phase4_case!(
        second_secondary_seeding_grant,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SecondSecondarySeedingGrantCompletion,
        "second-secondary-seeding-grant"
    );
    phase4_case!(
        second_secondary_seeding_grant_evidence,
        exercise_configuration_fault,
        "",
        PublicMutationFault::SecondSecondarySeedingGrantEvidenceCompletion,
        "second-secondary-seeding-grant-evidence"
    );
    phase4_case!(seed_boundaries, exercise_seed_faults, "seed-boundaries");
    phase4_case!(
        post_native_success,
        exercise_post_native_success_replay,
        "post-native-success"
    );
}

mod three_replica_mssql_seed_dispatch_handshake_faults {
    use super::*;

    phase4_case!(
        before_request_submission,
        exercise_seed_submission_fault,
        "seed-before-request-submission",
        PublicMutationFault::SeedBeforeRequestSubmission,
        "seed-before-request-submission"
    );
    phase4_case!(
        after_request_submission,
        exercise_seed_submission_fault,
        "seed-after-request-submission",
        PublicMutationFault::SeedAfterRequestSubmission,
        "seed-after-request-submission"
    );
    phase4_case!(
        accepted_reply_lost,
        exercise_seed_submission_fault,
        "seed-accepted-reply-lost",
        PublicMutationFault::SeedAcceptedReplyLost,
        "seed-accepted-reply-lost"
    );
    phase4_case!(
        definite_sql_rejection,
        exercise_seed_submission_fault,
        "seed-definite-sql-rejection",
        PublicMutationFault::SeedDefiniteSqlRejection,
        "seed-definite-sql-rejection"
    );
}

mod three_replica_mssql_conflict_evidence {
    use super::*;

    phase4_case!(
        native_source_recovery_partial,
        exercise_native_source_recovery_partial,
        "native-source-recovery-partial",
        "native-source-recovery-partial"
    );
    phase4_case!(
        native_unowned_source_database,
        exercise_native_sql_conflict,
        "native-unowned-source-database",
        "native-unowned-source-database",
        false
    );
    phase4_case!(
        native_unowned_availability_group,
        exercise_native_sql_conflict,
        "native-unowned-availability-group",
        "native-unowned-availability-group",
        true
    );
    phase4_case!(
        availability_group_identity_profile,
        exercise_conflict_scenario,
        "00-seam-availability-group-identity-profile",
        LicensedConflictScenario::AvailabilityGroupIdentityOrProfile,
        "seam-availability-group-identity-profile",
        "conflicts with the requested bootstrap topology"
    );
    phase4_case!(
        logical_replica_native_identity,
        exercise_conflict_scenario,
        "01-seam-logical-replica-native-identity",
        LicensedConflictScenario::LogicalReplicaNativeMismatch,
        "seam-logical-replica-native-identity",
        "conflicts with the requested bootstrap topology"
    );
    phase4_case!(
        endpoint_member_profile,
        exercise_conflict_scenario,
        "02-seam-endpoint-member-profile",
        LicensedConflictScenario::EndpointOrMemberProfile,
        "seam-endpoint-member-profile",
        "conflicts with the requested bootstrap topology"
    );
    phase4_case!(
        standalone_database_identity_recovery,
        exercise_conflict_scenario,
        "03-seam-standalone-database-identity-recovery",
        LicensedConflictScenario::StandaloneDatabaseIdentityOrRecovery,
        "seam-standalone-database-identity-recovery",
        "conflicts with the requested bootstrap topology"
    );
    phase4_case!(
        associated_database_identity_recovery,
        exercise_conflict_scenario,
        "04-seam-associated-database-identity-recovery",
        LicensedConflictScenario::AssociatedDatabaseIdentityOrRecovery,
        "seam-associated-database-identity-recovery",
        "conflicts with the requested bootstrap topology"
    );
    phase4_case!(
        target_lineage,
        exercise_conflict_scenario,
        "05-seam-target-lineage",
        LicensedConflictScenario::IncompatibleLineage,
        "seam-target-lineage",
        "conflicts with the requested bootstrap topology"
    );
    phase4_case!(
        sql_process_incarnation,
        exercise_conflict_scenario,
        "06-seam-sql-process-incarnation",
        LicensedConflictScenario::SqlProcessIncarnationReplacement,
        "seam-sql-process-incarnation",
        "is unsafe to continue"
    );
}

mod three_replica_mssql_terminal_seed_conflicts {
    use super::*;

    phase4_case!(
        association_removal,
        exercise_terminal_seed_conflict,
        "terminal-association-removal",
        false,
        "terminal-association-removal"
    );
    phase4_case!(
        ag_recreation,
        exercise_terminal_seed_conflict,
        "terminal-ag-recreation",
        true,
        "terminal-ag-recreation"
    );
}

mod three_replica_mssql_target_host_evidence_faults {
    use super::*;

    phase4_case!(
        pre_association_sql_process_restart,
        exercise_target_host_evidence_fault,
        "",
        TargetEvidenceWindow::PreAssociation,
        TargetEvidenceFault::SqlProcessRestart
    );
    phase4_case!(
        in_flight_seeding_sql_process_restart,
        exercise_target_host_evidence_fault,
        "",
        TargetEvidenceWindow::InFlightSeeding,
        TargetEvidenceFault::SqlProcessRestart
    );
    phase4_case!(
        post_native_pre_reply_sql_process_restart,
        exercise_target_host_evidence_fault,
        "",
        TargetEvidenceWindow::PostNativePreReply,
        TargetEvidenceFault::SqlProcessRestart
    );
    phase4_case!(
        lineage_mismatch_across_checkpoints,
        exercise_target_host_evidence_fault,
        "",
        TargetEvidenceWindow::PostNativePreReply,
        TargetEvidenceFault::LineageMismatch
    );
}

#[test]
#[ignore = "evaluates native prefixes with licensed owned-secondary HADR suspend/resume"]
fn three_replica_mssql_native_prefix_coverage() {
    run_native_experiment(NativeExperiment::PrefixCoverage);
}

#[derive(Clone, Copy)]
enum NativeExperiment {
    CommitRecord,
    PrefixCoverage,
}

fn run_native_experiment(kind: NativeExperiment) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("native commit probe runtime")
                .block_on(async {
                    let mut cancellation =
                        CancellationSignals::register().expect("cancellation signals");
                    let root = required_path("KUBERIC_MSSQL_THREE_REPLICA_ROOT");
                    let config =
                        FixtureConfig::for_test_fixture(&root).expect("test-only fixture config");
                    let mut launched = launch_three_members(config)
                        .await
                        .expect("owned native fixture");
                    let mut serialized_report = None;
                    let outcome = {
                        let experiment = AssertUnwindSafe(async {
                            launched.provision_availability_group().await?;
                            serialized_report = Some(match kind {
                                NativeExperiment::CommitRecord => serde_json::to_vec(
                                    &launched.probe_native_commit_coverage(2).await?,
                                )?,
                                NativeExperiment::PrefixCoverage => serde_json::to_vec(
                                    &launched.probe_native_prefix_coverage(2).await?,
                                )?,
                            });
                            Ok::<(), TestError>(())
                        })
                        .catch_unwind();
                        tokio::pin!(experiment);
                        tokio::select! {
                            result = &mut experiment => RuntimeOutcome::Completed(result),
                            signal = cancellation.recv() => RuntimeOutcome::Interrupted(signal),
                        }
                    };
                    let cleanup =
                        CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
                    finish_native_commit_report(
                        || finish_runtime_outcome(&root, launched, outcome, &cleanup),
                        serialized_report,
                        |bytes| {
                            let mut output = std::io::stdout().lock();
                            output.write_all(bytes)?;
                            writeln!(output)?;
                            output.flush()?;
                            Ok(())
                        },
                    )
                    .expect("native commit probe collection and cleanup");
                });
        })
        .expect("native commit probe thread")
        .join()
        .expect("native commit probe thread panicked");
}

fn finish_native_commit_report(
    cleanup: impl FnOnce() -> Result<(), TestError>,
    report: Option<Vec<u8>>,
    emit: impl FnOnce(&[u8]) -> Result<(), TestError>,
) -> Result<(), TestError> {
    cleanup()?;
    let report = report.ok_or("native commit probe completed without serialized evidence")?;
    emit(&report)
}

#[test]
fn native_probe_teardown_precedes_stalled_output_and_output_errors_survive() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};

    let cleaned = Arc::new(AtomicBool::new(false));
    let (entered, observed) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let cleanup_flag = cleaned.clone();
        finish_native_commit_report(
            || {
                cleanup_flag.store(true, Ordering::SeqCst);
                Ok(())
            },
            Some(b"evidence".to_vec()),
            |_| {
                entered.send(cleaned.load(Ordering::SeqCst))?;
                blocked.recv()?;
                Err("native probe output failed".into())
            },
        )
    });
    let cleanup_preceded_output = observed.recv_timeout(Duration::from_secs(5));
    let _ = release.send(());
    let result = writer.join().expect("controlled output worker");
    assert_eq!(cleanup_preceded_output, Ok(true));
    assert_eq!(
        result.unwrap_err().to_string(),
        "native probe output failed"
    );
}

#[test]
fn native_probe_failed_teardown_never_enters_output() {
    let error = finish_native_commit_report(
        || Err("native fixture cleanup failed".into()),
        Some(b"evidence".to_vec()),
        |_| panic!("output must not run with unresolved fixture ownership"),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "native fixture cleanup failed");
}

#[test]
fn native_prefix_operation_release_and_cleanup_errors_are_preserved() {
    use kuberic_mssql_tests::three_replica::{
        CommitCoverageError, FailureCategory, FailureStage, SanitizedFailure,
    };
    let primary = CommitCoverageError::Release {
        primary: Some(Box::new(CommitCoverageError::Control("work failed"))),
        release: Box::new(CommitCoverageError::Control("resume failed")),
    };
    let cleanup = CombinedFixtureError::new(
        SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::OwnershipMismatch),
        Vec::new(),
    );
    let expected_cleanup = cleanup.to_string();
    let error = finish_runtime_cleanup(
        Path::new("unused-native-prefix-root"),
        RuntimeOutcome::Completed(Ok(Err(Box::new(primary)))),
        Err(cleanup),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("work failed"));
    assert!(error.contains("resume failed"));
    assert!(error.contains(&expected_cleanup));
}

fn run_three_replica_test(validate_restart: bool) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("three-replica Tokio runtime")
                .block_on(run_three_replica_mssql_happy_path(validate_restart));
        })
        .expect("three-replica test thread")
        .join()
        .expect("three-replica test thread panicked");
}

fn run_phase4_test<F, Fut>(test: F)
where
    F: FnOnce(PathBuf) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), TestError>>,
{
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("three-replica Phase 4 Tokio runtime")
                .block_on(async move {
                    let root = required_path("KUBERIC_MSSQL_THREE_REPLICA_ROOT");
                    fs::create_dir_all(&root).expect("create SQL scenario namespace");
                    test(root).await.expect("licensed Phase 4 lifecycle");
                });
        })
        .expect("three-replica Phase 4 test thread")
        .join()
        .expect("three-replica Phase 4 test thread panicked");
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
fn native_probe_and_fixture_cleanup_errors_are_preserved() {
    use kuberic_mssql_tests::three_replica::{FailureCategory, FailureStage, SanitizedFailure};

    let cleanup = CombinedFixtureError::new(
        SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::OwnershipMismatch),
        Vec::new(),
    );
    let expected = cleanup.to_string();
    let error = finish_runtime_cleanup(
        Path::new("unused-native-probe-root"),
        RuntimeOutcome::Completed(Ok(Err("native probe query failed".into()))),
        Err(cleanup),
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("native probe query failed; cleanup also failed: {expected}")
    );
}

#[test]
#[ignore = "auto-accepts the SQL Server EULA for this test fixture and requires a qualified local Docker host"]
fn three_replica_sigterm_during_owned_launch_is_recoverable() {
    exercise_signal_recovery(libc::SIGTERM, "SIGTERM");
}

#[test]
#[ignore = "auto-accepts the SQL Server EULA for this test fixture and requires a qualified local Docker host"]
fn three_replica_sigint_during_owned_launch_is_recoverable() {
    exercise_signal_recovery(libc::SIGINT, "SIGINT");
}

#[test]
#[ignore = "auto-accepts the SQL Server EULA for this test fixture and requires a qualified local Docker host"]
fn three_replica_sigkill_is_recovered_by_a_separate_process() {
    exercise_signal_recovery(libc::SIGKILL, "SIGKILL");
}

fn exercise_signal_recovery(signal: i32, signal_name: &str) {
    let root = required_path("KUBERIC_MSSQL_THREE_REPLICA_ROOT");
    cleanup_three_replica_fixture(&root).expect("clean baseline before signal regression");

    let ready = root
        .parent()
        .expect("fixture root has a parent")
        .join(format!("signal-handlers-ready-{}", std::process::id()));
    if ready.exists() {
        fs::remove_file(&ready).expect("remove stale signal-ready evidence");
    }

    let mut interrupted = spawn_live_child(&root, Some(&ready));
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

    let signal_result = unsafe { libc::kill(interrupted.id() as i32, signal) };
    assert_eq!(signal_result, 0, "send {signal_name} to live subprocess");
    let status = wait_for_child_with_cleanup(&mut interrupted, Duration::from_secs(240), &root);
    assert!(
        !status.success(),
        "{signal_name} must make the interrupted validation fail explicitly"
    );

    let journal = load_journal_if_present(&root).expect("recoverable ownership journal");
    assert!(
        journal.state == RunState::Removed || !journal.resources.is_empty(),
        "{signal_name} must leave explicit cleanup or recoverable ownership evidence"
    );
    let mut cleanup = spawn_cleanup_child(&root);
    assert!(
        wait_for_child(&mut cleanup, Duration::from_secs(240)).success(),
        "separate cleanup process after {signal_name}"
    );
    assert_removed(&root);

    let mut retry = spawn_live_child(&root, None);
    let retry_status = wait_for_child_with_cleanup(&mut retry, Duration::from_secs(1200), &root);
    assert!(
        retry_status.success(),
        "retry after {signal_name} must succeed"
    );
    cleanup_three_replica_fixture(&root).expect("idempotent cleanup after retry");
    assert_removed(&root);
    fs::remove_file(&ready).expect("remove signal-ready evidence");
}

mod three_replica_post_ag_and_report_fault_checkpoints_recover {
    use super::*;

    #[test]
    #[ignore = "requires a qualified local Docker host and licensed SQL Server fixture"]
    fn fail_after_ag() {
        exercise_fault_checkpoint("fail-after-ag", 0, false);
    }

    #[test]
    #[ignore = "requires a qualified local Docker host and licensed SQL Server fixture"]
    fn panic_after_agent_start() {
        exercise_fault_checkpoint("panic-after-agent-start", 1, false);
    }

    #[test]
    #[ignore = "requires a qualified local Docker host and licensed SQL Server fixture"]
    fn fail_during_report() {
        exercise_fault_checkpoint("fail-during-report", 2, false);
    }

    #[test]
    #[ignore = "requires a qualified local Docker host and licensed SQL Server fixture"]
    fn fail_after_agent_restart_and_retry() {
        exercise_fault_checkpoint("fail-after-agent-restart", 3, true);
    }
}

fn exercise_fault_checkpoint(checkpoint: &str, index: usize, retry: bool) {
    let base = required_path("KUBERIC_MSSQL_THREE_REPLICA_ROOT");
    fs::create_dir_all(&base).expect("create fault-checkpoint namespace");
    let root = base.join(checkpoint);
    cleanup_three_replica_fixture(&root).expect("clean fault-checkpoint baseline");
    let receipt = root.join(format!(".fault-hit-{}-{index}", std::process::id()));
    assert!(!receipt.exists(), "checkpoint evidence must be fresh");
    let mut child = spawn_live_child_with_fault(&root, checkpoint, &receipt);
    let status = wait_for_child_with_cleanup(&mut child, Duration::from_secs(1200), &root);
    assert!(!status.success(), "{checkpoint} must fail explicitly");
    assert_eq!(
        fs::read_to_string(&receipt).expect("child must reach the requested checkpoint"),
        checkpoint,
    );
    fs::remove_file(receipt).expect("remove owned checkpoint receipt");
    let mut cleanup = spawn_cleanup_child(&root);
    assert!(
        wait_for_child(&mut cleanup, Duration::from_secs(240)).success(),
        "cleanup process after {checkpoint}"
    );
    assert_removed(&root);
    if retry {
        let mut child = spawn_restart_child(&root);
        assert!(
            wait_for_child_with_cleanup(&mut child, Duration::from_secs(1200), &root).success(),
            "retry after fault checkpoint"
        );
        cleanup_three_replica_fixture(&root).expect("cleanup after fault-checkpoint retry");
        assert_removed(&root);
    }
}

async fn run_three_replica_mssql_happy_path(validate_restart: bool) {
    let mut cancellation =
        CancellationSignals::register().expect("install SIGINT/SIGTERM cleanup handlers");
    write_signal_handler_ready_evidence();
    let root = PathBuf::from(
        std::env::var_os("KUBERIC_MSSQL_THREE_REPLICA_ROOT")
            .expect("KUBERIC_MSSQL_THREE_REPLICA_ROOT must be explicitly configured"),
    );
    let config =
        FixtureConfig::for_test_fixture(&root).expect("validated test-only fixture config");
    match AssertUnwindSafe(execute_live_lifecycle(
        &root,
        config,
        &mut cancellation,
        validate_restart,
    ))
    .catch_unwind()
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            panic!("three-replica lifecycle failed: {error}")
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn start_public_group_with_retry<C: CleanupClock>(
    root: &Path,
    run: &TopologyRun,
    native_binding: &NativeTopologyBinding,
    members: &[ReadyMember; 3],
    convergence_timeout: Duration,
    first_deadline: Instant,
    cleanup: &CleanupCoordinator<C>,
) -> Result<PublicMssqlGroup, MssqlGroupError> {
    for attempt in 1..=3 {
        let deadline = if attempt == 1 {
            first_deadline
        } else {
            Instant::now() + convergence_timeout
        };
        match PublicMssqlGroup::from_live_with_coordinator(
            root,
            run,
            native_binding,
            members,
            convergence_timeout,
            deadline,
            cleanup,
        )
        .await
        {
            Ok(group) => return Ok(group),
            Err(error)
                if !error.agent_termination_unconfirmed()
                    && error.to_string().contains("operation was cancelled")
                    && attempt < 3 =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded public Kuberic retry loop always returns")
}

async fn start_prepared_public_group_with_retry<C: CleanupClock>(
    root: &Path,
    run: &TopologyRun,
    prepared: &kuberic_mssql_tests::three_replica::PreparedAvailabilityGroup,
    members: &[ReadyMember; 3],
    convergence_timeout: Duration,
    first_deadline: Instant,
    cleanup: &CleanupCoordinator<C>,
) -> Result<PublicMssqlGroup, MssqlGroupError> {
    for attempt in 1..=3 {
        let deadline = if attempt == 1 {
            first_deadline
        } else {
            Instant::now() + convergence_timeout
        };
        match PublicMssqlGroup::from_prepared_with_coordinator(
            root,
            run,
            prepared,
            members,
            convergence_timeout,
            deadline,
            cleanup,
        )
        .await
        {
            Ok(group) => return Ok(group),
            Err(error)
                if !error.agent_termination_unconfirmed()
                    && error.to_string().contains("operation was cancelled")
                    && attempt < 3 =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded prepared public Kuberic retry loop always returns")
}

async fn execute_live_lifecycle(
    root: &Path,
    config: FixtureConfig,
    cancellation: &mut CancellationSignals,
    validate_restart: bool,
) -> Result<(), TestError> {
    let mut launched = launch_three_members(config)
        .await
        .map_err(|error| -> TestError { Box::new(error) })?;
    if !validate_restart {
        return execute_phase2_bootstrap_lifecycle(root, launched, cancellation).await;
    }
    if std::env::var_os("KUBERIC_MSSQL_LEGACY_PROVISIONED_RESTART").is_none() {
        return execute_phase3_bootstrap_restart_lifecycle(root, launched, cancellation).await;
    }
    let topology = launched.provision_availability_group().await?;
    if fault_checkpoint("fail-after-ag") {
        return Err("injected failure after AG provisioning".into());
    }
    let native_binding = launched
        .journal()
        .native_binding
        .clone()
        .ok_or("exact native topology binding was not journaled")?;
    if native_binding != topology.binding {
        return Err("journaled native binding changed".into());
    }
    let startup_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = match AssertUnwindSafe(start_public_group_with_retry(
        root,
        &launched.run,
        &native_binding,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &startup_cleanup,
    ))
    .catch_unwind()
    .await
    {
        Ok(Ok(group)) => group,
        Ok(Err(error)) => {
            if error.agent_termination_unconfirmed() {
                let diagnostic = format!("partial Kuberic startup failed: {error}");
                if let Err(block) = launched.block_cleanup() {
                    return Err(format!(
                        "{diagnostic}; ownership journal blocking also failed: {block}"
                    )
                    .into());
                }
                return Err(diagnostic.into());
            }
            let fixture_cleanup = launched.cleanup_with_coordinator(&startup_cleanup);
            return match fixture_cleanup {
                Ok(_) => Err(Box::new(error)),
                Err(cleanup) => Err(format!(
                    "partial Kuberic startup failed: {error}; cleanup also failed: {cleanup}"
                )
                .into()),
            };
        }
        Err(panic) => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!(
                    "partial Kuberic startup panicked: {}",
                    panic_message(&panic),
                ),
            );
        }
    };
    let first_resource_uid = group.resource_uid.clone();
    let first_effective_policy = group.effective_policy.clone();
    let first_configuration = group.configuration.clone();
    let first_pods = group.pods.each_ref().map(|pod| {
        (
            pod.identity.clone(),
            pod.stable_role,
            pod.pod_uid.clone(),
            pod.pvc_uid.clone(),
        )
    });
    let first_sessions = group.pods.each_ref().map(|pod| pod.session.clone());
    let first_outcome = {
        let runtime = AssertUnwindSafe(async {
            if fault_checkpoint("panic-after-agent-start") {
                panic!("injected panic after agent startup");
            }
            if fault_checkpoint("fail-during-report") {
                return Err::<(), TestError>("injected failure during bracketed reporting".into());
            }
            wait_for_exact_reports(
                &group,
                native_binding.configuration_sequence,
                launched.kuberic_convergence_timeout(),
            )
            .await?;
            let marker = launched
                .commit_replicated_marker(&topology.evidence)
                .await?;
            if marker.primary_ordinal != topology.evidence.primary_ordinal
                || marker.readable_ordinals != [1, 2, 3]
            {
                return Err("post-runtime marker was not readable from all exact members".into());
            }
            Ok(())
        })
        .catch_unwind();
        tokio::pin!(runtime);
        tokio::select! {
            result = &mut runtime => RuntimeOutcome::Completed(result),
            signal = cancellation.recv() => RuntimeOutcome::Interrupted(signal),
        }
    };
    let first_shutdown_cleanup =
        CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let shutdown = group
        .shutdown_with_coordinator(&first_shutdown_cleanup)
        .await;
    if let Err(shutdown) = shutdown {
        let diagnostic = format!(
            "{}; Kuberic shutdown failed before fixture cleanup: {shutdown}",
            first_outcome.description()
        );
        if let Err(block) = launched.block_cleanup() {
            return Err(
                format!("{diagnostic}; ownership journal blocking also failed: {block}").into(),
            );
        }
        return Err(diagnostic.into());
    }
    if !matches!(&first_outcome, RuntimeOutcome::Completed(Ok(Ok(())))) {
        let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
        return finish_runtime_outcome(root, launched, first_outcome, &cleanup);
    }
    let restart_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let restarted = match AssertUnwindSafe(start_public_group_with_retry(
        root,
        &launched.run,
        &native_binding,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &restart_cleanup,
    ))
    .catch_unwind()
    .await
    {
        Ok(Ok(group)) => group,
        Ok(Err(error)) => {
            if error.agent_termination_unconfirmed() {
                let diagnostic = format!("replacement Kuberic startup failed: {error}");
                if let Err(block) = launched.block_cleanup() {
                    return Err(format!(
                        "{diagnostic}; ownership journal blocking also failed: {block}"
                    )
                    .into());
                }
                return Err(diagnostic.into());
            }
            let fixture_cleanup = launched.cleanup_with_coordinator(&restart_cleanup);
            return match fixture_cleanup {
                Ok(_) => Err(format!("replacement Kuberic startup failed: {error}").into()),
                Err(cleanup) => Err(format!(
                    "replacement Kuberic startup failed: {error}; cleanup also failed: {cleanup}"
                )
                .into()),
            };
        }
        Err(panic) => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!(
                    "replacement Kuberic startup panicked: {}",
                    panic_message(&panic),
                ),
            );
        }
    };
    let outcome = {
        let runtime = AssertUnwindSafe(async {
            if restarted.resource_uid != first_resource_uid
                || restarted.effective_policy != first_effective_policy
                || restarted.configuration != first_configuration
                || restarted.pods.iter().zip(&first_pods).any(
                    |(pod, (identity, role, pod_uid, pvc_uid))| {
                        &pod.identity != identity
                            || pod.stable_role != *role
                            || &pod.pod_uid != pod_uid
                            || &pod.pvc_uid != pvc_uid
                    },
                )
            {
                return Err::<(), TestError>(
                    "replacement Kuberic hosts changed stable identity or authority".into(),
                );
            }
            restarted
                .assert_superseded_sessions_rejected(&first_sessions)
                .await?;
            if fault_checkpoint("fail-after-agent-restart") {
                return Err("injected failure after replacement agent startup".into());
            }
            wait_for_exact_reports(
                &restarted,
                native_binding.configuration_sequence,
                launched.kuberic_convergence_timeout(),
            )
            .await?;
            let marker = launched
                .commit_replicated_marker(&topology.evidence)
                .await?;
            if marker.primary_ordinal != topology.evidence.primary_ordinal
                || marker.readable_ordinals != [1, 2, 3]
            {
                return Err("post-restart marker was not readable from all exact members".into());
            }
            Ok(())
        })
        .catch_unwind();
        tokio::pin!(runtime);
        tokio::select! {
            result = &mut runtime => RuntimeOutcome::Completed(result),
            signal = cancellation.recv() => RuntimeOutcome::Interrupted(signal),
        }
    };
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let shutdown = restarted.shutdown_with_coordinator(&cleanup).await;
    if let Err(shutdown) = shutdown {
        let diagnostic = format!(
            "{}; replacement Kuberic shutdown failed before fixture cleanup: {shutdown}",
            outcome.description()
        );
        if let Err(block) = launched.block_cleanup() {
            return Err(
                format!("{diagnostic}; ownership journal blocking also failed: {block}").into(),
            );
        }
        return Err(diagnostic.into());
    }
    finish_runtime_outcome(root, launched, outcome, &cleanup)
}

async fn execute_phase2_bootstrap_lifecycle(
    root: &Path,
    mut launched: kuberic_mssql_tests::three_replica::LaunchedMembers,
    cancellation: &mut CancellationSignals,
) -> Result<(), TestError> {
    let prepared = launched.prepare_availability_group().await?;
    if launched.journal().native_binding.is_some() {
        return Err("prepared fixture unexpectedly claimed a native topology binding".into());
    }
    if fault_checkpoint("fail-after-ag") {
        return Err("injected failure after endpoint and peer preparation".into());
    }
    let startup_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = match AssertUnwindSafe(start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &startup_cleanup,
    ))
    .catch_unwind()
    .await
    {
        Ok(Ok(group)) => group,
        Ok(Err(error)) => {
            if error.agent_termination_unconfirmed() {
                let diagnostic = format!("partial bootstrap host startup failed: {error}");
                if let Err(block) = launched.block_cleanup() {
                    return Err(format!(
                        "{diagnostic}; ownership journal blocking also failed: {block}"
                    )
                    .into());
                }
                return Err(diagnostic.into());
            }
            let fixture_cleanup = launched.cleanup_with_coordinator(&startup_cleanup);
            return match fixture_cleanup {
                Ok(_) => Err(Box::new(error)),
                Err(cleanup) => Err(format!(
                    "partial bootstrap host startup failed: {error}; cleanup also failed: {cleanup}"
                )
                .into()),
            };
        }
        Err(panic) => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!(
                    "partial bootstrap host startup panicked: {}",
                    panic_message(&panic),
                ),
            );
        }
    };
    let outcome = {
        let runtime = AssertUnwindSafe(async {
            if fault_checkpoint("panic-after-agent-start") {
                panic!("injected panic after bootstrap agent startup");
            }
            if fault_checkpoint("fail-during-report") {
                return Err::<(), TestError>("injected failure during bracketed reporting".into());
            }
            let sequence = group.bootstrap_configuration_sequence().await?;
            wait_for_exact_reports(&group, sequence, launched.kuberic_convergence_timeout())
                .await?;
            let pending = exercise_public_build_retirement_and_supersession(&group).await?;
            let topology = launched
                .finalize_prepared_availability_group(&prepared)
                .await?;
            exercise_dispatched_build_lifecycle_commands(&group, pending).await?;
            if group.testing_seed_dispatch_count()? != 1 {
                return Err("final replacement did not dispatch exactly one ADD DATABASE".into());
            }
            let marker = launched
                .commit_replicated_marker(&topology.evidence)
                .await?;
            if marker.primary_ordinal != topology.evidence.primary_ordinal
                || marker.readable_ordinals != [1, 2, 3]
            {
                return Err(
                    "build-owned seeding marker was not readable from all exact members".into(),
                );
            }
            let peer = group.target_build_command(
                2,
                "native-only-seed-build-target-3",
                "native-only-seed-build-target-3",
            )?;
            let outcomes = group.execute_build_commands_concurrently(&[peer]).await;
            if let Some(error) = outcomes.into_iter().find_map(Result::err) {
                return Err::<(), TestError>(
                    format!("native-only peer target build failed: {error}").into(),
                );
            }
            Ok(())
        })
        .catch_unwind();
        tokio::pin!(runtime);
        tokio::select! {
            result = &mut runtime => RuntimeOutcome::Completed(result),
            signal = cancellation.recv() => RuntimeOutcome::Interrupted(signal),
        }
    };
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    if let Err(shutdown) = group.shutdown_with_coordinator(&cleanup).await {
        let diagnostic = format!(
            "{}; bootstrap Kuberic shutdown failed before fixture cleanup: {shutdown}",
            outcome.description()
        );
        if let Err(block) = launched.block_cleanup() {
            return Err(
                format!("{diagnostic}; ownership journal blocking also failed: {block}").into(),
            );
        }
        return Err(diagnostic.into());
    }
    finish_runtime_outcome(root, launched, outcome, &cleanup)
}

async fn exercise_public_build_retirement_and_supersession(
    group: &PublicMssqlGroup,
) -> Result<PendingSeedCompaction, TestError> {
    let retired_winner =
        group.target_build_command(1, "retired-winning-build", "retired-winning-build")?;
    let (retired_winner_release, retired_winner_callback) =
        submit_build_at_intent_checkpoint(group, &retired_winner, "retired winner").await?;
    assert_seed_intent_owner(group, &retired_winner, "retired winner")?;
    group
        .testing_execute_kuberic_build_command(&group.retire_build_command(&retired_winner))
        .await
        .map_err(|error| format!("winning build retirement failed: {error}"))?;
    assert_callback_cancelled(retired_winner_callback, "retired winning callback").await?;
    if retired_winner_release.send(()).is_ok() {
        return Err("retired winning callback remained live after exact retirement".into());
    }
    assert_seed_intent_owner(group, &retired_winner, "retired winner residue")?;
    assert_kuberic_build_rejected(group, &retired_winner, "retired winner replay").await?;
    group
        .testing_assert_source_build_success_not_published(&retired_winner)
        .await?;

    let superseded_winner =
        group.target_build_command(1, "superseded-winning-build", "superseded-winning-build")?;
    let (superseded_winner_release, superseded_winner_callback) =
        submit_build_at_intent_checkpoint(group, &superseded_winner, "superseded winner").await?;
    assert_seed_intent_owner(group, &superseded_winner, "replacement after retirement")?;
    group
        .testing_assert_build_success_not_published(&retired_winner)
        .await?;

    let current_winner = group.supersede_build_command(&superseded_winner, "current-winning-build");
    group
        .testing_execute_kuberic_build_command(&current_winner)
        .await
        .map_err(|error| format!("winning build supersession failed: {error}"))?;
    assert_callback_cancelled(superseded_winner_callback, "superseded winning callback").await?;
    if superseded_winner_release.send(()).is_ok() {
        return Err("superseded winning callback remained live after replacement".into());
    }
    let (current_release_tx, current_callback) =
        spawn_callback_at_intent_checkpoint(group, &current_winner, "superseding winner").await?;
    assert_seed_intent_owner(group, &current_winner, "superseding winner")?;
    assert_kuberic_build_rejected(group, &superseded_winner, "superseded winner replay").await?;
    group
        .testing_assert_build_success_not_published(&superseded_winner)
        .await?;

    let retired_loser =
        group.target_build_command(2, "retired-losing-build", "retired-losing-build")?;
    group
        .testing_execute_kuberic_build_command(&retired_loser)
        .await
        .map_err(|error| format!("losing build admission failed: {error}"))?;
    let retired_loser_callback = group
        .testing_spawn_admitted_seed_callback(&retired_loser)
        .map_err(|error| format!("losing build callback failed to start: {error}"))?;
    group
        .testing_execute_kuberic_build_command(&group.retire_build_command(&retired_loser))
        .await
        .map_err(|error| format!("losing build retirement failed: {error}"))?;
    assert_callback_cancelled(retired_loser_callback, "retired losing callback").await?;
    assert_seed_intent_owner(group, &current_winner, "winner after losing retirement")?;
    assert_kuberic_build_rejected(group, &retired_loser, "retired loser replay").await?;
    group
        .testing_assert_source_build_success_not_published(&retired_loser)
        .await?;

    let superseded_loser =
        group.target_build_command(2, "superseded-losing-build", "superseded-losing-build")?;
    group
        .testing_execute_kuberic_build_command(&superseded_loser)
        .await
        .map_err(|error| format!("losing build admission before supersession failed: {error}"))?;
    group
        .testing_assert_build_success_not_published(&retired_loser)
        .await?;
    let superseded_loser_callback = group
        .testing_spawn_admitted_seed_callback(&superseded_loser)
        .map_err(|error| format!("superseded losing callback failed to start: {error}"))?;
    let final_winner = group.supersede_build_command(&superseded_loser, "replacement-losing-build");
    group
        .testing_execute_kuberic_build_command(&final_winner)
        .await
        .map_err(|error| format!("losing build supersession failed: {error}"))?;
    assert_callback_cancelled(superseded_loser_callback, "superseded losing callback").await?;
    assert_seed_intent_owner(group, &current_winner, "winner after losing supersession")?;
    assert_kuberic_build_rejected(group, &superseded_loser, "superseded loser replay").await?;
    group
        .testing_assert_build_success_not_published(&superseded_loser)
        .await?;

    group
        .testing_execute_kuberic_build_command(&group.retire_build_command(&current_winner))
        .await
        .map_err(|error| format!("current winner retirement failed: {error}"))?;
    assert_callback_cancelled(current_callback, "retired current winning callback").await?;
    if current_release_tx.send(()).is_ok() {
        return Err("retired current winner callback remained live".into());
    }
    group
        .testing_assert_source_build_success_not_published(&current_winner)
        .await?;
    let (final_release_tx, final_callback) =
        spawn_callback_at_intent_checkpoint(group, &final_winner, "replacement losing build")
            .await?;
    assert_seed_intent_owner(group, &final_winner, "final replacement winner")?;
    if group.testing_seed_dispatch_count()? != 0 {
        return Err("retirement or supersession dispatched ADD DATABASE".into());
    }

    let (compaction_entered_tx, compaction_entered_rx) = tokio::sync::oneshot::channel();
    let (compaction_release_tx, compaction_release_rx) = tokio::sync::oneshot::channel();
    group.testing_gate_next_seed_compaction(compaction_entered_tx, compaction_release_rx)?;
    final_release_tx
        .send(())
        .map_err(|_| "final replacement intent checkpoint was not retained")?;
    let mut final_callback = final_callback;
    tokio::select! {
        entered = compaction_entered_rx => entered
            .map_err(|_| "final replacement ended before the accepted-dispatch checkpoint")?,
        result = &mut final_callback => {
            return Err(format!(
                "final replacement completed before the accepted-dispatch checkpoint: {result:?}"
            )
            .into());
        }
    }
    Ok(PendingSeedCompaction {
        command: final_winner,
        release: compaction_release_tx,
        callback: final_callback,
    })
}

async fn submit_build_at_intent_checkpoint(
    group: &PublicMssqlGroup,
    command: &PublicBuildCommand,
    label: &str,
) -> Result<
    (
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::Status>>,
    ),
    TestError,
> {
    group
        .testing_execute_kuberic_build_command(command)
        .await
        .map_err(|error| format!("{label} Kuberic command failed: {error}"))?;
    spawn_callback_at_intent_checkpoint(group, command, label).await
}

async fn spawn_callback_at_intent_checkpoint(
    group: &PublicMssqlGroup,
    command: &PublicBuildCommand,
    label: &str,
) -> Result<
    (
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::Status>>,
    ),
    TestError,
> {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    group.testing_gate_next_seed_intent(entered_tx, release_rx)?;
    let mut callback = group
        .testing_spawn_admitted_seed_callback(command)
        .map_err(|error| format!("{label} callback failed to start: {error}"))?;
    tokio::select! {
        entered = entered_rx => entered
            .map_err(|_| format!("{label} callback ended before its seed checkpoint"))?,
        result = &mut callback => {
            return Err(format!("{label} callback completed before its seed checkpoint: {result:?}").into());
        }
        _ = tokio::time::sleep(Duration::from_secs(120)) => {
            return Err(format!("{label} did not reach its deterministic seed checkpoint").into());
        }
    }
    Ok((release_tx, callback))
}

async fn assert_callback_cancelled(
    callback: tokio::task::JoinHandle<Result<(), tonic::Status>>,
    label: &str,
) -> Result<(), TestError> {
    // The pinned runtime has no public source-callback delivery command. The
    // owned task models the callback future that Kuberic cancels after the
    // durable retirement or supersession command has completed.
    callback.abort();
    match callback.await {
        Err(error) if error.is_cancelled() => Ok(()),
        result => {
            Err(format!("{label} was able to publish after losing authority: {result:?}").into())
        }
    }
}

fn assert_seed_intent_owner(
    group: &PublicMssqlGroup,
    command: &PublicBuildCommand,
    label: &str,
) -> Result<(), TestError> {
    let record = group
        .testing_seed_dispatch_record()?
        .ok_or_else(|| format!("{label} did not retain seed intent"))?;
    if record.phase() != SeedDispatchPhase::Intent
        || record.winning_build_id() != command.build_id.as_str()
    {
        return Err(format!("{label} did not own the exact retained seed intent").into());
    }
    Ok(())
}

async fn assert_kuberic_build_rejected(
    group: &PublicMssqlGroup,
    command: &PublicBuildCommand,
    label: &str,
) -> Result<(), TestError> {
    if group
        .testing_execute_kuberic_build_command(command)
        .await
        .is_ok()
    {
        return Err(format!("{label} was accepted after losing durable authority").into());
    }
    Ok(())
}

async fn exercise_dispatched_build_lifecycle_commands(
    group: &PublicMssqlGroup,
    pending: PendingSeedCompaction,
) -> Result<(), TestError> {
    let PendingSeedCompaction {
        command: winner,
        release: compaction_release,
        callback: mut winner_callback,
    } = pending;
    let retired_loser =
        group.target_build_command(1, "dispatched-retired-loser", "dispatched-retired-loser")?;
    group
        .testing_execute_kuberic_build_command(&retired_loser)
        .await
        .map_err(|error| format!("dispatched losing build admission failed: {error}"))?;
    group
        .testing_execute_kuberic_build_command(&group.retire_build_command(&retired_loser))
        .await
        .map_err(|error| format!("dispatched losing build retirement failed: {error}"))?;
    assert_kuberic_build_rejected(group, &retired_loser, "dispatched retired loser replay").await?;
    group
        .testing_assert_source_build_success_not_published(&retired_loser)
        .await?;

    let superseded_loser = group.target_build_command(
        1,
        "dispatched-superseded-loser",
        "dispatched-superseded-loser",
    )?;
    group
        .testing_execute_kuberic_build_command(&superseded_loser)
        .await
        .map_err(|error| format!("dispatched losing build admission failed: {error}"))?;
    group
        .testing_assert_build_success_not_published(&retired_loser)
        .await?;
    let replacement_loser =
        group.supersede_build_command(&superseded_loser, "dispatched-replacement-loser");
    group
        .testing_execute_kuberic_build_command(&replacement_loser)
        .await
        .map_err(|error| format!("dispatched losing build supersession failed: {error}"))?;
    assert_kuberic_build_rejected(
        group,
        &superseded_loser,
        "dispatched superseded loser replay",
    )
    .await?;
    group
        .testing_assert_build_success_not_published(&superseded_loser)
        .await?;

    let retained = group
        .testing_seed_dispatch_record()?
        .ok_or("dispatched lifecycle commands released the retained seed record")?;
    if !matches!(
        retained.phase(),
        SeedDispatchPhase::Submitting | SeedDispatchPhase::Accepted | SeedDispatchPhase::Ambiguous
    ) || retained.winning_build_id() != winner.build_id.as_str()
        || group.testing_seed_dispatch_count()? != 1
    {
        return Err("retirement or supersession mutated durable seed authority".into());
    }

    let (evidence_entered_tx, evidence_entered_rx) = tokio::sync::oneshot::channel();
    let (evidence_release_tx, evidence_release_rx) = tokio::sync::oneshot::channel();
    group.testing_gate_next_target_evidence(evidence_entered_tx, evidence_release_rx)?;
    compaction_release
        .send(())
        .map_err(|_| "dispatched winner compaction checkpoint was not retained")?;
    tokio::select! {
        entered = evidence_entered_rx => entered
            .map_err(|_| "dispatched winner ended before target evidence collection")?,
        result = &mut winner_callback => {
            return Err(format!(
                "dispatched winner completed before target evidence supersession: {result:?}"
            )
            .into());
        }
        _ = tokio::time::sleep(Duration::from_secs(120)) => {
            return Err("dispatched winner did not reach target evidence collection".into());
        }
    }

    let replacement_winner =
        group.supersede_build_command(&winner, "dispatched-replacement-winner");
    group
        .testing_execute_kuberic_build_command(&replacement_winner)
        .await
        .map_err(|error| format!("dispatched winning build supersession failed: {error}"))?;
    evidence_release_tx
        .send(())
        .map_err(|_| "dispatched winner target evidence checkpoint was not retained")?;
    let stale_error = winner_callback
        .await
        .map_err(|error| format!("dispatched winner callback task failed: {error}"))?
        .expect_err("superseded dispatched winner must not obtain completion evidence");
    if !stale_error.message().contains("target-host test authority") {
        return Err(format!(
            "superseded dispatched winner returned an unexpected error: {stale_error}"
        )
        .into());
    }
    assert_kuberic_build_rejected(group, &winner, "dispatched stale winner replay").await?;
    group
        .testing_assert_build_success_not_published(&winner)
        .await?;

    group
        .testing_execute_admitted_seed_callback_with_retry(&replacement_winner)
        .await
        .map_err(|error| format!("replacement winner reconciliation failed: {error}"))?;
    if group.testing_seed_dispatch_count()? != 1 || group.testing_seed_dispatch_record()?.is_some()
    {
        return Err("replacement winner redispatched or retained reconciled seed state".into());
    }

    let replay = group.replay_build_command(&replacement_winner);
    group
        .testing_execute_kuberic_build_command(&replay)
        .await
        .map_err(|error| format!("replacement winner replay authority failed: {error}"))?;
    group
        .testing_execute_admitted_seed_callback_with_retry(&replay)
        .await
        .map_err(|error| format!("replacement winner replay callback failed: {error}"))?;
    if group.testing_seed_dispatch_count()? != 1 || group.testing_seed_dispatch_record()?.is_some()
    {
        return Err("idempotent current replay redispatched or recreated seed state".into());
    }
    Ok(())
}

async fn execute_phase3_bootstrap_restart_lifecycle(
    root: &Path,
    mut launched: kuberic_mssql_tests::three_replica::LaunchedMembers,
    _cancellation: &mut CancellationSignals,
) -> Result<(), TestError> {
    let prepared = launched.prepare_availability_group().await?;
    let convergence = launched.kuberic_convergence_timeout();

    let first_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let first = match start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        convergence,
        launched.complete_deadline(),
        &first_cleanup,
    )
    .await
    {
        Ok(group) => group,
        Err(error) if error.agent_termination_unconfirmed() => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!("pre-association startup termination unconfirmed: {error}"),
            );
        }
        Err(error) => {
            let cleanup = launched.cleanup_with_coordinator(&first_cleanup);
            return match cleanup {
                Ok(_) => Err(Box::new(error)),
                Err(cleanup) => Err(format!(
                    "pre-association bootstrap startup failed: {error}; cleanup also failed: {cleanup}"
                )
                .into()),
            };
        }
    };
    let stable_resource = first.resource_uid.clone();
    let stable_policy = first.effective_policy.clone();
    let stable_configuration = first.configuration.clone();
    let stable_pods = first.pods.each_ref().map(|pod| {
        (
            pod.identity.clone(),
            pod.stable_role,
            pod.pod_uid.clone(),
            pod.pvc_uid.clone(),
        )
    });
    let first_sessions = first.pods.each_ref().map(|pod| pod.session.clone());
    let first_sequence = first.bootstrap_configuration_sequence().await?;
    wait_for_exact_reports(&first, first_sequence, convergence).await?;
    let interrupted_build = first.target_build_command(
        1,
        "restart-dispatched-build-2",
        "restart-dispatched-build-2",
    )?;
    let (dispatch_entered_tx, dispatch_entered_rx) = tokio::sync::oneshot::channel();
    let (dispatch_release_tx, dispatch_release_rx) = tokio::sync::oneshot::channel();
    first.testing_gate_next_seed_compaction(dispatch_entered_tx, dispatch_release_rx)?;
    first
        .testing_execute_kuberic_build_command(&interrupted_build)
        .await
        .map_err(|error| format!("licensed restart build command failed: {error}"))?;
    let mut interrupted = first
        .testing_spawn_admitted_seed_callback(&interrupted_build)
        .map_err(|error| format!("licensed restart callback failed to start: {error}"))?;
    tokio::select! {
        entered = dispatch_entered_rx => entered
            .map_err(|_| "licensed build ended before the compaction checkpoint")?,
        result = &mut interrupted => {
            return Err(format!(
                "licensed build completed before the compaction checkpoint: {result:?}"
            )
            .into());
        }
    }
    let topology = launched
        .finalize_prepared_availability_group(&prepared)
        .await?;
    let marker = launched
        .commit_replicated_marker(&topology.evidence)
        .await?;
    let retained = first
        .testing_seed_dispatch_record()?
        .ok_or("compaction checkpoint did not retain seed authority")?;
    if !matches!(
        retained.phase(),
        SeedDispatchPhase::Submitting | SeedDispatchPhase::Accepted | SeedDispatchPhase::Ambiguous
    ) || retained.winning_build_id() != interrupted_build.build_id.as_str()
    {
        return Err("compaction checkpoint retained the wrong seed authority".into());
    }
    if first.testing_seed_dispatch_count()? != 1 {
        return Err("compaction checkpoint did not execute exactly one ADD DATABASE".into());
    }
    first.testing_abort_seed_source();
    dispatch_release_tx
        .send(())
        .map_err(|_| "licensed compaction checkpoint was not retained")?;
    let retained = first
        .testing_seed_dispatch_record()?
        .ok_or("host abort released a durably dispatched seed record")?;
    if !matches!(
        retained.phase(),
        SeedDispatchPhase::Submitting | SeedDispatchPhase::Accepted | SeedDispatchPhase::Ambiguous
    ) {
        return Err("host abort made dispatched seed authority releasable".into());
    }
    first.shutdown_with_coordinator(&first_cleanup).await?;
    match interrupted.await {
        Ok(Err(_)) => {}
        result => {
            return Err(format!(
                "stale dispatched callback published after host shutdown: {result:?}"
            )
            .into());
        }
    }

    let replacement_cleanup =
        CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let replacement = match start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        convergence,
        launched.complete_deadline(),
        &replacement_cleanup,
    )
    .await
    {
        Ok(group) => group,
        Err(error) if error.agent_termination_unconfirmed() => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!("dispatched replacement termination unconfirmed: {error}"),
            );
        }
        Err(error) => {
            let cleanup = launched.cleanup_with_coordinator(&replacement_cleanup);
            return match cleanup {
                Ok(_) => Err(format!("dispatched replacement startup failed: {error}").into()),
                Err(cleanup) => Err(format!(
                    "dispatched replacement startup failed: {error}; cleanup also failed: {cleanup}"
                )
                .into()),
            };
        }
    };
    replacement
        .assert_superseded_sessions_rejected(&first_sessions)
        .await?;
    if replacement.resource_uid != stable_resource
        || replacement.effective_policy != stable_policy
        || replacement.configuration != stable_configuration
        || replacement.pods.iter().zip(&stable_pods).any(
            |(pod, (identity, role, pod_uid, pvc_uid))| {
                &pod.identity != identity
                    || pod.stable_role != *role
                    || &pod.pod_uid != pod_uid
                    || &pod.pvc_uid != pvc_uid
            },
        )
    {
        return Err("dispatched replacement changed stable authority".into());
    }
    if fault_checkpoint("fail-after-agent-restart") {
        let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
        if let Err(error) = replacement.shutdown_with_coordinator(&cleanup).await {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!("injected replacement shutdown failed: {error}"),
            );
        }
        return finish_runtime_outcome(
            root,
            launched,
            RuntimeOutcome::Completed(Ok(Err(
                "injected failure after replacement agent startup".into()
            ))),
            &cleanup,
        );
    }
    if replacement.testing_seed_dispatch_record()?.is_some() {
        let builds = [
            replacement.target_build_command(
                1,
                "replacement-native-build-2",
                "replacement-native-build-2",
            )?,
            replacement.target_build_command(
                2,
                "replacement-native-build-3",
                "replacement-native-build-3",
            )?,
        ];
        let outcomes = replacement
            .execute_build_commands_concurrently(&builds)
            .await;
        if let Some(error) = outcomes.into_iter().find_map(Result::err) {
            return Err(format!("native-only replacement build failed: {error}").into());
        }
    }
    if replacement.testing_seed_dispatch_count()? != 0 {
        return Err("replacement host issued a second ADD DATABASE dispatch".into());
    }
    if replacement.testing_seed_dispatch_record()?.is_some() {
        return Err("replacement host did not compact the dispatched seed record".into());
    }
    let verified_marker = launched.verify_replicated_marker(&marker).await?;
    if verified_marker != marker
        || verified_marker.primary_ordinal != topology.evidence.primary_ordinal
        || verified_marker.readable_ordinals != [1, 2, 3]
    {
        return Err("exact pre-restart marker changed after dispatched replacement".into());
    }
    replacement
        .shutdown_with_coordinator(&replacement_cleanup)
        .await?;
    finish_runtime_outcome(
        root,
        launched,
        RuntimeOutcome::Completed(Ok(Ok(()))),
        &replacement_cleanup,
    )
}

async fn exercise_terminal_seed_conflict(
    root: &Path,
    recreate_group: bool,
    label: &str,
) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &cleanup,
    )
    .await?;
    let sequence = group.bootstrap_configuration_sequence().await?;
    wait_for_exact_reports(&group, sequence, launched.kuberic_convergence_timeout()).await?;
    let owner =
        group.target_build_command(1, format!("{label}-owner"), format!("{label}-owner"))?;
    group
        .testing_admit_target_build_authority_only(&owner)
        .await
        .map_err(|error| format!("{label} seed authority admission failed: {error}"))?;
    group
        .testing_execute_admitted_seed_callback_with_retry(&owner)
        .await
        .map_err(|error| format!("{label} seed owner callback failed: {error}"))?;
    let dispatches = group.testing_seed_dispatch_count()?;
    let retained = group.testing_seed_dispatch_record()?;
    if dispatches != 1 || retained.is_some() {
        return Err(format!(
            "{label} did not compact exactly one accepted dispatch: \
             dispatches={dispatches}, retained={}",
            retained.is_some()
        )
        .into());
    }

    launched
        .testing_remove_managed_database_association()
        .await?;
    if recreate_group {
        launched
            .testing_recreate_availability_group_without_database()
            .await?;
    }
    let error = group
        .testing_execute_admitted_seed_callback(&owner)
        .await
        .expect_err("post-compaction topology loss must be terminal");
    let expected = if recreate_group {
        error.message().contains("availability group differs")
            || error.message().contains("availability-group")
    } else {
        error
            .message()
            .contains("terminal seed dispatch source association")
    };
    if !expected {
        return Err(format!("{label} returned an unexpected classification: {error}").into());
    }
    if group.testing_seed_dispatch_count()? != 1 || group.testing_seed_dispatch_record()?.is_some()
    {
        return Err(format!("{label} redispatched or recreated temporary seed state").into());
    }

    group.shutdown_with_coordinator(&cleanup).await?;
    let evidence = launched.cleanup_with_coordinator(&cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err(format!("{label} cleanup did not remove the exact owned topology").into());
    }
    assert_removed(root);
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

async fn exercise_seed_submission_fault(
    root: &Path,
    fault: PublicMutationFault,
    label: &str,
) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &cleanup,
    )
    .await?;
    let sequence = group.bootstrap_configuration_sequence().await?;
    wait_for_exact_reports(&group, sequence, launched.kuberic_convergence_timeout()).await?;
    let command =
        group.target_build_command(1, format!("{label}-build"), format!("{label}-build"))?;
    group
        .testing_admit_target_build_authority_only(&command)
        .await
        .map_err(|error| format!("{label} target authority admission failed: {error}"))?;

    group.testing_arm_mutation_fault(fault);
    let first = group.testing_execute_admitted_seed_callback(&command).await;
    if group.testing_mutation_fault_hits() != 1 {
        return Err(format!(
            "{label} did not hit the exact TDS submission checkpoint; callback={first:?}"
        )
        .into());
    }
    if let Err(error) = &first
        && error.to_string().contains("bounded callback budget")
    {
        return Err(format!("{label} degraded to a convergence deadline: {error}").into());
    }
    group
        .testing_assert_source_build_success_not_published(&command)
        .await?;
    group
        .testing_execute_admitted_seed_callback_with_retry(&command)
        .await
        .map_err(|error| format!("{label} safe callback retry failed: {error}"))?;
    if group.testing_seed_dispatch_record()?.is_some() {
        return Err(format!("{label} retained resolved dispatch ambiguity").into());
    }
    let attempts = group.testing_seed_dispatch_count()?;
    match fault {
        PublicMutationFault::SeedAcceptedReplyLost if attempts != 1 => {
            return Err(format!("{label} repeated an accepted ADD DATABASE request").into());
        }
        PublicMutationFault::SeedBeforeRequestSubmission
        | PublicMutationFault::SeedDefiniteSqlRejection
            if attempts != 2 =>
        {
            return Err(format!("{label} did not perform exactly one proven-safe retry").into());
        }
        PublicMutationFault::SeedAfterRequestSubmission if !(1..=2).contains(&attempts) => {
            return Err(format!("{label} produced an invalid submission count {attempts}").into());
        }
        _ => {}
    }

    let topology = launched
        .finalize_prepared_availability_group(&prepared)
        .await?;
    let peer = group.target_build_command(2, format!("{label}-peer"), format!("{label}-peer"))?;
    group
        .testing_admit_target_build_authority_only(&peer)
        .await
        .map_err(|error| format!("{label} peer authority admission failed: {error}"))?;
    group
        .testing_execute_admitted_seed_callback_with_retry(&peer)
        .await
        .map_err(|error| format!("{label} peer callback failed: {error}"))?;
    let marker = launched
        .commit_replicated_marker(&topology.evidence)
        .await?;
    if marker.readable_ordinals != [1, 2, 3] {
        return Err(format!("{label} did not converge the exact seeded topology").into());
    }
    group.shutdown_with_coordinator(&cleanup).await?;
    let evidence = launched.cleanup_with_coordinator(&cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err(format!("{label} cleanup did not remove exact owned resources").into());
    }
    assert_removed(root);
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

async fn exercise_configuration_fault(
    base_root: &Path,
    fault: PublicMutationFault,
    label: &str,
) -> Result<(), TestError> {
    let root = base_root.join(label);
    cleanup_three_replica_fixture(&root)?;
    let config = FixtureConfig::for_test_fixture(&root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    if fault == PublicMutationFault::SourceRecoveryCompletion {
        launched
            .testing_set_primary_model_recovery_simple()
            .await
            .map_err(|error| format!("source-recovery setup failed: {error}"))?;
    }
    if launched.journal().native_phase != NativeTopologyPhase::Prepared
        || launched.journal().native_binding.is_some()
    {
        return Err(format!("{label} did not retain the exact prepared native phase").into());
    }

    let injected = PublicFaultInjection::new(fault);
    let first_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let first = PublicMssqlGroup::from_prepared_with_fault(
        &root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &first_cleanup,
        injected.clone(),
        &format!("phase4-{label}-interrupted"),
    )
    .await;
    let error = match first {
        Err(error) => error,
        Ok(group) => {
            if let Err(error) = group.shutdown_with_coordinator(&first_cleanup).await {
                return hosted_witness::block_disposal(
                    &mut launched,
                    &format!("{label} unexpected success could not be drained: {error}"),
                );
            }
            return Err(format!("{label} unexpectedly published configuration success").into());
        }
    };
    if error.agent_termination_unconfirmed() {
        return hosted_witness::block_disposal(
            &mut launched,
            &format!("{label} interrupted startup termination unconfirmed: {error}"),
        );
    }
    let expected_effect = fault.effect_label();
    let diagnostic = error.to_string();
    if injected.hit_count() != 1
        || !diagnostic.contains(expected_effect)
        || !diagnostic.contains("Unreachable")
    {
        return Err(format!("{label} did not fail at its exact injected boundary: {error}").into());
    }
    let dispatches = injected.mutation_dispatch_count();
    if dispatches == 0 {
        return Err(format!("{label} did not dispatch its injected SQL mutation").into());
    }
    if fault == PublicMutationFault::SourceRecoveryCompletion
        && launched.testing_source_recovery_model().await? != "FULL"
    {
        return Err(format!("{label} did not retain the completed native recovery change").into());
    }
    let fixture_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let cleanup = launched.cleanup_with_coordinator(&fixture_cleanup)?;
    if cleanup.removed_container_ids.len() != 3 || cleanup.removed_network_id.is_empty() {
        return Err(format!("{label} cleanup did not remove the exact owned topology").into());
    }
    assert_removed(&root);
    cleanup_three_replica_fixture(&root)?;
    assert_removed(&root);
    Ok(())
}

async fn converge_minimal_seeded_topology(
    group: &PublicMssqlGroup,
    launched: &mut LaunchedMembers,
    prepared: &kuberic_mssql_tests::three_replica::PreparedAvailabilityGroup,
    label: &str,
) -> Result<(), TestError> {
    let sequence = group.bootstrap_configuration_sequence().await?;
    wait_for_exact_reports(group, sequence, launched.kuberic_convergence_timeout()).await?;
    let winner =
        group.target_build_command(1, format!("{label}-build-2"), format!("{label}-build-2"))?;
    group
        .testing_admit_target_build_authority_only(&winner)
        .await
        .map_err(|error| format!("{label} winner authority admission failed: {error}"))?;
    group
        .testing_execute_admitted_seed_callback_with_retry(&winner)
        .await
        .map_err(|error| format!("{label} same-root seed retry failed: {error}"))?;
    let topology = launched
        .finalize_prepared_availability_group(prepared)
        .await?;
    let peer =
        group.target_build_command(2, format!("{label}-build-3"), format!("{label}-build-3"))?;
    group
        .testing_admit_target_build_authority_only(&peer)
        .await
        .map_err(|error| format!("{label} peer authority admission failed: {error}"))?;
    group
        .testing_execute_admitted_seed_callback_with_retry(&peer)
        .await
        .map_err(|error| format!("{label} native-only peer retry failed: {error}"))?;
    if launched.journal().native_phase != NativeTopologyPhase::Converged
        || launched.journal().native_binding.as_ref() != Some(&topology.binding)
    {
        return Err(format!("{label} did not journal the exact converged native phase").into());
    }
    let marker = launched
        .commit_replicated_marker(&topology.evidence)
        .await?;
    if marker.primary_ordinal != topology.evidence.primary_ordinal
        || marker.readable_ordinals != [1, 2, 3]
    {
        return Err(format!("{label} retry did not converge the exact native topology").into());
    }
    Ok(())
}

async fn exercise_seed_faults(root: &Path) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &cleanup,
    )
    .await?;
    let sequence = group.bootstrap_configuration_sequence().await?;
    wait_for_exact_reports(&group, sequence, launched.kuberic_convergence_timeout()).await?;
    let command =
        group.target_build_command(1, "phase4-seed-boundaries", "phase4-seed-boundaries")?;
    group
        .testing_admit_target_build_authority_only(&command)
        .await
        .map_err(|error| format!("seed-boundary target authority admission failed: {error}"))?;

    let (intent_entered_tx, intent_entered_rx) = tokio::sync::oneshot::channel();
    let (_intent_release_tx, intent_release_rx) = tokio::sync::oneshot::channel();
    group.testing_gate_next_seed_intent(intent_entered_tx, intent_release_rx)?;
    let mut intent_callback = group.testing_spawn_admitted_seed_callback(&command)?;
    await_checkpoint(intent_entered_rx, &mut intent_callback, "seed intent").await?;
    intent_callback.abort();
    let _ = intent_callback.await;
    assert_seed_intent_owner(&group, &command, "seed-intent interruption")?;
    group
        .testing_assert_source_build_success_not_published(&command)
        .await?;

    let (association_entered_tx, association_entered_rx) = tokio::sync::oneshot::channel();
    let (_association_release_tx, association_release_rx) = tokio::sync::oneshot::channel();
    group.testing_gate_next_seed_compaction(association_entered_tx, association_release_rx)?;
    let mut association_callback = group.testing_spawn_admitted_seed_callback(&command)?;
    await_checkpoint(
        association_entered_rx,
        &mut association_callback,
        "target association",
    )
    .await?;
    association_callback.abort();
    let _ = association_callback.await;
    let retained = group
        .testing_seed_dispatch_record()?
        .ok_or("target-association interruption lost dispatched seed authority")?;
    if !matches!(
        retained.phase(),
        SeedDispatchPhase::Submitting | SeedDispatchPhase::Accepted | SeedDispatchPhase::Ambiguous
    ) {
        return Err("target-association interruption changed dispatched authority".into());
    }
    group
        .testing_assert_source_build_success_not_published(&command)
        .await?;

    let (completion_entered_tx, completion_entered_rx) = tokio::sync::oneshot::channel();
    let (_completion_release_tx, completion_release_rx) = tokio::sync::oneshot::channel();
    group.testing_gate_next_seed_completion(completion_entered_tx, completion_release_rx)?;
    let mut completion_entered_rx = completion_entered_rx;
    let completion_deadline = tokio::time::Instant::now() + launched.kuberic_convergence_timeout();
    let mut completion_callback;
    loop {
        completion_callback = group.testing_spawn_admitted_seed_callback(&command)?;
        tokio::select! {
            entered = &mut completion_entered_rx => {
                entered.map_err(|_| "native completion callback ended before its checkpoint")?;
                break;
            }
            result = &mut completion_callback => {
                let retryable = matches!(
                    result,
                    Ok(Err(ref status))
                        if status.message().contains("observation is unavailable: Unsupported")
                            || status.message().contains("observation is unavailable: Malformed")
                );
                if !retryable || tokio::time::Instant::now() >= completion_deadline {
                    return Err(format!(
                        "native completion callback completed before interruption: {result:?}"
                    )
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ = tokio::time::sleep_until(completion_deadline) => {
                return Err("native completion checkpoint exceeded its bounded wait".into());
            }
        }
    }
    completion_callback.abort();
    let _ = completion_callback.await;
    group
        .testing_assert_source_build_success_not_published(&command)
        .await?;

    group
        .testing_execute_admitted_seed_callback(&command)
        .await
        .map_err(|error| format!("native-complete callback retry failed: {error}"))?;
    group
        .testing_execute_admitted_seed_callback(&command)
        .await
        .map_err(|error| format!("lost callback reply replay failed: {error}"))?;
    if group.testing_seed_dispatch_count()? != 1 || group.testing_seed_dispatch_record()?.is_some()
    {
        return Err("seed-boundary retries repeated dispatch or retained resolved state".into());
    }

    let peer = group.target_build_command(2, "phase4-seed-peer", "phase4-seed-peer")?;
    group
        .testing_admit_target_build_authority_only(&peer)
        .await
        .map_err(|error| format!("seed-boundary peer authority admission failed: {error}"))?;
    group
        .testing_execute_admitted_seed_callback(&peer)
        .await
        .map_err(|error| format!("seed-boundary peer callback failed: {error}"))?;
    let topology = launched
        .finalize_prepared_availability_group(&prepared)
        .await?;
    let marker = launched
        .commit_replicated_marker(&topology.evidence)
        .await?;
    if marker.readable_ordinals != [1, 2, 3] {
        return Err("seed-boundary retry did not converge all exact members".into());
    }
    group.shutdown_with_coordinator(&cleanup).await?;
    let evidence = launched.cleanup_with_coordinator(&cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err("seed-boundary cleanup did not remove exact owned resources".into());
    }
    assert_removed(root);
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

async fn exercise_post_native_success_replay(root: &Path) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    let convergence = launched.kuberic_convergence_timeout();
    let first_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let first = start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        convergence,
        launched.complete_deadline(),
        &first_cleanup,
    )
    .await;
    let first = match first {
        Ok(group) => group,
        Err(error) if error.agent_termination_unconfirmed() => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!("post-native-success startup termination unconfirmed: {error}"),
            );
        }
        Err(error) => return Err(error.into()),
    };
    let stable_resource = first.resource_uid.clone();
    let stable_policy = first.effective_policy.clone();
    let stable_configuration = first.configuration.clone();
    let stable_pods = first.pods.each_ref().map(|pod| {
        (
            pod.identity.clone(),
            pod.stable_role,
            pod.pod_uid.clone(),
            pod.pvc_uid.clone(),
        )
    });
    let first_result = async {
        let sequence = first.bootstrap_configuration_sequence().await?;
        wait_for_exact_reports(&first, sequence, convergence).await?;
        let command = first.target_build_command(
            1,
            "post-native-success-build",
            "post-native-success-build",
        )?;
        first
            .testing_admit_target_build_authority_only(&command)
            .await
            .map_err(|error| format!("post-native-success target admission failed: {error}"))?;
        first
            .testing_execute_admitted_seed_callback_with_retry(&command)
            .await
            .map_err(|error| format!("post-native-success callback failed: {error}"))?;
        if first.testing_seed_dispatch_count()? != 1 {
            return Err(TestError::from(
                "post-native-success checkpoint did not execute exactly one ADD DATABASE",
            ));
        }
        first
            .testing_assert_source_build_success_not_published(&command)
            .await?;
        let topology = launched
            .finalize_prepared_availability_group(&prepared)
            .await?;
        let marker = launched
            .commit_replicated_marker(&topology.evidence)
            .await?;

        first.testing_abort_seed_source();
        let rejected = first.testing_execute_admitted_seed_callback(&command).await;
        match rejected {
            Err(error)
                if error.code() == tonic::Code::FailedPrecondition
                    && error.message() == "bootstrap source replicator is unavailable" => {}
            other => return Err(format!(
                "post-native-success aborted source did not reject its callback exactly: {other:?}"
            )
            .into()),
        }
        if first.testing_seed_dispatch_count()? != 1 {
            return Err("post-native-success aborted source dispatched additional SQL".into());
        }
        Ok((command, marker))
    }
    .await;
    let source_cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    if let Err(error) = first.shutdown_with_coordinator(&source_cleanup).await {
        return hosted_witness::block_disposal(
            &mut launched,
            &format!("post-native-success source shutdown failed: {error}"),
        );
    }
    let (command, marker) = first_result?;

    let replacement_startup_cleanup =
        CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let replacement = start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        convergence,
        launched.complete_deadline(),
        &replacement_startup_cleanup,
    )
    .await;
    let replacement = match replacement {
        Ok(group) => group,
        Err(error) if error.agent_termination_unconfirmed() => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!("post-native-success replacement termination unconfirmed: {error}"),
            );
        }
        Err(error) => return Err(error.into()),
    };
    let replay: Result<(), TestError> = async {
        if replacement.resource_uid != stable_resource
            || replacement.effective_policy != stable_policy
            || replacement.configuration != stable_configuration
            || replacement.pods.iter().zip(&stable_pods).any(
                |(pod, (identity, role, pod_uid, pvc_uid))| {
                    &pod.identity != identity
                        || pod.stable_role != *role
                        || &pod.pod_uid != pod_uid
                        || &pod.pvc_uid != pvc_uid
                },
            )
        {
            return Err("post-native-success replacement changed exact durable authority".into());
        }
        replacement
            .testing_admit_target_build_authority_only(&command)
            .await
            .map_err(|error| {
                format!("post-replacement native callback admission failed: {error}")
            })?;
        replacement
            .testing_execute_admitted_seed_callback_with_retry(&command)
            .await
            .map_err(|error| {
                format!("native callback replay after host replacement failed: {error}")
            })?;
        if replacement.testing_seed_dispatch_count()? != 0 {
            return Err("post-native-success replay issued a second ADD DATABASE".into());
        }
        if replacement.testing_seed_dispatch_record()?.is_some() {
            return Err("post-native-success replay retained resolved seed authority".into());
        }
        let verified_marker = launched.verify_replicated_marker(&marker).await?;
        if verified_marker != marker || verified_marker.readable_ordinals != [1, 2, 3] {
            return Err("post-native-success replay changed the exact native result".into());
        }
        replacement
            .testing_assert_source_build_success_not_published(&command)
            .await?;
        Ok(())
    }
    .await;
    let replacement_cleanup =
        CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    if let Err(error) = replacement
        .shutdown_with_coordinator(&replacement_cleanup)
        .await
    {
        return hosted_witness::block_disposal(
            &mut launched,
            &format!("post-native-success replacement shutdown failed: {error}"),
        );
    }
    replay?;
    let evidence = launched.cleanup_with_coordinator(&replacement_cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err("post-native-success cleanup did not remove exact owned resources".into());
    }
    assert_removed(root);
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

async fn await_checkpoint(
    entered: tokio::sync::oneshot::Receiver<()>,
    callback: &mut tokio::task::JoinHandle<Result<(), tonic::Status>>,
    label: &str,
) -> Result<(), TestError> {
    tokio::select! {
        entered = entered => entered
            .map_err(|_| format!("{label} callback ended before its checkpoint").into()),
        result = callback => Err(format!("{label} callback completed before interruption: {result:?}").into()),
        _ = tokio::time::sleep(Duration::from_secs(120)) => {
            Err(format!("{label} checkpoint exceeded its bounded wait").into())
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TargetEvidenceWindow {
    PreAssociation,
    InFlightSeeding,
    PostNativePreReply,
}

impl TargetEvidenceWindow {
    const fn label(self) -> &'static str {
        match self {
            Self::PreAssociation => "pre-association",
            Self::InFlightSeeding => "in-flight-seeding",
            Self::PostNativePreReply => "post-native-pre-reply",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TargetEvidenceFault {
    SqlProcessRestart,
    LineageMismatch,
}

impl TargetEvidenceFault {
    const fn label(self) -> &'static str {
        match self {
            Self::SqlProcessRestart => "sql-process-restart",
            Self::LineageMismatch => "lineage-mismatch",
        }
    }
}

async fn await_owned_checkpoint(
    entered: tokio::sync::oneshot::Receiver<()>,
    callback_owner: &mut Option<tokio::task::JoinHandle<Result<(), tonic::Status>>>,
    label: &str,
) -> Result<(), TestError> {
    let completed = tokio::select! {
        entered = entered => {
            entered.map_err(|_| format!("{label} checkpoint sender disappeared"))?;
            None
        }
        result = callback_owner.as_mut().expect("callback owner armed") => Some(result),
        _ = tokio::time::sleep(Duration::from_secs(120)) => {
            return Err(format!("{label} checkpoint exceeded its bounded wait").into());
        }
    };
    if let Some(result) = completed {
        callback_owner.take();
        return Err(format!("{label} callback completed before its checkpoint: {result:?}").into());
    }
    Ok(())
}

#[tokio::test]
async fn owned_checkpoint_wait_preserves_callback_ownership() {
    let (_entered, receiver) = tokio::sync::oneshot::channel();
    let mut owner = Some(tokio::spawn(async { Ok(()) }));
    let error = await_owned_checkpoint(receiver, &mut owner, "early completion")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("callback completed before"));
    assert!(owner.is_none());

    let (entered, receiver) = tokio::sync::oneshot::channel();
    drop(entered);
    let mut owner = Some(tokio::spawn(std::future::pending::<
        Result<(), tonic::Status>,
    >()));
    let error = await_owned_checkpoint(receiver, &mut owner, "lost checkpoint")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("checkpoint sender disappeared"));
    let callback = owner.take().expect("pending callback owner retained");
    callback.abort();
    assert!(callback.await.unwrap_err().is_cancelled());
}

async fn exercise_target_host_evidence_fault(
    base_root: &Path,
    window: TargetEvidenceWindow,
    fault: TargetEvidenceFault,
) -> Result<(), TestError> {
    if matches!(fault, TargetEvidenceFault::LineageMismatch)
        && !matches!(window, TargetEvidenceWindow::PostNativePreReply)
    {
        return Err("synthetic lineage must retain all checkpoints before the final report".into());
    }
    let label = format!("{}-{}", window.label(), fault.label());
    let root = base_root.join(&label);
    cleanup_three_replica_fixture(&root)?;
    let config = FixtureConfig::for_test_fixture(&root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = start_prepared_public_group_with_retry(
        &root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &cleanup,
    )
    .await;
    let group = match group {
        Ok(group) => group,
        Err(error) if error.agent_termination_unconfirmed() => {
            return hosted_witness::block_disposal(
                &mut launched,
                &format!("{label} startup termination unconfirmed: {error}"),
            );
        }
        Err(error) => return Err(error.into()),
    };
    let mut callback_owner = None;
    let outcome: Result<(), TestError> = async {
        let sequence = group.bootstrap_configuration_sequence().await?;
        wait_for_exact_reports(&group, sequence, launched.kuberic_convergence_timeout()).await?;
        let command = group.target_build_command(
            1,
            format!("target-evidence-{label}"),
            format!("target-evidence-{label}"),
        )?;
        group
            .testing_admit_target_build_authority_only(&command)
            .await
            .map_err(|error| format!("{label} target authority admission failed: {error}"))?;

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        match window {
            TargetEvidenceWindow::PreAssociation => {
                group.testing_gate_next_seed_intent(entered_tx, release_rx)?;
            }
            TargetEvidenceWindow::InFlightSeeding => {
                group.testing_gate_seed_after_request(entered_tx, release_rx)?;
            }
            TargetEvidenceWindow::PostNativePreReply => {
                group.testing_gate_next_target_evidence(entered_tx, release_rx)?;
            }
        }
        let preceding_checkpoints = if matches!(fault, TargetEvidenceFault::LineageMismatch) {
            let (intent_entered, intent_receiver) = tokio::sync::oneshot::channel();
            let (intent_release, intent_wait) = tokio::sync::oneshot::channel();
            group.testing_gate_next_seed_intent(intent_entered, intent_wait)?;
            let (request_entered, request_receiver) = tokio::sync::oneshot::channel();
            let (request_release, request_wait) = tokio::sync::oneshot::channel();
            group.testing_gate_seed_after_request(request_entered, request_wait)?;
            Some([
                ("seed-intent", intent_receiver, intent_release),
                ("after-request", request_receiver, request_release),
            ])
        } else {
            None
        };
        callback_owner = Some(group.testing_spawn_admitted_seed_callback(&command)?);
        if let Some(checkpoints) = preceding_checkpoints {
            for (checkpoint, entered, release) in checkpoints {
                let checkpoint_label = format!("{label} {checkpoint}");
                await_owned_checkpoint(entered, &mut callback_owner, &checkpoint_label).await?;
                group
                    .testing_assert_source_build_success_not_published(&command)
                    .await?;
                if callback_owner.as_ref().expect("callback owner armed").is_finished() {
                    return Err(format!("{checkpoint_label} callback was not retained").into());
                }
                release
                    .send(())
                    .map_err(|_| format!("{checkpoint_label} checkpoint was not retained"))?;
            }
        }
        await_owned_checkpoint(entered_rx, &mut callback_owner, &label).await?;
        group
            .testing_assert_source_build_success_not_published(&command)
            .await?;
        let dispatches = group.testing_seed_dispatch_count()?;
        if callback_owner.as_ref().expect("callback owner armed").is_finished() {
            return Err(format!("{label} callback ended before fault initiation").into());
        }
        let mut verified_new_start = false;

        match fault {
            TargetEvidenceFault::SqlProcessRestart => {
                let frozen = group.testing_frozen_secondary_start_identity()?;
                let host_port = launched.restart_sql_process_for_test(1)?;
                let fresh = group
                    .testing_wait_for_fresh_secondary_observation(
                        &frozen,
                        host_port,
                        launched.kuberic_convergence_timeout(),
                    )
                    .await?;
                if fresh.instance.sqlserver_start_time == frozen {
                    return Err(
                        format!("{label} did not replace the target SQL start identity").into(),
                    );
                }
                verified_new_start = true;
            }
            TargetEvidenceFault::LineageMismatch => {
                group.testing_inject_target_lineage_mismatch()?;
            }
        }
        release_tx
            .send(())
            .map_err(|_| format!("{label} checkpoint was not retained"))?;
        let joined = tokio::time::timeout(
            launched.kuberic_convergence_timeout(),
            callback_owner.as_mut().expect("callback owner armed"),
        )
        .await
        .map_err(|_| format!("{label} callback did not terminate within its deadline"))?;
        callback_owner.take();
        let result = joined.map_err(|error| format!("{label} callback task failed: {error}"))?;
        let error = result
            .err()
            .ok_or_else(|| format!("{label} completed without current target-local evidence"))?;
        let diagnostic = error.to_string();
        match fault {
            TargetEvidenceFault::SqlProcessRestart
                if !target_restart_failure_is_fail_closed(
                    fault,
                    &error,
                    verified_new_start,
                    dispatches,
                    group.testing_seed_dispatch_count()?,
                ) =>
            {
                return Err(format!(
                    "{label} did not fail closed: verified_new_start={verified_new_start}, \
                     seed_dispatches_before={dispatches}, seed_dispatches_after={}; callback={error}",
                    group.testing_seed_dispatch_count()?,
                )
                .into());
            }
            TargetEvidenceFault::LineageMismatch if !diagnostic.contains("conflicts") => {
                return Err(
                    format!("{label} returned an unexpected lineage error: {error}").into(),
                );
            }
            _ => {}
        }
        if matches!(fault, TargetEvidenceFault::LineageMismatch) {
            group
                .testing_assert_source_build_success_not_published(&command)
                .await?;
        }
        Ok(())
    }
    .await;
    let callback_shutdown = if let Some(callback) = callback_owner.take() {
        callback.abort();
        match callback.await {
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(tonic::Status::internal(format!(
                "target callback cleanup failed: {error}"
            ))),
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => Err(tonic::Status::failed_precondition(
                "target callback completed after a failed evidence test",
            )),
        }
    } else {
        Ok(())
    };
    let outcome = combine_primary_and_shutdown(outcome, callback_shutdown);
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    if let Err(error) = group.shutdown_with_coordinator(&cleanup).await {
        return hosted_witness::block_disposal(
            &mut launched,
            &format!("{label} host shutdown failed: {error}"),
        );
    }
    outcome?;
    let evidence = launched.cleanup_with_coordinator(&cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err(format!("{label} cleanup did not remove exact owned resources").into());
    }
    assert_removed(&root);
    cleanup_three_replica_fixture(&root)?;
    assert_removed(&root);
    Ok(())
}

fn target_restart_failure_is_fail_closed(
    fault: TargetEvidenceFault,
    error: &tonic::Status,
    verified_new_start: bool,
    dispatches_before: usize,
    dispatches_after: usize,
) -> bool {
    matches!(fault, TargetEvidenceFault::SqlProcessRestart)
        && verified_new_start
        && dispatches_before <= dispatches_after
        && dispatches_after <= 1
        && error.code() == tonic::Code::Internal
        && matches!(
            error.message(),
            "application error: target SQL Server start identity is unsafe to continue"
                | "application error: SQL Server observation is unavailable: Unreachable"
                | "application error: SQL Server observation is unavailable: TimedOut"
                | "application error: bootstrap convergence exceeded its bounded callback budget"
        )
}

#[test]
fn target_restart_failure_requires_real_drift_and_no_duplicate_seed() {
    let budget = tonic::Status::internal(
        "application error: bootstrap convergence exceeded its bounded callback budget",
    );
    for message in [
        budget.message(),
        "application error: target SQL Server start identity is unsafe to continue",
        "application error: SQL Server observation is unavailable: Unreachable",
        "application error: SQL Server observation is unavailable: TimedOut",
    ] {
        for (before, after) in [(0, 0), (0, 1), (1, 1)] {
            assert!(target_restart_failure_is_fail_closed(
                TargetEvidenceFault::SqlProcessRestart,
                &tonic::Status::internal(message),
                true,
                before,
                after,
            ));
        }
    }
    for (fault, error, verified, before, after) in [
        (
            TargetEvidenceFault::LineageMismatch,
            budget.clone(),
            true,
            1,
            1,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            budget.clone(),
            false,
            1,
            1,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            budget.clone(),
            true,
            1,
            2,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            budget.clone(),
            true,
            1,
            0,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            budget.clone(),
            true,
            2,
            2,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            tonic::Status::deadline_exceeded(budget.message()),
            true,
            1,
            1,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            tonic::Status::internal("unrelated operation is unsafe or unavailable"),
            true,
            1,
            1,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            tonic::Status::internal(
                "application error: SQL Server observation is unavailable: PermissionDenied",
            ),
            true,
            1,
            1,
        ),
        (
            TargetEvidenceFault::SqlProcessRestart,
            tonic::Status::internal("administrator query failed"),
            true,
            1,
            1,
        ),
    ] {
        assert!(!target_restart_failure_is_fail_closed(
            fault, &error, verified, before, after,
        ));
    }
}

async fn exercise_native_source_recovery_partial(
    root: &Path,
    label: &str,
) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    launched
        .testing_set_primary_model_recovery_simple()
        .await
        .map_err(|error| format!("{label} setup failed: {error}"))?;
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &cleanup,
    )
    .await?;
    if launched.testing_source_recovery_model().await? != "FULL" {
        return Err(format!("{label} did not converge the SQL-native recovery step").into());
    }
    let sequence = group.bootstrap_configuration_sequence().await?;
    wait_for_exact_reports(&group, sequence, launched.kuberic_convergence_timeout()).await?;
    group.shutdown_with_coordinator(&cleanup).await?;
    let evidence = launched.cleanup_with_coordinator(&cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err(format!("{label} cleanup did not remove exact owned resources").into());
    }
    assert_removed(root);
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

async fn exercise_native_sql_conflict(
    root: &Path,
    label: &str,
    availability_group: bool,
) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    if availability_group {
        launched
            .testing_create_unowned_availability_group()
            .await
            .map_err(|error| format!("{label} setup failed: {error}"))?;
    } else {
        launched
            .testing_create_unowned_source_database()
            .await
            .map_err(|error| format!("{label} setup failed: {error}"))?;
    }
    let classification_cleanup =
        CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let result = PublicMssqlGroup::from_prepared_with_coordinator(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &classification_cleanup,
    )
    .await;
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    match result {
        Err(error)
            if error
                .to_string()
                .contains("conflicts with the requested bootstrap") => {}
        Err(error) => {
            return Err(
                format!("{label} returned the wrong native classification: {error}").into(),
            );
        }
        Ok(group) => {
            group.shutdown_with_coordinator(&cleanup).await?;
            return Err(format!("{label} adopted an unowned SQL-native resource").into());
        }
    }
    let evidence = launched.cleanup_with_coordinator(&cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err(format!("{label} cleanup did not remove exact owned resources").into());
    }
    assert_removed(root);
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

async fn exercise_conflict_scenario(
    root: &Path,
    scenario: LicensedConflictScenario,
    label: &str,
    expected: &str,
) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let config = FixtureConfig::for_test_fixture(root)?;
    let mut launched = launch_three_members(config).await?;
    let prepared = launched.prepare_availability_group().await?;
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let group = start_prepared_public_group_with_retry(
        root,
        &launched.run,
        &prepared,
        &launched.members,
        launched.kuberic_convergence_timeout(),
        launched.complete_deadline(),
        &cleanup,
    )
    .await?;
    converge_minimal_seeded_topology(&group, &mut launched, &prepared, label).await?;
    let command = group.target_build_command(
        1,
        format!("{label}-no-late-success"),
        format!("{label}-no-late-success"),
    )?;
    group
        .testing_admit_target_build_authority_only(&command)
        .await
        .map_err(|error| format!("{label} target authority admission failed: {error}"))?;
    group
        .testing_assert_source_build_success_not_published(&command)
        .await?;

    let incarnation_snapshot =
        if scenario == LicensedConflictScenario::SqlProcessIncarnationReplacement {
            let frozen = group.testing_frozen_secondary_start_identity()?;
            let host_port = launched.restart_sql_process_for_test(1)?;
            let fresh = group
                .testing_wait_for_fresh_secondary_observation(
                    &frozen,
                    host_port,
                    launched.kuberic_convergence_timeout(),
                )
                .await?;
            if fresh.instance.sqlserver_start_time == frozen {
                return Err("SQL process replacement retained its frozen start identity".into());
            }
            Some(fresh)
        } else {
            None
        };

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    if let Some(snapshot) = incarnation_snapshot {
        group.testing_arm_incarnation_conflict(snapshot, entered_tx, release_rx)?;
    } else {
        group.testing_arm_licensed_conflict(scenario, entered_tx, release_rx)?;
    }
    let error = {
        let callback = async {
            if scenario == LicensedConflictScenario::StandaloneDatabaseIdentityOrRecovery {
                group
                    .testing_execute_primary_configuration_callback()
                    .await
                    .map_err(|error| tonic::Status::internal(error.to_string()))
            } else if scenario == LicensedConflictScenario::SqlProcessIncarnationReplacement {
                group
                    .testing_execute_secondary_admitted_validator()
                    .await
                    .map_err(|error| tonic::Status::internal(error.to_string()))
            } else {
                group.testing_execute_admitted_seed_callback(&command).await
            }
        };
        tokio::pin!(callback);
        tokio::select! {
            entered = entered_rx => entered
                .map_err(|_| format!("{label} callback ended before consuming its injected observation"))?,
            result = &mut callback => {
                return Err(format!("{label} callback was not genuinely delayed: {result:?}").into());
            }
            _ = tokio::time::sleep(Duration::from_secs(120)) => {
                return Err(format!("{label} callback did not consume its injected observation").into());
            }
        }
        group
            .testing_assert_source_build_success_not_published(&command)
            .await?;
        release_tx
            .send(())
            .map_err(|_| format!("{label} callback released before classification"))?;
        match callback.await {
            Err(error) => error,
            Ok(()) => {
                return Err(format!(
                    "{label} callback returned success for an injected terminal conflict"
                )
                .into());
            }
        }
    };
    group.testing_clear_licensed_conflict();
    if !error.to_string().contains(expected) {
        return Err(format!(
            "{label} returned the wrong terminal classification: {error}; expected {expected}"
        )
        .into());
    }
    group
        .testing_assert_source_build_success_never_published(&command, Duration::from_secs(2))
        .await?;
    group.shutdown_with_coordinator(&cleanup).await?;
    let evidence = launched.cleanup_with_coordinator(&cleanup)?;
    if evidence.removed_container_ids.len() != 3 || evidence.removed_network_id.is_empty() {
        return Err(format!("{label} cleanup did not remove exact owned resources").into());
    }
    assert_removed(root);
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

fn finish_runtime_outcome(
    root: &Path,
    launched: LaunchedMembers,
    outcome: RuntimeOutcome,
    cleanup: &CleanupCoordinator<SystemCleanupClock>,
) -> Result<(), TestError> {
    let fixture_cleanup = launched.cleanup_with_coordinator(cleanup);
    finish_runtime_cleanup(root, outcome, fixture_cleanup)
}

fn finish_runtime_cleanup(
    root: &Path,
    outcome: RuntimeOutcome,
    fixture_cleanup: Result<NativeCleanupEvidence, CombinedFixtureError>,
) -> Result<(), TestError> {
    match (outcome, fixture_cleanup) {
        (RuntimeOutcome::Completed(Ok(Ok(()))), Ok(cleanup)) => {
            assert_eq!(cleanup.removed_container_ids.len(), 3);
            assert!(!cleanup.removed_network_id.is_empty());
            assert_removed(root);
            Ok(())
        }
        (RuntimeOutcome::Completed(Ok(Err(error))), cleanup) => {
            if let Err(cleanup) = cleanup {
                Err(combined_error(error, cleanup))
            } else {
                Err(error)
            }
        }
        (RuntimeOutcome::Completed(Err(panic)), cleanup) => {
            if let Err(cleanup) = cleanup {
                let primary = panic_message(&panic);
                panic!(
                    "three-replica lifecycle panicked: {primary}; cleanup also failed: {cleanup}"
                );
            }
            std::panic::resume_unwind(panic);
        }
        (RuntimeOutcome::Completed(Ok(Ok(()))), Err(error)) => Err(Box::new(error)),
        (RuntimeOutcome::Interrupted(signal), Ok(_)) => {
            Err(format!("three-replica lifecycle interrupted by {signal:?}").into())
        }
        (RuntimeOutcome::Interrupted(signal), Err(cleanup)) => Err(format!(
            "three-replica lifecycle interrupted by {signal:?}; cleanup also failed: {cleanup}"
        )
        .into()),
    }
}

enum RuntimeOutcome {
    Completed(Result<Result<(), TestError>, Box<dyn std::any::Any + Send + 'static>>),
    Interrupted(HandledCancellationSignal),
}

impl RuntimeOutcome {
    fn description(&self) -> String {
        match self {
            Self::Completed(Ok(Ok(()))) => "runtime completed".to_owned(),
            Self::Completed(Ok(Err(error))) => format!("runtime failed: {error}"),
            Self::Completed(Err(panic)) => format!("runtime panicked: {}", panic_message(panic)),
            Self::Interrupted(signal) => format!("runtime interrupted by {signal:?}"),
        }
    }
}

fn combined_error(primary: TestError, cleanup: CombinedFixtureError) -> TestError {
    format!("{primary}; cleanup also failed: {cleanup}").into()
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

fn spawn_live_child(root: &Path, ready: Option<&Path>) -> Child {
    spawn_test_child(root, "happy-path", ready, None)
}

fn spawn_restart_child(root: &Path) -> Child {
    spawn_test_child(root, "same-root-restart", None, None)
}

fn spawn_test_child(
    root: &Path,
    mode: &str,
    ready: Option<&Path>,
    checkpoint: Option<&str>,
) -> Child {
    test_child_command(root, mode, ready, checkpoint)
        .spawn()
        .expect("spawn exact live child")
}

fn test_child_command(
    root: &Path,
    mode: &str,
    ready: Option<&Path>,
    checkpoint: Option<&str>,
) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("current live test executable"));
    command
        .arg(LIFECYCLE_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("KUBERIC_MSSQL_THREE_REPLICA_ROOT", root)
        .env_remove(CHILD_MODE_ENV)
        .env(CHILD_MODE_ENV, mode)
        .env_remove("KUBERIC_MSSQL_SIGNAL_READY_FILE")
        .env_remove("KUBERIC_MSSQL_FAULT_CHECKPOINT")
        .env_remove(FAULT_HIT_ENV)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if let Some(ready) = ready {
        command.env("KUBERIC_MSSQL_SIGNAL_READY_FILE", ready);
    }
    if let Some(checkpoint) = checkpoint {
        command.env("KUBERIC_MSSQL_FAULT_CHECKPOINT", checkpoint);
    }
    command
}

fn spawn_live_child_with_fault(root: &Path, checkpoint: &str, receipt: &Path) -> Child {
    let mode = if checkpoint == "fail-after-agent-restart" {
        "same-root-restart"
    } else {
        "happy-path"
    };
    test_child_command(root, mode, None, Some(checkpoint))
        .env(FAULT_HIT_ENV, receipt)
        .spawn()
        .expect("spawn checkpoint fault child")
}

fn spawn_cleanup_child(root: &Path) -> Child {
    let binary = std::env::current_exe()
        .expect("current live test executable")
        .parent()
        .and_then(Path::parent)
        .expect("test executable target directory")
        .join("mssql-three-replica-fixture");
    Command::new(binary)
        .arg("cleanup")
        .arg("--root")
        .arg(root)
        .env("KUBERIC_MSSQL_THREE_REPLICA_ROOT", root)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn exact cleanup process")
}

fn fault_checkpoint(expected: &str) -> bool {
    if std::env::var("KUBERIC_MSSQL_FAULT_CHECKPOINT").as_deref() != Ok(expected) {
        return false;
    }
    if let Some(path) = std::env::var_os(FAULT_HIT_ENV) {
        fs::write(path, expected).expect("record reached fault checkpoint");
    }
    true
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

fn wait_for_child_with_cleanup(child: &mut Child, timeout: Duration, root: &Path) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll live child") {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill timed-out live child");
            let status = child.wait().expect("reap timed-out live child");
            let mut cleanup = spawn_cleanup_child(root);
            assert!(
                wait_for_child(&mut cleanup, Duration::from_secs(240)).success(),
                "exact cleanup after timed-out live child"
            );
            return status;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn load_journal_if_present(root: &Path) -> Option<OwnershipJournal> {
    let bytes = fs::read(root.join("ownership.json")).ok()?;
    OwnershipJournal::from_json(&bytes).ok()
}

fn assert_exact_reports(
    group: &PublicMssqlGroup,
    reports: &[proto::AgentStatusReport; 3],
    configuration_sequence: i64,
) -> Result<(), TestError> {
    for (pod, report) in group.pods.iter().zip(reports) {
        let expected_call = match pod.stable_role {
            ReplicaRole::Primary => PublicV1CallKind::InitialPrimary,
            ReplicaRole::ActiveSecondary => PublicV1CallKind::SecondaryEpochAdvance,
            _ => return Err("public Kuberic pod has an unstable role".into()),
        };
        let public = normalize_public_v1_agent_status_report(report.clone())?;
        let storage_matches = matches!(
            &public.storage_incarnation.storage,
            ReplicaStorageIdentity::Persistent { pod_uid, pvc_uid }
                if pod_uid == &pod.pod_uid && pvc_uid == &pod.pvc_uid
        );
        let configuration_completed = public.completed_operations.iter().any(|completion| {
            completion.current_configuration_id.as_ref()
                == Some(&group.configuration.configuration_id)
                && completion.call_kind == expected_call
                && completion.result == PublicV1OperationResult::Succeeded
        });
        if public.resource_uid != group.resource_uid
            || public.replica != pod.identity
            || public.process_session_id != pod.session
            || !storage_matches
            || public.report_sequence == 0
            || !configuration_completed
            || public.progress.role != PublicProgressRole::Recovery
            || public.progress.first != 0
            || public.progress.last != configuration_sequence
            || public.read_status != AccessStatus::NotPrimary
            || public.write_status != AccessStatus::NotPrimary
            || public.role != ReplicaRole::None
        {
            return Err(format!(
                "Kuberic report differs from the exact fenced topology: ordinal={}, \
                 resource_uid={}, identity={:?}, session={}, storage={:?}, \
                 report_sequence={}, configuration_completed={}, \
                 progress_role={:?}, first_progress={}, last_progress={}, expected_progress={}, \
                 expected_session={}, storage_matches={}, \
                 read_status={:?}, write_status={:?}, role={:?}",
                pod.ordinal,
                public.resource_uid,
                public.replica,
                public.process_session_id,
                public.storage_incarnation.storage,
                public.report_sequence,
                configuration_completed,
                public.progress.role,
                public.progress.first,
                public.progress.last,
                configuration_sequence,
                pod.session,
                storage_matches,
                public.read_status,
                public.write_status,
                public.role,
            )
            .into());
        }
    }
    Ok(())
}

async fn wait_for_exact_reports(
    group: &PublicMssqlGroup,
    configuration_sequence: i64,
    timeout: Duration,
) -> Result<[proto::AgentStatusReport; 3], TestError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_error = None::<String>;
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or_else(|| {
                last_error
                    .clone()
                    .unwrap_or_else(|| "public report convergence exceeded its deadline".into())
            })?;
        let reports = tokio::time::timeout(remaining, group.reports_bracketed())
            .await
            .map_err(|_| {
                last_error.clone().unwrap_or_else(|| {
                    "public bracketed reporting exceeded its deadline".to_owned()
                })
            })??;
        match assert_exact_reports(group, &reports, configuration_sequence) {
            Ok(()) => return Ok(reports),
            Err(error) => {
                let message = error.to_string();
                if last_error.is_none() {
                    eprintln!("waiting for exact Public V1 reports: {message}");
                }
                last_error = Some(message);
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .ok_or_else(|| last_error.clone().unwrap_or_else(|| error.to_string()))?;
                tokio::time::sleep(remaining.min(Duration::from_millis(20))).await;
            }
        }
    }
}

fn assert_removed(root: &Path) {
    let store = JournalStore::initialize(root).expect("cleanup journal root");
    let journal = store
        .load::<kuberic_mssql_tests::three_replica::OwnershipJournal>()
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
