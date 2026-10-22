//! Independent native predicates, not qualification of the blocked compound public journeys.

use super::*;
use kuberic_mssql::kuberic::{KubericAdapterError, NativeSourceFoundationEvidence};
use kuberic_mssql::{NativeRole, ObservationFailureKind};
use kuberic_mssql_tests::three_replica::NativeSourceFoundation;

#[derive(Clone, Copy)]
enum NativeCase {
    SimpleRecovery,
    ForeignSourceDatabase,
    ForeignAvailabilityGroup,
    ReplyLoss(PublicMutationFault),
}

macro_rules! native_case {
    ($name:ident, $case:expr) => {
        #[test]
        #[ignore = "native-only SQL foundation predicate; requires an explicitly licensed owned three-member fixture"]
        fn $name() {
            run_phase4_test(|root| async move {
                exercise(&root.join(stringify!($name)), $case).await
            });
        }
    };
}

native_case!(
    native_source_foundation_repairs_simple_recovery_to_full,
    NativeCase::SimpleRecovery
);
native_case!(
    native_source_foundation_rejects_foreign_source_database,
    NativeCase::ForeignSourceDatabase
);
native_case!(
    native_source_foundation_rejects_foreign_availability_group,
    NativeCase::ForeignAvailabilityGroup
);
native_case!(
    native_source_foundation_reply_loss_source_database_creation_intent,
    NativeCase::ReplyLoss(PublicMutationFault::SourceDatabaseCreationIntentCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_database_creation,
    NativeCase::ReplyLoss(PublicMutationFault::SourceDatabaseCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_database_creation_intent_removal,
    NativeCase::ReplyLoss(PublicMutationFault::SourceDatabaseCreationIntentRemovalCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_recovery,
    NativeCase::ReplyLoss(PublicMutationFault::SourceRecoveryCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_ownership,
    NativeCase::ReplyLoss(PublicMutationFault::SourceOwnershipCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_marker_table,
    NativeCase::ReplyLoss(PublicMutationFault::SourceMarkerTableCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_full_backup,
    NativeCase::ReplyLoss(PublicMutationFault::SourceFullBackupCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_log_backup,
    NativeCase::ReplyLoss(PublicMutationFault::SourceLogBackupCompletion)
);
native_case!(
    native_source_foundation_reply_loss_source_backup_verification,
    NativeCase::ReplyLoss(PublicMutationFault::SourceBackupVerificationCompletion)
);
native_case!(
    native_source_foundation_reply_loss_ag_ownership_intent,
    NativeCase::ReplyLoss(PublicMutationFault::AvailabilityGroupIntentCompletion)
);
native_case!(
    native_source_foundation_reply_loss_ag_creation_intent,
    NativeCase::ReplyLoss(PublicMutationFault::AvailabilityGroupCreationIntentCompletion)
);
native_case!(
    native_source_foundation_reply_loss_ag_creation,
    NativeCase::ReplyLoss(PublicMutationFault::AvailabilityGroupCompletion)
);
native_case!(
    native_source_foundation_reply_loss_ag_binding,
    NativeCase::ReplyLoss(PublicMutationFault::AvailabilityGroupBindingCompletion)
);

async fn exercise(root: &Path, case: NativeCase) -> Result<(), TestError> {
    cleanup_three_replica_fixture(root)?;
    let mut cancellation = CancellationSignals::register()?;
    let mut launched = launch_three_members(FixtureConfig::for_test_fixture(root)?).await?;
    let mut driver = None;
    let outcome = {
        let deadline = tokio::time::Instant::from_std(launched.complete_deadline());
        let work = AssertUnwindSafe(async {
            tokio::time::timeout_at(deadline, async {
                let prepared = launched.prepare_availability_group().await?;
                require_prepared_only(&launched)?;
                match case {
                    NativeCase::SimpleRecovery
                    | NativeCase::ReplyLoss(PublicMutationFault::SourceRecoveryCompletion) => {
                        launched.testing_set_primary_model_recovery_simple().await?;
                    }
                    NativeCase::ForeignSourceDatabase => {
                        launched.testing_create_unowned_source_database().await?;
                    }
                    NativeCase::ForeignAvailabilityGroup => {
                        launched.testing_create_unowned_availability_group().await?;
                    }
                    NativeCase::ReplyLoss(_) => {}
                }
                let faults = match case {
                    NativeCase::ReplyLoss(fault) => PublicFaultInjection::new(fault),
                    _ => PublicFaultInjection::default(),
                };
                driver = Some(
                    NativeSourceFoundation::new(&mut launched, &prepared, faults.clone())
                        .await?,
                );
                let driver = driver.as_ref().expect("native adapter retained before Open");
                driver.open().await?;
                let result = tokio::time::timeout(
                    launched.kuberic_convergence_timeout(),
                    driver.prepare(),
                )
                .await?;
                match case {
                    NativeCase::SimpleRecovery => {
                        let evidence = result?;
                        validate_foundation(&launched, &evidence)?;
                        if launched.testing_source_recovery_model().await? != "FULL" {
                            return Err("native source did not actually recover to FULL".into());
                        }
                    }
                    NativeCase::ForeignSourceDatabase => {
                        if !matches!(
                            result,
                            Err(KubericAdapterError::NativeConvergenceConflict(
                                "standalone source database"
                            ))
                        ) {
                            return Err(format!(
                                "foreign native source returned the wrong classification: {result:?}"
                            )
                            .into());
                        }
                        if faults.mutation_dispatch_count() != 0 {
                            return Err("foreign source was mutated before rejection".into());
                        }
                    }
                    NativeCase::ForeignAvailabilityGroup => {
                        if !matches!(
                            result,
                            Err(KubericAdapterError::NativeConvergenceConflict(
                                "availability-group ownership marker"
                            ))
                        ) {
                            return Err(format!(
                                "foreign native AG returned the wrong classification: {result:?}"
                            )
                            .into());
                        }
                    }
                    NativeCase::ReplyLoss(fault) => {
                        if !matches!(
                            &result,
                            Err(KubericAdapterError::NativeMutationFailed {
                                effect,
                                kind: ObservationFailureKind::Unreachable,
                                server_code: None,
                            }) if *effect == fault.effect_label()
                        ) || faults.hit_count() != 1
                            || faults.mutation_dispatch_count() == 0
                        {
                            return Err(format!(
                                "native fault {:?} did not reach its exact real dispatch: \
                                 {result:?}; hits={}, dispatches={}",
                                fault,
                                faults.hit_count(),
                                faults.mutation_dispatch_count()
                            )
                            .into());
                        }
                        if fault == PublicMutationFault::SourceRecoveryCompletion
                            && launched.testing_source_recovery_model().await? != "FULL"
                        {
                            return Err(
                                "lost recovery reply did not retain actual FULL recovery".into()
                            );
                        }
                    }
                }
                require_prepared_only(&launched)?;
                Ok::<(), TestError>(())
            })
            .await?
        })
        .catch_unwind();
        await_native_outcome(work, cancellation.recv()).await
    };
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), CLEANUP_BUDGET);
    let outcome = if let Some(driver) = driver {
        let (contained, shutdown_error) = contain(&driver, &cleanup).await;
        if !contained {
            let primary = format!(
                "{}; native adapter containment unconfirmed: {}",
                outcome.description(),
                shutdown_error.unwrap_or_else(|| "unknown containment failure".to_owned())
            );
            launched.block_cleanup().map_err(|error| -> TestError {
                format!("{primary}; journal blocking also failed: {error}").into()
            })?;
            return Err(format!("{primary}; fixture disposal blocked").into());
        }
        if let Some(error) = shutdown_error {
            RuntimeOutcome::Completed(Ok(Err(format!(
                "{}; native adapter shutdown also failed: {error}",
                outcome.description()
            )
            .into())))
        } else {
            outcome
        }
    } else {
        outcome
    };
    finish_runtime_outcome(root, launched, outcome, &cleanup)?;
    cleanup_three_replica_fixture(root)?;
    assert_removed(root);
    Ok(())
}

fn require_prepared_only(launched: &LaunchedMembers) -> Result<(), TestError> {
    if launched.journal().native_phase != NativeTopologyPhase::Prepared
        || launched.journal().native_binding.is_some()
    {
        return Err("native-only foundation must retain the exact prepared journal phase".into());
    }
    Ok(())
}

fn validate_foundation(
    launched: &LaunchedMembers,
    evidence: &NativeSourceFoundationEvidence,
) -> Result<(), TestError> {
    let source = &evidence.source;
    let ownership = source
        .ownership
        .as_ref()
        .ok_or("completed native source has no ownership")?;
    let group_ownership = source
        .availability_group_ownership
        .as_ref()
        .ok_or("completed native source has no AG ownership")?;
    if source.state != "ONLINE"
        || source.recovery_model != "FULL"
        || !source.accessible
        || !source.marker_table_ready
        || !source.full_backup_ready
        || !source.log_chain_ready
        || !source.backup_verified
        || ownership.resource_id != launched.run.resource_uid
        || group_ownership.resource_id != ownership.resource_id
        || group_ownership.configuration_id != ownership.configuration_id
        || group_ownership.activation_id != ownership.activation_id
        || group_ownership.name != evidence.group.identity.name
        || group_ownership.group_id.as_ref() != Some(&evidence.group.identity.group_id)
        || group_ownership.creation_group_id.as_ref() != Some(&evidence.group.identity.group_id)
        || source.identity.as_ref().is_none_or(|identity| {
            identity.name.as_str() != format!("km_db_{}", launched.run.run_id)
        })
        || evidence.group.local_replica.role != Some(NativeRole::Primary)
        || !evidence.group.local_replica.state_available
        || evidence.instance.server_name.as_str() != launched.members[0].server_name
    {
        return Err("native foundation evidence does not describe the exact owned source".into());
    }
    Ok(())
}

async fn contain(
    driver: &NativeSourceFoundation,
    cleanup: &CleanupCoordinator<SystemCleanupClock>,
) -> (bool, Option<String>) {
    let close = AssertUnwindSafe(tokio::time::timeout(cleanup.remaining(), driver.close()))
        .catch_unwind()
        .await;
    let mut errors = match close {
        Ok(Ok(Ok(()))) => Vec::new(),
        Ok(Ok(Err(error))) => vec![format!("Close: {error}")],
        Ok(Err(error)) => vec![format!("Close deadline: {error}")],
        Err(panic) => vec![format!("Close panic: {}", panic_message(&panic))],
    };
    if !errors.is_empty()
        && let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| driver.abort()))
    {
        errors.push(format!("Abort panic: {}", panic_message(&panic)));
        return (false, Some(errors.join("; ")));
    }
    let containment = AssertUnwindSafe(tokio::time::timeout(
        cleanup.remaining(),
        driver.wait_for_containment(),
    ))
    .catch_unwind()
    .await;
    let contained = match containment {
        Ok(Ok(Ok(()))) => true,
        Ok(Ok(Err(error))) => {
            errors.push(format!("containment: {error}"));
            false
        }
        Ok(Err(error)) => {
            errors.push(format!("containment deadline: {error}"));
            false
        }
        Err(panic) => {
            errors.push(format!("containment panic: {}", panic_message(&panic)));
            false
        }
    };
    (contained, (!errors.is_empty()).then(|| errors.join("; ")))
}
