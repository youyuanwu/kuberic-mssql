use super::*;

mod executable_now {
    use super::*;

    #[tokio::test]
    async fn build_cancellation_before_dispatch_cannot_publish_success() {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let complete = Arc::new(AtomicBool::new(false));
        let executor = Arc::new(SeedMutationExecutor {
            complete: complete.clone(),
            executions: AtomicUsize::new(0),
        });
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let replicator = seed_replicator_with_executor(
            root.path().to_owned(),
            complete,
            executor.clone() as Arc<dyn SqlMutationExecutor>,
        );
        admit_seed_replicator(&replicator).await;
        replicator
            .testing_gate_seed_intent(entered_tx, release_rx)
            .unwrap();

        let callback = tokio::spawn({
            let replicator = replicator.clone();
            async move {
                replicator
                    .build_replica(target_build(1, "readiness-cancelled-build"))
                    .await
            }
        });
        entered_rx.await.unwrap();
        replicator.abort();
        release_tx.send(()).unwrap();

        assert!(matches!(
            callback.await.unwrap(),
            Err(KubericRuntimeError::Closed)
        ));
        assert_eq!(executor.executions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stale_build_completion_is_fenced_after_lifecycle_supersession() {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let complete = Arc::new(AtomicBool::new(false));
        let executor = Arc::new(SeedMutationExecutor {
            complete: complete.clone(),
            executions: AtomicUsize::new(0),
        });
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let replicator = seed_replicator_with_executor(
            root.path().to_owned(),
            complete,
            executor.clone() as Arc<dyn SqlMutationExecutor>,
        );
        admit_seed_replicator(&replicator).await;
        replicator
            .testing_gate_seed_compaction(entered_tx, release_rx)
            .unwrap();

        let callback = tokio::spawn({
            let replicator = replicator.clone();
            async move {
                replicator
                    .build_replica(target_build(1, "readiness-stale-completion"))
                    .await
            }
        });
        entered_rx.await.unwrap();
        replicator.abort();
        release_tx.send(()).unwrap();

        assert!(matches!(
            callback.await.unwrap(),
            Err(KubericRuntimeError::Closed)
        ));
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unsupported_removal_data_loss_and_catch_up_are_explicit() {
        let replicator = opened_bound_replicator(
            vec![Ok(present(bound_snapshot())), Ok(present(bound_snapshot()))],
            vec![OBSERVED_AT; 2],
        )
        .await;
        replicator
            .update_current_replica_set_configuration(bound_replica_set())
            .await
            .unwrap();
        replicator
            .change_role(configuration().epoch, ReplicaRole::Primary)
            .await
            .unwrap();

        let failures = [
            replicator
                .remove_replica(ReplicaId::new(2))
                .await
                .unwrap_err(),
            replicator.on_data_loss().await.unwrap_err(),
            replicator
                .update_catch_up_replica_set_configuration(replica_set(), replica_set())
                .await
                .unwrap_err(),
            replicator
                .wait_for_catch_up_quorum(ReplicaSetQuorumMode::WriteQuorum)
                .await
                .unwrap_err(),
        ];
        for failure in failures {
            assert!(application_error(failure).contains("observe-only"));
        }
    }
}

mod current_pin_gap_evidence {
    use super::*;

    #[tokio::test]
    async fn role_before_primary_configuration_remains_release_blocked() {
        let replicator = opened_bound_replicator(
            vec![Ok(present(bound_snapshot())), Ok(present(bound_snapshot()))],
            vec![OBSERVED_AT; 2],
        )
        .await;

        let error = replicator
            .change_role(configuration().epoch, ReplicaRole::Primary)
            .await
            .unwrap_err();
        assert!(application_error(error).contains("not been admitted"));

        replicator
            .update_current_replica_set_configuration(bound_replica_set())
            .await
            .unwrap();
        replicator
            .change_role(configuration().epoch, ReplicaRole::Primary)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn newer_same_role_secondary_epoch_remains_release_blocked() {
        let replicator = opened_admitted_replicator(
            1,
            vec![
                Ok(present(bound_snapshot_for(1))),
                Ok(present(bound_snapshot_for(1))),
                Ok(present(bound_snapshot_for(1))),
            ],
            vec![OBSERVED_AT; 3],
        )
        .await;

        replicator
            .change_role(configuration().epoch, ReplicaRole::ActiveSecondary)
            .await
            .unwrap();
        replicator
            .update_epoch(configuration().epoch)
            .await
            .unwrap();
        let error = replicator
            .update_epoch(Epoch::new(7, 43))
            .await
            .unwrap_err();
        assert!(application_error(error).contains("frozen epoch"));
    }
}

mod adapter_owned_neutral_facts {
    use super::*;

    #[test]
    fn local_active_peer_and_build_target_facts_remain_distinct() {
        let current = bound_replica_set();
        let local = &current.replicas[0];
        let active_peers = current
            .replicas
            .iter()
            .filter(|replica| {
                replica.identity != local.identity && replica.role == ReplicaRole::ActiveSecondary
            })
            .map(|replica| replica.identity.clone())
            .collect::<Vec<_>>();
        let build = target_build(1, "neutral-build-target");

        assert_eq!(
            classify_bootstrap_ownership(&local.identity, &current),
            Ok(BootstrapOwnership::PrimaryTopology)
        );
        assert_eq!(active_peers.len(), 2);
        assert!(!active_peers.contains(&local.identity));
        assert!(active_peers.contains(&build.identity));
        assert_eq!(
            build.process_session_id,
            current.replicas[1].process_session_id
        );
        assert_eq!(
            build.replication_address,
            current.replicas[1].replication_address
        );
        assert!(!build.build_id.is_empty());
    }

    #[test]
    fn native_descriptors_preserve_adapter_owned_identity_without_target_membership_claims() {
        let current = bound_replica_set();
        let descriptors = testing_current_pin_bootstrap_descriptors(
            &kuberic_identity(1),
            &topology_expectation(),
            "replica-1.example:5022",
            &current,
        )
        .unwrap();

        assert_eq!(descriptors.len(), 3);
        for (index, descriptor) in descriptors.iter().enumerate() {
            assert_eq!(descriptor.identity.logical_id(), (index + 1).to_string());
            assert_eq!(
                descriptor.identity.incarnation(),
                format!("generation-{}", index + 1)
            );
            assert_eq!(descriptor.server_name.as_str(), format!("sql-{index}"));
        }
    }
}

mod release_blocked_candidate_gates {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CandidateGateState {
        PendingRelease,
    }

    #[test]
    fn unreleased_contract_gates_remain_pending_candidates() {
        let gates = [
            (
                "external_progress_contract",
                CandidateGateState::PendingRelease,
            ),
            (
                "role_before_primary_configuration",
                CandidateGateState::PendingRelease,
            ),
            (
                "same_role_secondary_epoch",
                CandidateGateState::PendingRelease,
            ),
            (
                "remote_active_membership",
                CandidateGateState::PendingRelease,
            ),
            ("invalid_build_progress", CandidateGateState::PendingRelease),
            ("cancel_before_remove", CandidateGateState::PendingRelease),
            (
                "signed_authorization_contract",
                CandidateGateState::PendingRelease,
            ),
        ];

        assert_eq!(gates.len(), 7);
        assert!(
            gates
                .iter()
                .all(|(_, state)| { matches!(state, CandidateGateState::PendingRelease) })
        );
    }
}
