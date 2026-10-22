use super::*;

#[tokio::test]
async fn ordinary_observer_cannot_acquire_native_bootstrap_authority() {
    let source = Arc::new(CountingSource {
        inner: ScriptedSource {
            config: observer_config(),
            samples: Mutex::new(VecDeque::new()),
        },
        observations: AtomicUsize::new(0),
    });
    let replicator = SqlServerReplicator::new_with_topology(
        "replica-1.example:5022".to_owned(),
        source.clone(),
        Arc::new(ScriptedClock {
            times: Mutex::new(vec![OBSERVED_AT].into()),
        }),
        kuberic_identity(1),
        topology_expectation(),
    )
    .unwrap();
    assert!(matches!(
        replicator
            .testing_prepare_native_source_foundation(remote_only_primary_replica_set())
            .await,
        Err(KubericAdapterError::NotOpen)
    ));
    replicator.open().await.unwrap();
    assert!(matches!(
        replicator
            .testing_prepare_native_source_foundation(remote_only_primary_replica_set())
            .await,
        Err(KubericAdapterError::TopologyNotAdmitted)
    ));
    assert_eq!(source.observations.load(Ordering::SeqCst), 0);
    assert!(replicator.admitted_topology().is_none());
    replicator.close().await.unwrap();
    assert!(matches!(
        replicator
            .testing_prepare_native_source_foundation(remote_only_primary_replica_set())
            .await,
        Err(KubericAdapterError::Closed)
    ));
}

#[tokio::test]
async fn native_foundation_rejects_secondary_full_deferred_and_malformed_scopes_before_effects() {
    let mut deferred = remote_only_primary_replica_set();
    deferred.replicas.pop();
    let mut invalid_session = remote_only_primary_replica_set();
    invalid_session.replicas[0].process_session_id = ProcessSessionId::default();
    let mut invalid_address = remote_only_primary_replica_set();
    invalid_address.replicas[0].replication_address = " invalid ".to_owned();
    for (local_index, current) in [
        (1, remote_only_primary_replica_set()),
        (0, bound_replica_set()),
        (0, deferred),
        (0, invalid_session),
        (0, invalid_address),
    ] {
        let (replicator, mutation, source) = counted_bootstrap_replicator_for(
            local_index,
            Vec::new(),
            Vec::new(),
            observer_config_for(local_index),
        );
        replicator
            .testing_bind_process_session(ProcessSessionId::new(format!(
                "session-{}",
                local_index + 1
            )))
            .unwrap();
        replicator.open().await.unwrap();
        assert!(
            replicator
                .testing_prepare_native_source_foundation(current)
                .await
                .is_err()
        );
        assert_eq!(source.observations.load(Ordering::SeqCst), 0);
        assert!(mutation.effects.lock().unwrap().is_empty());
        assert!(replicator.admitted_topology().is_none());
        replicator.close().await.unwrap();
    }
}

#[tokio::test]
async fn native_foundation_returns_validated_facts_without_public_or_native_progress_admission() {
    let database = owned_standalone_database();
    let (replicator, mutation, source) = counted_bootstrap_replicator_for(
        0,
        vec![Ok(present(bootstrap_foundation_snapshot(0)))],
        vec![Ok(None), Ok(Some(database.clone()))],
        observer_config(),
    );
    replicator
        .testing_bind_process_session(ProcessSessionId::new("session-1"))
        .unwrap();
    replicator.open().await.unwrap();
    let evidence = replicator
        .testing_prepare_native_source_foundation(remote_only_primary_replica_set())
        .await
        .unwrap();
    assert_eq!(evidence.source, database);
    assert_eq!(
        evidence.instance.server_name,
        ServerName::new("sql-0").unwrap()
    );
    assert_eq!(evidence.group.identity.group_id, guid(AG_ID));
    assert_eq!(evidence.group.local_replica.role, Some(NativeRole::Primary));
    assert!(evidence.group.databases.is_empty());
    assert!(
        evidence.group.replicas[1..]
            .iter()
            .all(|replica| replica.state.is_none())
    );
    assert_eq!(evidence.group_observed_at_unix_millis, OBSERVED_AT);
    assert_eq!(
        *mutation.effects.lock().unwrap(),
        vec!["retain source database creation intent"]
    );
    assert!(replicator.admitted_topology().is_none());
    assert_eq!(
        replicator.native_configuration_sequence().await.unwrap(),
        None
    );
    assert_unsupported(
        replicator.current_progress().await.unwrap_err(),
        ObserveOnlyOperation::CurrentProgress,
    );
    assert_unsupported(
        replicator
            .update_current_replica_set_configuration(remote_only_primary_replica_set())
            .await
            .unwrap_err(),
        ObserveOnlyOperation::CurrentConfiguration,
    );
    assert_eq!(source.observations.load(Ordering::SeqCst), 1);
    assert_eq!(mutation.effects.lock().unwrap().len(), 1);
    assert!(replicator.admitted_topology().is_none());
    replicator.close().await.unwrap();
}

#[tokio::test]
async fn native_foundation_rechecks_final_freshness_before_returning_evidence() {
    for final_time in [OBSERVED_AT - 1, OBSERVED_AT + MAX_AGE + 1] {
        let source = Arc::new(ScriptedSource {
            config: observer_config(),
            samples: Mutex::new(vec![Ok(present(bootstrap_foundation_snapshot(0)))].into()),
        });
        let mutation = Arc::new(ScriptedMutationExecutor {
            databases: Mutex::new(vec![Ok(Some(owned_standalone_database()))].into()),
            effects: Mutex::new(Vec::new()),
            executions: Mutex::new(VecDeque::new()),
        });
        let replicator = native_replicator(
            source,
            mutation.clone(),
            vec![OBSERVED_AT, OBSERVED_AT, final_time],
        );
        replicator.open().await.unwrap();
        let error = replicator
            .testing_prepare_native_source_foundation(remote_only_primary_replica_set())
            .await
            .unwrap_err();
        if final_time < OBSERVED_AT {
            assert!(matches!(error, KubericAdapterError::ObservationFromFuture));
        } else {
            assert!(matches!(error, KubericAdapterError::ObservationStale));
        }
        assert!(replicator.admitted_topology().is_none());
        assert!(mutation.effects.lock().unwrap().is_empty());
        replicator.close().await.unwrap();
    }
}

#[tokio::test]
async fn native_foundation_cannot_publish_after_abort_close_or_epoch_supersession() {
    for transition in 0..3 {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let source = Arc::new(GatedSource {
            config: observer_config(),
            samples: Mutex::new(vec![Ok(present(bootstrap_foundation_snapshot(0)))].into()),
            observations: AtomicUsize::new(0),
            gate_at: 0,
            entered: Mutex::new(Some(entered_tx)),
            release: Mutex::new(Some(release_rx)),
        });
        let mutation = Arc::new(ScriptedMutationExecutor {
            databases: Mutex::new(vec![Ok(Some(owned_standalone_database()))].into()),
            effects: Mutex::new(Vec::new()),
            executions: Mutex::new(VecDeque::new()),
        });
        let replicator = native_replicator(source, mutation.clone(), vec![OBSERVED_AT]);
        replicator.open().await.unwrap();
        let task = tokio::spawn({
            let replicator = replicator.clone();
            async move {
                replicator
                    .testing_prepare_native_source_foundation(remote_only_primary_replica_set())
                    .await
            }
        });
        entered_rx.await.unwrap();
        match transition {
            0 => replicator.abort(),
            1 => replicator.close().await.unwrap(),
            _ => replicator.update_epoch(Epoch::new(7, 43)).await.unwrap(),
        }
        let _ = release_tx.send(());
        assert!(matches!(
            task.await.unwrap(),
            Err(KubericAdapterError::Closed)
        ));
        assert!(replicator.admitted_topology().is_none());
        assert!(mutation.effects.lock().unwrap().is_empty());
        replicator.abort();
        replicator.wait_for_containment().await.unwrap();
    }
}

fn native_replicator(
    source: Arc<dyn SqlServerObservationSource>,
    mutation: Arc<dyn SqlMutationExecutor>,
    times: Vec<u64>,
) -> Arc<SqlServerReplicator> {
    let replicator = SqlServerReplicator::new_bootstrap_with_topology(
        SqlServerServiceConfig::new(ResourceUid::new("resource-a"), "replica-1.example:5022")
            .unwrap(),
        source,
        Arc::new(ScriptedClock {
            times: Mutex::new(times.into()),
        }),
        kuberic_identity(1),
        topology_expectation(),
        SqlServerBootstrapConfig::new(SqlIdentifier::new("app-db").unwrap()),
        mutation,
        private_seed_root(),
    )
    .unwrap();
    replicator
        .testing_bind_process_session(ProcessSessionId::new("session-1"))
        .unwrap();
    Arc::new(replicator)
}
