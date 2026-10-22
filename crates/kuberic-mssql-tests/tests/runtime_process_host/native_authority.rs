use super::*;

use kuberic_runtime::replicator::{PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration};

async fn secondary_replay_case() {
    let root = tempfile::tempdir_in(".").unwrap();
    let mut attempts = start_attempts(root.path()).await;
    let (mut running, _, _) = initialize_open_attempts(&mut attempts).await;
    let (_, opened) = current_public_reports(&attempts).await;
    for index in [1, 2] {
        // This calls the limited native topology API directly. No peer progress
        // is manufactured and no public Configuration/Progress receipt is claimed.
        let configuration = ReplicaSetConfiguration {
            configuration: configuration(),
            replicas: opened
                .iter()
                .enumerate()
                .filter(|(peer, _)| *peer != index)
                .map(|(peer, report)| {
                    let mut replica = ReplicaInformation::new(
                        OperationId::new(format!("native-peer-{}", peer + 1)),
                        report.replica.clone(),
                        format!("http://{}", attempts[peer].replication),
                    );
                    replica.process_session_id = report.process_session_id.clone();
                    replica.storage_incarnation = Some(report.storage_incarnation.clone());
                    replica.role = configuration().members[peer].role;
                    replica
                })
                .collect(),
        };
        let replicator = attempts[index].application.testing_replicator().unwrap();
        replicator
            .update_current_replica_set_configuration(configuration.clone())
            .await
            .unwrap();
        let original = replicator.admitted_topology().unwrap();
        replicator
            .update_current_replica_set_configuration(configuration.clone())
            .await
            .unwrap();
        assert_eq!(replicator.admitted_topology().unwrap(), original);
        let local = original
            .members()
            .iter()
            .find(|member| member.kuberic_identity() == original.local_identity())
            .unwrap();
        assert_eq!(
            local.process_session_id(),
            &opened[index].process_session_id
        );
        assert_eq!(
            Some(local.replication_address()),
            opened[index].replication_address.as_deref()
        );
        for mutation in 0..2 {
            let mut changed = configuration.clone();
            match mutation {
                0 => {
                    changed.replicas[0].process_session_id =
                        kuberic_runtime::protocol::types::ProcessSessionId::new(
                            "replaced-remote-session",
                        )
                }
                1 => changed.replicas[0].replication_address = "http://127.0.0.1:32001".into(),
                _ => unreachable!(),
            }
            let error = replicator
                .update_current_replica_set_configuration(changed)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("replay differs"), "{error}");
            assert_eq!(replicator.admitted_topology().unwrap(), original);
        }
    }
    let (_, reports) = current_public_reports(&attempts).await;
    for report in &reports {
        assert_unsupported_queries(report);
        assert!(report.configuration.is_none());
        assert_ne!(report.read_status, AccessStatus::Granted);
        assert_ne!(report.write_status, AccessStatus::Granted);
    }
    stop_all(&mut running).await;
}

#[test]
fn remote_only_secondary_configuration_replay_keeps_factory_local_context() {
    run_host_test(secondary_replay_case);
}

async fn partial_startup_ownership_cases() {
    use kuberic_mssql_tests::three_replica::{
        PublicStartupProbeHandle, PublicStartupProbeMember, PublicStartupProbeMode,
        exercise_partial_public_startup_cleanup,
    };
    for mode in 0..5 {
        let root = tempfile::tempdir_in(".").unwrap();
        let mut attempts = start_attempts(root.path()).await;
        let (running, _, _) = initialize_open_attempts(&mut attempts).await;
        let (_, reports) = current_public_reports(&attempts).await;
        for report in &reports {
            assert_unsupported_queries(report);
            assert!(report.configuration.is_none());
        }
        let replicators = attempts
            .iter()
            .map(|attempt| attempt.application.testing_replicator().unwrap())
            .collect::<Vec<_>>();
        let controls = attempts
            .iter()
            .map(|attempt| attempt.control)
            .collect::<Vec<_>>();
        let (release, held) = tokio::sync::oneshot::channel();
        let mut held = Some(held);
        let mut replicas = running.into_iter().map(Some).collect::<Vec<_>>();
        let mut failed = if mode >= 3 {
            let mut primary = replicas[0].take().unwrap();
            primary.shutdown();
            primary.wait().await.unwrap();
            let task = tokio::spawn(async { panic!("injected startup task panic") });
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            Some(task)
        } else {
            None
        };
        let members = replicas
            .into_iter()
            .zip(attempts)
            .enumerate()
            .map(|(index, (running, attempt))| {
                let handle = if index == 0 && mode >= 3 {
                    PublicStartupProbeHandle::Pending(failed.take().unwrap())
                } else if index == 1 && mode < 3 {
                    let running = running.unwrap();
                    let held = held.take().unwrap();
                    PublicStartupProbeHandle::Pending(tokio::spawn(async move {
                        held.await.expect("release owned startup");
                        Ok(Some(running))
                    }))
                } else {
                    PublicStartupProbeHandle::Started(running.unwrap())
                };
                PublicStartupProbeMember {
                    config: attempt.config,
                    application: attempt.application,
                    source: attempt.source,
                    report: reports[index].clone(),
                    handle,
                }
            })
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|_| unreachable!());
        let (drained, mut completion) = tokio::sync::oneshot::channel();
        let budget = if mode == 2 {
            Duration::from_millis(20)
        } else {
            Duration::from_secs(5)
        };
        let probe_mode = match mode {
            3 => PublicStartupProbeMode::ReadinessFailure,
            4 => PublicStartupProbeMode::ReportFailure,
            _ => PublicStartupProbeMode::Drain,
        };
        let mut cleanup = Box::pin(exercise_partial_public_startup_cleanup(
            members, budget, drained, probe_mode,
        ));
        if mode >= 3 {
            let error = cleanup.await.unwrap_err();
            assert!(error.agent_termination_unconfirmed(), "{error}");
            assert!(error.to_string().contains("injected startup task panic"));
        } else if mode == 0 {
            release.send(()).unwrap();
            cleanup.await.unwrap();
        } else {
            tokio::select! {
                result = &mut cleanup => panic!("cleanup finished before held startup: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
            assert!(matches!(
                completion.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            if mode == 1 {
                drop(cleanup);
            } else {
                assert!(cleanup.await.unwrap_err().agent_termination_unconfirmed());
            }
            assert!(matches!(
                completion.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            release.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), completion)
            .await
            .unwrap()
            .unwrap();
        for (control, replicator) in controls.into_iter().zip(replicators) {
            assert!(tokio::net::TcpStream::connect(control).await.is_err());
            assert!(matches!(
                replicator.current_progress().await,
                Err(KubericRuntimeError::Closed)
            ));
            replicator.wait_for_containment().await.unwrap();
        }
    }
}

#[test]
fn partial_open_observer_startup_drains_transferred_and_late_hosts() {
    run_host_test(partial_startup_ownership_cases);
}

async fn blocked_startup_case() {
    use kuberic_mssql_tests::three_replica::{
        PublicStartupProbeHandle, PublicStartupProbeMember, exercise_public_configuration_startup,
    };
    let root = tempfile::tempdir_in(".").unwrap();
    let mut attempts = start_attempts(root.path()).await;
    let running = initialize_stores(&mut attempts).await;
    let (_, initial) = current_public_reports(&attempts).await;
    for report in &initial {
        assert!(report.configuration.is_none());
        assert_ne!(report.read_status, AccessStatus::Granted);
        assert_ne!(report.write_status, AccessStatus::Granted);
    }
    let controls = attempts
        .iter()
        .map(|attempt| attempt.control)
        .collect::<Vec<_>>();
    let applications = attempts
        .iter()
        .map(|attempt| attempt.application.clone())
        .collect::<Vec<_>>();
    let members = attempts
        .into_iter()
        .zip(running)
        .enumerate()
        .map(|(index, (attempt, running))| PublicStartupProbeMember {
            config: attempt.config,
            application: attempt.application,
            source: attempt.source,
            report: initial[index].clone(),
            handle: PublicStartupProbeHandle::Started(running),
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| unreachable!());
    let (drained, completion) = tokio::sync::oneshot::channel();
    let error = match exercise_public_configuration_startup(
        members,
        &configuration(),
        Duration::from_secs(10),
        drained,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("unsupported public progress must block hosted configuration startup"),
    };
    let diagnostic = error.to_string();
    assert!(
        diagnostic.starts_with("configuration peer current progress is unavailable: "),
        "{diagnostic}"
    );
    for operation in ["current_progress", "catch_up_capability"] {
        assert!(
            diagnostic.contains(&format!(
                "{operation} is disabled by the observe-only SQL Server adapter"
            )),
            "{diagnostic}"
        );
    }
    assert!(
        diagnostic.contains("current_progress: Failed("),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("catch_up_capability: Failed("),
        "{diagnostic}"
    );
    assert!(!error.agent_termination_unconfirmed(), "{error}");
    tokio::time::timeout(Duration::from_secs(5), completion)
        .await
        .unwrap()
        .unwrap();
    for (control, application) in controls.into_iter().zip(applications) {
        assert!(tokio::net::TcpStream::connect(control).await.is_err());
        assert!(application.testing_replicator().is_none());
        application.wait_for_containment().await.unwrap();
        assert!(matches!(
            application.change_role(ReplicaRole::Primary).await,
            Err(KubericRuntimeError::Closed)
        ));
    }
}

#[test]
fn unsupported_public_queries_block_hosted_startup_and_drain_owned_observers() {
    run_host_test(blocked_startup_case);
}
