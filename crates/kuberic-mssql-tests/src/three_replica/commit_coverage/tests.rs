use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::FutureExt;
use kuberic_mssql::observation::{
    AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, DatabaseSnapshot, InstanceMetadata,
    LocalDatabaseSnapshot, LocalRecoveryMetadata, LocalReplicaSnapshot, ReplicaSnapshot,
};
use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, ConfigurationSequence, DatabaseIdentity,
    DatabaseLineage, Guid, NativeProgress, ReplicaIdentity, ServerName, SqlIdentifier,
};

use super::*;
use crate::three_replica::NativeMemberBinding;

fn guid(value: u8) -> Guid {
    Guid::parse("test", format!("{value:08x}-0000-4000-8000-000000000000")).unwrap()
}

fn bound_sample() -> (InstanceSnapshot, ReadyMember, NativeTopologyBinding) {
    let members = std::array::from_fn(|index| NativeMemberBinding {
        ordinal: (index + 1) as u8,
        server_name: format!("sql{}", index + 1),
        container_id: format!("container{}", index + 1),
        sql_start_time: "2026-10-17T00:00:00".to_owned(),
        sql_start_unix_millis: 1,
        native_replica_id: guid((index + 1) as u8).to_string(),
        local_database_id: 5,
        database_guid: guid(10 + index as u8).to_string(),
        role: if index == 0 { "PRIMARY" } else { "SECONDARY" }.to_owned(),
        endpoint_url: format!("TCP://sql{}:5022", index + 1),
        endpoint_name: "endpoint".to_owned(),
        endpoint_port: 5022,
        endpoint_certificate_name: "certificate".to_owned(),
        endpoint_certificate_thumbprint: "thumbprint".to_owned(),
    });
    let binding = NativeTopologyBinding {
        session_id: "fixture-run-not-public-session".to_owned(),
        availability_group_name: "ag".to_owned(),
        availability_group_id: guid(20).to_string(),
        configuration_sequence: 42,
        database_name: "db".to_owned(),
        group_database_id: guid(21).to_string(),
        family_guid: guid(22).to_string(),
        recovery_fork_id: guid(23).to_string(),
        seeding_operation_ids: [guid(24).to_string(), guid(25).to_string()],
        members,
    };
    let member = ReadyMember {
        ordinal: 1,
        server_name: "sql1".to_owned(),
        container_id: "container1".to_owned(),
        host_port: 40000,
        sql_start_time: binding.members[0].sql_start_time.clone(),
        sql_start_unix_millis: 1,
        observer_config: "unused".into(),
        admin_username: "unused".into(),
        admin_password: "unused".into(),
        ca_certificate: "unused".into(),
    };
    let database = DatabaseIdentity {
        name: SqlIdentifier::new("db").unwrap(),
        group_database_id: guid(21),
    };
    let sample = InstanceSnapshot {
        observed_at_unix_millis: 1000,
        instance: InstanceMetadata {
            server_name: ServerName::new("sql1").unwrap(),
            property_server_name: ServerName::new("sql1").unwrap(),
            product_version: "17.0.5005.3".to_owned(),
            product_major_version: 17,
            edition: "Enterprise Developer Edition".to_owned(),
            engine_edition: 3,
            hadr_enabled: true,
            host_platform: "Linux".to_owned(),
            host_distribution: None,
            architecture: "X64".to_owned(),
            sqlserver_start_time: member.sql_start_time.clone(),
        },
        availability_group: Observation::Present {
            observed_at_unix_millis: 1000,
            value: AvailabilityGroupSnapshot {
                identity: AvailabilityGroupIdentity {
                    name: AvailabilityGroupName::new("ag").unwrap(),
                    group_id: guid(20),
                },
                configuration_sequence: ConfigurationSequence::parse("42").unwrap(),
                cluster_type: "external".to_owned(),
                required_synchronized_secondaries_to_commit: 1,
                basic_features: false,
                is_distributed: false,
                local_replica: LocalReplicaSnapshot {
                    identity: ReplicaIdentity::observed("replica", guid(1), "fixture").unwrap(),
                    state_available: true,
                    role: Some(NativeRole::Primary),
                },
                replicas: binding
                    .members
                    .iter()
                    .map(|member| ReplicaSnapshot {
                        replica_id: Guid::parse("replica", &member.native_replica_id).unwrap(),
                        server_name: ServerName::new(&member.server_name).unwrap(),
                        endpoint_url: Some(member.endpoint_url.clone()),
                        availability_mode: "SYNCHRONOUS_COMMIT".to_owned(),
                        failover_mode: "EXTERNAL".to_owned(),
                        seeding_mode: "AUTOMATIC".to_owned(),
                        state: None,
                    })
                    .collect(),
                databases: vec![DatabaseSnapshot {
                    identity: database.clone(),
                    local: Some(LocalDatabaseSnapshot {
                        database_id: 5,
                        replica_id: guid(1),
                        state: Some("ONLINE".to_owned()),
                        recovery_model: Some("FULL".to_owned()),
                        recovery: Some(LocalRecoveryMetadata {
                            database_guid: Some(guid(10)),
                            family_guid: Some(guid(22)),
                            recovery_fork_guid: Some(guid(23)),
                            first_recovery_fork_guid: Some(guid(23)),
                            fork_point_lsn: None,
                        }),
                    }),
                    replicas: vec![DatabaseReplicaSnapshot {
                        group_database_id: guid(21),
                        replica_id: guid(1),
                        database_id: 5,
                        provenance: NativeProvenance::Local,
                        lineage: RecoveryLineageObservation::Local {
                            value: DatabaseLineage {
                                database,
                                recovery_fork_id: guid(23),
                            },
                        },
                        is_primary_replica: Some(true),
                        synchronization_state: Some("SYNCHRONIZED".to_owned()),
                        synchronization_health: Some("HEALTHY".to_owned()),
                        database_state: Some("ONLINE".to_owned()),
                        is_suspended: Some(false),
                        suspend_reason: None,
                        is_commit_participant: Some(true),
                        progress: NativeProgress {
                            hardened_block: Some(
                                DecimalProgress::parse("9999999999999999999999999").unwrap(),
                            ),
                            redone_record: None,
                            committed_record: Some(
                                DecimalProgress::parse("9223372036854775808").unwrap(),
                            ),
                        },
                    }],
                }],
                automatic_seeding: vec![],
                physical_seeding: vec![],
            },
        },
    };
    (sample, member, binding)
}

#[test]
fn native_samples_require_request_freshness_and_exact_instance_and_authority() {
    let (sample, member, binding) = bound_sample();
    validate_native_sample(&sample, &member, &binding, 900, 1100).unwrap();
    for (requested, completed) in [(1001, 1100), (900, 999), (900, 31_001)] {
        assert!(validate_native_sample(&sample, &member, &binding, requested, completed).is_err());
    }
    let mut replaced = member.clone();
    replaced.container_id = "replacement".to_owned();
    assert!(validate_native_sample(&sample, &replaced, &binding, 900, 1100).is_err());
    let mut restarted = sample.clone();
    restarted.instance.sqlserver_start_time.push('1');
    assert!(validate_native_sample(&restarted, &member, &binding, 900, 1100).is_err());
    let mut superseded = binding.clone();
    superseded.configuration_sequence += 1;
    assert!(validate_native_sample(&sample, &member, &superseded, 900, 1100).is_err());
}

#[test]
fn native_samples_reject_missing_remote_or_incompatible_history() {
    let (sample, member, binding) = bound_sample();
    for change in 0..6 {
        let mut changed = sample.clone();
        let Observation::Present { value: group, .. } = &mut changed.availability_group else {
            panic!()
        };
        match change {
            0 => group.databases[0].replicas[0].provenance = NativeProvenance::PrimaryRemote,
            1 => {
                group.databases[0].replicas[0].lineage =
                    RecoveryLineageObservation::RemoteUnavailable
            }
            2 => {
                group.databases[0]
                    .local
                    .as_mut()
                    .unwrap()
                    .recovery
                    .as_mut()
                    .unwrap()
                    .family_guid = Some(guid(99))
            }
            3 => group.databases[0].identity.group_database_id = guid(99),
            4 => group.identity.group_id = guid(99),
            5 => {
                group.databases[0].replicas[0].lineage = RecoveryLineageObservation::Local {
                    value: DatabaseLineage {
                        database: group.databases[0].identity.clone(),
                        recovery_fork_id: guid(99),
                    },
                }
            }
            _ => unreachable!(),
        }
        assert!(validate_native_sample(&changed, &member, &binding, 900, 1100).is_err());
    }
    let mut missing = sample;
    missing.availability_group = Observation::Absent {
        observed_at_unix_millis: 1000,
    };
    assert!(validate_native_sample(&missing, &member, &binding, 900, 1100).is_err());
}

fn transaction_sample(state: i32, lsn: Option<&str>) -> TransactionSample {
    TransactionSample {
        phase: "overlap",
        requested_at_unix_millis: 1000,
        completed_at_unix_millis: 1001,
        row: Some(parse_transaction_row(Some("123"), Some(5), Some(state), lsn, "123", 5).unwrap()),
    }
}

#[test]
fn exact_transaction_rows_preserve_native_width_but_never_certify_public_coverage() {
    let sample = transaction_sample(12, Some("9999999999999999999999999"));
    let outcome = record_probe_outcome(&[sample]).unwrap();
    let json = serde_json::to_value(&outcome).unwrap();
    assert_eq!(json["commit_lsn"], "9999999999999999999999999");
    let conclusion = serde_json::to_value(coverage_conclusion(&outcome)).unwrap();
    assert_eq!(conclusion["status"], "blocked");
    assert!(
        conclusion["missing_evidence"][0]
            .as_str()
            .unwrap()
            .contains("not collected")
    );
    for (transaction, database, lsn) in [
        ("124", 5, "1"),
        ("123", 6, "1"),
        ("123", 5, "-1"),
        ("123", 5, "10000000000000000000000000"),
    ] {
        assert!(
            parse_transaction_row(
                Some(transaction),
                Some(database),
                Some(12),
                Some(lsn),
                "123",
                5
            )
            .is_err()
        );
    }
    assert!(
        record_probe_outcome(&[
            transaction_sample(12, Some("1")),
            transaction_sample(10, Some("2")),
        ])
        .is_err()
    );
    let (snapshot, _, _) = bound_sample();
    let raw = serde_json::to_value(snapshot).unwrap();
    assert_eq!(
        raw["availability_group"]["value"]["databases"][0]["replicas"][0]["progress"]["hardened_block"],
        "9999999999999999999999999"
    );
}

#[test]
fn missing_zero_or_uncommitted_record_is_collection_limit_not_coverage() {
    for samples in [
        vec![],
        vec![transaction_sample(4, None)],
        vec![transaction_sample(12, Some("0"))],
        vec![transaction_sample(4, Some("100"))],
    ] {
        let record = record_probe_outcome(&samples).unwrap();
        assert_eq!(record, RecordProbeOutcome::NotObservedInBoundedWindow);
        let CoverageConclusion::Blocked {
            missing_evidence, ..
        } = coverage_conclusion(&record);
        assert!(
            missing_evidence
                .iter()
                .any(|reason| reason.contains("bounded window"))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn sampling_exhaustion_still_awaits_ack_and_allows_post_ack_work() {
    let start = Instant::now();
    let (ack, samples) = complete_probe(
        async {
            sleep(Duration::from_secs(5)).await;
            Ok(42)
        },
        async {
            sleep(SAMPLE_WINDOW).await;
            Ok((Vec::<&str>::new(), ()))
        },
        |mut samples, ()| async move {
            samples.push("post_ack");
            Ok(samples)
        },
    )
    .await
    .unwrap();
    assert_eq!(Instant::now() - start, Duration::from_secs(5));
    assert_eq!(ack, 42);
    assert_eq!(samples, ["post_ack"]);
}

#[tokio::test(start_paused = true)]
async fn either_probe_error_waits_for_other_branch_and_dual_errors_survive() {
    for sampler_fails in [false, true] {
        let start = Instant::now();
        let result = join_commit_and_sampling(
            async {
                sleep(Duration::from_secs(2)).await;
                Err(DataError::Admin(AdminError::WriteUnconfirmed))
            },
            async {
                sleep(Duration::from_secs(3)).await;
                if sampler_fails {
                    Err(CommitCoverageError::Clock)
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap_err();
        assert_eq!(Instant::now() - start, Duration::from_secs(3));
        assert!(matches!(
            &result,
            CommitCoverageError::Concurrent {
                commit: DataError::Admin(AdminError::WriteUnconfirmed),
                ..
            }
        ));
        if sampler_fails {
            assert!(matches!(result, CommitCoverageError::Concurrent {
                sampling: Some(error), ..
            } if matches!(*error, CommitCoverageError::Clock)));
        }
    }
    let start = Instant::now();
    assert!(matches!(
        join_commit_and_sampling(
            async {
                sleep(Duration::from_secs(4)).await;
                Ok(42)
            },
            async {
                Err::<(), _>(CommitCoverageError::Data(DataError::Admin(
                    AdminError::Query,
                )))
            },
        )
        .await,
        Err(CommitCoverageError::Data(DataError::Admin(
            AdminError::Query
        )))
    ));
    assert_eq!(Instant::now() - start, Duration::from_secs(4));
}

struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn deadline_cancellation_and_panic_drop_both_lexical_branches() {
    for panic in [false, true] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let commit_guard = Dropped(dropped.clone());
        let sample_guard = Dropped(dropped.clone());
        let future = join_commit_and_sampling(
            async move {
                let _guard = commit_guard;
                if panic {
                    panic!("probe interruption");
                }
                std::future::pending::<Result<u64, DataError>>().await
            },
            async move {
                let _guard = sample_guard;
                std::future::pending::<Result<(), CommitCoverageError>>().await
            },
        );
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            std::panic::AssertUnwindSafe(future).catch_unwind(),
        )
        .await;
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }
}
