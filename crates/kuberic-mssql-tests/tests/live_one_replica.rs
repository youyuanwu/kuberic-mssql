use kuberic_mssql_tests::one_replica::{
    OneReplicaConfig, launch_one_replica, run_unique_scenarios,
};
use kuberic_mssql_tests::three_replica::{FailureCategory, FailureStage, SanitizedFailure};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires licensed SQL Server container prerequisites"]
async fn one_replica_mssql_observation_and_cli() {
    let config = OneReplicaConfig::from_environment().expect("valid one-replica fixture config");
    let fixture = launch_one_replica(config)
        .await
        .unwrap_or_else(|error| panic!("one-replica launch failed: {error}"));
    let evidence = fixture
        .run_with_cleanup(std::time::Duration::from_secs(120), |member| async move {
            run_unique_scenarios(
                &member,
                std::path::Path::new(env!("CARGO_BIN_EXE_sqlserver-observer-test")),
            )
            .await
            .map_err(|error| {
                SanitizedFailure::with_detail(
                    FailureStage::Test,
                    FailureCategory::SqlUnavailable,
                    error.to_string(),
                )
            })
        })
        .await
        .unwrap_or_else(|error| panic!("one-replica lifecycle failed: {error}"));
    assert!(evidence.absent_observed_at_unix_millis > 0);
    assert!(evidence.cli_observed_at_unix_millis > 0);
}
