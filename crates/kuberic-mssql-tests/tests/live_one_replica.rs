use kuberic_mssql_tests::one_replica::{
    OneReplicaConfig, launch_one_replica, run_unique_scenarios,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires licensed SQL Server container prerequisites"]
async fn one_replica_mssql_observation_and_cli() {
    let config = OneReplicaConfig::from_environment().expect("valid one-replica fixture config");
    let fixture = launch_one_replica(config)
        .await
        .unwrap_or_else(|error| panic!("one-replica launch failed: {error}"));
    let scenarios = run_unique_scenarios(
        &fixture.member,
        std::path::Path::new(env!("CARGO_BIN_EXE_sqlserver-observer-test")),
    )
    .await;
    let cleanup = fixture.cleanup();
    if let Err(error) = cleanup {
        panic!("one-replica cleanup failed: {error}");
    }
    let evidence =
        scenarios.unwrap_or_else(|error| panic!("one-replica scenarios failed: {error}"));
    assert!(evidence.absent_observed_at_unix_millis > 0);
    assert!(evidence.cli_observed_at_unix_millis > 0);
}
