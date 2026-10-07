use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql_tests::one_replica::{
    OneReplicaFixtureFiles, validate_cli_report, validate_scenario_file_separation,
};
use serde_json::{Value, json};

fn config() -> ObserverConfig {
    let directory = tempfile::tempdir().unwrap();
    let username = directory.path().join("username");
    let password = directory.path().join("password");
    let ca = directory.path().join("ca.crt");
    std::fs::write(&username, "observer").unwrap();
    std::fs::write(&password, "password").unwrap();
    std::fs::write(&ca, "certificate").unwrap();
    ObserverConfig::from_json(
        &serde_json::to_vec(&json!({
            "mode": "observe_only",
            "host": "localhost",
            "port": 14330,
            "availability_group": "absent-ag",
            "expected_server_name": "km-one-test",
            "replica_id": "1",
            "incarnation": "container:1",
            "observer_username_file": username,
            "observer_password_file": password,
            "ca_certificate_file": ca,
            "connect_timeout_ms": 1000,
            "query_timeout_ms": 1000,
            "sample_timeout_ms": 5000,
            "poll_interval_ms": 1000,
            "max_age_ms": 60000
        }))
        .unwrap(),
    )
    .unwrap()
}

fn report(config: &ObserverConfig, observed_at: u64) -> Value {
    json!({
        "schema_version": 1,
        "source": {
            "host": config.connection().endpoint().host(),
            "port": config.connection().endpoint().port(),
            "availability_group": config.target().availability_group.as_str(),
            "expected_server_name": config.target().expected_server_name.as_str(),
            "replica": serde_json::to_value(&config.target().replica).unwrap()
        },
        "evaluated_at_unix_millis": observed_at + 1,
        "max_age_millis": 60000,
        "fresh": true,
        "observation": {
            "status": "present",
            "observed_at_unix_millis": observed_at,
            "value": {
                "instance": {
                    "product_version": "17.0.5005.3",
                    "product_major_version": 17,
                    "edition": "Enterprise Developer Edition (64-bit)",
                    "engine_edition": 3,
                    "hadr_enabled": true,
                    "host_platform": "Linux",
                    "architecture": "x86_64",
                    "server_name": config.target().expected_server_name.as_str()
                },
                "availability_group": {
                    "status": "absent",
                    "observed_at_unix_millis": observed_at
                }
            }
        }
    })
}

#[test]
fn valid_cli_report_preserves_fresh_absent_provenance() {
    let config = config();
    let report = report(&config, 1_000_000);
    assert_eq!(
        validate_cli_report(&report, &config, 1_000_001).unwrap(),
        1_000_000
    );
}

#[test]
fn stale_or_future_cli_reports_are_rejected() {
    let config = config();
    let report = report(&config, 1_000_000);
    assert!(validate_cli_report(&report, &config, 1_060_001).is_err());
    assert!(validate_cli_report(&report, &config, 999_999).is_err());
}

#[test]
fn malformed_failed_or_wrong_provenance_reports_are_rejected() {
    let config = config();
    assert!(validate_cli_report(&json!({}), &config, 1_000_001).is_err());

    let mut failed = report(&config, 1_000_000);
    failed["observation"]["status"] = json!("failed");
    assert!(validate_cli_report(&failed, &config, 1_000_001).is_err());

    let mut wrong_source = report(&config, 1_000_000);
    wrong_source["source"]["expected_server_name"] = json!("replacement");
    assert!(validate_cli_report(&wrong_source, &config, 1_000_001).is_err());
}

#[test]
fn wrong_engine_or_present_group_reports_are_rejected() {
    let config = config();
    let mut wrong_engine = report(&config, 1_000_000);
    wrong_engine["observation"]["value"]["instance"]["product_version"] = json!("17.0.0.0");
    assert!(validate_cli_report(&wrong_engine, &config, 1_000_001).is_err());

    let mut present = report(&config, 1_000_000);
    present["observation"]["value"]["availability_group"]["status"] = json!("present");
    assert!(validate_cli_report(&present, &config, 1_000_001).is_err());
}

fn scenario_files() -> OneReplicaFixtureFiles {
    OneReplicaFixtureFiles {
        ca_certificate: "/fixture/trusted-ca.crt".into(),
        bad_ca_certificate: "/fixture/bad-ca.crt".into(),
        admin_username: "/fixture/admin-username".into(),
        admin_password: "/fixture/admin-password".into(),
        observer_username: "/fixture/observer-username".into(),
        observer_password: "/fixture/observer-password".into(),
        denied_username: "/fixture/denied-username".into(),
        denied_password: "/fixture/denied-password".into(),
        absent_config: "/fixture/absent.json".into(),
        denied_config: "/fixture/denied.json".into(),
        bad_tls_config: "/fixture/bad-tls.json".into(),
    }
}

#[test]
fn scenario_credentials_ca_and_configs_are_isolated() {
    let files = scenario_files();
    assert!(validate_scenario_file_separation(&files).is_ok());

    let mut shared_login = files.clone();
    shared_login.denied_username = shared_login.observer_username.clone();
    assert!(validate_scenario_file_separation(&shared_login).is_err());

    let mut shared_ca = files.clone();
    shared_ca.bad_ca_certificate = shared_ca.ca_certificate.clone();
    assert!(validate_scenario_file_separation(&shared_ca).is_err());

    let mut shared_config = files;
    shared_config.denied_config = shared_config.absent_config.clone();
    assert!(validate_scenario_file_separation(&shared_config).is_err());
}
