use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use kuberic_mssql::executor::{QueryRow, SqlExecutor, SqlSession};
use kuberic_mssql::instance::SqlServerInstanceManager;
use kuberic_mssql::kuberic::SqlServerReplicator;
use kuberic_mssql::monitor::ObservationReport;
use kuberic_mssql::query::ReadQuery;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::runtime_error::RuntimeError;
use kuberic_mssql::tds::{testing_classify_driver_error, testing_classify_server_error};
use kuberic_mssql::{AvailabilityGroupName, Observation, ObservationFailureKind};
use kuberic_runtime::RuntimeError as KubericRuntimeError;
use kuberic_runtime::replicator::Replicator;

const OBSERVED_AT: u64 = 1_000;
const SENSITIVE_SERVER_MESSAGE: &str = "sensitive-driver-server-message";

#[derive(Clone, Copy)]
enum FailureCase {
    Authentication,
    Tls,
    Permission,
    ServerIdentity,
    AvailabilityGroupIdentity,
}

struct FailureExecutor(FailureCase);

#[async_trait]
impl SqlExecutor for FailureExecutor {
    async fn connect(&self) -> Result<Box<dyn SqlSession>, RuntimeError> {
        match self.0 {
            FailureCase::Authentication => Err(testing_classify_server_error(18456)),
            FailureCase::Tls => Err(testing_classify_driver_error(tiberius::error::Error::Tls(
                SENSITIVE_SERVER_MESSAGE.into(),
            ))),
            FailureCase::Permission => Ok(Box::new(ScriptedSession::new(vec![Step {
                query: ReadQuery::Permissions,
                rows: vec![permissions(false)],
            }]))),
            FailureCase::ServerIdentity => Ok(Box::new(ScriptedSession::new(vec![
                Step {
                    query: ReadQuery::Permissions,
                    rows: vec![permissions(true)],
                },
                Step {
                    query: ReadQuery::Anchor,
                    rows: vec![anchor("wrong-server", "test-ag")],
                },
            ]))),
            FailureCase::AvailabilityGroupIdentity => Ok(Box::new(ScriptedSession::new(vec![
                Step {
                    query: ReadQuery::Permissions,
                    rows: vec![permissions(true)],
                },
                Step {
                    query: ReadQuery::Anchor,
                    rows: vec![anchor("sql-0", "wrong-ag")],
                },
            ]))),
        }
    }
}

struct Step {
    query: ReadQuery,
    rows: Vec<QueryRow>,
}

struct ScriptedSession {
    steps: VecDeque<Step>,
}

impl ScriptedSession {
    fn new(steps: Vec<Step>) -> Self {
        Self {
            steps: steps.into(),
        }
    }
}

#[async_trait]
impl SqlSession for ScriptedSession {
    async fn query(
        &mut self,
        query: ReadQuery,
        _: &AvailabilityGroupName,
    ) -> Result<Vec<QueryRow>, RuntimeError> {
        let step = self.steps.pop_front().expect("unexpected query");
        assert_eq!(step.query, query);
        Ok(step.rows)
    }
}

fn row(fields: &[(&str, Option<&str>)]) -> QueryRow {
    fields
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.map(str::to_owned)))
        .collect()
}

fn permissions(allowed: bool) -> QueryRow {
    let allowed = if allowed { "1" } else { "0" };
    row(&[
        ("product_major_version", Some("17")),
        ("view_server_state", Some(allowed)),
        ("view_server_performance_state", Some(allowed)),
        ("view_any_definition", Some(allowed)),
        ("view_any_database", Some(allowed)),
    ])
}

fn anchor(server: &str, availability_group: &str) -> QueryRow {
    row(&[
        ("server_name", Some(server)),
        ("property_server_name", Some(server)),
        ("product_version", Some("17.0.5005.3")),
        ("product_major_version", Some("17")),
        ("edition", Some("Enterprise Developer Edition (64-bit)")),
        ("engine_edition", Some("3")),
        ("is_hadr_enabled", Some("1")),
        ("host_platform", Some("Linux")),
        ("host_distribution", Some("Ubuntu")),
        ("architecture", Some("x86_64")),
        ("sqlserver_start_time", Some("2026-10-07T12:00:00")),
        ("group_id", Some("11111111-1111-4111-8111-111111111111")),
        ("group_name", Some(availability_group)),
        ("cluster_type", Some("2")),
        ("cluster_type_desc", Some("EXTERNAL")),
        ("sequence_number", Some("42")),
        ("required_synchronized_secondaries_to_commit", Some("1")),
        ("basic_features", Some("0")),
        ("is_distributed", Some("0")),
        (
            "local_replica_id",
            Some("22222222-2222-4222-8222-222222222222"),
        ),
        ("local_replica_server_name", Some(server)),
        (
            "local_state_group_id",
            Some("11111111-1111-4111-8111-111111111111"),
        ),
        (
            "local_state_replica_id",
            Some("22222222-2222-4222-8222-222222222222"),
        ),
        ("local_role_desc", Some("PRIMARY")),
    ])
}

fn config() -> ObserverConfig {
    ObserverConfig::from_json(
        br#"{
            "host":"sql.example",
            "port":1433,
            "availability_group":"test-ag",
            "expected_server_name":"sql-0",
            "replica_id":"logical-0",
            "incarnation":"pod-0",
            "observer_username_file":"/secrets/sensitive-observer-secret-username",
            "observer_password_file":"/secrets/sensitive-observer-secret-password",
            "sample_timeout_ms":1000,
            "connect_timeout_ms":1000,
            "query_timeout_ms":1000,
            "poll_interval_ms":1000,
            "max_age_ms":60000
        }"#,
    )
    .unwrap()
}

fn expected_kind(case: FailureCase) -> ObservationFailureKind {
    match case {
        FailureCase::Authentication => ObservationFailureKind::Authentication,
        FailureCase::Tls => ObservationFailureKind::Tls,
        FailureCase::Permission => ObservationFailureKind::PermissionDenied,
        FailureCase::ServerIdentity | FailureCase::AvailabilityGroupIdentity => {
            ObservationFailureKind::Inconsistent
        }
    }
}

#[tokio::test]
async fn observer_and_runtime_share_actual_failure_classification() {
    for case in [
        FailureCase::Authentication,
        FailureCase::Tls,
        FailureCase::Permission,
        FailureCase::ServerIdentity,
        FailureCase::AvailabilityGroupIdentity,
    ] {
        let observer_manager = SqlServerInstanceManager::new(FailureExecutor(case), config());
        let observation = observer_manager.observe().await.unwrap();
        let report = ObservationReport::new(&config(), observation, OBSERVED_AT);
        let Observation::Failed(report_failure) = &report.observation else {
            panic!("observer failure report expected");
        };
        assert_eq!(report_failure.kind, expected_kind(case));

        let runtime_manager = Arc::new(SqlServerInstanceManager::new(
            FailureExecutor(case),
            config(),
        ));
        let replicator = Arc::new(SqlServerReplicator::new(
            "replica.example:5022".into(),
            runtime_manager,
            Arc::new(FixedClock),
        ));
        replicator.open().await.unwrap();
        let error = replicator.current_progress().await.unwrap_err();
        let KubericRuntimeError::Application(message) = error else {
            panic!("runtime application error expected");
        };
        assert!(message.contains(&format!("{:?}", expected_kind(case))));

        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("sensitive-observer-secret"));
        assert!(!message.contains("sensitive-observer-secret"));
        assert!(!serialized.contains(SENSITIVE_SERVER_MESSAGE));
        assert!(!message.contains(SENSITIVE_SERVER_MESSAGE));
    }
}

struct FixedClock;

impl kuberic_mssql::kuberic::ObservationClock for FixedClock {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
        Ok(OBSERVED_AT)
    }
}
