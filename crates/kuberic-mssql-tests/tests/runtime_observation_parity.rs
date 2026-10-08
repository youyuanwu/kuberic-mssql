use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_mssql::kuberic::{ObservationClock, SqlServerObservationSource, SqlServerReplicator};
use kuberic_mssql::monitor::ObservationReport;
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::runtime_error::RuntimeError;
use kuberic_mssql::{Observation, ObservationFailure, ObservationFailureKind};
use kuberic_mssql_tests::Fixture;
use kuberic_runtime::RuntimeError as KubericRuntimeError;
use kuberic_runtime::replicator::Replicator;

const OBSERVED_AT: u64 = 1_000;

struct ScriptedSource {
    config: ObserverConfig,
    samples: Mutex<VecDeque<Observation<InstanceSnapshot>>>,
}

#[async_trait]
impl SqlServerObservationSource for ScriptedSource {
    fn observer_config(&self) -> &ObserverConfig {
        &self.config
    }

    async fn observe(&self) -> Result<Observation<InstanceSnapshot>, RuntimeError> {
        Ok(self.samples.lock().unwrap().pop_front().unwrap())
    }
}

struct FixedClock;

impl ObservationClock for FixedClock {
    fn now_unix_millis(&self) -> Result<u64, RuntimeError> {
        Ok(OBSERVED_AT)
    }
}

#[tokio::test]
async fn observer_reports_and_runtime_errors_preserve_failure_classification() {
    for (case, kind) in [
        ("credential", ObservationFailureKind::Authentication),
        ("TLS", ObservationFailureKind::Tls),
        ("permission", ObservationFailureKind::PermissionDenied),
        ("server identity", ObservationFailureKind::Inconsistent),
        (
            "availability-group identity",
            ObservationFailureKind::Inconsistent,
        ),
    ] {
        let config = Fixture::new().config();
        let observation = Observation::Failed(ObservationFailure {
            kind,
            message: format!("{case}: redacted"),
            observed_at_unix_millis: OBSERVED_AT,
        });
        let report = ObservationReport::new(&config, observation.clone(), OBSERVED_AT);
        let Observation::Failed(report_failure) = &report.observation else {
            panic!("{case}: observer failure report expected");
        };
        assert_eq!(report_failure.kind, kind, "{case}");
        assert!(!report.fresh, "{case}");

        let replicator = Arc::new(SqlServerReplicator::new(
            "replica.example:5022".into(),
            Arc::new(ScriptedSource {
                config,
                samples: Mutex::new(vec![observation].into()),
            }),
            Arc::new(FixedClock),
        ));
        replicator.open().await.unwrap();
        let error = replicator.current_progress().await.unwrap_err();
        let KubericRuntimeError::Application(message) = error else {
            panic!("{case}: application error expected");
        };
        assert!(message.contains(&format!("{kind:?}")), "{case}: {message}");
        assert!(!message.contains("sensitive"), "{case}");
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("sensitive"),
            "{case}"
        );
    }
}
