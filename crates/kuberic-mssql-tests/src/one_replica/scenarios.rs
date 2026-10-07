use std::fmt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

use kuberic_mssql::instance::SqlServerInstanceManager;
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::tds::TdsExecutor;
use kuberic_mssql::{Observation, ObservationFailureKind};

use crate::fixture::admin::EXPECTED_SQL_SERVER_VERSION;
use crate::fixture::process::{BoundedProcessRunner, CommandSpec, ProcessRunner};

use super::member::{ONE_REPLICA_FAULT_ENV, ReadyOneReplica};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneReplicaScenarioEvidence {
    pub absent_observed_at_unix_millis: u64,
    pub cli_observed_at_unix_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OneReplicaScenarioError {
    Config,
    Observe,
    ExpectedAbsent,
    ExpectedPermissionDenied,
    ExpectedTls,
    Cli,
    CliReport,
    Clock,
    InjectedFault,
}

impl fmt::Display for OneReplicaScenarioError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Config => "one-replica observer configuration is invalid",
            Self::Observe => "one-replica production observation failed",
            Self::ExpectedAbsent => "one-replica absent availability group evidence is invalid",
            Self::ExpectedPermissionDenied => "one-replica permission-denied evidence is invalid",
            Self::ExpectedTls => "one-replica TLS-failure evidence is invalid",
            Self::Cli => "one-replica production observer command failed",
            Self::CliReport => "one-replica production observer report is invalid",
            Self::Clock => "one-replica scenario clock is invalid",
            Self::InjectedFault => "one-replica scenario fault was injected",
        })
    }
}

impl std::error::Error for OneReplicaScenarioError {}

pub async fn run_unique_scenarios(
    member: &ReadyOneReplica,
    observer_binary: &Path,
) -> Result<OneReplicaScenarioEvidence, OneReplicaScenarioError> {
    check_fault("before-scenarios")?;
    let absent_config = ObserverConfig::read(&member.files.absent_config)
        .await
        .map_err(|_| OneReplicaScenarioError::Config)?;
    let absent = observe(absent_config.clone()).await?;
    let absent_observed_at_unix_millis = match absent {
        Observation::Present {
            value,
            observed_at_unix_millis,
        } if matches!(value.availability_group, Observation::Absent { .. }) => {
            observed_at_unix_millis
        }
        _ => return Err(OneReplicaScenarioError::ExpectedAbsent),
    };

    let denied_config = ObserverConfig::read(&member.files.denied_config)
        .await
        .map_err(|_| OneReplicaScenarioError::Config)?;
    if !matches!(
        observe(denied_config).await?,
        Observation::Failed(failure)
            if failure.kind == ObservationFailureKind::PermissionDenied
    ) {
        return Err(OneReplicaScenarioError::ExpectedPermissionDenied);
    }

    let bad_tls_config = ObserverConfig::read(&member.files.bad_tls_config)
        .await
        .map_err(|_| OneReplicaScenarioError::Config)?;
    if !matches!(
        observe(bad_tls_config).await?,
        Observation::Failed(failure) if failure.kind == ObservationFailureKind::Tls
    ) {
        return Err(OneReplicaScenarioError::ExpectedTls);
    }

    let result = BoundedProcessRunner
        .run(
            &CommandSpec::new(
                observer_binary,
                "run one-replica production observer command",
                Duration::from_secs(90),
            )
            .args(["--config"])
            .arg(&member.files.absent_config),
        )
        .map_err(|_| OneReplicaScenarioError::Cli)?;
    check_fault("before-cli-validation")?;
    let report: Value = serde_json::from_str(result.stdout.trim())
        .map_err(|_| OneReplicaScenarioError::CliReport)?;
    let now_unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OneReplicaScenarioError::Clock)?
        .as_millis() as u64;
    let cli_observed_at_unix_millis =
        validate_cli_report(&report, &absent_config, now_unix_millis)?;
    check_fault("after-cli-validation")?;
    Ok(OneReplicaScenarioEvidence {
        absent_observed_at_unix_millis,
        cli_observed_at_unix_millis,
    })
}

fn check_fault(point: &str) -> Result<(), OneReplicaScenarioError> {
    if std::env::var(ONE_REPLICA_FAULT_ENV).ok().as_deref() == Some(point) {
        Err(OneReplicaScenarioError::InjectedFault)
    } else {
        Ok(())
    }
}

async fn observe(
    config: ObserverConfig,
) -> Result<Observation<InstanceSnapshot>, OneReplicaScenarioError> {
    let executor = TdsExecutor::new(config.connection().clone());
    SqlServerInstanceManager::new(executor, config)
        .observe()
        .await
        .map_err(|_| OneReplicaScenarioError::Observe)
}

pub fn validate_cli_report(
    report: &Value,
    config: &ObserverConfig,
    now_unix_millis: u64,
) -> Result<u64, OneReplicaScenarioError> {
    let observation = report
        .get("observation")
        .ok_or(OneReplicaScenarioError::CliReport)?;
    let observed_at = observation
        .get("observed_at_unix_millis")
        .and_then(Value::as_u64)
        .ok_or(OneReplicaScenarioError::CliReport)?;
    let instance = observation
        .pointer("/value/instance")
        .ok_or(OneReplicaScenarioError::CliReport)?;
    let replica = serde_json::to_value(&config.target().replica)
        .map_err(|_| OneReplicaScenarioError::CliReport)?;
    let valid = report.get("schema_version").and_then(Value::as_u64) == Some(1)
        && report.get("fresh").and_then(Value::as_bool) == Some(true)
        && report.get("max_age_millis").and_then(Value::as_u64) == Some(60_000)
        && observation.get("status").and_then(Value::as_str) == Some("present")
        && now_unix_millis
            .checked_sub(observed_at)
            .is_some_and(|age| age <= 60_000)
        && report.pointer("/source/host").and_then(Value::as_str)
            == Some(config.connection().endpoint().host())
        && report.pointer("/source/port").and_then(Value::as_u64)
            == Some(u64::from(config.connection().endpoint().port()))
        && report
            .pointer("/source/availability_group")
            .and_then(Value::as_str)
            == Some(config.target().availability_group.as_str())
        && report
            .pointer("/source/expected_server_name")
            .and_then(Value::as_str)
            == Some(config.target().expected_server_name.as_str())
        && report.pointer("/source/replica") == Some(&replica)
        && instance.get("product_version").and_then(Value::as_str)
            == Some(EXPECTED_SQL_SERVER_VERSION)
        && instance
            .get("product_major_version")
            .and_then(Value::as_u64)
            == Some(17)
        && instance.get("edition").and_then(Value::as_str)
            == Some("Enterprise Developer Edition (64-bit)")
        && instance.get("engine_edition").and_then(Value::as_i64) == Some(3)
        && instance.get("hadr_enabled").and_then(Value::as_bool) == Some(true)
        && instance.get("host_platform").and_then(Value::as_str) == Some("Linux")
        && instance.get("architecture").and_then(Value::as_str) == Some("x86_64")
        && instance.get("server_name").and_then(Value::as_str)
            == Some(config.target().expected_server_name.as_str())
        && observation
            .pointer("/value/availability_group/status")
            .and_then(Value::as_str)
            == Some("absent");
    if valid {
        Ok(observed_at)
    } else {
        Err(OneReplicaScenarioError::CliReport)
    }
}
