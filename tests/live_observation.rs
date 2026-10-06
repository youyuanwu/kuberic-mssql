//! Explicitly provision fixtures before running with `--ignored`.
//!
//! Set SQLSERVER_TEST_EULA_ACCEPTED=true and identify a digest-pinned image.
//! Each test also requires its named SQLSERVER_LIVE_*_CONFIG path. No fixture is
//! created or mutated by this Rust test binary.

use std::path::PathBuf;

use sqlserver_replicated::instance::SqlServerInstanceManager;
use sqlserver_replicated::observation::InstanceSnapshot;
use sqlserver_replicated::runtime_config::ObserverConfig;
use sqlserver_replicated::tds::TdsExecutor;
use sqlserver_replicated::{NativeRole, Observation, ObservationFailureKind, PinnedImage};

const AG_NAME: &str = "kuberic-progress-ag";
const LOCAL_SERVER: &str = "kuberic-mssql-observer";
const PEER_SERVERS: [&str; 2] = ["kuberic-mssql-peer-1", "kuberic-mssql-peer-2"];

fn fixture_image(image: Option<String>) -> Result<PinnedImage, &'static str> {
    PinnedImage::new(image.ok_or("the fixture image reference is missing")?)
        .map_err(|_| "the fixture image must be pinned by sha256 digest")
}

fn optional_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => panic!("{name} must be valid UTF-8"),
    }
}

async fn observe(config_variable: &str) -> Observation<InstanceSnapshot> {
    assert_eq!(
        std::env::var("SQLSERVER_TEST_EULA_ACCEPTED").as_deref(),
        Ok("true"),
        "live fixture EULA acknowledgement is missing; shared just recipes and the fixture helper set SQLSERVER_TEST_EULA_ACCEPTED=true"
    );
    fixture_image(optional_env("SQLSERVER_TEST_IMAGE"))
        .expect("fixture owner must identify the pinned engine image");
    let path = PathBuf::from(
        std::env::var(config_variable)
            .unwrap_or_else(|_| panic!("{config_variable} must reference a provisioned fixture")),
    );
    let config = ObserverConfig::read(&path)
        .await
        .expect("valid fixture configuration");
    let executor = TdsExecutor::new(config.connection().clone());
    SqlServerInstanceManager::new(executor, config)
        .observe()
        .await
        .expect("valid system clock")
}

#[test]
fn fixture_provenance_requires_an_immutable_container_image() {
    let image = format!(
        "mcr.microsoft.com/mssql/server:2025-CU@sha256:{}",
        "a".repeat(64)
    );
    assert!(fixture_image(Some(image)).is_ok());
    assert!(fixture_image(None).is_err());
    assert!(
        fixture_image(Some(
            "mcr.microsoft.com/mssql/server:2025-latest".to_owned()
        ))
        .is_err()
    );
}
#[tokio::test]
#[ignore = "requires an explicitly licensed SQL Server fixture without the requested AG"]
async fn live_absent_availability_group() {
    let observation = observe("SQLSERVER_LIVE_ABSENT_CONFIG").await;
    match observation {
        Observation::Present { value, .. } => {
            assert!(matches!(
                value.availability_group,
                Observation::Absent { .. }
            ));
        }
        other => panic!("expected supported instance and absent AG, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires an explicitly licensed SQL Server fixture with a preconfigured EXTERNAL AG"]
async fn live_present_availability_group() {
    let observation = observe("SQLSERVER_LIVE_AG_CONFIG").await;
    match observation {
        Observation::Present { value, .. } => {
            let group = match value.availability_group {
                Observation::Present { value, .. } => value,
                other => panic!("expected fixture AG, got {other:?}"),
            };
            assert_eq!(group.identity.name.as_str(), AG_NAME);
            assert!(group.configuration_sequence.value() > 0);
            assert!(group.cluster_type.eq_ignore_ascii_case("EXTERNAL"));
            assert_eq!(group.required_synchronized_secondaries_to_commit, 1);
            assert!(!group.basic_features);
            assert!(!group.is_distributed);
            assert_eq!(group.local_replica.role, Some(NativeRole::Primary));
            assert!(group.local_replica.state_available);
            assert!(group.databases.is_empty());
            assert_eq!(group.replicas.len(), 3);
            let mut replicas = group
                .replicas
                .iter()
                .map(|replica| {
                    assert_eq!(replica.availability_mode, "SYNCHRONOUS_COMMIT");
                    assert_eq!(replica.failover_mode, "EXTERNAL");
                    assert_eq!(replica.seeding_mode, "AUTOMATIC");
                    (
                        replica.server_name.as_str().to_owned(),
                        replica.endpoint_url.clone(),
                    )
                })
                .collect::<Vec<_>>();
            replicas.sort();
            let mut expected = [LOCAL_SERVER, PEER_SERVERS[0], PEER_SERVERS[1]]
                .into_iter()
                .map(|server| (server.to_owned(), Some(format!("TCP://{server}:5022"))))
                .collect::<Vec<_>>();
            expected.sort();
            assert_eq!(replicas, expected);
            for replica in &group.replicas {
                let state = replica
                    .state
                    .as_ref()
                    .expect("fixture replica state row must be visible");
                if replica.server_name.as_str() == LOCAL_SERVER {
                    assert_eq!(state.role, Some(NativeRole::Primary));
                    assert_eq!(state.connected_state.as_deref(), Some("CONNECTED"));
                    assert_eq!(state.operational_state.as_deref(), Some("ONLINE"));
                } else {
                    assert_eq!(state.role, Some(NativeRole::Secondary));
                    assert_eq!(state.connected_state.as_deref(), Some("DISCONNECTED"));
                    assert!(state.operational_state.is_none());
                }
                assert_eq!(state.synchronization_health.as_deref(), Some("NOT_HEALTHY"));
            }
        }
        other => panic!("expected supported instance and present AG, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires a valid TLS/login fixture lacking observation permissions"]
async fn live_permission_denial_is_not_absence() {
    match observe("SQLSERVER_LIVE_DENIED_CONFIG").await {
        Observation::Failed(failure) => {
            assert_eq!(failure.kind, ObservationFailureKind::PermissionDenied);
        }
        other => panic!("expected a permission failure, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires a reachable fixture with a mismatched TLS CA or hostname"]
async fn live_invalid_tls_is_rejected() {
    match observe("SQLSERVER_LIVE_BAD_TLS_CONFIG").await {
        Observation::Failed(failure) => {
            assert_eq!(failure.kind, ObservationFailureKind::Tls);
        }
        other => panic!("expected a TLS verification failure, got {other:?}"),
    }
}
