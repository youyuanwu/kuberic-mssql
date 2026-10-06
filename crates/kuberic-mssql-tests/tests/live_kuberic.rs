//! Licensed single-container Kuberic progress validation; fixture mutation is external.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use kuberic_mssql::instance::SqlServerInstanceManager;
use kuberic_mssql::kuberic::{SqlServerService, SqlServerServiceConfig};
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::tds::TdsExecutor;
use kuberic_mssql::{NativeRole, Observation, PinnedImage};
use kuberic_runtime::application::OpenMode;
use kuberic_runtime::protocol::types::{
    AccessStatus, AgentGeneration, EffectivePolicy, InitializationId, PodUid, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
};
use kuberic_runtime::testing::hosting::PodRuntime;
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{AgentState, SCHEMA_VERSION, StorageIdentity};

struct StoreDirectory(PathBuf);

impl Drop for StoreDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn require_fixture() -> PathBuf {
    assert_eq!(
        std::env::var("SQLSERVER_TEST_EULA_ACCEPTED").as_deref(),
        Ok("true")
    );
    PinnedImage::new(
        std::env::var("SQLSERVER_TEST_IMAGE")
            .expect("fixture owner must identify the digest-pinned image"),
    )
    .expect("fixture image must be pinned by sha256 digest");
    PathBuf::from(
        std::env::var("SQLSERVER_LIVE_AG_CONFIG")
            .expect("SQLSERVER_LIVE_AG_CONFIG must reference the ready fixture"),
    )
}

fn testing_identity() -> (ReplicaIdentity, ResourceUid) {
    let generation = AgentGeneration::new("kuberic-live");
    (
        ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new("kuberic-live-instance"),
            agent_generation: generation.clone(),
        },
        ResourceUid::new(format!("partition-{}", generation.as_str())),
    )
}

fn create_store(root: &Path, local: ReplicaIdentity, resource: ResourceUid) -> Arc<SqliteStore> {
    let database = SqliteStore::metadata_database_path(root);
    let state = AgentState::new(StorageIdentity {
        schema_version: SCHEMA_VERSION,
        resource_uid: resource,
        pod_uid: PodUid::new("kuberic-live-pod"),
        pvc_uid: PvcUid::new("kuberic-live-pvc"),
        initialization_id: InitializationId::new("kuberic-live-initialization"),
        local_identity: local,
        effective_policy: EffectivePolicy::fixed(1, 0).expect("valid singleton testing policy"),
    });
    drop(
        SqliteStore::create_authorized(&database, state)
            .expect("create isolated Kuberic testing store"),
    );
    Arc::new(SqliteStore::open_existing(&database, None).expect("reopen Kuberic testing store"))
}

fn sequence(observation: Observation<InstanceSnapshot>) -> i64 {
    let instance = match observation {
        Observation::Present { value, .. } => value,
        other => panic!("expected a supported SQL Server instance, got {other:?}"),
    };
    match instance.availability_group {
        Observation::Present { value, .. } => {
            assert_eq!(value.local_replica.role, Some(NativeRole::Primary));
            value.configuration_sequence.value()
        }
        other => panic!("expected the fixture availability group, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires the explicitly licensed fixture-owned EXTERNAL availability group"]
async fn live_kuberic_progress_matches_fresh_direct_observation() {
    let config_path = require_fixture();
    let config = ObserverConfig::read(&config_path)
        .await
        .expect("valid fixture configuration");
    let fixture_root = config_path.parent().expect("fixture config has a parent");
    let store_path = fixture_root.join(format!("kuberic-testing-store-{}", std::process::id()));
    std::fs::create_dir(&store_path).expect("create isolated Kuberic testing directory");
    let _store_directory = StoreDirectory(store_path.clone());

    let (local, resource) = testing_identity();
    let store = create_store(&store_path, local.clone(), resource.clone());
    let service = Arc::new(SqlServerService::new(
        SqlServerServiceConfig::new(resource, "kuberic-mssql-observer:5022")
            .expect("valid SQL Server service configuration"),
        SqlServerInstanceManager::new(
            TdsExecutor::new(config.connection().clone()),
            config.clone(),
        ),
    ));
    let runtime = PodRuntime::new(local.clone(), service, store);
    runtime
        .reconstruct(
            OpenMode::Existing,
            ReplicaRole::None,
            AccessStatus::NotPrimary,
            AccessStatus::NotPrimary,
            None,
        )
        .await
        .expect("open and reconstruct SQL Server service through Kuberic");
    let snapshot = runtime.snapshot().await;
    assert!(snapshot.open);
    assert_eq!(snapshot.role, ReplicaRole::None);
    let through_kuberic = snapshot.current_progress;
    assert!(through_kuberic > 0);

    let direct =
        SqlServerInstanceManager::new(TdsExecutor::new(config.connection().clone()), config)
            .observe()
            .await
            .expect("fresh direct observation");
    assert_eq!(through_kuberic, sequence(direct));
}
