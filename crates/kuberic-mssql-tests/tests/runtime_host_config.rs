use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use kuberic_mssql::runtime_host::{
    PeerRoutes, PeerRoutesError, ResolverConfig, RuntimeBindingError, RuntimeBindingStore,
    RuntimeHostArgs, RuntimeHostConfig, RuntimeHostConfigError,
};
use kuberic_runtime::host::ApplicationStorageState;

struct Files {
    observer: PathBuf,
    topology: PathBuf,
    routes: PathBuf,
    token: PathBuf,
}

fn write_private(path: &Path, value: impl AsRef<[u8]>) {
    fs::write(path, value).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn files(root: &Path) -> Files {
    let username = root.join("observer-username");
    let password = root.join("observer-password");
    write_private(&username, "observer");
    write_private(&password, "password");

    let observer = root.join("observer.json");
    write_private(
        &observer,
        format!(
            r#"{{
                "host":"sql-1.example",
                "port":1433,
                "availability_group":"app-ag",
                "expected_server_name":"sql-1",
                "replica_id":"logical-1",
                "incarnation":"pod-1",
                "observer_username_file":"{}",
                "observer_password_file":"{}",
                "sample_timeout_ms":30000,
                "connect_timeout_ms":5000,
                "query_timeout_ms":5000,
                "poll_interval_ms":1000,
                "max_age_ms":60000
            }}"#,
            username.display(),
            password.display()
        ),
    );

    let topology = root.join("topology.json");
    write_private(
        &topology,
        br#"{
            "schema_version": 1,
            "members": [
                {"replica_id":1,"server_name":"sql-1","endpoint_url":"TCP://sql-1:5022"},
                {"replica_id":2,"server_name":"sql-2","endpoint_url":"TCP://sql-2:5022"},
                {"replica_id":3,"server_name":"sql-3","endpoint_url":"TCP://sql-3:5022"}
            ]
        }"#,
    );

    let routes = root.join("routes.json");
    write_private(
        &routes,
        br#"{
            "schema_version": 1,
            "routes": [
                {
                    "replica_id": 2,
                    "instance_id": "pod-2",
                    "agent_generation": "generation-2",
                    "control_endpoint": "http://127.0.0.1:51051",
                    "replication_endpoint": "http://127.0.0.1:51052"
                },
                {
                    "replica_id": 3,
                    "instance_id": "pod-3",
                    "agent_generation": "generation-3",
                    "control_endpoint": "http://127.0.0.1:52051",
                    "replication_endpoint": "http://127.0.0.1:52052"
                }
            ]
        }"#,
    );

    let token = root.join("agent-token");
    write_private(&token, "agent-token-value");

    Files {
        observer,
        topology,
        routes,
        token,
    }
}

fn args(files: &Files) -> RuntimeHostArgs {
    RuntimeHostArgs {
        resource_uid: "resource-a".into(),
        replica_id: 1,
        pod_uid: "pod-1".into(),
        pvc_uid: "pvc-1".into(),
        data_root: PathBuf::from("state"),
        application_root: Some(PathBuf::from("application")),
        control_address: "127.0.0.1:50051".parse::<SocketAddr>().unwrap(),
        replication_address: "127.0.0.1:50052".parse::<SocketAddr>().unwrap(),
        control_endpoint: "http://127.0.0.1:50051".into(),
        replication_endpoint: "http://127.0.0.1:50052".into(),
        namespace: None,
        peer_routes: Some(files.routes.clone()),
        observer_config: files.observer.clone(),
        topology_config: files.topology.clone(),
        bearer_token_file: files.token.clone(),
        rpc_deadline_ms: 5_000,
        transport_window_capacity: 256,
        shutdown_deadline_ms: 10_000,
    }
}

async fn config(root: &Path) -> RuntimeHostConfig {
    let files = files(root);
    RuntimeHostConfig::load(args(&files), root).await.unwrap()
}

#[tokio::test]
async fn runtime_config_loads_strict_file_backed_inputs_and_redacts_secrets() {
    let temporary = tempfile::tempdir().unwrap();
    let files = files(temporary.path());
    let config = RuntimeHostConfig::load(args(&files), temporary.path())
        .await
        .unwrap();

    assert_eq!(config.resource_uid().as_str(), "resource-a");
    assert_eq!(config.replica_id().value(), 1);
    assert_eq!(config.data_root(), temporary.path().join("state"));
    assert_eq!(
        config.application_root(),
        temporary.path().join("application")
    );
    assert_eq!(config.bearer_token(), "agent-token-value");
    assert!(matches!(config.resolver(), ResolverConfig::PeerRoutes(_)));
    let debug = format!("{config:?}");
    assert!(!debug.contains("agent-token-value"));
    assert!(!debug.contains("password"));
}

#[tokio::test]
async fn runtime_config_rejects_bad_secret_resolver_endpoint_and_deadline_inputs() {
    let temporary = tempfile::tempdir().unwrap();
    let files = files(temporary.path());

    let mut invalid = args(&files);
    invalid.namespace = Some("default".into());
    assert_eq!(
        RuntimeHostConfig::load(invalid, temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::InvalidResolver
    );

    let mut invalid = args(&files);
    invalid.peer_routes = None;
    assert_eq!(
        RuntimeHostConfig::load(invalid, temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::InvalidResolver
    );

    let mut dns = args(&files);
    dns.peer_routes = None;
    dns.namespace = Some("default".into());
    assert!(matches!(
        RuntimeHostConfig::load(dns, temporary.path())
            .await
            .unwrap()
            .resolver(),
        ResolverConfig::KubernetesDns { namespace } if namespace == "default"
    ));

    let mut invalid = args(&files);
    invalid.peer_routes = None;
    invalid.namespace = Some("bad.namespace".into());
    assert_eq!(
        RuntimeHostConfig::load(invalid, temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::InvalidResolver
    );

    let mut invalid = args(&files);
    invalid.control_endpoint = "not-an-endpoint".into();
    assert_eq!(
        RuntimeHostConfig::load(invalid, temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::InvalidEndpoint
    );

    let mut invalid = args(&files);
    invalid.rpc_deadline_ms = 0;
    assert_eq!(
        RuntimeHostConfig::load(invalid, temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::InvalidDuration
    );

    let observer_target = temporary.path().join("observer-target.json");
    fs::copy(&files.observer, &observer_target).unwrap();
    fs::remove_file(&files.observer).unwrap();
    symlink(&observer_target, &files.observer).unwrap();
    assert_eq!(
        RuntimeHostConfig::load(args(&files), temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::InvalidPath
    );
    fs::remove_file(&files.observer).unwrap();
    fs::copy(&observer_target, &files.observer).unwrap();

    write_private(&files.token, vec![b'x'; 4_097]);
    assert_eq!(
        RuntimeHostConfig::load(args(&files), temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::Secret
    );

    write_private(&files.token, "agent-token-value");
    fs::set_permissions(&files.token, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        RuntimeHostConfig::load(args(&files), temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::Secret
    );
    fs::set_permissions(&files.token, fs::Permissions::from_mode(0o600)).unwrap();

    write_private(&files.token, "token\n");
    let error = RuntimeHostConfig::load(args(&files), temporary.path())
        .await
        .unwrap_err();
    assert_eq!(error, RuntimeHostConfigError::Secret);
    assert!(!error.to_string().contains("agent-token-value"));

    fs::remove_file(&files.token).unwrap();
    let target = temporary.path().join("token-target");
    write_private(&target, "agent-token-value");
    symlink(&target, &files.token).unwrap();
    assert_eq!(
        RuntimeHostConfig::load(args(&files), temporary.path())
            .await
            .unwrap_err(),
        RuntimeHostConfigError::Secret
    );
}

#[tokio::test]
async fn peer_routes_are_bounded_strict_and_sanitized() {
    let valid = br#"{
        "schema_version":1,
        "routes":[{
            "replica_id":2,
            "instance_id":"pod-2",
            "agent_generation":"generation-2",
            "control_endpoint":"http://127.0.0.1:51051",
            "replication_endpoint":"http://127.0.0.1:51052"
        }]
    }"#;
    let routes = PeerRoutes::from_json(valid).unwrap();
    assert_eq!(routes.len(), 1);

    let mut unknown: serde_json::Value = serde_json::from_slice(valid).unwrap();
    unknown["unknown"] = serde_json::json!("sensitive-route-value");
    let error = PeerRoutes::from_json(&serde_json::to_vec(&unknown).unwrap()).unwrap_err();
    assert_eq!(error, PeerRoutesError::Invalid);
    assert!(!error.to_string().contains("sensitive-route-value"));

    let duplicate = br#"{
        "schema_version":1,
        "routes":[
            {"replica_id":2,"instance_id":"pod-2","agent_generation":"generation-2","control_endpoint":"http://127.0.0.1:51051","replication_endpoint":"http://127.0.0.1:51052"},
            {"replica_id":2,"instance_id":"pod-2","agent_generation":"generation-2","control_endpoint":"http://127.0.0.1:52051","replication_endpoint":"http://127.0.0.1:52052"}
        ]
    }"#;
    assert_eq!(
        PeerRoutes::from_json(duplicate),
        Err(PeerRoutesError::Invalid)
    );

    let path_endpoint = br#"{
        "schema_version":1,
        "routes":[{
            "replica_id":2,
            "instance_id":"pod-2",
            "agent_generation":"generation-2",
            "control_endpoint":"http://host:80/path:81",
            "replication_endpoint":"http://127.0.0.1:51052"
        }]
    }"#;
    assert_eq!(
        PeerRoutes::from_json(path_endpoint),
        Err(PeerRoutesError::Invalid)
    );

    assert_eq!(
        PeerRoutes::from_json(&vec![b' '; 65_537]),
        Err(PeerRoutesError::Invalid)
    );

    let routes = (1..=33)
        .map(|replica_id| {
            serde_json::json!({
                "replica_id": replica_id,
                "instance_id": format!("pod-{replica_id}"),
                "agent_generation": format!("generation-{replica_id}"),
                "control_endpoint": format!("http://127.0.0.1:{}", 10_000 + replica_id),
                "replication_endpoint": format!("http://127.0.0.1:{}", 20_000 + replica_id)
            })
        })
        .collect::<Vec<_>>();
    let too_many = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "routes": routes
    }))
    .unwrap();
    assert_eq!(
        PeerRoutes::from_json(&too_many),
        Err(PeerRoutesError::Invalid)
    );

    let temporary = tempfile::tempdir().unwrap();
    let target = temporary.path().join("routes-target.json");
    let link = temporary.path().join("routes-link.json");
    write_private(&target, valid);
    symlink(&target, &link).unwrap();
    assert_eq!(
        PeerRoutes::read(&link).await,
        Err(PeerRoutesError::Unavailable)
    );
}

#[tokio::test]
async fn runtime_binding_is_durable_secret_free_and_rejects_identity_drift() {
    let temporary = tempfile::tempdir().unwrap();
    let runtime_config = config(temporary.path()).await;
    let store = RuntimeBindingStore::new(&runtime_config).unwrap();
    assert_eq!(
        store.storage_state().unwrap(),
        ApplicationStorageState::FreshEmpty
    );
    store.initialize().unwrap();
    assert_eq!(
        store.storage_state().unwrap(),
        ApplicationStorageState::Established
    );
    store.validate().unwrap();
    let bytes = fs::read(store.path()).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains("agent-token-value"));
    assert!(!text.contains("password"));
    assert_eq!(
        fs::metadata(store.path()).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let files = files(temporary.path());
    let mut changed_args = args(&files);
    changed_args.resource_uid = "resource-b".into();
    let changed = RuntimeHostConfig::load(changed_args, temporary.path())
        .await
        .unwrap();
    let changed_store = RuntimeBindingStore::new(&changed).unwrap();
    assert_eq!(
        changed_store.storage_state(),
        Err(RuntimeBindingError::IdentityMismatch)
    );

    let original_digest = store.expected().topology_sha256().to_owned();
    write_private(
        &files.topology,
        br#"{
            "schema_version": 1,
            "members": [
                {"replica_id":3,"server_name":"sql-3","endpoint_url":"TCP://sql-3:5022"},
                {"replica_id":1,"server_name":"sql-1","endpoint_url":"TCP://sql-1:5022"},
                {"replica_id":2,"server_name":"sql-2","endpoint_url":"TCP://sql-2:5022"}
            ]
        }"#,
    );
    let reordered = RuntimeHostConfig::load(args(&files), temporary.path())
        .await
        .unwrap();
    let reordered_store = RuntimeBindingStore::new(&reordered).unwrap();
    assert_eq!(
        reordered_store.expected().topology_sha256(),
        original_digest
    );
    assert_eq!(
        reordered_store.storage_state().unwrap(),
        ApplicationStorageState::Established
    );

    write_private(
        &files.topology,
        br#"{
            "schema_version": 1,
            "members": [
                {"replica_id":1,"server_name":"sql-1","endpoint_url":"TCP://sql-1:5022"},
                {"replica_id":2,"server_name":"sql-2","endpoint_url":"TCP://sql-2:5022"},
                {"replica_id":3,"server_name":"sql-3","endpoint_url":"TCP://sql-3-new:5022"}
            ]
        }"#,
    );
    let changed_topology = RuntimeHostConfig::load(args(&files), temporary.path())
        .await
        .unwrap();
    assert_eq!(
        RuntimeBindingStore::new(&changed_topology)
            .unwrap()
            .storage_state(),
        Err(RuntimeBindingError::IdentityMismatch)
    );

    write_private(
        &files.topology,
        br#"{
            "schema_version": 1,
            "members": [
                {"replica_id":1,"server_name":"sql-1","endpoint_url":"TCP://sql-1:5022"},
                {"replica_id":2,"server_name":"sql-2","endpoint_url":"TCP://sql-2:5022"},
                {"replica_id":3,"server_name":"sql-3","endpoint_url":"TCP://sql-3:5022"}
            ]
        }"#,
    );
    let moved_root = temporary.path().join("moved-application");
    fs::create_dir(&moved_root).unwrap();
    fs::set_permissions(&moved_root, fs::Permissions::from_mode(0o700)).unwrap();
    let moved_binding = moved_root.join("runtime-binding.json");
    fs::copy(store.path(), &moved_binding).unwrap();
    fs::set_permissions(&moved_binding, fs::Permissions::from_mode(0o600)).unwrap();
    let mut moved_args = args(&files);
    moved_args.application_root = Some(moved_root);
    let moved = RuntimeHostConfig::load(moved_args, temporary.path())
        .await
        .unwrap();
    assert_eq!(
        RuntimeBindingStore::new(&moved).unwrap().storage_state(),
        Err(RuntimeBindingError::IdentityMismatch)
    );

    let original_observer = fs::read_to_string(&files.observer).unwrap();
    write_private(
        &files.observer,
        original_observer.replace("\"app-ag\"", "\"other-ag\""),
    );
    let changed_observer = RuntimeHostConfig::load(args(&files), temporary.path())
        .await
        .unwrap();
    assert_eq!(
        RuntimeBindingStore::new(&changed_observer)
            .unwrap()
            .storage_state(),
        Err(RuntimeBindingError::IdentityMismatch)
    );

    write_private(
        &files.observer,
        original_observer
            .replace("sql-1.example", "sql-2.example")
            .replace("\"sql-1\"", "\"sql-2\"")
            .replace("\"logical-1\"", "\"logical-2\"")
            .replace("\"pod-1\"", "\"pod-2\""),
    );
    write_private(
        &files.routes,
        br#"{
            "schema_version": 1,
            "routes": [
                {
                    "replica_id": 1,
                    "instance_id": "pod-1",
                    "agent_generation": "generation-1",
                    "control_endpoint": "http://127.0.0.1:50051",
                    "replication_endpoint": "http://127.0.0.1:50052"
                },
                {
                    "replica_id": 3,
                    "instance_id": "pod-3",
                    "agent_generation": "generation-3",
                    "control_endpoint": "http://127.0.0.1:52051",
                    "replication_endpoint": "http://127.0.0.1:52052"
                }
            ]
        }"#,
    );
    let mut changed_replica_args = args(&files);
    changed_replica_args.replica_id = 2;
    changed_replica_args.pod_uid = "pod-2".into();
    changed_replica_args.pvc_uid = "pvc-2".into();
    let changed_replica = RuntimeHostConfig::load(changed_replica_args, temporary.path())
        .await
        .unwrap();
    assert_eq!(
        RuntimeBindingStore::new(&changed_replica)
            .unwrap()
            .storage_state(),
        Err(RuntimeBindingError::IdentityMismatch)
    );
}

#[tokio::test]
async fn runtime_binding_rejects_extra_malformed_unsupported_and_symlinked_state() {
    let temporary = tempfile::tempdir().unwrap();
    let runtime_config = config(temporary.path()).await;
    let store = RuntimeBindingStore::new(&runtime_config).unwrap();
    store.initialize().unwrap();

    fs::set_permissions(store.path(), fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(store.storage_state(), Err(RuntimeBindingError::UnsafeState));
    fs::set_permissions(store.path(), fs::Permissions::from_mode(0o600)).unwrap();

    fs::write(store.root().join("foreign"), "foreign").unwrap();
    assert_eq!(store.storage_state(), Err(RuntimeBindingError::UnsafeState));
    fs::remove_file(store.root().join("foreign")).unwrap();

    let original = fs::read(store.path()).unwrap();
    fs::write(store.path(), b"{").unwrap();
    assert_eq!(store.storage_state(), Err(RuntimeBindingError::Malformed));

    let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
    value["schema_version"] = serde_json::json!(2);
    fs::write(store.path(), serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(
        store.storage_state(),
        Err(RuntimeBindingError::UnsupportedSchema(2))
    );

    let other = tempfile::tempdir().unwrap();
    let link = other.path().join("application-link");
    symlink(store.root(), &link).unwrap();
    let files = files(other.path());
    let mut linked_args = args(&files);
    linked_args.application_root = Some(link);
    let linked = RuntimeHostConfig::load(linked_args, other.path())
        .await
        .unwrap();
    assert_eq!(
        RuntimeBindingStore::new(&linked).unwrap_err(),
        RuntimeBindingError::InvalidPath
    );

    let unsafe_root = tempfile::tempdir().unwrap();
    let unsafe_config = config(unsafe_root.path()).await;
    fs::create_dir(unsafe_config.application_root()).unwrap();
    fs::set_permissions(
        unsafe_config.application_root(),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    assert_eq!(
        RuntimeBindingStore::new(&unsafe_config)
            .unwrap()
            .storage_state(),
        Err(RuntimeBindingError::UnsafeState)
    );
}

#[tokio::test]
async fn runtime_binding_recovers_only_stale_private_initialization_files() {
    let temporary = tempfile::tempdir().unwrap();
    let runtime_config = config(temporary.path()).await;
    let store = RuntimeBindingStore::new(&runtime_config).unwrap();
    fs::create_dir(store.root()).unwrap();
    fs::set_permissions(store.root(), fs::Permissions::from_mode(0o700)).unwrap();

    let stale = store.root().join(".runtime-binding.json.2147483647.new");
    write_private(&stale, "incomplete");
    assert_eq!(
        store.storage_state().unwrap(),
        ApplicationStorageState::FreshEmpty
    );
    assert!(!stale.exists());
    store.initialize().unwrap();

    fs::remove_file(store.path()).unwrap();
    let active = store
        .root()
        .join(format!(".runtime-binding.json.{}.new", std::process::id()));
    write_private(&active, "incomplete");
    assert_eq!(store.storage_state(), Err(RuntimeBindingError::UnsafeState));
    assert!(active.exists());
}

#[test]
fn shipped_peer_routes_example_is_valid() {
    let routes =
        PeerRoutes::from_json(include_bytes!("../../../runtime-peer-routes.example.json")).unwrap();
    assert_eq!(routes.len(), 2);
}
