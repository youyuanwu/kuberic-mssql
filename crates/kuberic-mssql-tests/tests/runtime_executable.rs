use std::collections::BTreeMap;
use std::future::pending;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kuberic_mssql::runtime_host::{
    RuntimeHostArgs, RuntimeHostConfig, RuntimeProcessError, run_runtime_with_shutdown,
    testing_run_host_with_shutdown,
};
use kuberic_runtime::application::{OpenContext, RoleChange, StatefulServiceReplica};
use kuberic_runtime::host::ApplicationStorageState;
use kuberic_runtime::{Replicator, Result as KubericResult, RuntimeError as KubericRuntimeError};

enum CloseBehavior {
    Error,
    Pending,
}

struct CleanupApplication {
    close_behavior: CloseBehavior,
}

#[async_trait]
impl StatefulServiceReplica for CleanupApplication {
    async fn open(self: Arc<Self>, _context: OpenContext) -> KubericResult<Arc<dyn Replicator>> {
        Err(KubericRuntimeError::Application(
            "cleanup test application must not open".into(),
        ))
    }

    async fn change_role(
        &self,
        _role: kuberic_runtime::protocol::types::ReplicaRole,
    ) -> KubericResult<RoleChange> {
        Err(KubericRuntimeError::Application(
            "cleanup test application has no role".into(),
        ))
    }

    async fn close(&self) -> KubericResult<()> {
        match self.close_behavior {
            CloseBehavior::Error => Err(KubericRuntimeError::Application(
                "injected application cleanup failure".into(),
            )),
            CloseBehavior::Pending => pending().await,
        }
    }

    fn abort(&self) {}
}

fn runtime_binary() -> PathBuf {
    std::env::var_os("NEXTEST_BIN_EXE_kuberic_mssql_runtime_test")
        .or_else(|| std::env::var_os("CARGO_BIN_EXE_kuberic-mssql-runtime-test"))
        .map(PathBuf::from)
        .or_else(|| option_env!("CARGO_BIN_EXE_kuberic-mssql-runtime-test").map(PathBuf::from))
        .expect("kuberic-mssql-runtime-test binary path")
}

fn write_private(path: &Path, value: impl AsRef<[u8]>) {
    std::fs::write(path, value).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

struct RuntimeFiles {
    observer: PathBuf,
    topology: PathBuf,
    routes: PathBuf,
    token: PathBuf,
}

fn files(root: &Path) -> RuntimeFiles {
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
                "observer_password_file":"{}"
            }}"#,
            username.display(),
            password.display()
        ),
    );
    let topology = root.join("topology.json");
    write_private(
        &topology,
        br#"{
            "schema_version":1,
            "members":[
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
            "schema_version":1,
            "routes":[
                {"replica_id":2,"instance_id":"pod-2","agent_generation":"generation-2","control_endpoint":"http://127.0.0.1:51051","replication_endpoint":"http://127.0.0.1:51052"},
                {"replica_id":3,"instance_id":"pod-3","agent_generation":"generation-3","control_endpoint":"http://127.0.0.1:52051","replication_endpoint":"http://127.0.0.1:52052"}
            ]
        }"#,
    );
    let token = root.join("agent-token");
    write_private(&token, "runtime-agent-token");
    RuntimeFiles {
        observer,
        topology,
        routes,
        token,
    }
}

fn free_address_pair() -> (String, String) {
    let control = TcpListener::bind("127.0.0.1:0").unwrap();
    let replication = TcpListener::bind("127.0.0.1:0").unwrap();
    (
        control.local_addr().unwrap().to_string(),
        replication.local_addr().unwrap().to_string(),
    )
}

fn command(root: &Path, files: &RuntimeFiles) -> Command {
    let (control, replication) = free_address_pair();
    command_with_addresses(root, files, &control, &replication)
}

fn command_with_addresses(
    root: &Path,
    files: &RuntimeFiles,
    control_address: &str,
    replication_address: &str,
) -> Command {
    let mut command = Command::new(runtime_binary());
    command
        .current_dir(root)
        .env("KUBERIC_RESOURCE_UID", "resource-a")
        .env("KUBERIC_REPLICA_ID", "1")
        .env("KUBERIC_POD_UID", "pod-1")
        .env("KUBERIC_PVC_UID", "pvc-1")
        .env("KUBERIC_DATA_ROOT", root.join("state"))
        .env("KUBERIC_CONTROL_ADDRESS", control_address)
        .env("KUBERIC_REPLICATION_ADDRESS", replication_address)
        .env("KUBERIC_CONTROL_ENDPOINT", "http://127.0.0.1:50051")
        .env("KUBERIC_REPLICATION_ENDPOINT", "http://127.0.0.1:50052")
        .env("KUBERIC_PEER_ROUTES", &files.routes)
        .env("KUBERIC_MSSQL_OBSERVER_CONFIG", &files.observer)
        .env("KUBERIC_MSSQL_TOPOLOGY_CONFIG", &files.topology)
        .env("KUBERIC_AGENT_BEARER_TOKEN_FILE", &files.token);
    command
}

fn runtime_args(root: &Path, files: &RuntimeFiles) -> RuntimeHostArgs {
    let (control_address, replication_address) = free_address_pair();
    RuntimeHostArgs {
        resource_uid: "resource-a".into(),
        replica_id: 1,
        pod_uid: "pod-1".into(),
        pvc_uid: "pvc-1".into(),
        data_root: root.join("state"),
        application_root: None,
        control_address: control_address.parse().unwrap(),
        replication_address: replication_address.parse().unwrap(),
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

#[test]
fn runtime_help_has_no_inline_secret_mutation_deployment_or_client_surface() {
    let output = Command::new(runtime_binary())
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--resource-uid",
        "--replica-id",
        "--observer-config",
        "--topology-config",
        "--bearer-token-file",
        "--control-address",
        "--replication-address",
    ] {
        assert!(help.contains(required), "{required}");
    }
    for forbidden in [
        "--bearer-token ",
        "--password",
        "--accept-eula",
        "--deploy",
        "--promote",
        "--failover",
        "--application-address",
    ] {
        assert!(!help.contains(forbidden), "{forbidden}");
    }
}

#[test]
fn malformed_topology_fails_without_echoing_external_values() {
    let temporary = tempfile::tempdir().unwrap();
    let files = files(temporary.path());
    write_private(
        &files.topology,
        br#"{"schema_version":1,"unknown":"sensitive-topology-value"}"#,
    );
    let output = command(temporary.path(), &files).output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("topology"));
    assert!(!stderr.contains("sensitive-topology-value"));
    assert!(!stderr.contains("runtime-agent-token"));
}

#[cfg(unix)]
#[tokio::test]
async fn sigint_and_sigterm_cancel_fresh_startup_without_application_state() {
    for signal in ["-INT", "-TERM"] {
        let temporary = tempfile::tempdir().unwrap();
        let files = files(temporary.path());
        let (control_address, replication_address) = free_address_pair();
        let child = tokio::process::Command::from(command_with_addresses(
            temporary.path(),
            &files,
            &control_address,
            &replication_address,
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if tokio::net::TcpStream::connect(&control_address)
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("runtime control listener becomes reachable");
        let pid = child.id().unwrap().to_string();
        assert!(
            tokio::process::Command::new("kill")
                .args([signal, &pid])
                .status()
                .await
                .unwrap()
                .success()
        );
        let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(output.status.success(), "{signal}: {output:?}");
        assert!(output.stdout.is_empty());
        assert!(output.stderr.is_empty());
        assert!(!temporary.path().join("state/application").exists());
    }
}

#[tokio::test]
async fn startup_shutdown_failure_is_preserved_after_host_cleanup() {
    let temporary = tempfile::tempdir().unwrap();
    let files = files(temporary.path());
    let config = RuntimeHostConfig::load(runtime_args(temporary.path(), &files), temporary.path())
        .await
        .unwrap();
    let error = run_runtime_with_shutdown(config, async {
        Err(RuntimeProcessError::new(
            "injected shutdown trigger failure",
        ))
    })
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected shutdown trigger failure")
    );
    assert!(!temporary.path().join("state/application").exists());
}

#[tokio::test]
async fn runner_preserves_trigger_and_application_cleanup_failures() {
    let temporary = tempfile::tempdir().unwrap();
    let files = files(temporary.path());
    let config = RuntimeHostConfig::load(runtime_args(temporary.path(), &files), temporary.path())
        .await
        .unwrap();
    let error = testing_run_host_with_shutdown(
        config,
        Arc::new(CleanupApplication {
            close_behavior: CloseBehavior::Error,
        }),
        ApplicationStorageState::FreshEmpty,
        None,
        async {
            Err(RuntimeProcessError::new(
                "injected shutdown trigger failure",
            ))
        },
    )
    .await
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("injected shutdown trigger failure"));
    assert!(message.contains("injected application cleanup failure"));
}

#[tokio::test]
async fn runner_bounds_application_cleanup_after_shutdown() {
    let temporary = tempfile::tempdir().unwrap();
    let files = files(temporary.path());
    let mut args = runtime_args(temporary.path(), &files);
    args.shutdown_deadline_ms = 20;
    let config = RuntimeHostConfig::load(args, temporary.path())
        .await
        .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        testing_run_host_with_shutdown(
            config,
            Arc::new(CleanupApplication {
                close_behavior: CloseBehavior::Pending,
            }),
            ApplicationStorageState::FreshEmpty,
            Some(BTreeMap::new()),
            async { Ok(()) },
        ),
    )
    .await
    .expect("runtime cleanup remains bounded")
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("runtime application shutdown timed out")
    );
}
