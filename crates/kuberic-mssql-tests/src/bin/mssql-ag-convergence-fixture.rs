use std::error::Error;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql_tests::three_replica::{
    FixtureConfig, LaunchedMembers, PINNED_SQL_SERVER_IMAGE, launch_three_members,
};
use serde_json::json;
use tokio::process::Command;

type TestError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("owned AG convergence fixture: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), TestError> {
    let mut arguments = std::env::args_os().skip(1);
    let root = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("missing fixture root")?;
    let executable = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("missing test executable")?;
    if arguments.next().is_some() || !root.is_absolute() || !executable.is_absolute() {
        return Err(
            "usage: mssql-ag-convergence-fixture <absolute-root> <absolute-test-executable>".into(),
        );
    }
    let mut fixture = launch_three_members(FixtureConfig::for_test_fixture(&root)?).await?;
    let result = execute(&executable, &mut fixture).await;
    let cleanup = fixture.cleanup();
    match (result, cleanup) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Err(error), Err(cleanup)) => {
            Err(format!("{error}; cleanup also failed: {cleanup}").into())
        }
    }
}

async fn execute(executable: &Path, fixture: &mut LaunchedMembers) -> Result<(), TestError> {
    let lab = tempfile::Builder::new().prefix("mssql-ag-lab-").tempdir()?;
    fixture.prepare_availability_group().await?;
    fixture
        .testing_prepare_operation_convergence_database()
        .await?;
    let primary = &fixture.members[0];

    let observer = ObserverConfig::read(&primary.observer_config).await?;
    let nodes = fixture
        .members
        .iter()
        .map(|member| {
            json!({
                "observer_config": member.observer_config,
                "replication_host": member.server_name,
                "replication_port": 5022,
                "mutation_username_file": member.admin_username,
                "mutation_password_file": member.admin_password,
            })
        })
        .collect::<Vec<_>>();
    let document = json!({
        "resource_id": fixture.run.resource_uid,
        "configuration_id": fixture.run.run_id,
        "epoch": 1,
        "primary_replica_id": observer.target().replica.logical_id(),
        "database_name": "ag_convergence_data",
        "journal_path": lab.path().join("ag-convergence.sqlite"),
        "nodes": nodes,
    });
    let path = lab.path().join("ag-lab.json");
    fs::write(&path, serde_json::to_vec(&document)?)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    let mut child = Command::new(executable)
        .args([
            "live_bootstrap_join_seed_and_replay",
            "--ignored",
            "--exact",
            "--nocapture",
        ])
        .env("SQLSERVER_AG_LAB_ALLOW_MUTATIONS", "true")
        .env("SQLSERVER_TEST_EULA_ACCEPTED", "true")
        .env("SQLSERVER_TEST_IMAGE", PINNED_SQL_SERVER_IMAGE)
        .env("SQLSERVER_AG_LAB_CONFIG", path)
        .kill_on_drop(true)
        .spawn()?;
    let status = match tokio::time::timeout(Duration::from_secs(900), child.wait()).await {
        Ok(status) => status?,
        Err(_) => {
            child.kill().await?;
            return Err("migrated live test exceeded its owned fixture budget".into());
        }
    };
    if !status.success() {
        return Err(format!("migrated live test exited with {status}").into());
    }
    Ok(())
}
