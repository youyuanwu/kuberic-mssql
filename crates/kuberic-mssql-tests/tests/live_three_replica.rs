//! Licensed, explicitly configured three-member SQL Server readiness checkpoint.

use std::collections::BTreeSet;
use std::path::PathBuf;

use kuberic_mssql_tests::three_replica::{
    FixtureConfig, JournalStore, RunState, launch_three_members,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit SQL Server EULA acknowledgement and a qualified local Docker host"]
async fn three_replica_mssql_happy_path() {
    let root = PathBuf::from(
        std::env::var_os("KUBERIC_MSSQL_THREE_REPLICA_ROOT")
            .expect("KUBERIC_MSSQL_THREE_REPLICA_ROOT must be explicitly configured"),
    );
    let acknowledgement = PathBuf::from(
        std::env::var_os("KUBERIC_MSSQL_THREE_REPLICA_EULA_FILE")
            .expect("KUBERIC_MSSQL_THREE_REPLICA_EULA_FILE must be explicitly configured"),
    );
    let config = FixtureConfig::new(&root, acknowledgement).expect("validated fixture config");
    let launched = launch_three_members(config)
        .await
        .expect("three exact SQL Server members must reach readiness");

    let checkpoint = (|| {
        if launched.members.len() != 3 {
            return Err("expected exactly three ready members");
        }
        let container_ids = launched
            .members
            .iter()
            .map(|member| member.container_id.as_str())
            .collect::<BTreeSet<_>>();
        let ports = launched
            .members
            .iter()
            .map(|member| member.host_port)
            .collect::<BTreeSet<_>>();
        if container_ids.len() != 3 || ports.len() != 3 {
            return Err("member container IDs and loopback ports must be distinct");
        }
        let incarnations = launched
            .journal()
            .sql_member_incarnations
            .as_ref()
            .ok_or("SQL member incarnations were not journaled")?;
        for (member, incarnation) in launched.members.iter().zip(incarnations) {
            incarnation
                .verify(&member.container_id, member.sql_start_unix_millis)
                .map_err(|_| "member incarnation binding changed")?;
            if !member.observer_config.is_file() {
                return Err("observer configuration is missing");
            }
        }
        Ok::<(), &'static str>(())
    })();

    let cleanup = launched.cleanup();
    checkpoint.expect("three-member readiness checkpoint");
    let cleanup = cleanup.expect("exact three-member cleanup");
    assert_eq!(cleanup.removed_container_ids.len(), 3);
    let store = JournalStore::initialize(&root).expect("cleanup journal root");
    let journal = store
        .load()
        .expect("cleanup journal")
        .expect("cleanup journal exists");
    assert_eq!(journal.state, RunState::Removed);
    assert!(journal.resources.iter().all(
        |resource| resource.state == kuberic_mssql_tests::three_replica::ResourceState::Removed
    ));
}
