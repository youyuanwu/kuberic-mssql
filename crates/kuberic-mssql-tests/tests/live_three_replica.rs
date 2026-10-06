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
    let mut launched = launch_three_members(config)
        .await
        .expect("three exact SQL Server members must reach readiness");
    let native = launched.provision_native_topology().await;

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
        let proof = native.as_ref().map_err(|error| {
            eprintln!("native availability-group/data proof failed: {error:?}");
            "native availability-group/data proof failed"
        })?;
        let binding = launched
            .journal()
            .native_binding
            .as_ref()
            .ok_or("native binding was not journaled")?;
        if launched.journal().native_intent.is_none()
            || binding.session_id != launched.run.run_id
            || binding.availability_group_id != proof.topology.evidence.availability_group_id
            || binding.group_database_id != proof.topology.evidence.group_database_id
            || binding.members.len() != 3
            || binding
                .members
                .iter()
                .zip(launched.members.iter())
                .zip(incarnations.iter())
                .any(|((bound, ready), frozen)| {
                    bound.ordinal != ready.ordinal
                        || bound.server_name != ready.server_name
                        || bound.container_id != ready.container_id
                        || bound.sql_start_unix_millis != ready.sql_start_unix_millis
                        || bound.container_id != frozen.container_id
                        || bound.sql_start_unix_millis != frozen.sql_start_unix_millis
                })
            || binding
                .members
                .iter()
                .map(|member| member.native_replica_id.as_str())
                .collect::<BTreeSet<_>>()
                .len()
                != 3
        {
            return Err("native intent or exact IDs were not bound");
        }
        if proof.topology.evidence.primary_ordinal != proof.marker.primary_ordinal
            || proof.marker.readable_ordinals != [1, 2, 3]
            || proof.topology.evidence.members.len() != 3
            || proof
                .topology
                .evidence
                .members
                .iter()
                .filter(|member| member.local_role == "PRIMARY")
                .count()
                != 1
            || proof
                .topology
                .evidence
                .members
                .iter()
                .filter(|member| member.local_role == "SECONDARY")
                .count()
                != 2
        {
            return Err("native roles or replicated marker evidence are incomplete");
        }
        Ok::<(), &'static str>(())
    })();

    let cleanup = launched.cleanup();
    checkpoint.expect("complete native three-member checkpoint");
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
