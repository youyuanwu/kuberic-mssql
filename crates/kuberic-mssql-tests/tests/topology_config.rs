use kuberic_mssql::{
    ServerName, SqlServerTopologyExpectation, SqlServerTopologyMemberExpectation,
    TopologyConfigError,
};
use kuberic_runtime::protocol::types::ReplicaId;

fn valid_json() -> Vec<u8> {
    br#"{
        "schema_version": 1,
        "members": [
            {"replica_id": 3, "server_name": "sql-3", "endpoint_url": "TCP://sql-3:5022"},
            {"replica_id": 1, "server_name": "sql-1", "endpoint_url": "TCP://sql-1:5022"},
            {"replica_id": 2, "server_name": "sql-2", "endpoint_url": "TCP://sql-2:5022"}
        ]
    }"#
    .to_vec()
}

#[test]
fn topology_schema_is_strict_sorted_and_deterministic() {
    let parsed =
        SqlServerTopologyExpectation::from_json(&valid_json(), ReplicaId::new(1), "logical-1")
            .unwrap();
    assert_eq!(parsed.local_replica_id(), ReplicaId::new(1));
    assert_eq!(
        parsed
            .members()
            .iter()
            .map(|member| member.replica_id().value())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );

    let canonical = parsed.canonical_json();
    let reparsed =
        SqlServerTopologyExpectation::from_json(&canonical, ReplicaId::new(1), "logical-1")
            .unwrap();
    assert_eq!(reparsed, parsed);
    assert_eq!(reparsed.canonical_json(), canonical);
}

#[test]
fn topology_rejects_unknown_unsupported_incomplete_and_oversized_documents() {
    let mut value: serde_json::Value = serde_json::from_slice(&valid_json()).unwrap();
    value["unknown"] = serde_json::json!(true);
    assert_eq!(
        SqlServerTopologyExpectation::from_json(
            &serde_json::to_vec(&value).unwrap(),
            ReplicaId::new(1),
            "logical-1",
        ),
        Err(TopologyConfigError::Invalid)
    );

    value.as_object_mut().unwrap().remove("unknown");
    value["schema_version"] = serde_json::json!(2);
    assert_eq!(
        SqlServerTopologyExpectation::from_json(
            &serde_json::to_vec(&value).unwrap(),
            ReplicaId::new(1),
            "logical-1",
        ),
        Err(TopologyConfigError::UnsupportedSchema(2))
    );

    value["schema_version"] = serde_json::json!(1);
    value["members"].as_array_mut().unwrap().pop();
    assert_eq!(
        SqlServerTopologyExpectation::from_json(
            &serde_json::to_vec(&value).unwrap(),
            ReplicaId::new(1),
            "logical-1",
        ),
        Err(TopologyConfigError::Invalid)
    );

    assert_eq!(
        SqlServerTopologyExpectation::from_json(
            &vec![b' '; 65_537],
            ReplicaId::new(1),
            "logical-1"
        ),
        Err(TopologyConfigError::Invalid)
    );
}

#[test]
fn topology_rejects_duplicate_missing_local_and_invalid_member_anchors() {
    let member = |replica_id, server: &str, endpoint: &str| {
        SqlServerTopologyMemberExpectation::new(
            ReplicaId::new(replica_id),
            ServerName::new(server).unwrap(),
            endpoint,
        )
        .unwrap()
    };
    let members = || {
        vec![
            member(1, "sql-1", "TCP://sql-1:5022"),
            member(2, "sql-2", "TCP://sql-2:5022"),
            member(3, "sql-3", "TCP://sql-3:5022"),
        ]
    };

    assert_eq!(
        SqlServerTopologyExpectation::new(ReplicaId::new(4), "logical-4", members()),
        Err(TopologyConfigError::Invalid)
    );
    assert_eq!(
        SqlServerTopologyExpectation::new(ReplicaId::new(1), "", members()),
        Err(TopologyConfigError::Invalid)
    );

    let mut duplicate_id = members();
    duplicate_id[2] = SqlServerTopologyMemberExpectation::new(
        ReplicaId::new(2),
        ServerName::new("sql-3").unwrap(),
        "TCP://sql-3:5022",
    )
    .unwrap();
    assert_eq!(
        SqlServerTopologyExpectation::new(ReplicaId::new(1), "logical-1", duplicate_id),
        Err(TopologyConfigError::Invalid)
    );

    let mut duplicate_server = members();
    duplicate_server[2] = member(3, "sql-2", "TCP://sql-3:5022");
    assert_eq!(
        SqlServerTopologyExpectation::new(ReplicaId::new(1), "logical-1", duplicate_server),
        Err(TopologyConfigError::Invalid)
    );

    let mut duplicate_endpoint = members();
    duplicate_endpoint[2] = member(3, "sql-3", "TCP://sql-2:5022");
    assert_eq!(
        SqlServerTopologyExpectation::new(ReplicaId::new(1), "logical-1", duplicate_endpoint),
        Err(TopologyConfigError::Invalid)
    );

    assert_eq!(
        SqlServerTopologyMemberExpectation::new(
            ReplicaId::new(0),
            ServerName::new("sql-0").unwrap(),
            "TCP://sql-0:5022",
        ),
        Err(TopologyConfigError::Invalid)
    );
    assert_eq!(
        SqlServerTopologyMemberExpectation::new(
            ReplicaId::new(1),
            ServerName::new("sql-1").unwrap(),
            " TCP://sql-1:5022",
        ),
        Err(TopologyConfigError::Invalid)
    );
    assert_eq!(
        SqlServerTopologyMemberExpectation::new(
            ReplicaId::new(1),
            ServerName::new("sql-1").unwrap(),
            "not-an-endpoint",
        ),
        Err(TopologyConfigError::Invalid)
    );
    assert_eq!(
        SqlServerTopologyMemberExpectation::new(
            ReplicaId::new(1),
            ServerName::new("sql-1").unwrap(),
            "TCP://sql-1:0",
        ),
        Err(TopologyConfigError::Invalid)
    );
}

#[test]
fn topology_errors_do_not_echo_external_values() {
    let sensitive = "sensitive-topology-value";
    let bytes = format!(
        r#"{{"schema_version":1,"members":[{{"replica_id":1,"server_name":"sql-1","endpoint_url":" {sensitive}"}}]}}"#
    );
    let error =
        SqlServerTopologyExpectation::from_json(bytes.as_bytes(), ReplicaId::new(1), "logical-1")
            .unwrap_err();
    assert!(!error.to_string().contains(sensitive));
}

#[test]
fn shipped_runtime_topology_example_is_valid() {
    let topology = SqlServerTopologyExpectation::from_json(
        include_bytes!("../../../runtime-topology.example.json"),
        ReplicaId::new(1),
        "logical-1",
    )
    .unwrap();
    assert_eq!(topology.members().len(), 3);
}
