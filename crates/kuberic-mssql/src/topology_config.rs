use serde::{Deserialize, Serialize};

use kuberic_runtime::protocol::types::ReplicaId;

use crate::ServerName;

const TOPOLOGY_SCHEMA_VERSION: u32 = 1;
const MAX_TOPOLOGY_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerTopologyMemberExpectation {
    replica_id: ReplicaId,
    server_name: ServerName,
    endpoint_url: String,
}

impl SqlServerTopologyMemberExpectation {
    pub fn new(
        replica_id: ReplicaId,
        server_name: ServerName,
        endpoint_url: impl Into<String>,
    ) -> Result<Self, TopologyConfigError> {
        let endpoint_url = endpoint_url.into();
        if replica_id.value() <= 0 {
            return Err(TopologyConfigError::Invalid);
        }
        validate_endpoint_url(&endpoint_url)?;
        Ok(Self {
            replica_id,
            server_name,
            endpoint_url,
        })
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    pub fn server_name(&self) -> &ServerName {
        &self.server_name
    }

    pub fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerTopologyExpectation {
    local_replica_id: ReplicaId,
    local_logical_replica_id: String,
    members: [SqlServerTopologyMemberExpectation; 3],
}

impl SqlServerTopologyExpectation {
    pub fn new(
        local_replica_id: ReplicaId,
        local_logical_replica_id: impl Into<String>,
        members: Vec<SqlServerTopologyMemberExpectation>,
    ) -> Result<Self, TopologyConfigError> {
        let local_logical_replica_id = local_logical_replica_id.into();
        if local_replica_id.value() <= 0
            || local_logical_replica_id.is_empty()
            || local_logical_replica_id.len() > 256
            || local_logical_replica_id.chars().any(char::is_control)
            || local_logical_replica_id.trim() != local_logical_replica_id
            || members.len() != 3
        {
            return Err(TopologyConfigError::Invalid);
        }
        let mut members = members;
        members.sort_by_key(|member| member.replica_id);
        if members
            .windows(2)
            .any(|pair| pair[0].replica_id == pair[1].replica_id)
            || members.iter().enumerate().any(|(index, member)| {
                members[..index].iter().any(|other| {
                    other.server_name == member.server_name
                        || other.endpoint_url == member.endpoint_url
                })
            })
            || members
                .iter()
                .filter(|member| member.replica_id == local_replica_id)
                .count()
                != 1
        {
            return Err(TopologyConfigError::Invalid);
        }
        Ok(Self {
            local_replica_id,
            local_logical_replica_id,
            members: members
                .try_into()
                .map_err(|_| TopologyConfigError::Invalid)?,
        })
    }

    pub fn from_json(
        bytes: &[u8],
        local_replica_id: ReplicaId,
        local_logical_replica_id: impl Into<String>,
    ) -> Result<Self, TopologyConfigError> {
        if bytes.len() > MAX_TOPOLOGY_BYTES {
            return Err(TopologyConfigError::Invalid);
        }
        let document: TopologyDocument =
            serde_json::from_slice(bytes).map_err(|_| TopologyConfigError::Invalid)?;
        if document.schema_version != TOPOLOGY_SCHEMA_VERSION {
            return Err(TopologyConfigError::UnsupportedSchema(
                document.schema_version,
            ));
        }
        let members = document
            .members
            .into_iter()
            .map(|member| {
                SqlServerTopologyMemberExpectation::new(
                    ReplicaId::new(member.replica_id),
                    ServerName::new(member.server_name)
                        .map_err(|_| TopologyConfigError::Invalid)?,
                    member.endpoint_url,
                )
            })
            .collect::<Result<Vec<_>, TopologyConfigError>>()?;
        Self::new(local_replica_id, local_logical_replica_id, members)
    }

    pub fn canonical_json(&self) -> Vec<u8> {
        serde_json::to_vec(&TopologyDocument {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            members: self
                .members
                .iter()
                .map(|member| TopologyMemberDocument {
                    replica_id: member.replica_id.value(),
                    server_name: member.server_name.as_str().to_owned(),
                    endpoint_url: member.endpoint_url.clone(),
                })
                .collect(),
        })
        .expect("typed topology expectation is serializable")
    }

    pub fn local_replica_id(&self) -> ReplicaId {
        self.local_replica_id
    }

    pub fn local_logical_replica_id(&self) -> &str {
        &self.local_logical_replica_id
    }

    pub fn members(&self) -> &[SqlServerTopologyMemberExpectation; 3] {
        &self.members
    }

    pub fn local_member(&self) -> &SqlServerTopologyMemberExpectation {
        self.members
            .iter()
            .find(|member| member.replica_id == self.local_replica_id)
            .expect("constructor requires the local member")
    }

    pub fn member(&self, replica_id: ReplicaId) -> Option<&SqlServerTopologyMemberExpectation> {
        self.members
            .iter()
            .find(|member| member.replica_id == replica_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyConfigError {
    Invalid,
    UnsupportedSchema(u32),
}

impl std::fmt::Display for TopologyConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid => formatter.write_str("SQL Server topology expectation is invalid"),
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "unsupported SQL Server topology expectation schema version {version}"
                )
            }
        }
    }
}

impl std::error::Error for TopologyConfigError {}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TopologyDocument {
    schema_version: u32,
    members: Vec<TopologyMemberDocument>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TopologyMemberDocument {
    replica_id: i64,
    server_name: String,
    endpoint_url: String,
}

fn validate_endpoint_url(endpoint_url: &str) -> Result<(), TopologyConfigError> {
    if endpoint_url.is_empty()
        || endpoint_url.len() > 1024
        || endpoint_url.chars().any(char::is_control)
        || endpoint_url.trim() != endpoint_url
    {
        Err(TopologyConfigError::Invalid)
    } else {
        Ok(())
    }
}
