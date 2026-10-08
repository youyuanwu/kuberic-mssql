use std::collections::BTreeMap;
use std::path::Path;

use kuberic_runtime::protocol::types::{
    AgentGeneration, ReplicaId, ReplicaIdentity, ReplicaInstanceId,
};
use serde::Deserialize;

const PEER_ROUTES_SCHEMA_VERSION: u32 = 1;
const MAX_PEER_ROUTES_BYTES: u64 = 65_536;
const MAX_PEER_ROUTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRoute {
    identity: ReplicaIdentity,
    control_endpoint: String,
    replication_endpoint: String,
}

impl PeerRoute {
    pub fn identity(&self) -> &ReplicaIdentity {
        &self.identity
    }

    pub fn control_endpoint(&self) -> &str {
        &self.control_endpoint
    }

    pub fn replication_endpoint(&self) -> &str {
        &self.replication_endpoint
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRoutes {
    routes: BTreeMap<ReplicaIdentity, PeerRoute>,
}

impl PeerRoutes {
    pub async fn read(path: &Path) -> Result<Self, PeerRoutesError> {
        let metadata = tokio::fs::symlink_metadata(path)
            .await
            .map_err(|_| PeerRoutesError::Unavailable)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_PEER_ROUTES_BYTES
        {
            return Err(PeerRoutesError::Invalid);
        }
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| PeerRoutesError::Unavailable)?;
        Self::from_json(&bytes)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, PeerRoutesError> {
        if bytes.len() as u64 > MAX_PEER_ROUTES_BYTES {
            return Err(PeerRoutesError::Invalid);
        }
        let document: PeerRoutesDocument =
            serde_json::from_slice(bytes).map_err(|_| PeerRoutesError::Invalid)?;
        if document.schema_version != PEER_ROUTES_SCHEMA_VERSION {
            return Err(PeerRoutesError::UnsupportedSchema(document.schema_version));
        }
        if document.routes.is_empty() || document.routes.len() > MAX_PEER_ROUTES {
            return Err(PeerRoutesError::Invalid);
        }
        let mut routes = BTreeMap::new();
        for route in document.routes {
            if route.replica_id <= 0
                || !valid_identity_component(&route.instance_id)
                || !valid_identity_component(&route.agent_generation)
            {
                return Err(PeerRoutesError::Invalid);
            }
            validate_http_endpoint(&route.control_endpoint)?;
            validate_http_endpoint(&route.replication_endpoint)?;
            let identity = ReplicaIdentity {
                replica_id: ReplicaId::new(route.replica_id),
                instance_id: ReplicaInstanceId::new(route.instance_id),
                agent_generation: AgentGeneration::new(route.agent_generation),
            };
            let route = PeerRoute {
                identity: identity.clone(),
                control_endpoint: route.control_endpoint,
                replication_endpoint: route.replication_endpoint,
            };
            if routes.insert(identity, route).is_some() {
                return Err(PeerRoutesError::Invalid);
            }
        }
        Ok(Self { routes })
    }

    pub fn get(&self, identity: &ReplicaIdentity) -> Option<&PeerRoute> {
        self.routes.get(identity)
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub fn values(&self) -> impl Iterator<Item = &PeerRoute> {
        self.routes.values()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRoutesError {
    Unavailable,
    Invalid,
    UnsupportedSchema(u32),
}

impl std::fmt::Display for PeerRoutesError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("peer routes are unavailable"),
            Self::Invalid => formatter.write_str("peer routes are invalid"),
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "unsupported peer routes schema version {version}"
                )
            }
        }
    }
}

impl std::error::Error for PeerRoutesError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerRoutesDocument {
    schema_version: u32,
    routes: Vec<PeerRouteDocument>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerRouteDocument {
    replica_id: i64,
    instance_id: String,
    agent_generation: String,
    control_endpoint: String,
    replication_endpoint: String,
}

fn valid_identity_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

pub(crate) fn validate_http_endpoint(endpoint: &str) -> Result<(), PeerRoutesError> {
    if endpoint.len() > 512
        || endpoint.trim() != endpoint
        || endpoint.chars().any(char::is_control)
        || !endpoint.starts_with("http://")
    {
        return Err(PeerRoutesError::Invalid);
    }
    let Some((host, port)) = endpoint[7..].rsplit_once(':') else {
        return Err(PeerRoutesError::Invalid);
    };
    if host.is_empty()
        || host.chars().any(char::is_whitespace)
        || port.parse::<u16>().ok().is_none_or(|port| port == 0)
    {
        return Err(PeerRoutesError::Invalid);
    }
    Ok(())
}
