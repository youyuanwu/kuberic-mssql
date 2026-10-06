use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyRun {
    pub run_id: String,
    pub resource_uid: String,
    pub members: [SqlMember; 3],
    pub kuberic_members: [KubericMember; 3],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlMember {
    pub ordinal: u8,
    pub server_name: String,
    pub container_name: String,
    pub data_directory: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KubericMember {
    pub ordinal: u8,
    pub replica_id: i64,
    pub instance_id: String,
    pub pod_uid: String,
    pub pvc_uid: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Preparing,
    Ready,
    Cleaning,
    Blocked,
    Removed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Network,
    Container,
    DataDirectory,
    SecretFile,
    AvailabilityGroup,
    Database,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ResourceState {
    Intended,
    Bound { immutable_id: String },
    Cleaning,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRecord {
    pub kind: ResourceKind,
    pub logical_name: String,
    pub path: Option<PathBuf>,
    pub state: ResourceState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipJournal {
    pub schema_version: u32,
    pub run: TopologyRun,
    pub state: RunState,
    pub resources: Vec<ResourceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalError {
    Malformed,
    UnsupportedSchema(u32),
}

impl fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => formatter.write_str("ownership journal is malformed"),
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "unsupported ownership journal schema version {version}"
                )
            }
        }
    }
}

impl Error for JournalError {}

impl OwnershipJournal {
    pub fn new(run: TopologyRun) -> Self {
        Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            run,
            state: RunState::Preparing,
            resources: Vec::new(),
        }
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, JournalError> {
        let journal: Self = serde_json::from_slice(bytes).map_err(|_| JournalError::Malformed)?;
        if journal.schema_version != JOURNAL_SCHEMA_VERSION {
            return Err(JournalError::UnsupportedSchema(journal.schema_version));
        }
        Ok(journal)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, JournalError> {
        serde_json::to_vec_pretty(self).map_err(|_| JournalError::Malformed)
    }
}
