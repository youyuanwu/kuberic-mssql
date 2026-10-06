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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMemberBinding {
    pub ordinal: u8,
    pub server_name: String,
    pub container_id: String,
    pub sql_start_unix_millis: i64,
    pub native_replica_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTopologyBinding {
    pub availability_group_id: String,
    pub database_id: String,
    pub recovery_fork_id: String,
    pub members: [NativeMemberBinding; 3],
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
#[serde(rename_all = "snake_case")]
pub enum ResourceState {
    Intended,
    Dispatched,
    Bound,
    Cleaning,
    Blocked,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBinding {
    pub immutable_id: String,
    pub attributes_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRecord {
    pub kind: ResourceKind,
    pub logical_name: String,
    pub path: Option<PathBuf>,
    pub binding: Option<ResourceBinding>,
    pub state: ResourceState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipJournal {
    pub schema_version: u32,
    pub run: TopologyRun,
    pub state: RunState,
    pub native_binding: Option<NativeTopologyBinding>,
    pub resources: Vec<ResourceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalError {
    Malformed,
    UnsupportedSchema(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureStage {
    Setup,
    Test,
    Cleanup,
}

impl fmt::Display for FailureStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Setup => "setup",
            Self::Test => "test",
            Self::Cleanup => "cleanup",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureCategory {
    Preflight,
    ContainerCreation,
    ContainerRemoval,
    NetworkRemoval,
    PathRemoval,
    OwnershipMismatch,
    DeadlineExceeded,
    SqlUnavailable,
    Journal,
}

impl fmt::Display for FailureCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Preflight => "preflight failed",
            Self::ContainerCreation => "container creation failed",
            Self::ContainerRemoval => "container removal failed",
            Self::NetworkRemoval => "network removal failed",
            Self::PathRemoval => "path removal failed",
            Self::OwnershipMismatch => "ownership validation failed",
            Self::DeadlineExceeded => "stage deadline exceeded",
            Self::SqlUnavailable => "SQL Server unavailable",
            Self::Journal => "ownership journal update failed",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SanitizedFailure {
    stage: FailureStage,
    category: FailureCategory,
}

impl SanitizedFailure {
    pub const fn new(stage: FailureStage, category: FailureCategory) -> Self {
        Self { stage, category }
    }
}

impl fmt::Display for SanitizedFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.stage, self.category)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombinedFixtureError {
    primary: SanitizedFailure,
    cleanup: Vec<SanitizedFailure>,
}

impl CombinedFixtureError {
    pub fn new(primary: SanitizedFailure, cleanup: Vec<SanitizedFailure>) -> Self {
        Self { primary, cleanup }
    }

    pub fn primary(&self) -> SanitizedFailure {
        self.primary
    }

    pub fn cleanup(&self) -> &[SanitizedFailure] {
        &self.cleanup
    }
}

impl fmt::Display for CombinedFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.primary)?;
        for cleanup in &self.cleanup {
            write!(formatter, "; {cleanup}")?;
        }
        Ok(())
    }
}

impl Error for CombinedFixtureError {}

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
            native_binding: None,
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
