use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use crate::fixture::model::{
    CombinedFixtureError, FailureCategory, FailureStage, JournalError, ProcessIncarnation,
    ResourceBinding, ResourceKind, ResourceRecord, ResourceState, RunState, SanitizedFailure,
};

pub const JOURNAL_SCHEMA_VERSION: u32 = 7;
const LEGACY_JOURNAL_SCHEMA_VERSION: u32 = 5;
const PREVIOUS_JOURNAL_SCHEMA_VERSION: u32 = 6;

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
    pub sql_start_time: String,
    pub sql_start_unix_millis: i64,
    pub native_replica_id: String,
    pub local_database_id: u32,
    pub database_guid: String,
    pub role: String,
    pub endpoint_url: String,
    pub endpoint_name: String,
    pub endpoint_port: u16,
    pub endpoint_certificate_name: String,
    pub endpoint_certificate_thumbprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlMemberIncarnation {
    pub ordinal: u8,
    pub server_name: String,
    pub container_id: String,
    pub sql_start_time: String,
    pub sql_start_unix_millis: i64,
}

impl SqlMemberIncarnation {
    pub fn verify(
        &self,
        container_id: &str,
        sql_start_time: &str,
        sql_start_unix_millis: i64,
    ) -> Result<(), IncarnationError> {
        if self.container_id != container_id {
            return Err(IncarnationError::ContainerReplaced);
        }
        if self.sql_start_time != sql_start_time
            || self.sql_start_unix_millis != sql_start_unix_millis
        {
            return Err(IncarnationError::SqlRestarted);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncarnationError {
    ContainerReplaced,
    SqlRestarted,
}

impl fmt::Display for IncarnationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ContainerReplaced => "container identity changed",
            Self::SqlRestarted => "SQL Server start identity changed",
        })
    }
}

impl Error for IncarnationError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTopologyBinding {
    pub session_id: String,
    pub availability_group_name: String,
    pub availability_group_id: String,
    pub configuration_sequence: i64,
    pub database_name: String,
    pub group_database_id: String,
    pub family_guid: String,
    pub recovery_fork_id: String,
    pub seeding_operation_ids: [String; 2],
    pub members: [NativeMemberBinding; 3],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTopologyIntent {
    pub session_id: String,
    pub availability_group_name: String,
    pub database_name: String,
    pub endpoint_name: String,
    pub endpoint_port: u16,
    pub members: [NativeMemberIntent; 3],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMemberIntent {
    pub ordinal: u8,
    pub server_name: String,
    pub container_id: String,
    pub sql_start_time: String,
    pub sql_start_unix_millis: i64,
    pub endpoint_certificate_name: String,
    pub peer_login_names: [String; 2],
    pub peer_user_names: [String; 2],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipJournal {
    pub schema_version: u32,
    pub run: TopologyRun,
    pub state: RunState,
    #[serde(default)]
    pub blocked_owner: Option<ProcessIncarnation>,
    #[serde(default)]
    pub blocked_owner_unknown: bool,
    pub sql_member_incarnations: Option<[SqlMemberIncarnation; 3]>,
    pub native_intent: Option<NativeTopologyIntent>,
    pub native_binding: Option<NativeTopologyBinding>,
    pub resources: Vec<ResourceRecord>,
}

impl OwnershipJournal {
    pub fn new(run: TopologyRun) -> Self {
        Self {
            schema_version: JOURNAL_SCHEMA_VERSION,
            run,
            state: RunState::Preparing,
            blocked_owner: None,
            blocked_owner_unknown: false,
            sql_member_incarnations: None,
            native_intent: None,
            native_binding: None,
            resources: Vec::new(),
        }
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, JournalError> {
        let mut journal: Self =
            serde_json::from_slice(bytes).map_err(|_| JournalError::Malformed)?;
        if journal.schema_version == LEGACY_JOURNAL_SCHEMA_VERSION {
            journal.blocked_owner_unknown =
                journal.state == RunState::Blocked && journal.blocked_owner.is_none();
            journal.schema_version = JOURNAL_SCHEMA_VERSION;
        } else if journal.schema_version == PREVIOUS_JOURNAL_SCHEMA_VERSION {
            journal.schema_version = JOURNAL_SCHEMA_VERSION;
        } else if journal.schema_version != JOURNAL_SCHEMA_VERSION {
            return Err(JournalError::UnsupportedSchema(journal.schema_version));
        }
        Ok(journal)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, JournalError> {
        serde_json::to_vec_pretty(self).map_err(|_| JournalError::Malformed)
    }
}
