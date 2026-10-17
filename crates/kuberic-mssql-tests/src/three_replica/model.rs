use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::fixture::cleanup::{CleanupJournal, CleanupJournalStore};
use crate::fixture::ownership::{
    JournalDocument, JournalStore, NewJournal, ProcessOwnedJournal, ReconcileError,
};

pub use crate::fixture::model::{
    CombinedFixtureError, FailureCategory, FailureStage, JournalError, ProcessIncarnation,
    ResourceBinding, ResourceKind, ResourceRecord, ResourceState, RunState, SanitizedFailure,
};

pub const JOURNAL_SCHEMA_VERSION: u32 = 9;
const LEGACY_JOURNAL_SCHEMA_VERSION: u32 = 5;
const PREVIOUS_JOURNAL_SCHEMA_VERSIONS: [u32; 3] = [6, 7, 8];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecyclePhase {
    StopPending,
    Stopped,
    StartPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleExclusion {
    pub container_id: String,
    pub phase: LifecyclePhase,
}

impl LifecycleExclusion {
    pub fn permits_restart(&self, container_id: &str) -> bool {
        self.container_id == container_id && self.phase == LifecyclePhase::Stopped
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeTopologyPhase {
    #[default]
    Absent,
    Preparing,
    Prepared,
    Converged,
}

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
    #[serde(default)]
    pub native_phase: NativeTopologyPhase,
    pub native_intent: Option<NativeTopologyIntent>,
    pub native_binding: Option<NativeTopologyBinding>,
    #[serde(default)]
    pub lifecycle_exclusion: Option<LifecycleExclusion>,
    pub resources: Vec<ResourceRecord>,
}

impl CleanupJournal for OwnershipJournal {
    fn state_mut(&mut self) -> &mut RunState {
        &mut self.state
    }

    fn resources(&self) -> &[ResourceRecord] {
        &self.resources
    }

    fn resources_mut(&mut self) -> &mut Vec<ResourceRecord> {
        &mut self.resources
    }

    fn clear_blocked_owner(&mut self) {
        self.blocked_owner = None;
        self.blocked_owner_unknown = false;
    }
}

impl JournalDocument for OwnershipJournal {
    fn from_json(bytes: &[u8]) -> Result<Self, JournalError> {
        Self::from_json(bytes)
    }

    fn to_json(&self) -> Result<Vec<u8>, JournalError> {
        self.to_json()
    }
}

impl NewJournal<TopologyRun> for OwnershipJournal {
    fn new_journal(run: TopologyRun) -> Self {
        Self::new(run)
    }
}

impl ProcessOwnedJournal for OwnershipJournal {
    fn block_for_owner(&mut self, owner: ProcessIncarnation) {
        self.blocked_owner = Some(owner);
        self.blocked_owner_unknown = false;
        self.state = RunState::Blocked;
    }
}

impl CleanupJournalStore<OwnershipJournal> for JournalStore {
    fn save_cleanup_journal(&self, journal: &OwnershipJournal) -> Result<(), ReconcileError> {
        self.save(journal)
    }
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
            native_phase: NativeTopologyPhase::Absent,
            native_intent: None,
            native_binding: None,
            lifecycle_exclusion: None,
            resources: Vec::new(),
        }
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, JournalError> {
        let document: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| JournalError::Malformed)?;
        let schema = document
            .get("schema_version")
            .and_then(serde_json::Value::as_u64);
        if schema.is_some_and(|version| version >= 8) && document.get("native_phase").is_none() {
            return Err(JournalError::Malformed);
        }
        if schema == Some(u64::from(JOURNAL_SCHEMA_VERSION))
            && document.get("lifecycle_exclusion").is_none()
        {
            return Err(JournalError::Malformed);
        }
        let mut journal: Self =
            serde_json::from_slice(bytes).map_err(|_| JournalError::Malformed)?;
        if journal.schema_version == LEGACY_JOURNAL_SCHEMA_VERSION {
            journal.blocked_owner_unknown =
                journal.state == RunState::Blocked && journal.blocked_owner.is_none();
            journal.schema_version = JOURNAL_SCHEMA_VERSION;
            journal.infer_legacy_native_phase();
        } else if PREVIOUS_JOURNAL_SCHEMA_VERSIONS.contains(&journal.schema_version) {
            if journal.schema_version < 8 {
                journal.infer_legacy_native_phase();
            }
            journal.schema_version = JOURNAL_SCHEMA_VERSION;
        } else if journal.schema_version != JOURNAL_SCHEMA_VERSION {
            return Err(JournalError::UnsupportedSchema(journal.schema_version));
        }
        journal.validate_native_phase()?;
        Ok(journal)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, JournalError> {
        self.validate_native_phase()?;
        serde_json::to_vec_pretty(self).map_err(|_| JournalError::Malformed)
    }

    fn infer_legacy_native_phase(&mut self) {
        self.native_phase = if self.native_binding.is_some() {
            NativeTopologyPhase::Converged
        } else if self.native_intent.is_some() {
            NativeTopologyPhase::Preparing
        } else {
            NativeTopologyPhase::Absent
        };
    }

    fn validate_native_phase(&self) -> Result<(), JournalError> {
        let valid = match self.native_phase {
            NativeTopologyPhase::Absent => {
                self.native_intent.is_none() && self.native_binding.is_none()
            }
            NativeTopologyPhase::Preparing | NativeTopologyPhase::Prepared => {
                self.native_intent.is_some() && self.native_binding.is_none()
            }
            NativeTopologyPhase::Converged => {
                self.native_intent.is_some() && self.native_binding.is_some()
            }
        };
        valid.then_some(()).ok_or(JournalError::Malformed)
    }
}
