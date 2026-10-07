use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::fixture::cleanup::{CleanupJournal, CleanupJournalStore};
use crate::fixture::model::{
    JournalError, ProcessIncarnation, ResourceBinding, ResourceRecord, ResourceState, RunState,
};
use crate::fixture::ownership::{JournalStore, ReconcileError, current_process_incarnation};

pub const ONE_REPLICA_JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OneReplicaMember {
    pub ordinal: u8,
    pub server_name: String,
    pub container_name: String,
    pub network_name: String,
    pub data_directory: PathBuf,
    pub environment_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OneReplicaRun {
    pub run_id: String,
    pub resource_uid: String,
    pub image_id: String,
    pub member: OneReplicaMember,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OneReplicaJournal {
    pub schema_version: u32,
    pub run: OneReplicaRun,
    pub state: RunState,
    #[serde(default)]
    pub blocked_owner: Option<ProcessIncarnation>,
    #[serde(default)]
    pub blocked_owner_unknown: bool,
    pub resources: Vec<ResourceRecord>,
}

impl OneReplicaJournal {
    pub fn new(run: OneReplicaRun) -> Self {
        Self {
            schema_version: ONE_REPLICA_JOURNAL_SCHEMA_VERSION,
            run,
            state: RunState::Preparing,
            blocked_owner: None,
            blocked_owner_unknown: false,
            resources: Vec::new(),
        }
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, JournalError> {
        let journal: Self = serde_json::from_slice(bytes).map_err(|_| JournalError::Malformed)?;
        if journal.schema_version != ONE_REPLICA_JOURNAL_SCHEMA_VERSION {
            return Err(JournalError::UnsupportedSchema(journal.schema_version));
        }
        Ok(journal)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, JournalError> {
        serde_json::to_vec_pretty(self).map_err(|_| JournalError::Malformed)
    }
}

impl CleanupJournal for OneReplicaJournal {
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

#[derive(Debug, Clone)]
pub struct OneReplicaJournalStore {
    inner: JournalStore,
}

impl OneReplicaJournalStore {
    pub fn initialize(root: &std::path::Path) -> Result<Self, ReconcileError> {
        JournalStore::initialize(root).map(|inner| Self { inner })
    }

    pub fn root(&self) -> &std::path::Path {
        self.inner.root()
    }

    pub fn path(&self) -> &std::path::Path {
        self.inner.path()
    }

    pub fn load(&self) -> Result<Option<OneReplicaJournal>, ReconcileError> {
        self.inner
            .load_bytes()?
            .map(|bytes| OneReplicaJournal::from_json(&bytes).map_err(ReconcileError::Journal))
            .transpose()
    }

    pub fn create(&self, run: OneReplicaRun) -> Result<OneReplicaJournal, ReconcileError> {
        let journal = OneReplicaJournal::new(run);
        self.save(&journal)?;
        Ok(journal)
    }

    pub fn save(&self, journal: &OneReplicaJournal) -> Result<(), ReconcileError> {
        self.inner
            .save_bytes(&journal.to_json().map_err(ReconcileError::Journal)?)
    }

    pub fn record_intent(
        &self,
        journal: &mut OneReplicaJournal,
        record: ResourceRecord,
    ) -> Result<usize, ReconcileError> {
        if record.state != ResourceState::Intended || record.binding.is_some() {
            return Err(ReconcileError::InvalidTransition);
        }
        journal.resources.push(record);
        self.save(journal)?;
        Ok(journal.resources.len() - 1)
    }

    pub fn mark_dispatched(
        &self,
        journal: &mut OneReplicaJournal,
        index: usize,
    ) -> Result<(), ReconcileError> {
        let record = journal
            .resources
            .get_mut(index)
            .ok_or(ReconcileError::InvalidTransition)?;
        if record.state != ResourceState::Intended {
            return Err(ReconcileError::InvalidTransition);
        }
        record.state = ResourceState::Dispatched;
        self.save(journal)
    }

    pub fn bind(
        &self,
        journal: &mut OneReplicaJournal,
        index: usize,
        binding: ResourceBinding,
    ) -> Result<(), ReconcileError> {
        let record = journal
            .resources
            .get_mut(index)
            .ok_or(ReconcileError::InvalidTransition)?;
        if !matches!(
            record.state,
            ResourceState::Dispatched | ResourceState::Blocked
        ) || record.binding.is_some()
        {
            return Err(ReconcileError::InvalidTransition);
        }
        record.binding = Some(binding);
        record.state = ResourceState::Bound;
        self.save(journal)
    }

    pub fn block_for_current_process(
        &self,
        journal: &mut OneReplicaJournal,
    ) -> Result<(), ReconcileError> {
        journal.blocked_owner =
            Some(current_process_incarnation().map_err(|_| ReconcileError::Io)?);
        journal.blocked_owner_unknown = false;
        journal.state = RunState::Blocked;
        self.save(journal)
    }
}

impl CleanupJournalStore<OneReplicaJournal> for OneReplicaJournalStore {
    fn save_cleanup_journal(&self, journal: &OneReplicaJournal) -> Result<(), ReconcileError> {
        self.save(journal)
    }
}
