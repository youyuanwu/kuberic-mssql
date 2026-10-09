use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::binding::{BoundRoot, process_is_gone, temporary_file_pid};

pub const SEED_DISPATCH_SCHEMA_VERSION: u32 = 1;
pub(super) const SEED_STATE_FILE: &str = "seed-dispatch-v1.json";
pub(super) const SEED_OWNER_FILE: &str = "seed-dispatch-v1.owner";
pub(super) const MAX_SEED_STATE_BYTES: u64 = 65_536;
const MAX_ID_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedDispatchPhase {
    Intent,
    Dispatched,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedDispatchTarget {
    replica_id: String,
    instance_id: String,
    agent_generation: String,
    process_session_id: String,
    native_replica_id: String,
}

impl SeedDispatchTarget {
    pub fn new(
        replica_id: impl Into<String>,
        instance_id: impl Into<String>,
        agent_generation: impl Into<String>,
        process_session_id: impl Into<String>,
        native_replica_id: impl Into<String>,
    ) -> Result<Self, SeedStateError> {
        let value = Self {
            replica_id: replica_id.into(),
            instance_id: instance_id.into(),
            agent_generation: agent_generation.into(),
            process_session_id: process_session_id.into(),
            native_replica_id: native_replica_id.into(),
        };
        validate_id(&value.replica_id)?;
        validate_id(&value.instance_id)?;
        validate_id(&value.agent_generation)?;
        validate_id(&value.process_session_id)?;
        validate_guid(&value.native_replica_id)?;
        Ok(value)
    }

    pub fn replica_id(&self) -> &str {
        &self.replica_id
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn agent_generation(&self) -> &str {
        &self.agent_generation
    }

    pub fn process_session_id(&self) -> &str {
        &self.process_session_id
    }

    pub fn native_replica_id(&self) -> &str {
        &self.native_replica_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedDispatchRecord {
    schema_version: u32,
    revision: u64,
    resource_uid: String,
    configuration_id: String,
    availability_group_name: String,
    availability_group_id: String,
    database_name: String,
    database_guid: String,
    family_guid: String,
    recovery_fork_guid: String,
    source: SeedDispatchTarget,
    targets: [SeedDispatchTarget; 2],
    winning_target_replica_id: String,
    winning_build_id: String,
    sqlserver_start_time: String,
    phase: SeedDispatchPhase,
    group_database_id: Option<String>,
}

impl SeedDispatchRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        resource_uid: impl Into<String>,
        configuration_id: impl Into<String>,
        availability_group_name: impl Into<String>,
        availability_group_id: impl Into<String>,
        database_name: impl Into<String>,
        database_guid: impl Into<String>,
        family_guid: impl Into<String>,
        recovery_fork_guid: impl Into<String>,
        source: SeedDispatchTarget,
        targets: [SeedDispatchTarget; 2],
        winning_target_replica_id: impl Into<String>,
        winning_build_id: impl Into<String>,
        sqlserver_start_time: impl Into<String>,
    ) -> Result<Self, SeedStateError> {
        let record = Self {
            schema_version: SEED_DISPATCH_SCHEMA_VERSION,
            revision: 1,
            resource_uid: resource_uid.into(),
            configuration_id: configuration_id.into(),
            availability_group_name: availability_group_name.into(),
            availability_group_id: availability_group_id.into(),
            database_name: database_name.into(),
            database_guid: database_guid.into(),
            family_guid: family_guid.into(),
            recovery_fork_guid: recovery_fork_guid.into(),
            source,
            targets,
            winning_target_replica_id: winning_target_replica_id.into(),
            winning_build_id: winning_build_id.into(),
            sqlserver_start_time: sqlserver_start_time.into(),
            phase: SeedDispatchPhase::Intent,
            group_database_id: None,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn phase(&self) -> SeedDispatchPhase {
        self.phase
    }

    pub fn winning_build_id(&self) -> &str {
        &self.winning_build_id
    }

    pub fn winning_target_replica_id(&self) -> &str {
        &self.winning_target_replica_id
    }

    pub fn source(&self) -> &SeedDispatchTarget {
        &self.source
    }

    pub fn targets(&self) -> &[SeedDispatchTarget; 2] {
        &self.targets
    }

    pub fn group_database_id(&self) -> Option<&str> {
        self.group_database_id.as_deref()
    }

    pub fn mark_dispatched(&mut self) -> Result<(), SeedStateError> {
        if self.phase != SeedDispatchPhase::Intent || self.group_database_id.is_some() {
            return Err(SeedStateError::InvalidTransition);
        }
        self.phase = SeedDispatchPhase::Dispatched;
        Ok(())
    }

    pub fn associate_group_database(
        &mut self,
        group_database_id: impl Into<String>,
    ) -> Result<(), SeedStateError> {
        if self.phase != SeedDispatchPhase::Dispatched {
            return Err(SeedStateError::InvalidTransition);
        }
        let group_database_id = group_database_id.into();
        validate_guid(&group_database_id)?;
        if self
            .group_database_id
            .as_ref()
            .is_some_and(|existing| existing != &group_database_id)
        {
            return Err(SeedStateError::IdentityMismatch);
        }
        self.group_database_id = Some(group_database_id);
        Ok(())
    }

    fn validate(&self) -> Result<(), SeedStateError> {
        if self.schema_version != SEED_DISPATCH_SCHEMA_VERSION {
            return Err(SeedStateError::UnsupportedSchema(self.schema_version));
        }
        if self.revision == 0
            || (self.phase == SeedDispatchPhase::Intent
                && (self.revision != 1 || self.group_database_id.is_some()))
            || (self.phase == SeedDispatchPhase::Dispatched && self.revision < 2)
            || (self.group_database_id.is_some() && self.revision < 3)
        {
            return Err(SeedStateError::Malformed);
        }
        for value in [
            &self.resource_uid,
            &self.configuration_id,
            &self.availability_group_name,
            &self.database_name,
            &self.winning_target_replica_id,
            &self.winning_build_id,
            &self.sqlserver_start_time,
        ] {
            validate_id(value)?;
        }
        for value in [
            &self.availability_group_id,
            &self.database_guid,
            &self.family_guid,
            &self.recovery_fork_guid,
        ] {
            validate_guid(value)?;
        }
        if let Some(group_database_id) = &self.group_database_id {
            validate_guid(group_database_id)?;
        }
        for member in std::iter::once(&self.source).chain(self.targets.iter()) {
            validate_id(&member.replica_id)?;
            validate_id(&member.instance_id)?;
            validate_id(&member.agent_generation)?;
            validate_id(&member.process_session_id)?;
            validate_guid(&member.native_replica_id)?;
        }
        if self.targets[0].replica_id == self.targets[1].replica_id
            || self
                .targets
                .iter()
                .any(|target| target.replica_id == self.source.replica_id)
            || !self
                .targets
                .iter()
                .any(|target| target.replica_id == self.winning_target_replica_id)
        {
            return Err(SeedStateError::IdentityMismatch);
        }
        Ok(())
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.resource_uid == other.resource_uid
            && self.configuration_id == other.configuration_id
            && self.availability_group_name == other.availability_group_name
            && self.availability_group_id == other.availability_group_id
            && self.database_name == other.database_name
            && self.database_guid == other.database_guid
            && self.family_guid == other.family_guid
            && self.recovery_fork_guid == other.recovery_fork_guid
            && self.source == other.source
            && self.targets == other.targets
            && self.winning_target_replica_id == other.winning_target_replica_id
            && self.winning_build_id == other.winning_build_id
            && self.sqlserver_start_time == other.sqlserver_start_time
    }

    pub fn same_dispatch_context(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.resource_uid == other.resource_uid
            && self.configuration_id == other.configuration_id
            && self.availability_group_name == other.availability_group_name
            && self.availability_group_id == other.availability_group_id
            && self.database_name == other.database_name
            && self.database_guid == other.database_guid
            && self.family_guid == other.family_guid
            && self.recovery_fork_guid == other.recovery_fork_guid
            && self.source == other.source
            && self.targets == other.targets
            && self.sqlserver_start_time == other.sqlserver_start_time
    }

    fn validate_initial(&self) -> Result<(), SeedStateError> {
        self.validate()?;
        if self.revision != 1
            || self.phase != SeedDispatchPhase::Intent
            || self.group_database_id.is_some()
        {
            return Err(SeedStateError::InvalidTransition);
        }
        Ok(())
    }

    fn validate_transition(&self, next: &Self) -> Result<(), SeedStateError> {
        if !self.same_identity(next) || self.revision.checked_add(1) != Some(next.revision) {
            return Err(SeedStateError::RevisionMismatch);
        }
        match (
            self.phase,
            self.group_database_id.as_deref(),
            next.phase,
            next.group_database_id.as_deref(),
        ) {
            (SeedDispatchPhase::Intent, None, SeedDispatchPhase::Dispatched, None)
            | (SeedDispatchPhase::Dispatched, None, SeedDispatchPhase::Dispatched, Some(_)) => {
                Ok(())
            }
            (
                SeedDispatchPhase::Dispatched,
                Some(current),
                SeedDispatchPhase::Dispatched,
                Some(next),
            ) if current == next => Ok(()),
            _ => Err(SeedStateError::InvalidTransition),
        }
    }
}

pub struct SeedDispatchStore {
    root: BoundRoot,
    _owner: File,
    path: PathBuf,
    transaction: Mutex<()>,
}

impl SeedDispatchStore {
    pub fn open(root: &Path) -> Result<Self, SeedStateError> {
        let root = BoundRoot::open(root).map_err(map_root_error)?;
        root.verify_path().map_err(SeedStateError::from)?;
        let owner = root
            .openat(
                SEED_OWNER_FILE,
                libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
            .map_err(|_| SeedStateError::Io)?;
        verify_private_file(&owner, 0)?;
        if unsafe { libc::flock(owner.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(SeedStateError::AlreadyOwned);
        }
        let path = root.path().join(SEED_STATE_FILE);
        let store = Self {
            root,
            _owner: owner,
            path,
            transaction: Mutex::new(()),
        };
        if let Some(record) = store.load()? {
            record.validate()?;
        }
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<Option<SeedDispatchRecord>, SeedStateError> {
        let _transaction = self
            .transaction
            .lock()
            .map_err(|_| SeedStateError::UnsafeState)?;
        self.load_unlocked()
    }

    fn load_unlocked(&self) -> Result<Option<SeedDispatchRecord>, SeedStateError> {
        let file = match self.root.openat(
            SEED_STATE_FILE,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(SeedStateError::Io),
        };
        verify_private_file(&file, MAX_SEED_STATE_BYTES)?;
        let mut bytes = Vec::new();
        file.take(MAX_SEED_STATE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SeedStateError::Io)?;
        if bytes.len() as u64 > MAX_SEED_STATE_BYTES {
            return Err(SeedStateError::UnsafeState);
        }
        let record: SeedDispatchRecord =
            serde_json::from_slice(&bytes).map_err(|_| SeedStateError::Malformed)?;
        record.validate()?;
        Ok(Some(record))
    }

    pub fn create(&self, record: &SeedDispatchRecord) -> Result<(), SeedStateError> {
        let _transaction = self
            .transaction
            .lock()
            .map_err(|_| SeedStateError::UnsafeState)?;
        record.validate_initial()?;
        if self.load_unlocked()?.is_some() {
            return Err(SeedStateError::AlreadyExists);
        }
        self.write_unlocked(record)
    }

    pub fn replace(
        &self,
        expected_revision: u64,
        mut record: SeedDispatchRecord,
    ) -> Result<SeedDispatchRecord, SeedStateError> {
        let _transaction = self
            .transaction
            .lock()
            .map_err(|_| SeedStateError::UnsafeState)?;
        let current = self.load_unlocked()?.ok_or(SeedStateError::Missing)?;
        if current.revision != expected_revision || record.revision != expected_revision {
            return Err(SeedStateError::RevisionMismatch);
        }
        record.revision = expected_revision
            .checked_add(1)
            .ok_or(SeedStateError::RevisionMismatch)?;
        current.validate_transition(&record)?;
        record.validate()?;
        self.write_unlocked(&record)?;
        Ok(record)
    }

    pub fn remove(&self, expected_revision: u64) -> Result<(), SeedStateError> {
        let _transaction = self
            .transaction
            .lock()
            .map_err(|_| SeedStateError::UnsafeState)?;
        let current = self.load_unlocked()?.ok_or(SeedStateError::Missing)?;
        if current.revision != expected_revision {
            return Err(SeedStateError::RevisionMismatch);
        }
        if current.phase != SeedDispatchPhase::Dispatched || current.group_database_id.is_none() {
            return Err(SeedStateError::InvalidTransition);
        }
        self.root
            .unlinkat(SEED_STATE_FILE)
            .map_err(SeedStateError::from)?;
        self.root.file().sync_all().map_err(|_| SeedStateError::Io)
    }

    pub fn remove_intent(&self, expected_revision: u64) -> Result<(), SeedStateError> {
        let _transaction = self
            .transaction
            .lock()
            .map_err(|_| SeedStateError::UnsafeState)?;
        let current = self.load_unlocked()?.ok_or(SeedStateError::Missing)?;
        if current.revision != expected_revision {
            return Err(SeedStateError::RevisionMismatch);
        }
        if current.phase != SeedDispatchPhase::Intent || current.group_database_id.is_some() {
            return Err(SeedStateError::InvalidTransition);
        }
        self.root
            .unlinkat(SEED_STATE_FILE)
            .map_err(SeedStateError::from)?;
        self.root.file().sync_all().map_err(|_| SeedStateError::Io)
    }

    fn write_unlocked(&self, record: &SeedDispatchRecord) -> Result<(), SeedStateError> {
        record.validate()?;
        let bytes = serde_json::to_vec_pretty(record).map_err(|_| SeedStateError::Malformed)?;
        if bytes.len() as u64 > MAX_SEED_STATE_BYTES {
            return Err(SeedStateError::UnsafeState);
        }
        let temporary = format!(".{SEED_STATE_FILE}.{}.new", std::process::id());
        let mut file = self
            .root
            .openat(
                &temporary,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
            .map_err(|_| SeedStateError::Io)?;
        let result = (|| {
            file.write_all(&bytes).map_err(|_| SeedStateError::Io)?;
            file.sync_all().map_err(|_| SeedStateError::Io)?;
            self.root.verify_path().map_err(SeedStateError::from)?;
            self.root
                .renameat(&temporary, SEED_STATE_FILE)
                .map_err(SeedStateError::from)?;
            self.root.file().sync_all().map_err(|_| SeedStateError::Io)
        })();
        if result.is_err() {
            let _ = self.root.unlinkat(&temporary);
        }
        result
    }
}

pub(super) fn validate_seed_entry(root: &BoundRoot, name: &str) -> Result<(), SeedStateError> {
    let max = match name {
        SEED_STATE_FILE => MAX_SEED_STATE_BYTES,
        SEED_OWNER_FILE => 0,
        _ => return Err(SeedStateError::UnsafeState),
    };
    let file = root
        .openat(name, libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW, 0)
        .map_err(|_| SeedStateError::UnsafeState)?;
    verify_private_file(&file, max)?;
    if name == SEED_STATE_FILE {
        let mut bytes = Vec::new();
        file.take(MAX_SEED_STATE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SeedStateError::Io)?;
        if bytes.len() as u64 > MAX_SEED_STATE_BYTES {
            return Err(SeedStateError::UnsafeState);
        }
        let record: SeedDispatchRecord =
            serde_json::from_slice(&bytes).map_err(|_| SeedStateError::Malformed)?;
        record.validate()?;
    }
    Ok(())
}

pub(super) fn recover_seed_temporary(root: &BoundRoot, name: &str) -> Result<bool, SeedStateError> {
    let Some(pid) = temporary_file_pid(name, SEED_STATE_FILE) else {
        return Ok(false);
    };
    if !process_is_gone(pid).map_err(SeedStateError::from)? {
        return Err(SeedStateError::UnsafeState);
    }
    let temporary = root
        .openat(name, libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW, 0)
        .map_err(|_| SeedStateError::UnsafeState)?;
    verify_private_file(&temporary, MAX_SEED_STATE_BYTES)?;
    drop(temporary);
    root.unlinkat(name).map_err(SeedStateError::from)?;
    Ok(true)
}

fn verify_private_file(file: &File, max_bytes: u64) -> Result<(), SeedStateError> {
    let metadata = file.metadata().map_err(|_| SeedStateError::Io)?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > max_bytes
    {
        return Err(SeedStateError::UnsafeState);
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<(), SeedStateError> {
    if value.is_empty() || value.len() > MAX_ID_BYTES || value.chars().any(char::is_control) {
        Err(SeedStateError::Malformed)
    } else {
        Ok(())
    }
}

fn validate_guid(value: &str) -> Result<(), SeedStateError> {
    let valid = value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
        && !value.bytes().all(|byte| byte == b'0' || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(SeedStateError::Malformed)
    }
}

fn map_root_error(error: std::io::Error) -> SeedStateError {
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        SeedStateError::UnsafeState
    } else {
        SeedStateError::Io
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedStateError {
    UnsafeState,
    IdentityMismatch,
    AlreadyOwned,
    AlreadyExists,
    Missing,
    RevisionMismatch,
    InvalidTransition,
    Malformed,
    UnsupportedSchema(u32),
    Io,
}

impl From<super::binding::RuntimeBindingError> for SeedStateError {
    fn from(error: super::binding::RuntimeBindingError) -> Self {
        match error {
            super::binding::RuntimeBindingError::UnsafeState => Self::UnsafeState,
            _ => Self::Io,
        }
    }
}

impl std::fmt::Display for SeedStateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsafeState => formatter.write_str("seed dispatch state is unsafe"),
            Self::IdentityMismatch => formatter.write_str("seed dispatch identity differs"),
            Self::AlreadyOwned => formatter.write_str("seed dispatch state is already owned"),
            Self::AlreadyExists => formatter.write_str("seed dispatch state already exists"),
            Self::Missing => formatter.write_str("seed dispatch state is missing"),
            Self::RevisionMismatch => formatter.write_str("seed dispatch revision differs"),
            Self::InvalidTransition => formatter.write_str("seed dispatch transition is invalid"),
            Self::Malformed => formatter.write_str("seed dispatch state is malformed"),
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "unsupported seed dispatch schema version {version}"
                )
            }
            Self::Io => formatter.write_str("seed dispatch state I/O failed"),
        }
    }
}

impl std::error::Error for SeedStateError {}

use std::os::fd::AsRawFd;
