use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use kuberic_runtime::host::ApplicationStorageState;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::config::RuntimeHostConfig;

pub const RUNTIME_BINDING_SCHEMA_VERSION: u32 = 1;
const BINDING_FILE: &str = "runtime-binding.json";
const MAX_BINDING_BYTES: u64 = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBindingIdentity {
    schema_version: u32,
    resource_uid: String,
    replica_id: i64,
    observer: ObserverBinding,
    topology_sha256: String,
    application_root: String,
}

impl RuntimeBindingIdentity {
    pub fn from_config(config: &RuntimeHostConfig) -> Result<Self, RuntimeBindingError> {
        let application_root = canonical_root(config.application_root())?;
        let mut digest = Sha256::new();
        digest.update(config.topology().canonical_json());
        let target = config.observer().target();
        Ok(Self {
            schema_version: RUNTIME_BINDING_SCHEMA_VERSION,
            resource_uid: config.resource_uid().as_str().to_owned(),
            replica_id: config.replica_id().value(),
            observer: ObserverBinding {
                availability_group: target.availability_group.as_str().to_owned(),
                expected_server_name: target.expected_server_name.as_str().to_owned(),
                logical_replica_id: target.replica.logical_id().to_owned(),
                incarnation: target.replica.incarnation().to_owned(),
            },
            topology_sha256: hex(&digest.finalize()),
            application_root: application_root
                .to_str()
                .ok_or(RuntimeBindingError::InvalidPath)?
                .to_owned(),
        })
    }

    pub fn resource_uid(&self) -> &str {
        &self.resource_uid
    }

    pub fn replica_id(&self) -> i64 {
        self.replica_id
    }

    pub fn topology_sha256(&self) -> &str {
        &self.topology_sha256
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserverBinding {
    availability_group: String,
    expected_server_name: String,
    logical_replica_id: String,
    incarnation: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeBindingStore {
    root: PathBuf,
    path: PathBuf,
    expected: RuntimeBindingIdentity,
}

impl RuntimeBindingStore {
    pub fn new(config: &RuntimeHostConfig) -> Result<Self, RuntimeBindingError> {
        let root = canonical_root(config.application_root())?;
        let expected = RuntimeBindingIdentity::from_config(config)?;
        Ok(Self {
            path: root.join(BINDING_FILE),
            root,
            expected,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn expected(&self) -> &RuntimeBindingIdentity {
        &self.expected
    }

    pub fn storage_state(&self) -> Result<ApplicationStorageState, RuntimeBindingError> {
        let metadata = match fs::symlink_metadata(&self.root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ApplicationStorageState::FreshEmpty);
            }
            Err(_) => return Err(RuntimeBindingError::Io),
        };
        validate_private_directory(&metadata)?;
        let entries = fs::read_dir(&self.root)
            .map_err(|_| RuntimeBindingError::Io)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RuntimeBindingError::Io)?;
        if entries.is_empty() {
            return Ok(ApplicationStorageState::FreshEmpty);
        }
        if entries.len() != 1 || entries[0] != BINDING_FILE {
            return Err(RuntimeBindingError::UnsafeState);
        }
        let actual = self.load()?;
        if actual != self.expected {
            return Err(RuntimeBindingError::IdentityMismatch);
        }
        Ok(ApplicationStorageState::Established)
    }

    pub fn initialize(&self) -> Result<(), RuntimeBindingError> {
        if self.storage_state()? != ApplicationStorageState::FreshEmpty {
            return Err(RuntimeBindingError::AlreadyInitialized);
        }
        if !self.root.exists() {
            DirBuilder::new()
                .recursive(false)
                .mode(0o700)
                .create(&self.root)
                .map_err(|_| RuntimeBindingError::Io)?;
        }
        let metadata = fs::symlink_metadata(&self.root).map_err(|_| RuntimeBindingError::Io)?;
        validate_private_directory(&metadata)?;
        if self
            .root
            .canonicalize()
            .map_err(|_| RuntimeBindingError::Io)?
            != self.root
        {
            return Err(RuntimeBindingError::InvalidPath);
        }
        self.save()
    }

    pub fn validate(&self) -> Result<(), RuntimeBindingError> {
        if self.storage_state()? == ApplicationStorageState::Established {
            Ok(())
        } else {
            Err(RuntimeBindingError::NotInitialized)
        }
    }

    fn load(&self) -> Result<RuntimeBindingIdentity, RuntimeBindingError> {
        let metadata = fs::symlink_metadata(&self.path).map_err(|_| RuntimeBindingError::Io)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_BINDING_BYTES
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(RuntimeBindingError::UnsafeState);
        }
        let bytes = fs::read(&self.path).map_err(|_| RuntimeBindingError::Io)?;
        let identity: RuntimeBindingIdentity =
            serde_json::from_slice(&bytes).map_err(|_| RuntimeBindingError::Malformed)?;
        if identity.schema_version != RUNTIME_BINDING_SCHEMA_VERSION {
            return Err(RuntimeBindingError::UnsupportedSchema(
                identity.schema_version,
            ));
        }
        Ok(identity)
    }

    fn save(&self) -> Result<(), RuntimeBindingError> {
        let bytes = serde_json::to_vec_pretty(&self.expected)
            .map_err(|_| RuntimeBindingError::Malformed)?;
        let temporary = self
            .root
            .join(format!(".{BINDING_FILE}.{}.new", std::process::id()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|_| RuntimeBindingError::Io)?;
        let result = (|| {
            file.write_all(&bytes)
                .map_err(|_| RuntimeBindingError::Io)?;
            file.sync_all().map_err(|_| RuntimeBindingError::Io)?;
            fs::rename(&temporary, &self.path).map_err(|_| RuntimeBindingError::Io)?;
            File::open(&self.root)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| RuntimeBindingError::Io)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeBindingError {
    InvalidPath,
    UnsafeState,
    IdentityMismatch,
    AlreadyInitialized,
    NotInitialized,
    Malformed,
    UnsupportedSchema(u32),
    Io,
}

impl std::fmt::Display for RuntimeBindingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPath => formatter.write_str("runtime application root is invalid"),
            Self::UnsafeState => formatter.write_str("runtime application state is unsafe"),
            Self::IdentityMismatch => {
                formatter.write_str("runtime application binding identity differs")
            }
            Self::AlreadyInitialized => {
                formatter.write_str("runtime application binding is already initialized")
            }
            Self::NotInitialized => {
                formatter.write_str("runtime application binding is not initialized")
            }
            Self::Malformed => formatter.write_str("runtime application binding is malformed"),
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "unsupported runtime application binding schema version {version}"
                )
            }
            Self::Io => formatter.write_str("runtime application binding I/O failed"),
        }
    }
}

impl std::error::Error for RuntimeBindingError {}

fn canonical_root(path: &Path) -> Result<PathBuf, RuntimeBindingError> {
    if !path.is_absolute() {
        return Err(RuntimeBindingError::InvalidPath);
    }
    if path.exists() {
        if fs::symlink_metadata(path)
            .map_err(|_| RuntimeBindingError::InvalidPath)?
            .file_type()
            .is_symlink()
        {
            return Err(RuntimeBindingError::InvalidPath);
        }
        return path
            .canonicalize()
            .map_err(|_| RuntimeBindingError::InvalidPath);
    }
    let parent = path.parent().ok_or(RuntimeBindingError::InvalidPath)?;
    let parent = parent
        .canonicalize()
        .map_err(|_| RuntimeBindingError::InvalidPath)?;
    let name = path.file_name().ok_or(RuntimeBindingError::InvalidPath)?;
    Ok(parent.join(name))
}

fn validate_private_directory(metadata: &fs::Metadata) -> Result<(), RuntimeBindingError> {
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        Err(RuntimeBindingError::UnsafeState)
    } else {
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[usize::from(byte >> 4)] as char);
        value.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    value
}
