use std::ffi::CString;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
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
        let root = match BoundRoot::open(&self.root) {
            Ok(root) => root,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ApplicationStorageState::FreshEmpty);
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(RuntimeBindingError::UnsafeState);
            }
            Err(_) => return Err(RuntimeBindingError::Io),
        };
        root.verify_path()?;
        let mut entries = root.entries()?;
        let mut recovered = false;
        for entry in &entries {
            if entry == BINDING_FILE {
                continue;
            }
            let Some(name) = entry.to_str() else {
                return Err(RuntimeBindingError::UnsafeState);
            };
            let Some(pid) = temporary_binding_pid(name) else {
                return Err(RuntimeBindingError::UnsafeState);
            };
            if !process_is_gone(pid)? {
                return Err(RuntimeBindingError::UnsafeState);
            }
            let temporary = root
                .openat(name, libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW, 0)
                .map_err(|_| RuntimeBindingError::UnsafeState)?;
            let metadata = temporary
                .metadata()
                .map_err(|_| RuntimeBindingError::UnsafeState)?;
            if !metadata.is_file()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(RuntimeBindingError::UnsafeState);
            }
            drop(temporary);
            root.unlinkat(name)?;
            recovered = true;
        }
        if recovered {
            root.file.sync_all().map_err(|_| RuntimeBindingError::Io)?;
            root.verify_path()?;
            entries = root.entries()?;
        }
        if entries.is_empty() {
            return Ok(ApplicationStorageState::FreshEmpty);
        }
        if entries.len() != 1 || entries[0] != BINDING_FILE {
            return Err(RuntimeBindingError::UnsafeState);
        }
        let actual = self.load(&root)?;
        root.verify_path()?;
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
        let root = BoundRoot::open(&self.root).map_err(|error| {
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                RuntimeBindingError::UnsafeState
            } else {
                RuntimeBindingError::Io
            }
        })?;
        root.verify_path()?;
        self.save(&root)
    }

    pub fn validate(&self) -> Result<(), RuntimeBindingError> {
        if self.storage_state()? == ApplicationStorageState::Established {
            Ok(())
        } else {
            Err(RuntimeBindingError::NotInitialized)
        }
    }

    fn load(&self, root: &BoundRoot) -> Result<RuntimeBindingIdentity, RuntimeBindingError> {
        let file = root
            .openat(
                BINDING_FILE,
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0,
            )
            .map_err(|_| RuntimeBindingError::Io)?;
        let metadata = file.metadata().map_err(|_| RuntimeBindingError::Io)?;
        if !metadata.is_file()
            || metadata.len() > MAX_BINDING_BYTES
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(RuntimeBindingError::UnsafeState);
        }
        let mut bytes = Vec::new();
        file.take(MAX_BINDING_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| RuntimeBindingError::Io)?;
        if bytes.len() as u64 > MAX_BINDING_BYTES {
            return Err(RuntimeBindingError::UnsafeState);
        }
        let identity: RuntimeBindingIdentity =
            serde_json::from_slice(&bytes).map_err(|_| RuntimeBindingError::Malformed)?;
        if identity.schema_version != RUNTIME_BINDING_SCHEMA_VERSION {
            return Err(RuntimeBindingError::UnsupportedSchema(
                identity.schema_version,
            ));
        }
        Ok(identity)
    }

    fn save(&self, root: &BoundRoot) -> Result<(), RuntimeBindingError> {
        let bytes = serde_json::to_vec_pretty(&self.expected)
            .map_err(|_| RuntimeBindingError::Malformed)?;
        let temporary = format!(".{BINDING_FILE}.{}.new", std::process::id());
        let mut file = root
            .openat(
                &temporary,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
            .map_err(|_| RuntimeBindingError::Io)?;
        let result = (|| {
            file.write_all(&bytes)
                .map_err(|_| RuntimeBindingError::Io)?;
            file.sync_all().map_err(|_| RuntimeBindingError::Io)?;
            root.verify_path()?;
            root.renameat(&temporary, BINDING_FILE)?;
            root.file.sync_all().map_err(|_| RuntimeBindingError::Io)
        })();
        if result.is_err() {
            let _ = root.unlinkat(&temporary);
        }
        result
    }
}

struct BoundRoot {
    path: PathBuf,
    file: File,
    device: u64,
    inode: u64,
}

impl BoundRoot {
    fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        }
        Ok(Self {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
            file,
        })
    }

    fn verify_path(&self) -> Result<(), RuntimeBindingError> {
        let metadata = fs::symlink_metadata(&self.path).map_err(|_| RuntimeBindingError::Io)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
            || self
                .path
                .canonicalize()
                .map_err(|_| RuntimeBindingError::Io)?
                != self.path
        {
            return Err(RuntimeBindingError::UnsafeState);
        }
        let bound = self.file.metadata().map_err(|_| RuntimeBindingError::Io)?;
        if bound.dev() != self.device || bound.ino() != self.inode {
            return Err(RuntimeBindingError::UnsafeState);
        }
        Ok(())
    }

    fn openat(&self, name: &str, flags: i32, mode: libc::mode_t) -> std::io::Result<File> {
        let name = CString::new(name)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let descriptor = unsafe { libc::openat(self.file.as_raw_fd(), name.as_ptr(), flags, mode) };
        if descriptor < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(unsafe { File::from_raw_fd(descriptor) })
        }
    }

    fn entries(&self) -> Result<Vec<std::ffi::OsString>, RuntimeBindingError> {
        fs::read_dir(format!("/proc/self/fd/{}", self.file.as_raw_fd()))
            .map_err(|_| RuntimeBindingError::Io)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| RuntimeBindingError::Io)
    }

    fn renameat(&self, from: &str, to: &str) -> Result<(), RuntimeBindingError> {
        let from = CString::new(from).map_err(|_| RuntimeBindingError::Io)?;
        let to = CString::new(to).map_err(|_| RuntimeBindingError::Io)?;
        if unsafe {
            libc::renameat(
                self.file.as_raw_fd(),
                from.as_ptr(),
                self.file.as_raw_fd(),
                to.as_ptr(),
            )
        } == 0
        {
            Ok(())
        } else {
            Err(RuntimeBindingError::Io)
        }
    }

    fn unlinkat(&self, name: &str) -> Result<(), RuntimeBindingError> {
        let name = CString::new(name).map_err(|_| RuntimeBindingError::Io)?;
        if unsafe { libc::unlinkat(self.file.as_raw_fd(), name.as_ptr(), 0) } == 0 {
            Ok(())
        } else {
            Err(RuntimeBindingError::Io)
        }
    }
}

fn temporary_binding_pid(name: &str) -> Option<libc::pid_t> {
    let prefix = format!(".{BINDING_FILE}.");
    name.strip_prefix(&prefix)?
        .strip_suffix(".new")?
        .parse::<libc::pid_t>()
        .ok()
        .filter(|pid| *pid > 0)
}

fn process_is_gone(pid: libc::pid_t) -> Result<bool, RuntimeBindingError> {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(false);
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => Ok(true),
        Some(libc::EPERM) => Ok(false),
        _ => Err(RuntimeBindingError::Io),
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

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[usize::from(byte >> 4)] as char);
        value.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    value
}
