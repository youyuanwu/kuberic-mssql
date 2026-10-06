use std::error::Error;
use std::ffi::CString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::SystemTime;

use rustix::fs::{FlockOperation, flock};
use sha2::{Digest, Sha256};

use super::model::{
    JournalError, OwnershipJournal, ResourceBinding, ResourceRecord, ResourceState, RunState,
    TopologyRun,
};
use super::process::{CommandSpec, ProcessRunner};

const JOURNAL_FILE: &str = "ownership.json";
const DIRECTORY_MARKER: &str = ".kuberic-mssql-owner";
static DIRECTORY_MARKER_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static JOURNAL_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct RootLock {
    file: File,
    canonical_root: PathBuf,
    lock_path: PathBuf,
}

impl RootLock {
    pub fn canonical_root(&self) -> &Path {
        &self.canonical_root
    }

    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for RootLock {
    fn drop(&mut self) {
        let _ = flock(&self.file, FlockOperation::Unlock);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockError {
    InvalidRoot,
    Contended,
    Unavailable,
}

impl fmt::Display for LockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRoot => "fixture root is invalid",
            Self::Contended => "another three-replica fixture owns this canonical root",
            Self::Unavailable => "fixture root lock is unavailable",
        })
    }
}

impl Error for LockError {}

pub fn acquire_root_lock(root: &Path) -> Result<RootLock, LockError> {
    if !root.is_absolute() {
        return Err(LockError::InvalidRoot);
    }
    let canonical_root = canonical_missing_path(root)?;
    let parent = canonical_root.parent().ok_or(LockError::InvalidRoot)?;
    let mut digest = Sha256::new();
    digest.update(canonical_root.as_os_str().as_bytes());
    let digest = hex(&digest.finalize());
    let lock_path = parent.join(format!(
        ".kuberic-mssql-three-replica-{}.lock",
        &digest[..24]
    ));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&lock_path)
        .map_err(|_| LockError::Unavailable)?;
    match flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(RootLock {
            file,
            canonical_root,
            lock_path,
        }),
        Err(error) if error == rustix::io::Errno::WOULDBLOCK => Err(LockError::Contended),
        Err(_) => Err(LockError::Unavailable),
    }
}

#[derive(Debug, Clone)]
pub struct JournalStore {
    root: PathBuf,
    path: PathBuf,
    root_directory: Arc<File>,
    root_device: u64,
    root_inode: u64,
}

impl JournalStore {
    pub fn initialize(root: &Path) -> Result<Self, ReconcileError> {
        initialize_private_root(root)?;
        let root = root.canonicalize().map_err(|_| ReconcileError::Io)?;
        let root_directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&root)
            .map_err(|_| ReconcileError::Io)?;
        let metadata = root_directory.metadata().map_err(|_| ReconcileError::Io)?;
        Ok(Self {
            path: root.join(JOURNAL_FILE),
            root,
            root_device: metadata.dev(),
            root_inode: metadata.ino(),
            root_directory: Arc::new(root_directory),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<Option<OwnershipJournal>, ReconcileError> {
        self.verify_root_identity()?;
        match self.openat(
            JOURNAL_FILE,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)
                    .map_err(|_| ReconcileError::Io)?;
                OwnershipJournal::from_json(&bytes)
                    .map(Some)
                    .map_err(ReconcileError::Journal)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(ReconcileError::Io),
        }
    }

    pub fn create(&self, run: TopologyRun) -> Result<OwnershipJournal, ReconcileError> {
        let journal = OwnershipJournal::new(run);
        self.save(&journal)?;
        Ok(journal)
    }

    pub fn save(&self, journal: &OwnershipJournal) -> Result<(), ReconcileError> {
        self.verify_root_identity()?;
        let bytes = journal.to_json().map_err(ReconcileError::Journal)?;
        let temporary = format!(
            ".{JOURNAL_FILE}.{}.{}.new",
            std::process::id(),
            JOURNAL_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let mut file = self
            .openat(
                &temporary,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
            .map_err(|_| ReconcileError::Io)?;
        let result = (|| {
            file.write_all(&bytes).map_err(|_| ReconcileError::Io)?;
            file.sync_all().map_err(|_| ReconcileError::Io)?;
            self.verify_root_identity()?;
            self.renameat(&temporary, JOURNAL_FILE)?;
            self.root_directory
                .sync_all()
                .map_err(|_| ReconcileError::Io)
        })();
        if result.is_err() {
            self.unlinkat(&temporary);
        }
        result
    }

    fn verify_root_identity(&self) -> Result<(), ReconcileError> {
        let metadata = fs::symlink_metadata(&self.root).map_err(|_| ReconcileError::Io)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.dev() != self.root_device
            || metadata.ino() != self.root_inode
        {
            return Err(ReconcileError::OwnershipMismatch);
        }
        let canonical = self.root.canonicalize().map_err(|_| ReconcileError::Io)?;
        if canonical != self.root {
            return Err(ReconcileError::OwnershipMismatch);
        }
        let bound = self
            .root_directory
            .metadata()
            .map_err(|_| ReconcileError::Io)?;
        if bound.dev() != self.root_device || bound.ino() != self.root_inode {
            return Err(ReconcileError::OwnershipMismatch);
        }
        Ok(())
    }

    fn openat(&self, name: &str, flags: i32, mode: libc::mode_t) -> io::Result<File> {
        let name = CString::new(name).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let descriptor =
            unsafe { libc::openat(self.root_directory.as_raw_fd(), name.as_ptr(), flags, mode) };
        if descriptor < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(unsafe { File::from_raw_fd(descriptor) })
        }
    }

    fn renameat(&self, from: &str, to: &str) -> Result<(), ReconcileError> {
        let from = CString::new(from).map_err(|_| ReconcileError::Io)?;
        let to = CString::new(to).map_err(|_| ReconcileError::Io)?;
        if unsafe {
            libc::renameat(
                self.root_directory.as_raw_fd(),
                from.as_ptr(),
                self.root_directory.as_raw_fd(),
                to.as_ptr(),
            )
        } == 0
        {
            Ok(())
        } else {
            Err(ReconcileError::Io)
        }
    }

    fn unlinkat(&self, name: &str) {
        let Ok(name) = CString::new(name) else {
            return;
        };
        unsafe {
            libc::unlinkat(self.root_directory.as_raw_fd(), name.as_ptr(), 0);
        }
    }

    pub fn record_intent(
        &self,
        journal: &mut OwnershipJournal,
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
        journal: &mut OwnershipJournal,
        index: usize,
    ) -> Result<(), ReconcileError> {
        transition(
            journal,
            index,
            ResourceState::Intended,
            ResourceState::Dispatched,
        )?;
        self.save(journal)
    }

    pub fn bind(
        &self,
        journal: &mut OwnershipJournal,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceObservation {
    Absent,
    Owned {
        binding: ResourceBinding,
        foreign_attachments: Vec<String>,
    },
    Foreign,
}

pub trait OwnershipInspector {
    fn inspect(&self, resource: &ResourceRecord) -> Result<ResourceObservation, ReconcileError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub recovered: Vec<String>,
    pub removed: Vec<String>,
    pub blocked: Vec<String>,
}

impl ReconcileReport {
    pub fn is_blocked(&self) -> bool {
        !self.blocked.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileError {
    Journal(JournalError),
    Io,
    InvalidTransition,
    OwnershipMismatch,
    AmbiguousCreate,
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Journal(_) => "ownership journal is invalid",
            Self::Io => "ownership journal I/O failed",
            Self::InvalidTransition => "ownership journal transition is invalid",
            Self::OwnershipMismatch => "resource ownership could not be verified",
            Self::AmbiguousCreate => "dispatched create remains unresolved",
        })
    }
}

impl Error for ReconcileError {}

pub fn reconcile(
    store: &JournalStore,
    journal: &mut OwnershipJournal,
    inspector: &impl OwnershipInspector,
) -> Result<ReconcileReport, ReconcileError> {
    let mut report = ReconcileReport {
        recovered: Vec::new(),
        removed: Vec::new(),
        blocked: Vec::new(),
    };
    for record in &mut journal.resources {
        match record.state {
            ResourceState::Removed => {}
            ResourceState::Intended => {
                record.state = ResourceState::Removed;
                report.removed.push(record.logical_name.clone());
            }
            ResourceState::Dispatched | ResourceState::Blocked => {
                match inspector.inspect(record)? {
                    ResourceObservation::Owned { binding, .. } => {
                        if let Some(expected) = &record.binding {
                            if expected != &binding {
                                record.state = ResourceState::Blocked;
                                report.blocked.push(record.logical_name.clone());
                                continue;
                            }
                        } else {
                            record.binding = Some(binding);
                        }
                        record.state = ResourceState::Bound;
                        report.recovered.push(record.logical_name.clone());
                    }
                    ResourceObservation::Absent | ResourceObservation::Foreign => {
                        record.state = ResourceState::Blocked;
                        report.blocked.push(record.logical_name.clone());
                    }
                }
            }
            ResourceState::Bound | ResourceState::Cleaning => match inspector.inspect(record)? {
                ResourceObservation::Absent => {
                    record.state = ResourceState::Removed;
                    report.removed.push(record.logical_name.clone());
                }
                ResourceObservation::Owned { binding, .. }
                    if record.binding.as_ref() == Some(&binding) =>
                {
                    report.recovered.push(record.logical_name.clone());
                }
                ResourceObservation::Owned { .. } | ResourceObservation::Foreign => {
                    record.state = ResourceState::Blocked;
                    report.blocked.push(record.logical_name.clone());
                }
            },
        }
    }
    journal.state = if report.is_blocked() {
        RunState::Blocked
    } else {
        RunState::Preparing
    };
    store.save(journal)?;
    Ok(report)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclEvidence {
    pub host_access: bool,
    pub sql_access: bool,
    pub host_default: bool,
    pub sql_default: bool,
    pub access_mask: bool,
    pub default_mask: bool,
}

impl AclEvidence {
    pub fn complete(&self) -> bool {
        self.host_access
            && self.sql_access
            && self.host_default
            && self.sql_default
            && self.access_mask
            && self.default_mask
    }
}

pub trait AclController {
    fn apply(
        &self,
        path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
    ) -> Result<(), MemberDirectoryError>;
    fn inspect(
        &self,
        path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
    ) -> Result<AclEvidence, MemberDirectoryError>;
}

pub struct CommandAclController<R> {
    runner: R,
}

impl<R> CommandAclController<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R: ProcessRunner> AclController for CommandAclController<R> {
    fn apply(
        &self,
        path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
    ) -> Result<(), MemberDirectoryError> {
        let entries = format!(
            "u:{host_uid}:rwx,u:{sql_uid}:rwx,d:u:{host_uid}:rwx,d:u:{sql_uid}:rwx,m:rwx,d:m:rwx"
        );
        self.runner
            .run(
                &CommandSpec::new("setfacl", "configure member data ACL", timeout)
                    .args(["-m", &entries])
                    .arg(path),
            )
            .map(|_| ())
            .map_err(|_| MemberDirectoryError::Acl)
    }

    fn inspect(
        &self,
        path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
    ) -> Result<AclEvidence, MemberDirectoryError> {
        let output = self
            .runner
            .run(
                &CommandSpec::new("getfacl", "inspect member data ACL", timeout)
                    .args(["--absolute-names", "--numeric"])
                    .arg(path),
            )
            .map_err(|_| MemberDirectoryError::Acl)?;
        Ok(parse_acl_evidence(&output.stdout, host_uid, sql_uid))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryBinding {
    pub canonical_path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub marker_device: u64,
    pub marker_inode: u64,
    pub marker_sha256: String,
    pub binding: ResourceBinding,
    pub acl: AclEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateDirectoryBinding {
    pub canonical_path: PathBuf,
    pub binding: ResourceBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDirectoryError {
    Escape,
    Symlink,
    Replaced,
    Permissions,
    Acl,
    Io,
}

impl fmt::Display for MemberDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Escape => "member data directory escapes the fixture root",
            Self::Symlink => "member data directory path contains a symlink",
            Self::Replaced => "member data directory identity changed",
            Self::Permissions => "member data directory permissions are not private",
            Self::Acl => "member data directory ACL is incomplete",
            Self::Io => "member data directory I/O failed",
        })
    }
}

impl Error for MemberDirectoryError {}

pub fn prepare_member_directory(
    root: &Path,
    path: &Path,
    host_uid: u32,
    sql_uid: u32,
    timeout: Duration,
    acl: &impl AclController,
) -> Result<DirectoryBinding, MemberDirectoryError> {
    let canonical_root = root.canonicalize().map_err(|_| MemberDirectoryError::Io)?;
    verify_private_root(&canonical_root)?;
    if path.parent() != Some(canonical_root.as_path()) || path.exists() {
        return Err(MemberDirectoryError::Escape);
    }
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path).map_err(|_| MemberDirectoryError::Io)?;
    let result = (|| {
        let canonical_path = path.canonicalize().map_err(|_| MemberDirectoryError::Io)?;
        if !canonical_path.starts_with(&canonical_root) || canonical_path == canonical_root {
            return Err(MemberDirectoryError::Escape);
        }
        acl.apply(&canonical_path, host_uid, sql_uid, timeout)?;
        let metadata =
            fs::symlink_metadata(&canonical_path).map_err(|_| MemberDirectoryError::Io)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(MemberDirectoryError::Symlink);
        }
        if metadata.uid() != host_uid {
            return Err(MemberDirectoryError::Permissions);
        }
        let marker_path = canonical_path.join(DIRECTORY_MARKER);
        let marker_value = directory_marker_value(&canonical_path);
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&marker_path)
            .map_err(|_| MemberDirectoryError::Io)?;
        marker
            .write_all(marker_value.as_bytes())
            .map_err(|_| MemberDirectoryError::Io)?;
        marker.sync_all().map_err(|_| MemberDirectoryError::Io)?;
        directory_binding(&canonical_path, host_uid, sql_uid, timeout, acl)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(path);
    }
    result
}

pub fn create_private_owned_directory(
    root: &Path,
    path: &Path,
) -> Result<PrivateDirectoryBinding, MemberDirectoryError> {
    let canonical_root = root.canonicalize().map_err(|_| MemberDirectoryError::Io)?;
    verify_private_root(&canonical_root)?;
    let parent = path.parent().ok_or(MemberDirectoryError::Escape)?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|_| MemberDirectoryError::Io)?;
    if parent != canonical_parent
        || !canonical_parent.starts_with(&canonical_root)
        || canonical_parent == canonical_root && path == canonical_root
    {
        return Err(MemberDirectoryError::Escape);
    }
    if path.exists() {
        return Err(MemberDirectoryError::Replaced);
    }
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path).map_err(|_| MemberDirectoryError::Io)?;
    let marker_path = path.join(DIRECTORY_MARKER);
    let marker_value = directory_marker_value(path);
    let mut marker = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&marker_path)
        .map_err(|_| MemberDirectoryError::Io)?;
    marker
        .write_all(marker_value.as_bytes())
        .map_err(|_| MemberDirectoryError::Io)?;
    marker.sync_all().map_err(|_| MemberDirectoryError::Io)?;
    inspect_private_owned_directory(&canonical_root, path)
}

pub fn inspect_private_owned_directory(
    root: &Path,
    path: &Path,
) -> Result<PrivateDirectoryBinding, MemberDirectoryError> {
    let binding = inspect_owned_directory(root, path)?;
    let metadata = fs::symlink_metadata(&binding.canonical_path)
        .map_err(|_| MemberDirectoryError::Replaced)?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(MemberDirectoryError::Permissions);
    }
    Ok(binding)
}

pub fn inspect_owned_directory(
    root: &Path,
    path: &Path,
) -> Result<PrivateDirectoryBinding, MemberDirectoryError> {
    let canonical_root = root.canonicalize().map_err(|_| MemberDirectoryError::Io)?;
    verify_private_root(&canonical_root)?;
    let canonical_path = path
        .canonicalize()
        .map_err(|_| MemberDirectoryError::Replaced)?;
    if canonical_path != path
        || !canonical_path.starts_with(&canonical_root)
        || canonical_path == canonical_root
    {
        return Err(MemberDirectoryError::Escape);
    }
    let metadata =
        fs::symlink_metadata(&canonical_path).map_err(|_| MemberDirectoryError::Replaced)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(MemberDirectoryError::Replaced);
    }
    let mut digest = Sha256::new();
    let marker_path = canonical_path.join(DIRECTORY_MARKER);
    let marker_metadata =
        fs::symlink_metadata(&marker_path).map_err(|_| MemberDirectoryError::Replaced)?;
    if !marker_metadata.is_file() || marker_metadata.file_type().is_symlink() {
        return Err(MemberDirectoryError::Replaced);
    }
    let marker = fs::read(&marker_path).map_err(|_| MemberDirectoryError::Replaced)?;
    digest.update(canonical_path.as_os_str().as_bytes());
    digest.update(metadata.dev().to_le_bytes());
    digest.update(metadata.ino().to_le_bytes());
    digest.update(marker_metadata.dev().to_le_bytes());
    digest.update(marker_metadata.ino().to_le_bytes());
    digest.update(Sha256::digest(marker));
    Ok(PrivateDirectoryBinding {
        canonical_path,
        binding: ResourceBinding {
            immutable_id: format!("device:{}/inode:{}", metadata.dev(), metadata.ino()),
            attributes_sha256: hex(&digest.finalize()),
        },
    })
}

pub fn verify_member_directory(
    root: &Path,
    expected: &DirectoryBinding,
    host_uid: u32,
    sql_uid: u32,
    timeout: Duration,
    acl: &impl AclController,
) -> Result<(), MemberDirectoryError> {
    let current = inspect_member_directory(
        root,
        &expected.canonical_path,
        host_uid,
        sql_uid,
        timeout,
        acl,
    )?;
    if &current != expected {
        return Err(MemberDirectoryError::Replaced);
    }
    Ok(())
}

pub fn inspect_member_directory(
    root: &Path,
    path: &Path,
    host_uid: u32,
    sql_uid: u32,
    timeout: Duration,
    acl: &impl AclController,
) -> Result<DirectoryBinding, MemberDirectoryError> {
    let canonical_root = root.canonicalize().map_err(|_| MemberDirectoryError::Io)?;
    let canonical = path
        .canonicalize()
        .map_err(|_| MemberDirectoryError::Replaced)?;
    if canonical != path || !canonical.starts_with(&canonical_root) || canonical == canonical_root {
        return Err(MemberDirectoryError::Replaced);
    }
    directory_binding(&canonical, host_uid, sql_uid, timeout, acl)
}

fn directory_binding(
    canonical_path: &Path,
    host_uid: u32,
    sql_uid: u32,
    timeout: Duration,
    acl: &impl AclController,
) -> Result<DirectoryBinding, MemberDirectoryError> {
    let metadata =
        fs::symlink_metadata(canonical_path).map_err(|_| MemberDirectoryError::Replaced)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() || metadata.uid() != host_uid {
        return Err(MemberDirectoryError::Replaced);
    }
    let evidence = acl.inspect(canonical_path, host_uid, sql_uid, timeout)?;
    if !evidence.complete() {
        return Err(MemberDirectoryError::Acl);
    }
    let marker_path = canonical_path.join(DIRECTORY_MARKER);
    let marker_metadata =
        fs::symlink_metadata(&marker_path).map_err(|_| MemberDirectoryError::Replaced)?;
    if !marker_metadata.is_file() || marker_metadata.file_type().is_symlink() {
        return Err(MemberDirectoryError::Replaced);
    }
    let marker = fs::read(&marker_path).map_err(|_| MemberDirectoryError::Replaced)?;
    let marker_sha256 = hex(&Sha256::digest(&marker));
    let mut digest = Sha256::new();
    digest.update(canonical_path.as_os_str().as_bytes());
    digest.update(metadata.dev().to_le_bytes());
    digest.update(metadata.ino().to_le_bytes());
    digest.update(marker_metadata.dev().to_le_bytes());
    digest.update(marker_metadata.ino().to_le_bytes());
    digest.update(marker_sha256.as_bytes());
    digest.update([
        evidence.host_access as u8,
        evidence.sql_access as u8,
        evidence.host_default as u8,
        evidence.sql_default as u8,
    ]);
    Ok(DirectoryBinding {
        canonical_path: canonical_path.to_path_buf(),
        device: metadata.dev(),
        inode: metadata.ino(),
        marker_device: marker_metadata.dev(),
        marker_inode: marker_metadata.ino(),
        marker_sha256,
        binding: ResourceBinding {
            immutable_id: format!("device:{}/inode:{}", metadata.dev(), metadata.ino()),
            attributes_sha256: hex(&digest.finalize()),
        },
        acl: evidence,
    })
}

fn initialize_private_root(root: &Path) -> Result<(), ReconcileError> {
    if root.exists() {
        let metadata = fs::symlink_metadata(root).map_err(|_| ReconcileError::Io)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(ReconcileError::OwnershipMismatch);
        }
    } else {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(root).map_err(|_| ReconcileError::Io)?;
    }
    verify_private_root(root).map_err(|_| ReconcileError::OwnershipMismatch)
}

fn verify_private_root(root: &Path) -> Result<(), MemberDirectoryError> {
    let metadata = fs::symlink_metadata(root).map_err(|_| MemberDirectoryError::Io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(MemberDirectoryError::Symlink);
    }
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.permissions().mode() & 0o077 != 0 {
        return Err(MemberDirectoryError::Permissions);
    }
    Ok(())
}

fn canonical_missing_path(root: &Path) -> Result<PathBuf, LockError> {
    let mut suffix = Vec::new();
    let mut current = root;
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() && current == root {
                    return Err(LockError::InvalidRoot);
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    current
                        .file_name()
                        .ok_or(LockError::InvalidRoot)?
                        .to_os_string(),
                );
                current = current.parent().ok_or(LockError::InvalidRoot)?;
            }
            Err(_) => return Err(LockError::InvalidRoot),
        }
    }
    let mut canonical = current.canonicalize().map_err(|_| LockError::InvalidRoot)?;
    for part in suffix.into_iter().rev() {
        canonical.push(part);
    }
    Ok(canonical)
}

fn transition(
    journal: &mut OwnershipJournal,
    index: usize,
    from: ResourceState,
    to: ResourceState,
) -> Result<(), ReconcileError> {
    let record = journal
        .resources
        .get_mut(index)
        .ok_or(ReconcileError::InvalidTransition)?;
    if record.state != from {
        return Err(ReconcileError::InvalidTransition);
    }
    record.state = to;
    Ok(())
}

fn acl_line(output: &str, prefix: &str, uid: u32) -> bool {
    output
        .lines()
        .any(|line| line == format!("{prefix}:{uid}:rwx"))
}

pub fn parse_acl_evidence(output: &str, host_uid: u32, sql_uid: u32) -> AclEvidence {
    AclEvidence {
        host_access: acl_line(output, "user", host_uid),
        sql_access: acl_line(output, "user", sql_uid),
        host_default: acl_line(output, "default:user", host_uid),
        sql_default: acl_line(output, "default:user", sql_uid),
        access_mask: output.lines().any(|line| line == "mask::rwx"),
        default_mask: output.lines().any(|line| line == "default:mask::rwx"),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn directory_marker_value(path: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(path.as_os_str().as_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(
        DIRECTORY_MARKER_SEQUENCE
            .fetch_add(1, Ordering::Relaxed)
            .to_le_bytes(),
    );
    digest.update(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    hex(&digest.finalize())
}
