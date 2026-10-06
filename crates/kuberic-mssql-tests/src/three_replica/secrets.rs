use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use super::model::ResourceBinding;

const MAX_PRIVATE_FILE_BYTES: u64 = 4096;
const RANDOM_BYTES: usize = 32;

#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(Zeroizing<String>);

impl SecretValue {
    pub fn generate_password() -> Result<Self, SecretError> {
        let mut random = [0_u8; RANDOM_BYTES];
        File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut random))
            .map_err(|_| SecretError::Random)?;
        let mut value = String::with_capacity(RANDOM_BYTES * 2 + 4);
        for byte in random {
            use std::fmt::Write as _;
            write!(value, "{byte:02x}").map_err(|_| SecretError::Random)?;
        }
        value.push_str("Aa1!");
        Ok(Self(Zeroizing::new(value)))
    }

    pub fn from_test(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateFile {
    path: PathBuf,
    binding: ResourceBinding,
    sql_shared: bool,
}

impl PrivateFile {
    pub fn create(path: impl Into<PathBuf>, value: &SecretValue) -> Result<Self, SecretError> {
        Self::create_bytes(path, value.expose().as_bytes())
    }

    pub fn create_text(
        path: impl Into<PathBuf>,
        value: impl AsRef<str>,
    ) -> Result<Self, SecretError> {
        Self::create_bytes(path, value.as_ref().as_bytes())
    }

    pub fn create_bytes(path: impl Into<PathBuf>, value: &[u8]) -> Result<Self, SecretError> {
        let path = path.into();
        if value.is_empty() || value.len() as u64 > MAX_PRIVATE_FILE_BYTES {
            return Err(SecretError::InvalidValue);
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    SecretError::AlreadyExists
                } else {
                    SecretError::Io
                }
            })?;
        file.write_all(value).map_err(|_| SecretError::Io)?;
        file.sync_all().map_err(|_| SecretError::Io)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|_| SecretError::Io)?;
        Self::inspect(path)
    }

    pub fn inspect(path: impl Into<PathBuf>) -> Result<Self, SecretError> {
        Self::inspect_mode(path.into(), false)
    }

    pub fn inspect_sql_shared(path: impl Into<PathBuf>) -> Result<Self, SecretError> {
        Self::inspect_mode(path.into(), true)
    }

    fn inspect_mode(path: PathBuf, sql_shared: bool) -> Result<Self, SecretError> {
        let metadata = fs::symlink_metadata(&path).map_err(|_| SecretError::Io)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != unsafe { libc::geteuid() }
            || if sql_shared {
                metadata.permissions().mode() & 0o007 != 0
            } else {
                metadata.permissions().mode() & 0o077 != 0
            }
            || metadata.len() == 0
            || metadata.len() > MAX_PRIVATE_FILE_BYTES
        {
            return Err(SecretError::Permissions);
        }
        let bytes = fs::read(&path).map_err(|_| SecretError::Io)?;
        let mut attributes = Sha256::new();
        attributes.update(path.as_os_str().as_encoded_bytes());
        attributes.update(metadata.dev().to_le_bytes());
        attributes.update(metadata.ino().to_le_bytes());
        attributes.update(metadata.len().to_le_bytes());
        attributes.update(Sha256::digest(&bytes));
        Ok(Self {
            path,
            binding: ResourceBinding {
                immutable_id: format!("device:{}/inode:{}", metadata.dev(), metadata.ino()),
                attributes_sha256: hex(&attributes.finalize()),
            },
            sql_shared,
        })
    }

    pub fn verify(&self) -> Result<(), SecretError> {
        if &Self::inspect_mode(self.path.clone(), self.sql_shared)? != self {
            return Err(SecretError::Replaced);
        }
        Ok(())
    }

    pub fn read_secret(&self) -> Result<SecretValue, SecretError> {
        self.verify()?;
        let bytes = fs::read(&self.path).map_err(|_| SecretError::Io)?;
        if bytes
            .iter()
            .any(|byte| matches!(byte, b'\0' | b'\n' | b'\r'))
        {
            return Err(SecretError::InvalidValue);
        }
        String::from_utf8(bytes)
            .map(SecretValue::from_test)
            .map_err(|_| SecretError::InvalidValue)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn binding(&self) -> &ResourceBinding {
        &self.binding
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialFiles {
    pub sa_username: PrivateFile,
    pub sa_password: PrivateFile,
    pub admin_username: PrivateFile,
    pub admin_password: PrivateFile,
    pub observer_username: PrivateFile,
    pub observer_password: PrivateFile,
    pub endpoint_master_key_passwords: [PrivateFile; 3],
}

impl CredentialFiles {
    pub fn generate(root: &Path, run_id: &str) -> Result<Self, SecretError> {
        let directory = root.join("credentials");
        create_private_directory(&directory)?;
        let suffix = validated_suffix(run_id)?;
        let sa_password = SecretValue::generate_password()?;
        let admin_password = SecretValue::generate_password()?;
        let observer_password = SecretValue::generate_password()?;
        let admin_username = format!("km_admin_{suffix}");
        let observer_username = format!("km_observer_{suffix}");
        let endpoint_master_key_passwords = (0..3)
            .map(|index| {
                PrivateFile::create(
                    directory.join(format!("endpoint-master-key-{}", index + 1)),
                    &SecretValue::generate_password()?,
                )
            })
            .collect::<Result<Vec<_>, SecretError>>()?
            .try_into()
            .map_err(|_| SecretError::Io)?;
        Ok(Self {
            sa_username: PrivateFile::create_text(directory.join("sa-username"), "sa")?,
            sa_password: PrivateFile::create(directory.join("sa-password"), &sa_password)?,
            admin_username: PrivateFile::create_text(
                directory.join("admin-username"),
                admin_username,
            )?,
            admin_password: PrivateFile::create(directory.join("admin-password"), &admin_password)?,
            observer_username: PrivateFile::create_text(
                directory.join("observer-username"),
                observer_username,
            )?,
            observer_password: PrivateFile::create(
                directory.join("observer-password"),
                &observer_password,
            )?,
            endpoint_master_key_passwords,
        })
    }

    pub fn all(&self) -> impl Iterator<Item = &PrivateFile> {
        [
            &self.sa_username,
            &self.sa_password,
            &self.admin_username,
            &self.admin_password,
            &self.observer_username,
            &self.observer_password,
            &self.endpoint_master_key_passwords[0],
            &self.endpoint_master_key_passwords[1],
            &self.endpoint_master_key_passwords[2],
        ]
        .into_iter()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretError {
    Random,
    InvalidValue,
    InvalidIdentity,
    AlreadyExists,
    Permissions,
    Replaced,
    Io,
}

impl fmt::Display for SecretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Random => "secure random credential generation failed",
            Self::InvalidValue => "private file value is invalid",
            Self::InvalidIdentity => "credential identity is invalid",
            Self::AlreadyExists => "private file already exists",
            Self::Permissions => "private file ownership or permissions are invalid",
            Self::Replaced => "private file identity changed",
            Self::Io => "private file operation failed",
        })
    }
}

impl std::error::Error for SecretError {}

pub(crate) fn create_private_directory(path: &Path) -> Result<(), SecretError> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            SecretError::AlreadyExists
        } else {
            SecretError::Io
        }
    })?;
    let metadata = fs::symlink_metadata(path).map_err(|_| SecretError::Io)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(SecretError::Permissions);
    }
    Ok(())
}

fn validated_suffix(run_id: &str) -> Result<&str, SecretError> {
    if run_id.len() < 8 || run_id.len() > 32 || !run_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(SecretError::InvalidIdentity);
    }
    Ok(&run_id[..8])
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
