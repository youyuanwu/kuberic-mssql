use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use kuberic_mssql::SqlServerEulaAcknowledgement;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const ACKNOWLEDGEMENT_SCHEMA_VERSION: u32 = 1;
pub const PINNED_SQL_SERVER_IMAGE: &str = concat!(
    "mcr.microsoft.com/mssql/server@",
    "sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726"
);
const MAX_ACKNOWLEDGEMENT_BYTES: u64 = 4096;
const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixtureConfigError {
    AcknowledgementUnavailable,
    AcknowledgementNotRegular,
    AcknowledgementTooLarge,
    AcknowledgementMalformed,
    AcknowledgementSchema(u32),
    AcknowledgementDenied,
    AcknowledgementChanged,
    InvalidFixtureRoot,
}

impl fmt::Display for FixtureConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AcknowledgementUnavailable => {
                formatter.write_str("SQL Server EULA acknowledgement is unavailable")
            }
            Self::AcknowledgementNotRegular => {
                formatter.write_str("SQL Server EULA acknowledgement must be a regular file")
            }
            Self::AcknowledgementTooLarge => {
                formatter.write_str("SQL Server EULA acknowledgement exceeds 4096 bytes")
            }
            Self::AcknowledgementMalformed => {
                formatter.write_str("SQL Server EULA acknowledgement is malformed")
            }
            Self::AcknowledgementSchema(version) => {
                write!(
                    formatter,
                    "unsupported SQL Server EULA acknowledgement schema version {version}"
                )
            }
            Self::AcknowledgementDenied => {
                formatter.write_str("SQL Server EULA acknowledgement must be explicitly true")
            }
            Self::AcknowledgementChanged => {
                formatter.write_str("SQL Server EULA acknowledgement changed during launch")
            }
            Self::InvalidFixtureRoot => {
                formatter.write_str("three-replica fixture root must be an absolute path")
            }
        }
    }
}

impl Error for FixtureConfigError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcknowledgementDocument {
    schema_version: u32,
    sql_server_eula: EulaField,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EulaField {
    accepted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgementSource {
    path: PathBuf,
    device: u64,
    inode: u64,
    length: u64,
    change_time_seconds: i64,
    change_time_nanoseconds: i64,
    sha256: [u8; 32],
}

impl AcknowledgementSource {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sha256(&self) -> &[u8; 32] {
        &self.sha256
    }
}

#[derive(Debug, Clone)]
pub struct LaunchAuthorization {
    source: Option<AcknowledgementSource>,
    acknowledgement: SqlServerEulaAcknowledgement,
}

impl LaunchAuthorization {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, FixtureConfigError> {
        let path = path.as_ref();
        let (source, accepted) = read_acknowledgement(path)?;
        let acknowledgement = SqlServerEulaAcknowledgement::new(accepted)
            .map_err(|_| FixtureConfigError::AcknowledgementDenied)?;
        Ok(Self {
            source: Some(source),
            acknowledgement,
        })
    }

    pub fn for_test_fixture() -> Self {
        Self {
            source: None,
            acknowledgement: SqlServerEulaAcknowledgement::new(true)
                .expect("test fixture acknowledgement is affirmative"),
        }
    }

    pub fn source(&self) -> Option<&AcknowledgementSource> {
        self.source.as_ref()
    }

    pub fn revalidate(&self) -> Result<(), FixtureConfigError> {
        let Some(source) = &self.source else {
            return Ok(());
        };
        let (current, accepted) = read_acknowledgement(&source.path)?;
        SqlServerEulaAcknowledgement::new(accepted)
            .map_err(|_| FixtureConfigError::AcknowledgementChanged)?;
        if current != *source {
            return Err(FixtureConfigError::AcknowledgementChanged);
        }
        Ok(())
    }

    pub fn sql_server_environment(&self) -> [(&'static str, &'static str); 1] {
        debug_assert!(self.acknowledgement.accepted());
        [("ACCEPT_EULA", "Y")]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourcePolicy {
    pub container_memory_bytes: u64,
    pub sql_server_memory_mb: u32,
    pub container_cpu_nanos: u64,
    pub minimum_available_memory_bytes: u64,
    pub minimum_fixture_bytes: u64,
    pub minimum_docker_root_before_pull_bytes: u64,
    pub minimum_docker_root_after_image_bytes: u64,
    pub minimum_effective_cpus: u32,
}

impl Default for ResourcePolicy {
    fn default() -> Self {
        Self {
            container_memory_bytes: 3 * GIB,
            sql_server_memory_mb: 2048,
            container_cpu_nanos: 2_000_000_000,
            minimum_available_memory_bytes: 10 * GIB,
            minimum_fixture_bytes: 15 * GIB,
            minimum_docker_root_before_pull_bytes: 8 * GIB,
            minimum_docker_root_after_image_bytes: 2 * GIB,
            minimum_effective_cpus: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageDeadlines {
    pub docker_command: Duration,
    pub image_pull: Duration,
    pub tls_helper: Duration,
    pub sql_connect: Duration,
    pub sql_batch: Duration,
    pub member_readiness: Duration,
    pub availability_group: Duration,
    pub kuberic_convergence: Duration,
    pub marker_convergence: Duration,
    pub complete_run: Duration,
    pub cleanup: Duration,
}

impl Default for StageDeadlines {
    fn default() -> Self {
        Self {
            docker_command: Duration::from_secs(30),
            image_pull: Duration::from_secs(900),
            tls_helper: Duration::from_secs(30),
            sql_connect: Duration::from_secs(15),
            sql_batch: Duration::from_secs(30),
            member_readiness: Duration::from_secs(180),
            availability_group: Duration::from_secs(300),
            kuberic_convergence: Duration::from_secs(60),
            marker_convergence: Duration::from_secs(60),
            complete_run: Duration::from_secs(1200),
            cleanup: Duration::from_secs(180),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FixtureConfig {
    root: PathBuf,
    authorization: LaunchAuthorization,
    image: &'static str,
    resources: ResourcePolicy,
    deadlines: StageDeadlines,
}

impl FixtureConfig {
    pub fn new(
        root: impl Into<PathBuf>,
        acknowledgement: impl AsRef<Path>,
    ) -> Result<Self, FixtureConfigError> {
        Self::with_authorization(root.into(), LaunchAuthorization::load(acknowledgement)?)
    }

    pub fn for_test_fixture(root: impl Into<PathBuf>) -> Result<Self, FixtureConfigError> {
        Self::with_authorization(root.into(), LaunchAuthorization::for_test_fixture())
    }

    pub(crate) fn for_test_fixture_with_policy(
        root: impl Into<PathBuf>,
        resources: ResourcePolicy,
        deadlines: StageDeadlines,
    ) -> Result<Self, FixtureConfigError> {
        let mut config =
            Self::with_authorization(root.into(), LaunchAuthorization::for_test_fixture())?;
        config.resources = resources;
        config.deadlines = deadlines;
        Ok(config)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn authorization(&self) -> &LaunchAuthorization {
        &self.authorization
    }

    pub fn image(&self) -> &'static str {
        self.image
    }

    pub fn resources(&self) -> ResourcePolicy {
        self.resources
    }

    pub fn deadlines(&self) -> StageDeadlines {
        self.deadlines
    }

    fn with_authorization(
        root: PathBuf,
        authorization: LaunchAuthorization,
    ) -> Result<Self, FixtureConfigError> {
        Ok(Self {
            root: canonical_fixture_root(root)?,
            authorization,
            image: PINNED_SQL_SERVER_IMAGE,
            resources: ResourcePolicy::default(),
            deadlines: StageDeadlines::default(),
        })
    }
}

fn read_acknowledgement(path: &Path) -> Result<(AcknowledgementSource, bool), FixtureConfigError> {
    let mut file = open_regular(path)?;
    let metadata = file
        .metadata()
        .map_err(|_| FixtureConfigError::AcknowledgementUnavailable)?;
    if !metadata.is_file() {
        return Err(FixtureConfigError::AcknowledgementNotRegular);
    }
    if metadata.len() > MAX_ACKNOWLEDGEMENT_BYTES {
        return Err(FixtureConfigError::AcknowledgementTooLarge);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(MAX_ACKNOWLEDGEMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| FixtureConfigError::AcknowledgementUnavailable)?;
    if bytes.len() as u64 > MAX_ACKNOWLEDGEMENT_BYTES {
        return Err(FixtureConfigError::AcknowledgementTooLarge);
    }
    let document: AcknowledgementDocument =
        serde_json::from_slice(&bytes).map_err(|_| FixtureConfigError::AcknowledgementMalformed)?;
    if document.schema_version != ACKNOWLEDGEMENT_SCHEMA_VERSION {
        return Err(FixtureConfigError::AcknowledgementSchema(
            document.schema_version,
        ));
    }
    let path = path
        .canonicalize()
        .map_err(|_| FixtureConfigError::AcknowledgementUnavailable)?;
    let digest = Sha256::digest(&bytes);
    Ok((
        AcknowledgementSource {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            change_time_seconds: metadata.ctime(),
            change_time_nanoseconds: metadata.ctime_nsec(),
            sha256: digest.into(),
        },
        document.sql_server_eula.accepted,
    ))
}

fn open_regular(path: &Path) -> Result<File, FixtureConfigError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| {
            if error.raw_os_error() == Some(libc::ELOOP) {
                FixtureConfigError::AcknowledgementNotRegular
            } else {
                FixtureConfigError::AcknowledgementUnavailable
            }
        })
}

fn canonical_fixture_root(root: PathBuf) -> Result<PathBuf, FixtureConfigError> {
    if !root.is_absolute() {
        return Err(FixtureConfigError::InvalidFixtureRoot);
    }
    let mut missing = Vec::new();
    let mut existing = root.as_path();
    loop {
        match existing.symlink_metadata() {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = existing
                    .file_name()
                    .ok_or(FixtureConfigError::InvalidFixtureRoot)?;
                missing.push(name.to_os_string());
                existing = existing
                    .parent()
                    .ok_or(FixtureConfigError::InvalidFixtureRoot)?;
            }
            Err(_) => return Err(FixtureConfigError::InvalidFixtureRoot),
        }
    }
    let mut canonical = existing
        .canonicalize()
        .map_err(|_| FixtureConfigError::InvalidFixtureRoot)?;
    for component in missing.into_iter().rev() {
        canonical.push(component);
    }
    Ok(canonical)
}
