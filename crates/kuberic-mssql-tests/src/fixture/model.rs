use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIncarnation {
    pub pid: u32,
    pub starttime_ticks: u64,
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
    Directory,
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
    pub intent: Option<ResourceBinding>,
    pub binding: Option<ResourceBinding>,
    pub state: ResourceState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalError {
    Malformed,
    UnsupportedSchema(u32),
}

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
    Acknowledgement,
    DataDirectory,
    Tls,
    Secret,
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
            Self::Acknowledgement => "EULA acknowledgement validation failed",
            Self::DataDirectory => "member data directory validation failed",
            Self::Tls => "TLS asset operation failed",
            Self::Secret => "private credential operation failed",
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SanitizedFailure {
    stage: FailureStage,
    category: FailureCategory,
    detail: Option<String>,
}

impl SanitizedFailure {
    pub const fn new(stage: FailureStage, category: FailureCategory) -> Self {
        Self {
            stage,
            category,
            detail: None,
        }
    }

    pub fn with_detail(
        stage: FailureStage,
        category: FailureCategory,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            stage,
            category,
            detail: Some(detail.into()),
        }
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        let context = context.into();
        self.detail = Some(match self.detail.take() {
            Some(detail) => format!("{context}: {detail}"),
            None => context,
        });
        self
    }
}

impl fmt::Display for SanitizedFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.stage, self.category)?;
        if let Some(detail) = &self.detail {
            write!(formatter, " ({detail})")?;
        }
        Ok(())
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
        self.primary.clone()
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
