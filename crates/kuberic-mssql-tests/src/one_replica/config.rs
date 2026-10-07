use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::fixture::config::{FixtureConfig, FixtureConfigError, ResourcePolicy, StageDeadlines};

pub const ONE_REPLICA_ROOT_ENV: &str = "SQLSERVER_ONE_REPLICA_ROOT";
const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct OneReplicaConfig {
    fixture: FixtureConfig,
}

impl OneReplicaConfig {
    pub fn for_test_fixture(root: impl Into<PathBuf>) -> Result<Self, FixtureConfigError> {
        let resources = ResourcePolicy {
            minimum_available_memory_bytes: 5 * GIB,
            minimum_fixture_bytes: 6 * GIB,
            ..ResourcePolicy::default()
        };
        let deadlines = StageDeadlines {
            availability_group: Duration::from_secs(1),
            kuberic_convergence: Duration::from_secs(1),
            marker_convergence: Duration::from_secs(1),
            complete_run: Duration::from_secs(600),
            ..StageDeadlines::default()
        };
        FixtureConfig::for_test_fixture_with_policy(root, resources, deadlines)
            .map(|fixture| Self { fixture })
    }

    pub fn from_environment() -> Result<Self, FixtureConfigError> {
        Self::for_test_fixture(selected_root(
            env::var_os(ONE_REPLICA_ROOT_ENV).map(PathBuf::from),
            env::var_os("GITHUB_ACTIONS").as_deref(),
            env::var_os("RUNNER_TEMP").map(PathBuf::from),
            env::var_os("HOME").map(PathBuf::from),
        )?)
    }

    pub fn root(&self) -> &Path {
        self.fixture.root()
    }

    pub(crate) fn fixture(&self) -> &FixtureConfig {
        &self.fixture
    }
}

pub(crate) fn selected_root(
    explicit: Option<PathBuf>,
    github_actions: Option<&std::ffi::OsStr>,
    runner_temp: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Result<PathBuf, FixtureConfigError> {
    if let Some(root) = explicit {
        return Ok(root);
    }
    if github_actions == Some(std::ffi::OsStr::new("true")) {
        return runner_temp
            .map(|root| root.join("sqlserver-observer"))
            .ok_or(FixtureConfigError::InvalidFixtureRoot);
    }
    home.map(|root| root.join(".local/state/kuberic-mssql/fixture"))
        .ok_or(FixtureConfigError::InvalidFixtureRoot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_selection_prefers_explicit_then_ci_then_legacy_local_default() {
        assert_eq!(
            selected_root(
                Some(PathBuf::from("/explicit")),
                Some(std::ffi::OsStr::new("true")),
                Some(PathBuf::from("/runner")),
                Some(PathBuf::from("/home")),
            )
            .unwrap(),
            PathBuf::from("/explicit")
        );
        assert_eq!(
            selected_root(
                None,
                Some(std::ffi::OsStr::new("true")),
                Some(PathBuf::from("/runner")),
                Some(PathBuf::from("/home")),
            )
            .unwrap(),
            PathBuf::from("/runner/sqlserver-observer")
        );
        assert_eq!(
            selected_root(None, None, None, Some(PathBuf::from("/home")),).unwrap(),
            PathBuf::from("/home/.local/state/kuberic-mssql/fixture")
        );
    }
}
