use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::fixture::docker::{DockerApi, DockerError};

const LEGACY_OWNER: &[u8] = b"kuberic-sqlserver-observer-container-v1\n";
pub const LEGACY_CLEANUP_SECTION: &str = "Legacy Python fixture cleanup";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyStateError {
    root: PathBuf,
    indicators: Vec<String>,
}

impl LegacyStateError {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn indicators(&self) -> &[String] {
        &self.indicators
    }
}

impl fmt::Display for LegacyStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legacy Python fixture state at {} ({}) was not adopted or deleted; complete the documented \"{}\" procedure before retrying",
            self.root.display(),
            self.indicators.join(", "),
            LEGACY_CLEANUP_SECTION
        )
    }
}

impl std::error::Error for LegacyStateError {}

pub(crate) fn detect_legacy_state(
    root: &Path,
    docker: &impl DockerApi,
    timeout: Duration,
) -> Result<(), LegacyProbeError> {
    let mut indicators = Vec::new();
    if root
        .join("fixture-run.json")
        .try_exists()
        .map_err(|_| LegacyProbeError::Io)?
    {
        indicators.push("fixture-run.json".to_owned());
    }
    let owner = root.join("owner");
    if owner.try_exists().map_err(|_| LegacyProbeError::Io)?
        && std::fs::read(&owner).map_err(|_| LegacyProbeError::Io)? == LEGACY_OWNER
    {
        indicators.push("legacy owner marker".to_owned());
    }
    if !indicators.is_empty() {
        return Err(LegacyProbeError::Legacy(LegacyStateError {
            root: root.to_path_buf(),
            indicators,
        }));
    }
    if root
        .join("ownership.json")
        .try_exists()
        .map_err(|_| LegacyProbeError::Io)?
    {
        return Ok(());
    }
    let container_name = legacy_container_name(root)?;
    if docker
        .inspect_container(&container_name, timeout)
        .map_err(LegacyProbeError::Docker)?
        .is_some()
    {
        indicators.push(format!("container {container_name}"));
    }
    if !indicators.is_empty() {
        Err(LegacyProbeError::Legacy(LegacyStateError {
            root: root.to_path_buf(),
            indicators,
        }))
    } else {
        Ok(())
    }
}

pub(crate) fn legacy_container_name(root: &Path) -> Result<String, LegacyProbeError> {
    let text = root.to_str().ok_or(LegacyProbeError::InvalidRoot)?;
    let digest = Sha256::digest(text.as_bytes());
    Ok(format!(
        "kuberic-mssql-observer-{}",
        digest[..6]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

#[derive(Debug)]
pub(crate) enum LegacyProbeError {
    InvalidRoot,
    Io,
    Docker(DockerError),
    Legacy(LegacyStateError),
}

impl fmt::Display for LegacyProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRoot => formatter.write_str("legacy fixture root is invalid"),
            Self::Io => formatter.write_str("legacy fixture state could not be inspected"),
            Self::Docker(error) => write!(formatter, "legacy container inspection failed: {error}"),
            Self::Legacy(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for LegacyProbeError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::fixture::docker::{
        ContainerInspection, ContainerLimits, ContainerRequest, DockerCapabilities,
        ImageInspection, NetworkInspection, NetworkRequest,
    };

    use super::*;

    struct LegacyContainerDocker {
        name: String,
    }

    impl DockerApi for LegacyContainerDocker {
        fn capabilities(&self, _: Duration) -> Result<DockerCapabilities, DockerError> {
            unreachable!()
        }

        fn docker_root(&self, _: Duration) -> Result<PathBuf, DockerError> {
            unreachable!()
        }

        fn inspect_image(
            &self,
            _: &str,
            _: Duration,
        ) -> Result<Option<ImageInspection>, DockerError> {
            unreachable!()
        }

        fn pull_image(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            unreachable!()
        }

        fn inspect_network(
            &self,
            _: &str,
            _: Duration,
        ) -> Result<Option<NetworkInspection>, DockerError> {
            unreachable!()
        }

        fn create_network(&self, _: &NetworkRequest, _: Duration) -> Result<String, DockerError> {
            unreachable!()
        }

        fn remove_network(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            unreachable!()
        }

        fn inspect_container(
            &self,
            identity: &str,
            _: Duration,
        ) -> Result<Option<ContainerInspection>, DockerError> {
            assert_eq!(identity, self.name);
            Ok(Some(ContainerInspection {
                id: "legacy-container-id".to_owned(),
                name: self.name.clone(),
                hostname: "legacy".to_owned(),
                image_id: "legacy-image".to_owned(),
                labels: BTreeMap::new(),
                environment: Vec::new(),
                user: "mssql".to_owned(),
                running: true,
                restart_policy: "no".to_owned(),
                network_mode: "bridge".to_owned(),
                network_names: Vec::new(),
                mounts: Vec::new(),
                ports: Vec::new(),
                limits: ContainerLimits {
                    memory_bytes: 0,
                    memory_swap_bytes: 0,
                    nano_cpus: 0,
                },
            }))
        }

        fn create_container(
            &self,
            _: &ContainerRequest,
            _: Duration,
        ) -> Result<String, DockerError> {
            unreachable!()
        }

        fn start_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            unreachable!()
        }

        fn stop_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            unreachable!()
        }

        fn remove_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            unreachable!()
        }
    }

    #[test]
    fn deterministic_legacy_container_is_refused_without_file_markers() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("fixture");
        let name = legacy_container_name(&root).unwrap();
        let error = detect_legacy_state(
            &root,
            &LegacyContainerDocker { name: name.clone() },
            Duration::from_secs(1),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains(&root.display().to_string()));
        assert!(error.contains(&name));
        assert!(error.contains("not adopted or deleted"));
        assert!(error.contains(LEGACY_CLEANUP_SECTION));
    }
}
