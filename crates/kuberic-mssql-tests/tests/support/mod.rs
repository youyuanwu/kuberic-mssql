use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use kuberic_mssql_tests::three_replica::{
    ContainerInspection, ContainerLimits, ContainerRequest, DockerApi, DockerCapabilities,
    DockerError, ImageInspection, NetworkInspection, NetworkRequest,
};

#[derive(Default)]
pub struct FakeDocker {
    containers: HashMap<String, ContainerInspection>,
}

impl FakeDocker {
    #[allow(dead_code)]
    pub fn with_container(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.containers.insert(
            name.clone(),
            ContainerInspection {
                id: format!("{name}-id"),
                name,
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
            },
        );
        self
    }
}

impl DockerApi for FakeDocker {
    fn capabilities(&self, _: Duration) -> Result<DockerCapabilities, DockerError> {
        Err(DockerError::Command)
    }

    fn docker_root(&self, _: Duration) -> Result<PathBuf, DockerError> {
        Err(DockerError::Command)
    }

    fn inspect_image(&self, _: &str, _: Duration) -> Result<Option<ImageInspection>, DockerError> {
        Err(DockerError::Command)
    }

    fn pull_image(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Err(DockerError::Command)
    }

    fn inspect_network(
        &self,
        _: &str,
        _: Duration,
    ) -> Result<Option<NetworkInspection>, DockerError> {
        Ok(None)
    }

    fn create_network(&self, _: &NetworkRequest, _: Duration) -> Result<String, DockerError> {
        Err(DockerError::Command)
    }

    fn remove_network(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Err(DockerError::Command)
    }

    fn inspect_container(
        &self,
        identity: &str,
        _: Duration,
    ) -> Result<Option<ContainerInspection>, DockerError> {
        Ok(self.containers.get(identity).cloned())
    }

    fn create_container(&self, _: &ContainerRequest, _: Duration) -> Result<String, DockerError> {
        Err(DockerError::Command)
    }

    fn start_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Err(DockerError::Command)
    }

    fn stop_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Err(DockerError::Command)
    }

    fn remove_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Err(DockerError::Command)
    }
}
