use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::fixture::cleanup::{
    CleanupBackend, CleanupCoordinator, CleanupError, CleanupReport, SystemCleanupClock,
};
use crate::fixture::docker::{
    ContainerRequest, DockerApi, DockerCli, NetworkRequest, OwnedLabels, SQL_SERVER_UID,
    SqlServerContainerSpec,
};
use crate::fixture::model::{
    FailureCategory, FailureStage, ResourceKind, ResourceRecord, ResourceState, SanitizedFailure,
};
use crate::fixture::ownership::{
    CommandAclController, OwnershipInspector, ReconcileError, ResourceObservation,
    inspect_member_directory, inspect_owned_directory, process_incarnation_is_alive,
};
use crate::fixture::process::{BoundedProcessRunner, CommandSpec, ProcessRunner};
use crate::fixture::secrets::PrivateFile;

use super::config::{OneReplicaConfig, acquire_one_replica_root_lock};
use super::legacy::detect_legacy_state;
use super::model::{OneReplicaJournal, OneReplicaJournalStore};

const FIXTURE_LABEL: &str = "one-replica";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneReplicaCleanupEvidence {
    pub journal_path: Option<PathBuf>,
    pub report: CleanupReport,
}

pub fn format_cleanup_summary(evidence: &OneReplicaCleanupEvidence) -> String {
    format!(
        "one-replica cleanup complete: removed={}, unresolved={}, journal={}",
        evidence.report.removed.len(),
        evidence.report.unresolved.len(),
        evidence
            .journal_path
            .as_deref()
            .map_or_else(|| "absent".to_owned(), |path| path.display().to_string())
    )
}

#[derive(Debug)]
pub enum OneReplicaCleanupError {
    Config,
    Lock,
    Legacy(String),
    Journal,
    ActiveOwner,
    UnknownOwner,
    Cleanup(CleanupReport),
}

impl fmt::Display for OneReplicaCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config => formatter.write_str("one-replica fixture configuration failed"),
            Self::Lock => formatter.write_str("one-replica fixture root lock failed"),
            Self::Legacy(error) => formatter.write_str(error),
            Self::Journal => formatter.write_str("one-replica ownership journal failed"),
            Self::ActiveOwner => {
                formatter.write_str("one-replica fixture owner process is still alive")
            }
            Self::UnknownOwner => {
                formatter.write_str("one-replica fixture owner identity is unavailable")
            }
            Self::Cleanup(_) => formatter.write_str("one-replica exact cleanup failed"),
        }
    }
}

impl std::error::Error for OneReplicaCleanupError {}

pub fn cleanup_one_replica_fixture(
    root: impl AsRef<Path>,
) -> Result<OneReplicaCleanupEvidence, OneReplicaCleanupError> {
    let runner = BoundedProcessRunner;
    let docker = DockerCli::new(runner);
    cleanup_one_replica_fixture_with(root, &docker, runner)
}

#[doc(hidden)]
pub fn cleanup_one_replica_fixture_with<D, R>(
    root: impl AsRef<Path>,
    docker: &D,
    runner: R,
) -> Result<OneReplicaCleanupEvidence, OneReplicaCleanupError>
where
    D: DockerApi,
    R: ProcessRunner + Copy,
{
    let config = OneReplicaConfig::for_test_fixture(root.as_ref())
        .map_err(|_| OneReplicaCleanupError::Config)?;
    let _lock =
        acquire_one_replica_root_lock(config.root()).map_err(|_| OneReplicaCleanupError::Lock)?;
    detect_legacy_state(
        config.root(),
        docker,
        config.fixture().deadlines().docker_command,
    )
    .map_err(|error| OneReplicaCleanupError::Legacy(error.to_string()))?;
    if !config
        .root()
        .try_exists()
        .map_err(|_| OneReplicaCleanupError::Journal)?
    {
        return Ok(OneReplicaCleanupEvidence {
            journal_path: None,
            report: empty_report(),
        });
    }
    let store = OneReplicaJournalStore::initialize(config.root())
        .map_err(|_| OneReplicaCleanupError::Journal)?;
    let Some(mut journal) = store.load().map_err(|_| OneReplicaCleanupError::Journal)? else {
        return Ok(OneReplicaCleanupEvidence {
            journal_path: Some(store.path().to_path_buf()),
            report: empty_report(),
        });
    };
    if journal.blocked_owner_unknown {
        return Err(OneReplicaCleanupError::UnknownOwner);
    }
    if let Some(owner) = journal.blocked_owner {
        if process_incarnation_is_alive(owner).map_err(|_| OneReplicaCleanupError::Journal)? {
            return Err(OneReplicaCleanupError::ActiveOwner);
        }
        journal.blocked_owner = None;
        store
            .save(&journal)
            .map_err(|_| OneReplicaCleanupError::Journal)?;
    }
    let artifacts = CleanupArtifacts::from_journal(&config, &journal);
    let image_id = journal.run.image_id.clone();
    let backend = OneReplicaCleanupBackend {
        root: store.root(),
        docker,
        runner,
        image_id: &image_id,
        network: artifacts.network.as_ref(),
        container_name: artifacts.container_name.as_deref(),
        container: artifacts.container.as_ref(),
    };
    let coordinator = CleanupCoordinator::new(
        SystemCleanupClock::default(),
        config.fixture().deadlines().cleanup,
    );
    let report = coordinator.cleanup_shared(&store, &mut journal, &backend);
    let evidence = OneReplicaCleanupEvidence {
        journal_path: Some(store.path().to_path_buf()),
        report: report.clone(),
    };
    if report.succeeded() {
        Ok(evidence)
    } else {
        Err(OneReplicaCleanupError::Cleanup(report))
    }
}

fn empty_report() -> CleanupReport {
    CleanupReport {
        removed: Vec::new(),
        unresolved: Vec::new(),
        errors: Vec::new(),
    }
}

pub(crate) struct CleanupArtifacts {
    pub network: Option<NetworkRequest>,
    pub container_name: Option<String>,
    pub container: Option<ContainerRequest>,
}

impl CleanupArtifacts {
    pub(crate) fn from_journal(config: &OneReplicaConfig, journal: &OneReplicaJournal) -> Self {
        let run = &journal.run;
        let has_network = journal.resources.iter().any(|resource| {
            resource.kind == ResourceKind::Network && resource.state != ResourceState::Removed
        });
        let has_container = journal.resources.iter().any(|resource| {
            resource.kind == ResourceKind::Container && resource.state != ResourceState::Removed
        });
        let network = has_network.then(|| NetworkRequest {
            name: run.member.network_name.clone(),
            labels: OwnedLabels::network_for_fixture(FIXTURE_LABEL, run.run_id.clone()),
        });
        let container_name = has_container.then(|| run.member.container_name.clone());
        let container = if has_container {
            PrivateFile::inspect(config.root().join("credentials/sa-password"))
                .and_then(|file| file.read_secret())
                .ok()
                .and_then(|password| {
                    ContainerRequest::sql_server(
                        SqlServerContainerSpec {
                            name: run.member.container_name.clone(),
                            hostname: run.member.server_name.clone(),
                            network_name: run.member.network_name.clone(),
                            data_directory: run.member.data_directory.clone(),
                            environment_file: run.member.environment_file.clone(),
                            sa_password: password,
                        },
                        OwnedLabels::container_for_fixture(FIXTURE_LABEL, run.run_id.clone(), 1),
                        config.fixture().resources(),
                        config.fixture().authorization().sql_server_environment(),
                    )
                    .ok()
                })
        } else {
            None
        };
        Self {
            network,
            container_name,
            container,
        }
    }
}

pub(crate) struct OneReplicaCleanupBackend<'a, D, R> {
    pub root: &'a Path,
    pub docker: &'a D,
    pub runner: R,
    pub image_id: &'a str,
    pub network: Option<&'a NetworkRequest>,
    pub container_name: Option<&'a str>,
    pub container: Option<&'a ContainerRequest>,
}

impl<D, R> OneReplicaCleanupBackend<'_, D, R>
where
    D: DockerApi,
    R: ProcessRunner + Copy,
{
    fn inspect_path(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<ResourceObservation, ReconcileError> {
        let path = resource
            .path
            .as_ref()
            .ok_or(ReconcileError::OwnershipMismatch)?;
        match fs::symlink_metadata(path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ResourceObservation::Absent);
            }
            Err(_) => return Err(ReconcileError::Io),
        }
        let binding = match resource.kind {
            ResourceKind::DataDirectory => {
                inspect_member_directory(
                    self.root,
                    path,
                    unsafe { libc::geteuid() },
                    SQL_SERVER_UID,
                    remaining.min(Duration::from_secs(30)),
                    &CommandAclController::new(self.runner),
                )
                .map_err(|_| ReconcileError::OwnershipMismatch)?
                .binding
            }
            ResourceKind::Directory => {
                inspect_owned_directory(self.root, path)
                    .map_err(|_| ReconcileError::OwnershipMismatch)?
                    .binding
            }
            ResourceKind::SecretFile => PrivateFile::inspect_owned(path)
                .map_err(|_| ReconcileError::OwnershipMismatch)?
                .binding()
                .clone(),
            _ => return Err(ReconcileError::OwnershipMismatch),
        };
        Ok(ResourceObservation::Owned {
            binding,
            foreign_attachments: Vec::new(),
        })
    }

    fn operation_error(resource: &ResourceRecord, category: FailureCategory) -> CleanupError {
        CleanupError {
            resource: resource.logical_name.clone(),
            failure: SanitizedFailure::new(FailureStage::Cleanup, category),
        }
    }

    fn ownership_error(resource: &ResourceRecord) -> CleanupError {
        Self::operation_error(resource, FailureCategory::OwnershipMismatch)
    }
}

impl<D, R> OwnershipInspector for OneReplicaCleanupBackend<'_, D, R>
where
    D: DockerApi,
    R: ProcessRunner + Copy,
{
    fn inspect(&self, resource: &ResourceRecord) -> Result<ResourceObservation, ReconcileError> {
        CleanupBackend::inspect_cleanup(self, resource, Duration::from_secs(30))
    }
}

impl<D, R> CleanupBackend for OneReplicaCleanupBackend<'_, D, R>
where
    D: DockerApi,
    R: ProcessRunner + Copy,
{
    fn inspect_cleanup(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<ResourceObservation, ReconcileError> {
        match resource.kind {
            ResourceKind::Network => {
                let network = self.network.ok_or(ReconcileError::OwnershipMismatch)?;
                let identity = resource
                    .binding
                    .as_ref()
                    .map_or(network.name.as_str(), |binding| {
                        binding.immutable_id.as_str()
                    });
                let Some(inspection) = self
                    .docker
                    .inspect_network(identity, remaining.min(Duration::from_secs(30)))
                    .map_err(|_| ReconcileError::Io)?
                else {
                    return Ok(ResourceObservation::Absent);
                };
                let binding = network
                    .resource_binding(&inspection)
                    .map_err(|_| ReconcileError::OwnershipMismatch)?;
                if resource
                    .binding
                    .as_ref()
                    .is_some_and(|expected| expected != &binding)
                {
                    return Ok(ResourceObservation::Foreign);
                }
                Ok(ResourceObservation::Owned {
                    binding,
                    foreign_attachments: inspection.attached_container_ids,
                })
            }
            ResourceKind::Container => {
                let container_name = self
                    .container_name
                    .ok_or(ReconcileError::OwnershipMismatch)?;
                let identity = resource
                    .binding
                    .as_ref()
                    .map_or(container_name, |binding| binding.immutable_id.as_str());
                let Some(inspection) = self
                    .docker
                    .inspect_container(identity, remaining.min(Duration::from_secs(30)))
                    .map_err(|_| ReconcileError::Io)?
                else {
                    return Ok(ResourceObservation::Absent);
                };
                let container = self.container.ok_or(ReconcileError::OwnershipMismatch)?;
                let binding = container
                    .resource_binding(self.image_id, &inspection)
                    .map_err(|_| ReconcileError::OwnershipMismatch)?;
                if resource
                    .binding
                    .as_ref()
                    .is_some_and(|expected| expected != &binding)
                {
                    return Ok(ResourceObservation::Foreign);
                }
                Ok(ResourceObservation::Owned {
                    binding,
                    foreign_attachments: Vec::new(),
                })
            }
            ResourceKind::DataDirectory | ResourceKind::Directory | ResourceKind::SecretFile => {
                self.inspect_path(resource, remaining)
            }
            ResourceKind::AvailabilityGroup | ResourceKind::Database => {
                Err(ReconcileError::OwnershipMismatch)
            }
        }
    }

    fn remove_container(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        let identity = resource
            .binding
            .as_ref()
            .ok_or_else(|| Self::ownership_error(resource))?
            .immutable_id
            .as_str();
        if let Some(inspection) = self
            .docker
            .inspect_container(identity, remaining.min(Duration::from_secs(30)))
            .map_err(|_| Self::operation_error(resource, FailureCategory::ContainerRemoval))?
        {
            self.container
                .ok_or_else(|| Self::ownership_error(resource))?
                .verify_inspection(&inspection, self.image_id, inspection.running)
                .map_err(|_| Self::ownership_error(resource))?;
            if inspection.running {
                self.docker
                    .stop_container(identity, remaining.min(Duration::from_secs(30)))
                    .map_err(|_| {
                        Self::operation_error(resource, FailureCategory::ContainerRemoval)
                    })?;
            }
            self.docker
                .remove_container(identity, remaining.min(Duration::from_secs(30)))
                .map_err(|_| Self::operation_error(resource, FailureCategory::ContainerRemoval))?;
        }
        Ok(())
    }

    fn remove_network(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        let identity = resource
            .binding
            .as_ref()
            .ok_or_else(|| Self::ownership_error(resource))?
            .immutable_id
            .as_str();
        self.docker
            .remove_network(identity, remaining.min(Duration::from_secs(30)))
            .map_err(|_| Self::operation_error(resource, FailureCategory::NetworkRemoval))
    }

    fn remove_path(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        let expected = resource
            .binding
            .as_ref()
            .ok_or_else(|| Self::ownership_error(resource))?;
        let ResourceObservation::Owned { binding, .. } = self
            .inspect_path(resource, remaining)
            .map_err(|_| Self::ownership_error(resource))?
        else {
            return Err(Self::ownership_error(resource));
        };
        if &binding != expected {
            return Err(Self::ownership_error(resource));
        }
        let path = resource
            .path
            .as_ref()
            .ok_or_else(|| Self::ownership_error(resource))?;
        match resource.kind {
            ResourceKind::SecretFile => fs::remove_file(path),
            ResourceKind::Directory => remove_tree_before(path, Instant::now() + remaining),
            ResourceKind::DataDirectory => {
                self.runner
                    .run(
                        &CommandSpec::new(
                            "sudo",
                            "restore host cleanup ACL",
                            remaining.min(Duration::from_secs(30)),
                        )
                        .args([
                            "-n",
                            "setfacl",
                            "--recursive",
                            "--physical",
                            "--modify",
                            &format!("u:{}:rwx,m:rwx", unsafe { libc::geteuid() }),
                        ])
                        .arg(path),
                    )
                    .map_err(|_| Self::operation_error(resource, FailureCategory::PathRemoval))?;
                remove_tree_before(path, Instant::now() + remaining)
            }
            _ => return Err(Self::ownership_error(resource)),
        }
        .map_err(|_| Self::operation_error(resource, FailureCategory::PathRemoval))
    }
}

fn remove_tree_before(path: &Path, deadline: Instant) -> std::io::Result<()> {
    fn remove(path: &Path, deadline: Instant) -> std::io::Result<()> {
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "cleanup deadline exceeded",
            ));
        }
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return fs::remove_file(path);
        }
        for entry in fs::read_dir(path)? {
            remove(&entry?.path(), deadline)?;
        }
        fs::remove_dir(path)
    }
    match remove(path, deadline) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::os::unix::fs::PermissionsExt;

    use crate::fixture::cleanup::{CleanupClock, CleanupCompletion};
    use crate::fixture::docker::{ContainerInspection, ContainerMount, ContainerPort};
    use crate::fixture::model::{CombinedFixtureError, ResourceBinding, ResourceState, RunState};
    use crate::fixture::secrets::PrivateFile;

    use super::super::model::{OneReplicaMember, OneReplicaRun};
    use super::*;

    struct FakeBackend {
        foreign: bool,
        attachments: bool,
        removed: Cell<bool>,
    }

    impl OwnershipInspector for FakeBackend {
        fn inspect(
            &self,
            resource: &ResourceRecord,
        ) -> Result<ResourceObservation, ReconcileError> {
            self.inspect_cleanup(resource, Duration::from_secs(30))
        }
    }

    impl CleanupBackend for FakeBackend {
        fn inspect_cleanup(
            &self,
            resource: &ResourceRecord,
            _: Duration,
        ) -> Result<ResourceObservation, ReconcileError> {
            if self.removed.get() {
                Ok(ResourceObservation::Absent)
            } else if self.foreign {
                Ok(ResourceObservation::Foreign)
            } else {
                Ok(ResourceObservation::Owned {
                    binding: resource.binding.clone().unwrap_or(ResourceBinding {
                        immutable_id: "late-container-id".to_owned(),
                        attributes_sha256: "c".repeat(64),
                    }),
                    foreign_attachments: if self.attachments {
                        vec!["foreign-container".to_owned()]
                    } else {
                        Vec::new()
                    },
                })
            }
        }

        fn remove_container(&self, _: &ResourceRecord, _: Duration) -> Result<(), CleanupError> {
            self.removed.set(true);
            Ok(())
        }

        fn remove_network(&self, _: &ResourceRecord, _: Duration) -> Result<(), CleanupError> {
            unreachable!()
        }

        fn remove_path(&self, _: &ResourceRecord, _: Duration) -> Result<(), CleanupError> {
            unreachable!()
        }
    }

    #[derive(Clone)]
    struct AdvancingClock {
        now: Cell<u64>,
    }

    impl CleanupClock for AdvancingClock {
        fn now(&self) -> Duration {
            let current = self.now.get();
            self.now.set(current + 2);
            Duration::from_secs(current)
        }
    }

    fn journal(root: &Path) -> (OneReplicaJournalStore, OneReplicaJournal) {
        let store = OneReplicaJournalStore::initialize(root).unwrap();
        let run = OneReplicaRun {
            run_id: "test-run".to_owned(),
            resource_uid: "test-resource".to_owned(),
            image_id: "test-image".to_owned(),
            member: OneReplicaMember {
                ordinal: 1,
                server_name: "sql-one".to_owned(),
                container_name: "container-one".to_owned(),
                network_name: "network-one".to_owned(),
                data_directory: root.join("member-1"),
                environment_file: root.join("credentials/container.env"),
            },
        };
        let mut journal = store.create(run).unwrap();
        journal.resources.push(ResourceRecord {
            kind: ResourceKind::Container,
            logical_name: "container-1".to_owned(),
            path: None,
            intent: None,
            binding: Some(ResourceBinding {
                immutable_id: "container-id".to_owned(),
                attributes_sha256: "a".repeat(64),
            }),
            state: ResourceState::Bound,
        });
        store.save(&journal).unwrap();
        (store, journal)
    }

    #[test]
    fn reconstructed_container_request_has_exact_one_replica_identity() {
        let root = tempfile::tempdir().unwrap();
        let fixture_root = root.path().join("fixture");
        let (store, mut journal) = journal(&fixture_root);
        let credentials = fixture_root.join("credentials");
        fs::create_dir(&credentials).unwrap();
        fs::set_permissions(&credentials, fs::Permissions::from_mode(0o700)).unwrap();
        PrivateFile::create_text(credentials.join("sa-password"), "ValidPassword1!").unwrap();
        journal.resources[0].intent = Some(ResourceBinding {
            immutable_id: journal.run.member.container_name.clone(),
            attributes_sha256: "d".repeat(64),
        });
        store.save(&journal).unwrap();
        let config = OneReplicaConfig::for_test_fixture(&fixture_root).unwrap();
        let artifacts = CleanupArtifacts::from_journal(&config, &journal);
        let request = artifacts.container.unwrap();
        assert_eq!(
            request.labels.as_map()["io.kuberic.mssql.fixture"],
            "one-replica"
        );
        assert_eq!(request.data_directory, journal.run.member.data_directory);
        assert_eq!(
            request.environment_keys().collect::<Vec<_>>(),
            [
                "ACCEPT_EULA",
                "MSSQL_PID",
                "MSSQL_SA_PASSWORD",
                "MSSQL_ENABLE_HADR",
                "MSSQL_MEMORY_LIMIT_MB",
            ]
        );
        let inspection = ContainerInspection {
            id: "container-id".to_owned(),
            name: request.name.clone(),
            hostname: request.hostname.clone(),
            image_id: journal.run.image_id.clone(),
            labels: request.labels.as_map(),
            environment: request
                .environment_file_contents()
                .lines()
                .map(str::to_owned)
                .collect(),
            user: "mssql".to_owned(),
            running: true,
            restart_policy: "no".to_owned(),
            network_mode: request.network_name.clone(),
            network_names: vec![request.network_name.clone()],
            mounts: vec![ContainerMount {
                source: request.data_directory.clone(),
                destination: PathBuf::from("/var/opt/mssql"),
                read_only: false,
            }],
            ports: vec![ContainerPort {
                container_port: 1433,
                host_ip: "127.0.0.1".to_owned(),
                host_port: 14330,
            }],
            limits: request.limits,
        };
        assert!(
            request
                .resource_binding(&journal.run.image_id, &inspection)
                .is_ok()
        );
        let mut replaced = inspection;
        replaced.hostname = "replacement".to_owned();
        assert!(
            request
                .resource_binding(&journal.run.image_id, &replaced)
                .is_err()
        );
    }

    #[test]
    fn shared_cleanup_removes_owned_one_replica_resources() {
        let root = tempfile::tempdir().unwrap();
        let (store, mut journal) = journal(&root.path().join("fixture"));
        let backend = FakeBackend {
            foreign: false,
            attachments: false,
            removed: Cell::new(false),
        };
        let report = CleanupCoordinator::default().cleanup_shared(&store, &mut journal, &backend);
        assert!(report.succeeded());
        assert_eq!(journal.state, RunState::Removed);
        assert_eq!(journal.resources[0].state, ResourceState::Removed);
    }

    #[test]
    fn replacement_resource_is_blocked_without_removal() {
        let root = tempfile::tempdir().unwrap();
        let (store, mut journal) = journal(&root.path().join("fixture"));
        let backend = FakeBackend {
            foreign: true,
            attachments: false,
            removed: Cell::new(false),
        };
        let report = CleanupCoordinator::default().cleanup_shared(&store, &mut journal, &backend);
        assert!(!report.succeeded());
        assert!(!backend.removed.get());
        assert_eq!(journal.state, RunState::Blocked);
        assert_eq!(journal.resources[0].state, ResourceState::Blocked);
    }

    #[test]
    fn exhausted_cleanup_deadline_blocks_without_removal() {
        let root = tempfile::tempdir().unwrap();
        let (store, mut journal) = journal(&root.path().join("fixture"));
        let backend = FakeBackend {
            foreign: false,
            attachments: false,
            removed: Cell::new(false),
        };
        let coordinator =
            CleanupCoordinator::new(AdvancingClock { now: Cell::new(0) }, Duration::from_secs(1));
        let report = coordinator.cleanup_shared(&store, &mut journal, &backend);
        assert!(!report.succeeded());
        assert!(!backend.removed.get());
        assert_eq!(journal.resources[0].state, ResourceState::Blocked);
    }

    #[test]
    fn panic_completion_still_cleans_owned_resources() {
        let root = tempfile::tempdir().unwrap();
        let (store, mut journal) = journal(&root.path().join("fixture"));
        let backend = FakeBackend {
            foreign: false,
            attachments: false,
            removed: Cell::new(false),
        };
        let result: Result<(), CombinedFixtureError> = CleanupCoordinator::default()
            .coordinate_shared(
                CleanupCompletion::CaughtPanic(SanitizedFailure::new(
                    FailureStage::Test,
                    FailureCategory::OwnershipMismatch,
                )),
                &store,
                &mut journal,
                &backend,
            );
        assert!(result.is_err());
        assert!(backend.removed.get());
        assert_eq!(journal.state, RunState::Removed);
    }

    #[test]
    fn foreign_attachments_block_removal() {
        let root = tempfile::tempdir().unwrap();
        let (store, mut journal) = journal(&root.path().join("fixture"));
        let backend = FakeBackend {
            foreign: false,
            attachments: true,
            removed: Cell::new(false),
        };
        let report = CleanupCoordinator::default().cleanup_shared(&store, &mut journal, &backend);
        assert!(!report.succeeded());
        assert!(!backend.removed.get());
        assert_eq!(journal.resources[0].state, ResourceState::Blocked);
    }

    #[test]
    fn dispatched_late_create_is_bound_then_removed() {
        let root = tempfile::tempdir().unwrap();
        let (store, mut journal) = journal(&root.path().join("fixture"));
        journal.resources[0].state = ResourceState::Dispatched;
        journal.resources[0].binding = None;
        store.save(&journal).unwrap();
        let backend = FakeBackend {
            foreign: false,
            attachments: false,
            removed: Cell::new(false),
        };
        let report = CleanupCoordinator::default().cleanup_shared(&store, &mut journal, &backend);
        assert!(report.succeeded());
        assert!(backend.removed.get());
        assert_eq!(journal.resources[0].state, ResourceState::Removed);
    }

    #[test]
    fn handled_signal_and_failed_result_still_clean() {
        for completion in [
            CleanupCompletion::HandledSignal {
                signal: crate::fixture::cleanup::HandledCancellationSignal::Terminate,
                failure: SanitizedFailure::new(
                    FailureStage::Test,
                    FailureCategory::DeadlineExceeded,
                ),
            },
            CleanupCompletion::Result(Err(SanitizedFailure::new(
                FailureStage::Test,
                FailureCategory::SqlUnavailable,
            ))),
        ] {
            let root = tempfile::tempdir().unwrap();
            let (store, mut journal) = journal(&root.path().join("fixture"));
            let backend = FakeBackend {
                foreign: false,
                attachments: false,
                removed: Cell::new(false),
            };
            let result: Result<(), CombinedFixtureError> = CleanupCoordinator::default()
                .coordinate_shared(completion.clone(), &store, &mut journal, &backend);
            assert!(result.is_err());
            assert!(backend.removed.get());
            assert_eq!(journal.state, RunState::Removed);
        }
    }
}
