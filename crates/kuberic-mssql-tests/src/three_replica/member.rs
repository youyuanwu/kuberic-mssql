use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::time::sleep;

use kuberic_mssql::runtime_config::ObserverConfig;

use super::admin::{
    AdminDeadlines, AdminEndpoint, AdminError, AdminSession, LoginFiles, MemberReadinessEvidence,
};
use super::availability_group::{
    AvailabilityGroupError, FrozenMemberVerifier, ProvisionContext, ProvisionedAvailabilityGroup,
    provision,
};
use super::cleanup::{
    CLEANUP_BUDGET, CleanupBackend, CleanupClock, CleanupCoordinator, CleanupError, CleanupReport,
    OperationBudget, SystemCleanupClock, combine_with_cleanup,
};
use super::config::FixtureConfig;
use super::data::{DataContext, DataError, MarkerEvidence, prove_replicated_marker};
use super::docker::{
    ContainerInspection, ContainerRequest, DockerApi, DockerCli, DockerError, NetworkRequest,
    OwnedLabels, SQL_SERVER_UID, SqlServerContainerSpec,
};
use super::model::{
    CombinedFixtureError, FailureCategory, FailureStage, KubericMember, OwnershipJournal,
    ResourceBinding, ResourceKind, ResourceRecord, ResourceState, RunState, SanitizedFailure,
    SqlMember, SqlMemberIncarnation, TopologyRun,
};
use super::ownership::{
    CommandAclController, JournalStore, OwnershipInspector, PrivateDirectoryBinding,
    ReconcileError, ResourceObservation, RootLock, acquire_root_lock,
    create_private_owned_directory, inspect_member_directory, inspect_owned_directory,
    prepare_member_directory,
};
use super::preflight::{CommandAclProbe, LocalHostProbe, run_preflight};
use super::process::{BoundedProcessRunner, CommandSpec, ProcessRunner};
use super::secrets::{CredentialFiles, PrivateFile, SecretError, SecretValue};
use super::tls::{TlsAssetRecorder, TlsAssets, TlsError};

const READINESS_RETRY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyMember {
    pub ordinal: u8,
    pub server_name: String,
    pub container_id: String,
    pub host_port: u16,
    pub sql_start_unix_millis: i64,
    pub observer_config: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupEvidence {
    pub removed_container_ids: Vec<String>,
    pub removed_network_id: String,
    pub journal_path: PathBuf,
}

pub struct LaunchedMembers {
    context: LaunchContext,
    pub run: TopologyRun,
    pub network_id: String,
    pub members: [ReadyMember; 3],
}

impl LaunchedMembers {
    pub fn journal(&self) -> &OwnershipJournal {
        &self.context.journal
    }

    pub fn cleanup(mut self) -> Result<CleanupEvidence, CombinedFixtureError> {
        self.context.cleanup_exact()
    }

    pub async fn provision_native_topology(&mut self) -> Result<NativeDataProof, NativePhaseError> {
        let credentials = self
            .context
            .credentials
            .as_ref()
            .ok_or(NativePhaseError::Unavailable)?;
        let tls = self
            .context
            .tls
            .as_ref()
            .ok_or(NativePhaseError::Unavailable)?;
        let frozen_member_verifier = LaunchFrozenMemberVerifier {
            docker: &self.context.docker,
            containers: &self.context.containers,
            image_id: &self.context.image_id,
        };
        let provisioned = provision(ProvisionContext {
            run: &self.run,
            members: &self.members,
            credentials,
            tls,
            deadlines: self.context.config.deadlines(),
            complete_deadline: self.context.deadline,
            store: &self.context.store,
            journal: &mut self.context.journal,
            runner: &self.context.runner,
            frozen_member_verifier: &frozen_member_verifier,
        })
        .await
        .map_err(NativePhaseError::AvailabilityGroup)?;
        let marker = prove_replicated_marker(
            &DataContext {
                run: &self.run,
                members: &self.members,
                credentials,
                tls,
                deadlines: self.context.config.deadlines(),
                complete_deadline: self.context.deadline,
            },
            &provisioned.evidence,
        )
        .await
        .map_err(NativePhaseError::Data)?;
        Ok(NativeDataProof {
            topology: provisioned,
            marker,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeDataProof {
    pub topology: ProvisionedAvailabilityGroup,
    pub marker: MarkerEvidence,
}

#[derive(Debug)]
pub enum NativePhaseError {
    Unavailable,
    AvailabilityGroup(AvailabilityGroupError),
    Data(DataError),
}

impl fmt::Display for NativePhaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "native phase requires the live member launch context",
            Self::AvailabilityGroup(_) => "native availability-group phase failed",
            Self::Data(_) => "native replicated-data phase failed",
        })
    }
}

impl std::error::Error for NativePhaseError {}

pub async fn launch_three_members(
    config: FixtureConfig,
) -> Result<LaunchedMembers, CombinedFixtureError> {
    let mut context = LaunchContext::prepare(config)
        .map_err(|error| CombinedFixtureError::new(error.sanitized(), Vec::new()))?;
    match context.launch().await {
        Ok((run, network_id, members)) => Ok(LaunchedMembers {
            context,
            run,
            network_id,
            members,
        }),
        Err(primary) => {
            let report = context.cleanup_report();
            combine_with_cleanup::<()>(Err(primary.sanitized()), &report).map(|_| unreachable!())
        }
    }
}

struct OwnedNetwork {
    request: NetworkRequest,
    id: Option<String>,
}

struct OwnedContainer {
    request: ContainerRequest,
    id: Option<String>,
    frozen_running_inspection: Option<ContainerInspection>,
}

struct LaunchFrozenMemberVerifier<'a> {
    docker: &'a DockerCli<BoundedProcessRunner>,
    containers: &'a [OwnedContainer],
    image_id: &'a str,
}

impl FrozenMemberVerifier for LaunchFrozenMemberVerifier<'_> {
    fn verify(
        &self,
        parent_deadline: Instant,
        operation_timeout: Duration,
    ) -> Result<(), AvailabilityGroupError> {
        if self.containers.len() != 3 {
            return Err(AvailabilityGroupError::FrozenMember);
        }
        for container in self.containers {
            let remaining = parent_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AvailabilityGroupError::Deadline);
            }
            let id = container
                .id
                .as_deref()
                .ok_or(AvailabilityGroupError::FrozenMember)?;
            let current = self
                .docker
                .inspect_container(id, operation_timeout.min(remaining))
                .map_err(|_| AvailabilityGroupError::FrozenMember)?
                .ok_or(AvailabilityGroupError::FrozenMember)?;
            container
                .request
                .verify_frozen_running_inspection(
                    &current,
                    container
                        .frozen_running_inspection
                        .as_ref()
                        .ok_or(AvailabilityGroupError::FrozenMember)?,
                    self.image_id,
                )
                .map_err(|_| AvailabilityGroupError::FrozenMember)?;
            if Instant::now() >= parent_deadline {
                return Err(AvailabilityGroupError::Deadline);
            }
        }
        Ok(())
    }
}

struct LaunchContext {
    config: FixtureConfig,
    _lock: RootLock,
    store: JournalStore,
    journal: OwnershipJournal,
    docker: DockerCli<BoundedProcessRunner>,
    runner: BoundedProcessRunner,
    image_id: String,
    deadline: Instant,
    private_files: Vec<PrivateFile>,
    credentials: Option<CredentialFiles>,
    tls: Option<TlsAssets>,
    network: Option<OwnedNetwork>,
    containers: Vec<OwnedContainer>,
}

impl LaunchContext {
    fn prepare(config: FixtureConfig) -> Result<Self, NativeLaunchError> {
        let lock = acquire_root_lock(config.root()).map_err(|_| NativeLaunchError::RootLock)?;
        let store =
            JournalStore::initialize(config.root()).map_err(|_| NativeLaunchError::Journal)?;
        if let Some(existing) = store.load().map_err(|_| NativeLaunchError::Journal)?
            && existing.state != RunState::Removed
        {
            return Err(NativeLaunchError::PriorRunUnresolved);
        }
        let runner = BoundedProcessRunner;
        let docker = DockerCli::new(runner);
        let preflight = run_preflight(
            &config,
            &LocalHostProbe,
            &CommandAclProbe::new(runner),
            &docker,
        )
        .map_err(|_| NativeLaunchError::Preflight)?;
        let run = topology_run(store.root())?;
        let journal = store.create(run).map_err(|_| NativeLaunchError::Journal)?;
        Ok(Self {
            deadline: Instant::now() + config.deadlines().complete_run,
            config,
            _lock: lock,
            store,
            journal,
            docker,
            runner,
            image_id: preflight.image.id,
            private_files: Vec::new(),
            credentials: None,
            tls: None,
            network: None,
            containers: Vec::new(),
        })
    }

    async fn launch(
        &mut self,
    ) -> Result<(TopologyRun, String, [ReadyMember; 3]), NativeLaunchError> {
        let run = self.journal.run.clone();
        self.prepare_member_directories(&run)?;
        let credentials = self.generate_credentials(&run)?;
        let hostnames = std::array::from_fn(|index| run.members[index].server_name.as_str());
        let data_directories =
            std::array::from_fn(|index| run.members[index].data_directory.as_path());
        let tls_timeout = self.limit(self.config.deadlines().tls_helper)?;
        let root = self.store.root().to_path_buf();
        let runner = self.runner;
        let mut recorder = LaunchAssetRecorder {
            store: &self.store,
            journal: &mut self.journal,
            private_files: &mut self.private_files,
        };
        let tls = TlsAssets::generate_recorded(
            &root,
            hostnames,
            data_directories,
            tls_timeout,
            &runner,
            &mut recorder,
        )
        .map_err(NativeLaunchError::Tls)?;
        self.write_sql_configuration(&run)?;
        let network_name = format!("km-three-{}", run.run_id);
        let network_request = NetworkRequest {
            name: network_name.clone(),
            labels: OwnedLabels::network(&run.run_id),
        };
        let network_index = self.record_intent(
            ResourceKind::Network,
            "docker-network",
            None,
            Some(network_request.intent_binding()),
        )?;
        self.network = Some(OwnedNetwork {
            request: network_request,
            id: None,
        });
        self.store
            .mark_dispatched(&mut self.journal, network_index)
            .map_err(|_| NativeLaunchError::Journal)?;
        let network_inspection = reconcile_network_create(
            &self.docker,
            self.network.as_ref().expect("network intent"),
            self.limit(self.config.deadlines().docker_command)?,
        )?;
        let network_id = network_inspection.id.clone();
        self.network.as_mut().expect("network intent").id = Some(network_id.clone());
        self.store
            .bind(
                &mut self.journal,
                network_index,
                network_resource_binding(
                    &self.network.as_ref().expect("network intent").request,
                    &network_inspection,
                )?,
            )
            .map_err(|_| NativeLaunchError::Journal)?;

        let sa_password = credentials
            .sa_password
            .read_secret()
            .map_err(NativeLaunchError::Secret)?;
        let environment_file = self.store.root().join("credentials/container.env");
        for index in 0..3 {
            self.config
                .authorization()
                .revalidate()
                .map_err(|_| NativeLaunchError::AuthorizationChanged)?;
            let member = &run.members[index];
            let request = ContainerRequest::sql_server(
                SqlServerContainerSpec {
                    name: member.container_name.clone(),
                    hostname: member.server_name.clone(),
                    network_name: network_name.clone(),
                    data_directory: member.data_directory.clone(),
                    environment_file: environment_file.clone(),
                    sa_password: sa_password.clone(),
                },
                OwnedLabels::container(&run.run_id, member.ordinal),
                self.config.resources(),
                self.config.authorization().sql_server_environment(),
            )
            .map_err(NativeLaunchError::Docker)?;
            if index == 0 {
                let file = self.create_recorded_text_file(
                    "container-environment",
                    &environment_file,
                    request.environment_file_contents(),
                    false,
                )?;
                drop(file);
            }
            let logical_name = format!("container-{}", member.ordinal);
            let record_index = self.record_intent(
                ResourceKind::Container,
                &logical_name,
                None,
                Some(request.intent_binding(&self.image_id)),
            )?;
            self.containers.push(OwnedContainer {
                request,
                id: None,
                frozen_running_inspection: None,
            });
            self.store
                .mark_dispatched(&mut self.journal, record_index)
                .map_err(|_| NativeLaunchError::Journal)?;
            let inspection = reconcile_container_create(
                &self.docker,
                &self.containers[index],
                &self.image_id,
                self.limit(self.config.deadlines().docker_command)?,
            )?;
            let id = inspection.id.clone();
            self.containers[index].id = Some(id.clone());
            self.store
                .bind(
                    &mut self.journal,
                    record_index,
                    container_resource_binding(
                        &self.containers[index].request,
                        &self.image_id,
                        &inspection,
                    )?,
                )
                .map_err(|_| NativeLaunchError::Journal)?;
        }
        self.verify_network_attachments(false)?;
        for index in 0..self.containers.len() {
            let id = self.containers[index]
                .id
                .clone()
                .ok_or(NativeLaunchError::Ownership)?;
            self.docker
                .start_container(&id, self.limit(self.config.deadlines().docker_command)?)
                .map_err(NativeLaunchError::Docker)?;
            let inspection = self.inspect_container(&id)?;
            self.containers[index]
                .request
                .verify_inspection(&inspection, &self.image_id, true)
                .map_err(NativeLaunchError::Docker)?;
            self.containers[index].frozen_running_inspection = Some(inspection);
        }
        self.verify_network_attachments(true)?;

        let mut ready = Vec::with_capacity(3);
        let mut incarnations = Vec::with_capacity(3);
        for index in 0..3 {
            let id = self.containers[index]
                .id
                .as_deref()
                .ok_or(NativeLaunchError::Ownership)?;
            let inspection = self.inspect_container(id)?;
            self.containers[index]
                .request
                .verify_frozen_running_inspection(
                    &inspection,
                    self.containers[index]
                        .frozen_running_inspection
                        .as_ref()
                        .ok_or(NativeLaunchError::Ownership)?,
                    &self.image_id,
                )
                .map_err(NativeLaunchError::Docker)?;
            let port = inspection
                .ports
                .first()
                .map(|port| port.host_port)
                .ok_or(NativeLaunchError::Ownership)?;
            let evidence = self
                .initialize_member(index, port, &run, &credentials, &tls)
                .await?;
            evidence
                .verify(&run.members[index].server_name)
                .map_err(NativeLaunchError::Admin)?;
            let incarnation = SqlMemberIncarnation {
                ordinal: run.members[index].ordinal,
                server_name: run.members[index].server_name.clone(),
                container_id: inspection.id.clone(),
                sql_start_unix_millis: evidence.sql_start_unix_millis,
            };
            let observer_config =
                self.write_observer_config(index, port, &run, &credentials, &tls, &incarnation)?;
            ready.push(ReadyMember {
                ordinal: run.members[index].ordinal,
                server_name: run.members[index].server_name.clone(),
                container_id: inspection.id,
                host_port: port,
                sql_start_unix_millis: evidence.sql_start_unix_millis,
                observer_config,
            });
            incarnations.push(incarnation);
        }
        self.journal.sql_member_incarnations = Some(
            incarnations
                .try_into()
                .map_err(|_| NativeLaunchError::Readiness)?,
        );
        self.journal.state = RunState::Ready;
        self.store
            .save(&self.journal)
            .map_err(|_| NativeLaunchError::Journal)?;
        self.credentials = Some(credentials);
        self.tls = Some(tls);
        Ok((
            run,
            network_id,
            ready.try_into().map_err(|_| NativeLaunchError::Readiness)?,
        ))
    }

    fn prepare_member_directories(&mut self, run: &TopologyRun) -> Result<(), NativeLaunchError> {
        let host_uid = unsafe { libc::geteuid() };
        let acl = CommandAclController::new(self.runner);
        for member in &run.members {
            let index = self.record_intent(
                ResourceKind::DataDirectory,
                &format!("member-data-{}", member.ordinal),
                Some(member.data_directory.clone()),
                None,
            )?;
            self.store
                .mark_dispatched(&mut self.journal, index)
                .map_err(|_| NativeLaunchError::Journal)?;
            let binding = prepare_member_directory(
                self.store.root(),
                &member.data_directory,
                host_uid,
                SQL_SERVER_UID,
                self.limit(self.config.deadlines().tls_helper)?,
                &acl,
            )
            .map_err(|_| NativeLaunchError::DataDirectory)?;
            probe_sql_uid_access(
                &member.data_directory,
                self.limit(self.config.deadlines().tls_helper)?,
                &self.runner,
            )?;
            self.store
                .bind(&mut self.journal, index, binding.binding.clone())
                .map_err(|_| NativeLaunchError::Journal)?;
        }
        Ok(())
    }

    fn generate_credentials(
        &mut self,
        run: &TopologyRun,
    ) -> Result<CredentialFiles, NativeLaunchError> {
        let directory = self.store.root().join("credentials");
        self.create_recorded_directory("credentials-directory", &directory)?;
        let suffix = run.run_id.get(..8).ok_or(NativeLaunchError::Readiness)?;
        let sa_password = SecretValue::generate_password().map_err(NativeLaunchError::Secret)?;
        let admin_password = SecretValue::generate_password().map_err(NativeLaunchError::Secret)?;
        let observer_password =
            SecretValue::generate_password().map_err(NativeLaunchError::Secret)?;
        let endpoint_master_key_passwords = (0..3)
            .map(|index| {
                let value = SecretValue::generate_password().map_err(NativeLaunchError::Secret)?;
                self.create_recorded_file(
                    &format!("credential-endpoint-master-key-{}", index + 1),
                    &directory.join(format!("endpoint-master-key-{}", index + 1)),
                    &value,
                    false,
                )
            })
            .collect::<Result<Vec<_>, NativeLaunchError>>()?
            .try_into()
            .map_err(|_| NativeLaunchError::Readiness)?;
        Ok(CredentialFiles {
            sa_username: self.create_recorded_text_file(
                "credential-sa-username",
                &directory.join("sa-username"),
                "sa",
                false,
            )?,
            sa_password: self.create_recorded_file(
                "credential-sa-password",
                &directory.join("sa-password"),
                &sa_password,
                false,
            )?,
            admin_username: self.create_recorded_text_file(
                "credential-admin-username",
                &directory.join("admin-username"),
                format!("km_admin_{suffix}"),
                false,
            )?,
            admin_password: self.create_recorded_file(
                "credential-admin-password",
                &directory.join("admin-password"),
                &admin_password,
                false,
            )?,
            observer_username: self.create_recorded_text_file(
                "credential-observer-username",
                &directory.join("observer-username"),
                format!("km_observer_{suffix}"),
                false,
            )?,
            observer_password: self.create_recorded_file(
                "credential-observer-password",
                &directory.join("observer-password"),
                &observer_password,
                false,
            )?,
            endpoint_master_key_passwords,
        })
    }

    fn write_sql_configuration(&mut self, run: &TopologyRun) -> Result<(), NativeLaunchError> {
        for member in &run.members {
            let path = member.data_directory.join("mssql.conf");
            let record_index = self
                .record_file_intent(&format!("member-{}-configuration", member.ordinal), &path)?;
            PrivateFile::create_text(
                &path,
                "[network]\n\
                 forceencryption = 1\n\
                 tlscert = /var/opt/mssql/secrets/kuberic/server.crt\n\
                 tlskey = /var/opt/mssql/secrets/kuberic/server.key\n\
                 tlsprotocols = 1.2\n\
                 [hadr]\n\
                 hadrenabled = 1\n\
                 [memory]\n\
                 memorylimitmb = 2048\n",
            )
            .map_err(NativeLaunchError::Secret)?;
            self.runner
                .run(
                    &CommandSpec::new(
                        "setfacl",
                        "configure SQL Server configuration ACL",
                        self.limit(self.config.deadlines().tls_helper)?,
                    )
                    .args(["-m", &format!("u:{SQL_SERVER_UID}:r,m:r")])
                    .arg(&path),
                )
                .map_err(|_| NativeLaunchError::DataDirectory)?;
            let file = PrivateFile::inspect_sql_shared(path).map_err(NativeLaunchError::Secret)?;
            self.bind_file_record(record_index, &file)?;
            self.private_files.push(file);
        }
        Ok(())
    }

    async fn initialize_member(
        &self,
        index: usize,
        port: u16,
        run: &TopologyRun,
        credentials: &CredentialFiles,
        tls: &TlsAssets,
    ) -> Result<MemberReadinessEvidence, NativeLaunchError> {
        let endpoint = AdminEndpoint {
            tcp_host: "127.0.0.1".to_owned(),
            tls_hostname: "localhost".to_owned(),
            port,
            ca_certificate: tls.ca_certificate.path().to_path_buf(),
        };
        let sa_login = LoginFiles {
            username: credentials.sa_username.path().to_path_buf(),
            password: credentials.sa_password.path().to_path_buf(),
        };
        let readiness_deadline =
            Instant::now() + self.limit(self.config.deadlines().member_readiness)?;
        let mut sa = loop {
            let deadlines = self.admin_deadlines(readiness_deadline)?;
            match AdminSession::connect(&endpoint, &sa_login, deadlines).await {
                Ok(mut session) => {
                    session
                        .set_query_timeout(
                            self.limit_until(
                                self.config.deadlines().sql_batch,
                                readiness_deadline,
                            )?,
                        )
                        .map_err(NativeLaunchError::Admin)?;
                    match session.readiness().await {
                        Ok(evidence) => {
                            evidence
                                .verify(&run.members[index].server_name)
                                .map_err(NativeLaunchError::Admin)?;
                            break session;
                        }
                        Err(error) if Instant::now() < readiness_deadline => {
                            let _ = error;
                        }
                        Err(error) => return Err(NativeLaunchError::Admin(error)),
                    }
                }
                Err(_) if Instant::now() < readiness_deadline => {}
                Err(error) => return Err(NativeLaunchError::Admin(error)),
            }
            if Instant::now() >= readiness_deadline {
                return Err(NativeLaunchError::Readiness);
            }
            sleep(
                READINESS_RETRY.min(readiness_deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        };
        let admin_username = credentials
            .admin_username
            .read_secret()
            .map_err(NativeLaunchError::Secret)?;
        let admin_password = credentials
            .admin_password
            .read_secret()
            .map_err(NativeLaunchError::Secret)?;
        let observer_username = credentials
            .observer_username
            .read_secret()
            .map_err(NativeLaunchError::Secret)?;
        let observer_password = credentials
            .observer_password
            .read_secret()
            .map_err(NativeLaunchError::Secret)?;
        sa.set_query_timeout(
            self.limit_until(self.config.deadlines().sql_batch, readiness_deadline)?,
        )
        .map_err(NativeLaunchError::Admin)?;
        sa.bootstrap_admin(admin_username.expose(), &admin_password)
            .await
            .map_err(NativeLaunchError::Admin)?;
        sa.set_query_timeout(
            self.limit_until(self.config.deadlines().sql_batch, readiness_deadline)?,
        )
        .map_err(NativeLaunchError::Admin)?;
        sa.bootstrap_observer(observer_username.expose(), &observer_password)
            .await
            .map_err(NativeLaunchError::Admin)?;
        drop(sa);

        let admin_login = LoginFiles {
            username: credentials.admin_username.path().to_path_buf(),
            password: credentials.admin_password.path().to_path_buf(),
        };
        let mut admin = AdminSession::connect(
            &endpoint,
            &admin_login,
            self.admin_deadlines(readiness_deadline)?,
        )
        .await
        .map_err(NativeLaunchError::Admin)?;
        admin
            .set_query_timeout(
                self.limit_until(self.config.deadlines().sql_batch, readiness_deadline)?,
            )
            .map_err(NativeLaunchError::Admin)?;
        let evidence = admin.readiness().await.map_err(NativeLaunchError::Admin)?;
        evidence
            .verify(&run.members[index].server_name)
            .map_err(NativeLaunchError::Admin)?;
        drop(admin);
        let observer_login = LoginFiles {
            username: credentials.observer_username.path().to_path_buf(),
            password: credentials.observer_password.path().to_path_buf(),
        };
        let mut observer = AdminSession::connect(
            &endpoint,
            &observer_login,
            self.admin_deadlines(readiness_deadline)?,
        )
        .await
        .map_err(NativeLaunchError::Admin)?;
        observer
            .set_query_timeout(
                self.limit_until(self.config.deadlines().sql_batch, readiness_deadline)?,
            )
            .map_err(NativeLaunchError::Admin)?;
        observer
            .verify_observer_permissions()
            .await
            .map_err(NativeLaunchError::Admin)?;
        Ok(evidence)
    }

    fn write_observer_config(
        &mut self,
        index: usize,
        port: u16,
        run: &TopologyRun,
        credentials: &CredentialFiles,
        tls: &TlsAssets,
        incarnation: &SqlMemberIncarnation,
    ) -> Result<PathBuf, NativeLaunchError> {
        let directory = self.store.root().join("observer");
        if index == 0 {
            self.create_recorded_directory("observer-directory", &directory)?;
        }
        #[derive(Serialize)]
        struct Document<'a> {
            mode: &'static str,
            host: &'static str,
            port: u16,
            availability_group: String,
            expected_server_name: &'a str,
            replica_id: String,
            incarnation: String,
            observer_username_file: &'a Path,
            observer_password_file: &'a Path,
            ca_certificate_file: &'a Path,
            connect_timeout_ms: u64,
            query_timeout_ms: u64,
            sample_timeout_ms: u64,
            poll_interval_ms: u64,
            max_age_ms: u64,
        }
        let document = Document {
            mode: "observe_only",
            host: "localhost",
            port,
            availability_group: format!("km_ag_{}", run.run_id),
            expected_server_name: &run.members[index].server_name,
            replica_id: run.kuberic_members[index].replica_id.to_string(),
            incarnation: format!(
                "{}:{}",
                incarnation.container_id, incarnation.sql_start_unix_millis
            ),
            observer_username_file: credentials.observer_username.path(),
            observer_password_file: credentials.observer_password.path(),
            ca_certificate_file: tls.ca_certificate.path(),
            connect_timeout_ms: self.config.deadlines().sql_connect.as_millis() as u64,
            query_timeout_ms: self.config.deadlines().sql_batch.as_millis() as u64,
            sample_timeout_ms: self.config.deadlines().member_readiness.as_millis() as u64,
            poll_interval_ms: 2_000,
            max_age_ms: 30_000,
        };
        let bytes =
            serde_json::to_vec_pretty(&document).map_err(|_| NativeLaunchError::Readiness)?;
        ObserverConfig::from_json(&bytes).map_err(|_| NativeLaunchError::Readiness)?;
        let path = directory.join(format!("member-{}.json", index + 1));
        self.create_recorded_bytes_file(
            &format!("observer-config-{}", index + 1),
            &path,
            &bytes,
            false,
        )?;
        Ok(path)
    }

    fn verify_network_attachments(&self, require_all: bool) -> Result<(), NativeLaunchError> {
        let network = self.network.as_ref().ok_or(NativeLaunchError::Ownership)?;
        let id = network.id.as_deref().ok_or(NativeLaunchError::Ownership)?;
        let inspection = self
            .docker
            .inspect_network(id, self.limit(self.config.deadlines().docker_command)?)
            .map_err(NativeLaunchError::Docker)?
            .ok_or(NativeLaunchError::Ownership)?;
        network
            .request
            .verify_inspection(&inspection)
            .map_err(NativeLaunchError::Docker)?;
        let actual = inspection
            .attached_container_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected = self
            .containers
            .iter()
            .map(|container| container.id.clone().ok_or(NativeLaunchError::Ownership))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if if require_all {
            actual != expected
        } else {
            !actual.is_subset(&expected)
        } {
            return Err(NativeLaunchError::Ownership);
        }
        Ok(())
    }

    fn inspect_container(&self, id: &str) -> Result<ContainerInspection, NativeLaunchError> {
        self.docker
            .inspect_container(id, self.limit(self.config.deadlines().docker_command)?)
            .map_err(NativeLaunchError::Docker)?
            .ok_or(NativeLaunchError::Ownership)
    }

    fn record_intent(
        &mut self,
        kind: ResourceKind,
        logical_name: &str,
        path: Option<PathBuf>,
        intent: Option<ResourceBinding>,
    ) -> Result<usize, NativeLaunchError> {
        self.store
            .record_intent(
                &mut self.journal,
                ResourceRecord {
                    kind,
                    logical_name: logical_name.to_owned(),
                    path,
                    intent,
                    binding: None,
                    state: ResourceState::Intended,
                },
            )
            .map_err(|_| NativeLaunchError::Journal)
    }

    fn record_file_intent(
        &mut self,
        logical_name: &str,
        path: &Path,
    ) -> Result<usize, NativeLaunchError> {
        let index = self.record_intent(
            ResourceKind::SecretFile,
            logical_name,
            Some(path.to_path_buf()),
            None,
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| NativeLaunchError::Journal)?;
        Ok(index)
    }

    fn bind_file_record(
        &mut self,
        index: usize,
        file: &PrivateFile,
    ) -> Result<(), NativeLaunchError> {
        self.store
            .bind(&mut self.journal, index, file.binding().clone())
            .map_err(|_| NativeLaunchError::Journal)
    }

    fn create_recorded_directory(
        &mut self,
        logical_name: &str,
        path: &Path,
    ) -> Result<PrivateDirectoryBinding, NativeLaunchError> {
        let index = self.record_intent(
            ResourceKind::Directory,
            logical_name,
            Some(path.to_path_buf()),
            None,
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| NativeLaunchError::Journal)?;
        let directory = create_private_owned_directory(self.store.root(), path)
            .map_err(|_| NativeLaunchError::DataDirectory)?;
        self.store
            .bind(&mut self.journal, index, directory.binding.clone())
            .map_err(|_| NativeLaunchError::Journal)?;
        Ok(directory)
    }

    fn create_recorded_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        value: &SecretValue,
        sql_shared: bool,
    ) -> Result<PrivateFile, NativeLaunchError> {
        self.create_recorded_bytes_file(logical_name, path, value.expose().as_bytes(), sql_shared)
    }

    fn create_recorded_text_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        value: impl AsRef<str>,
        sql_shared: bool,
    ) -> Result<PrivateFile, NativeLaunchError> {
        self.create_recorded_bytes_file(logical_name, path, value.as_ref().as_bytes(), sql_shared)
    }

    fn create_recorded_bytes_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        value: &[u8],
        sql_shared: bool,
    ) -> Result<PrivateFile, NativeLaunchError> {
        let index = self.record_file_intent(logical_name, path)?;
        let mut file = PrivateFile::create_bytes(path, value).map_err(NativeLaunchError::Secret)?;
        if sql_shared {
            file = PrivateFile::inspect_sql_shared(path).map_err(NativeLaunchError::Secret)?;
        }
        self.bind_file_record(index, &file)?;
        self.private_files.push(file.clone());
        Ok(file)
    }

    fn limit(&self, stage: Duration) -> Result<Duration, NativeLaunchError> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        let value = remaining.min(stage);
        if value.is_zero() {
            Err(NativeLaunchError::Deadline)
        } else {
            Ok(value)
        }
    }

    fn limit_until(
        &self,
        stage: Duration,
        stage_deadline: Instant,
    ) -> Result<Duration, NativeLaunchError> {
        let remaining_parent = self.deadline.saturating_duration_since(Instant::now());
        let remaining_stage = stage_deadline.saturating_duration_since(Instant::now());
        let value = stage.min(remaining_parent).min(remaining_stage);
        if value.is_zero() {
            Err(NativeLaunchError::Deadline)
        } else {
            Ok(value)
        }
    }

    fn admin_deadlines(
        &self,
        readiness_deadline: Instant,
    ) -> Result<AdminDeadlines, NativeLaunchError> {
        Ok(AdminDeadlines {
            connect: self.limit_until(self.config.deadlines().sql_connect, readiness_deadline)?,
            query: self.limit_until(self.config.deadlines().sql_batch, readiness_deadline)?,
        })
    }

    fn cleanup_report(&mut self) -> CleanupReport {
        let clock = SystemCleanupClock::default();
        let coordinator = CleanupCoordinator::new(clock.clone(), CLEANUP_BUDGET);
        let backend = NativeCleanupBackend {
            root: self.store.root(),
            docker: &self.docker,
            runner: &self.runner,
            clock,
            image_id: &self.image_id,
            network: self.network.as_ref(),
            containers: &self.containers,
        };
        coordinator.cleanup(&self.store, &mut self.journal, &backend)
    }

    fn cleanup_exact(&mut self) -> Result<CleanupEvidence, CombinedFixtureError> {
        let removed_container_ids = self
            .containers
            .iter()
            .filter_map(|container| container.id.clone())
            .collect();
        let removed_network_id = self
            .network
            .as_ref()
            .and_then(|network| network.id.clone())
            .unwrap_or_default();
        let evidence = CleanupEvidence {
            removed_container_ids,
            removed_network_id,
            journal_path: self.store.path().to_path_buf(),
        };
        let report = self.cleanup_report();
        combine_with_cleanup(Ok(evidence), &report)
    }
}

impl Drop for LaunchContext {
    fn drop(&mut self) {
        if self.journal.state != RunState::Removed {
            eprintln!(
                "three-replica fixture journal retained at {} with state {:?}; explicit cleanup is required",
                self.store.path().display(),
                self.journal.state
            );
        }
    }
}

struct LaunchAssetRecorder<'a> {
    store: &'a JournalStore,
    journal: &'a mut OwnershipJournal,
    private_files: &'a mut Vec<PrivateFile>,
}

impl LaunchAssetRecorder<'_> {
    fn record_file_intent(&mut self, logical_name: &str, path: &Path) -> Result<usize, TlsError> {
        let index = self
            .store
            .record_intent(
                self.journal,
                ResourceRecord {
                    kind: ResourceKind::SecretFile,
                    logical_name: logical_name.to_owned(),
                    path: Some(path.to_path_buf()),
                    intent: None,
                    binding: None,
                    state: ResourceState::Intended,
                },
            )
            .map_err(|_| TlsError::Journal)?;
        self.store
            .mark_dispatched(self.journal, index)
            .map_err(|_| TlsError::Journal)?;
        Ok(index)
    }
}

impl TlsAssetRecorder for LaunchAssetRecorder<'_> {
    fn create_directory(&mut self, logical_name: &str, path: &Path) -> Result<(), TlsError> {
        let index = self
            .store
            .record_intent(
                self.journal,
                ResourceRecord {
                    kind: ResourceKind::Directory,
                    logical_name: logical_name.to_owned(),
                    path: Some(path.to_path_buf()),
                    intent: None,
                    binding: None,
                    state: ResourceState::Intended,
                },
            )
            .map_err(|_| TlsError::Journal)?;
        self.store
            .mark_dispatched(self.journal, index)
            .map_err(|_| TlsError::Journal)?;
        let directory = create_private_owned_directory(self.store.root(), path)
            .map_err(|_| TlsError::Helper)?;
        self.store
            .bind(self.journal, index, directory.binding.clone())
            .map_err(|_| TlsError::Journal)?;
        Ok(())
    }

    fn create_text_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        contents: &str,
    ) -> Result<PrivateFile, TlsError> {
        let index = self.record_file_intent(logical_name, path)?;
        let file = PrivateFile::create_text(path, contents).map_err(TlsError::Secret)?;
        self.store
            .bind(self.journal, index, file.binding().clone())
            .map_err(|_| TlsError::Journal)?;
        self.private_files.push(file.clone());
        Ok(file)
    }

    fn dispatch_file(&mut self, logical_name: &str, path: &Path) -> Result<usize, TlsError> {
        self.record_file_intent(logical_name, path)
    }

    fn bind_file(
        &mut self,
        record_index: usize,
        path: &Path,
        sql_shared: bool,
    ) -> Result<PrivateFile, TlsError> {
        let file = if sql_shared {
            PrivateFile::inspect_sql_shared(path)
        } else {
            PrivateFile::inspect(path)
        }
        .map_err(TlsError::Secret)?;
        self.store
            .bind(self.journal, record_index, file.binding().clone())
            .map_err(|_| TlsError::Journal)?;
        self.private_files.push(file.clone());
        Ok(file)
    }
}

struct NativeCleanupBackend<'a, D, R, C> {
    root: &'a Path,
    docker: &'a D,
    runner: &'a R,
    clock: C,
    image_id: &'a str,
    network: Option<&'a OwnedNetwork>,
    containers: &'a [OwnedContainer],
}

impl<D, R, C> NativeCleanupBackend<'_, D, R, C>
where
    D: DockerApi,
    R: ProcessRunner + Copy,
    C: CleanupClock,
{
    fn operation_budget(&self, remaining: Duration) -> OperationBudget<'_, C> {
        OperationBudget::new(&self.clock, remaining.min(Duration::from_secs(30)))
    }

    fn timeout(
        &self,
        resource: &ResourceRecord,
        budget: &OperationBudget<'_, C>,
    ) -> Result<Duration, CleanupError> {
        budget.remaining().ok_or_else(|| CleanupError {
            resource: resource.logical_name.clone(),
            failure: SanitizedFailure::new(
                FailureStage::Cleanup,
                FailureCategory::DeadlineExceeded,
            ),
        })
    }

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
        if resource.binding.is_none() {
            return Ok(ResourceObservation::Foreign);
        }
        let canonical = path
            .canonicalize()
            .map_err(|_| ReconcileError::OwnershipMismatch)?;
        if canonical != *path || !canonical.starts_with(self.root) || canonical == self.root {
            return Ok(ResourceObservation::Foreign);
        }
        let binding = match resource.kind {
            ResourceKind::DataDirectory => {
                let timeout = remaining.min(Duration::from_secs(30));
                if timeout.is_zero() {
                    return Err(ReconcileError::Io);
                }
                inspect_member_directory(
                    self.root,
                    path,
                    unsafe { libc::geteuid() },
                    SQL_SERVER_UID,
                    timeout,
                    &CommandAclController::new(*self.runner),
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
}

impl<D, R, C> OwnershipInspector for NativeCleanupBackend<'_, D, R, C>
where
    D: DockerApi,
    R: ProcessRunner + Copy,
    C: CleanupClock,
{
    fn inspect(&self, resource: &ResourceRecord) -> Result<ResourceObservation, ReconcileError> {
        CleanupBackend::inspect_cleanup(self, resource, Duration::from_secs(30))
    }
}

impl<D, R, C> CleanupBackend for NativeCleanupBackend<'_, D, R, C>
where
    D: DockerApi,
    R: ProcessRunner + Copy,
    C: CleanupClock,
{
    fn inspect_cleanup(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<ResourceObservation, ReconcileError> {
        match resource.kind {
            ResourceKind::Network => {
                let network = self
                    .network
                    .filter(|network| {
                        resource.intent.as_ref() == Some(&network.request.intent_binding())
                    })
                    .ok_or(ReconcileError::OwnershipMismatch)?;
                let identity = resource.binding.as_ref().map_or_else(
                    || {
                        network
                            .id
                            .as_deref()
                            .unwrap_or(network.request.name.as_str())
                    },
                    |binding| binding.immutable_id.as_str(),
                );
                let inspection = self
                    .docker
                    .inspect_network(identity, remaining.min(Duration::from_secs(30)))
                    .map_err(|_| ReconcileError::Io)?;
                let Some(inspection) = inspection else {
                    return Ok(ResourceObservation::Absent);
                };
                if network.request.verify_inspection(&inspection).is_err() {
                    return Ok(ResourceObservation::Foreign);
                }
                let binding = if let Some(expected) = resource.binding.as_ref() {
                    if inspection.id != expected.immutable_id {
                        return Ok(ResourceObservation::Foreign);
                    }
                    expected.clone()
                } else {
                    network_resource_binding(&network.request, &inspection)
                        .map_err(|_| ReconcileError::OwnershipMismatch)?
                };
                Ok(ResourceObservation::Owned {
                    binding,
                    foreign_attachments: inspection
                        .attached_container_ids
                        .iter()
                        .filter(|id| {
                            !self
                                .containers
                                .iter()
                                .filter_map(|container| container.id.as_ref())
                                .any(|owned| owned == *id)
                        })
                        .cloned()
                        .collect(),
                })
            }
            ResourceKind::Container => {
                let container = self
                    .containers
                    .iter()
                    .find(|container| {
                        resource.intent.as_ref().is_some_and(|intent| {
                            intent == &container.request.intent_binding(self.image_id)
                        })
                    })
                    .ok_or(ReconcileError::OwnershipMismatch)?;
                let identity = resource.binding.as_ref().map_or_else(
                    || {
                        container
                            .id
                            .as_deref()
                            .unwrap_or(container.request.name.as_str())
                    },
                    |binding| binding.immutable_id.as_str(),
                );
                let inspection = self
                    .docker
                    .inspect_container(identity, remaining.min(Duration::from_secs(30)))
                    .map_err(|_| ReconcileError::Io)?;
                let Some(inspection) = inspection else {
                    return Ok(ResourceObservation::Absent);
                };
                if container
                    .request
                    .verify_inspection(&inspection, self.image_id, inspection.running)
                    .is_err()
                {
                    return Ok(ResourceObservation::Foreign);
                }
                let binding = if let Some(expected) = resource.binding.as_ref() {
                    if inspection.id != expected.immutable_id {
                        return Ok(ResourceObservation::Foreign);
                    }
                    expected.clone()
                } else {
                    container_resource_binding(&container.request, self.image_id, &inspection)
                        .map_err(|_| ReconcileError::OwnershipMismatch)?
                };
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
        let container = self
            .containers
            .iter()
            .find(|container| {
                resource.intent.as_ref().is_some_and(|intent| {
                    intent == &container.request.intent_binding(self.image_id)
                })
            })
            .ok_or_else(|| cleanup_ownership_error(resource))?;
        let expected = resource
            .binding
            .as_ref()
            .ok_or_else(|| cleanup_ownership_error(resource))?;
        let identity = expected.immutable_id.as_str();
        let budget = self.operation_budget(remaining);
        if let Some(inspection) = self
            .docker
            .inspect_container(identity, self.timeout(resource, &budget)?)
            .map_err(|_| cleanup_operation_error(resource, FailureCategory::ContainerRemoval))?
        {
            container
                .request
                .verify_inspection(&inspection, self.image_id, inspection.running)
                .map_err(|_| cleanup_ownership_error(resource))?;
            if inspection.id != expected.immutable_id {
                return Err(cleanup_ownership_error(resource));
            }
            if inspection.running {
                self.docker
                    .stop_container(identity, self.timeout(resource, &budget)?)
                    .map_err(|_| {
                        cleanup_operation_error(resource, FailureCategory::ContainerRemoval)
                    })?;
            }
            self.docker
                .remove_container(identity, self.timeout(resource, &budget)?)
                .map_err(|_| {
                    cleanup_operation_error(resource, FailureCategory::ContainerRemoval)
                })?;
        }
        Ok(())
    }

    fn remove_network(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        let _network = self
            .network
            .filter(|network| resource.intent.as_ref() == Some(&network.request.intent_binding()))
            .ok_or_else(|| cleanup_ownership_error(resource))?;
        let identity = resource
            .binding
            .as_ref()
            .ok_or_else(|| cleanup_ownership_error(resource))?
            .immutable_id
            .as_str();
        let budget = self.operation_budget(remaining);
        self.docker
            .remove_network(identity, self.timeout(resource, &budget)?)
            .map_err(|_| cleanup_operation_error(resource, FailureCategory::NetworkRemoval))
    }

    fn remove_path(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        let expected = resource
            .binding
            .as_ref()
            .ok_or_else(|| cleanup_ownership_error(resource))?;
        let budget = self.operation_budget(remaining);
        let ResourceObservation::Owned { binding, .. } = self
            .inspect_path(resource, self.timeout(resource, &budget)?)
            .map_err(|_| cleanup_ownership_error(resource))?
        else {
            return Err(cleanup_ownership_error(resource));
        };
        if &binding != expected {
            return Err(cleanup_ownership_error(resource));
        }
        let path = resource
            .path
            .as_ref()
            .ok_or_else(|| cleanup_ownership_error(resource))?;
        match resource.kind {
            ResourceKind::SecretFile => fs::remove_file(path),
            ResourceKind::Directory => fs::remove_dir_all(path),
            ResourceKind::DataDirectory => {
                restore_host_cleanup_acl(
                    path,
                    unsafe { libc::geteuid() },
                    self.timeout(resource, &budget)?,
                    self.runner,
                )
                .map_err(|_| cleanup_operation_error(resource, FailureCategory::PathRemoval))?;
                let current = inspect_owned_directory(self.root, path)
                    .map_err(|_| cleanup_ownership_error(resource))?;
                if current.binding.immutable_id != expected.immutable_id {
                    return Err(cleanup_ownership_error(resource));
                }
                fs::remove_dir_all(path)
            }
            _ => return Err(cleanup_ownership_error(resource)),
        }
        .map_err(|_| cleanup_operation_error(resource, FailureCategory::PathRemoval))
    }
}

fn cleanup_ownership_error(resource: &ResourceRecord) -> CleanupError {
    cleanup_operation_error(resource, FailureCategory::OwnershipMismatch)
}

fn cleanup_operation_error(resource: &ResourceRecord, category: FailureCategory) -> CleanupError {
    CleanupError {
        resource: resource.logical_name.clone(),
        failure: SanitizedFailure::new(FailureStage::Cleanup, category),
    }
}

fn reconcile_network_create(
    docker: &impl DockerApi,
    network: &OwnedNetwork,
    timeout: Duration,
) -> Result<super::docker::NetworkInspection, NativeLaunchError> {
    let create = docker.create_network(&network.request, timeout);
    let returned_id = create.as_ref().ok().filter(|id| !id.is_empty());
    let identities = returned_id
        .into_iter()
        .map(String::as_str)
        .chain(std::iter::once(network.request.name.as_str()));
    for identity in identities {
        if let Ok(Some(inspection)) = docker.inspect_network(identity, timeout)
            && network.request.verify_inspection(&inspection).is_ok()
        {
            return Ok(inspection);
        }
    }
    match create {
        Err(error) => Err(NativeLaunchError::Docker(error)),
        Ok(_) => Err(NativeLaunchError::Ownership),
    }
}

fn reconcile_container_create(
    docker: &impl DockerApi,
    container: &OwnedContainer,
    image_id: &str,
    timeout: Duration,
) -> Result<ContainerInspection, NativeLaunchError> {
    let create = docker.create_container(&container.request, timeout);
    let returned_id = create.as_ref().ok().filter(|id| !id.is_empty());
    let identities = returned_id
        .into_iter()
        .map(String::as_str)
        .chain(std::iter::once(container.request.name.as_str()));
    for identity in identities {
        if let Ok(Some(inspection)) = docker.inspect_container(identity, timeout)
            && container
                .request
                .verify_inspection(&inspection, image_id, false)
                .is_ok()
        {
            return Ok(inspection);
        }
    }
    match create {
        Err(error) => Err(NativeLaunchError::Docker(error)),
        Ok(_) => Err(NativeLaunchError::Ownership),
    }
}

#[derive(Debug)]
pub enum NativeLaunchError {
    RootLock,
    PriorRunUnresolved,
    AuthorizationChanged,
    Preflight,
    Journal,
    DataDirectory,
    Secret(SecretError),
    Tls(TlsError),
    Docker(DockerError),
    Admin(AdminError),
    Ownership,
    Readiness,
    Deadline,
    Cleanup,
}

impl fmt::Display for NativeLaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RootLock => "three-replica fixture root lock failed",
            Self::PriorRunUnresolved => "a prior three-replica run remains unresolved",
            Self::AuthorizationChanged => "SQL Server EULA acknowledgement changed before create",
            Self::Preflight => "three-replica host preflight failed",
            Self::Journal => "three-replica ownership journal failed",
            Self::DataDirectory => "three-replica member data directory failed",
            Self::Secret(_) => "three-replica private credential operation failed",
            Self::Tls(_) => "three-replica TLS asset operation failed",
            Self::Docker(_) => "three-replica Docker operation failed",
            Self::Admin(_) => "three-replica SQL administration failed",
            Self::Ownership => "three-replica resource ownership verification failed",
            Self::Readiness => "three-replica SQL readiness failed",
            Self::Deadline => "three-replica setup deadline exceeded",
            Self::Cleanup => "three-replica exact cleanup failed",
        })
    }
}

impl std::error::Error for NativeLaunchError {}

impl NativeLaunchError {
    fn sanitized(&self) -> SanitizedFailure {
        let category = match self {
            Self::Preflight => FailureCategory::Preflight,
            Self::Docker(_) => FailureCategory::ContainerCreation,
            Self::Admin(_) | Self::Readiness => FailureCategory::SqlUnavailable,
            Self::Deadline => FailureCategory::DeadlineExceeded,
            Self::Journal => FailureCategory::Journal,
            Self::Ownership | Self::PriorRunUnresolved | Self::RootLock => {
                FailureCategory::OwnershipMismatch
            }
            Self::AuthorizationChanged
            | Self::DataDirectory
            | Self::Secret(_)
            | Self::Tls(_)
            | Self::Cleanup => FailureCategory::PathRemoval,
        };
        SanitizedFailure::new(FailureStage::Setup, category)
    }
}

fn topology_run(root: &Path) -> Result<TopologyRun, NativeLaunchError> {
    let run_id = random_hex(12)?;
    let hostname_prefix = &run_id[..10];
    Ok(TopologyRun {
        run_id: run_id.clone(),
        resource_uid: format!("mssql-{run_id}"),
        members: std::array::from_fn(|index| SqlMember {
            ordinal: (index + 1) as u8,
            server_name: format!("km{hostname_prefix}n{}", index + 1),
            container_name: format!("km-three-{run_id}-{}", index + 1),
            data_directory: root.join(format!("member-{}", index + 1)),
        }),
        kuberic_members: std::array::from_fn(|index| KubericMember {
            ordinal: (index + 1) as u8,
            replica_id: (index + 1) as i64,
            instance_id: format!("{run_id}-instance-{}", index + 1),
            pod_uid: format!("{run_id}-pod-{}", index + 1),
            pvc_uid: format!("{run_id}-pvc-{}", index + 1),
        }),
    })
}

fn random_hex(length: usize) -> Result<String, NativeLaunchError> {
    let value = SecretValue::generate_password().map_err(NativeLaunchError::Secret)?;
    Ok(value.expose()[..length].to_owned())
}

fn probe_sql_uid_access(
    data_directory: &Path,
    timeout: Duration,
    runner: &impl ProcessRunner,
) -> Result<(), NativeLaunchError> {
    let probe = data_directory.join(".sql-uid-probe");
    const SCRIPT: &str = r#"
import os
import sys
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
os.setgroups([])
os.setgid(0)
os.setuid(10001)
os.mkdir(".sql-uid-probe", dir_fd=fd)
probe = os.open(".sql-uid-probe/probe", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd)
os.write(probe, b"sql-uid")
os.close(probe)
os.close(fd)
"#;
    runner
        .run(
            &CommandSpec::new("sudo", "verify SQL UID data access", timeout)
                .args(["-n", "python3", "-c", SCRIPT])
                .arg(data_directory),
        )
        .map_err(|_| NativeLaunchError::DataDirectory)?;
    fs::remove_dir_all(&probe).map_err(|_| NativeLaunchError::DataDirectory)?;
    if probe.exists() {
        return Err(NativeLaunchError::DataDirectory);
    }
    Ok(())
}

fn restore_host_cleanup_acl(
    data_directory: &Path,
    host_uid: u32,
    timeout: Duration,
    runner: &impl ProcessRunner,
) -> Result<(), NativeLaunchError> {
    runner
        .run(
            &CommandSpec::new("sudo", "restore host cleanup ACL", timeout)
                .args([
                    "-n",
                    "setfacl",
                    "--recursive",
                    "--physical",
                    "--modify",
                    &format!("u:{host_uid}:rwx,m:rwx"),
                ])
                .arg(data_directory),
        )
        .map(|_| ())
        .map_err(|_| NativeLaunchError::Cleanup)
}

fn network_resource_binding(
    request: &NetworkRequest,
    inspection: &super::docker::NetworkInspection,
) -> Result<ResourceBinding, NativeLaunchError> {
    request
        .verify_inspection(inspection)
        .map_err(NativeLaunchError::Docker)?;
    stable_docker_binding(
        &inspection.id,
        &request.intent_binding(),
        &Vec::<super::docker::ContainerPort>::new(),
    )
}

fn container_resource_binding(
    request: &ContainerRequest,
    image_id: &str,
    inspection: &ContainerInspection,
) -> Result<ResourceBinding, NativeLaunchError> {
    request
        .verify_inspection(inspection, image_id, inspection.running)
        .map_err(NativeLaunchError::Docker)?;
    stable_docker_binding(
        &inspection.id,
        &request.intent_binding(image_id),
        &inspection.ports,
    )
}

fn stable_docker_binding(
    immutable_id: &str,
    intent: &ResourceBinding,
    ports: &[super::docker::ContainerPort],
) -> Result<ResourceBinding, NativeLaunchError> {
    if immutable_id.is_empty() {
        return Err(NativeLaunchError::Ownership);
    }
    let bytes = serde_json::to_vec(&(intent, ports)).map_err(|_| NativeLaunchError::Ownership)?;
    let digest = Sha256::digest(bytes);
    Ok(ResourceBinding {
        immutable_id: immutable_id.to_owned(),
        attributes_sha256: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::os::unix::fs::PermissionsExt;
    use std::rc::Rc;

    use tempfile::tempdir;

    use super::*;
    use crate::three_replica::docker::{DockerCapabilities, ImageInspection, NetworkInspection};
    use crate::three_replica::ownership::reconcile;

    #[derive(Clone)]
    struct FakeClock {
        now: Rc<Cell<Duration>>,
    }

    impl FakeClock {
        fn new() -> Self {
            Self {
                now: Rc::new(Cell::new(Duration::ZERO)),
            }
        }
    }

    impl CleanupClock for FakeClock {
        fn now(&self) -> Duration {
            self.now.get()
        }
    }

    struct CleanupDocker {
        clock: FakeClock,
        elapsed_per_call: Duration,
        container: RefCell<Option<ContainerInspection>>,
        network: RefCell<Option<NetworkInspection>>,
        calls: RefCell<Vec<(String, String, Duration)>>,
    }

    impl CleanupDocker {
        fn record(&self, operation: &str, identity: &str, timeout: Duration) {
            self.calls
                .borrow_mut()
                .push((operation.to_owned(), identity.to_owned(), timeout));
            self.clock
                .now
                .set(self.clock.now.get() + self.elapsed_per_call);
        }
    }

    impl DockerApi for CleanupDocker {
        fn capabilities(&self, _: Duration) -> Result<DockerCapabilities, DockerError> {
            Err(DockerError::Command)
        }

        fn docker_root(&self, _: Duration) -> Result<PathBuf, DockerError> {
            Err(DockerError::Command)
        }

        fn inspect_image(
            &self,
            _: &str,
            _: Duration,
        ) -> Result<Option<ImageInspection>, DockerError> {
            Err(DockerError::Command)
        }

        fn pull_image(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            Err(DockerError::Command)
        }

        fn inspect_network(
            &self,
            identity: &str,
            timeout: Duration,
        ) -> Result<Option<NetworkInspection>, DockerError> {
            self.record("inspect-network", identity, timeout);
            Ok(self.network.borrow().clone())
        }

        fn create_network(&self, _: &NetworkRequest, _: Duration) -> Result<String, DockerError> {
            Err(DockerError::Command)
        }

        fn remove_network(&self, id: &str, timeout: Duration) -> Result<(), DockerError> {
            self.record("remove-network", id, timeout);
            self.network.borrow_mut().take();
            Ok(())
        }

        fn inspect_container(
            &self,
            identity: &str,
            timeout: Duration,
        ) -> Result<Option<ContainerInspection>, DockerError> {
            self.record("inspect-container", identity, timeout);
            Ok(self.container.borrow().clone())
        }

        fn create_container(
            &self,
            _: &ContainerRequest,
            _: Duration,
        ) -> Result<String, DockerError> {
            Err(DockerError::Command)
        }

        fn start_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            Err(DockerError::Command)
        }

        fn stop_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError> {
            self.record("stop-container", id, timeout);
            if let Some(container) = self.container.borrow_mut().as_mut() {
                container.running = false;
            }
            Ok(())
        }

        fn remove_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError> {
            self.record("remove-container", id, timeout);
            self.container.borrow_mut().take();
            Ok(())
        }
    }

    fn cleanup_container(
        root: &Path,
        running: bool,
    ) -> (ContainerRequest, ContainerInspection, OwnedContainer) {
        let data = root.join("member");
        fs::create_dir(&data).unwrap();
        let request = ContainerRequest::sql_server(
            SqlServerContainerSpec {
                name: "container-name".to_owned(),
                hostname: "km0123456789n1".to_owned(),
                network_name: "network-name".to_owned(),
                data_directory: data.clone(),
                environment_file: root.join("container.env"),
                sa_password: SecretValue::from_test("Password-Aa1!"),
            },
            OwnedLabels::container("0123456789ab", 1),
            super::super::config::ResourcePolicy::default(),
            [("ACCEPT_EULA", "Y")],
        )
        .unwrap();
        let inspection = ContainerInspection {
            id: "journaled-container-id".to_owned(),
            name: request.name.clone(),
            hostname: request.hostname.clone(),
            image_id: "image-id".to_owned(),
            labels: request.labels.as_map(),
            environment: request
                .environment_file_contents()
                .lines()
                .map(str::to_owned)
                .collect(),
            user: "mssql".to_owned(),
            running,
            restart_policy: "no".to_owned(),
            network_mode: request.network_name.clone(),
            network_names: vec![request.network_name.clone()],
            mounts: vec![super::super::docker::ContainerMount {
                source: data.canonicalize().unwrap(),
                destination: PathBuf::from("/var/opt/mssql"),
                read_only: false,
            }],
            ports: vec![super::super::docker::ContainerPort {
                container_port: 1433,
                host_ip: "127.0.0.1".to_owned(),
                host_port: 49171,
            }],
            limits: super::super::docker::ContainerLimits::from_policy(
                super::super::config::ResourcePolicy::default(),
            ),
        };
        let owned = OwnedContainer {
            request: request.clone(),
            id: None,
            frozen_running_inspection: None,
        };
        (request, inspection, owned)
    }

    struct LateNetworkDocker {
        inspection: RefCell<Result<Option<NetworkInspection>, DockerError>>,
        creates: Cell<u32>,
    }

    impl DockerApi for LateNetworkDocker {
        fn capabilities(&self, _: Duration) -> Result<DockerCapabilities, DockerError> {
            Err(DockerError::Command)
        }

        fn docker_root(&self, _: Duration) -> Result<PathBuf, DockerError> {
            Err(DockerError::Command)
        }

        fn inspect_image(
            &self,
            _: &str,
            _: Duration,
        ) -> Result<Option<ImageInspection>, DockerError> {
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
            self.inspection.borrow().clone()
        }

        fn create_network(&self, _: &NetworkRequest, _: Duration) -> Result<String, DockerError> {
            self.creates.set(self.creates.get() + 1);
            Err(DockerError::Command)
        }

        fn remove_network(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            Err(DockerError::Command)
        }

        fn inspect_container(
            &self,
            _: &str,
            _: Duration,
        ) -> Result<Option<ContainerInspection>, DockerError> {
            Err(DockerError::Command)
        }

        fn create_container(
            &self,
            _: &ContainerRequest,
            _: Duration,
        ) -> Result<String, DockerError> {
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

    fn run(root: &Path) -> TopologyRun {
        TopologyRun {
            run_id: "0123456789ab".to_owned(),
            resource_uid: "resource".to_owned(),
            members: std::array::from_fn(|index| SqlMember {
                ordinal: (index + 1) as u8,
                server_name: format!("member-{}", index + 1),
                container_name: format!("container-{}", index + 1),
                data_directory: root.join(format!("member-{}", index + 1)),
            }),
            kuberic_members: std::array::from_fn(|index| KubericMember {
                ordinal: (index + 1) as u8,
                replica_id: (index + 1) as i64,
                instance_id: format!("instance-{}", index + 1),
                pod_uid: format!("pod-{}", index + 1),
                pvc_uid: format!("pvc-{}", index + 1),
            }),
        }
    }

    #[test]
    fn docker_cleanup_binding_ignores_runtime_state_but_keeps_exact_port_identity() {
        let temporary = tempdir().unwrap();
        let data = temporary.path().join("member");
        fs::create_dir(&data).unwrap();
        let request = ContainerRequest::sql_server(
            SqlServerContainerSpec {
                name: "container".to_owned(),
                hostname: "km0123456789n1".to_owned(),
                network_name: "network".to_owned(),
                data_directory: data.clone(),
                environment_file: temporary.path().join("container.env"),
                sa_password: SecretValue::from_test("Password-Aa1!"),
            },
            OwnedLabels::container("0123456789ab", 1),
            super::super::config::ResourcePolicy::default(),
            [("ACCEPT_EULA", "Y")],
        )
        .unwrap();
        let mut stopped = ContainerInspection {
            id: "container-id".to_owned(),
            name: request.name.clone(),
            hostname: request.hostname.clone(),
            image_id: "image-id".to_owned(),
            labels: request.labels.as_map(),
            environment: request
                .environment_file_contents()
                .lines()
                .map(str::to_owned)
                .collect(),
            user: "mssql".to_owned(),
            running: false,
            restart_policy: "no".to_owned(),
            network_mode: request.network_name.clone(),
            network_names: vec![request.network_name.clone()],
            mounts: vec![super::super::docker::ContainerMount {
                source: data.canonicalize().unwrap(),
                destination: PathBuf::from("/var/opt/mssql"),
                read_only: false,
            }],
            ports: vec![super::super::docker::ContainerPort {
                container_port: 1433,
                host_ip: "127.0.0.1".to_owned(),
                host_port: 49171,
            }],
            limits: super::super::docker::ContainerLimits::from_policy(
                super::super::config::ResourcePolicy::default(),
            ),
        };
        let before = container_resource_binding(&request, "image-id", &stopped).unwrap();
        stopped.running = true;
        let running = container_resource_binding(&request, "image-id", &stopped).unwrap();
        assert_eq!(before, running);
        stopped.ports[0].host_port += 1;
        assert_ne!(
            running,
            container_resource_binding(&request, "image-id", &stopped).unwrap()
        );
    }

    #[test]
    fn cleanup_removes_exact_container_before_start_without_frozen_running_snapshot() {
        let temporary = tempdir().unwrap();
        let (request, inspection, owned) = cleanup_container(temporary.path(), false);
        let binding = container_resource_binding(&request, "image-id", &inspection).unwrap();
        let resource = ResourceRecord {
            kind: ResourceKind::Container,
            logical_name: "container-1".to_owned(),
            path: None,
            intent: Some(request.intent_binding("image-id")),
            binding: Some(binding),
            state: ResourceState::Bound,
        };
        let clock = FakeClock::new();
        let docker = CleanupDocker {
            clock: clock.clone(),
            elapsed_per_call: Duration::ZERO,
            container: RefCell::new(Some(inspection)),
            network: RefCell::new(None),
            calls: RefCell::new(Vec::new()),
        };
        let runner = BoundedProcessRunner;
        let backend = NativeCleanupBackend {
            root: temporary.path(),
            docker: &docker,
            runner: &runner,
            clock,
            image_id: "image-id",
            network: None,
            containers: &[owned],
        };

        backend
            .remove_container(&resource, Duration::from_secs(180))
            .unwrap();

        assert_eq!(
            docker.calls.borrow().as_slice(),
            [
                (
                    "inspect-container".to_owned(),
                    "journaled-container-id".to_owned(),
                    Duration::from_secs(30),
                ),
                (
                    "remove-container".to_owned(),
                    "journaled-container-id".to_owned(),
                    Duration::from_secs(30),
                ),
            ]
        );
    }

    #[test]
    fn sequential_container_cleanup_operations_receive_decreasing_budget() {
        let temporary = tempdir().unwrap();
        let (request, inspection, owned) = cleanup_container(temporary.path(), true);
        let mut created = inspection.clone();
        created.running = false;
        created.ports[0].host_port = 0;
        let binding = container_resource_binding(&request, "image-id", &created).unwrap();
        let resource = ResourceRecord {
            kind: ResourceKind::Container,
            logical_name: "container-1".to_owned(),
            path: None,
            intent: Some(request.intent_binding("image-id")),
            binding: Some(binding),
            state: ResourceState::Bound,
        };
        let clock = FakeClock::new();
        let docker = CleanupDocker {
            clock: clock.clone(),
            elapsed_per_call: Duration::from_secs(7),
            container: RefCell::new(Some(inspection)),
            network: RefCell::new(None),
            calls: RefCell::new(Vec::new()),
        };
        let runner = BoundedProcessRunner;
        let backend = NativeCleanupBackend {
            root: temporary.path(),
            docker: &docker,
            runner: &runner,
            clock,
            image_id: "image-id",
            network: None,
            containers: &[owned],
        };

        backend
            .remove_container(&resource, Duration::from_secs(180))
            .unwrap();

        assert_eq!(
            docker
                .calls
                .borrow()
                .iter()
                .map(|(_, _, timeout)| *timeout)
                .collect::<Vec<_>>(),
            [
                Duration::from_secs(30),
                Duration::from_secs(23),
                Duration::from_secs(16),
            ]
        );
    }

    #[test]
    fn bound_network_removal_uses_journaled_immutable_id() {
        let temporary = tempdir().unwrap();
        let request = NetworkRequest {
            name: "logical-network-name".to_owned(),
            labels: OwnedLabels::network("0123456789ab"),
        };
        let network = OwnedNetwork {
            request: request.clone(),
            id: None,
        };
        let resource = ResourceRecord {
            kind: ResourceKind::Network,
            logical_name: "docker-network".to_owned(),
            path: None,
            intent: Some(request.intent_binding()),
            binding: Some(ResourceBinding {
                immutable_id: "journaled-network-id".to_owned(),
                attributes_sha256: "attributes".to_owned(),
            }),
            state: ResourceState::Bound,
        };
        let clock = FakeClock::new();
        let docker = CleanupDocker {
            clock: clock.clone(),
            elapsed_per_call: Duration::ZERO,
            container: RefCell::new(None),
            network: RefCell::new(None),
            calls: RefCell::new(Vec::new()),
        };
        let runner = BoundedProcessRunner;
        let backend = NativeCleanupBackend {
            root: temporary.path(),
            docker: &docker,
            runner: &runner,
            clock,
            image_id: "image-id",
            network: Some(&network),
            containers: &[],
        };

        backend
            .remove_network(&resource, Duration::from_secs(180))
            .unwrap();

        assert_eq!(
            docker.calls.borrow().as_slice(),
            [(
                "remove-network".to_owned(),
                "journaled-network-id".to_owned(),
                Duration::from_secs(30),
            )]
        );
    }

    #[test]
    fn late_reconciled_network_removal_switches_from_name_to_journaled_id() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("fixture");
        let store = JournalStore::initialize(&root).unwrap();
        let mut journal = store.create(run(&root)).unwrap();
        let request = NetworkRequest {
            name: "logical-network-name".to_owned(),
            labels: OwnedLabels::network("0123456789ab"),
        };
        journal.resources.push(ResourceRecord {
            kind: ResourceKind::Network,
            logical_name: "docker-network".to_owned(),
            path: None,
            intent: Some(request.intent_binding()),
            binding: None,
            state: ResourceState::Dispatched,
        });
        store.save(&journal).unwrap();
        let network = OwnedNetwork {
            request: request.clone(),
            id: None,
        };
        let inspection = NetworkInspection {
            id: "late-network-id".to_owned(),
            name: request.name.clone(),
            driver: "bridge".to_owned(),
            labels: request.labels.as_map(),
            attached_container_ids: Vec::new(),
        };
        let clock = FakeClock::new();
        let docker = CleanupDocker {
            clock: clock.clone(),
            elapsed_per_call: Duration::ZERO,
            container: RefCell::new(None),
            network: RefCell::new(Some(inspection)),
            calls: RefCell::new(Vec::new()),
        };
        let runner = BoundedProcessRunner;
        let backend = NativeCleanupBackend {
            root: &root,
            docker: &docker,
            runner: &runner,
            clock: clock.clone(),
            image_id: "image-id",
            network: Some(&network),
            containers: &[],
        };

        let report =
            CleanupCoordinator::new(clock, CLEANUP_BUDGET).cleanup(&store, &mut journal, &backend);

        assert!(report.succeeded());
        assert_eq!(
            docker
                .calls
                .borrow()
                .iter()
                .map(|(operation, identity, _)| (operation.as_str(), identity.as_str()))
                .collect::<Vec<_>>(),
            [
                ("inspect-network", "logical-network-name"),
                ("remove-network", "late-network-id"),
                ("inspect-network", "late-network-id"),
            ]
        );
    }

    #[test]
    fn cleanup_budget_is_independent_after_setup_deadline_expires() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("fixture");
        let acknowledgement = temporary.path().join("acknowledgement.json");
        fs::write(
            &acknowledgement,
            r#"{"schema_version":1,"sql_server_eula":{"accepted":true}}"#,
        )
        .unwrap();
        let config = FixtureConfig::new(&root, acknowledgement).unwrap();
        let lock = acquire_root_lock(&root).unwrap();
        let store = JournalStore::initialize(&root).unwrap();
        let mut journal = store.create(run(&root)).unwrap();
        journal.resources.push(ResourceRecord {
            kind: ResourceKind::SecretFile,
            logical_name: "never-created".to_owned(),
            path: Some(root.join("never-created")),
            intent: None,
            binding: None,
            state: ResourceState::Intended,
        });
        store.save(&journal).unwrap();
        let runner = BoundedProcessRunner;
        let mut context = LaunchContext {
            config,
            _lock: lock,
            store,
            journal,
            docker: DockerCli::new(runner),
            runner,
            image_id: "unused".to_owned(),
            deadline: Instant::now(),
            private_files: Vec::new(),
            credentials: None,
            tls: None,
            network: None,
            containers: Vec::new(),
        };

        let report = context.cleanup_report();

        assert!(report.succeeded());
        assert_eq!(context.journal.state, RunState::Removed);
    }

    #[test]
    fn colliding_launcher_directory_is_retained_and_blocks_actual_cleanup() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("fixture");
        let acknowledgement = temporary.path().join("acknowledgement.json");
        fs::write(
            &acknowledgement,
            r#"{"schema_version":1,"sql_server_eula":{"accepted":true}}"#,
        )
        .unwrap();
        let config = FixtureConfig::new(&root, acknowledgement).unwrap();
        let lock = acquire_root_lock(&root).unwrap();
        let store = JournalStore::initialize(&root).unwrap();
        let journal = store.create(run(&root)).unwrap();
        let collision = root.join("credentials");
        fs::create_dir(&collision).unwrap();
        fs::set_permissions(&collision, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(collision.join("unowned"), "keep").unwrap();
        let runner = BoundedProcessRunner;
        let mut context = LaunchContext {
            config,
            _lock: lock,
            store,
            journal,
            docker: DockerCli::new(runner),
            runner,
            image_id: "unused".to_owned(),
            deadline: Instant::now() + Duration::from_secs(180),
            private_files: Vec::new(),
            credentials: None,
            tls: None,
            network: None,
            containers: Vec::new(),
        };
        let topology = context.journal.run.clone();
        assert!(context.generate_credentials(&topology).is_err());
        let report = context.cleanup_report();
        assert!(!report.succeeded());
        assert_eq!(context.journal.state, RunState::Blocked);
        assert_eq!(context.journal.resources[0].state, ResourceState::Blocked);
        assert_eq!(
            fs::read_to_string(collision.join("unowned")).unwrap(),
            "keep"
        );
    }

    #[test]
    fn replaced_launcher_directory_identity_is_revalidated_before_recursive_delete() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("fixture");
        let store = JournalStore::initialize(&root).unwrap();
        let mut journal = store.create(run(&root)).unwrap();
        let directory = root.join("observer");
        let mut private_files = Vec::new();
        let mut recorder = LaunchAssetRecorder {
            store: &store,
            journal: &mut journal,
            private_files: &mut private_files,
        };
        recorder
            .create_directory("observer-directory", &directory)
            .unwrap();
        fs::remove_dir_all(&directory).unwrap();
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(directory.join("replacement"), "keep").unwrap();

        let docker = DockerCli::new(BoundedProcessRunner);
        let runner = BoundedProcessRunner;
        let backend = NativeCleanupBackend {
            root: &root,
            docker: &docker,
            runner: &runner,
            clock: SystemCleanupClock::default(),
            image_id: "unused",
            network: None,
            containers: &[],
        };
        let report = CleanupCoordinator::default().cleanup(&store, &mut journal, &backend);
        assert!(!report.succeeded());
        assert_eq!(journal.resources[0].state, ResourceState::Blocked);
        assert_eq!(
            fs::read_to_string(directory.join("replacement")).unwrap(),
            "keep"
        );
    }

    #[test]
    fn lost_network_create_output_and_inspection_recovers_on_later_appearance() {
        let temporary = tempdir().unwrap();
        let root = temporary.path().join("fixture");
        let store = JournalStore::initialize(&root).unwrap();
        let mut journal = store.create(run(&root)).unwrap();
        let request = NetworkRequest {
            name: "km-three-0123456789ab".to_owned(),
            labels: OwnedLabels::network("0123456789ab"),
        };
        let index = store
            .record_intent(
                &mut journal,
                ResourceRecord {
                    kind: ResourceKind::Network,
                    logical_name: "docker-network".to_owned(),
                    path: None,
                    intent: Some(request.intent_binding()),
                    binding: None,
                    state: ResourceState::Intended,
                },
            )
            .unwrap();
        store.mark_dispatched(&mut journal, index).unwrap();
        let docker = LateNetworkDocker {
            inspection: RefCell::new(Err(DockerError::Command)),
            creates: Cell::new(0),
        };
        let owned = OwnedNetwork {
            request: request.clone(),
            id: None,
        };
        assert!(reconcile_network_create(&docker, &owned, Duration::from_secs(1)).is_err());
        assert_eq!(journal.resources[index].state, ResourceState::Dispatched);
        let inspection = NetworkInspection {
            id: "network-id".to_owned(),
            name: request.name.clone(),
            driver: "bridge".to_owned(),
            labels: request.labels.as_map(),
            attached_container_ids: Vec::new(),
        };
        *docker.inspection.borrow_mut() = Ok(Some(inspection));
        struct LateInspector<'a> {
            docker: &'a LateNetworkDocker,
            request: &'a NetworkRequest,
        }
        impl OwnershipInspector for LateInspector<'_> {
            fn inspect(&self, _: &ResourceRecord) -> Result<ResourceObservation, ReconcileError> {
                let inspection = self
                    .docker
                    .inspect_network(&self.request.name, Duration::from_secs(1))
                    .map_err(|_| ReconcileError::Io)?
                    .ok_or(ReconcileError::AmbiguousCreate)?;
                self.request
                    .verify_inspection(&inspection)
                    .map_err(|_| ReconcileError::OwnershipMismatch)?;
                Ok(ResourceObservation::Owned {
                    binding: network_resource_binding(self.request, &inspection)
                        .map_err(|_| ReconcileError::OwnershipMismatch)?,
                    foreign_attachments: Vec::new(),
                })
            }
        }
        let report = reconcile(
            &store,
            &mut journal,
            &LateInspector {
                docker: &docker,
                request: &request,
            },
        )
        .unwrap();
        assert_eq!(report.recovered, vec!["docker-network"]);
        assert_eq!(journal.resources[index].state, ResourceState::Bound);
        assert_eq!(
            journal.resources[index]
                .binding
                .as_ref()
                .unwrap()
                .immutable_id,
            "network-id"
        );
    }
}
