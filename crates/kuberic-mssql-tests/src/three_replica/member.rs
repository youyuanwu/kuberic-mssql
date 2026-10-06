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
use super::config::FixtureConfig;
use super::docker::{
    ContainerInspection, ContainerRequest, DockerApi, DockerCli, DockerError, NetworkRequest,
    OwnedLabels, SQL_SERVER_UID, SqlServerContainerSpec,
};
use super::model::{
    KubericMember, OwnershipJournal, ResourceBinding, ResourceKind, ResourceRecord, ResourceState,
    RunState, SqlMember, SqlMemberIncarnation, TopologyRun,
};
use super::ownership::{
    CommandAclController, DirectoryBinding, JournalStore, RootLock, acquire_root_lock,
    prepare_member_directory, verify_member_directory,
};
use super::preflight::{CommandAclProbe, LocalHostProbe, run_preflight};
use super::process::{BoundedProcessRunner, CommandSpec, ProcessRunner};
use super::secrets::{
    CredentialFiles, PrivateFile, SecretError, SecretValue, create_private_directory,
};
use super::tls::{TlsAssets, TlsError};

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

    pub fn cleanup(mut self) -> Result<CleanupEvidence, NativeLaunchError> {
        self.context.cleanup_exact()
    }
}

pub async fn launch_three_members(
    config: FixtureConfig,
) -> Result<LaunchedMembers, NativeLaunchError> {
    let mut context = LaunchContext::prepare(config)?;
    match context.launch().await {
        Ok((run, network_id, members)) => Ok(LaunchedMembers {
            context,
            run,
            network_id,
            members,
        }),
        Err(primary) => {
            let _ = context.cleanup_exact();
            Err(primary)
        }
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
    directories: Vec<DirectoryBinding>,
    private_files: Vec<PrivateFile>,
    credentials: Option<CredentialFiles>,
    tls: Option<TlsAssets>,
    network: Option<(usize, NetworkRequest, String)>,
    containers: Vec<(usize, ContainerRequest, String)>,
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
            directories: Vec::new(),
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
        let tls = TlsAssets::generate(
            self.store.root(),
            hostnames,
            data_directories,
            self.limit(self.config.deadlines().tls_helper)?,
            &self.runner,
        )
        .map_err(NativeLaunchError::Tls)?;
        self.record_tls_files(&tls)?;
        self.write_sql_configuration(&run)?;
        let network_name = format!("km-three-{}", run.run_id);
        let network_request = NetworkRequest {
            name: network_name.clone(),
            labels: OwnedLabels::network(&run.run_id),
        };
        let network_index = self.record_intent(ResourceKind::Network, "docker-network", None)?;
        self.store
            .mark_dispatched(&mut self.journal, network_index)
            .map_err(|_| NativeLaunchError::Journal)?;
        let network_id = self
            .docker
            .create_network(
                &network_request,
                self.limit(self.config.deadlines().docker_command)?,
            )
            .map_err(NativeLaunchError::Docker)?;
        let network_inspection = self
            .docker
            .inspect_network(
                &network_id,
                self.limit(self.config.deadlines().docker_command)?,
            )
            .map_err(NativeLaunchError::Docker)?
            .ok_or(NativeLaunchError::Ownership)?;
        network_request
            .verify_inspection(&network_inspection)
            .map_err(NativeLaunchError::Docker)?;
        self.store
            .bind(
                &mut self.journal,
                network_index,
                inspection_binding(&network_inspection)?,
            )
            .map_err(|_| NativeLaunchError::Journal)?;
        self.network = Some((network_index, network_request, network_id.clone()));

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
                let file = PrivateFile::create_text(
                    &environment_file,
                    request.environment_file_contents(),
                )
                .map_err(NativeLaunchError::Secret)?;
                self.record_bound_file("container-environment", &file)?;
                self.private_files.push(file);
            }
            let logical_name = format!("container-{}", member.ordinal);
            let record_index = self.record_intent(ResourceKind::Container, &logical_name, None)?;
            self.store
                .mark_dispatched(&mut self.journal, record_index)
                .map_err(|_| NativeLaunchError::Journal)?;
            let id = self
                .docker
                .create_container(
                    &request,
                    self.limit(self.config.deadlines().docker_command)?,
                )
                .map_err(NativeLaunchError::Docker)?;
            self.containers.push((record_index, request, id.clone()));
            let inspection = self.inspect_container(&id)?;
            self.containers[index]
                .1
                .verify_inspection(&inspection, &self.image_id, false)
                .map_err(NativeLaunchError::Docker)?;
            self.store
                .bind(
                    &mut self.journal,
                    record_index,
                    inspection_binding(&inspection)?,
                )
                .map_err(|_| NativeLaunchError::Journal)?;
        }
        self.verify_network_attachments(false)?;
        for index in 0..self.containers.len() {
            let id = self.containers[index].2.clone();
            self.docker
                .start_container(&id, self.limit(self.config.deadlines().docker_command)?)
                .map_err(NativeLaunchError::Docker)?;
            let inspection = self.inspect_container(&id)?;
            self.containers[index]
                .1
                .verify_inspection(&inspection, &self.image_id, true)
                .map_err(NativeLaunchError::Docker)?;
        }
        self.verify_network_attachments(true)?;

        let mut ready = Vec::with_capacity(3);
        let mut incarnations = Vec::with_capacity(3);
        for index in 0..3 {
            let inspection = self.inspect_container(&self.containers[index].2)?;
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
            self.directories.push(binding);
        }
        Ok(())
    }

    fn generate_credentials(
        &mut self,
        run: &TopologyRun,
    ) -> Result<CredentialFiles, NativeLaunchError> {
        let credentials = CredentialFiles::generate(self.store.root(), &run.run_id)
            .map_err(NativeLaunchError::Secret)?;
        for (index, file) in credentials.all().enumerate() {
            self.record_bound_file(&format!("credential-{index}"), file)?;
            self.private_files.push(file.clone());
        }
        Ok(credentials)
    }

    fn record_tls_files(&mut self, tls: &TlsAssets) -> Result<(), NativeLaunchError> {
        for (index, file) in tls.private_files().enumerate() {
            self.record_bound_file(&format!("tls-asset-{index}"), file)?;
            self.private_files.push(file.clone());
        }
        Ok(())
    }

    fn write_sql_configuration(&mut self, run: &TopologyRun) -> Result<(), NativeLaunchError> {
        for member in &run.members {
            let path = member.data_directory.join("mssql.conf");
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
            self.record_bound_file(&format!("member-{}-configuration", member.ordinal), &file)?;
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
        let deadlines = AdminDeadlines {
            connect: self.config.deadlines().sql_connect,
            query: self.config.deadlines().sql_batch,
        };
        let sa_login = LoginFiles {
            username: credentials.sa_username.path().to_path_buf(),
            password: credentials.sa_password.path().to_path_buf(),
        };
        let readiness_deadline =
            Instant::now() + self.limit(self.config.deadlines().member_readiness)?;
        let mut sa = loop {
            match AdminSession::connect(&endpoint, &sa_login, deadlines).await {
                Ok(mut session) => match session.readiness().await {
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
                },
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
        sa.bootstrap_admin(admin_username.expose(), &admin_password)
            .await
            .map_err(NativeLaunchError::Admin)?;
        sa.bootstrap_observer(observer_username.expose(), &observer_password)
            .await
            .map_err(NativeLaunchError::Admin)?;
        drop(sa);

        let admin_login = LoginFiles {
            username: credentials.admin_username.path().to_path_buf(),
            password: credentials.admin_password.path().to_path_buf(),
        };
        let mut admin = AdminSession::connect(&endpoint, &admin_login, deadlines)
            .await
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
        let mut observer = AdminSession::connect(&endpoint, &observer_login, deadlines)
            .await
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
        if !directory.exists() {
            create_private_directory(&directory).map_err(NativeLaunchError::Secret)?;
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
        let file = PrivateFile::create_bytes(&path, &bytes).map_err(NativeLaunchError::Secret)?;
        self.record_bound_file(&format!("observer-config-{}", index + 1), &file)?;
        self.private_files.push(file);
        Ok(path)
    }

    fn verify_network_attachments(&self, require_all: bool) -> Result<(), NativeLaunchError> {
        let (_, request, id) = self.network.as_ref().ok_or(NativeLaunchError::Ownership)?;
        let inspection = self
            .docker
            .inspect_network(id, self.limit(self.config.deadlines().docker_command)?)
            .map_err(NativeLaunchError::Docker)?
            .ok_or(NativeLaunchError::Ownership)?;
        request
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
            .map(|(_, _, id)| id.clone())
            .collect::<BTreeSet<_>>();
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
    ) -> Result<usize, NativeLaunchError> {
        self.store
            .record_intent(
                &mut self.journal,
                ResourceRecord {
                    kind,
                    logical_name: logical_name.to_owned(),
                    path,
                    binding: None,
                    state: ResourceState::Intended,
                },
            )
            .map_err(|_| NativeLaunchError::Journal)
    }

    fn record_bound_file(
        &mut self,
        logical_name: &str,
        file: &PrivateFile,
    ) -> Result<(), NativeLaunchError> {
        let index = self.record_intent(
            ResourceKind::SecretFile,
            logical_name,
            Some(file.path().to_path_buf()),
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| NativeLaunchError::Journal)?;
        self.store
            .bind(&mut self.journal, index, file.binding().clone())
            .map_err(|_| NativeLaunchError::Journal)
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

    fn cleanup_exact(&mut self) -> Result<CleanupEvidence, NativeLaunchError> {
        let mut removed = Vec::new();
        for (_, request, id) in self.containers.iter().rev() {
            if let Some(inspection) = self
                .docker
                .inspect_container(id, self.config.deadlines().docker_command)
                .map_err(NativeLaunchError::Docker)?
            {
                request
                    .verify_inspection(&inspection, &self.image_id, inspection.running)
                    .map_err(NativeLaunchError::Docker)?;
                if inspection.running {
                    self.docker
                        .stop_container(id, self.config.deadlines().docker_command)
                        .map_err(NativeLaunchError::Docker)?;
                }
                self.docker
                    .remove_container(id, self.config.deadlines().docker_command)
                    .map_err(NativeLaunchError::Docker)?;
                if self
                    .docker
                    .inspect_container(id, self.config.deadlines().docker_command)
                    .map_err(NativeLaunchError::Docker)?
                    .is_some()
                {
                    return Err(NativeLaunchError::Cleanup);
                }
            }
            removed.push(id.clone());
        }
        let mut removed_network_id = String::new();
        if let Some((_, network_request, network_id)) = &self.network {
            if let Some(inspection) = self
                .docker
                .inspect_network(network_id, self.config.deadlines().docker_command)
                .map_err(NativeLaunchError::Docker)?
            {
                network_request
                    .verify_inspection(&inspection)
                    .map_err(NativeLaunchError::Docker)?;
                if !inspection.attached_container_ids.is_empty() {
                    return Err(NativeLaunchError::Cleanup);
                }
                self.docker
                    .remove_network(network_id, self.config.deadlines().docker_command)
                    .map_err(NativeLaunchError::Docker)?;
                if self
                    .docker
                    .inspect_network(network_id, self.config.deadlines().docker_command)
                    .map_err(NativeLaunchError::Docker)?
                    .is_some()
                {
                    return Err(NativeLaunchError::Cleanup);
                }
            }
            removed_network_id.clone_from(network_id);
        }
        for file in &self.private_files {
            file.verify().map_err(|_| NativeLaunchError::Cleanup)?;
        }
        let host_uid = unsafe { libc::geteuid() };
        let acl = CommandAclController::new(self.runner);
        for directory in self.directories.iter().rev() {
            verify_member_directory(
                self.store.root(),
                directory,
                host_uid,
                SQL_SERVER_UID,
                self.config.deadlines().tls_helper,
                &acl,
            )
            .map_err(|_| NativeLaunchError::Cleanup)?;
            restore_host_cleanup_acl(
                &directory.canonical_path,
                host_uid,
                self.config.deadlines().tls_helper,
                &self.runner,
            )?;
            fs::remove_dir_all(&directory.canonical_path)
                .map_err(|_| NativeLaunchError::Cleanup)?;
        }
        for directory in ["observer", "endpoint-exchange", "tls", "credentials"] {
            let path = self.store.root().join(directory);
            if path.exists() {
                fs::remove_dir_all(path).map_err(|_| NativeLaunchError::Cleanup)?;
            }
        }
        for resource in &mut self.journal.resources {
            resource.state = ResourceState::Removed;
        }
        self.journal.state = RunState::Removed;
        self.store
            .save(&self.journal)
            .map_err(|_| NativeLaunchError::Journal)?;
        Ok(CleanupEvidence {
            removed_container_ids: removed,
            removed_network_id,
            journal_path: self.store.path().to_path_buf(),
        })
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

fn inspection_binding(value: &impl Serialize) -> Result<ResourceBinding, NativeLaunchError> {
    let bytes = serde_json::to_vec(value).map_err(|_| NativeLaunchError::Ownership)?;
    let digest = Sha256::digest(&bytes);
    let immutable_id = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| {
            value
                .get("id")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .ok_or(NativeLaunchError::Ownership)?;
    Ok(ResourceBinding {
        immutable_id,
        attributes_sha256: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    })
}
