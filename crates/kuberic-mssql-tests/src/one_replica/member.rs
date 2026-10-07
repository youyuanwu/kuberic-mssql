use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tiberius::ToSql;
use tokio::time::sleep;

use crate::fixture::admin::{
    AdminDeadlines, AdminEndpoint, AdminSession, LoginFiles, MemberReadinessEvidence,
    validated_identifier,
};
use crate::fixture::cleanup::{
    CleanupCoordinator, CleanupReport, SystemCleanupClock, combine_with_cleanup,
};
use crate::fixture::docker::{
    ContainerInspection, ContainerRequest, DockerApi, DockerCli, NetworkRequest, OwnedLabels,
    SQL_SERVER_UID, SqlServerContainerSpec,
};
use crate::fixture::model::{
    CombinedFixtureError, FailureCategory, FailureStage, ResourceKind, ResourceRecord,
    ResourceState, RunState, SanitizedFailure,
};
use crate::fixture::ownership::{
    CommandAclController, DirectoryBinding, PrivateDirectoryBinding, RootLock,
    acquire_fixture_root_lock, create_private_owned_directory, prepare_member_directory,
    process_incarnation_is_alive,
};
use crate::fixture::preflight::{CommandAclProbe, LocalHostProbe, run_preflight_with_deadline};
use crate::fixture::process::{BoundedProcessRunner, CommandSpec, ProcessRunner};
use crate::fixture::secrets::{PrivateFile, SecretValue};
use crate::fixture::tls::{SharedTlsAssets, TlsAssetRecorder, TlsError, TlsGenerationRequest};

use super::cleanup::{
    CleanupArtifacts, OneReplicaCleanupBackend, OneReplicaCleanupError, OneReplicaCleanupEvidence,
};
use super::config::OneReplicaConfig;
use super::legacy::detect_legacy_state;
use super::model::{OneReplicaJournal, OneReplicaJournalStore, OneReplicaMember, OneReplicaRun};

const FIXTURE_LABEL: &str = "one-replica";
const READINESS_RETRY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneReplicaFixtureFiles {
    pub ca_certificate: PathBuf,
    pub bad_ca_certificate: PathBuf,
    pub admin_username: PathBuf,
    pub admin_password: PathBuf,
    pub observer_username: PathBuf,
    pub observer_password: PathBuf,
    pub denied_username: PathBuf,
    pub denied_password: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyOneReplica {
    pub server_name: String,
    pub container_id: String,
    pub host_port: u16,
    pub sql_start_time: String,
    pub sql_start_unix_millis: i64,
    pub files: OneReplicaFixtureFiles,
}

pub struct LaunchedOneReplica {
    context: LaunchContext,
    pub run: OneReplicaRun,
    pub member: ReadyOneReplica,
}

impl LaunchedOneReplica {
    pub fn journal(&self) -> &OneReplicaJournal {
        &self.context.journal
    }

    pub fn cleanup(mut self) -> Result<OneReplicaCleanupEvidence, CombinedFixtureError> {
        let report = self.context.cleanup_report();
        combine_with_cleanup(Ok(()), &report)?;
        Ok(OneReplicaCleanupEvidence {
            journal_path: Some(self.context.store.path().to_path_buf()),
            report,
        })
    }

    pub fn block_for_recovery(&mut self) -> Result<(), OneReplicaCleanupError> {
        self.context
            .store
            .block_for_current_process(&mut self.context.journal)
            .map_err(|_| OneReplicaCleanupError::Journal)
    }
}

pub async fn launch_one_replica(
    config: OneReplicaConfig,
) -> Result<LaunchedOneReplica, CombinedFixtureError> {
    let mut context = LaunchContext::prepare(config)
        .map_err(|failure| CombinedFixtureError::new(failure, Vec::new()))?;
    match context.launch().await {
        Ok((run, member)) => Ok(LaunchedOneReplica {
            context,
            run,
            member,
        }),
        Err(error) => {
            let failure = error.sanitized();
            let report = context.cleanup_report();
            Err(combine_with_cleanup::<()>(Err(failure), &report).unwrap_err())
        }
    }
}

struct LaunchContext {
    config: OneReplicaConfig,
    _lock: RootLock,
    store: OneReplicaJournalStore,
    journal: OneReplicaJournal,
    runner: BoundedProcessRunner,
    docker: DockerCli<BoundedProcessRunner>,
    deadline: Instant,
    private_files: Vec<PrivateFile>,
    network_request: Option<NetworkRequest>,
    container_request: Option<ContainerRequest>,
    member_directory: Option<DirectoryBinding>,
}

impl LaunchContext {
    fn prepare(config: OneReplicaConfig) -> Result<Self, SanitizedFailure> {
        let runner = BoundedProcessRunner;
        let docker = DockerCli::new(runner);
        let lock = acquire_fixture_root_lock(config.root(), FIXTURE_LABEL)
            .map_err(|_| failure(FailureCategory::OwnershipMismatch))?;
        detect_legacy_state(
            config.root(),
            &docker,
            config.fixture().deadlines().docker_command,
        )
        .map_err(|error| {
            SanitizedFailure::with_detail(
                FailureStage::Setup,
                FailureCategory::OwnershipMismatch,
                error.to_string(),
            )
        })?;
        let store = OneReplicaJournalStore::initialize(config.root())
            .map_err(|_| failure(FailureCategory::Journal))?;
        if let Some(mut previous) = store
            .load()
            .map_err(|_| failure(FailureCategory::Journal))?
        {
            if previous.blocked_owner_unknown {
                return Err(failure(FailureCategory::OwnershipMismatch));
            }
            if previous
                .blocked_owner
                .is_some_and(|owner| process_incarnation_is_alive(owner).unwrap_or(true))
            {
                return Err(failure(FailureCategory::OwnershipMismatch));
            }
            let artifacts = CleanupArtifacts::from_journal(&config, &previous)
                .map_err(|_| failure(FailureCategory::OwnershipMismatch))?;
            let image_id = previous.run.image_id.clone();
            let backend = OneReplicaCleanupBackend {
                root: store.root(),
                docker: &docker,
                runner,
                image_id: &image_id,
                network: artifacts.network.as_ref(),
                container: artifacts.container.as_ref(),
            };
            let report = CleanupCoordinator::new(
                SystemCleanupClock::default(),
                config.fixture().deadlines().cleanup,
            )
            .cleanup_shared(&store, &mut previous, &backend);
            if !report.succeeded() {
                return Err(failure(FailureCategory::OwnershipMismatch));
            }
        }
        let deadline = Instant::now() + config.fixture().deadlines().complete_run;
        let preflight = run_preflight_with_deadline(
            config.fixture(),
            &LocalHostProbe,
            &CommandAclProbe::new(runner),
            &docker,
            deadline,
        )
        .map_err(|_| failure(FailureCategory::Preflight))?;
        let run = new_run(config.root(), preflight.image.id);
        let journal = store
            .create(run)
            .map_err(|_| failure(FailureCategory::Journal))?;
        Ok(Self {
            config,
            _lock: lock,
            store,
            journal,
            runner,
            docker,
            deadline,
            private_files: Vec::new(),
            network_request: None,
            container_request: None,
            member_directory: None,
        })
    }

    async fn launch(&mut self) -> Result<(OneReplicaRun, ReadyOneReplica), OneReplicaLaunchError> {
        let run = self.journal.run.clone();
        self.prepare_data_directory(&run)?;
        let credentials = self.generate_credentials(&run)?;
        let tls = self.generate_tls(&run)?;
        let bad_ca_certificate = self.generate_bad_ca()?;
        self.write_sql_configuration(&run)?;
        self.create_network(&run)?;
        let inspection = self.create_and_start_container(&run, &credentials)?;
        let host_port = inspection
            .ports
            .iter()
            .find(|port| port.container_port == 1433 && port.host_ip == "127.0.0.1")
            .map(|port| port.host_port)
            .ok_or(OneReplicaLaunchError::Ownership)?;
        let readiness = self
            .initialize_member(host_port, &run, &credentials, &tls)
            .await?;
        self.journal.state = RunState::Ready;
        self.store
            .save(&self.journal)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        Ok((
            run.clone(),
            ReadyOneReplica {
                server_name: run.member.server_name,
                container_id: inspection.id,
                host_port,
                sql_start_time: readiness.sql_start_time,
                sql_start_unix_millis: readiness.sql_start_unix_millis,
                files: OneReplicaFixtureFiles {
                    ca_certificate: tls.ca_certificate.path().to_path_buf(),
                    bad_ca_certificate: bad_ca_certificate.path().to_path_buf(),
                    admin_username: credentials.admin_username.path().to_path_buf(),
                    admin_password: credentials.admin_password.path().to_path_buf(),
                    observer_username: credentials.observer_username.path().to_path_buf(),
                    observer_password: credentials.observer_password.path().to_path_buf(),
                    denied_username: credentials.denied_username.path().to_path_buf(),
                    denied_password: credentials.denied_password.path().to_path_buf(),
                },
            },
        ))
    }

    fn prepare_data_directory(&mut self, run: &OneReplicaRun) -> Result<(), OneReplicaLaunchError> {
        let index = self.record_intent(
            ResourceKind::DataDirectory,
            "member-data-1",
            Some(run.member.data_directory.clone()),
            None,
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        let binding = prepare_member_directory(
            self.store.root(),
            &run.member.data_directory,
            unsafe { libc::geteuid() },
            SQL_SERVER_UID,
            self.limit(self.config.fixture().deadlines().tls_helper)?,
            &CommandAclController::new(self.runner),
        )
        .map_err(|_| OneReplicaLaunchError::DataDirectory)?;
        self.store
            .bind(&mut self.journal, index, binding.binding.clone())
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        probe_sql_uid_access(
            &run.member.data_directory,
            self.limit(self.config.fixture().deadlines().tls_helper)?,
            &self.runner,
        )?;
        self.member_directory = Some(binding);
        Ok(())
    }

    fn generate_credentials(
        &mut self,
        run: &OneReplicaRun,
    ) -> Result<OneReplicaCredentials, OneReplicaLaunchError> {
        let directory = self.store.root().join("credentials");
        self.create_recorded_directory("credentials-directory", &directory)?;
        let suffix = run.run_id.get(..8).ok_or(OneReplicaLaunchError::Secret)?;
        let sa_password =
            SecretValue::generate_password().map_err(|_| OneReplicaLaunchError::Secret)?;
        let admin_password =
            SecretValue::generate_password().map_err(|_| OneReplicaLaunchError::Secret)?;
        let observer_password =
            SecretValue::generate_password().map_err(|_| OneReplicaLaunchError::Secret)?;
        let denied_password =
            SecretValue::generate_password().map_err(|_| OneReplicaLaunchError::Secret)?;
        Ok(OneReplicaCredentials {
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
            denied_username: self.create_recorded_text_file(
                "credential-denied-username",
                &directory.join("denied-username"),
                format!("km_denied_{suffix}"),
                false,
            )?,
            denied_password: self.create_recorded_file(
                "credential-denied-password",
                &directory.join("denied-password"),
                &denied_password,
                false,
            )?,
        })
    }

    fn generate_tls(
        &mut self,
        run: &OneReplicaRun,
    ) -> Result<SharedTlsAssets, OneReplicaLaunchError> {
        let root = self.store.root().to_path_buf();
        let hostname = run.member.server_name.as_str();
        let data_directory = run.member.data_directory.as_path();
        let timeout = self.limit(self.config.fixture().deadlines().tls_helper)?;
        let runner = self.runner;
        let mut recorder = OneReplicaAssetRecorder {
            store: &self.store,
            journal: &mut self.journal,
            private_files: &mut self.private_files,
        };
        SharedTlsAssets::generate_recorded_with_clock(
            TlsGenerationRequest {
                root: &root,
                hostnames: &[hostname],
                member_data_directories: &[data_directory],
                ca_common_name: "Kuberic one replica test CA",
                timeout,
            },
            &runner,
            &mut recorder,
            &SystemCleanupClock::default(),
        )
        .map_err(|_| OneReplicaLaunchError::Tls)
    }

    fn generate_bad_ca(&mut self) -> Result<PrivateFile, OneReplicaLaunchError> {
        let directory = self.store.root().join("bad-tls");
        self.create_recorded_directory("bad-tls-directory", &directory)?;
        let key = directory.join("ca.key");
        let certificate = directory.join("ca.crt");
        let key_index = self.record_file_intent("bad-tls-private-key", &key)?;
        self.runner
            .run(
                &CommandSpec::new(
                    "openssl",
                    "generate untrusted TLS private key",
                    self.limit(self.config.fixture().deadlines().tls_helper)?,
                )
                .args([
                    "genpkey",
                    "-algorithm",
                    "RSA",
                    "-pkeyopt",
                    "rsa_keygen_bits:2048",
                    "-out",
                ])
                .arg(&key),
            )
            .map_err(|_| OneReplicaLaunchError::TlsHelper)?;
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600))
            .map_err(|_| OneReplicaLaunchError::TlsHelper)?;
        let key_file = PrivateFile::inspect(&key).map_err(|_| OneReplicaLaunchError::Secret)?;
        self.bind_file_record(key_index, &key_file)?;
        self.private_files.push(key_file);

        let cert_index = self.record_file_intent("bad-tls-certificate", &certificate)?;
        self.runner
            .run(
                &CommandSpec::new(
                    "openssl",
                    "generate untrusted TLS certificate",
                    self.limit(self.config.fixture().deadlines().tls_helper)?,
                )
                .args(["req", "-x509", "-new", "-sha256", "-days", "2", "-key"])
                .arg(&key)
                .args(["-subj", "/CN=Kuberic untrusted test CA", "-out"])
                .arg(&certificate),
            )
            .map_err(|_| OneReplicaLaunchError::TlsHelper)?;
        fs::set_permissions(&certificate, fs::Permissions::from_mode(0o600))
            .map_err(|_| OneReplicaLaunchError::TlsHelper)?;
        let certificate_file =
            PrivateFile::inspect(&certificate).map_err(|_| OneReplicaLaunchError::Secret)?;
        self.bind_file_record(cert_index, &certificate_file)?;
        self.private_files.push(certificate_file.clone());
        Ok(certificate_file)
    }

    fn write_sql_configuration(
        &mut self,
        run: &OneReplicaRun,
    ) -> Result<(), OneReplicaLaunchError> {
        let path = run.member.data_directory.join("mssql.conf");
        let index = self.record_file_intent("member-1-configuration", &path)?;
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
        .map_err(|_| OneReplicaLaunchError::Secret)?;
        self.runner
            .run(
                &CommandSpec::new(
                    "setfacl",
                    "configure SQL Server configuration ACL",
                    self.limit(self.config.fixture().deadlines().tls_helper)?,
                )
                .args(["-m", &format!("u:{SQL_SERVER_UID}:r,m:r")])
                .arg(&path),
            )
            .map_err(|_| OneReplicaLaunchError::DataDirectory)?;
        let file =
            PrivateFile::inspect_sql_shared(&path).map_err(|_| OneReplicaLaunchError::Secret)?;
        self.bind_file_record(index, &file)?;
        self.private_files.push(file);
        Ok(())
    }

    fn create_network(&mut self, run: &OneReplicaRun) -> Result<(), OneReplicaLaunchError> {
        let request = NetworkRequest {
            name: run.member.network_name.clone(),
            labels: OwnedLabels::network_for_fixture(FIXTURE_LABEL, run.run_id.clone()),
        };
        if self
            .docker
            .inspect_network(
                &request.name,
                self.limit(self.config.fixture().deadlines().docker_command)?,
            )
            .map_err(|_| OneReplicaLaunchError::Docker)?
            .is_some()
        {
            return Err(OneReplicaLaunchError::Ownership);
        }
        let index = self.record_intent(
            ResourceKind::Network,
            "docker-network",
            None,
            Some(request.intent_binding()),
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        let timeout = self.limit(self.config.fixture().deadlines().docker_command)?;
        let created = self.docker.create_network(&request, timeout);
        let identity = created.as_deref().unwrap_or(request.name.as_str());
        let inspection = self
            .docker
            .inspect_network(identity, timeout)
            .map_err(|_| OneReplicaLaunchError::Docker)?
            .ok_or(OneReplicaLaunchError::Docker)?;
        let binding = request
            .resource_binding(&inspection)
            .map_err(|_| OneReplicaLaunchError::Ownership)?;
        self.store
            .bind(&mut self.journal, index, binding)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        self.network_request = Some(request);
        Ok(())
    }

    fn create_and_start_container(
        &mut self,
        run: &OneReplicaRun,
        credentials: &OneReplicaCredentials,
    ) -> Result<ContainerInspection, OneReplicaLaunchError> {
        self.config
            .fixture()
            .authorization()
            .revalidate()
            .map_err(|_| OneReplicaLaunchError::Authorization)?;
        let password = credentials
            .sa_password
            .read_secret()
            .map_err(|_| OneReplicaLaunchError::Secret)?;
        let request = ContainerRequest::sql_server(
            SqlServerContainerSpec {
                name: run.member.container_name.clone(),
                hostname: run.member.server_name.clone(),
                network_name: run.member.network_name.clone(),
                data_directory: run.member.data_directory.clone(),
                environment_file: run.member.environment_file.clone(),
                sa_password: password,
            },
            OwnedLabels::container_for_fixture(FIXTURE_LABEL, run.run_id.clone(), 1),
            self.config.fixture().resources(),
            self.config
                .fixture()
                .authorization()
                .sql_server_environment(),
        )
        .map_err(|_| OneReplicaLaunchError::Docker)?;
        self.create_recorded_text_file(
            "container-environment",
            &run.member.environment_file,
            request.environment_file_contents(),
            false,
        )?;
        let directory = self
            .member_directory
            .as_ref()
            .ok_or(OneReplicaLaunchError::DataDirectory)?;
        request
            .verify_data_directory_binding(
                self.store.root(),
                directory,
                unsafe { libc::geteuid() },
                self.limit(self.config.fixture().deadlines().tls_helper)?,
                &CommandAclController::new(self.runner),
            )
            .map_err(|_| OneReplicaLaunchError::DataDirectory)?;
        if self
            .docker
            .inspect_container(
                &request.name,
                self.limit(self.config.fixture().deadlines().docker_command)?,
            )
            .map_err(|_| OneReplicaLaunchError::Docker)?
            .is_some()
        {
            return Err(OneReplicaLaunchError::Ownership);
        }
        let index = self.record_intent(
            ResourceKind::Container,
            "container-1",
            None,
            Some(request.intent_binding(&self.journal.run.image_id)),
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        let timeout = self.limit(self.config.fixture().deadlines().docker_command)?;
        let created = self.docker.create_container(&request, timeout);
        let identity = created.as_deref().unwrap_or(request.name.as_str());
        let inspection = self
            .docker
            .inspect_container(identity, timeout)
            .map_err(|_| OneReplicaLaunchError::Docker)?
            .ok_or(OneReplicaLaunchError::Docker)?;
        let binding = request
            .resource_binding(&self.journal.run.image_id, &inspection)
            .map_err(|_| OneReplicaLaunchError::Ownership)?;
        self.store
            .bind(&mut self.journal, index, binding)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        self.docker
            .start_container(&inspection.id, timeout)
            .map_err(|_| OneReplicaLaunchError::Docker)?;
        let running = self
            .docker
            .inspect_container(&inspection.id, timeout)
            .map_err(|_| OneReplicaLaunchError::Docker)?
            .ok_or(OneReplicaLaunchError::Docker)?;
        request
            .verify_inspection(&running, &self.journal.run.image_id, true)
            .map_err(|_| OneReplicaLaunchError::Ownership)?;
        self.container_request = Some(request);
        Ok(running)
    }

    async fn initialize_member(
        &self,
        port: u16,
        run: &OneReplicaRun,
        credentials: &OneReplicaCredentials,
        tls: &SharedTlsAssets,
    ) -> Result<MemberReadinessEvidence, OneReplicaLaunchError> {
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
            Instant::now() + self.limit(self.config.fixture().deadlines().member_readiness)?;
        let mut sa = loop {
            let deadlines = self.admin_deadlines(readiness_deadline)?;
            match AdminSession::connect(&endpoint, &sa_login, deadlines).await {
                Ok(mut session) => match session.readiness().await {
                    Ok(evidence) => {
                        evidence
                            .verify(&run.member.server_name)
                            .map_err(|_| OneReplicaLaunchError::Admin)?;
                        break session;
                    }
                    Err(_) if Instant::now() < readiness_deadline => {}
                    Err(_) => return Err(OneReplicaLaunchError::Admin),
                },
                Err(_) if Instant::now() < readiness_deadline => {}
                Err(_) => return Err(OneReplicaLaunchError::Admin),
            }
            if Instant::now() >= readiness_deadline {
                return Err(OneReplicaLaunchError::Deadline);
            }
            sleep(
                READINESS_RETRY.min(readiness_deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        };
        let admin_username = credentials
            .admin_username
            .read_secret()
            .map_err(|_| OneReplicaLaunchError::Secret)?;
        let admin_password = credentials
            .admin_password
            .read_secret()
            .map_err(|_| OneReplicaLaunchError::Secret)?;
        let observer_username = credentials
            .observer_username
            .read_secret()
            .map_err(|_| OneReplicaLaunchError::Secret)?;
        let observer_password = credentials
            .observer_password
            .read_secret()
            .map_err(|_| OneReplicaLaunchError::Secret)?;
        let denied_username = credentials
            .denied_username
            .read_secret()
            .map_err(|_| OneReplicaLaunchError::Secret)?;
        let denied_password = credentials
            .denied_password
            .read_secret()
            .map_err(|_| OneReplicaLaunchError::Secret)?;
        sa.bootstrap_admin(admin_username.expose(), &admin_password)
            .await
            .map_err(|_| OneReplicaLaunchError::Admin)?;
        sa.bootstrap_observer(observer_username.expose(), &observer_password)
            .await
            .map_err(|_| OneReplicaLaunchError::Admin)?;
        let denied_identifier = validated_identifier(denied_username.expose())
            .map_err(|_| OneReplicaLaunchError::Admin)?;
        let denied_batch = format!(
            "SET NOCOUNT ON;\n\
             IF SUSER_ID(@P1) IS NULL\n\
             BEGIN\n\
                 DECLARE @create nvarchar(max) = N'CREATE LOGIN ' + QUOTENAME(@P1) + \
                 N' WITH PASSWORD = ' + QUOTENAME(@P2, NCHAR(39)) + N', CHECK_POLICY = ON';\n\
                 EXEC (@create);\n\
             END;\n\
             GRANT VIEW SERVER STATE TO [{denied_identifier}];"
        );
        sa.execute(
            &denied_batch,
            &[
                &denied_username.expose() as &dyn ToSql,
                &denied_password.expose() as &dyn ToSql,
            ],
        )
        .await
        .map_err(|_| OneReplicaLaunchError::Admin)?;
        let evidence = sa
            .readiness()
            .await
            .map_err(|_| OneReplicaLaunchError::Admin)?;
        evidence
            .verify(&run.member.server_name)
            .map_err(|_| OneReplicaLaunchError::Admin)?;
        Ok(evidence)
    }

    fn record_intent(
        &mut self,
        kind: ResourceKind,
        logical_name: &str,
        path: Option<PathBuf>,
        intent: Option<crate::fixture::model::ResourceBinding>,
    ) -> Result<usize, OneReplicaLaunchError> {
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
            .map_err(|_| OneReplicaLaunchError::Journal)
    }

    fn create_recorded_directory(
        &mut self,
        logical_name: &str,
        path: &Path,
    ) -> Result<PrivateDirectoryBinding, OneReplicaLaunchError> {
        let index = self.record_intent(
            ResourceKind::Directory,
            logical_name,
            Some(path.to_path_buf()),
            None,
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        let directory = create_private_owned_directory(self.store.root(), path)
            .map_err(|_| OneReplicaLaunchError::DataDirectory)?;
        self.store
            .bind(&mut self.journal, index, directory.binding.clone())
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        Ok(directory)
    }

    fn record_file_intent(
        &mut self,
        logical_name: &str,
        path: &Path,
    ) -> Result<usize, OneReplicaLaunchError> {
        let index = self.record_intent(
            ResourceKind::SecretFile,
            logical_name,
            Some(path.to_path_buf()),
            None,
        )?;
        self.store
            .mark_dispatched(&mut self.journal, index)
            .map_err(|_| OneReplicaLaunchError::Journal)?;
        Ok(index)
    }

    fn bind_file_record(
        &mut self,
        index: usize,
        file: &PrivateFile,
    ) -> Result<(), OneReplicaLaunchError> {
        self.store
            .bind(&mut self.journal, index, file.binding().clone())
            .map_err(|_| OneReplicaLaunchError::Journal)
    }

    fn create_recorded_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        value: &SecretValue,
        sql_shared: bool,
    ) -> Result<PrivateFile, OneReplicaLaunchError> {
        self.create_recorded_bytes_file(logical_name, path, value.expose().as_bytes(), sql_shared)
    }

    fn create_recorded_text_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        value: impl AsRef<str>,
        sql_shared: bool,
    ) -> Result<PrivateFile, OneReplicaLaunchError> {
        self.create_recorded_bytes_file(logical_name, path, value.as_ref().as_bytes(), sql_shared)
    }

    fn create_recorded_bytes_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        value: &[u8],
        sql_shared: bool,
    ) -> Result<PrivateFile, OneReplicaLaunchError> {
        let index = self.record_file_intent(logical_name, path)?;
        let mut file =
            PrivateFile::create_bytes(path, value).map_err(|_| OneReplicaLaunchError::Secret)?;
        if sql_shared {
            file =
                PrivateFile::inspect_sql_shared(path).map_err(|_| OneReplicaLaunchError::Secret)?;
        }
        self.bind_file_record(index, &file)?;
        self.private_files.push(file.clone());
        Ok(file)
    }

    fn admin_deadlines(
        &self,
        readiness_deadline: Instant,
    ) -> Result<AdminDeadlines, OneReplicaLaunchError> {
        Ok(AdminDeadlines {
            connect: self.limit_until(
                self.config.fixture().deadlines().sql_connect,
                readiness_deadline,
            )?,
            query: self.limit_until(
                self.config.fixture().deadlines().sql_batch,
                readiness_deadline,
            )?,
        })
    }

    fn limit(&self, stage: Duration) -> Result<Duration, OneReplicaLaunchError> {
        let value = stage.min(self.deadline.saturating_duration_since(Instant::now()));
        if value.is_zero() {
            Err(OneReplicaLaunchError::Deadline)
        } else {
            Ok(value)
        }
    }

    fn limit_until(
        &self,
        stage: Duration,
        stage_deadline: Instant,
    ) -> Result<Duration, OneReplicaLaunchError> {
        let value = stage
            .min(self.deadline.saturating_duration_since(Instant::now()))
            .min(stage_deadline.saturating_duration_since(Instant::now()));
        if value.is_zero() {
            Err(OneReplicaLaunchError::Deadline)
        } else {
            Ok(value)
        }
    }

    fn cleanup_report(&mut self) -> CleanupReport {
        let artifacts = match CleanupArtifacts::from_journal(&self.config, &self.journal) {
            Ok(artifacts) => artifacts,
            Err(_) => {
                return CleanupReport {
                    removed: Vec::new(),
                    unresolved: self
                        .journal
                        .resources
                        .iter()
                        .filter(|resource| resource.state != ResourceState::Removed)
                        .map(|resource| resource.logical_name.clone())
                        .collect(),
                    errors: Vec::new(),
                };
            }
        };
        let image_id = self.journal.run.image_id.clone();
        let backend = OneReplicaCleanupBackend {
            root: self.store.root(),
            docker: &self.docker,
            runner: self.runner,
            image_id: &image_id,
            network: artifacts.network.as_ref(),
            container: artifacts.container.as_ref(),
        };
        CleanupCoordinator::new(
            SystemCleanupClock::default(),
            self.config.fixture().deadlines().cleanup,
        )
        .cleanup_shared(&self.store, &mut self.journal, &backend)
    }
}

struct OneReplicaCredentials {
    sa_username: PrivateFile,
    sa_password: PrivateFile,
    admin_username: PrivateFile,
    admin_password: PrivateFile,
    observer_username: PrivateFile,
    observer_password: PrivateFile,
    denied_username: PrivateFile,
    denied_password: PrivateFile,
}

struct OneReplicaAssetRecorder<'a> {
    store: &'a OneReplicaJournalStore,
    journal: &'a mut OneReplicaJournal,
    private_files: &'a mut Vec<PrivateFile>,
}

impl OneReplicaAssetRecorder<'_> {
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

impl TlsAssetRecorder for OneReplicaAssetRecorder<'_> {
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
            .bind(self.journal, index, directory.binding)
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

#[derive(Debug)]
enum OneReplicaLaunchError {
    Authorization,
    DataDirectory,
    Secret,
    Tls,
    TlsHelper,
    Docker,
    Ownership,
    Admin,
    Journal,
    Deadline,
}

impl OneReplicaLaunchError {
    fn sanitized(&self) -> SanitizedFailure {
        let category = match self {
            Self::Authorization => FailureCategory::Acknowledgement,
            Self::DataDirectory => FailureCategory::DataDirectory,
            Self::Secret => FailureCategory::Secret,
            Self::Tls | Self::TlsHelper => FailureCategory::Tls,
            Self::Docker => FailureCategory::ContainerCreation,
            Self::Ownership => FailureCategory::OwnershipMismatch,
            Self::Admin => FailureCategory::SqlUnavailable,
            Self::Journal => FailureCategory::Journal,
            Self::Deadline => FailureCategory::DeadlineExceeded,
        };
        failure(category)
    }
}

impl fmt::Display for OneReplicaLaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.sanitized().to_string())
    }
}

impl std::error::Error for OneReplicaLaunchError {}

fn failure(category: FailureCategory) -> SanitizedFailure {
    SanitizedFailure::new(FailureStage::Setup, category)
}

fn new_run(root: &Path, image_id: String) -> OneReplicaRun {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut digest = Sha256::new();
    digest.update(root.as_os_str().as_encoded_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(now.to_le_bytes());
    let id = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let run_id = id[..16].to_owned();
    OneReplicaRun {
        resource_uid: id[..32].to_owned(),
        image_id,
        member: OneReplicaMember {
            ordinal: 1,
            server_name: format!("km-one-{}", &run_id[..6]),
            container_name: format!("kuberic-mssql-one-{run_id}"),
            network_name: format!("km-one-{run_id}"),
            data_directory: root.join("member-1"),
            environment_file: root.join("credentials/container.env"),
        },
        run_id,
    }
}

fn probe_sql_uid_access(
    data_directory: &Path,
    timeout: Duration,
    runner: &impl ProcessRunner,
) -> Result<(), OneReplicaLaunchError> {
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
        .map_err(|_| OneReplicaLaunchError::DataDirectory)?;
    fs::remove_dir_all(&probe).map_err(|_| OneReplicaLaunchError::DataDirectory)
}
