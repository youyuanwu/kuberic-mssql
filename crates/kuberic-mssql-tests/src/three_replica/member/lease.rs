use std::fmt;
use std::time::{Duration, Instant};

use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, Guid, OpaqueId, ReplicaIdentity,
    SqlServerStartIncarnation,
    lease_fence::{FenceReceipt, FenceRequest, LeaseAuthority, LeaseFenceError},
};
use kuberic_runtime::protocol::types::Epoch;

use super::{LaunchedMembers, NativeLaunchError, NativePhaseError, ReadyMember};
use crate::fixture::admin::{AdminError, required_text, single_row, validated_identifier};
use crate::fixture::docker::{ContainerInspection, ContainerRequest, DockerApi, DockerError};
use crate::fixture::ownership::JournalStore;
use crate::three_replica::availability_group::{
    ProvisionContext, ProvisionedAvailabilityGroup, availability_group_name, database_name,
    provision_with_lease,
};
use crate::three_replica::model::{LifecycleExclusion, LifecyclePhase, OwnershipJournal, RunState};

#[derive(Debug)]
pub enum FixtureFenceError {
    Ownership,
    Lifecycle,
    Deadline,
    Admin(AdminError),
    Docker(DockerError),
    Contract(LeaseFenceError),
    Launch(NativeLaunchError),
}

impl fmt::Display for FixtureFenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ownership => write!(f, "fixture fence ownership or incarnation mismatch"),
            Self::Lifecycle => write!(f, "fixture lifecycle operation remains unresolved"),
            Self::Deadline => write!(f, "fixture fence deadline exceeded"),
            Self::Admin(error) => write!(f, "fixture fence SQL observation failed: {error}"),
            Self::Docker(error) => write!(f, "fixture fence Docker operation failed: {error}"),
            Self::Contract(error) => write!(f, "fixture fence contract rejected: {error}"),
            Self::Launch(error) => write!(f, "fixture fence lifecycle failed: {error}"),
        }
    }
}

impl std::error::Error for FixtureFenceError {}

impl From<AdminError> for FixtureFenceError {
    fn from(error: AdminError) -> Self {
        Self::Admin(error)
    }
}
impl From<DockerError> for FixtureFenceError {
    fn from(error: DockerError) -> Self {
        Self::Docker(error)
    }
}
impl From<LeaseFenceError> for FixtureFenceError {
    fn from(error: LeaseFenceError) -> Self {
        Self::Contract(error)
    }
}
impl From<NativeLaunchError> for FixtureFenceError {
    fn from(error: NativeLaunchError) -> Self {
        Self::Launch(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseObservation {
    pub group_id: String,
    pub replica_id: String,
    pub sql_start: String,
    pub role: String,
    pub operational_state: Option<String>,
    /// Unsupported native telemetry, retained raw without a tick-unit conversion.
    pub remaining_ticks: Option<i64>,
    pub connected_replicas: i32,
    pub synchronized_databases: i32,
}

#[derive(Clone, PartialEq, Eq)]
pub struct OwnedFenceRequest {
    request: FenceRequest,
    index: usize,
    run_id: String,
    inspection: ContainerInspection,
}

impl fmt::Debug for OwnedFenceRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedFenceRequest")
            .field("request", &self.request)
            .field("index", &self.index)
            .field("run_id", &self.run_id)
            .field("container_id", &self.inspection.id)
            .field("started_at", &self.inspection.started_at)
            .finish()
    }
}

impl OwnedFenceRequest {
    pub fn request(&self) -> &FenceRequest {
        &self.request
    }
}

pub(super) fn restart_owned_member<D: DockerApi>(
    docker: &D,
    member: &mut ReadyMember,
    store: &JournalStore,
    journal: &mut OwnershipJournal,
    deadline: Instant,
    command_timeout: Duration,
) -> Result<u16, NativeLaunchError> {
    let id = &member.container_id;
    if journal.state == RunState::Removed
        || journal
            .lifecycle_exclusion
            .as_ref()
            .is_some_and(|exclusion| !exclusion.permits_restart(id))
    {
        return Err(NativeLaunchError::PriorRunUnresolved);
    }
    let remaining = || {
        deadline
            .checked_duration_since(Instant::now())
            .map(|left| left.min(command_timeout))
            .filter(|left| !left.is_zero())
            .ok_or(NativeLaunchError::Deadline)
    };
    let mut record = |phase| {
        journal.lifecycle_exclusion = Some(LifecycleExclusion {
            container_id: id.clone(),
            phase,
        });
        store.save(journal).map_err(|_| NativeLaunchError::Journal)
    };
    record(LifecyclePhase::StopPending)?;
    docker
        .stop_container(id, remaining()?)
        .map_err(NativeLaunchError::Docker)?;
    record(LifecyclePhase::Stopped)?;
    record(LifecyclePhase::StartPending)?;
    docker
        .start_container(id, remaining()?)
        .map_err(NativeLaunchError::Docker)?;
    let inspection = docker
        .inspect_container(id, remaining()?)
        .map_err(NativeLaunchError::Docker)?
        .ok_or(NativeLaunchError::Ownership)?;
    if !inspection.running || &inspection.id != id || inspection.started_at.is_empty() {
        return Err(NativeLaunchError::Readiness);
    }
    let port = inspection
        .ports
        .iter()
        .find(|port| {
            port.container_port == 1433 && port.host_ip == "127.0.0.1" && port.host_port != 0
        })
        .map(|port| port.host_port)
        .ok_or(NativeLaunchError::Readiness)?;
    remaining()?;
    member.host_port = port;
    journal.lifecycle_exclusion = None;
    store
        .save(journal)
        .map_err(|_| NativeLaunchError::Journal)?;
    Ok(port)
}

impl LaunchedMembers {
    pub async fn provision_write_lease_group_for_test(
        &mut self,
        seconds: u32,
    ) -> Result<(ProvisionedAvailabilityGroup, Instant), NativePhaseError> {
        let verifier = super::LaunchFrozenMemberVerifier {
            docker: &self.context.docker,
            containers: &self.context.containers,
            image_id: &self.context.image_id,
        };
        provision_with_lease(
            ProvisionContext {
                run: &self.run,
                members: &self.members,
                credentials: self
                    .context
                    .credentials
                    .as_ref()
                    .ok_or(NativePhaseError::Unavailable)?,
                tls: self
                    .context
                    .tls
                    .as_ref()
                    .ok_or(NativePhaseError::Unavailable)?,
                deadlines: self.context.config.deadlines(),
                complete_deadline: self.context.deadline,
                store: &self.context.store,
                journal: &mut self.context.journal,
                runner: &self.context.runner,
                frozen_member_verifier: &verifier,
            },
            Some(seconds),
        )
        .await
        .map_err(NativePhaseError::AvailabilityGroup)
    }

    pub async fn observe_write_lease_for_test(
        &self,
        index: usize,
    ) -> Result<LeaseObservation, AdminError> {
        let group = availability_group_name(&self.run);
        let mut session = self.testing_admin_session(index).await?;
        let readiness = session.readiness().await?;
        readiness.verify(
            &self
                .members
                .get(index)
                .ok_or(AdminError::WrongIdentity)?
                .server_name,
        )?;
        let row = single_row(
            session
                .query_rows(
                    "SELECT CONVERT(nvarchar(36), s.group_id), \
             CONVERT(nvarchar(36), s.replica_id), s.role_desc, s.operational_state_desc, \
             s.write_lease_remaining_ticks, \
             (SELECT COUNT(*) FROM sys.dm_hadr_availability_replica_states AS peers \
              WHERE peers.group_id = s.group_id AND peers.connected_state_desc = N'CONNECTED'), \
             (SELECT COUNT(*) FROM sys.dm_hadr_database_replica_states AS databases \
              WHERE databases.group_id = s.group_id AND databases.synchronization_state_desc = N'SYNCHRONIZED') \
             FROM sys.dm_hadr_availability_replica_states AS s \
             JOIN sys.availability_groups AS g ON g.group_id = s.group_id \
             WHERE s.is_local = 1 AND g.name = @P1;",
                    &[&group],
                )
                .await?,
        )?;
        Ok(LeaseObservation {
            group_id: required_text(&row, 0)?,
            replica_id: required_text(&row, 1)?,
            sql_start: readiness.sql_start_time,
            role: required_text(&row, 2)?,
            operational_state: row.get::<&str, _>(3).map(str::to_owned),
            remaining_ticks: row.get::<i64, _>(4),
            connected_replicas: row.get::<i32, _>(5).ok_or(AdminError::MalformedResult)?,
            synchronized_databases: row.get::<i32, _>(6).ok_or(AdminError::MalformedResult)?,
        })
    }

    pub async fn renew_write_lease_for_test(
        &self,
        index: usize,
        seconds: u32,
    ) -> Result<(), AdminError> {
        let group = availability_group_name(&self.run);
        let batch = renewal_batch(&group, seconds)?;
        self.testing_admin_session(index)
            .await?
            .execute_diagnostic(&batch, &[])
            .await
    }

    pub async fn cancel_write_lease_request_for_test(
        &self,
        index: usize,
        seconds: u32,
    ) -> Result<(), AdminError> {
        let group = availability_group_name(&self.run);
        let renewal = renewal_batch(&group, seconds)?;
        let mut session = self.testing_admin_session(index).await?;
        session.set_query_timeout(Duration::from_millis(100))?;
        let result = session
            .execute_diagnostic(&format!("WAITFOR DELAY '00:00:02'; {renewal}"), &[])
            .await;
        drop(session);
        result
    }

    pub async fn probe_lease_write_for_test(&self, index: usize) -> Result<(), AdminError> {
        let database = database_name(&self.run);
        validated_identifier(&database)?;
        let mut session = self.testing_admin_session(index).await?;
        session.set_query_timeout(Duration::from_secs(3))?;
        let marker = required_text(
            &single_row(
                session
                    .query_rows_diagnostic("SELECT CONVERT(nvarchar(36), NEWID());", &[])
                    .await?,
            )?,
            0,
        )?;
        session
            .execute_diagnostic(
                &format!(
                    "SET NOCOUNT ON; SET LOCK_TIMEOUT 1000; SET XACT_ABORT ON; \
                 INSERT INTO [{database}].dbo.kuberic_marker(id, value) \
                 VALUES (CONVERT(uniqueidentifier, @P1), N'lease-experiment');"
                ),
                &[&marker],
            )
            .await?;
        drop(session);
        let mut readback = self.testing_admin_session(index).await?;
        readback.set_query_timeout(Duration::from_secs(3))?;
        let rows = readback.query_rows_diagnostic(
            &format!("SELECT value FROM [{database}].dbo.kuberic_marker WHERE id = CONVERT(uniqueidentifier, @P1);"),
            &[&marker],
        ).await?;
        if rows.is_empty() {
            return Err(AdminError::WriteUnconfirmed);
        }
        let row = single_row(rows)?;
        if required_text(&row, 0)? != "lease-experiment" {
            return Err(AdminError::MalformedResult);
        }
        Ok(())
    }

    fn inspect_fence_target(&self, index: usize) -> Result<ContainerInspection, FixtureFenceError> {
        if self
            .context
            .journal
            .lifecycle_exclusion
            .as_ref()
            .is_some_and(|exclusion| exclusion.phase != LifecyclePhase::Stopped)
        {
            return Err(FixtureFenceError::Lifecycle);
        }
        let member = self
            .members
            .get(index)
            .ok_or(FixtureFenceError::Ownership)?;
        let owned = self
            .context
            .containers
            .get(index)
            .ok_or(FixtureFenceError::Ownership)?;
        if owned.id.as_deref() != Some(member.container_id.as_str()) {
            return Err(FixtureFenceError::Ownership);
        }
        let inspection = self
            .context
            .docker
            .inspect_container(
                &member.container_id,
                self.context
                    .limit(self.context.config.deadlines().docker_command)?,
            )?
            .ok_or(FixtureFenceError::Ownership)?;
        owned
            .request
            .verify_inspection(&inspection, &self.context.image_id, true)?;
        if inspection.id != member.container_id || inspection.started_at.is_empty() {
            return Err(FixtureFenceError::Ownership);
        }
        Ok(inspection)
    }

    async fn fence_authority(
        &self,
        index: usize,
        inspection: &ContainerInspection,
    ) -> Result<LeaseAuthority, FixtureFenceError> {
        let observed = self.observe_write_lease_for_test(index).await?;
        let invalid = |_| FixtureFenceError::Ownership;
        LeaseAuthority::new(
            self.run.resource_uid.clone(),
            self.run.run_id.clone(),
            AvailabilityGroupIdentity {
                name: AvailabilityGroupName::new(availability_group_name(&self.run))
                    .map_err(invalid)?,
                group_id: Guid::parse("fixture AG", observed.group_id).map_err(invalid)?,
            },
            Epoch::new(1, 1),
            ReplicaIdentity::observed(
                self.members[index].ordinal.to_string(),
                Guid::parse("fixture replica", observed.replica_id).map_err(invalid)?,
                format!("{}@{}", inspection.id, inspection.started_at),
            )
            .map_err(invalid)?,
            SqlServerStartIncarnation::new(observed.sql_start)
                .map_err(|_| FixtureFenceError::Ownership)?,
        )
        .map_err(FixtureFenceError::Contract)
    }

    pub async fn capture_fence_request_for_test(
        &self,
        index: usize,
        operation_id: OpaqueId,
    ) -> Result<OwnedFenceRequest, FixtureFenceError> {
        let inspection = self.inspect_fence_target(index)?;
        let authority = self.fence_authority(index, &inspection).await?;
        Ok(OwnedFenceRequest {
            request: FenceRequest::new(operation_id, authority),
            index,
            run_id: self.run.run_id.clone(),
            inspection,
        })
    }

    pub async fn fence_owned_incarnation_for_test(
        &mut self,
        request: &OwnedFenceRequest,
    ) -> Result<FenceReceipt, FixtureFenceError> {
        if request.run_id != self.run.run_id {
            return Err(FixtureFenceError::Ownership);
        }
        let current = self.inspect_fence_target(request.index)?;
        if current != request.inspection {
            return Err(FixtureFenceError::Ownership);
        }
        let authority = self.fence_authority(request.index, &current).await?;
        request.request.authority().matches(&authority)?;
        let timeout = self
            .context
            .limit(self.context.config.deadlines().docker_command)?;
        let container_request = &self.context.containers[request.index].request;
        let store = &self.context.store;
        let journal = &mut self.context.journal;
        stop_exact_incarnation(
            &self.context.docker,
            container_request,
            &current,
            timeout,
            |phase| {
                journal.lifecycle_exclusion = Some(LifecycleExclusion {
                    container_id: current.id.clone(),
                    phase,
                });
                store
                    .save(journal)
                    .map_err(|_| FixtureFenceError::Lifecycle)
            },
        )?;
        FenceReceipt::from_verified_stop(
            request.request.clone(),
            &authority,
            OpaqueId::new("fence provider", "rust-owned-container")
                .map_err(|_| FixtureFenceError::Ownership)?,
        )
        .map_err(FixtureFenceError::Contract)
    }
}

fn renewal_batch(group: &str, seconds: u32) -> Result<String, AdminError> {
    validated_identifier(group)?;
    if seconds == 0 {
        return Err(AdminError::InvalidDeadline);
    }
    Ok(format!(
        "ALTER AVAILABILITY GROUP [{group}] SET (WRITE_LEASE_VALIDITY = {seconds});"
    ))
}

fn stop_exact_incarnation<D: DockerApi>(
    docker: &D,
    request: &ContainerRequest,
    expected: &ContainerInspection,
    timeout: Duration,
    mut persist: impl FnMut(LifecyclePhase) -> Result<(), FixtureFenceError>,
) -> Result<(), FixtureFenceError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(FixtureFenceError::Deadline)?;
    let remaining = || {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|value| !value.is_zero())
            .ok_or(FixtureFenceError::Deadline)
    };
    let before = docker
        .inspect_container(&expected.id, remaining()?)?
        .ok_or(FixtureFenceError::Ownership)?;
    request.verify_inspection(&before, &expected.image_id, true)?;
    if &before != expected || before.started_at.is_empty() {
        return Err(FixtureFenceError::Ownership);
    }
    persist(LifecyclePhase::StopPending)?;
    docker.stop_container(&expected.id, remaining()?)?;
    let after = docker
        .inspect_container(&expected.id, remaining()?)?
        .ok_or(FixtureFenceError::Ownership)?;
    if after.running {
        return Err(FixtureFenceError::Ownership);
    }
    let mut normalized = after;
    normalized.running = before.running;
    normalized.ports = before.ports.clone();
    if normalized != before {
        return Err(FixtureFenceError::Ownership);
    }
    remaining()?;
    persist(LifecyclePhase::Stopped)?;
    remaining()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{
        config::ResourcePolicy,
        docker::{
            ContainerLimits, ContainerMount, ContainerPort, DockerCapabilities, ImageInspection,
            NetworkInspection, NetworkRequest, OwnedLabels, SqlServerContainerSpec,
        },
        secrets::SecretValue,
    };
    use std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
    };

    struct ScriptedDocker {
        inspections: RefCell<VecDeque<Result<Option<ContainerInspection>, DockerError>>>,
        stops: Cell<usize>,
        starts: Cell<usize>,
        stop_error: bool,
        native_running: Arc<AtomicBool>,
        stop_gate: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
        current: RefCell<Option<ContainerInspection>>,
        removed_ids: RefCell<Vec<String>>,
    }

    impl DockerApi for ScriptedDocker {
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
            _: &str,
            _: Duration,
        ) -> Result<Option<ContainerInspection>, DockerError> {
            self.inspections
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(self.current.borrow().clone().map(|mut current| {
                        current.running = self.native_running.load(Ordering::SeqCst);
                        current
                    }))
                })
        }
        fn create_container(
            &self,
            _: &ContainerRequest,
            _: Duration,
        ) -> Result<String, DockerError> {
            unreachable!()
        }
        fn start_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            self.starts.set(self.starts.get() + 1);
            Ok(())
        }
        fn stop_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
            self.stops.set(self.stops.get() + 1);
            if let Some((dispatched, cancelled)) = &self.stop_gate {
                dispatched.send(()).map_err(|_| DockerError::Command)?;
                cancelled
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| DockerError::Command)?;
            }
            if self.stop_error {
                Err(DockerError::Command)
            } else {
                self.native_running.store(false, Ordering::SeqCst);
                Ok(())
            }
        }
        fn remove_container(&self, id: &str, _: Duration) -> Result<(), DockerError> {
            if self
                .current
                .borrow()
                .as_ref()
                .is_none_or(|current| current.id != id)
            {
                return Err(DockerError::OwnershipMismatch);
            }
            self.current.replace(None);
            self.removed_ids.borrow_mut().push(id.to_owned());
            Ok(())
        }
    }

    fn target(root: &std::path::Path) -> (ContainerRequest, ContainerInspection) {
        let request = ContainerRequest::sql_server(
            SqlServerContainerSpec {
                name: "fixture-member".to_owned(),
                hostname: "km0123456789n1".to_owned(),
                network_name: "fixture-network".to_owned(),
                data_directory: root.to_path_buf(),
                environment_file: root.join("environment"),
                sa_password: SecretValue::from_test("Password-Aa1!"),
            },
            OwnedLabels::container("0123456789ab", 1),
            ResourcePolicy::default(),
            [("ACCEPT_EULA", "Y")],
        )
        .unwrap();
        let inspection = ContainerInspection {
            id: "container".to_owned(),
            name: request.name.clone(),
            hostname: request.hostname.clone(),
            image_id: "image".to_owned(),
            labels: request.labels.as_map(),
            environment: request
                .environment_file_contents()
                .lines()
                .map(str::to_owned)
                .collect(),
            user: "mssql".to_owned(),
            running: true,
            started_at: "start-1".to_owned(),
            restart_policy: "no".to_owned(),
            network_mode: request.network_name.clone(),
            network_names: vec![request.network_name.clone()],
            mounts: vec![ContainerMount {
                source: root.to_path_buf(),
                destination: "/var/opt/mssql".into(),
                read_only: false,
            }],
            ports: vec![ContainerPort {
                container_port: 1433,
                host_ip: "127.0.0.1".to_owned(),
                host_port: 14330,
            }],
            limits: ContainerLimits::from_policy(ResourcePolicy::default()),
        };
        (request, inspection)
    }

    fn docker(
        inspections: Vec<Result<Option<ContainerInspection>, DockerError>>,
        stop_error: bool,
    ) -> ScriptedDocker {
        ScriptedDocker {
            inspections: RefCell::new(inspections.into()),
            stops: Cell::new(0),
            starts: Cell::new(0),
            stop_error,
            native_running: Arc::new(AtomicBool::new(true)),
            stop_gate: None,
            current: RefCell::new(None),
            removed_ids: RefCell::new(Vec::new()),
        }
    }

    #[test]
    fn exact_stop_requires_both_verified_identity_and_stopped_postcondition() {
        let root = tempfile::tempdir().unwrap();
        let (request, expected) = target(root.path());
        let mut stopped = expected.clone();
        stopped.running = false;
        stopped.ports.clear();
        let docker = docker(vec![Ok(Some(expected.clone())), Ok(Some(stopped))], false);
        let mut phases = Vec::new();
        stop_exact_incarnation(
            &docker,
            &request,
            &expected,
            Duration::from_secs(1),
            |phase| {
                phases.push(phase);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            phases,
            [LifecyclePhase::StopPending, LifecyclePhase::Stopped]
        );
        assert_eq!(docker.stops.get(), 1);
    }

    #[test]
    fn stale_or_foreign_target_is_rejected_before_any_stop() {
        let root = tempfile::tempdir().unwrap();
        let (request, expected) = target(root.path());
        for field in 0..6 {
            let mut changed = expected.clone();
            match field {
                0 => changed.id = "replacement".to_owned(),
                1 => changed.started_at = "start-2".to_owned(),
                2 => changed.labels.clear(),
                3 => changed.image_id = "other-image".to_owned(),
                4 => changed.mounts.clear(),
                _ => changed.started_at.clear(),
            }
            let docker = docker(vec![Ok(Some(changed))], false);
            let mut persisted = false;
            assert!(
                stop_exact_incarnation(
                    &docker,
                    &request,
                    &expected,
                    Duration::from_secs(1),
                    |_| {
                        persisted = true;
                        Ok(())
                    }
                )
                .is_err()
            );
            assert_eq!(docker.stops.get(), 0);
            assert!(!persisted);
        }
    }

    #[test]
    fn stop_or_postcondition_uncertainty_retains_exclusion_and_blocks_restart() {
        let root = tempfile::tempdir().unwrap();
        let (request, expected) = target(root.path());
        for case in 0..5 {
            let mut changed = expected.clone();
            changed.running = false;
            if case == 3 {
                changed.started_at = "replacement".to_owned();
            }
            let after = match case {
                1 => Err(DockerError::Command),
                2 => Ok(Some(expected.clone())),
                3 => Ok(Some(changed)),
                _ => Ok(None),
            };
            let docker = docker(vec![Ok(Some(expected.clone())), after], case == 0);
            let mut exclusion = None;
            let result = stop_exact_incarnation(
                &docker,
                &request,
                &expected,
                Duration::from_secs(1),
                |phase| {
                    exclusion = Some(LifecycleExclusion {
                        container_id: expected.id.clone(),
                        phase,
                    });
                    Ok(())
                },
            );
            assert!(result.is_err());
            let pending = exclusion.unwrap();
            assert_eq!(pending.phase, LifecyclePhase::StopPending);
            // Re-entry reloads the durable exclusion, not an empty owner state.
            let restored: LifecycleExclusion =
                serde_json::from_slice(&serde_json::to_vec(&pending).unwrap()).unwrap();
            if restored.permits_restart(&expected.id) {
                docker
                    .start_container(&expected.id, Duration::from_secs(1))
                    .unwrap();
            }
            assert_eq!(docker.starts.get(), 0);
            // A late daemon stop can still happen; no replacement was started.
            docker.native_running.store(false, Ordering::SeqCst);
            assert!(!docker.native_running.load(Ordering::SeqCst));
            assert_eq!(docker.stops.get(), 1);
        }
    }

    #[test]
    fn persistence_failure_prevents_dispatch_and_pending_start_never_permits_restart() {
        let root = tempfile::tempdir().unwrap();
        let (request, expected) = target(root.path());
        let docker = docker(vec![Ok(Some(expected.clone()))], false);
        assert!(
            stop_exact_incarnation(&docker, &request, &expected, Duration::from_secs(1), |_| {
                Err(FixtureFenceError::Lifecycle)
            })
            .is_err()
        );
        assert_eq!(docker.stops.get(), 0);
        for phase in [LifecyclePhase::StopPending, LifecyclePhase::StartPending] {
            assert!(
                !LifecycleExclusion {
                    container_id: expected.id.clone(),
                    phase
                }
                .permits_restart(&expected.id)
            );
        }
    }

    #[test]
    fn preinspection_failure_or_exhausted_budget_cannot_dispatch_a_stop() {
        let root = tempfile::tempdir().unwrap();
        let (request, expected) = target(root.path());
        for inspection in [Err(DockerError::Command), Ok(None)] {
            let docker = docker(vec![inspection], false);
            assert!(
                stop_exact_incarnation(
                    &docker,
                    &request,
                    &expected,
                    Duration::from_secs(1),
                    |_| Ok(())
                )
                .is_err()
            );
            assert_eq!(docker.stops.get(), 0);
        }
        let docker = docker(Vec::new(), false);
        assert!(matches!(
            stop_exact_incarnation(&docker, &request, &expected, Duration::ZERO, |_| Ok(())),
            Err(FixtureFenceError::Deadline)
        ));
        assert_eq!(docker.stops.get(), 0);
    }

    #[test]
    fn renewal_uses_validated_identifier_and_positive_integer_seconds() {
        assert_eq!(
            renewal_batch("fixture_ag", 30).unwrap(),
            "ALTER AVAILABILITY GROUP [fixture_ag] SET (WRITE_LEASE_VALIDITY = 30);"
        );
        assert_eq!(
            renewal_batch("fixture_ag", 0),
            Err(AdminError::InvalidDeadline)
        );
        assert_eq!(
            renewal_batch("ag]; SELECT secret", 30),
            Err(AdminError::InvalidIdentifier)
        );
    }

    #[test]
    fn cancelled_stop_excludes_actual_owner_restart_and_reentry_until_exact_cleanup() {
        use crate::fixture::{
            cleanup::{CleanupCoordinator, SystemCleanupClock},
            process::BoundedProcessRunner,
        };
        use crate::three_replica::model::{ResourceKind, ResourceRecord, ResourceState};
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("fixture");
        let store = JournalStore::initialize(&root).unwrap();
        let run = super::super::topology_run(store.root()).unwrap();
        let mut journal = store.create::<OwnershipJournal, _>(run.clone()).unwrap();
        let (request, expected) = target(store.root());
        journal.resources.push(ResourceRecord {
            kind: ResourceKind::Container,
            logical_name: "gated-target".to_owned(),
            path: None,
            intent: Some(request.intent_binding("image")),
            binding: Some(request.resource_binding("image", &expected).unwrap()),
            state: ResourceState::Bound,
        });
        store.save(&journal).unwrap();
        let mut member = ReadyMember {
            ordinal: 1,
            server_name: run.members[0].server_name.clone(),
            container_id: expected.id.clone(),
            host_port: 14330,
            sql_start_time: "sql-start-1".to_owned(),
            sql_start_unix_millis: 1,
            observer_config: root.join("observer"),
            admin_username: root.join("username"),
            admin_password: root.join("password"),
            ca_certificate: root.join("ca.crt"),
        };
        let (dispatched, dispatch_received) = mpsc::channel();
        let (cancel_response, cancelled) = mpsc::channel();
        let (allow_native_effect, native_effect) = mpsc::channel();
        let mut docker = docker(vec![Ok(Some(expected.clone()))], true);
        docker.current.replace(Some(expected.clone()));
        docker.stop_gate = Some((dispatched, cancelled));
        let daemon_running = docker.native_running.clone();
        let daemon = std::thread::spawn(move || {
            native_effect.recv_timeout(Duration::from_secs(5)).unwrap();
            daemon_running.store(false, Ordering::SeqCst);
        });
        let owned = super::super::OwnedContainer {
            request: request.clone(),
            data_directory: None,
            id: Some(expected.id.clone()),
            frozen_running_inspection: Some(expected.clone()),
        };
        let worker_store = JournalStore::initialize(&root).unwrap();
        let expected_for_worker = expected.clone();
        let worker = std::thread::spawn(move || {
            let result = stop_exact_incarnation(
                &docker,
                &request,
                &expected_for_worker,
                Duration::from_secs(5),
                |phase| {
                    journal.lifecycle_exclusion = Some(LifecycleExclusion {
                        container_id: expected_for_worker.id.clone(),
                        phase,
                    });
                    worker_store
                        .save(&journal)
                        .map_err(|_| FixtureFenceError::Lifecycle)
                },
            );
            (docker, journal, result)
        });
        dispatch_received
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let during_dispatch = store.load::<OwnershipJournal>().unwrap().unwrap();
        assert_eq!(
            during_dispatch.lifecycle_exclusion.unwrap().phase,
            LifecyclePhase::StopPending
        );
        cancel_response.send(()).unwrap();
        let (docker, mut journal, result) = worker.join().unwrap();
        assert!(
            result.is_err(),
            "cancelled native completion cannot issue fence evidence"
        );
        assert!(docker.native_running.load(Ordering::SeqCst));
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(matches!(
            restart_owned_member(
                &docker,
                &mut member,
                &store,
                &mut journal,
                deadline,
                Duration::from_secs(1)
            ),
            Err(NativeLaunchError::PriorRunUnresolved)
        ));
        let reopened_store = JournalStore::initialize(&root).unwrap();
        let mut restored = reopened_store.load::<OwnershipJournal>().unwrap().unwrap();
        assert!(matches!(
            restart_owned_member(
                &docker,
                &mut member,
                &reopened_store,
                &mut restored,
                deadline,
                Duration::from_secs(1)
            ),
            Err(NativeLaunchError::PriorRunUnresolved)
        ));
        assert_eq!(docker.starts.get(), 0);
        assert_eq!(docker.stops.get(), 1);
        allow_native_effect.send(()).unwrap();
        daemon.join().unwrap();
        assert!(!docker.native_running.load(Ordering::SeqCst));
        let backend = super::super::NativeCleanupBackend {
            root: store.root(),
            docker: &docker,
            runner: &BoundedProcessRunner,
            clock: SystemCleanupClock::default(),
            image_id: "image",
            network: None,
            containers: &[owned],
        };
        let report = CleanupCoordinator::default().cleanup(&store, &mut restored, &backend);
        assert!(report.succeeded(), "{report:?}");
        assert_eq!(restored.state, RunState::Removed);
        assert_eq!(docker.removed_ids.borrow().as_slice(), [expected.id]);
        assert!(docker.current.borrow().is_none());
        assert!(matches!(
            restart_owned_member(
                &docker,
                &mut member,
                &store,
                &mut restored,
                deadline,
                Duration::from_secs(1)
            ),
            Err(NativeLaunchError::PriorRunUnresolved)
        ));
        let next = store
            .create::<OwnershipJournal, _>(super::super::topology_run(store.root()).unwrap())
            .unwrap();
        assert_ne!(next.run.run_id, run.run_id);
        assert!(next.lifecycle_exclusion.is_none());
        assert!(
            next.resources.is_empty(),
            "a new owner cannot adopt the retired container"
        );
    }
}
