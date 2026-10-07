use std::error::Error;
use std::fmt;
use std::fs;
use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use kuberic_mssql::instance::SqlServerInstanceManager;
use kuberic_mssql::kuberic::{
    HealthyTopologyBinding, HealthyTopologyMemberBinding, ObservationClock,
    RuntimeAuthorityContext, RuntimeAuthorityContextSource, SqlServerMemberEndpoint,
    SqlServerObservationSource, SqlServerService, SqlServerServiceConfig,
    SqlServerStartIncarnation, SystemObservationClock,
};
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::tds::TdsExecutor;
use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, DatabaseIdentity, DatabaseLineage, Guid,
    NativeRole, Observation, ServerName, SqlIdentifier,
};
use kuberic_runtime::application::OpenMode;
use kuberic_runtime::control::proto;
use kuberic_runtime::protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy,
    Epoch, InitializationId, OperationId, PodUid, ProcessSessionId, PvcUid, ReplicaId,
    ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
};
use kuberic_runtime::replicator::ReplicaInformation;
use kuberic_runtime::testing::authority::AdmittedAuthority;
use kuberic_runtime::testing::effects::{RuntimeEffect, RuntimeEffectAction};
use kuberic_runtime::testing::hosting::PodRuntime;
use kuberic_runtime::testing::runtime_adapter::RuntimeAdapter;
use kuberic_runtime::testing::service::AgentService;
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use tonic::Request;

use super::deadline::{BoundedOperationError, complete_before};
use super::member::ReadyMember;
use super::model::{NativeTopologyBinding, TopologyRun};

pub const MSSQL_FAILOVER_DELAY_SECONDS: u64 = 30;
const AGENT_TOKEN: &str = "mssql-three-replica-agent";

type Source = Arc<dyn SqlServerObservationSource>;
type Clock = Arc<dyn ObservationClock>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlGroupError(String);

impl MssqlGroupError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for MssqlGroupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for MssqlGroupError {}

#[derive(Debug, Clone, Copy)]
struct ConvergenceBudget {
    deadline: tokio::time::Instant,
    operation_timeout: Duration,
    complete_deadline: StdInstant,
}

impl ConvergenceBudget {
    fn new(
        operation_timeout: Duration,
        complete_deadline: StdInstant,
    ) -> Result<Self, MssqlGroupError> {
        let remaining = complete_deadline.saturating_duration_since(StdInstant::now());
        if operation_timeout.is_zero() || remaining.is_zero() {
            return Err(MssqlGroupError::new(
                "Kuberic convergence deadline exceeded",
            ));
        }
        Ok(Self {
            deadline: tokio::time::Instant::now() + operation_timeout.min(remaining),
            operation_timeout,
            complete_deadline,
        })
    }

    fn check(self, operation: &'static str) -> Result<(), MssqlGroupError> {
        if tokio::time::Instant::now() >= self.deadline
            || StdInstant::now() >= self.complete_deadline
        {
            Err(MssqlGroupError::new(format!(
                "{operation} exceeded the Kuberic convergence deadline"
            )))
        } else {
            Ok(())
        }
    }

    async fn run<T, F>(
        self,
        operation: &'static str,
        operation_timeout: Duration,
        future: F,
    ) -> Result<T, MssqlGroupError>
    where
        F: Future<Output = T>,
    {
        self.check(operation)?;
        complete_before(self.deadline, operation_timeout, future)
            .await
            .map_err(|error| match error {
                BoundedOperationError::Deadline => MssqlGroupError::new(format!(
                    "{operation} exceeded the Kuberic convergence deadline"
                )),
                BoundedOperationError::OperationTimeout => {
                    MssqlGroupError::new(format!("{operation} timed out"))
                }
            })
    }

    fn shutdown_budget(self) -> Result<Self, MssqlGroupError> {
        Self::new(self.operation_timeout, self.complete_deadline)
    }
}

pub struct MssqlPod {
    pub ordinal: u8,
    pub identity: ReplicaIdentity,
    pub session: ProcessSessionId,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    pub initialization_id: InitializationId,
    pub store: Arc<SqliteStore>,
    pub runtime: Arc<PodRuntime>,
    pub application: Arc<SqlServerService>,
    pub store_root: PathBuf,
    pub application_root: PathBuf,
    pub control_address: SocketAddr,
    pub replication_address: SocketAddr,
    source: Source,
    agent: AgentService<SqliteStore, PodRuntime>,
    shutdown: Option<tokio::sync::watch::Sender<bool>>,
    server: Option<tokio::task::JoinHandle<kuberic_runtime::host::Result<()>>>,
}

impl MssqlPod {
    async fn effect(
        &self,
        action: RuntimeEffectAction,
        budget: ConvergenceBudget,
    ) -> Result<(), MssqlGroupError> {
        budget
            .run("RuntimeAdapter effect", budget.operation_timeout, async {
                let sequence = self
                    .store
                    .load_state()
                    .await
                    .map_err(display_error)?
                    .next_effect_sequence;
                RuntimeAdapter::new(self.store.clone(), self.runtime.clone())
                    .execute(RuntimeEffect {
                        operation_id: OperationId::new(format!(
                            "mssql-member-{}-effect-{sequence}",
                            self.ordinal
                        )),
                        sequence,
                        action,
                    })
                    .await
                    .map_err(display_error)?;
                Ok(())
            })
            .await
            .and_then(|result| result)
    }

    async fn start_agent(&mut self, budget: ConvergenceBudget) -> Result<(), MssqlGroupError> {
        let (ready, mut ready_rx) = tokio::sync::watch::channel(false);
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let agent = self.agent.clone();
        let control = self.control_address;
        let replication = self.replication_address;
        let server =
            tokio::spawn(
                async move { agent.serve(control, replication, ready, shutdown_rx).await },
            );
        self.shutdown = Some(shutdown);
        self.server = Some(server);
        budget
            .run(
                "agent service readiness",
                budget.operation_timeout,
                ready_rx.wait_for(|value| *value),
            )
            .await?
            .map_err(display_error)?;
        Ok(())
    }

    async fn report(
        &self,
        resource_uid: &ResourceUid,
        budget: ConvergenceBudget,
    ) -> Result<proto::AgentStatusReport, MssqlGroupError> {
        budget
            .run("agent report", budget.operation_timeout, async {
                let mut client = proto::agent_control_client::AgentControlClient::connect(format!(
                    "http://{}",
                    self.control_address
                ))
                .await
                .map_err(display_error)?;
                let mut request = Request::new(proto::GetAgentStatusRequest {
                    protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                    resource_uid: resource_uid.to_string(),
                    replica_id: self.identity.replica_id.value(),
                    expected_instance_id: self.identity.instance_id.to_string(),
                });
                request.metadata_mut().insert(
                    "authorization",
                    format!("Bearer {AGENT_TOKEN}")
                        .parse()
                        .map_err(display_error)?,
                );
                client
                    .get_status(request)
                    .await
                    .map_err(display_error)
                    .map(|response| response.into_inner())
            })
            .await
            .and_then(|result| result)
    }

    async fn shutdown(&mut self, budget: ConvergenceBudget) -> Result<(), MssqlGroupError> {
        self.runtime.abort();
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send_replace(true);
        }
        if let Some(server) = self.server.take() {
            match budget
                .run("agent shutdown", budget.operation_timeout, server)
                .await
            {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => return Err(display_error(error)),
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => return Err(display_error(error)),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl Drop for MssqlPod {
    fn drop(&mut self) {
        self.runtime.abort();
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send_replace(true);
        }
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

pub struct MssqlGroup {
    pub resource_uid: ResourceUid,
    pub effective_policy: EffectivePolicy,
    pub configuration: ConfigurationDescriptor,
    pub pods: [MssqlPod; 3],
    native_binding: NativeTopologyBinding,
    topology_bindings: [HealthyTopologyBinding; 3],
    convergence_budget: ConvergenceBudget,
}

impl MssqlGroup {
    pub async fn from_live(
        root: &Path,
        run: &TopologyRun,
        native_binding: &NativeTopologyBinding,
        members: &[ReadyMember; 3],
        convergence_timeout: Duration,
        complete_deadline: StdInstant,
    ) -> Result<Self, MssqlGroupError> {
        let budget = ConvergenceBudget::new(convergence_timeout, complete_deadline)?;
        let mut sources = Vec::with_capacity(3);
        for member in members {
            let config = budget
                .run(
                    "observer configuration read",
                    convergence_timeout,
                    ObserverConfig::read(&member.observer_config),
                )
                .await
                .and_then(|result| result.map_err(display_error))?;
            sources.push(Arc::new(SqlServerInstanceManager::new(
                TdsExecutor::new(config.connection().clone()),
                config,
            )) as Source);
        }
        Self::assemble_with_store_policies(
            root,
            run,
            native_binding,
            sources
                .try_into()
                .map_err(|_| MssqlGroupError::new("exactly three observation sources required"))?,
            std::array::from_fn(|_| Arc::new(SystemObservationClock) as Clock),
            std::array::from_fn(|_| exact_policy()),
            budget,
        )
        .await
    }

    pub async fn assemble(
        root: &Path,
        run: &TopologyRun,
        native_binding: &NativeTopologyBinding,
        sources: [Source; 3],
        clocks: [Clock; 3],
        convergence_timeout: Duration,
        complete_deadline: StdInstant,
    ) -> Result<Self, MssqlGroupError> {
        let budget = ConvergenceBudget::new(convergence_timeout, complete_deadline)?;
        Self::assemble_with_store_policies(
            root,
            run,
            native_binding,
            sources,
            clocks,
            std::array::from_fn(|_| exact_policy()),
            budget,
        )
        .await
    }

    async fn assemble_with_store_policies(
        root: &Path,
        run: &TopologyRun,
        native_binding: &NativeTopologyBinding,
        sources: [Source; 3],
        clocks: [Clock; 3],
        store_policies: [EffectivePolicy; 3],
        budget: ConvergenceBudget,
    ) -> Result<Self, MssqlGroupError> {
        validate_run_binding(run, native_binding)?;
        budget.check("Kuberic topology assembly")?;
        let resource_uid = ResourceUid::new(run.resource_uid.clone());
        let effective_policy = exact_policy();
        let identities: [ReplicaIdentity; 3] =
            std::array::from_fn(|index| kuberic_identity(run, index));
        let roles = stable_roles(native_binding)?;
        let primary_index = roles
            .iter()
            .position(|role| *role == ReplicaRole::Primary)
            .ok_or_else(|| MssqlGroupError::new("native binding has no primary"))?;
        let configuration = ConfigurationDescriptor::new(
            Epoch::new(0, native_binding.configuration_sequence),
            identities[primary_index].replica_id,
            identities
                .iter()
                .cloned()
                .zip(roles)
                .map(|(identity, role)| ConfigurationMember { identity, role })
                .collect(),
            2,
        );
        let control_addresses = reserve_addresses()?;
        let replication_addresses = reserve_addresses()?;

        let mut pods = Vec::with_capacity(3);
        for index in 0..3 {
            if !run.members[index].data_directory.starts_with(root) {
                return Err(MssqlGroupError::new(
                    "Kuberic member roots must remain inside the fixture root",
                ));
            }
            let store_root = run.members[index]
                .data_directory
                .join("kuberic-runtime")
                .join("store");
            let application_root = run.members[index]
                .data_directory
                .join("kuberic-runtime")
                .join("application");
            create_private_directory(&store_root)?;
            create_private_directory(&application_root)?;
            let database = SqliteStore::metadata_database_path(&store_root);
            let pod_uid = PodUid::new(run.kuberic_members[index].pod_uid.clone());
            let pvc_uid = PvcUid::new(run.kuberic_members[index].pvc_uid.clone());
            let initialization_id =
                InitializationId::new(format!("{}-initialization-{}", run.run_id, index + 1));
            let store = Arc::new(
                SqliteStore::create_authorized(
                    &database,
                    AgentState::new(StorageIdentity {
                        schema_version: SCHEMA_VERSION,
                        resource_uid: resource_uid.clone(),
                        local_identity: identities[index].clone(),
                        pod_uid: pod_uid.clone(),
                        pvc_uid: pvc_uid.clone(),
                        initialization_id: initialization_id.clone(),
                        effective_policy: store_policies[index].clone(),
                    }),
                )
                .map_err(display_error)?,
            );
            let application = Arc::new(SqlServerService::with_observation_source(
                SqlServerServiceConfig::new(
                    resource_uid.clone(),
                    replication_addresses[index].to_string(),
                )
                .map_err(display_error)?,
                sources[index].clone(),
                clocks[index].clone(),
            ));
            let runtime = Arc::new(PodRuntime::new(
                identities[index].clone(),
                application.clone(),
                store.clone(),
            ));
            let agent =
                AgentService::new(store.clone(), runtime.clone(), runtime.clone(), AGENT_TOKEN)
                    .map_err(display_error)?;
            let session = agent.sessions().local_session().clone();
            runtime
                .bind_replica_session(resource_uid.clone(), session.clone())
                .map_err(display_error)?;
            pods.push(MssqlPod {
                ordinal: (index + 1) as u8,
                identity: identities[index].clone(),
                session,
                pod_uid,
                pvc_uid,
                initialization_id,
                store,
                runtime,
                application,
                store_root,
                application_root,
                control_address: control_addresses[index],
                replication_address: replication_addresses[index],
                source: sources[index].clone(),
                agent,
                shutdown: None,
                server: None,
            });
        }
        let mut pods: [MssqlPod; 3] = pods
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three Kuberic pods required"))?;

        let initial = observe_sources(&pods, budget).await?;
        let members = build_member_bindings(run, native_binding, &pods, &initial, &roles)?;
        let availability_group = availability_group_identity(native_binding)?;
        let database_lineage = database_lineage(native_binding)?;
        let mut topology_bindings = Vec::with_capacity(3);
        for pod in &pods {
            let binding = HealthyTopologyBinding::new(
                resource_uid.clone(),
                pod.identity.clone(),
                configuration.clone(),
                effective_policy.clone(),
                availability_group.clone(),
                database_lineage.clone(),
                members.clone(),
            )
            .map_err(display_error)?;
            topology_bindings.push(binding.clone());
            budget
                .run(
                    "healthy topology binding",
                    budget.operation_timeout,
                    pod.application.bind_topology(
                        binding,
                        Arc::new(StoreAuthorityContextSource {
                            store: pod.store.clone(),
                        }),
                    ),
                )
                .await?
                .map_err(display_error)?;
        }
        let topology_bindings: [HealthyTopologyBinding; 3] = topology_bindings
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three topology bindings required"))?;

        for pod in &pods {
            pod.effect(RuntimeEffectAction::Open(OpenMode::New), budget)
                .await?;
        }
        for source_index in 0..3 {
            for target_index in 0..3 {
                if source_index == target_index {
                    continue;
                }
                let target = &pods[target_index];
                pods[source_index]
                    .effect(
                        RuntimeEffectAction::RegisterPeerSession {
                            identity: target.identity.clone(),
                            session: target.session.clone(),
                        },
                        budget,
                    )
                    .await?;
                let mut description = ReplicaInformation::new(
                    OperationId::default(),
                    target.identity.clone(),
                    target.replication_address.to_string(),
                );
                description.process_session_id = target.session.clone();
                description.role = roles[target_index];
                budget
                    .run(
                        "peer description",
                        budget.operation_timeout,
                        kuberic_runtime::testing::describe_peer(
                            &pods[source_index].runtime,
                            description,
                        ),
                    )
                    .await?
                    .map_err(display_error)?;
            }
        }
        for pod in &pods {
            pod.effect(
                RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                    local_identity: pod.identity.clone(),
                    transition_kind: None,
                    previous_configuration: None,
                    current_configuration: configuration.clone(),
                    switchover_handoff: None,
                    secondary_removal: None,
                    scale_up: None,
                })),
                budget,
            )
            .await?;
        }
        observe_sources(&pods, budget).await?;
        for (pod, role) in pods.iter().zip(roles) {
            pod.effect(RuntimeEffectAction::ChangeRole(role), budget)
                .await?;
            pod.effect(
                RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::ReconfigurationPending,
                    write: if role == ReplicaRole::Primary {
                        AccessStatus::ReconfigurationPending
                    } else {
                        AccessStatus::NotPrimary
                    },
                },
                budget,
            )
            .await?;
            pod.effect(RuntimeEffectAction::RefreshApplicationProgress, budget)
                .await?;
        }
        for pod in &mut pods {
            pod.start_agent(budget).await?;
        }

        let group = Self {
            resource_uid,
            effective_policy,
            configuration,
            pods,
            native_binding: native_binding.clone(),
            topology_bindings,
            convergence_budget: budget,
        };
        group.validate_durable_state().await?;
        Ok(group)
    }

    pub async fn reports_bracketed(
        &self,
    ) -> Result<[proto::AgentStatusReport; 3], MssqlGroupError> {
        let before = observe_sources(&self.pods, self.convergence_budget).await?;
        let mut reports = Vec::with_capacity(3);
        for pod in &self.pods {
            reports.push(
                pod.report(&self.resource_uid, self.convergence_budget)
                    .await?,
            );
        }
        let after = observe_sources(&self.pods, self.convergence_budget).await?;
        for index in 0..3 {
            validate_bound_observation(
                index,
                &self.native_binding,
                &self.topology_bindings[index],
                &before[index],
                self.pods[index].source.observer_config(),
            )?;
            validate_bound_observation(
                index,
                &self.native_binding,
                &self.topology_bindings[index],
                &after[index],
                self.pods[index].source.observer_config(),
            )?;
            self.validate_report(index, &reports[index])?;
        }
        self.convergence_budget.check("report comparison")?;
        reports
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three reports required"))
    }

    pub async fn validate_durable_state(&self) -> Result<(), MssqlGroupError> {
        for (index, pod) in self.pods.iter().enumerate() {
            let state = self
                .convergence_budget
                .run(
                    "durable state validation",
                    self.convergence_budget.operation_timeout,
                    pod.store.load_state(),
                )
                .await?
                .map_err(display_error)?;
            if state.identity.resource_uid != self.resource_uid
                || state.identity.local_identity != pod.identity
                || state.identity.pod_uid != pod.pod_uid
                || state.identity.pvc_uid != pod.pvc_uid
                || state.identity.initialization_id != pod.initialization_id
                || state.identity.effective_policy != self.effective_policy
                || state.current_configuration.as_ref() != Some(&self.configuration)
                || state.previous_configuration.is_some()
                || state.role != self.configuration.members[index].role
                || state.pending_effect.is_some()
            {
                return Err(MssqlGroupError::new(format!(
                    "member {} durable Kuberic state differs from the frozen topology",
                    index + 1
                )));
            }
            let expected_write = if state.role == ReplicaRole::Primary {
                AccessStatus::ReconfigurationPending
            } else {
                AccessStatus::NotPrimary
            };
            if state.read_status != AccessStatus::ReconfigurationPending
                || state.write_status != expected_write
            {
                return Err(MssqlGroupError::new(format!(
                    "member {} durable access is not fenced",
                    index + 1
                )));
            }
            let snapshot = self
                .convergence_budget
                .run(
                    "runtime state validation",
                    self.convergence_budget.operation_timeout,
                    pod.runtime.snapshot(),
                )
                .await?;
            if snapshot
                .authority
                .as_ref()
                .map(|authority| &authority.current_configuration)
                != Some(&self.configuration)
                || snapshot.role != state.role
                || snapshot.current_progress != self.native_binding.configuration_sequence
                || snapshot.write_status != expected_write
            {
                return Err(MssqlGroupError::new(format!(
                    "member {} runtime state differs from durable state",
                    index + 1
                )));
            }
        }
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), MssqlGroupError> {
        let mut errors = Vec::new();
        let budget = self.convergence_budget.shutdown_budget()?;
        for pod in &mut self.pods {
            if let Err(error) = pod.shutdown(budget).await {
                errors.push(error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(MssqlGroupError::new(format!(
                "Kuberic group shutdown failed: {errors:?}"
            )))
        }
    }

    fn validate_report(
        &self,
        index: usize,
        report: &proto::AgentStatusReport,
    ) -> Result<(), MssqlGroupError> {
        let pod = &self.pods[index];
        let role = self.configuration.members[index].role;
        let expected_write = if role == ReplicaRole::Primary {
            proto::AccessStatus::ReconfigurationPending
        } else {
            proto::AccessStatus::NotPrimary
        };
        if report.protocol_version != kuberic_runtime::protocol::PROTOCOL_VERSION
            || report.resource_uid != self.resource_uid.as_str()
            || report.replica_id != pod.identity.replica_id.value()
            || report.process_session_id != pod.session.as_str()
            || report.report_sequence == 0
            || report.role != proto_role(role) as i32
            || report.read_status != proto::AccessStatus::ReconfigurationPending as i32
            || report.write_status != expected_write as i32
            || report.previous_configuration.is_some()
            || !report.healthy
            || report.storage_state != proto::AgentStorageState::Initialized as i32
            || !report.storage_error.is_empty()
            || report.pod_uid != pod.pod_uid.as_str()
            || report.pvc_uid != pod.pvc_uid.as_str()
            || report.current_progress != self.native_binding.configuration_sequence
            || report.catch_up_capability != Some(self.native_binding.configuration_sequence)
        {
            return Err(MssqlGroupError::new(format!(
                "member {} agent report differs from the exact fenced state",
                index + 1
            )));
        }
        let identity = report
            .identity
            .as_ref()
            .ok_or_else(|| MssqlGroupError::new("agent report identity is missing"))?;
        if identity.replica_id != pod.identity.replica_id.value()
            || identity.instance_id != pod.identity.instance_id.as_str()
            || identity.agent_generation != pod.identity.agent_generation.as_str()
        {
            return Err(MssqlGroupError::new("agent report identity differs"));
        }
        validate_wire_configuration(
            report
                .current_configuration
                .as_ref()
                .ok_or_else(|| MssqlGroupError::new("current configuration is missing"))?,
            &self.configuration,
        )
    }
}

struct StoreAuthorityContextSource {
    store: Arc<SqliteStore>,
}

#[async_trait]
impl RuntimeAuthorityContextSource for StoreAuthorityContextSource {
    async fn current_authority_context(
        &self,
    ) -> kuberic_runtime::Result<Option<RuntimeAuthorityContext>> {
        let state = self
            .store
            .load_state()
            .await
            .map_err(|error| kuberic_runtime::RuntimeError::Application(error.to_string()))?;
        let pending = state.pending_effect.as_ref().and_then(|pending| {
            if let RuntimeEffectAction::AdmitAuthority(authority) = &pending.effect.action {
                Some(authority.as_ref())
            } else {
                None
            }
        });
        let configuration = pending
            .map(|authority| authority.current_configuration.clone())
            .or(state.current_configuration);
        let Some(configuration) = configuration else {
            return Ok(None);
        };
        let policy = state
            .admitted_policy
            .unwrap_or_else(|| state.identity.effective_policy.clone());
        Ok(Some(RuntimeAuthorityContext::new(
            state.identity.local_identity,
            configuration,
            policy,
        )))
    }
}

fn exact_policy() -> EffectivePolicy {
    EffectivePolicy::fixed(3, MSSQL_FAILOVER_DELAY_SECONDS).expect("three-member policy is valid")
}

fn kuberic_identity(run: &TopologyRun, index: usize) -> ReplicaIdentity {
    let member = &run.kuberic_members[index];
    ReplicaIdentity {
        replica_id: ReplicaId::new(member.replica_id),
        instance_id: ReplicaInstanceId::new(member.instance_id.clone()),
        agent_generation: AgentGeneration::new(format!(
            "{}-generation-{}",
            run.run_id, member.ordinal
        )),
    }
}

fn stable_roles(
    native_binding: &NativeTopologyBinding,
) -> Result<[ReplicaRole; 3], MssqlGroupError> {
    let roles = native_binding
        .members
        .iter()
        .map(|member| match member.role.as_str() {
            "PRIMARY" => Ok(ReplicaRole::Primary),
            "SECONDARY" => Ok(ReplicaRole::ActiveSecondary),
            _ => Err(MssqlGroupError::new(
                "native binding contains an unstable role",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let roles: [ReplicaRole; 3] = roles
        .try_into()
        .map_err(|_| MssqlGroupError::new("native binding must contain three roles"))?;
    if roles
        .iter()
        .filter(|role| **role == ReplicaRole::Primary)
        .count()
        != 1
        || roles
            .iter()
            .filter(|role| **role == ReplicaRole::ActiveSecondary)
            .count()
            != 2
    {
        return Err(MssqlGroupError::new(
            "native binding must contain one primary and two secondaries",
        ));
    }
    Ok(roles)
}

async fn observe_sources(
    pods: &[MssqlPod; 3],
    budget: ConvergenceBudget,
) -> Result<[InstanceSnapshot; 3], MssqlGroupError> {
    let mut snapshots = Vec::with_capacity(3);
    for pod in pods {
        snapshots.push(observe_source(&pod.source, budget).await?);
    }
    snapshots
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three direct observations required"))
}

async fn observe_source(
    source: &Source,
    budget: ConvergenceBudget,
) -> Result<InstanceSnapshot, MssqlGroupError> {
    let observation = budget
        .run(
            "direct SQL observation",
            source.observer_config().sample_timeout(),
            source.observe(),
        )
        .await?
        .map_err(display_error)?;
    match observation {
        Observation::Present { value, .. } => Ok(value),
        Observation::Absent { .. } => Err(MssqlGroupError::new("availability group is absent")),
        Observation::Failed(failure) => Err(MssqlGroupError::new(format!(
            "direct observation failed: {:?}",
            failure.kind
        ))),
    }
}

fn build_member_bindings(
    run: &TopologyRun,
    native_binding: &NativeTopologyBinding,
    pods: &[MssqlPod; 3],
    snapshots: &[InstanceSnapshot; 3],
    roles: &[ReplicaRole; 3],
) -> Result<Vec<HealthyTopologyMemberBinding>, MssqlGroupError> {
    (0..3)
        .map(|index| {
            validate_native_observation(
                index,
                native_binding,
                &snapshots[index],
                pods[index].source.observer_config(),
            )?;
            let group = present_group(&snapshots[index])?;
            HealthyTopologyMemberBinding::new(
                pods[index].identity.clone(),
                pods[index].session.clone(),
                pods[index].replication_address.to_string(),
                group.local_replica.identity.clone(),
                SqlServerMemberEndpoint::new(
                    ServerName::new(run.members[index].server_name.clone())
                        .map_err(display_error)?,
                    native_binding.members[index].endpoint_url.clone(),
                )
                .map_err(display_error)?,
                SqlServerStartIncarnation::new(
                    snapshots[index].instance.sqlserver_start_time.clone(),
                )
                .map_err(display_error)?,
                roles[index],
            )
            .map_err(display_error)
        })
        .collect()
}

fn validate_run_binding(
    run: &TopologyRun,
    native_binding: &NativeTopologyBinding,
) -> Result<(), MssqlGroupError> {
    if native_binding.session_id != run.run_id
        || native_binding.members.len() != 3
        || run.members.len() != 3
        || run.kuberic_members.len() != 3
        || native_binding.configuration_sequence < 0
        || run
            .kuberic_members
            .iter()
            .enumerate()
            .any(|(index, member)| member.replica_id != index as i64 + 1)
    {
        return Err(MssqlGroupError::new(
            "native binding does not belong to the exact three-member run",
        ));
    }
    for index in 0..3 {
        if native_binding.members[index].ordinal != run.members[index].ordinal
            || native_binding.members[index].server_name != run.members[index].server_name
        {
            return Err(MssqlGroupError::new(
                "native member ordering differs from the fixture run",
            ));
        }
    }
    Ok(())
}

fn validate_native_observation(
    index: usize,
    native_binding: &NativeTopologyBinding,
    snapshot: &InstanceSnapshot,
    config: &ObserverConfig,
) -> Result<(), MssqlGroupError> {
    let member = &native_binding.members[index];
    let group = present_group(snapshot)?;
    let expected_role = if member.role == "PRIMARY" {
        NativeRole::Primary
    } else {
        NativeRole::Secondary
    };
    let observed_native = group
        .local_replica
        .identity
        .native_replica_id()
        .ok_or_else(|| MssqlGroupError::new("direct observation has no native replica ID"))?;
    let configured_incarnation =
        format!("{}:{}", member.container_id, member.sql_start_unix_millis);
    let profile = group
        .replicas
        .iter()
        .find(|replica| replica.replica_id == *observed_native)
        .ok_or_else(|| MssqlGroupError::new("local native replica profile is missing"))?;
    let database = group
        .databases
        .iter()
        .find(|database| database.identity.name.as_str() == native_binding.database_name)
        .ok_or_else(|| MssqlGroupError::new("bound database is missing"))?;
    let local_database = database
        .local
        .as_ref()
        .ok_or_else(|| MssqlGroupError::new("local database evidence is missing"))?;
    let recovery = local_database
        .recovery
        .as_ref()
        .ok_or_else(|| MssqlGroupError::new("local recovery evidence is missing"))?;
    let age = unix_millis()?
        .checked_sub(snapshot.observed_at_unix_millis)
        .ok_or_else(|| MssqlGroupError::new("direct observation is future-dated"))?;
    if age > config.max_age_millis()
        || snapshot.instance.server_name.as_str() != member.server_name
        || snapshot.instance.property_server_name.as_str() != member.server_name
        || group.identity.name.as_str() != native_binding.availability_group_name
        || group.identity.group_id.to_string() != native_binding.availability_group_id
        || group.configuration_sequence.value() != native_binding.configuration_sequence
        || observed_native.to_string() != member.native_replica_id
        || group.local_replica.role != Some(expected_role)
        || group.local_replica.identity.incarnation() != configured_incarnation
        || profile.server_name.as_str() != member.server_name
        || profile.endpoint_url.as_deref() != Some(member.endpoint_url.as_str())
        || database.identity.group_database_id.to_string() != native_binding.group_database_id
        || local_database.database_id != member.local_database_id
        || local_database.replica_id.to_string() != member.native_replica_id
        || recovery
            .database_guid
            .as_ref()
            .map(ToString::to_string)
            .as_deref()
            != Some(member.database_guid.as_str())
        || recovery
            .family_guid
            .as_ref()
            .map(ToString::to_string)
            .as_deref()
            != Some(native_binding.family_guid.as_str())
        || recovery
            .recovery_fork_guid
            .as_ref()
            .map(ToString::to_string)
            .as_deref()
            != Some(native_binding.recovery_fork_id.as_str())
        || config.target().expected_server_name.as_str() != member.server_name
        || config.target().availability_group.as_str() != native_binding.availability_group_name
        || config.target().replica.incarnation() != configured_incarnation
    {
        return Err(MssqlGroupError::new(format!(
            "member {} direct observation differs from the exact native binding",
            index + 1
        )));
    }
    Ok(())
}

fn validate_bound_observation(
    index: usize,
    native_binding: &NativeTopologyBinding,
    binding: &HealthyTopologyBinding,
    snapshot: &InstanceSnapshot,
    config: &ObserverConfig,
) -> Result<(), MssqlGroupError> {
    validate_native_observation(index, native_binding, snapshot, config)?;
    let role = binding
        .members()
        .iter()
        .find(|member| member.kuberic_identity() == binding.local_identity())
        .map(HealthyTopologyMemberBinding::stable_role)
        .ok_or_else(|| MssqlGroupError::new("local topology binding member is missing"))?;
    binding
        .validate_snapshot(config, snapshot, unix_millis()?, true, Some(role))
        .map_err(display_error)
}

fn present_group(
    snapshot: &InstanceSnapshot,
) -> Result<&kuberic_mssql::observation::AvailabilityGroupSnapshot, MssqlGroupError> {
    match &snapshot.availability_group {
        Observation::Present {
            value,
            observed_at_unix_millis,
        } if *observed_at_unix_millis == snapshot.observed_at_unix_millis => Ok(value),
        _ => Err(MssqlGroupError::new(
            "direct observation is absent, failed, or inconsistently timestamped",
        )),
    }
}

fn availability_group_identity(
    native_binding: &NativeTopologyBinding,
) -> Result<AvailabilityGroupIdentity, MssqlGroupError> {
    Ok(AvailabilityGroupIdentity {
        name: AvailabilityGroupName::new(native_binding.availability_group_name.clone())
            .map_err(display_error)?,
        group_id: Guid::parse(
            "native availability-group ID",
            &native_binding.availability_group_id,
        )
        .map_err(display_error)?,
    })
}

fn database_lineage(
    native_binding: &NativeTopologyBinding,
) -> Result<DatabaseLineage, MssqlGroupError> {
    Ok(DatabaseLineage {
        database: DatabaseIdentity {
            name: SqlIdentifier::new(native_binding.database_name.clone())
                .map_err(display_error)?,
            group_database_id: Guid::parse(
                "native group database ID",
                &native_binding.group_database_id,
            )
            .map_err(display_error)?,
        },
        recovery_fork_id: Guid::parse("native recovery fork ID", &native_binding.recovery_fork_id)
            .map_err(display_error)?,
    })
}

fn reserve_addresses() -> Result<[SocketAddr; 3], MssqlGroupError> {
    let mut addresses = Vec::with_capacity(3);
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(display_error)?;
        addresses.push(listener.local_addr().map_err(display_error)?);
    }
    addresses
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three loopback addresses required"))
}

fn create_private_directory(path: &Path) -> Result<(), MssqlGroupError> {
    fs::create_dir_all(path).map_err(display_error)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(display_error)
}

fn validate_wire_configuration(
    wire: &proto::Configuration,
    expected: &ConfigurationDescriptor,
) -> Result<(), MssqlGroupError> {
    let epoch = wire
        .epoch
        .as_ref()
        .ok_or_else(|| MssqlGroupError::new("report epoch is missing"))?;
    if wire.configuration_id != expected.configuration_id.as_str()
        || epoch.data_loss_number != expected.epoch.data_loss_number
        || epoch.configuration_number != expected.epoch.configuration_number
        || wire.primary_id != expected.primary_id.value()
        || wire.write_quorum != expected.write_quorum
        || wire.members.len() != expected.members.len()
    {
        return Err(MssqlGroupError::new(
            "reported current configuration descriptor differs",
        ));
    }
    for (actual, expected) in wire.members.iter().zip(&expected.members) {
        let identity = actual
            .identity
            .as_ref()
            .ok_or_else(|| MssqlGroupError::new("reported member identity is missing"))?;
        if identity.replica_id != expected.identity.replica_id.value()
            || identity.instance_id != expected.identity.instance_id.as_str()
            || identity.agent_generation != expected.identity.agent_generation.as_str()
            || actual.role != proto_role(expected.role) as i32
        {
            return Err(MssqlGroupError::new(
                "reported configuration member differs",
            ));
        }
    }
    Ok(())
}

fn proto_role(role: ReplicaRole) -> proto::ReplicaRole {
    match role {
        ReplicaRole::Primary => proto::ReplicaRole::Primary,
        ReplicaRole::ActiveSecondary => proto::ReplicaRole::ActiveSecondary,
        ReplicaRole::IdleSecondary => proto::ReplicaRole::IdleSecondary,
        ReplicaRole::None => proto::ReplicaRole::None,
    }
}

fn unix_millis() -> Result<u64, MssqlGroupError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(display_error)
        .and_then(|duration| {
            u64::try_from(duration.as_millis())
                .map_err(|_| MssqlGroupError::new("system time exceeds millisecond range"))
        })
}

fn display_error(error: impl fmt::Display) -> MssqlGroupError {
    MssqlGroupError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    use kuberic_mssql::observation::{
        AvailabilityGroupSnapshot, DatabaseReplicaSnapshot, DatabaseSnapshot, InstanceMetadata,
        LocalDatabaseSnapshot, LocalRecoveryMetadata, LocalReplicaSnapshot, NativeProvenance,
        RecoveryLineageObservation, ReplicaSnapshot, ReplicaState,
    };
    use kuberic_mssql::{
        ConfigurationSequence, DecimalProgress, NativeProgress,
        ReplicaIdentity as SqlReplicaIdentity,
    };

    use super::*;
    use crate::three_replica::{KubericMember, NativeMemberBinding, SqlMember, TopologyRun};

    const AG_ID: &str = "11111111-1111-4111-8111-111111111111";
    const DATABASE_ID: &str = "22222222-2222-4222-8222-222222222222";
    const FAMILY_ID: &str = "33333333-3333-4333-8333-333333333333";
    const FORK_ID: &str = "44444444-4444-4444-8444-444444444444";
    const NATIVE_IDS: [&str; 3] = [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    ];
    const DATABASE_GUIDS: [&str; 3] = [
        "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "ffffffff-ffff-4fff-8fff-ffffffffffff",
    ];

    struct StaticSource {
        config: ObserverConfig,
        snapshot: InstanceSnapshot,
        observations: AtomicUsize,
    }

    #[async_trait]
    impl SqlServerObservationSource for StaticSource {
        fn observer_config(&self) -> &ObserverConfig {
            &self.config
        }

        async fn observe(
            &self,
        ) -> Result<Observation<InstanceSnapshot>, kuberic_mssql::runtime_error::RuntimeError>
        {
            self.observations.fetch_add(1, Ordering::SeqCst);
            Ok(Observation::Present {
                value: self.snapshot.clone(),
                observed_at_unix_millis: self.snapshot.observed_at_unix_millis,
            })
        }
    }

    struct FixedClock(AtomicU64);

    impl ObservationClock for FixedClock {
        fn now_unix_millis(&self) -> Result<u64, kuberic_mssql::runtime_error::RuntimeError> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    struct ToggleSource {
        inner: StaticSource,
        fail: AtomicBool,
    }

    #[async_trait]
    impl SqlServerObservationSource for ToggleSource {
        fn observer_config(&self) -> &ObserverConfig {
            self.inner.observer_config()
        }

        async fn observe(
            &self,
        ) -> Result<Observation<InstanceSnapshot>, kuberic_mssql::runtime_error::RuntimeError>
        {
            if self.fail.load(Ordering::SeqCst) {
                return Err(kuberic_mssql::runtime_error::RuntimeError::new(
                    kuberic_mssql::ObservationFailureKind::Unreachable,
                    "phase7 fake report",
                    "injected report observation failure",
                ));
            }
            self.inner.observe().await
        }
    }

    struct DelayedSource {
        inner: StaticSource,
        delay: Duration,
    }

    #[async_trait]
    impl SqlServerObservationSource for DelayedSource {
        fn observer_config(&self) -> &ObserverConfig {
            self.inner.observer_config()
        }

        async fn observe(
            &self,
        ) -> Result<Observation<InstanceSnapshot>, kuberic_mssql::runtime_error::RuntimeError>
        {
            tokio::time::sleep(self.delay).await;
            self.inner.observe().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_observation_at_remaining_overall_deadline_is_rejected() {
        let root = PathBuf::from("/phase7-deadline-test");
        let run = run(&root);
        let now = unix_millis().unwrap();
        let source: Source = Arc::new(DelayedSource {
            inner: source(&run, 0, now),
            delay: Duration::from_millis(25),
        });
        let budget = ConvergenceBudget::new(
            Duration::from_secs(60),
            StdInstant::now() + Duration::from_millis(25),
        )
        .unwrap();
        let error = observe_source(&source, budget).await.unwrap_err();
        assert!(
            error.to_string().contains("Kuberic convergence deadline"),
            "{error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_observation_before_convergence_deadline_succeeds() {
        let root = PathBuf::from("/phase7-deadline-success-test");
        let run = run(&root);
        let now = unix_millis().unwrap();
        let source: Source = Arc::new(DelayedSource {
            inner: source(&run, 0, now),
            delay: Duration::from_millis(24),
        });
        let budget = ConvergenceBudget::new(
            Duration::from_millis(25),
            StdInstant::now() + Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            observe_source(&source, budget)
                .await
                .unwrap()
                .observed_at_unix_millis,
            now
        );
    }

    #[test]
    fn three_runtime_assembly_uses_actual_sessions_and_fenced_reports() {
        run_group_test(async {
            let root = test_root("assembly");
            let run = run(&root);
            let native = native_binding(&run);
            let now = unix_millis().unwrap();
            let sources: [Arc<StaticSource>; 3] =
                std::array::from_fn(|index| Arc::new(source(&run, index, now)));
            let group = MssqlGroup::assemble(
                &root,
                &run,
                &native,
                sources.clone().map(|source| source as Source),
                std::array::from_fn(|_| Arc::new(FixedClock(AtomicU64::new(now))) as Clock),
                Duration::from_secs(60),
                StdInstant::now() + Duration::from_secs(120),
            )
            .await
            .unwrap();

            let sessions = group
                .pods
                .iter()
                .map(|pod| pod.session.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            let stores = group
                .pods
                .iter()
                .map(|pod| pod.store.path())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(sessions.len(), 3);
            assert_eq!(stores.len(), 3);
            assert!(
                group
                    .pods
                    .iter()
                    .all(|pod| pod.store_root != pod.application_root)
            );
            let reports = group.reports_bracketed().await.unwrap();
            assert_eq!(
                reports
                    .iter()
                    .filter(|report| report.role == proto::ReplicaRole::Primary as i32)
                    .count(),
                1
            );
            assert_eq!(
                reports
                    .iter()
                    .filter(|report| report.role == proto::ReplicaRole::ActiveSecondary as i32)
                    .count(),
                2
            );
            assert!(
                sources
                    .iter()
                    .all(|source| source.observations.load(Ordering::SeqCst) >= 8)
            );
            group.shutdown().await.unwrap();
            fs::remove_dir_all(&root).unwrap();
        });
    }

    #[test]
    fn store_policy_mismatch_fails_before_authority_is_durable() {
        run_group_test(async {
            let root = test_root("policy");
            let run = run(&root);
            let native = native_binding(&run);
            let now = unix_millis().unwrap();
            let sources: [Source; 3] = std::array::from_fn(|index| {
                Arc::new(source(&run, index, now)) as Arc<dyn SqlServerObservationSource>
            });
            let error = match MssqlGroup::assemble_with_store_policies(
                &root,
                &run,
                &native,
                sources,
                std::array::from_fn(|_| Arc::new(FixedClock(AtomicU64::new(now))) as Clock),
                [
                    exact_policy(),
                    EffectivePolicy::fixed(3, MSSQL_FAILOVER_DELAY_SECONDS + 1).unwrap(),
                    exact_policy(),
                ],
                ConvergenceBudget::new(
                    Duration::from_secs(60),
                    StdInstant::now() + Duration::from_secs(120),
                )
                .unwrap(),
            )
            .await
            {
                Ok(group) => {
                    group.shutdown().await.unwrap();
                    panic!("policy mismatch unexpectedly assembled")
                }
                Err(error) => error,
            };
            assert!(error.to_string().contains("effective policy"), "{error}");
            fs::remove_dir_all(&root).unwrap();
        });
    }

    #[test]
    fn report_observation_failure_remains_fenced_and_shutdown_is_clean() {
        run_group_test(async {
            let root = test_root("report-failure");
            let run = run(&root);
            let native = native_binding(&run);
            let now = unix_millis().unwrap();
            let sources: [Arc<ToggleSource>; 3] = std::array::from_fn(|index| {
                Arc::new(ToggleSource {
                    inner: source(&run, index, now),
                    fail: AtomicBool::new(false),
                })
            });
            let group = MssqlGroup::assemble(
                &root,
                &run,
                &native,
                sources.clone().map(|source| source as Source),
                std::array::from_fn(|_| Arc::new(FixedClock(AtomicU64::new(now))) as Clock),
                Duration::from_secs(60),
                StdInstant::now() + Duration::from_secs(120),
            )
            .await
            .unwrap();
            sources[1].fail.store(true, Ordering::SeqCst);
            let error = group.reports_bracketed().await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected report observation failure")
            );
            group.shutdown().await.unwrap();
            fs::remove_dir_all(&root).unwrap();
        });
    }

    #[test]
    fn direct_report_brackets_reject_start_and_health_drift() {
        run_group_test(async {
            let root = test_root("direct-drift");
            let run = run(&root);
            let native = native_binding(&run);
            let now = unix_millis().unwrap();
            let sources: [Arc<StaticSource>; 3] =
                std::array::from_fn(|index| Arc::new(source(&run, index, now)));
            let group = MssqlGroup::assemble(
                &root,
                &run,
                &native,
                sources.clone().map(|source| source as Source),
                std::array::from_fn(|_| Arc::new(FixedClock(AtomicU64::new(now))) as Clock),
                Duration::from_secs(60),
                StdInstant::now() + Duration::from_secs(120),
            )
            .await
            .unwrap();

            let mut start_drift = sources[0].snapshot.clone();
            start_drift.instance.sqlserver_start_time = "2026-10-07T01:02:03".into();
            let error = validate_bound_observation(
                0,
                &native,
                &group.topology_bindings[0],
                &start_drift,
                sources[0].observer_config(),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("incarnation evidence"),
                "{error}"
            );

            let mut health_drift = sources[0].snapshot.clone();
            let local = present_group(&health_drift)
                .unwrap()
                .replicas
                .iter()
                .position(|replica| replica.state.is_some())
                .unwrap();
            let group_snapshot = match &mut health_drift.availability_group {
                Observation::Present { value, .. } => value,
                _ => unreachable!(),
            };
            group_snapshot.replicas[local]
                .state
                .as_mut()
                .unwrap()
                .synchronization_health = Some("NOT_HEALTHY".into());
            let error = validate_bound_observation(
                0,
                &native,
                &group.topology_bindings[0],
                &health_drift,
                sources[0].observer_config(),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("synchronization health"),
                "{error}"
            );

            group.shutdown().await.unwrap();
            fs::remove_dir_all(&root).unwrap();
        });
    }

    fn run_group_test(future: impl Future<Output = ()> + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(32 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(future);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    fn test_root(name: &str) -> PathBuf {
        static SEQUENCE: AtomicUsize = AtomicUsize::new(1);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .join("target")
            .join(format!(
                "phase7-{name}-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        fs::create_dir_all(&root).unwrap();
        root.canonicalize().unwrap()
    }

    fn run(root: &Path) -> TopologyRun {
        TopologyRun {
            run_id: "phase7test".to_owned(),
            resource_uid: "mssql-phase7test".to_owned(),
            members: std::array::from_fn(|index| SqlMember {
                ordinal: (index + 1) as u8,
                server_name: format!("sql-{index}"),
                container_name: format!("container-{index}"),
                data_directory: root.join(format!("member-{}", index + 1)),
            }),
            kuberic_members: std::array::from_fn(|index| KubericMember {
                ordinal: (index + 1) as u8,
                replica_id: index as i64 + 1,
                instance_id: format!("instance-{}", index + 1),
                pod_uid: format!("pod-{}", index + 1),
                pvc_uid: format!("pvc-{}", index + 1),
            }),
        }
    }

    fn native_binding(run: &TopologyRun) -> NativeTopologyBinding {
        NativeTopologyBinding {
            session_id: run.run_id.clone(),
            availability_group_name: format!("km_ag_{}", run.run_id),
            availability_group_id: AG_ID.to_owned(),
            configuration_sequence: 42,
            database_name: format!("km_db_{}", run.run_id),
            group_database_id: DATABASE_ID.to_owned(),
            family_guid: FAMILY_ID.to_owned(),
            recovery_fork_id: FORK_ID.to_owned(),
            seeding_operation_ids: ["seed-1".to_owned(), "seed-2".to_owned()],
            members: std::array::from_fn(|index| NativeMemberBinding {
                ordinal: (index + 1) as u8,
                server_name: run.members[index].server_name.clone(),
                container_id: format!("container-id-{}", index + 1),
                sql_start_unix_millis: 1000 + index as i64,
                native_replica_id: NATIVE_IDS[index].to_owned(),
                local_database_id: 5,
                database_guid: DATABASE_GUIDS[index].to_owned(),
                role: if index == 0 {
                    "PRIMARY".to_owned()
                } else {
                    "SECONDARY".to_owned()
                },
                endpoint_url: format!("TCP://{}:5022", run.members[index].server_name),
                endpoint_name: "kuberic_hadr".to_owned(),
                endpoint_port: 5022,
                endpoint_certificate_name: format!("certificate-{}", index + 1),
                endpoint_certificate_thumbprint: format!("thumbprint-{}", index + 1),
            }),
        }
    }

    fn source(run: &TopologyRun, index: usize, now: u64) -> StaticSource {
        let incarnation = format!("container-id-{}:{}", index + 1, 1000 + index as i64);
        let config = ObserverConfig::from_json(
            format!(
                r#"{{
                    "host":"localhost",
                    "port":1433,
                    "availability_group":"km_ag_{}",
                    "expected_server_name":"sql-{index}",
                    "replica_id":"{}",
                    "incarnation":"{incarnation}",
                    "observer_username_file":"/secrets/username",
                    "observer_password_file":"/secrets/password",
                    "sample_timeout_ms":1000,
                    "connect_timeout_ms":1000,
                    "query_timeout_ms":1000,
                    "poll_interval_ms":1000,
                    "max_age_ms":300000
                }}"#,
                run.run_id,
                index + 1
            )
            .as_bytes(),
        )
        .unwrap();
        StaticSource {
            config,
            snapshot: snapshot(run, index, now, &incarnation),
            observations: AtomicUsize::new(0),
        }
    }

    fn snapshot(
        run: &TopologyRun,
        local_index: usize,
        now: u64,
        incarnation: &str,
    ) -> InstanceSnapshot {
        let native_role = if local_index == 0 {
            NativeRole::Primary
        } else {
            NativeRole::Secondary
        };
        let lineage = DatabaseLineage {
            database: DatabaseIdentity {
                name: SqlIdentifier::new(format!("km_db_{}", run.run_id)).unwrap(),
                group_database_id: guid(DATABASE_ID),
            },
            recovery_fork_id: guid(FORK_ID),
        };
        InstanceSnapshot {
            observed_at_unix_millis: now,
            instance: InstanceMetadata {
                server_name: ServerName::new(format!("sql-{local_index}")).unwrap(),
                property_server_name: ServerName::new(format!("sql-{local_index}")).unwrap(),
                product_version: "17.0.5005.3".into(),
                product_major_version: 17,
                edition: "Enterprise Developer Edition (64-bit)".into(),
                engine_edition: 3,
                hadr_enabled: true,
                host_platform: "Linux".into(),
                host_distribution: Some("Ubuntu".into()),
                architecture: "x86_64".into(),
                sqlserver_start_time: format!("2026-10-07T00:00:0{local_index}"),
            },
            availability_group: Observation::Present {
                value: AvailabilityGroupSnapshot {
                    identity: AvailabilityGroupIdentity {
                        name: AvailabilityGroupName::new(format!("km_ag_{}", run.run_id)).unwrap(),
                        group_id: guid(AG_ID),
                    },
                    configuration_sequence: ConfigurationSequence::parse("42").unwrap(),
                    cluster_type: "EXTERNAL".into(),
                    required_synchronized_secondaries_to_commit: 1,
                    basic_features: false,
                    is_distributed: false,
                    local_replica: LocalReplicaSnapshot {
                        identity: SqlReplicaIdentity::observed(
                            (local_index + 1).to_string(),
                            guid(NATIVE_IDS[local_index]),
                            incarnation,
                        )
                        .unwrap(),
                        state_available: true,
                        role: Some(native_role.clone()),
                    },
                    replicas: (0..3)
                        .map(|index| ReplicaSnapshot {
                            replica_id: guid(NATIVE_IDS[index]),
                            server_name: ServerName::new(format!("sql-{index}")).unwrap(),
                            endpoint_url: Some(format!("TCP://sql-{index}:5022")),
                            availability_mode: "SYNCHRONOUS_COMMIT".into(),
                            failover_mode: "EXTERNAL".into(),
                            seeding_mode: "AUTOMATIC".into(),
                            state: (index == local_index).then_some(ReplicaState {
                                provenance: NativeProvenance::Local,
                                role: Some(native_role.clone()),
                                operational_state: Some("ONLINE".into()),
                                connected_state: Some("CONNECTED".into()),
                                recovery_health: Some("ONLINE".into()),
                                synchronization_health: Some("HEALTHY".into()),
                                last_connect_error_number: Some(0),
                            }),
                        })
                        .collect(),
                    databases: vec![DatabaseSnapshot {
                        identity: lineage.database.clone(),
                        local: Some(LocalDatabaseSnapshot {
                            database_id: 5,
                            replica_id: guid(NATIVE_IDS[local_index]),
                            state: Some("ONLINE".into()),
                            recovery_model: Some("FULL".into()),
                            recovery: Some(LocalRecoveryMetadata {
                                database_guid: Some(guid(DATABASE_GUIDS[local_index])),
                                family_guid: Some(guid(FAMILY_ID)),
                                recovery_fork_guid: Some(guid(FORK_ID)),
                                first_recovery_fork_guid: Some(guid(FORK_ID)),
                                fork_point_lsn: None,
                            }),
                        }),
                        replicas: vec![DatabaseReplicaSnapshot {
                            group_database_id: guid(DATABASE_ID),
                            replica_id: guid(NATIVE_IDS[local_index]),
                            database_id: 5,
                            provenance: NativeProvenance::Local,
                            lineage: RecoveryLineageObservation::Local { value: lineage },
                            is_primary_replica: Some(local_index == 0),
                            synchronization_state: Some("SYNCHRONIZED".into()),
                            synchronization_health: Some("HEALTHY".into()),
                            database_state: Some("ONLINE".into()),
                            is_suspended: Some(false),
                            suspend_reason: None,
                            is_commit_participant: Some(true),
                            progress: NativeProgress {
                                hardened_block: Some(DecimalProgress::parse("100").unwrap()),
                                redone_record: Some(DecimalProgress::parse("100").unwrap()),
                                committed_record: Some(DecimalProgress::parse("100").unwrap()),
                            },
                        }],
                    }],
                    automatic_seeding: Vec::new(),
                    physical_seeding: Vec::new(),
                },
                observed_at_unix_millis: now,
            },
        }
    }

    fn guid(value: &str) -> Guid {
        Guid::parse("test GUID", value).unwrap()
    }
}
