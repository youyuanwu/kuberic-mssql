use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use kuberic_mssql::instance::SqlServerInstanceManager;
use kuberic_mssql::kuberic::SqlServerObservationSource;
use kuberic_mssql::observation::InstanceSnapshot;
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::runtime_host::{
    RuntimeEndpointResolver, RuntimeHostApplication, RuntimeHostArgs, RuntimeHostConfig,
};
use kuberic_mssql::tds::TdsExecutor;
use kuberic_mssql::{NativeRole, Observation};
use kuberic_runtime::RuntimeError as KubericRuntimeError;
use kuberic_runtime::application::StatefulServiceReplica;
use kuberic_runtime::control::proto::{self as wire, agent_control_client::AgentControlClient};
use kuberic_runtime::host::{HostError, ReplicaHost, RunningReplica};
use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch, PodUid, PvcUid,
    ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
    derive_agent_generation, derive_initialization_id,
};
use tonic::{Request, transport::Channel};

use super::cleanup::{CleanupClock, CleanupCoordinator};
use super::kuberic_group::{MSSQL_FAILOVER_DELAY_SECONDS, MssqlGroupError};
use super::member::ReadyMember;
use super::model::{NativeTopologyBinding, TopologyRun};

const AGENT_TOKEN: &str = "mssql-three-replica-agent";

type Source = Arc<dyn SqlServerObservationSource>;

pub struct PublicMssqlPod {
    pub ordinal: u8,
    pub identity: ReplicaIdentity,
    pub session: kuberic_runtime::protocol::types::ProcessSessionId,
    pub stable_role: ReplicaRole,
    pub pod_uid: PodUid,
    pub pvc_uid: PvcUid,
    resource_uid: ResourceUid,
    control_address: SocketAddr,
    running: Option<RunningReplica>,
    source: Source,
    application: Arc<RuntimeHostApplication>,
}

pub struct PublicMssqlGroup {
    pub resource_uid: ResourceUid,
    pub effective_policy: EffectivePolicy,
    pub configuration: ConfigurationDescriptor,
    pub pods: [PublicMssqlPod; 3],
    native_binding: NativeTopologyBinding,
    operation_timeout: Duration,
    complete_deadline: Instant,
}

struct HostAttempt {
    shutdown: tokio::sync::watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<kuberic_runtime::host::Result<Option<RunningReplica>>>>,
}

struct RuntimeConfigContext<'a> {
    root: &'a Path,
    run: &'a TopologyRun,
    native: &'a NativeTopologyBinding,
    members: &'a [ReadyMember; 3],
    controls: &'a [SocketAddr; 3],
    replications: &'a [SocketAddr; 3],
    identities: &'a [ReplicaIdentity; 3],
}

impl PublicMssqlGroup {
    pub async fn from_live_with_coordinator(
        root: &Path,
        run: &TopologyRun,
        native_binding: &NativeTopologyBinding,
        members: &[ReadyMember; 3],
        convergence_timeout: Duration,
        complete_deadline: Instant,
        cleanup: &CleanupCoordinator<impl CleanupClock>,
    ) -> Result<Self, MssqlGroupError> {
        check_deadline(complete_deadline, "public Kuberic assembly")?;
        let resource_uid = ResourceUid::new(run.resource_uid.clone());
        let identities = identities(run, &resource_uid);
        let roles = stable_roles(native_binding)?;
        let configuration = configuration(native_binding, &identities, roles)?;
        let effective_policy = EffectivePolicy::fixed(3, MSSQL_FAILOVER_DELAY_SECONDS)
            .ok_or_else(|| MssqlGroupError::new("three-member policy is invalid"))?;
        let controls: [SocketAddr; 3] = (0..3)
            .map(|_| free_address())
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three control addresses required"))?;
        let replications: [SocketAddr; 3] = (0..3)
            .map(|_| free_address())
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three replication addresses required"))?;
        let operation_timeout =
            convergence_timeout.min(remaining(complete_deadline, "public Kuberic assembly")?);

        let mut attempts = Vec::new();
        let mut pods = Vec::new();
        let config_context = RuntimeConfigContext {
            root,
            run,
            native: native_binding,
            members,
            controls: &controls,
            replications: &replications,
            identities: &identities,
        };
        for index in 0..3 {
            let config = runtime_config(&config_context, index).await?;
            let observer = ObserverConfig::read(&members[index].observer_config)
                .await
                .map_err(display_error)?;
            let source = Arc::new(SqlServerInstanceManager::new(
                TdsExecutor::new(observer.connection().clone()),
                observer,
            )) as Source;
            let application = Arc::new(
                RuntimeHostApplication::with_observation_source(
                    &config,
                    source.clone(),
                    Arc::new(kuberic_mssql::kuberic::SystemObservationClock),
                )
                .map_err(display_error)?,
            );
            let host = ReplicaHost::new(
                config.replica_process_config(),
                application.clone(),
                application.storage_state().map_err(display_error)?,
                Arc::new(RuntimeEndpointResolver::new(&config).map_err(display_error)?),
            )
            .with_application_storage_paths(application.storage_paths());
            let (shutdown, receiver) = tokio::sync::watch::channel(false);
            attempts.push(HostAttempt {
                shutdown,
                task: Some(tokio::spawn(host.start_with_shutdown(receiver))),
            });
            pods.push(PublicMssqlPod {
                ordinal: index as u8 + 1,
                identity: identities[index].clone(),
                session: Default::default(),
                stable_role: roles[index],
                pod_uid: PodUid::new(run.kuberic_members[index].pod_uid.clone()),
                pvc_uid: PvcUid::new(run.kuberic_members[index].pvc_uid.clone()),
                resource_uid: resource_uid.clone(),
                control_address: controls[index],
                running: None,
                source,
                application,
            });
        }
        let mut pods: [PublicMssqlPod; 3] = pods
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three public Kuberic pods required"))?;

        let result = async {
            validate_observations(&pods, native_binding, operation_timeout, complete_deadline)
                .await?;
            let startup_reports =
                startup_reports(&mut pods, &mut attempts, operation_timeout).await?;
            let uninitialized = startup_reports
                .iter()
                .filter(|report| {
                    report.storage_state == wire::AgentStorageState::Uninitialized as i32
                })
                .count();
            let initialized = startup_reports
                .iter()
                .filter(|report| {
                    report.storage_state == wire::AgentStorageState::Initialized as i32
                })
                .count();
            if uninitialized != 0 && initialized != 0 {
                return Err(MssqlGroupError::new(
                    "public Kuberic stores have mixed fresh and established state",
                ));
            }
            if uninitialized == pods.len() {
                let initialization_sessions = startup_reports
                    .iter()
                    .map(|report| report.process_session_id.clone())
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| {
                        MssqlGroupError::new("exactly three initialization sessions required")
                    })?;
                initialize(
                    &pods,
                    run,
                    &identities,
                    &configuration,
                    &effective_policy,
                    &initialization_sessions,
                    operation_timeout,
                )
                .await?;
            } else if initialized != pods.len() {
                return Err(MssqlGroupError::new(
                    "public Kuberic storage state is neither fresh nor established",
                ));
            }
            for (index, attempt) in attempts.iter_mut().enumerate() {
                if pods[index].running.is_some() {
                    continue;
                }
                let mut task = attempt
                    .task
                    .take()
                    .ok_or_else(|| MssqlGroupError::new("public host task is missing"))?;
                let joined =
                    timeout(operation_timeout, "public Kuberic readiness", &mut task).await;
                let joined = match joined {
                    Ok(joined) => joined,
                    Err(error) => {
                        attempt.task = Some(task);
                        return Err(error);
                    }
                };
                let started = joined.map_err(display_error)?;
                let running = started
                    .map_err(display_error)?
                    .ok_or_else(|| MssqlGroupError::new("public Kuberic startup was cancelled"))?;
                pods[index].running = Some(running);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            let sessions = ensure_configuration(
                &pods,
                &identities,
                &resource_uid,
                &configuration,
                &effective_policy,
                operation_timeout,
            )
            .await?;
            for (pod, session) in pods.iter_mut().zip(sessions) {
                pod.session = session;
            }
            Ok::<(), MssqlGroupError>(())
        }
        .await;
        if let Err(error) = result {
            let shutdown = cancel_attempts(&mut attempts, &mut pods, cleanup).await;
            return Err(match shutdown {
                Ok(()) => error,
                Err(shutdown) => MssqlGroupError::termination_unconfirmed(format!(
                    "{error}; public Kuberic startup cleanup failed: {shutdown}"
                )),
            });
        }
        Ok(Self {
            resource_uid,
            effective_policy,
            configuration,
            pods,
            native_binding: native_binding.clone(),
            operation_timeout,
            complete_deadline,
        })
    }

    pub async fn reports_bracketed(&self) -> Result<[wire::AgentStatusReport; 3], MssqlGroupError> {
        let before =
            observe_sources(&self.pods, self.operation_timeout, self.complete_deadline).await?;
        validate_snapshots(&before, &self.native_binding)?;
        let mut reports = Vec::new();
        for pod in &self.pods {
            reports.push(status(pod, self.operation_timeout).await?.1);
        }
        let after =
            observe_sources(&self.pods, self.operation_timeout, self.complete_deadline).await?;
        validate_snapshots(&after, &self.native_binding)?;
        reports
            .try_into()
            .map_err(|_| MssqlGroupError::new("exactly three public Kuberic reports required"))
    }

    pub async fn shutdown_with_coordinator(
        mut self,
        cleanup: &CleanupCoordinator<impl CleanupClock>,
    ) -> Result<(), MssqlGroupError> {
        for pod in &self.pods {
            if let Some(running) = &pod.running {
                running.shutdown();
            }
        }
        let mut errors = Vec::new();
        for pod in &mut self.pods {
            if let Some(mut running) = pod.running.take() {
                let deadline = cleanup.remaining().min(Duration::from_secs(10));
                match tokio::time::timeout(deadline, running.wait()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(error.to_string()),
                    Err(_) => {
                        pod.application.abort();
                        match tokio::time::timeout(
                            cleanup.remaining().min(Duration::from_secs(10)),
                            running.wait(),
                        )
                        .await
                        {
                            Ok(Ok(())) => errors.push(format!(
                                "member {} shutdown exceeded its deadline",
                                pod.ordinal
                            )),
                            Ok(Err(error)) => errors.push(format!(
                                "member {} shutdown timed out and abort failed: {error}",
                                pod.ordinal
                            )),
                            Err(_) => errors.push(format!(
                                "member {} termination remains unconfirmed after abort",
                                pod.ordinal
                            )),
                        }
                    }
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(MssqlGroupError::termination_unconfirmed(format!(
                "public Kuberic shutdown failed: {errors:?}"
            )))
        }
    }
}

async fn runtime_config(
    context: &RuntimeConfigContext<'_>,
    local_index: usize,
) -> Result<RuntimeHostConfig, MssqlGroupError> {
    let runtime_root = context.run.members[local_index]
        .data_directory
        .join("kuberic-public-runtime");
    create_private_directory(&runtime_root)?;
    create_private_directory(&runtime_root.join("state"))?;
    let token = runtime_root.join("agent-token");
    write_private(&token, AGENT_TOKEN)?;
    let topology = runtime_root.join("topology.json");
    let topology_members = (0..3)
        .map(|index| {
            serde_json::json!({
                "replica_id": context.run.kuberic_members[index].replica_id,
                "server_name": context.run.members[index].server_name,
                "endpoint_url": context.native.members[index].endpoint_url
            })
        })
        .collect::<Vec<_>>();
    write_private(
        &topology,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "members": topology_members
        }))
        .map_err(display_error)?,
    )?;
    let routes = runtime_root.join("routes.json");
    let route_values = (0..3)
        .filter(|index| *index != local_index)
        .map(|index| {
            serde_json::json!({
                "replica_id": context.identities[index].replica_id.value(),
                "instance_id": context.identities[index].instance_id.as_str(),
                "agent_generation": context.identities[index].agent_generation.as_str(),
                "control_endpoint": format!("http://{}", context.controls[index]),
                "replication_endpoint": format!("http://{}", context.replications[index])
            })
        })
        .collect::<Vec<_>>();
    write_private(
        &routes,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "routes": route_values
        }))
        .map_err(display_error)?,
    )?;
    RuntimeHostConfig::load(
        RuntimeHostArgs {
            resource_uid: context.run.resource_uid.clone(),
            replica_id: context.run.kuberic_members[local_index].replica_id,
            pod_uid: context.run.kuberic_members[local_index].pod_uid.clone(),
            pvc_uid: context.run.kuberic_members[local_index].pvc_uid.clone(),
            data_root: runtime_root.join("state"),
            application_root: None,
            control_address: context.controls[local_index],
            replication_address: context.replications[local_index],
            control_endpoint: format!("http://{}", context.controls[local_index]),
            replication_endpoint: format!("http://{}", context.replications[local_index]),
            namespace: None,
            peer_routes: Some(routes),
            observer_config: context.members[local_index].observer_config.clone(),
            topology_config: topology,
            bearer_token_file: token,
            rpc_deadline_ms: 5_000,
            transport_window_capacity: 256,
            shutdown_deadline_ms: 10_000,
        },
        context.root,
    )
    .await
    .map_err(display_error)
}

fn identities(run: &TopologyRun, resource: &ResourceUid) -> [ReplicaIdentity; 3] {
    std::array::from_fn(|index| {
        let replica_id = ReplicaId::new(run.kuberic_members[index].replica_id);
        let pod_uid = PodUid::new(run.kuberic_members[index].pod_uid.clone());
        let pvc_uid = PvcUid::new(run.kuberic_members[index].pvc_uid.clone());
        let initialization = derive_initialization_id(resource, replica_id, &pod_uid, &pvc_uid);
        ReplicaIdentity {
            replica_id,
            instance_id: ReplicaInstanceId::new(pod_uid.as_str()),
            agent_generation: derive_agent_generation(&initialization),
        }
    })
}

fn stable_roles(native: &NativeTopologyBinding) -> Result<[ReplicaRole; 3], MssqlGroupError> {
    native
        .members
        .iter()
        .map(|member| match member.role.as_str() {
            "PRIMARY" => Ok(ReplicaRole::Primary),
            "SECONDARY" => Ok(ReplicaRole::ActiveSecondary),
            _ => Err(MssqlGroupError::new("native role is not stable")),
        })
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three native roles required"))
}

fn configuration(
    native: &NativeTopologyBinding,
    identities: &[ReplicaIdentity; 3],
    roles: [ReplicaRole; 3],
) -> Result<ConfigurationDescriptor, MssqlGroupError> {
    let primary = roles
        .iter()
        .position(|role| *role == ReplicaRole::Primary)
        .ok_or_else(|| MssqlGroupError::new("native topology has no primary"))?;
    Ok(ConfigurationDescriptor::new(
        Epoch::new(0, native.configuration_sequence),
        identities[primary].replica_id,
        identities
            .iter()
            .cloned()
            .zip(roles)
            .map(|(identity, role)| ConfigurationMember { identity, role })
            .collect(),
        2,
    ))
}

async fn startup_reports(
    pods: &mut [PublicMssqlPod; 3],
    attempts: &mut [HostAttempt],
    timeout_duration: Duration,
) -> Result<[wire::AgentStatusReport; 3], MssqlGroupError> {
    let mut reports = Vec::new();
    for index in 0..3 {
        enum StartupOutcome {
            Report(Box<wire::AgentStatusReport>),
            Task(
                Result<
                    kuberic_runtime::host::Result<Option<RunningReplica>>,
                    tokio::task::JoinError,
                >,
            ),
        }
        let outcome = {
            let task = attempts[index]
                .task
                .as_mut()
                .ok_or_else(|| MssqlGroupError::new("public host task is missing"))?;
            tokio::select! {
                report = async {
                    status(&pods[index], timeout_duration)
                        .await
                        .map(|(_, report)| Box::new(report))
                } => {
                    StartupOutcome::Report(report?)
                }
                result = task => StartupOutcome::Task(result),
            }
        };
        match outcome {
            StartupOutcome::Report(report) => reports.push(*report),
            StartupOutcome::Task(result) => {
                attempts[index].task.take();
                let running = result
                    .map_err(display_error)?
                    .map_err(display_error)?
                    .ok_or_else(|| MssqlGroupError::new("public host startup was cancelled"))?;
                pods[index].running = Some(running);
                reports.push(status(&pods[index], timeout_duration).await?.1);
            }
        }
    }
    reports
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three startup reports required"))
}

async fn initialize(
    pods: &[PublicMssqlPod; 3],
    run: &TopologyRun,
    identities: &[ReplicaIdentity; 3],
    configuration: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
    sessions: &[String; 3],
    timeout_duration: Duration,
) -> Result<(), MssqlGroupError> {
    let results = join_all((0..3).map(|index| async move {
        let (mut client, _) = status(&pods[index], timeout_duration).await?;
        let initialization = derive_initialization_id(
            &ResourceUid::new(run.resource_uid.clone()),
            identities[index].replica_id,
            &PodUid::new(run.kuberic_members[index].pod_uid.clone()),
            &PvcUid::new(run.kuberic_members[index].pvc_uid.clone()),
        );
        client
            .execute(authorized(wire::ExecuteCommandRequest {
                protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                resource_uid: run.resource_uid.clone(),
                target: Some(identities[index].clone().into()),
                expected_process_session_id: sessions[index].clone(),
                command: Some(
                    wire::execute_command_request::Command::InitializeAgentStore(
                        wire::InitializeAgentStoreCommand {
                            initialization_id: initialization.to_string(),
                            resource_uid: run.resource_uid.clone(),
                            local_replica_id: identities[index].replica_id.value(),
                            expected_instance_id: identities[index].instance_id.to_string(),
                            expected_pod_uid: run.kuberic_members[index].pod_uid.clone(),
                            expected_pvc_uid: run.kuberic_members[index].pvc_uid.clone(),
                            assigned_agent_generation: identities[index]
                                .agent_generation
                                .to_string(),
                            effective_policy: Some(policy.clone().into()),
                            bootstrap_configuration: Some(configuration.clone().into()),
                            provisioning: None,
                        },
                    ),
                ),
            }))
            .await
            .map_err(display_error)?;
        Ok::<(), MssqlGroupError>(())
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn ensure_configuration(
    pods: &[PublicMssqlPod; 3],
    identities: &[ReplicaIdentity; 3],
    resource_uid: &ResourceUid,
    configuration: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
    timeout_duration: Duration,
) -> Result<[kuberic_runtime::protocol::types::ProcessSessionId; 3], MssqlGroupError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let mut clients = Vec::new();
        let mut sessions = Vec::new();
        for pod in pods {
            let (client, report) = status(pod, timeout_duration).await?;
            clients.push(client);
            sessions.push(report.process_session_id);
        }
        let results = join_all(clients.into_iter().enumerate().map(|(index, mut client)| {
            let request = configuration_request(
                resource_uid,
                configuration,
                policy,
                &identities[index],
                &kuberic_runtime::protocol::types::ProcessSessionId::new(sessions[index].clone()),
                &format!("bootstrap-configuration-{}", index + 1),
            );
            async move { client.execute(request).await }
        }))
        .await;
        if results.iter().all(Result::is_ok) {
            return sessions
                .into_iter()
                .map(kuberic_runtime::protocol::types::ProcessSessionId::new)
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| MssqlGroupError::new("exactly three agent sessions required"));
        }
        let fatal = results
            .iter()
            .filter_map(|result| result.as_ref().err())
            .filter(|error| error.code() != tonic::Code::Cancelled)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !fatal.is_empty() || tokio::time::Instant::now() >= deadline {
            return Err(MssqlGroupError::new(format!(
                "public current configuration failed: {fatal:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn configuration_request(
    resource_uid: &ResourceUid,
    configuration: &ConfigurationDescriptor,
    policy: &EffectivePolicy,
    identity: &ReplicaIdentity,
    session: &kuberic_runtime::protocol::types::ProcessSessionId,
    operation_id: &str,
) -> Request<wire::ExecuteCommandRequest> {
    authorized(wire::ExecuteCommandRequest {
        protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
        resource_uid: resource_uid.to_string(),
        target: Some(identity.clone().into()),
        expected_process_session_id: session.to_string(),
        command: Some(wire::execute_command_request::Command::EnsureConfiguration(
            Box::new(wire::EnsureConfigurationCommand {
                operation_id: operation_id.into(),
                current_configuration: Some(configuration.clone().into()),
                current_epoch: Some(configuration.epoch.into()),
                effective_policy: Some(policy.clone().into()),
                local_replica_id: identity.replica_id.value(),
                expected_instance_id: identity.instance_id.to_string(),
                expected_agent_generation: identity.agent_generation.to_string(),
                transition_kind: wire::TransitionKind::Bootstrap as i32,
                primary_write_status: wire::AccessStatus::ReconfigurationPending as i32,
                ..Default::default()
            }),
        )),
    })
}

async fn status(
    pod: &PublicMssqlPod,
    timeout_duration: Duration,
) -> Result<(AgentControlClient<Channel>, wire::AgentStatusReport), MssqlGroupError> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut last: Option<String>;
    loop {
        match AgentControlClient::connect(format!("http://{}", pod.control_address)).await {
            Ok(mut client) => match client
                .get_status(authorized(wire::GetAgentStatusRequest {
                    protocol_version: kuberic_runtime::protocol::PROTOCOL_VERSION,
                    resource_uid: pod.resource_uid.to_string(),
                    replica_id: pod.identity.replica_id.value(),
                    expected_instance_id: pod.identity.instance_id.to_string(),
                }))
                .await
            {
                Ok(report) => return Ok((client, report.into_inner())),
                Err(error) => last = Some(error.to_string()),
            },
            Err(error) => last = Some(error.to_string()),
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(MssqlGroupError::new(format!(
                "public agent status unavailable: {last}",
                last = last.as_deref().unwrap_or("no response")
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {AGENT_TOKEN}")
            .parse()
            .expect("token metadata"),
    );
    request
}

async fn observe_sources(
    pods: &[PublicMssqlPod; 3],
    operation_timeout: Duration,
    complete_deadline: Instant,
) -> Result<[InstanceSnapshot; 3], MssqlGroupError> {
    let remaining = remaining(complete_deadline, "direct observation")?;
    let values = join_all(pods.iter().map(|pod| async move {
        timeout(
            operation_timeout.min(remaining),
            "direct SQL observation",
            pod.source.observe(),
        )
        .await?
        .map_err(display_error)
        .and_then(|observation| match observation {
            Observation::Present { value, .. } => Ok(value),
            Observation::Absent { .. } => Err(MssqlGroupError::new("availability group is absent")),
            Observation::Failed(failure) => Err(MssqlGroupError::new(format!(
                "direct SQL observation failed: {:?}",
                failure.kind
            ))),
        })
    }))
    .await;
    values
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| MssqlGroupError::new("exactly three direct observations required"))
}

async fn validate_observations(
    pods: &[PublicMssqlPod; 3],
    native: &NativeTopologyBinding,
    operation_timeout: Duration,
    complete_deadline: Instant,
) -> Result<(), MssqlGroupError> {
    let snapshots = observe_sources(pods, operation_timeout, complete_deadline).await?;
    validate_snapshots(&snapshots, native)
}

fn validate_snapshots(
    snapshots: &[InstanceSnapshot; 3],
    native: &NativeTopologyBinding,
) -> Result<(), MssqlGroupError> {
    for (index, snapshot) in snapshots.iter().enumerate() {
        let group = match &snapshot.availability_group {
            Observation::Present { value, .. } => value,
            _ => return Err(MssqlGroupError::new("native snapshot is unavailable")),
        };
        let member = &native.members[index];
        let expected_role = if member.role == "PRIMARY" {
            NativeRole::Primary
        } else {
            NativeRole::Secondary
        };
        if snapshot.instance.server_name.as_str() != member.server_name
            || snapshot.instance.sqlserver_start_time != member.sql_start_time
            || group.identity.group_id.as_str() != native.availability_group_id
            || group.configuration_sequence.value() != native.configuration_sequence
            || group
                .local_replica
                .identity
                .native_replica_id()
                .map(|id| id.as_str())
                != Some(member.native_replica_id.as_str())
            || group.local_replica.role.as_ref() != Some(&expected_role)
        {
            return Err(MssqlGroupError::new(
                "direct observation differs from the frozen native topology",
            ));
        }
    }
    Ok(())
}

async fn cancel_attempts(
    attempts: &mut [HostAttempt],
    pods: &mut [PublicMssqlPod; 3],
    cleanup: &CleanupCoordinator<impl CleanupClock>,
) -> Result<(), MssqlGroupError> {
    for attempt in attempts.iter() {
        attempt.shutdown.send_replace(true);
    }
    for (index, attempt) in attempts.iter_mut().enumerate() {
        let Some(mut task) = attempt.task.take() else {
            continue;
        };
        let deadline = cleanup.remaining().min(Duration::from_secs(10));
        match tokio::time::timeout(deadline, &mut task).await {
            Ok(Ok(Ok(Some(mut running)))) => {
                running.shutdown();
                tokio::time::timeout(deadline, running.wait())
                    .await
                    .map_err(|_| {
                        MssqlGroupError::termination_unconfirmed("public host shutdown timed out")
                    })?
                    .map_err(display_error)?;
            }
            Ok(Ok(Ok(None))) => {}
            Ok(Ok(Err(HostError::Runtime(KubericRuntimeError::OperationCancelled)))) => {}
            Ok(Ok(Err(error))) => return Err(display_error(error)),
            Ok(Err(error)) => return Err(display_error(error)),
            Err(_) => {
                pods[index].application.abort();
                attempt.task = Some(task);
                return Err(MssqlGroupError::termination_unconfirmed(format!(
                    "member {} startup cancellation timed out",
                    index + 1
                )));
            }
        }
        pods[index].running = None;
    }
    Ok(())
}

fn free_address() -> Result<SocketAddr, MssqlGroupError> {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map_err(display_error)
}

fn create_private_directory(path: &Path) -> Result<(), MssqlGroupError> {
    if !path.exists() {
        fs::create_dir_all(path).map_err(display_error)?;
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(display_error)
}

fn write_private(path: &Path, bytes: impl AsRef<[u8]>) -> Result<(), MssqlGroupError> {
    fs::write(path, bytes).map_err(display_error)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(display_error)
}

fn remaining(deadline: Instant, stage: &str) -> Result<Duration, MssqlGroupError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| MssqlGroupError::new(format!("{stage} exceeded its deadline")))
}

fn check_deadline(deadline: Instant, stage: &str) -> Result<(), MssqlGroupError> {
    remaining(deadline, stage).map(|_| ())
}

async fn timeout<T>(
    duration: Duration,
    stage: &str,
    future: impl std::future::Future<Output = T>,
) -> Result<T, MssqlGroupError> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| MssqlGroupError::new(format!("{stage} timed out")))
}

fn display_error(error: impl std::fmt::Display) -> MssqlGroupError {
    MssqlGroupError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn public_live_group_uses_no_kuberic_testing_or_private_host_api() {
        let source = include_str!("public_kuberic_group.rs");
        let forbidden = [
            ["kuberic_runtime::test", "ing"].concat(),
            ["Sqlite", "Store"].concat(),
            ["Runtime", "Adapter"].concat(),
            ["Runtime", "Effect"].concat(),
            ["Agent", "Service"].concat(),
        ];
        for forbidden in forbidden {
            assert!(!source.contains(&forbidden), "{forbidden}");
        }
    }
}
