use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use kuberic_runtime::application::{OpenContext, OpenMode, RoleChange, StatefulServiceReplica};
use kuberic_runtime::host::{
    ApplicationStorageState, KubernetesDnsResolver, ReplicaEndpointResolver, ReplicaHost,
    RunningReplica,
};
use kuberic_runtime::protocol::types::{
    ReplicaIdentity, derive_agent_generation, derive_initialization_id,
};
use kuberic_runtime::{Replicator, Result as KubericResult, RuntimeError as KubericRuntimeError};

use crate::instance::SqlServerInstanceManager;
use crate::kuberic::{
    ObservationClock, SqlServerObservationSource, SqlServerService, SqlServerServiceConfig,
    SystemObservationClock,
};
use crate::tds::TdsExecutor;

use super::binding::{RuntimeBindingError, RuntimeBindingStore};
use super::config::{ResolverConfig, RuntimeHostArgs, RuntimeHostConfig};
use super::routes::PeerRoutes;

const APPLICATION_STORAGE_NAME: &str = "mssql-runtime-binding";

pub async fn run_from_env() -> ExitCode {
    let result = async {
        let current_directory = std::env::current_dir()
            .map_err(|_| RuntimeProcessError::new("runtime working directory is unavailable"))?;
        let config = RuntimeHostConfig::load(RuntimeHostArgs::parse(), &current_directory)
            .await
            .map_err(RuntimeProcessError::host)?;
        run_runtime(config).await
    }
    .await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kuberic-mssql-runtime: {error}");
            ExitCode::FAILURE
        }
    }
}

pub async fn run_runtime(config: RuntimeHostConfig) -> Result<(), RuntimeProcessError> {
    run_runtime_with_shutdown(config, shutdown_signal()).await
}

pub async fn run_runtime_with_shutdown(
    config: RuntimeHostConfig,
    shutdown: impl Future<Output = Result<(), RuntimeProcessError>> + Send + 'static,
) -> Result<(), RuntimeProcessError> {
    prepare_data_root(config.data_root())?;
    let executor = TdsExecutor::new(config.observer().connection().clone());
    let manager = SqlServerInstanceManager::new(executor, config.observer().clone());
    let application = Arc::new(RuntimeHostApplication::with_observation_source(
        &config,
        Arc::new(manager),
        Arc::new(SystemObservationClock),
    )?);
    let storage = application.storage_state()?;
    let storage_paths = application.storage_paths();
    run_host_with_shutdown(config, application, storage, Some(storage_paths), shutdown).await
}

#[cfg(feature = "kuberic-testing")]
pub async fn testing_run_host_with_shutdown<A>(
    config: RuntimeHostConfig,
    application: Arc<A>,
    storage: ApplicationStorageState,
    storage_paths: Option<BTreeMap<String, std::path::PathBuf>>,
    shutdown: impl Future<Output = Result<(), RuntimeProcessError>> + Send + 'static,
) -> Result<(), RuntimeProcessError>
where
    A: StatefulServiceReplica + 'static,
{
    prepare_data_root(config.data_root())?;
    run_host_with_shutdown(config, application, storage, storage_paths, shutdown).await
}

async fn run_host_with_shutdown<A>(
    config: RuntimeHostConfig,
    application: Arc<A>,
    storage: ApplicationStorageState,
    storage_paths: Option<BTreeMap<String, std::path::PathBuf>>,
    shutdown: impl Future<Output = Result<(), RuntimeProcessError>> + Send + 'static,
) -> Result<(), RuntimeProcessError>
where
    A: StatefulServiceReplica + 'static,
{
    let resolver = Arc::new(RuntimeEndpointResolver::new(&config)?);
    let mut host = ReplicaHost::new(
        config.replica_process_config(),
        application.clone(),
        storage,
        resolver,
    );
    if let Some(storage_paths) = storage_paths {
        host = host.with_application_storage_paths(storage_paths);
    }

    let shutdown_deadline = config.shutdown_deadline();
    tokio::pin!(shutdown);
    let (startup_shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut startup = tokio::spawn(host.start_with_shutdown(receiver));
    let (started, trigger) = tokio::select! {
        result = &mut startup => (result, None),
        result = &mut shutdown => {
            startup_shutdown.send_replace(true);
            match tokio::time::timeout(shutdown_deadline, &mut startup).await {
                Ok(started) => (started, Some(result)),
                Err(_) => {
                    application.abort();
                    let trigger = combine(
                        result,
                        Err(RuntimeProcessError::new(
                            "runtime startup cancellation exceeded its deadline",
                        )),
                        "startup cancellation",
                    );
                    let termination = abort_startup_task(&mut startup, shutdown_deadline).await;
                    let result = combine(trigger, termination, "startup task termination");
                    return finish_application(application.as_ref(), result, shutdown_deadline)
                        .await;
                }
            }
        }
    };
    let mut replica = match started {
        Ok(Ok(Some(replica))) => {
            if let Some(trigger) = trigger {
                return finish_shutdown(
                    replica,
                    application.as_ref(),
                    None,
                    trigger,
                    shutdown_deadline,
                )
                .await;
            }
            replica
        }
        result => {
            let startup_result = match result {
                Ok(Ok(None)) => Ok(()),
                Ok(Err(error)) => Err(RuntimeProcessError::host(error)),
                Err(error) => Err(RuntimeProcessError::task(error)),
                Ok(Ok(Some(_))) => unreachable!(),
            };
            let result = combine(
                startup_result,
                trigger.unwrap_or(Ok(())),
                "shutdown trigger",
            );
            return finish_application(application.as_ref(), result, shutdown_deadline).await;
        }
    };

    let (completion, trigger) = tokio::select! {
        result = replica.wait() => (Some(result), Ok(())),
        result = &mut shutdown => (None, result),
    };
    finish_shutdown(
        replica,
        application.as_ref(),
        completion,
        trigger,
        shutdown_deadline,
    )
    .await
}

async fn finish_shutdown(
    mut replica: RunningReplica,
    application: &impl StatefulServiceReplica,
    completion: Option<kuberic_runtime::host::Result<()>>,
    trigger: Result<(), RuntimeProcessError>,
    deadline: std::time::Duration,
) -> Result<(), RuntimeProcessError> {
    replica.shutdown();
    let completion = match completion {
        Some(result) => result.map_err(RuntimeProcessError::host),
        None => match tokio::time::timeout(deadline, replica.wait()).await {
            Ok(result) => result.map_err(RuntimeProcessError::host),
            Err(_) => {
                application.abort();
                let joined = match tokio::time::timeout(deadline, replica.wait()).await {
                    Ok(result) => result.map_err(RuntimeProcessError::host),
                    Err(_) => Err(RuntimeProcessError::new(
                        "runtime completion after abort timed out",
                    )),
                };
                combine(
                    Err(RuntimeProcessError::new(
                        "runtime shutdown acknowledgement timed out",
                    )),
                    joined,
                    "runtime completion after timeout",
                )
            }
        },
    };
    let result = combine(completion, trigger, "shutdown trigger");
    finish_application(application, result, deadline).await
}

async fn abort_startup_task(
    startup: &mut tokio::task::JoinHandle<kuberic_runtime::host::Result<Option<RunningReplica>>>,
    deadline: std::time::Duration,
) -> Result<(), RuntimeProcessError> {
    startup.abort();
    match tokio::time::timeout(deadline, startup).await {
        Ok(Err(error)) if error.is_cancelled() => Ok(()),
        Ok(Err(error)) => Err(RuntimeProcessError::task(error)),
        Ok(Ok(Err(error))) => Err(RuntimeProcessError::host(error)),
        Ok(Ok(Ok(None))) => Ok(()),
        Ok(Ok(Ok(Some(mut replica)))) => {
            replica.shutdown();
            match tokio::time::timeout(deadline, replica.wait()).await {
                Ok(result) => result.map_err(RuntimeProcessError::host),
                Err(_) => Err(RuntimeProcessError::new(
                    "runtime startup task completion after abort timed out",
                )),
            }
        }
        Err(_) => Err(RuntimeProcessError::new(
            "runtime startup task abort timed out",
        )),
    }
}

async fn finish_application(
    application: &impl StatefulServiceReplica,
    result: Result<(), RuntimeProcessError>,
    deadline: std::time::Duration,
) -> Result<(), RuntimeProcessError> {
    let cleanup = match tokio::time::timeout(deadline, application.close()).await {
        Ok(result) => result.map_err(RuntimeProcessError::application),
        Err(_) => Err(RuntimeProcessError::new(
            "runtime application shutdown timed out",
        )),
    };
    if cleanup.is_err() {
        application.abort();
    }
    combine(result, cleanup, "application shutdown")
}

fn combine(
    primary: Result<(), RuntimeProcessError>,
    cleanup: Result<(), RuntimeProcessError>,
    context: &str,
) -> Result<(), RuntimeProcessError> {
    match (primary, cleanup) {
        (Err(primary), Err(cleanup)) => Err(RuntimeProcessError::new(format!(
            "{primary}; {context}: {cleanup}"
        ))),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

pub struct RuntimeHostApplication {
    inner: Arc<SqlServerService>,
    binding: RuntimeBindingStore,
}

impl RuntimeHostApplication {
    pub fn with_observation_source(
        config: &RuntimeHostConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
    ) -> Result<Self, RuntimeProcessError> {
        let binding = RuntimeBindingStore::new(config).map_err(RuntimeProcessError::binding)?;
        let inner = Arc::new(
            SqlServerService::with_observation_source_and_topology(
                SqlServerServiceConfig::new(
                    config.resource_uid().clone(),
                    config.replication_endpoint(),
                )
                .map_err(RuntimeProcessError::application)?,
                source,
                clock,
                config.topology().clone(),
            )
            .map_err(RuntimeProcessError::application)?,
        );
        Ok(Self { inner, binding })
    }

    pub fn storage_state(&self) -> Result<ApplicationStorageState, RuntimeProcessError> {
        self.binding
            .storage_state()
            .map_err(RuntimeProcessError::binding)
    }

    pub fn storage_paths(&self) -> BTreeMap<String, std::path::PathBuf> {
        BTreeMap::from([(
            APPLICATION_STORAGE_NAME.to_owned(),
            self.binding.root().to_owned(),
        )])
    }
}

#[async_trait]
impl StatefulServiceReplica for RuntimeHostApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> KubericResult<Arc<dyn Replicator>> {
        match context.mode {
            OpenMode::New => match self.binding.storage_state().map_err(binding_error)? {
                ApplicationStorageState::FreshEmpty => {
                    self.binding.initialize().map_err(binding_error)?;
                }
                ApplicationStorageState::Established => {
                    self.binding.validate().map_err(binding_error)?;
                }
            },
            OpenMode::Existing => self.binding.validate().map_err(binding_error)?,
        }
        self.inner.clone().open(context).await
    }

    async fn change_role(
        &self,
        role: kuberic_runtime::protocol::types::ReplicaRole,
    ) -> KubericResult<RoleChange> {
        self.inner.change_role(role).await
    }

    async fn close(&self) -> KubericResult<()> {
        self.inner.close().await
    }

    fn abort(&self) {
        self.inner.abort();
    }
}

fn binding_error(error: RuntimeBindingError) -> KubericRuntimeError {
    KubericRuntimeError::Application(error.to_string())
}

pub enum RuntimeEndpointResolver {
    KubernetesDns(KubernetesDnsResolver),
    PeerRoutes {
        local_identity: ReplicaIdentity,
        control_endpoint: String,
        replication_endpoint: String,
        routes: PeerRoutes,
    },
}

impl RuntimeEndpointResolver {
    pub fn new(config: &RuntimeHostConfig) -> Result<Self, RuntimeProcessError> {
        let initialization = derive_initialization_id(
            config.resource_uid(),
            config.replica_id(),
            config.pod_uid(),
            config.pvc_uid(),
        );
        let local_identity = ReplicaIdentity {
            replica_id: config.replica_id(),
            instance_id: kuberic_runtime::protocol::types::ReplicaInstanceId::new(
                config.pod_uid().as_str(),
            ),
            agent_generation: derive_agent_generation(&initialization),
        };
        Ok(match config.resolver() {
            ResolverConfig::KubernetesDns { namespace } => Self::KubernetesDns(
                KubernetesDnsResolver::new(config.resource_uid().clone(), namespace.clone()),
            ),
            ResolverConfig::PeerRoutes(routes) => Self::PeerRoutes {
                local_identity,
                control_endpoint: config.control_endpoint().to_owned(),
                replication_endpoint: config.replication_endpoint().to_owned(),
                routes: routes.clone(),
            },
        })
    }
}

impl ReplicaEndpointResolver for RuntimeEndpointResolver {
    fn control_endpoint(&self, identity: &ReplicaIdentity) -> String {
        match self {
            Self::KubernetesDns(resolver) => resolver.control_endpoint(identity),
            Self::PeerRoutes {
                local_identity,
                control_endpoint,
                routes,
                ..
            } => {
                if identity == local_identity {
                    control_endpoint.clone()
                } else {
                    routes
                        .get(identity)
                        .map(|route| route.control_endpoint().to_owned())
                        .unwrap_or_default()
                }
            }
        }
    }

    fn replication_endpoint(&self, identity: &ReplicaIdentity) -> String {
        match self {
            Self::KubernetesDns(resolver) => resolver.replication_endpoint(identity),
            Self::PeerRoutes {
                local_identity,
                replication_endpoint,
                routes,
                ..
            } => {
                if identity == local_identity {
                    replication_endpoint.clone()
                } else {
                    routes
                        .get(identity)
                        .map(|route| route.replication_endpoint().to_owned())
                        .unwrap_or_default()
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProcessError {
    message: String,
}

impl RuntimeProcessError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn binding(error: RuntimeBindingError) -> Self {
        Self::new(error.to_string())
    }

    fn host(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }

    fn application(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }

    fn task(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }
}

impl fmt::Display for RuntimeProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RuntimeProcessError {}

fn prepare_data_root(path: &Path) -> Result<(), RuntimeProcessError> {
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| RuntimeProcessError::new("runtime data root is unavailable"))?;
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(RuntimeProcessError::new("runtime data root is unsafe"));
        }
        return Ok(());
    }
    std::fs::DirBuilder::new()
        .recursive(false)
        .mode(0o700)
        .create(path)
        .map_err(|_| RuntimeProcessError::new("runtime data root cannot be created"))
}

async fn shutdown_signal() -> Result<(), RuntimeProcessError> {
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|_| RuntimeProcessError::new("runtime shutdown signal is unavailable"))?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.map_err(|_| RuntimeProcessError::new("runtime shutdown signal is unavailable"))
        }
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[test]
    fn completion_never_converts_primary_or_cleanup_failure_to_success() {
        let combined = combine(
            Err(RuntimeProcessError::new("runtime failed")),
            Err(RuntimeProcessError::new("cleanup failed")),
            "application shutdown",
        )
        .unwrap_err();
        assert_eq!(
            combined.to_string(),
            "runtime failed; application shutdown: cleanup failed"
        );
        assert_eq!(
            combine(
                Err(RuntimeProcessError::new("runtime failed")),
                Ok(()),
                "cleanup",
            )
            .unwrap_err()
            .to_string(),
            "runtime failed"
        );
        assert_eq!(
            combine(
                Ok(()),
                Err(RuntimeProcessError::new("cleanup failed")),
                "cleanup",
            )
            .unwrap_err()
            .to_string(),
            "cleanup failed"
        );
    }

    #[tokio::test]
    async fn startup_task_abort_is_bounded() {
        let mut startup = tokio::spawn(pending::<
            kuberic_runtime::host::Result<Option<RunningReplica>>,
        >());
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            abort_startup_task(&mut startup, std::time::Duration::from_millis(100)),
        )
        .await
        .expect("startup task abort remains bounded")
        .unwrap();
    }
}
