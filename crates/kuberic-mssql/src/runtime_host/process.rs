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
#[cfg(feature = "kuberic-testing")]
use crate::kuberic::{SqlServerBootstrapConfig, SqlServerReplicator, TargetHostEvidenceExchange};
#[cfg(feature = "kuberic-testing")]
use crate::mutation::SqlMutationExecutor;
use crate::tds::TdsExecutor;

use super::binding::{RuntimeBindingError, RuntimeBindingStore};
use super::config::{ResolverConfig, RuntimeHostArgs, RuntimeHostConfig};
use super::routes::PeerRoutes;

mod retained;

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
    run_host_with_shutdown(
        config,
        application.clone(),
        Some(application),
        storage,
        Some(storage_paths),
        shutdown,
    )
    .await
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
    run_host_with_shutdown(config, application, None, storage, storage_paths, shutdown).await
}

async fn run_host_with_shutdown<A>(
    config: RuntimeHostConfig,
    application: Arc<A>,
    concrete: Option<Arc<RuntimeHostApplication>>,
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

    let control_address = config.control_address();
    retained::run(
        application,
        concrete,
        move |receiver| async move {
            let control_listener = tokio::net::TcpListener::bind(control_address).await?;
            host.start_with_shutdown(control_listener, receiver).await
        },
        shutdown,
        config.shutdown_deadline(),
    )
    .await
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
    aborted: tokio_util::sync::CancellationToken,
    terminal_entry: std::sync::Mutex<()>,
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
        Ok(Self::new(inner, binding))
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_with_bootstrap_sources(
        config: &RuntimeHostConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        bootstrap: SqlServerBootstrapConfig,
        mutation: Arc<dyn SqlMutationExecutor>,
    ) -> Result<Self, RuntimeProcessError> {
        let binding = RuntimeBindingStore::new(config).map_err(RuntimeProcessError::binding)?;
        let inner = Arc::new(
            SqlServerService::with_bootstrap_source_and_topology(
                SqlServerServiceConfig::new(
                    config.resource_uid().clone(),
                    config.replication_endpoint(),
                )
                .map_err(RuntimeProcessError::application)?,
                source,
                clock,
                config.topology().clone(),
                bootstrap,
                mutation,
                binding.root().to_owned(),
            )
            .map_err(RuntimeProcessError::application)?,
        );
        Ok(Self::new(inner, binding))
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_with_bootstrap_sources_and_target_evidence(
        config: &RuntimeHostConfig,
        source: Arc<dyn SqlServerObservationSource>,
        clock: Arc<dyn ObservationClock>,
        bootstrap: SqlServerBootstrapConfig,
        mutation: Arc<dyn SqlMutationExecutor>,
        target_evidence: Arc<dyn TargetHostEvidenceExchange>,
    ) -> Result<Self, RuntimeProcessError> {
        let binding = RuntimeBindingStore::new(config).map_err(RuntimeProcessError::binding)?;
        let inner = Arc::new(
            SqlServerService::with_bootstrap_source_and_topology_and_target_evidence(
                SqlServerServiceConfig::new(
                    config.resource_uid().clone(),
                    config.replication_endpoint(),
                )
                .map_err(RuntimeProcessError::application)?,
                source,
                clock,
                config.topology().clone(),
                bootstrap,
                mutation,
                target_evidence,
                binding.root().to_owned(),
            )
            .map_err(RuntimeProcessError::application)?,
        );
        Ok(Self::new(inner, binding))
    }

    fn new(inner: Arc<SqlServerService>, binding: RuntimeBindingStore) -> Self {
        Self {
            inner,
            binding,
            aborted: tokio_util::sync::CancellationToken::new(),
            terminal_entry: std::sync::Mutex::new(()),
        }
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

    /// Waits for MSSQL-local drainage, not generic host post-Abort containment.
    pub async fn wait_for_containment(&self) -> KubericResult<()> {
        self.inner.wait_for_containment().await.map_err(Into::into)
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_replicator(&self) -> Option<Arc<SqlServerReplicator>> {
        self.inner.replicator()
    }
}

#[async_trait]
impl StatefulServiceReplica for RuntimeHostApplication {
    async fn open(self: Arc<Self>, context: OpenContext) -> KubericResult<Arc<dyn Replicator>> {
        match context.mode {
            OpenMode::New => match self.binding.storage_state().map_err(binding_error)? {
                ApplicationStorageState::FreshEmpty => {
                    self.binding
                        .initialize(&context.identity)
                        .map_err(binding_error)?;
                }
                ApplicationStorageState::Established => {
                    return Err(binding_error(RuntimeBindingError::AlreadyInitialized));
                }
            },
            OpenMode::Existing => self
                .binding
                .validate(&context.identity)
                .map_err(binding_error)?,
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
        let close = self.inner.close();
        tokio::pin!(close);
        let result = futures::future::poll_fn(|cx| {
            let _entry = self
                .terminal_entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.aborted.is_cancelled() {
                return std::task::Poll::Ready(None);
            }
            // The first poll publishes the service's terminal state synchronously.
            // Serialize that entry with Abort, never the asynchronous drain.
            close.as_mut().poll(cx).map(Some)
        })
        .await;
        match result {
            Some(result) => result,
            None => self.wait_for_containment().await,
        }
    }

    fn abort(&self) {
        let _entry = self
            .terminal_entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.abort();
        // Publish only after fencing: another runtime worker may immediately
        // begin the inherent wait when this token wakes it.
        self.aborted.cancel();
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
}
