use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::net::SocketAddr;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use kuberic_runtime::host::ReplicaProcessConfig;
use kuberic_runtime::protocol::types::{PodUid, PvcUid, ReplicaId, ResourceUid};
use zeroize::Zeroizing;

use crate::runtime_config::ObserverConfig;
use crate::topology_config::SqlServerTopologyExpectation;

use super::routes::{PeerRoutes, PeerRoutesError, validate_http_endpoint};

const MAX_SECRET_BYTES: u64 = 4_096;
const MAX_TOPOLOGY_BYTES: u64 = 65_536;
const MAX_DURATION_MILLIS: u64 = 300_000;
const MAX_TRANSPORT_WINDOW_CAPACITY: usize = 65_536;

#[derive(Debug, Clone, Parser)]
#[command(about = "Run the observe-only SQL Server application for Kuberic")]
pub struct RuntimeHostArgs {
    #[arg(long, env = "KUBERIC_RESOURCE_UID")]
    pub resource_uid: String,
    #[arg(long, env = "KUBERIC_REPLICA_ID")]
    pub replica_id: i64,
    #[arg(long, env = "KUBERIC_POD_UID")]
    pub pod_uid: String,
    #[arg(long, env = "KUBERIC_PVC_UID")]
    pub pvc_uid: String,
    #[arg(long, env = "KUBERIC_DATA_ROOT")]
    pub data_root: PathBuf,
    #[arg(long, env = "KUBERIC_APPLICATION_ROOT")]
    pub application_root: Option<PathBuf>,
    #[arg(
        long,
        env = "KUBERIC_CONTROL_ADDRESS",
        default_value = "127.0.0.1:50051"
    )]
    pub control_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_REPLICATION_ADDRESS",
        default_value = "127.0.0.1:50052"
    )]
    pub replication_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_CONTROL_ENDPOINT",
        default_value = "http://127.0.0.1:50051"
    )]
    pub control_endpoint: String,
    #[arg(
        long,
        env = "KUBERIC_REPLICATION_ENDPOINT",
        default_value = "http://127.0.0.1:50052"
    )]
    pub replication_endpoint: String,
    #[arg(long, env = "KUBERIC_NAMESPACE", conflicts_with = "peer_routes")]
    pub namespace: Option<String>,
    #[arg(long, env = "KUBERIC_PEER_ROUTES", conflicts_with = "namespace")]
    pub peer_routes: Option<PathBuf>,
    #[arg(long, env = "KUBERIC_MSSQL_OBSERVER_CONFIG")]
    pub observer_config: PathBuf,
    #[arg(long, env = "KUBERIC_MSSQL_TOPOLOGY_CONFIG")]
    pub topology_config: PathBuf,
    #[arg(long, env = "KUBERIC_AGENT_BEARER_TOKEN_FILE")]
    pub bearer_token_file: PathBuf,
    #[arg(long, env = "KUBERIC_RPC_DEADLINE_MS", default_value_t = 5_000)]
    pub rpc_deadline_ms: u64,
    #[arg(long, env = "KUBERIC_TRANSPORT_WINDOW_CAPACITY", default_value_t = 256)]
    pub transport_window_capacity: usize,
    #[arg(long, env = "KUBERIC_SHUTDOWN_DEADLINE_MS", default_value_t = 10_000)]
    pub shutdown_deadline_ms: u64,
}

#[derive(Debug, Clone)]
pub enum ResolverConfig {
    KubernetesDns { namespace: String },
    PeerRoutes(PeerRoutes),
}

pub struct RuntimeHostConfig {
    resource_uid: ResourceUid,
    replica_id: ReplicaId,
    pod_uid: PodUid,
    pvc_uid: PvcUid,
    data_root: PathBuf,
    application_root: PathBuf,
    control_address: SocketAddr,
    replication_address: SocketAddr,
    control_endpoint: String,
    replication_endpoint: String,
    resolver: ResolverConfig,
    observer: ObserverConfig,
    topology: SqlServerTopologyExpectation,
    bearer_token: Zeroizing<String>,
    rpc_deadline: Duration,
    transport_window_capacity: usize,
    shutdown_deadline: Duration,
}

impl fmt::Debug for RuntimeHostConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeHostConfig")
            .field("resource_uid", &self.resource_uid)
            .field("replica_id", &self.replica_id)
            .field("pod_uid", &self.pod_uid)
            .field("pvc_uid", &self.pvc_uid)
            .field("data_root", &self.data_root)
            .field("application_root", &self.application_root)
            .field("control_address", &self.control_address)
            .field("replication_address", &self.replication_address)
            .field("control_endpoint", &self.control_endpoint)
            .field("replication_endpoint", &self.replication_endpoint)
            .field("resolver", &self.resolver)
            .field("observer", &"<redacted configuration>")
            .field("topology", &self.topology)
            .field("bearer_token", &"<redacted>")
            .field("rpc_deadline", &self.rpc_deadline)
            .field("transport_window_capacity", &self.transport_window_capacity)
            .field("shutdown_deadline", &self.shutdown_deadline)
            .finish()
    }
}

impl RuntimeHostConfig {
    pub async fn load(
        args: RuntimeHostArgs,
        current_directory: &Path,
    ) -> Result<Self, RuntimeHostConfigError> {
        if args.resource_uid.is_empty()
            || args.resource_uid.chars().any(char::is_control)
            || args.resource_uid.trim() != args.resource_uid
            || args.replica_id <= 0
            || !valid_identity_component(&args.pod_uid)
            || !valid_identity_component(&args.pvc_uid)
            || args.control_address.port() == 0
            || args.replication_address.port() == 0
            || args.transport_window_capacity == 0
            || args.transport_window_capacity > MAX_TRANSPORT_WINDOW_CAPACITY
        {
            return Err(RuntimeHostConfigError::InvalidIdentity);
        }
        validate_http_endpoint(&args.control_endpoint)
            .map_err(|_| RuntimeHostConfigError::InvalidEndpoint)?;
        validate_http_endpoint(&args.replication_endpoint)
            .map_err(|_| RuntimeHostConfigError::InvalidEndpoint)?;
        let rpc_deadline = duration(args.rpc_deadline_ms)?;
        let shutdown_deadline = duration(args.shutdown_deadline_ms)?;
        let data_root = absolute_root(current_directory, args.data_root)?;
        let application_root = absolute_root(
            current_directory,
            args.application_root
                .unwrap_or_else(|| data_root.join("application")),
        )?;

        for path in [
            &args.observer_config,
            &args.topology_config,
            &args.bearer_token_file,
        ] {
            if !path.is_absolute() {
                return Err(RuntimeHostConfigError::InvalidPath);
            }
        }

        let observer_bytes =
            read_regular_bounded(&args.observer_config, MAX_TOPOLOGY_BYTES, false)?;
        let observer = ObserverConfig::from_json(&observer_bytes)
            .map_err(|_| RuntimeHostConfigError::Observer)?;
        let topology_bytes =
            read_regular_bounded(&args.topology_config, MAX_TOPOLOGY_BYTES, false)?;
        let topology = SqlServerTopologyExpectation::from_json(
            &topology_bytes,
            ReplicaId::new(args.replica_id),
            observer.target().replica.logical_id(),
        )
        .map_err(|_| RuntimeHostConfigError::Topology)?;
        if observer.target().expected_server_name != *topology.local_member().server_name() {
            return Err(RuntimeHostConfigError::Topology);
        }

        let bearer_token = Zeroizing::new(read_secret(&args.bearer_token_file, MAX_SECRET_BYTES)?);
        let resolver = match (args.namespace, args.peer_routes) {
            (Some(namespace), None) if valid_namespace(&namespace) => {
                ResolverConfig::KubernetesDns { namespace }
            }
            (None, Some(path)) if path.is_absolute() => {
                let routes = PeerRoutes::read(&path)
                    .await
                    .map_err(RuntimeHostConfigError::PeerRoutes)?;
                if routes.len() != topology.members().len() - 1
                    || routes.values().any(|route| {
                        route.identity().replica_id == ReplicaId::new(args.replica_id)
                            || topology.member(route.identity().replica_id).is_none()
                    })
                    || topology.members().iter().any(|member| {
                        member.replica_id() != ReplicaId::new(args.replica_id)
                            && !routes
                                .values()
                                .any(|route| route.identity().replica_id == member.replica_id())
                    })
                {
                    return Err(RuntimeHostConfigError::InvalidResolver);
                }
                ResolverConfig::PeerRoutes(routes)
            }
            _ => return Err(RuntimeHostConfigError::InvalidResolver),
        };

        Ok(Self {
            resource_uid: ResourceUid::new(args.resource_uid),
            replica_id: ReplicaId::new(args.replica_id),
            pod_uid: PodUid::new(args.pod_uid),
            pvc_uid: PvcUid::new(args.pvc_uid),
            data_root,
            application_root,
            control_address: args.control_address,
            replication_address: args.replication_address,
            control_endpoint: args.control_endpoint,
            replication_endpoint: args.replication_endpoint,
            resolver,
            observer,
            topology,
            bearer_token,
            rpc_deadline,
            transport_window_capacity: args.transport_window_capacity,
            shutdown_deadline,
        })
    }

    pub fn resource_uid(&self) -> &ResourceUid {
        &self.resource_uid
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    pub fn pod_uid(&self) -> &PodUid {
        &self.pod_uid
    }

    pub fn pvc_uid(&self) -> &PvcUid {
        &self.pvc_uid
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    pub fn application_root(&self) -> &Path {
        &self.application_root
    }

    pub fn control_address(&self) -> SocketAddr {
        self.control_address
    }

    pub fn replication_address(&self) -> SocketAddr {
        self.replication_address
    }

    pub fn control_endpoint(&self) -> &str {
        &self.control_endpoint
    }

    pub fn replication_endpoint(&self) -> &str {
        &self.replication_endpoint
    }

    pub fn resolver(&self) -> &ResolverConfig {
        &self.resolver
    }

    pub fn observer(&self) -> &ObserverConfig {
        &self.observer
    }

    pub fn topology(&self) -> &SqlServerTopologyExpectation {
        &self.topology
    }

    pub fn bearer_token(&self) -> &str {
        &self.bearer_token
    }

    pub fn rpc_deadline(&self) -> Duration {
        self.rpc_deadline
    }

    pub fn transport_window_capacity(&self) -> usize {
        self.transport_window_capacity
    }

    pub fn shutdown_deadline(&self) -> Duration {
        self.shutdown_deadline
    }

    pub fn replica_process_config(&self) -> ReplicaProcessConfig {
        ReplicaProcessConfig {
            resource_uid: self.resource_uid.clone(),
            replica_id: self.replica_id,
            pod_uid: self.pod_uid.clone(),
            pvc_uid: self.pvc_uid.clone(),
            data_root: self.data_root.clone(),
            control_address: self.control_address,
            replication_address: self.replication_address,
            bearer_token: self.bearer_token.to_string(),
            rpc_deadline: self.rpc_deadline,
            transport_window_capacity: self.transport_window_capacity,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeHostConfigError {
    InvalidIdentity,
    InvalidEndpoint,
    InvalidPath,
    InvalidResolver,
    InvalidDuration,
    Secret,
    Observer,
    Topology,
    PeerRoutes(PeerRoutesError),
    Io,
}

impl fmt::Display for RuntimeHostConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidIdentity => "runtime identity configuration is invalid",
            Self::InvalidEndpoint => "runtime endpoint configuration is invalid",
            Self::InvalidPath => "runtime file or root path is invalid",
            Self::InvalidResolver => "runtime endpoint resolver configuration is invalid",
            Self::InvalidDuration => "runtime deadline is invalid",
            Self::Secret => "runtime bearer token is invalid",
            Self::Observer => "runtime observer configuration is invalid",
            Self::Topology => "runtime SQL topology configuration is invalid",
            Self::PeerRoutes(_) => "runtime peer routes are invalid",
            Self::Io => "runtime configuration file is unavailable",
        })
    }
}

impl std::error::Error for RuntimeHostConfigError {}

fn duration(millis: u64) -> Result<Duration, RuntimeHostConfigError> {
    if millis == 0 || millis > MAX_DURATION_MILLIS {
        Err(RuntimeHostConfigError::InvalidDuration)
    } else {
        Ok(Duration::from_millis(millis))
    }
}

fn absolute_root(
    current_directory: &Path,
    path: PathBuf,
) -> Result<PathBuf, RuntimeHostConfigError> {
    let path = if path.is_absolute() {
        path
    } else {
        current_directory.join(path)
    };
    if !path.is_absolute()
        || path
            .components()
            .any(|component| component.as_os_str() == "..")
    {
        return Err(RuntimeHostConfigError::InvalidPath);
    }
    Ok(path)
}

fn valid_identity_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && value
            .bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn read_regular_bounded(
    path: &Path,
    max_bytes: u64,
    private: bool,
) -> Result<Vec<u8>, RuntimeHostConfigError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            if error.raw_os_error() == Some(libc::ELOOP) {
                RuntimeHostConfigError::InvalidPath
            } else {
                RuntimeHostConfigError::Io
            }
        })?;
    let metadata = file.metadata().map_err(|_| RuntimeHostConfigError::Io)?;
    if !metadata.is_file()
        || metadata.len() > max_bytes
        || (private
            && (metadata.uid() != unsafe { libc::geteuid() }
                || metadata.permissions().mode() & 0o077 != 0))
    {
        return Err(RuntimeHostConfigError::InvalidPath);
    }
    let mut bytes = Vec::new();
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| RuntimeHostConfigError::Io)?;
    if bytes.len() as u64 > max_bytes {
        return Err(RuntimeHostConfigError::InvalidPath);
    }
    Ok(bytes)
}

fn read_secret(path: &Path, max_bytes: u64) -> Result<String, RuntimeHostConfigError> {
    let bytes =
        read_regular_bounded(path, max_bytes, true).map_err(|_| RuntimeHostConfigError::Secret)?;
    if bytes.is_empty() {
        return Err(RuntimeHostConfigError::Secret);
    }
    let token = String::from_utf8(bytes).map_err(|_| RuntimeHostConfigError::Secret)?;
    if token.is_empty()
        || token.trim() != token
        || token.chars().any(char::is_control)
        || token.contains('\0')
    {
        return Err(RuntimeHostConfigError::Secret);
    }
    Ok(token)
}
