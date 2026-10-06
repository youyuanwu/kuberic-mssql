use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::config::{PINNED_SQL_SERVER_IMAGE, ResourcePolicy};
use super::process::{CommandSpec, ProcessErrorKind, ProcessRunner};

pub const SQL_SERVER_UID: u32 = 10001;
pub const CONTAINER_MEMORY_BYTES: u64 = 3 * 1024 * 1024 * 1024;
pub const CONTAINER_MEMORY_SWAP_BYTES: u64 = CONTAINER_MEMORY_BYTES;
pub const CONTAINER_NANO_CPUS: u64 = 2_000_000_000;
pub const SQL_SERVER_MEMORY_MB: u32 = 2048;

const FIXTURE_LABEL: &str = "io.kuberic.mssql.fixture";
const RUN_LABEL: &str = "io.kuberic.mssql.run";
const KIND_LABEL: &str = "io.kuberic.mssql.kind";
const MEMBER_LABEL: &str = "io.kuberic.mssql.member";
const FIXTURE_LABEL_VALUE: &str = "three-replica";
const SQL_DATA_DESTINATION: &str = "/var/opt/mssql";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedLabels {
    pub run_id: String,
    pub resource_kind: String,
    pub member_ordinal: Option<u8>,
}

impl OwnedLabels {
    pub fn network(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            resource_kind: "network".to_owned(),
            member_ordinal: None,
        }
    }

    pub fn container(run_id: impl Into<String>, member_ordinal: u8) -> Self {
        Self {
            run_id: run_id.into(),
            resource_kind: "container".to_owned(),
            member_ordinal: Some(member_ordinal),
        }
    }

    pub fn as_map(&self) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::from([
            (FIXTURE_LABEL.to_owned(), FIXTURE_LABEL_VALUE.to_owned()),
            (RUN_LABEL.to_owned(), self.run_id.clone()),
            (KIND_LABEL.to_owned(), self.resource_kind.clone()),
        ]);
        if let Some(member) = self.member_ordinal {
            labels.insert(MEMBER_LABEL.to_owned(), member.to_string());
        }
        labels
    }

    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        let expected = self.as_map();
        expected
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value))
            && labels
                .keys()
                .filter(|key| {
                    matches!(
                        key.as_str(),
                        FIXTURE_LABEL | RUN_LABEL | KIND_LABEL | MEMBER_LABEL
                    )
                })
                .count()
                == expected.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerCapabilities {
    pub local: bool,
    pub linux: bool,
    pub x86_64: bool,
    pub memory_limit: bool,
    pub swap_limit: bool,
    pub cpu_quota: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageInspection {
    pub id: String,
    pub operating_system: String,
    pub architecture: String,
    pub repo_digests: Vec<String>,
    pub labels: BTreeMap<String, String>,
}

impl ImageInspection {
    pub fn verify_pinned_sql_server(&self) -> Result<(), DockerError> {
        if self.operating_system != "linux"
            || !matches!(self.architecture.as_str(), "amd64" | "x86_64")
        {
            return Err(DockerError::ImageMismatch);
        }
        if !self
            .repo_digests
            .iter()
            .any(|digest| digest == PINNED_SQL_SERVER_IMAGE)
        {
            return Err(DockerError::ImageMismatch);
        }
        if self.labels.get("com.microsoft.product").map(String::as_str)
            != Some("Microsoft SQL Server")
        {
            return Err(DockerError::ImageMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkInspection {
    pub id: String,
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub attached_container_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerMount {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerPort {
    pub container_port: u16,
    pub host_ip: String,
    pub host_port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerLimits {
    pub memory_bytes: u64,
    pub memory_swap_bytes: u64,
    pub nano_cpus: u64,
}

impl ContainerLimits {
    pub fn from_policy(policy: ResourcePolicy) -> Self {
        Self {
            memory_bytes: policy.container_memory_bytes,
            memory_swap_bytes: policy.container_memory_bytes,
            nano_cpus: policy.container_cpu_nanos,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInspection {
    pub id: String,
    pub name: String,
    pub image_id: String,
    pub labels: BTreeMap<String, String>,
    pub environment: Vec<String>,
    pub user: String,
    pub running: bool,
    pub restart_policy: String,
    pub mounts: Vec<ContainerMount>,
    pub ports: Vec<ContainerPort>,
    pub limits: ContainerLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkRequest {
    pub name: String,
    pub labels: OwnedLabels,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRequest {
    pub name: String,
    pub hostname: String,
    pub image: String,
    pub network_name: String,
    pub labels: OwnedLabels,
    pub environment: Vec<(String, String)>,
    pub data_directory: PathBuf,
    pub limits: ContainerLimits,
}

impl ContainerRequest {
    pub fn sql_server(
        name: impl Into<String>,
        hostname: impl Into<String>,
        network_name: impl Into<String>,
        labels: OwnedLabels,
        data_directory: impl Into<PathBuf>,
        policy: ResourcePolicy,
        acceptance_environment: [(&'static str, &'static str); 1],
    ) -> Self {
        let mut environment = acceptance_environment
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect::<Vec<_>>();
        environment.extend([
            ("MSSQL_ENABLE_HADR".to_owned(), "1".to_owned()),
            (
                "MSSQL_MEMORY_LIMIT_MB".to_owned(),
                policy.sql_server_memory_mb.to_string(),
            ),
        ]);
        Self {
            name: name.into(),
            hostname: hostname.into(),
            image: PINNED_SQL_SERVER_IMAGE.to_owned(),
            network_name: network_name.into(),
            labels,
            environment,
            data_directory: data_directory.into(),
            limits: ContainerLimits::from_policy(policy),
        }
    }

    pub fn verify_inspection(
        &self,
        inspection: &ContainerInspection,
        expected_image_id: &str,
    ) -> Result<(), DockerError> {
        if inspection.name != self.name
            || inspection.image_id != expected_image_id
            || !self.labels.matches(&inspection.labels)
            || inspection.user != "mssql"
            || inspection.restart_policy != "no"
            || inspection.limits != self.limits
        {
            return Err(DockerError::OwnershipMismatch);
        }
        let expected_mount = ContainerMount {
            source: self
                .data_directory
                .canonicalize()
                .map_err(|_| DockerError::OwnershipMismatch)?,
            destination: PathBuf::from(SQL_DATA_DESTINATION),
            read_only: false,
        };
        if inspection.mounts.as_slice() != [expected_mount] {
            return Err(DockerError::OwnershipMismatch);
        }
        for (key, value) in &self.environment {
            let expected = format!("{key}={value}");
            if inspection
                .environment
                .iter()
                .filter(|item| *item == &expected)
                .count()
                != 1
            {
                return Err(DockerError::OwnershipMismatch);
            }
            if inspection
                .environment
                .iter()
                .filter_map(|item| item.split_once('='))
                .filter(|(actual_key, _)| actual_key == key)
                .count()
                != 1
            {
                return Err(DockerError::OwnershipMismatch);
            }
        }
        if inspection.ports.len() != 1
            || inspection.ports[0].container_port != 1433
            || inspection.ports[0].host_ip != "127.0.0.1"
            || inspection.ports[0].host_port == 0
        {
            return Err(DockerError::OwnershipMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DockerError {
    Command,
    MalformedInspection,
    ImageMismatch,
    OwnershipMismatch,
    NotFound,
}

impl fmt::Display for DockerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Command => "Docker operation failed",
            Self::MalformedInspection => "Docker returned malformed inspection data",
            Self::ImageMismatch => {
                "Docker image identity does not match the pinned SQL Server image"
            }
            Self::OwnershipMismatch => "Docker resource ownership does not match",
            Self::NotFound => "Docker resource was not found",
        })
    }
}

impl Error for DockerError {}

pub trait DockerApi {
    fn capabilities(&self, timeout: Duration) -> Result<DockerCapabilities, DockerError>;
    fn docker_root(&self, timeout: Duration) -> Result<PathBuf, DockerError>;
    fn inspect_image(
        &self,
        image: &str,
        timeout: Duration,
    ) -> Result<Option<ImageInspection>, DockerError>;
    fn pull_image(&self, image: &str, timeout: Duration) -> Result<(), DockerError>;
    fn inspect_network(
        &self,
        identity: &str,
        timeout: Duration,
    ) -> Result<Option<NetworkInspection>, DockerError>;
    fn create_network(
        &self,
        request: &NetworkRequest,
        timeout: Duration,
    ) -> Result<String, DockerError>;
    fn remove_network(&self, id: &str, timeout: Duration) -> Result<(), DockerError>;
    fn inspect_container(
        &self,
        identity: &str,
        timeout: Duration,
    ) -> Result<Option<ContainerInspection>, DockerError>;
    fn create_container(
        &self,
        request: &ContainerRequest,
        timeout: Duration,
    ) -> Result<String, DockerError>;
    fn start_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError>;
    fn stop_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError>;
    fn remove_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError>;
}

#[derive(Debug)]
pub struct DockerCli<R> {
    runner: R,
    executable: PathBuf,
}

impl<R> DockerCli<R> {
    pub fn new(runner: R) -> Self {
        Self {
            runner,
            executable: PathBuf::from("docker"),
        }
    }

    fn command(&self, name: &str, timeout: Duration) -> CommandSpec {
        CommandSpec::new(&self.executable, name, timeout)
    }

    pub fn runner(&self) -> &R {
        &self.runner
    }
}

impl<R: ProcessRunner> DockerApi for DockerCli<R> {
    fn capabilities(&self, timeout: Duration) -> Result<DockerCapabilities, DockerError> {
        let endpoint = self
            .runner
            .run(&self.command("inspect Docker endpoint", timeout).args([
                "context",
                "inspect",
                "--format",
                "{{json .Endpoints.docker.Host}}",
            ]))
            .map_err(|_| DockerError::Command)?;
        let endpoint: String =
            serde_json::from_str(&endpoint.stdout).map_err(|_| DockerError::MalformedInspection)?;
        let result = self
            .runner
            .run(&self.command("inspect Docker capabilities", timeout).args([
                "info",
                "--format",
                "{{json .}}",
            ]))
            .map_err(|_| DockerError::Command)?;
        let value: Value =
            serde_json::from_str(&result.stdout).map_err(|_| DockerError::MalformedInspection)?;
        let architecture = string_at(&value, &["Architecture"])?;
        Ok(DockerCapabilities {
            local: endpoint.starts_with("unix://"),
            linux: string_at(&value, &["OSType"])? == "linux",
            x86_64: matches!(architecture.as_str(), "x86_64" | "amd64"),
            memory_limit: bool_at(&value, &["MemoryLimit"])?,
            swap_limit: bool_at(&value, &["SwapLimit"])?,
            cpu_quota: bool_at(&value, &["CpuCfsQuota"])?,
        })
    }

    fn docker_root(&self, timeout: Duration) -> Result<PathBuf, DockerError> {
        let result = self
            .runner
            .run(&self.command("inspect Docker root", timeout).args([
                "info",
                "--format",
                "{{json .DockerRootDir}}",
            ]))
            .map_err(|_| DockerError::Command)?;
        serde_json::from_str::<String>(&result.stdout)
            .map(PathBuf::from)
            .map_err(|_| DockerError::MalformedInspection)
    }

    fn inspect_image(
        &self,
        image: &str,
        timeout: Duration,
    ) -> Result<Option<ImageInspection>, DockerError> {
        let result = match self.runner.run(
            &self
                .command("inspect SQL Server image", timeout)
                .args(["image", "inspect", image]),
        ) {
            Ok(result) => result,
            Err(error)
                if error.kind() == ProcessErrorKind::Exit
                    && error
                        .diagnostic()
                        .to_ascii_lowercase()
                        .contains("no such image") =>
            {
                return Ok(None);
            }
            Err(_) => return Err(DockerError::Command),
        };
        parse_image_inspection(&result.stdout).map(Some)
    }

    fn pull_image(&self, image: &str, timeout: Duration) -> Result<(), DockerError> {
        self.runner
            .run(
                &self
                    .command("pull pinned SQL Server image", timeout)
                    .args(["image", "pull", image]),
            )
            .map(|_| ())
            .map_err(|_| DockerError::Command)
    }

    fn inspect_network(
        &self,
        identity: &str,
        timeout: Duration,
    ) -> Result<Option<NetworkInspection>, DockerError> {
        inspect_optional(
            &self.runner,
            self.command("inspect Docker network", timeout)
                .args(["network", "inspect", identity]),
            parse_network_inspection,
        )
    }

    fn create_network(
        &self,
        request: &NetworkRequest,
        timeout: Duration,
    ) -> Result<String, DockerError> {
        let mut command = self
            .command("create owned Docker network", timeout)
            .args(["network", "create", "--driver", "bridge"]);
        for (key, value) in request.labels.as_map() {
            command = command.args(["--label", &format!("{key}={value}")]);
        }
        self.runner
            .run(&command.arg(&request.name))
            .map(|result| result.stdout.trim().to_owned())
            .map_err(|_| DockerError::Command)
    }

    fn remove_network(&self, id: &str, timeout: Duration) -> Result<(), DockerError> {
        run_empty(
            &self.runner,
            self.command("remove owned Docker network", timeout)
                .args(["network", "rm", id]),
        )
    }

    fn inspect_container(
        &self,
        identity: &str,
        timeout: Duration,
    ) -> Result<Option<ContainerInspection>, DockerError> {
        inspect_optional(
            &self.runner,
            self.command("inspect Docker container", timeout).args([
                "container",
                "inspect",
                identity,
            ]),
            parse_container_inspection,
        )
    }

    fn create_container(
        &self,
        request: &ContainerRequest,
        timeout: Duration,
    ) -> Result<String, DockerError> {
        let canonical_data = request
            .data_directory
            .canonicalize()
            .map_err(|_| DockerError::OwnershipMismatch)?;
        let mut command = self
            .command("create owned SQL Server container", timeout)
            .args([
                "container",
                "create",
                "--name",
                &request.name,
                "--hostname",
                &request.hostname,
                "--network",
                &request.network_name,
                "--user",
                "mssql",
                "--restart",
                "no",
                "--memory",
                &request.limits.memory_bytes.to_string(),
                "--memory-swap",
                &request.limits.memory_swap_bytes.to_string(),
                "--cpus",
                "2",
                "--publish",
                "127.0.0.1::1433",
                "--mount",
                &format!(
                    "type=bind,src={},dst={SQL_DATA_DESTINATION}",
                    canonical_data.display()
                ),
            ]);
        for (key, value) in request.labels.as_map() {
            command = command.args(["--label", &format!("{key}={value}")]);
        }
        for (key, value) in &request.environment {
            command = command.args(["--env", &format!("{key}={value}")]);
        }
        self.runner
            .run(&command.arg(&request.image))
            .map(|result| result.stdout.trim().to_owned())
            .map_err(|_| DockerError::Command)
    }

    fn start_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError> {
        run_empty(
            &self.runner,
            self.command("start owned SQL Server container", timeout)
                .args(["container", "start", id]),
        )
    }

    fn stop_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError> {
        run_empty(
            &self.runner,
            self.command("stop owned SQL Server container", timeout)
                .args(["container", "stop", "--time", "30", id]),
        )
    }

    fn remove_container(&self, id: &str, timeout: Duration) -> Result<(), DockerError> {
        run_empty(
            &self.runner,
            self.command("remove owned SQL Server container", timeout)
                .args(["container", "rm", id]),
        )
    }
}

fn run_empty(runner: &impl ProcessRunner, command: CommandSpec) -> Result<(), DockerError> {
    runner
        .run(&command)
        .map(|_| ())
        .map_err(|_| DockerError::Command)
}

fn inspect_optional<T>(
    runner: &impl ProcessRunner,
    command: CommandSpec,
    parse: impl FnOnce(&str) -> Result<T, DockerError>,
) -> Result<Option<T>, DockerError> {
    match runner.run(&command) {
        Ok(result) => parse(&result.stdout).map(Some),
        Err(error)
            if error.kind() == ProcessErrorKind::Exit
                && error.diagnostic().to_ascii_lowercase().contains("no such") =>
        {
            Ok(None)
        }
        Err(_) => Err(DockerError::Command),
    }
}

fn parse_image_inspection(text: &str) -> Result<ImageInspection, DockerError> {
    let value = first_inspection(text)?;
    Ok(ImageInspection {
        id: string_at(&value, &["Id"])?,
        operating_system: string_at(&value, &["Os"])?,
        architecture: string_at(&value, &["Architecture"])?,
        repo_digests: strings_at(&value, &["RepoDigests"])?,
        labels: map_at(&value, &["Config", "Labels"])?,
    })
}

fn parse_network_inspection(text: &str) -> Result<NetworkInspection, DockerError> {
    let value = first_inspection(text)?;
    let attached_container_ids = object_at(&value, &["Containers"])?
        .keys()
        .cloned()
        .collect();
    Ok(NetworkInspection {
        id: string_at(&value, &["Id"])?,
        name: string_at(&value, &["Name"])?,
        labels: map_at(&value, &["Labels"])?,
        attached_container_ids,
    })
}

fn parse_container_inspection(text: &str) -> Result<ContainerInspection, DockerError> {
    let value = first_inspection(text)?;
    let name = string_at(&value, &["Name"])?
        .trim_start_matches('/')
        .to_owned();
    let mounts = array_at(&value, &["Mounts"])?
        .iter()
        .map(|mount| {
            Ok(ContainerMount {
                source: PathBuf::from(string_at(mount, &["Source"])?),
                destination: PathBuf::from(string_at(mount, &["Destination"])?),
                read_only: !bool_at(mount, &["RW"])?,
            })
        })
        .collect::<Result<Vec<_>, DockerError>>()?;
    let mut ports = Vec::new();
    for (container_port, bindings) in object_at(&value, &["NetworkSettings", "Ports"])? {
        let container_port = container_port
            .split('/')
            .next()
            .ok_or(DockerError::MalformedInspection)?
            .parse()
            .map_err(|_| DockerError::MalformedInspection)?;
        let Some(bindings) = bindings.as_array() else {
            continue;
        };
        for binding in bindings {
            ports.push(ContainerPort {
                container_port,
                host_ip: string_at(binding, &["HostIp"])?,
                host_port: string_at(binding, &["HostPort"])?
                    .parse()
                    .map_err(|_| DockerError::MalformedInspection)?,
            });
        }
    }
    Ok(ContainerInspection {
        id: string_at(&value, &["Id"])?,
        name,
        image_id: string_at(&value, &["Image"])?,
        labels: map_at(&value, &["Config", "Labels"])?,
        environment: strings_at(&value, &["Config", "Env"])?,
        user: string_at(&value, &["Config", "User"])?,
        running: bool_at(&value, &["State", "Running"])?,
        restart_policy: string_at(&value, &["HostConfig", "RestartPolicy", "Name"])?,
        mounts,
        ports,
        limits: ContainerLimits {
            memory_bytes: u64_at(&value, &["HostConfig", "Memory"])?,
            memory_swap_bytes: u64_at(&value, &["HostConfig", "MemorySwap"])?,
            nano_cpus: u64_at(&value, &["HostConfig", "NanoCpus"])?,
        },
    })
}

fn first_inspection(text: &str) -> Result<Value, DockerError> {
    let value: Value = serde_json::from_str(text).map_err(|_| DockerError::MalformedInspection)?;
    value
        .as_array()
        .and_then(|items| items.first().cloned())
        .ok_or(DockerError::MalformedInspection)
}

fn at<'a>(value: &'a Value, path: &[&str]) -> Result<&'a Value, DockerError> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .ok_or(DockerError::MalformedInspection)
}

fn string_at(value: &Value, path: &[&str]) -> Result<String, DockerError> {
    at(value, path)?
        .as_str()
        .map(str::to_owned)
        .ok_or(DockerError::MalformedInspection)
}

fn bool_at(value: &Value, path: &[&str]) -> Result<bool, DockerError> {
    at(value, path)?
        .as_bool()
        .ok_or(DockerError::MalformedInspection)
}

fn u64_at(value: &Value, path: &[&str]) -> Result<u64, DockerError> {
    at(value, path)?
        .as_u64()
        .ok_or(DockerError::MalformedInspection)
}

fn strings_at(value: &Value, path: &[&str]) -> Result<Vec<String>, DockerError> {
    array_at(value, path)?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or(DockerError::MalformedInspection)
        })
        .collect()
}

fn array_at<'a>(value: &'a Value, path: &[&str]) -> Result<&'a Vec<Value>, DockerError> {
    at(value, path)?
        .as_array()
        .ok_or(DockerError::MalformedInspection)
}

fn object_at<'a>(
    value: &'a Value,
    path: &[&str],
) -> Result<&'a serde_json::Map<String, Value>, DockerError> {
    at(value, path)?
        .as_object()
        .ok_or(DockerError::MalformedInspection)
}

fn map_at(value: &Value, path: &[&str]) -> Result<BTreeMap<String, String>, DockerError> {
    object_at(value, path)?
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_owned()))
                .ok_or(DockerError::MalformedInspection)
        })
        .collect()
}
