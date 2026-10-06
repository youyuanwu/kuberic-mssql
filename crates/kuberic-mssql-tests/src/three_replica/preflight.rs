use std::error::Error;
use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::config::FixtureConfig;
use super::docker::{DockerApi, DockerCapabilities, ImageInspection, SQL_SERVER_UID};
use super::process::{CommandSpec, ProcessRunner};

static ACL_PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPlatform {
    pub operating_system: String,
    pub architecture: String,
}

impl HostPlatform {
    fn supported(&self) -> bool {
        self.operating_system == "linux" && matches!(self.architecture.as_str(), "x86_64" | "amd64")
    }
}

pub trait HostProbe {
    fn platform(&self) -> Result<HostPlatform, PreflightError>;
    fn available_memory_bytes(&self) -> Result<u64, PreflightError>;
    fn effective_cpus(&self) -> Result<u32, PreflightError>;
    fn available_space_bytes(&self, path: &Path) -> Result<u64, PreflightError>;
    fn host_uid(&self) -> Result<u32, PreflightError>;
}

pub trait AclProbe {
    fn verify(
        &self,
        filesystem_path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
    ) -> Result<(), PreflightError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightError {
    Acknowledgement,
    UnsupportedHost,
    DockerUnavailable,
    DockerNotLocal,
    DockerCapabilities,
    ImageUnavailable,
    ImageMismatch,
    InsufficientMemory { available: u64, required: u64 },
    InsufficientCpus { available: u32, required: u32 },
    InsufficientFixtureSpace { available: u64, required: u64 },
    InsufficientDockerSpace { available: u64, required: u64 },
    AclTools,
    AclFilesystem,
    Unverifiable,
}

impl fmt::Display for PreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledgement => {
                formatter.write_str("SQL Server EULA acknowledgement failed revalidation")
            }
            Self::UnsupportedHost => {
                formatter.write_str("three-replica fixture requires local Linux x86-64")
            }
            Self::DockerUnavailable => formatter.write_str("Docker is unavailable"),
            Self::DockerNotLocal => formatter.write_str("Docker engine must be local"),
            Self::DockerCapabilities => {
                formatter.write_str("Docker cannot enforce exact memory, no-swap, and CPU limits")
            }
            Self::ImageUnavailable => formatter.write_str("pinned SQL Server image is unavailable"),
            Self::ImageMismatch => {
                formatter.write_str("pinned SQL Server image verification failed")
            }
            Self::InsufficientMemory {
                available,
                required,
            } => write!(
                formatter,
                "insufficient effective memory: {available} bytes available, {required} required"
            ),
            Self::InsufficientCpus {
                available,
                required,
            } => write!(
                formatter,
                "insufficient effective CPUs: {available} available, {required} required"
            ),
            Self::InsufficientFixtureSpace {
                available,
                required,
            } => write!(
                formatter,
                "insufficient fixture filesystem space: {available} bytes available, {required} required"
            ),
            Self::InsufficientDockerSpace {
                available,
                required,
            } => write!(
                formatter,
                "insufficient Docker-root space: {available} bytes available, {required} required"
            ),
            Self::AclTools => formatter.write_str("setfacl and getfacl are required"),
            Self::AclFilesystem => formatter.write_str(
                "fixture filesystem does not support the required ACL inheritance and deletion",
            ),
            Self::Unverifiable => formatter.write_str("host capability could not be verified"),
        }
    }
}

impl Error for PreflightError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightReport {
    pub available_memory_bytes: u64,
    pub effective_cpus: u32,
    pub fixture_available_bytes: u64,
    pub docker_available_bytes: u64,
    pub image: ImageInspection,
    pub image_was_cached: bool,
}

pub fn run_preflight(
    config: &FixtureConfig,
    host: &impl HostProbe,
    acl: &impl AclProbe,
    docker: &impl DockerApi,
) -> Result<PreflightReport, PreflightError> {
    config
        .authorization()
        .revalidate()
        .map_err(|_| PreflightError::Acknowledgement)?;
    let platform = host.platform()?;
    if !platform.supported() {
        return Err(PreflightError::UnsupportedHost);
    }

    let policy = config.resources();
    let available_memory = host.available_memory_bytes()?;
    if available_memory < policy.minimum_available_memory_bytes {
        return Err(PreflightError::InsufficientMemory {
            available: available_memory,
            required: policy.minimum_available_memory_bytes,
        });
    }
    let cpus = host.effective_cpus()?;
    if cpus < policy.minimum_effective_cpus {
        return Err(PreflightError::InsufficientCpus {
            available: cpus,
            required: policy.minimum_effective_cpus,
        });
    }
    let initial_fixture_available = host.available_space_bytes(config.root())?;
    if initial_fixture_available < policy.minimum_fixture_bytes {
        return Err(PreflightError::InsufficientFixtureSpace {
            available: initial_fixture_available,
            required: policy.minimum_fixture_bytes,
        });
    }
    let capabilities = docker
        .capabilities(config.deadlines().docker_command)
        .map_err(|_| PreflightError::DockerUnavailable)?;
    validate_docker_capabilities(capabilities)?;

    let host_uid = host.host_uid()?;
    acl.verify(
        config.root(),
        host_uid,
        SQL_SERVER_UID,
        config.deadlines().tls_helper,
    )?;

    let docker_root = docker
        .docker_root(config.deadlines().docker_command)
        .map_err(|_| PreflightError::DockerUnavailable)?;
    let cached = docker
        .inspect_image(config.image(), config.deadlines().docker_command)
        .map_err(|_| PreflightError::DockerUnavailable)?;
    let image_was_cached = cached.is_some();
    let image = if let Some(image) = cached {
        image
    } else {
        let available = host.available_space_bytes(&docker_root)?;
        if available < policy.minimum_docker_root_before_pull_bytes {
            return Err(PreflightError::InsufficientDockerSpace {
                available,
                required: policy.minimum_docker_root_before_pull_bytes,
            });
        }
        docker
            .pull_image(config.image(), config.deadlines().image_pull)
            .map_err(|_| PreflightError::ImageUnavailable)?;
        docker
            .inspect_image(config.image(), config.deadlines().docker_command)
            .map_err(|_| PreflightError::ImageUnavailable)?
            .ok_or(PreflightError::ImageUnavailable)?
    };
    image
        .verify_pinned_sql_server()
        .map_err(|_| PreflightError::ImageMismatch)?;
    let docker_available = host.available_space_bytes(&docker_root)?;
    if docker_available < policy.minimum_docker_root_after_image_bytes {
        return Err(PreflightError::InsufficientDockerSpace {
            available: docker_available,
            required: policy.minimum_docker_root_after_image_bytes,
        });
    }
    let fixture_available = host.available_space_bytes(config.root())?;
    if fixture_available < policy.minimum_fixture_bytes {
        return Err(PreflightError::InsufficientFixtureSpace {
            available: fixture_available,
            required: policy.minimum_fixture_bytes,
        });
    }
    Ok(PreflightReport {
        available_memory_bytes: available_memory,
        effective_cpus: cpus,
        fixture_available_bytes: fixture_available,
        docker_available_bytes: docker_available,
        image,
        image_was_cached,
    })
}

fn validate_docker_capabilities(capabilities: DockerCapabilities) -> Result<(), PreflightError> {
    if !capabilities.local {
        return Err(PreflightError::DockerNotLocal);
    }
    if !capabilities.linux || !capabilities.x86_64 {
        return Err(PreflightError::UnsupportedHost);
    }
    if !capabilities.memory_limit || !capabilities.swap_limit || !capabilities.cpu_quota {
        return Err(PreflightError::DockerCapabilities);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalHostProbe;

impl HostProbe for LocalHostProbe {
    fn platform(&self) -> Result<HostPlatform, PreflightError> {
        Ok(HostPlatform {
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
        })
    }

    fn available_memory_bytes(&self) -> Result<u64, PreflightError> {
        let meminfo =
            fs::read_to_string("/proc/meminfo").map_err(|_| PreflightError::Unverifiable)?;
        let host_available = meminfo
            .lines()
            .find_map(|line| {
                let value = line.strip_prefix("MemAvailable:")?;
                value
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .and_then(|kilobytes| kilobytes.checked_mul(1024))
            })
            .ok_or(PreflightError::Unverifiable)?;
        if let Some(cgroup) = cgroup_v2_path()? {
            return cgroup_v2_available_memory(
                Path::new("/sys/fs/cgroup"),
                &cgroup,
                host_available,
            );
        }
        let memory = cgroup_v1_path("memory");
        let (limit, current) = if memory.is_some() {
            (
                read_optional_number(memory.as_deref(), "memory.limit_in_bytes"),
                read_optional_number(memory.as_deref(), "memory.usage_in_bytes"),
            )
        } else {
            (None, None)
        };
        Ok(available_memory(host_available, limit, current))
    }

    fn effective_cpus(&self) -> Result<u32, PreflightError> {
        let status =
            fs::read_to_string("/proc/self/status").map_err(|_| PreflightError::Unverifiable)?;
        let affinity = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
            .map(str::trim)
            .and_then(|value| parse_cpu_list(value).ok())
            .ok_or(PreflightError::Unverifiable)?;
        let (cpuset, quota) = if let Some(cgroup) = cgroup_v2_path()? {
            let cpuset = fs::read_to_string(cgroup.join("cpuset.cpus.effective"))
                .map_err(|_| PreflightError::Unverifiable)
                .and_then(|value| parse_cpu_list(value.trim()))?;
            (
                Some(cpuset),
                cgroup_v2_effective_cpu_quota(Path::new("/sys/fs/cgroup"), &cgroup)?,
            )
        } else {
            let cpuset = cgroup_v1_path("cpuset")
                .and_then(|path| fs::read_to_string(path.join("cpuset.cpus")).ok())
                .and_then(|value| parse_cpu_list(value.trim()).ok());
            let cpu = cgroup_v1_path("cpu");
            let quota = read_signed_number(cpu.as_deref(), "cpu.cfs_quota_us")
                .filter(|quota| *quota >= 0)
                .and_then(|quota| {
                    read_optional_number(cpu.as_deref(), "cpu.cfs_period_us")
                        .map(|period| (quota as u64, period))
                });
            (cpuset, quota)
        };
        Ok(effective_cpu_count(affinity, cpuset, quota))
    }

    fn available_space_bytes(&self, path: &Path) -> Result<u64, PreflightError> {
        let existing = existing_ancestor(path)?;
        let stats = rustix::fs::statvfs(existing).map_err(|_| PreflightError::Unverifiable)?;
        stats
            .f_bavail
            .checked_mul(stats.f_frsize)
            .ok_or(PreflightError::Unverifiable)
    }

    fn host_uid(&self) -> Result<u32, PreflightError> {
        Ok(unsafe { libc::geteuid() })
    }
}

pub struct CommandAclProbe<R> {
    runner: R,
}

impl<R> CommandAclProbe<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }
}

impl<R: ProcessRunner> AclProbe for CommandAclProbe<R> {
    fn verify(
        &self,
        filesystem_path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
    ) -> Result<(), PreflightError> {
        for tool in ["setfacl", "getfacl"] {
            self.runner
                .run(&CommandSpec::new(tool, "verify ACL helper", timeout).arg("--version"))
                .map_err(|_| PreflightError::AclTools)?;
        }
        let parent = existing_ancestor(filesystem_path)?;
        let probe = parent.join(format!(
            ".kuberic-mssql-acl-probe-{}-{}",
            std::process::id(),
            ACL_PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&probe).map_err(|_| PreflightError::AclFilesystem)?;
        fs::set_permissions(&probe, fs::Permissions::from_mode(0o700))
            .map_err(|_| PreflightError::AclFilesystem)?;
        let acl = format!(
            "u:{host_uid}:rwx,u:{sql_uid}:rwx,d:u:{host_uid}:rwx,d:u:{sql_uid}:rwx,m:rwx,d:m:rwx"
        );
        let result = (|| {
            self.runner
                .run(
                    &CommandSpec::new("setfacl", "configure ACL probe", timeout)
                        .args(["-m", &acl])
                        .arg(&probe),
                )
                .map_err(|_| PreflightError::AclFilesystem)?;
            let nested = probe.join("nested");
            fs::create_dir(&nested).map_err(|_| PreflightError::AclFilesystem)?;
            fs::write(nested.join("probe"), b"acl").map_err(|_| PreflightError::AclFilesystem)?;
            let output = self
                .runner
                .run(
                    &CommandSpec::new("getfacl", "inspect ACL probe", timeout)
                        .args(["--absolute-names", "--numeric"])
                        .arg(&nested),
                )
                .map_err(|_| PreflightError::AclFilesystem)?;
            if !acl_output_has_users(&output.stdout, host_uid, sql_uid) {
                return Err(PreflightError::AclFilesystem);
            }
            fs::remove_dir_all(&probe).map_err(|_| PreflightError::AclFilesystem)?;
            if probe.exists() {
                return Err(PreflightError::AclFilesystem);
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&probe);
        }
        result
    }
}

pub fn available_memory(
    host_available: u64,
    cgroup_limit: Option<u64>,
    cgroup_current: Option<u64>,
) -> u64 {
    match (cgroup_limit, cgroup_current) {
        (Some(limit), Some(current)) => host_available.min(limit.saturating_sub(current)),
        _ => host_available,
    }
}

pub fn effective_cpu_count(
    affinity_count: u32,
    cpuset_count: Option<u32>,
    quota: Option<(u64, u64)>,
) -> u32 {
    let cpuset = cpuset_count.unwrap_or(affinity_count);
    let quota_count = quota
        .and_then(|(quota, period)| {
            quota
                .checked_div(period)
                .map(|count| count.min(u64::from(u32::MAX)) as u32)
        })
        .unwrap_or(affinity_count);
    affinity_count.min(cpuset).min(quota_count)
}

pub fn parse_cpu_list(value: &str) -> Result<u32, PreflightError> {
    let mut total = 0_u32;
    for item in value.split(',').filter(|item| !item.is_empty()) {
        let count = if let Some((start, end)) = item.split_once('-') {
            let start = start
                .parse::<u32>()
                .map_err(|_| PreflightError::Unverifiable)?;
            let end = end
                .parse::<u32>()
                .map_err(|_| PreflightError::Unverifiable)?;
            end.checked_sub(start)
                .and_then(|difference| difference.checked_add(1))
                .ok_or(PreflightError::Unverifiable)?
        } else {
            item.parse::<u32>()
                .map_err(|_| PreflightError::Unverifiable)?;
            1
        };
        total = total
            .checked_add(count)
            .ok_or(PreflightError::Unverifiable)?;
    }
    if total == 0 {
        return Err(PreflightError::Unverifiable);
    }
    Ok(total)
}

fn existing_ancestor(path: &Path) -> Result<PathBuf, PreflightError> {
    let mut current = path;
    loop {
        if current.exists() {
            return current
                .canonicalize()
                .map_err(|_| PreflightError::Unverifiable);
        }
        current = current.parent().ok_or(PreflightError::Unverifiable)?;
    }
}

fn cgroup_v2_path() -> Result<Option<PathBuf>, PreflightError> {
    let cgroup =
        fs::read_to_string("/proc/self/cgroup").map_err(|_| PreflightError::Unverifiable)?;
    let mut relative = None;
    for line in cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next().ok_or(PreflightError::Unverifiable)?;
        let controllers = fields.next().ok_or(PreflightError::Unverifiable)?;
        let path = fields.next().ok_or(PreflightError::Unverifiable)?;
        if hierarchy == "0"
            && (!controllers.is_empty() || path.is_empty() || relative.replace(path).is_some())
        {
            return Err(PreflightError::Unverifiable);
        }
    }
    if Path::new("/sys/fs/cgroup/cgroup.controllers").exists() && relative.is_none() {
        return Err(PreflightError::Unverifiable);
    }
    Ok(relative.map(|relative| Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'))))
}

fn cgroup_v1_path(controller: &str) -> Option<PathBuf> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cgroup.lines().find_map(|line| {
        let (_, rest) = line.split_once(':')?;
        let (controllers, path) = rest.split_once(':')?;
        controllers
            .split(',')
            .any(|candidate| candidate == controller)
            .then_some(path)
    })?;
    Some(
        Path::new("/sys/fs/cgroup")
            .join(controller)
            .join(relative.trim_start_matches('/')),
    )
}

fn read_optional_number(path: Option<&Path>, file: &str) -> Option<u64> {
    fs::read_to_string(path?.join(file))
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn read_signed_number(path: Option<&Path>, file: &str) -> Option<i64> {
    fs::read_to_string(path?.join(file))
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn acl_output_has_users(output: &str, host_uid: u32, sql_uid: u32) -> bool {
    [host_uid, sql_uid].into_iter().all(|uid| {
        output.lines().any(|line| line == format!("user:{uid}:rwx"))
            && output
                .lines()
                .any(|line| line == format!("default:user:{uid}:rwx"))
    }) && output.lines().any(|line| line == "mask::rwx")
        && output.lines().any(|line| line == "default:mask::rwx")
}

pub fn cgroup_v2_available_memory(
    mount: &Path,
    current: &Path,
    host_available: u64,
) -> Result<u64, PreflightError> {
    let mut available = host_available;
    for path in cgroup_v2_hierarchy(mount, current)? {
        let maximum = read_cgroup_limit(&path.join("memory.max"))?;
        if let Some(maximum) = maximum {
            let current = read_required_u64(&path.join("memory.current"))?;
            available = available.min(maximum.saturating_sub(current));
        }
    }
    Ok(available)
}

pub fn cgroup_v2_effective_cpu_quota(
    mount: &Path,
    current: &Path,
) -> Result<Option<(u64, u64)>, PreflightError> {
    let mut effective = None;
    for path in cgroup_v2_hierarchy(mount, current)? {
        let value =
            fs::read_to_string(path.join("cpu.max")).map_err(|_| PreflightError::Unverifiable)?;
        let value = value.trim();
        let mut fields = value.split_whitespace();
        let quota = fields.next().ok_or(PreflightError::Unverifiable)?;
        let period = fields
            .next()
            .ok_or(PreflightError::Unverifiable)?
            .parse::<u64>()
            .map_err(|_| PreflightError::Unverifiable)?;
        if fields.next().is_some() || period == 0 {
            return Err(PreflightError::Unverifiable);
        }
        if quota == "max" {
            continue;
        }
        let quota = quota
            .parse::<u64>()
            .map_err(|_| PreflightError::Unverifiable)?;
        if quota == 0 {
            return Err(PreflightError::Unverifiable);
        }
        let candidate = (quota, period);
        if effective
            .map(|existing| quota_is_lower(candidate, existing))
            .unwrap_or(true)
        {
            effective = Some(candidate);
        }
    }
    Ok(effective)
}

fn cgroup_v2_hierarchy(mount: &Path, current: &Path) -> Result<Vec<PathBuf>, PreflightError> {
    if !current.starts_with(mount) {
        return Err(PreflightError::Unverifiable);
    }
    let mut paths = Vec::new();
    let mut path = current.to_path_buf();
    loop {
        paths.push(path.clone());
        if path == mount {
            break;
        }
        path = path
            .parent()
            .ok_or(PreflightError::Unverifiable)?
            .to_path_buf();
    }
    Ok(paths)
}

fn read_cgroup_limit(path: &Path) -> Result<Option<u64>, PreflightError> {
    let value = fs::read_to_string(path).map_err(|_| PreflightError::Unverifiable)?;
    let value = value.trim();
    if value == "max" {
        Ok(None)
    } else {
        value
            .parse()
            .map(Some)
            .map_err(|_| PreflightError::Unverifiable)
    }
}

fn read_required_u64(path: &Path) -> Result<u64, PreflightError> {
    fs::read_to_string(path)
        .map_err(|_| PreflightError::Unverifiable)?
        .trim()
        .parse()
        .map_err(|_| PreflightError::Unverifiable)
}

fn quota_is_lower(left: (u64, u64), right: (u64, u64)) -> bool {
    u128::from(left.0) * u128::from(right.1) < u128::from(right.0) * u128::from(left.1)
}
