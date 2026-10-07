use std::error::Error;
use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::cleanup::{CleanupClock, OperationBudget, SystemCleanupClock};
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
    UnsupportedCgroupV1,
    ImageUnavailable,
    ImageMismatch,
    InsufficientMemory { available: u64, required: u64 },
    InsufficientCpus { available: u32, required: u32 },
    InsufficientFixtureSpace { available: u64, required: u64 },
    InsufficientDockerSpace { available: u64, required: u64 },
    AclTools,
    AclFilesystem,
    Privilege,
    Deadline,
    Unverifiable,
}

impl fmt::Display for PreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledgement => {
                formatter.write_str("SQL Server EULA acknowledgement failed revalidation")
            }
            Self::UnsupportedHost => {
                formatter.write_str("SQL Server fixture requires local Linux x86-64")
            }
            Self::DockerUnavailable => formatter.write_str("Docker is unavailable"),
            Self::DockerNotLocal => formatter.write_str("Docker engine must be local"),
            Self::DockerCapabilities => {
                formatter.write_str("Docker cannot enforce exact memory, no-swap, and CPU limits")
            }
            Self::UnsupportedCgroupV1 => {
                formatter.write_str("cgroup v1 is unsupported for resource verification")
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
            Self::Privilege => formatter.write_str(
                "noninteractive sudo access for the SQL UID probe and ACL recovery is required",
            ),
            Self::Deadline => formatter.write_str("complete-run deadline expired during preflight"),
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
    run_preflight_with_deadline(
        config,
        host,
        acl,
        docker,
        Instant::now() + config.deadlines().complete_run,
    )
}

pub fn run_preflight_with_deadline(
    config: &FixtureConfig,
    host: &impl HostProbe,
    acl: &impl AclProbe,
    docker: &impl DockerApi,
    deadline: Instant,
) -> Result<PreflightReport, PreflightError> {
    let limit = |local: Duration| {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(PreflightError::Deadline)?;
        if remaining.is_zero() {
            Err(PreflightError::Deadline)
        } else {
            Ok(local.min(remaining))
        }
    };
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
        .capabilities(limit(config.deadlines().docker_command)?)
        .map_err(|_| PreflightError::DockerUnavailable)?;
    validate_docker_capabilities(capabilities)?;

    let host_uid = host.host_uid()?;
    acl.verify(
        config.root(),
        host_uid,
        SQL_SERVER_UID,
        limit(config.deadlines().tls_helper)?,
    )?;

    let docker_root = docker
        .docker_root(limit(config.deadlines().docker_command)?)
        .map_err(|_| PreflightError::DockerUnavailable)?;
    let cached = docker
        .inspect_image(config.image(), limit(config.deadlines().docker_command)?)
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
            .pull_image(config.image(), limit(config.deadlines().image_pull)?)
            .map_err(|_| PreflightError::ImageUnavailable)?;
        docker
            .inspect_image(config.image(), limit(config.deadlines().docker_command)?)
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
        let cgroup = cgroup_v2_path()?;
        cgroup_v2_available_memory(Path::new("/sys/fs/cgroup"), &cgroup, host_available)
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
        let cgroup = cgroup_v2_path()?;
        let cpuset = cgroup_v2_effective_cpuset(Path::new("/sys/fs/cgroup"), &cgroup)?;
        let quota = cgroup_v2_effective_cpu_quota(Path::new("/sys/fs/cgroup"), &cgroup)?;
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

    pub fn runner(&self) -> &R {
        &self.runner
    }

    pub fn verify_with_clock(
        &self,
        filesystem_path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
        clock: &impl CleanupClock,
    ) -> Result<(), PreflightError>
    where
        R: ProcessRunner,
    {
        let budget = OperationBudget::new(clock, timeout);
        let limit = || budget.limit(timeout).ok_or(PreflightError::Deadline);
        for tool in ["setfacl", "getfacl"] {
            self.runner
                .run(&CommandSpec::new(tool, "verify ACL helper", limit()?).arg("--version"))
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
                    &CommandSpec::new("setfacl", "configure ACL probe", limit()?)
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
                    &CommandSpec::new("getfacl", "inspect ACL probe", limit()?)
                        .args(["--absolute-names", "--numeric"])
                        .arg(&nested),
                )
                .map_err(|_| PreflightError::AclFilesystem)?;
            if !acl_output_has_users(&output.stdout, host_uid, sql_uid) {
                return Err(PreflightError::AclFilesystem);
            }
            const UID_PROBE: &str = r#"
import os
import sys
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
os.setgroups([])
os.setgid(0)
os.setuid(10001)
os.mkdir("uid-probe", dir_fd=fd)
os.close(fd)
"#;
            self.runner
                .run(
                    &CommandSpec::new("sudo", "verify SQL UID privilege", limit()?)
                        .args(["-n", "python3", "-c", UID_PROBE])
                        .arg(&probe),
                )
                .map_err(|_| PreflightError::Privilege)?;
            self.runner
                .run(
                    &CommandSpec::new("sudo", "verify cleanup ACL privilege", limit()?)
                        .args([
                            "-n",
                            "setfacl",
                            "--recursive",
                            "--physical",
                            "--modify",
                            &format!("u:{host_uid}:rwx,m:rwx"),
                        ])
                        .arg(&probe),
                )
                .map_err(|_| PreflightError::Privilege)?;
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

impl<R: ProcessRunner> AclProbe for CommandAclProbe<R> {
    fn verify(
        &self,
        filesystem_path: &Path,
        host_uid: u32,
        sql_uid: u32,
        timeout: Duration,
    ) -> Result<(), PreflightError> {
        self.verify_with_clock(
            filesystem_path,
            host_uid,
            sql_uid,
            timeout,
            &SystemCleanupClock::default(),
        )
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

fn cgroup_v2_path() -> Result<PathBuf, PreflightError> {
    let cgroup =
        fs::read_to_string("/proc/self/cgroup").map_err(|_| PreflightError::Unverifiable)?;
    cgroup_v2_path_from(&cgroup, Path::new("/sys/fs/cgroup"))
}

pub fn cgroup_v2_path_from(cgroup: &str, mount: &Path) -> Result<PathBuf, PreflightError> {
    let mut relative = None;
    let mut legacy = false;
    for line in cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next().ok_or(PreflightError::Unverifiable)?;
        let controllers = fields.next().ok_or(PreflightError::Unverifiable)?;
        let path = fields.next().ok_or(PreflightError::Unverifiable)?;
        if hierarchy.is_empty() || path.is_empty() || !path.starts_with('/') {
            return Err(PreflightError::Unverifiable);
        }
        if hierarchy == "0" {
            if !controllers.is_empty() || relative.replace(path).is_some() {
                return Err(PreflightError::Unverifiable);
            }
        } else {
            if controllers.is_empty() {
                return Err(PreflightError::Unverifiable);
            }
            legacy = true;
        }
    }
    let unified_mount = mount.join("cgroup.controllers").exists();
    if legacy {
        return Err(PreflightError::UnsupportedCgroupV1);
    }
    if let Some(relative) = relative {
        if !unified_mount {
            return Err(PreflightError::Unverifiable);
        }
        return Ok(mount.join(relative.trim_start_matches('/')));
    }
    if unified_mount {
        return Err(PreflightError::Unverifiable);
    }
    Err(PreflightError::Unverifiable)
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
        let maximum_path = path.join("memory.max");
        let current_path = path.join("memory.current");
        let maximum = if path == mount {
            match read_optional_cgroup_limit(&maximum_path)? {
                Some(maximum) => maximum,
                None => {
                    if current_path
                        .try_exists()
                        .map_err(|_| PreflightError::Unverifiable)?
                    {
                        return Err(PreflightError::Unverifiable);
                    }
                    continue;
                }
            }
        } else {
            read_cgroup_limit(&maximum_path)?
        };
        let current = read_required_u64(&current_path)?;
        if let Some(maximum) = maximum {
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
        let cpu_max = path.join("cpu.max");
        let value = match fs::read_to_string(&cpu_max) {
            Ok(value) => value,
            Err(error) if path == mount && error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }

            Err(_) => return Err(PreflightError::Unverifiable),
        };
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

pub fn cgroup_v2_effective_cpuset(
    mount: &Path,
    current: &Path,
) -> Result<Option<u32>, PreflightError> {
    for path in cgroup_v2_hierarchy(mount, current)? {
        match fs::read_to_string(path.join("cpuset.cpus.effective")) {
            Ok(value) if !value.trim().is_empty() => return parse_cpu_list(value.trim()).map(Some),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(PreflightError::Unverifiable),
        }
    }
    Ok(None)
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
    parse_cgroup_limit(&value)
}

fn read_optional_cgroup_limit(path: &Path) -> Result<Option<Option<u64>>, PreflightError> {
    match fs::read_to_string(path) {
        Ok(value) => parse_cgroup_limit(&value).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(PreflightError::Unverifiable),
    }
}

fn parse_cgroup_limit(value: &str) -> Result<Option<u64>, PreflightError> {
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
