use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use kuberic_mssql_tests::three_replica::{
    ACKNOWLEDGEMENT_SCHEMA_VERSION, AclController, AclEvidence, AclProbe, BoundedProcessRunner,
    ChildDisposition, CleanupBackend, CleanupClock, CleanupCompletion, CleanupCoordinator,
    CleanupError, CommandAclController, CommandSpec, ContainerInspection, ContainerLimits,
    ContainerMount, ContainerPort, ContainerRequest, DockerApi, DockerCapabilities, DockerCli,
    DockerError, FailureCategory, FailureStage, FixtureConfig, HandledCancellationSignal,
    HostPlatform, HostProbe, ImageInspection, JournalStore, KubericMember, LockError,
    NetworkInspection, NetworkRequest, OwnedLabels, OwnershipInspector, OwnershipJournal,
    PINNED_SQL_SERVER_IMAGE, PreflightError, ProcessError, ProcessErrorKind, ProcessResult,
    ProcessRunner, ReconcileError, ResourceBinding, ResourceKind, ResourceObservation,
    ResourcePolicy, ResourceRecord, ResourceState, RunState, SQL_SERVER_UID, SanitizedFailure,
    SqlMember, TopologyRun, acquire_root_lock, available_memory, cgroup_v2_available_memory,
    cgroup_v2_effective_cpu_quota, cleanup, combine_with_cleanup, effective_cpu_count,
    inspect_member_directory, parse_acl_evidence, parse_cpu_list, prepare_member_directory,
    reconcile, run_preflight, verify_member_directory,
};

fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

fn write_acknowledgement(path: &Path) {
    fs::write(
        path,
        format!(
            "{{\"schema_version\":{ACKNOWLEDGEMENT_SCHEMA_VERSION},\
             \"sql_server_eula\":{{\"accepted\":true}}}}"
        ),
    )
    .unwrap();
}

fn fixture_config(directory: &Path) -> FixtureConfig {
    let acknowledgement = directory.join("acknowledgement.json");
    write_acknowledgement(&acknowledgement);
    FixtureConfig::new(directory.join("fixture"), acknowledgement).unwrap()
}

fn sample_run(root: &Path) -> TopologyRun {
    TopologyRun {
        run_id: "run-phase-3".to_owned(),
        resource_uid: "resource-phase-3".to_owned(),
        members: std::array::from_fn(|index| SqlMember {
            ordinal: (index + 1) as u8,
            server_name: format!("sql-{}", index + 1),
            container_name: format!("container-{}", index + 1),
            data_directory: root.join(format!("member-{}", index + 1)),
        }),
        kuberic_members: std::array::from_fn(|index| KubericMember {
            ordinal: (index + 1) as u8,
            replica_id: (index + 1) as i64,
            instance_id: format!("instance-{}", index + 1),
            pod_uid: format!("pod-{}", index + 1),
            pvc_uid: format!("pvc-{}", index + 1),
        }),
    }
}

fn binding(id: &str) -> ResourceBinding {
    ResourceBinding {
        immutable_id: id.to_owned(),
        attributes_sha256: "a".repeat(64),
    }
}

fn record(
    kind: ResourceKind,
    name: &str,
    state: ResourceState,
    resource_binding: Option<ResourceBinding>,
) -> ResourceRecord {
    ResourceRecord {
        kind,
        logical_name: name.to_owned(),
        path: matches!(kind, ResourceKind::DataDirectory | ResourceKind::SecretFile)
            .then(|| PathBuf::from(format!("/owned/{name}"))),
        binding: resource_binding,
        state,
    }
}

#[derive(Clone)]
struct FakeHost {
    platform: HostPlatform,
    memory: u64,
    cpus: u32,
    spaces: RefCell<VecDeque<u64>>,
    uid: u32,
}

impl FakeHost {
    fn sufficient() -> Self {
        let policy = ResourcePolicy::default();
        Self {
            platform: HostPlatform {
                operating_system: "linux".to_owned(),
                architecture: "x86_64".to_owned(),
            },
            memory: policy.minimum_available_memory_bytes,
            cpus: policy.minimum_effective_cpus,
            spaces: RefCell::new(VecDeque::from([
                policy.minimum_fixture_bytes,
                policy.minimum_docker_root_after_image_bytes,
                policy.minimum_fixture_bytes,
            ])),
            uid: effective_uid(),
        }
    }
}

impl HostProbe for FakeHost {
    fn platform(&self) -> Result<HostPlatform, PreflightError> {
        Ok(self.platform.clone())
    }

    fn available_memory_bytes(&self) -> Result<u64, PreflightError> {
        Ok(self.memory)
    }

    fn effective_cpus(&self) -> Result<u32, PreflightError> {
        Ok(self.cpus)
    }

    fn available_space_bytes(&self, _: &Path) -> Result<u64, PreflightError> {
        self.spaces
            .borrow_mut()
            .pop_front()
            .ok_or(PreflightError::Unverifiable)
    }

    fn host_uid(&self) -> Result<u32, PreflightError> {
        Ok(self.uid)
    }
}

struct FakeAclProbe {
    result: Result<(), PreflightError>,
    calls: Cell<u32>,
}

impl FakeAclProbe {
    fn supported() -> Self {
        Self {
            result: Ok(()),
            calls: Cell::new(0),
        }
    }
}

impl AclProbe for FakeAclProbe {
    fn verify(&self, _: &Path, _: u32, sql_uid: u32, _: Duration) -> Result<(), PreflightError> {
        assert_eq!(sql_uid, SQL_SERVER_UID);
        self.calls.set(self.calls.get() + 1);
        self.result.clone()
    }
}

struct FakeDocker {
    capabilities: DockerCapabilities,
    image: RefCell<Option<ImageInspection>>,
    pulled_image: Option<ImageInspection>,
    pulls: Cell<u32>,
    network_creates: Cell<u32>,
    container_creates: Cell<u32>,
}

impl FakeDocker {
    fn valid_image() -> ImageInspection {
        ImageInspection {
            id: "sha256:image-id".to_owned(),
            operating_system: "linux".to_owned(),
            architecture: "amd64".to_owned(),
            repo_digests: vec![PINNED_SQL_SERVER_IMAGE.to_owned()],
            labels: BTreeMap::from([(
                "com.microsoft.product".to_owned(),
                "Microsoft SQL Server".to_owned(),
            )]),
        }
    }

    fn cached() -> Self {
        Self {
            capabilities: DockerCapabilities {
                local: true,
                linux: true,
                x86_64: true,
                memory_limit: true,
                swap_limit: true,
                cpu_quota: true,
            },
            image: RefCell::new(Some(Self::valid_image())),
            pulled_image: Some(Self::valid_image()),
            pulls: Cell::new(0),
            network_creates: Cell::new(0),
            container_creates: Cell::new(0),
        }
    }

    fn absent() -> Self {
        Self {
            image: RefCell::new(None),
            ..Self::cached()
        }
    }

    fn create_count(&self) -> u32 {
        self.network_creates.get() + self.container_creates.get()
    }
}

impl DockerApi for FakeDocker {
    fn capabilities(&self, _: Duration) -> Result<DockerCapabilities, DockerError> {
        Ok(self.capabilities)
    }

    fn docker_root(&self, _: Duration) -> Result<PathBuf, DockerError> {
        Ok(PathBuf::from("/docker-root"))
    }

    fn inspect_image(&self, _: &str, _: Duration) -> Result<Option<ImageInspection>, DockerError> {
        Ok(self.image.borrow().clone())
    }

    fn pull_image(&self, image: &str, timeout: Duration) -> Result<(), DockerError> {
        assert_eq!(image, PINNED_SQL_SERVER_IMAGE);
        assert_eq!(timeout, Duration::from_secs(900));
        self.pulls.set(self.pulls.get() + 1);
        *self.image.borrow_mut() = self.pulled_image.clone();
        Ok(())
    }

    fn inspect_network(
        &self,
        _: &str,
        _: Duration,
    ) -> Result<Option<NetworkInspection>, DockerError> {
        Ok(None)
    }

    fn create_network(&self, _: &NetworkRequest, _: Duration) -> Result<String, DockerError> {
        self.network_creates.set(self.network_creates.get() + 1);
        Ok("network-id".to_owned())
    }

    fn remove_network(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Ok(())
    }

    fn inspect_container(
        &self,
        _: &str,
        _: Duration,
    ) -> Result<Option<ContainerInspection>, DockerError> {
        Ok(None)
    }

    fn create_container(&self, _: &ContainerRequest, _: Duration) -> Result<String, DockerError> {
        self.container_creates.set(self.container_creates.get() + 1);
        Ok("container-id".to_owned())
    }

    fn start_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Ok(())
    }

    fn stop_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Ok(())
    }

    fn remove_container(&self, _: &str, _: Duration) -> Result<(), DockerError> {
        Ok(())
    }
}

#[test]
fn cgroup_memory_and_cpu_thresholds_use_the_effective_minimum() {
    assert_eq!(
        available_memory(20 * 1024, Some(12 * 1024), Some(3 * 1024)),
        9 * 1024
    );
    assert_eq!(
        available_memory(8 * 1024, Some(20 * 1024), Some(1)),
        8 * 1024
    );
    assert_eq!(parse_cpu_list("0-2,5,8-9").unwrap(), 6);
    assert_eq!(effective_cpu_count(8, Some(4), Some((250_000, 100_000))), 2);
    assert_eq!(effective_cpu_count(8, Some(1), None), 1);
}

#[test]
fn cgroup_v2_limits_use_all_ancestors_and_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let mount = directory.path().join("cgroup");
    let team = mount.join("team");
    let current = team.join("job");
    fs::create_dir_all(&current).unwrap();
    for (path, memory_max, memory_current, cpu_max) in [
        (&mount, "20000", "1024", "400000 100000"),
        (&team, "12000", "5000", "250000 100000"),
        (&current, "max", "6000", "100000 100000"),
    ] {
        fs::write(path.join("memory.max"), memory_max).unwrap();
        fs::write(path.join("memory.current"), memory_current).unwrap();
        fs::write(path.join("cpu.max"), cpu_max).unwrap();
    }
    assert_eq!(
        cgroup_v2_available_memory(&mount, &current, 50_000).unwrap(),
        7_000
    );
    assert_eq!(
        cgroup_v2_effective_cpu_quota(&mount, &current).unwrap(),
        Some((100_000, 100_000))
    );

    fs::write(team.join("memory.max"), "not-a-limit").unwrap();
    assert_eq!(
        cgroup_v2_available_memory(&mount, &current, 50_000).unwrap_err(),
        PreflightError::Unverifiable
    );
    fs::write(team.join("memory.max"), "12000").unwrap();
    fs::remove_file(mount.join("cpu.max")).unwrap();
    assert_eq!(
        cgroup_v2_effective_cpu_quota(&mount, &current).unwrap_err(),
        PreflightError::Unverifiable
    );
}

#[test]
fn preflight_enforces_exact_numeric_policy_and_deadlines() {
    let policy = ResourcePolicy::default();
    assert_eq!(policy.container_memory_bytes, 3 * 1024 * 1024 * 1024);
    assert_eq!(policy.container_cpu_nanos, 2_000_000_000);
    assert_eq!(policy.sql_server_memory_mb, 2048);
    assert_eq!(
        policy.minimum_available_memory_bytes,
        10 * 1024 * 1024 * 1024
    );
    assert_eq!(policy.minimum_fixture_bytes, 15 * 1024 * 1024 * 1024);
    assert_eq!(
        policy.minimum_docker_root_before_pull_bytes,
        8 * 1024 * 1024 * 1024
    );
    assert_eq!(
        policy.minimum_docker_root_after_image_bytes,
        2 * 1024 * 1024 * 1024
    );
    let directory = tempfile::tempdir().unwrap();
    let deadlines = fixture_config(directory.path()).deadlines();
    assert_eq!(deadlines.docker_command, Duration::from_secs(30));
    assert_eq!(deadlines.image_pull, Duration::from_secs(900));
    assert_eq!(deadlines.complete_run, Duration::from_secs(1200));
    assert_eq!(deadlines.cleanup, Duration::from_secs(180));
}

#[test]
fn every_preflight_failure_precedes_network_and_container_creation() {
    let directory = tempfile::tempdir().unwrap();
    let config = fixture_config(directory.path());
    let policy = config.resources();
    let cases = [
        FakeHost {
            memory: policy.minimum_available_memory_bytes - 1,
            ..FakeHost::sufficient()
        },
        FakeHost {
            cpus: policy.minimum_effective_cpus - 1,
            ..FakeHost::sufficient()
        },
        FakeHost {
            spaces: RefCell::new(VecDeque::from([policy.minimum_fixture_bytes - 1])),
            ..FakeHost::sufficient()
        },
        FakeHost {
            platform: HostPlatform {
                operating_system: "linux".to_owned(),
                architecture: "aarch64".to_owned(),
            },
            ..FakeHost::sufficient()
        },
    ];
    for host in cases {
        let docker = FakeDocker::cached();
        assert!(run_preflight(&config, &host, &FakeAclProbe::supported(), &docker).is_err());
        assert_eq!(docker.create_count(), 0);
    }

    let mut docker = FakeDocker::cached();
    docker.capabilities.swap_limit = false;
    assert!(
        run_preflight(
            &config,
            &FakeHost::sufficient(),
            &FakeAclProbe::supported(),
            &docker
        )
        .is_err()
    );
    assert_eq!(docker.create_count(), 0);
}

#[test]
fn changed_acknowledgement_and_acl_failures_create_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let acknowledgement = directory.path().join("acknowledgement.json");
    write_acknowledgement(&acknowledgement);
    let config = FixtureConfig::new(directory.path().join("fixture"), &acknowledgement).unwrap();
    fs::write(
        &acknowledgement,
        r#"{"schema_version":1,"sql_server_eula":{"accepted":false}}"#,
    )
    .unwrap();
    let docker = FakeDocker::cached();
    assert_eq!(
        run_preflight(
            &config,
            &FakeHost::sufficient(),
            &FakeAclProbe::supported(),
            &docker
        )
        .unwrap_err(),
        PreflightError::Acknowledgement
    );
    assert_eq!(docker.create_count(), 0);

    write_acknowledgement(&acknowledgement);
    let config = FixtureConfig::new(directory.path().join("fixture"), &acknowledgement).unwrap();
    for error in [PreflightError::AclTools, PreflightError::AclFilesystem] {
        let acl = FakeAclProbe {
            result: Err(error.clone()),
            calls: Cell::new(0),
        };
        assert_eq!(
            run_preflight(&config, &FakeHost::sufficient(), &acl, &docker).unwrap_err(),
            error
        );
        assert_eq!(docker.create_count(), 0);
    }
}

#[test]
fn cached_image_uses_post_image_capacity_without_pull() {
    let directory = tempfile::tempdir().unwrap();
    let config = fixture_config(directory.path());
    let docker = FakeDocker::cached();
    let report = run_preflight(
        &config,
        &FakeHost::sufficient(),
        &FakeAclProbe::supported(),
        &docker,
    )
    .unwrap();
    assert!(report.image_was_cached);
    assert_eq!(docker.pulls.get(), 0);
    assert_eq!(docker.create_count(), 0);
}

#[test]
fn absent_image_requires_pre_pull_and_post_pull_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let config = fixture_config(directory.path());
    let policy = config.resources();
    let host = FakeHost {
        spaces: RefCell::new(VecDeque::from([
            policy.minimum_fixture_bytes,
            policy.minimum_docker_root_before_pull_bytes,
            policy.minimum_docker_root_after_image_bytes,
            policy.minimum_fixture_bytes,
        ])),
        ..FakeHost::sufficient()
    };
    let docker = FakeDocker::absent();
    let report = run_preflight(&config, &host, &FakeAclProbe::supported(), &docker).unwrap();
    assert!(!report.image_was_cached);
    assert_eq!(docker.pulls.get(), 1);
    assert_eq!(docker.create_count(), 0);
}

#[test]
fn fixture_capacity_is_rechecked_after_image_preparation() {
    let directory = tempfile::tempdir().unwrap();
    let config = fixture_config(directory.path());
    let policy = config.resources();
    let host = FakeHost {
        spaces: RefCell::new(VecDeque::from([
            policy.minimum_fixture_bytes,
            policy.minimum_docker_root_after_image_bytes,
            policy.minimum_fixture_bytes - 1,
        ])),
        ..FakeHost::sufficient()
    };
    assert!(matches!(
        run_preflight(
            &config,
            &host,
            &FakeAclProbe::supported(),
            &FakeDocker::cached()
        ),
        Err(PreflightError::InsufficientFixtureSpace { .. })
    ));
}

#[test]
fn image_capacity_and_identity_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let config = fixture_config(directory.path());
    let policy = config.resources();
    let host = FakeHost {
        spaces: RefCell::new(VecDeque::from([
            policy.minimum_fixture_bytes,
            policy.minimum_docker_root_before_pull_bytes - 1,
        ])),
        ..FakeHost::sufficient()
    };
    let docker = FakeDocker::absent();
    assert!(matches!(
        run_preflight(&config, &host, &FakeAclProbe::supported(), &docker),
        Err(PreflightError::InsufficientDockerSpace { .. })
    ));
    assert_eq!(docker.pulls.get(), 0);

    let docker = FakeDocker::cached();
    docker.image.borrow_mut().as_mut().unwrap().architecture = "arm64".to_owned();
    assert_eq!(
        run_preflight(
            &config,
            &FakeHost::sufficient(),
            &FakeAclProbe::supported(),
            &docker
        )
        .unwrap_err(),
        PreflightError::ImageMismatch
    );

    let host = FakeHost {
        spaces: RefCell::new(VecDeque::from([
            policy.minimum_fixture_bytes,
            policy.minimum_docker_root_before_pull_bytes,
            policy.minimum_docker_root_after_image_bytes - 1,
        ])),
        ..FakeHost::sufficient()
    };
    let docker = FakeDocker::absent();
    assert!(matches!(
        run_preflight(&config, &host, &FakeAclProbe::supported(), &docker),
        Err(PreflightError::InsufficientDockerSpace { .. })
    ));
    assert_eq!(docker.pulls.get(), 1);
}

#[test]
fn remote_docker_engines_are_rejected_before_creation() {
    let directory = tempfile::tempdir().unwrap();
    let config = fixture_config(directory.path());
    let mut docker = FakeDocker::cached();
    docker.capabilities.local = false;
    assert_eq!(
        run_preflight(
            &config,
            &FakeHost::sufficient(),
            &FakeAclProbe::supported(),
            &docker,
        )
        .unwrap_err(),
        PreflightError::DockerNotLocal
    );
    assert_eq!(docker.create_count(), 0);
}

#[test]
fn container_request_freezes_limits_mount_labels_and_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("data");
    fs::create_dir(&data).unwrap();
    let labels = OwnedLabels::container("run-1", 1);
    let request = ContainerRequest::sql_server(
        "sql-1",
        "sql-1",
        "network-1",
        labels.clone(),
        &data,
        ResourcePolicy::default(),
        [("ACCEPT_EULA", "Y")],
    );
    assert_eq!(request.limits.memory_bytes, 3 * 1024 * 1024 * 1024);
    assert_eq!(
        request.limits.memory_swap_bytes,
        request.limits.memory_bytes
    );
    assert_eq!(request.limits.nano_cpus, 2_000_000_000);
    assert_eq!(
        request
            .environment
            .iter()
            .filter(|(key, value)| key == "ACCEPT_EULA" && value == "Y")
            .count(),
        1
    );
    let inspection = ContainerInspection {
        id: "container-id".to_owned(),
        name: "sql-1".to_owned(),
        image_id: "image-id".to_owned(),
        labels: labels.as_map(),
        environment: request
            .environment
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect(),
        user: "mssql".to_owned(),
        running: false,
        restart_policy: "no".to_owned(),
        mounts: vec![ContainerMount {
            source: data.canonicalize().unwrap(),
            destination: PathBuf::from("/var/opt/mssql"),
            read_only: false,
        }],
        ports: vec![ContainerPort {
            container_port: 1433,
            host_ip: "127.0.0.1".to_owned(),
            host_port: 49152,
        }],
        limits: ContainerLimits::from_policy(ResourcePolicy::default()),
    };
    request.verify_inspection(&inspection, "image-id").unwrap();

    let mut wrong = inspection;
    wrong.limits.memory_swap_bytes += 1;
    assert_eq!(
        request.verify_inspection(&wrong, "image-id").unwrap_err(),
        DockerError::OwnershipMismatch
    );
}

#[test]
fn exact_ownership_labels_reject_foreign_lookalikes() {
    let expected = OwnedLabels::container("run-1", 2);
    let mut labels = expected.as_map();
    assert!(expected.matches(&labels));
    labels.insert("io.kuberic.mssql.run".to_owned(), "run-2".to_owned());
    assert!(!expected.matches(&labels));
    labels.insert("io.kuberic.mssql.run".to_owned(), "run-1".to_owned());
    labels.insert("io.kuberic.mssql.member".to_owned(), "1".to_owned());
    assert!(!expected.matches(&labels));
}

#[derive(Default)]
struct ScriptedRunner {
    results: Mutex<VecDeque<Result<ProcessResult, ProcessError>>>,
    commands: Mutex<Vec<CommandSpec>>,
}

impl ProcessRunner for ScriptedRunner {
    fn run(&self, command: &CommandSpec) -> Result<ProcessResult, ProcessError> {
        self.commands.lock().unwrap().push(command.clone());
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted process result")
    }
}

fn successful(stdout: impl Into<String>) -> Result<ProcessResult, ProcessError> {
    Ok(ProcessResult {
        status: 0,
        stdout: stdout.into(),
        stderr: String::new(),
        child: ChildDisposition {
            pid: Some(10),
            terminated: false,
            reaped: true,
        },
    })
}

fn failed(diagnostic: &str) -> Result<ProcessResult, ProcessError> {
    Err(ProcessError::new(
        ProcessErrorKind::Exit,
        "docker command",
        Some(1),
        diagnostic,
        ChildDisposition {
            pid: Some(10),
            terminated: false,
            reaped: true,
        },
    ))
}

#[test]
fn docker_cli_parses_structured_inspection_and_never_invokes_a_shell() {
    let image_json = serde_json::json!([{
        "Id": "image-id",
        "Os": "linux",
        "Architecture": "amd64",
        "RepoDigests": [PINNED_SQL_SERVER_IMAGE],
        "Config": {"Labels": {"com.microsoft.product": "Microsoft SQL Server"}}
    }]);
    let runner = ScriptedRunner::default();
    runner
        .results
        .lock()
        .unwrap()
        .push_back(successful(image_json.to_string()));
    let docker = DockerCli::new(runner);
    docker
        .inspect_image(PINNED_SQL_SERVER_IMAGE, Duration::from_secs(30))
        .unwrap()
        .unwrap()
        .verify_pinned_sql_server()
        .unwrap();
    let command = docker.runner().commands.lock().unwrap()[0].clone();
    assert_eq!(command.program(), Path::new("docker"));
    assert_eq!(
        command.arguments(),
        ["image", "inspect", PINNED_SQL_SERVER_IMAGE]
            .map(std::ffi::OsString::from)
            .as_slice()
    );
}

#[test]
fn process_timeout_model_requires_exact_termination_and_reaping() {
    let runner = ScriptedRunner::default();
    runner
        .results
        .lock()
        .unwrap()
        .push_back(Err(ProcessError::new(
            ProcessErrorKind::Timeout,
            "create Docker resource",
            None,
            "deadline exceeded",
            ChildDisposition {
                pid: Some(42),
                terminated: true,
                reaped: true,
            },
        )));
    let error = runner
        .run(&CommandSpec::new(
            "docker",
            "create Docker resource",
            Duration::from_secs(30),
        ))
        .unwrap_err();
    assert_eq!(error.kind(), ProcessErrorKind::Timeout);
    assert_eq!(
        error.child(),
        ChildDisposition {
            pid: Some(42),
            terminated: true,
            reaped: true,
        }
    );
}

#[test]
fn bounded_process_runner_terminates_descendants_that_retain_pipes() {
    let started = Instant::now();
    let result = BoundedProcessRunner
        .run(
            &CommandSpec::new("/bin/sh", "fork descriptor holder", Duration::from_secs(2))
                .args(["-c", "sleep 30 & echo $!; exit 0"]),
        )
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(result.child.reaped);
    let descendant = result.stdout.trim().parse::<i32>().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while unsafe { libc::kill(descendant, 0) } == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(unsafe { libc::kill(descendant, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}

#[test]
fn docker_absence_is_classified_per_resource_and_not_by_generic_no_such_text() {
    for (diagnostic, operation) in [
        ("Error response from daemon: No such image: pinned", "image"),
        (
            "Error response from daemon: network owned-network not found",
            "network",
        ),
        ("Error: No such container: owned-container", "container"),
    ] {
        let runner = ScriptedRunner::default();
        runner.results.lock().unwrap().push_back(failed(diagnostic));
        let docker = DockerCli::new(runner);
        match operation {
            "image" => assert!(
                docker
                    .inspect_image("pinned", Duration::from_secs(1))
                    .unwrap()
                    .is_none()
            ),
            "network" => assert!(
                docker
                    .inspect_network("owned-network", Duration::from_secs(1))
                    .unwrap()
                    .is_none()
            ),
            "container" => assert!(
                docker
                    .inspect_container("owned-container", Duration::from_secs(1))
                    .unwrap()
                    .is_none()
            ),
            _ => unreachable!(),
        }
    }

    let runner = ScriptedRunner::default();
    runner.results.lock().unwrap().push_back(failed(
        "docker: failed to open config: No such file or directory",
    ));
    let docker = DockerCli::new(runner);
    assert_eq!(
        docker
            .inspect_container("owned-container", Duration::from_secs(1))
            .unwrap_err(),
        DockerError::Command
    );

    let runner = ScriptedRunner::default();
    runner.results.lock().unwrap().extend([
        failed("Error response from daemon: network owned-network not found"),
        failed("Error response from daemon: No such container: owned-container"),
    ]);
    let docker = DockerCli::new(runner);
    docker
        .remove_network("owned-network", Duration::from_secs(1))
        .unwrap();
    docker
        .remove_container("owned-container", Duration::from_secs(1))
        .unwrap();
}

#[test]
fn process_diagnostics_and_debug_output_redact_sensitive_arguments() {
    let command = CommandSpec::new("helper", "run helper", Duration::from_secs(1))
        .arg("--password")
        .sensitive_arg("actual-secret");
    assert_eq!(
        command.sanitize_diagnostic(b"server rejected actual-secret"),
        "server rejected <redacted>"
    );
    let debug = format!("{command:?}");
    assert!(!debug.contains("actual-secret"));
    let error = ProcessError::new(
        ProcessErrorKind::Exit,
        "run helper",
        Some(1),
        command.sanitize_diagnostic(b"actual-secret failed"),
        ChildDisposition {
            pid: Some(1),
            terminated: false,
            reaped: true,
        },
    );
    assert!(!error.to_string().contains("actual-secret"));
}

#[test]
fn canonical_root_aliases_share_one_exclusive_lock() {
    let directory = tempfile::tempdir().unwrap();
    let real_parent = directory.path().join("real");
    fs::create_dir(&real_parent).unwrap();
    let alias = directory.path().join("alias");
    symlink(&real_parent, &alias).unwrap();
    let first = acquire_root_lock(&real_parent.join("fixture")).unwrap();
    assert_eq!(
        acquire_root_lock(&alias.join("fixture")).unwrap_err(),
        LockError::Contended
    );
    drop(first);
    acquire_root_lock(&alias.join("fixture")).unwrap();
}

#[test]
fn private_root_and_journal_updates_are_durable_and_same_directory() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    let store = JournalStore::initialize(&root).unwrap();
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let mut journal = store.create(sample_run(&root)).unwrap();
    let index = store
        .record_intent(
            &mut journal,
            record(
                ResourceKind::Network,
                "network",
                ResourceState::Intended,
                None,
            ),
        )
        .unwrap();
    assert_eq!(
        store.load().unwrap().unwrap().resources[index].state,
        ResourceState::Intended
    );
    store.mark_dispatched(&mut journal, index).unwrap();
    assert_eq!(
        store.load().unwrap().unwrap().resources[index].state,
        ResourceState::Dispatched
    );
    store
        .bind(&mut journal, index, binding("network-id"))
        .unwrap();
    assert_eq!(
        store.load().unwrap().unwrap().resources[index].state,
        ResourceState::Bound
    );
    assert!(fs::read_dir(&root).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".new")
    }));
}

#[test]
fn journal_store_rejects_renamed_root_and_symlink_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    let moved = directory.path().join("moved-fixture");
    let attacker = directory.path().join("attacker");
    let store = JournalStore::initialize(&root).unwrap();
    let journal = store.create(sample_run(&root)).unwrap();

    fs::rename(&root, &moved).unwrap();
    fs::create_dir(&attacker).unwrap();
    fs::set_permissions(&attacker, fs::Permissions::from_mode(0o700)).unwrap();
    symlink(&attacker, &root).unwrap();

    assert!(matches!(
        store.load(),
        Err(ReconcileError::OwnershipMismatch | ReconcileError::Io)
    ));
    assert!(matches!(
        store.save(&journal),
        Err(ReconcileError::OwnershipMismatch | ReconcileError::Io)
    ));
    assert!(!attacker.join("ownership.json").exists());
    assert!(moved.join("ownership.json").exists());
}

#[test]
fn every_host_resource_create_boundary_persists_intent_then_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    let store = JournalStore::initialize(&root).unwrap();
    let mut journal = store.create(sample_run(&root)).unwrap();
    for (index, kind) in [
        ResourceKind::Network,
        ResourceKind::DataDirectory,
        ResourceKind::SecretFile,
        ResourceKind::Container,
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("resource-{index}");
        let record_index = store
            .record_intent(
                &mut journal,
                record(kind, &name, ResourceState::Intended, None),
            )
            .unwrap();
        assert_eq!(
            store.load().unwrap().unwrap().resources[record_index].state,
            ResourceState::Intended
        );
        store.mark_dispatched(&mut journal, record_index).unwrap();
        assert_eq!(
            store.load().unwrap().unwrap().resources[record_index].state,
            ResourceState::Dispatched
        );
    }
}

#[test]
fn stale_or_unknown_journal_schema_blocks_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    let store = JournalStore::initialize(&root).unwrap();
    let journal = store.create(sample_run(&root)).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&journal.to_json().unwrap()).unwrap();
    value["schema_version"] = serde_json::json!(999);
    fs::write(store.path(), serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(store.load().is_err());

    value["schema_version"] = serde_json::json!(1);
    value["unknown"] = serde_json::json!(true);
    fs::write(store.path(), serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(store.load().is_err());
}

struct FakeAclController {
    evidence: AclEvidence,
    apply_calls: RefCell<Vec<(u32, u32)>>,
}

impl AclController for FakeAclController {
    fn apply(
        &self,
        _: &Path,
        host_uid: u32,
        sql_uid: u32,
        _: Duration,
    ) -> Result<(), kuberic_mssql_tests::three_replica::MemberDirectoryError> {
        self.apply_calls.borrow_mut().push((host_uid, sql_uid));
        Ok(())
    }

    fn inspect(
        &self,
        _: &Path,
        _: u32,
        _: u32,
        _: Duration,
    ) -> Result<AclEvidence, kuberic_mssql_tests::three_replica::MemberDirectoryError> {
        Ok(self.evidence.clone())
    }
}

fn complete_acl() -> AclEvidence {
    AclEvidence {
        host_access: true,
        sql_access: true,
        host_default: true,
        sql_default: true,
        access_mask: true,
        default_mask: true,
    }
}

#[test]
fn acl_parser_requires_access_default_entries_and_effective_masks() {
    let host_uid = effective_uid();
    let complete = format!(
        "user:{host_uid}:rwx\nuser:{SQL_SERVER_UID}:rwx\nmask::rwx\n\
             default:user:{host_uid}:rwx\ndefault:user:{SQL_SERVER_UID}:rwx\n\
             default:mask::rwx\n"
    );
    assert!(parse_acl_evidence(&complete, host_uid, SQL_SERVER_UID).complete());
    let masked = complete.replace("mask::rwx", "mask::r-x");
    assert!(!parse_acl_evidence(&masked, host_uid, SQL_SERVER_UID).complete());
    let missing_default = complete.replace(&format!("default:user:{SQL_SERVER_UID}:rwx\n"), "");
    assert!(!parse_acl_evidence(&missing_default, host_uid, SQL_SERVER_UID).complete());
}

#[test]
fn command_acl_controller_validates_inheritance_and_masks_deterministically() {
    let host_uid = effective_uid();
    let output = format!(
        "user:{host_uid}:rwx\nuser:{SQL_SERVER_UID}:rwx\nmask::rwx\n\
             default:user:{host_uid}:rwx\ndefault:user:{SQL_SERVER_UID}:rwx\n\
             default:mask::rwx\n"
    );
    let runner = ScriptedRunner::default();
    runner.results.lock().unwrap().push_back(successful(output));
    let controller = CommandAclController::new(runner);
    assert!(
        controller
            .inspect(
                Path::new("/fixture/member/nested"),
                host_uid,
                SQL_SERVER_UID,
                Duration::from_secs(1),
            )
            .unwrap()
            .complete()
    );
}

#[test]
fn member_directories_bind_host_and_sql_uid_access_and_default_acls() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let acl = FakeAclController {
        evidence: complete_acl(),
        apply_calls: RefCell::new(Vec::new()),
    };
    let path = root.join("member-1");
    let host_uid = effective_uid();
    let expected = prepare_member_directory(
        &root,
        &path,
        host_uid,
        SQL_SERVER_UID,
        Duration::from_secs(30),
        &acl,
    )
    .unwrap();
    assert_eq!(
        acl.apply_calls.borrow().as_slice(),
        &[(host_uid, SQL_SERVER_UID)]
    );
    assert!(expected.acl.complete());
    verify_member_directory(
        &root,
        &expected,
        host_uid,
        SQL_SERVER_UID,
        Duration::from_secs(30),
        &acl,
    )
    .unwrap();

    let nested = path.join("sql-created").join("data");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("database.mdf"), b"fixture").unwrap();
    assert_eq!(
        inspect_member_directory(
            &root,
            &path,
            host_uid,
            SQL_SERVER_UID,
            Duration::from_secs(30),
            &acl,
        )
        .unwrap()
        .binding,
        expected.binding
    );
    fs::remove_dir_all(&path).unwrap();
    assert!(!path.exists());
}

#[test]
fn wrong_sql_uid_acl_symlinks_and_replaced_paths_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let incomplete = FakeAclController {
        evidence: AclEvidence {
            sql_access: false,
            ..complete_acl()
        },
        apply_calls: RefCell::new(Vec::new()),
    };
    assert!(
        prepare_member_directory(
            &root,
            &root.join("member-bad"),
            effective_uid(),
            SQL_SERVER_UID,
            Duration::from_secs(30),
            &incomplete,
        )
        .is_err()
    );

    let acl = FakeAclController {
        evidence: complete_acl(),
        apply_calls: RefCell::new(Vec::new()),
    };
    let path = root.join("member-good");
    let host_uid = effective_uid();
    let expected = prepare_member_directory(
        &root,
        &path,
        host_uid,
        SQL_SERVER_UID,
        Duration::from_secs(30),
        &acl,
    )
    .unwrap();
    fs::remove_dir_all(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(
        verify_member_directory(
            &root,
            &expected,
            host_uid,
            SQL_SERVER_UID,
            Duration::from_secs(30),
            &acl,
        )
        .is_err()
    );

    let target = root.join("target");
    fs::create_dir(&target).unwrap();
    let link = root.join("member-link");
    symlink(&target, &link).unwrap();
    assert!(
        prepare_member_directory(
            &root,
            &link,
            host_uid,
            SQL_SERVER_UID,
            Duration::from_secs(30),
            &acl,
        )
        .is_err()
    );
}

#[test]
#[ignore = "requires setfacl/getfacl and CAP_CHOWN as a non-root host user"]
fn real_acl_allows_host_recursive_delete_of_uid_10001_nested_files() {
    let host_uid = effective_uid();
    if host_uid == 0 {
        eprintln!("SKIPPED: root deletion would not prove host-user ACL authority");
        return;
    }
    let runner = BoundedProcessRunner;
    if runner
        .run(&CommandSpec::new("setfacl", "check setfacl", Duration::from_secs(2)).arg("--version"))
        .is_err()
        || runner
            .run(
                &CommandSpec::new("getfacl", "check getfacl", Duration::from_secs(2))
                    .arg("--version"),
            )
            .is_err()
    {
        eprintln!("SKIPPED: ACL tools are unavailable");
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let member = root.join("member");
    let controller = CommandAclController::new(BoundedProcessRunner);
    prepare_member_directory(
        &root,
        &member,
        host_uid,
        SQL_SERVER_UID,
        Duration::from_secs(5),
        &controller,
    )
    .unwrap();
    let nested = member.join("sql-created").join("deep");
    fs::create_dir_all(&nested).unwrap();
    let file = nested.join("database.mdf");
    fs::write(&file, b"sql-owned").unwrap();
    assert!(
        controller
            .inspect(&nested, host_uid, SQL_SERVER_UID, Duration::from_secs(5),)
            .unwrap()
            .complete()
    );

    let mut chowned = true;
    for path in [&file, &nested, &member.join("sql-created")] {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        if unsafe { libc::chown(path.as_ptr(), SQL_SERVER_UID, u32::MAX) } != 0 {
            chowned = false;
            break;
        }
    }
    if !chowned {
        eprintln!("SKIPPED: current process lacks CAP_CHOWN");
        fs::remove_dir_all(&member).unwrap();
        return;
    }
    fs::remove_dir_all(&member).unwrap();
    assert!(!member.exists());
}

#[derive(Default)]
struct FakeLifecycle {
    observations: RefCell<HashMap<String, VecDeque<ResourceObservation>>>,
    removals: RefCell<Vec<String>>,
    fail_removal: RefCell<HashSet<String>>,
}

impl FakeLifecycle {
    fn set(&self, name: &str, observations: impl IntoIterator<Item = ResourceObservation>) {
        self.observations
            .borrow_mut()
            .insert(name.to_owned(), observations.into_iter().collect());
    }

    fn remove(
        &self,
        resource: &ResourceRecord,
        category: FailureCategory,
    ) -> Result<(), CleanupError> {
        self.removals
            .borrow_mut()
            .push(resource.logical_name.clone());
        if self.fail_removal.borrow().contains(&resource.logical_name) {
            return Err(CleanupError {
                resource: resource.logical_name.clone(),
                failure: SanitizedFailure::new(FailureStage::Cleanup, category),
            });
        }
        self.set(&resource.logical_name, [ResourceObservation::Absent]);
        Ok(())
    }
}

impl OwnershipInspector for FakeLifecycle {
    fn inspect(&self, resource: &ResourceRecord) -> Result<ResourceObservation, ReconcileError> {
        let mut observations = self.observations.borrow_mut();
        let queue = observations
            .get_mut(&resource.logical_name)
            .ok_or(ReconcileError::OwnershipMismatch)?;
        if queue.len() > 1 {
            Ok(queue.pop_front().unwrap())
        } else {
            queue
                .front()
                .cloned()
                .ok_or(ReconcileError::OwnershipMismatch)
        }
    }
}

impl CleanupBackend for FakeLifecycle {
    fn remove_container(&self, resource: &ResourceRecord, _: Duration) -> Result<(), CleanupError> {
        self.remove(resource, FailureCategory::ContainerRemoval)
    }

    fn remove_network(&self, resource: &ResourceRecord, _: Duration) -> Result<(), CleanupError> {
        self.remove(resource, FailureCategory::NetworkRemoval)
    }

    fn remove_path(&self, resource: &ResourceRecord, _: Duration) -> Result<(), CleanupError> {
        self.remove(resource, FailureCategory::PathRemoval)
    }
}

#[derive(Clone)]
struct FakeCleanupClock {
    now: Rc<Cell<Duration>>,
}

impl FakeCleanupClock {
    fn new() -> Self {
        Self {
            now: Rc::new(Cell::new(Duration::ZERO)),
        }
    }

    fn advance_within(&self, requested: Duration, cost: Duration) {
        self.now.set(self.now.get() + requested.min(cost));
    }
}

impl CleanupClock for FakeCleanupClock {
    fn now(&self) -> Duration {
        self.now.get()
    }
}

struct BudgetLifecycle {
    clock: FakeCleanupClock,
    operation_cost: Duration,
    observations: RefCell<HashMap<String, ResourceObservation>>,
    remaining: RefCell<Vec<Duration>>,
    removals: RefCell<Vec<String>>,
}

impl OwnershipInspector for BudgetLifecycle {
    fn inspect(&self, resource: &ResourceRecord) -> Result<ResourceObservation, ReconcileError> {
        self.observations
            .borrow()
            .get(&resource.logical_name)
            .cloned()
            .ok_or(ReconcileError::OwnershipMismatch)
    }
}

impl CleanupBackend for BudgetLifecycle {
    fn inspect_cleanup(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<ResourceObservation, ReconcileError> {
        self.remaining.borrow_mut().push(remaining);
        self.clock.advance_within(remaining, self.operation_cost);
        self.inspect(resource)
    }

    fn remove_container(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        self.remaining.borrow_mut().push(remaining);
        self.clock.advance_within(remaining, self.operation_cost);
        self.removals
            .borrow_mut()
            .push(resource.logical_name.clone());
        self.observations
            .borrow_mut()
            .insert(resource.logical_name.clone(), ResourceObservation::Absent);
        Ok(())
    }

    fn remove_network(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        self.remove_container(resource, remaining)
    }

    fn remove_path(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError> {
        self.remove_container(resource, remaining)
    }
}

#[test]
fn lost_create_output_is_bound_from_exact_post_create_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
    let mut journal = store.create(sample_run(store.root())).unwrap();
    journal.resources.push(record(
        ResourceKind::Container,
        "sql-1",
        ResourceState::Dispatched,
        None,
    ));
    store.save(&journal).unwrap();
    let backend = FakeLifecycle::default();
    backend.set(
        "sql-1",
        [ResourceObservation::Owned {
            binding: binding("container-id"),
            foreign_attachments: Vec::new(),
        }],
    );
    let report = reconcile(&store, &mut journal, &backend).unwrap();
    assert_eq!(report.recovered, ["sql-1"]);
    assert_eq!(journal.resources[0].state, ResourceState::Bound);
    assert_eq!(
        journal.resources[0].binding.as_ref().unwrap().immutable_id,
        "container-id"
    );
}

#[test]
fn dispatched_create_initial_absence_remains_blocked_for_late_daemon_completion() {
    let directory = tempfile::tempdir().unwrap();
    let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
    let mut journal = store.create(sample_run(store.root())).unwrap();
    journal.resources.push(record(
        ResourceKind::Container,
        "sql-late",
        ResourceState::Dispatched,
        None,
    ));
    store.save(&journal).unwrap();
    let backend = FakeLifecycle::default();
    backend.set(
        "sql-late",
        [
            ResourceObservation::Absent,
            ResourceObservation::Owned {
                binding: binding("late-id"),
                foreign_attachments: Vec::new(),
            },
            ResourceObservation::Absent,
        ],
    );
    let first = cleanup(&store, &mut journal, &backend);
    assert!(!first.succeeded());
    assert_eq!(journal.resources[0].state, ResourceState::Blocked);
    assert!(backend.removals.borrow().is_empty());

    let second = cleanup(&store, &mut journal, &backend);
    assert!(second.succeeded());
    assert_eq!(backend.removals.borrow().as_slice(), ["sql-late"]);
    assert_eq!(journal.state, RunState::Removed);
}

#[test]
fn cleanup_is_reverse_order_and_distinguishes_container_path_and_network() {
    let directory = tempfile::tempdir().unwrap();
    let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
    let mut journal = store.create(sample_run(store.root())).unwrap();
    journal.resources = vec![
        record(
            ResourceKind::Network,
            "network",
            ResourceState::Bound,
            Some(binding("network-id")),
        ),
        record(
            ResourceKind::DataDirectory,
            "data",
            ResourceState::Bound,
            Some(binding("data-id")),
        ),
        record(
            ResourceKind::Container,
            "container",
            ResourceState::Bound,
            Some(binding("container-id")),
        ),
    ];
    store.save(&journal).unwrap();
    let backend = FakeLifecycle::default();
    for (name, id) in [
        ("network", "network-id"),
        ("data", "data-id"),
        ("container", "container-id"),
    ] {
        backend.set(
            name,
            [ResourceObservation::Owned {
                binding: binding(id),
                foreign_attachments: Vec::new(),
            }],
        );
    }
    let report = cleanup(&store, &mut journal, &backend);
    assert!(report.succeeded());
    assert_eq!(
        backend.removals.borrow().as_slice(),
        ["container", "data", "network"]
    );
}

#[test]
fn cleanup_budget_is_parent_scoped_and_propagates_only_remaining_time() {
    let directory = tempfile::tempdir().unwrap();
    let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
    let mut journal = store.create(sample_run(store.root())).unwrap();
    journal.resources = vec![
        record(
            ResourceKind::Container,
            "first",
            ResourceState::Bound,
            Some(binding("first-id")),
        ),
        record(
            ResourceKind::Container,
            "second",
            ResourceState::Bound,
            Some(binding("second-id")),
        ),
    ];
    store.save(&journal).unwrap();
    let clock = FakeCleanupClock::new();
    let backend = BudgetLifecycle {
        clock: clock.clone(),
        operation_cost: Duration::from_secs(100),
        observations: RefCell::new(HashMap::from([
            (
                "first".to_owned(),
                ResourceObservation::Owned {
                    binding: binding("first-id"),
                    foreign_attachments: Vec::new(),
                },
            ),
            (
                "second".to_owned(),
                ResourceObservation::Owned {
                    binding: binding("second-id"),
                    foreign_attachments: Vec::new(),
                },
            ),
        ])),
        remaining: RefCell::new(Vec::new()),
        removals: RefCell::new(Vec::new()),
    };
    let coordinator = CleanupCoordinator::new(clock.clone(), Duration::from_secs(180));
    let report = coordinator.cleanup(&store, &mut journal, &backend);
    assert!(!report.succeeded());
    assert_eq!(clock.now(), Duration::from_secs(180));
    assert_eq!(
        backend.remaining.borrow().as_slice(),
        [Duration::from_secs(180), Duration::from_secs(80)]
    );
    assert_eq!(backend.removals.borrow().as_slice(), ["second"]);
    assert!(report.errors.iter().any(|error| {
        error.failure
            == SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::DeadlineExceeded)
    }));
}

#[test]
fn coordinator_routes_results_errors_panics_and_cancellation_through_cleanup() {
    for completion in [
        CleanupCompletion::Result(Ok::<_, SanitizedFailure>("ok")),
        CleanupCompletion::Result(Err(SanitizedFailure::new(
            FailureStage::Test,
            FailureCategory::Preflight,
        ))),
        CleanupCompletion::CaughtPanic(SanitizedFailure::new(
            FailureStage::Test,
            FailureCategory::OwnershipMismatch,
        )),
        CleanupCompletion::HandledSignal {
            signal: HandledCancellationSignal::Interrupt,
            failure: SanitizedFailure::new(FailureStage::Test, FailureCategory::DeadlineExceeded),
        },
        CleanupCompletion::HandledSignal {
            signal: HandledCancellationSignal::Terminate,
            failure: SanitizedFailure::new(FailureStage::Test, FailureCategory::DeadlineExceeded),
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
        let mut journal = store.create(sample_run(store.root())).unwrap();
        journal.resources.push(record(
            ResourceKind::SecretFile,
            "never-dispatched",
            ResourceState::Intended,
            None,
        ));
        store.save(&journal).unwrap();
        let coordinator =
            CleanupCoordinator::new(FakeCleanupClock::new(), Duration::from_secs(180));
        let result =
            coordinator.coordinate(completion, &store, &mut journal, &FakeLifecycle::default());
        assert_eq!(journal.state, RunState::Removed);
        assert_eq!(result.is_ok(), result.as_ref().ok() == Some(&"ok"));
    }
}

#[test]
fn failed_or_unresolved_containers_block_data_directory_and_network_deletion() {
    for late_create in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
        let mut journal = store.create(sample_run(store.root())).unwrap();
        journal.resources = vec![
            record(
                ResourceKind::Network,
                "network",
                ResourceState::Bound,
                Some(binding("network-id")),
            ),
            record(
                ResourceKind::DataDirectory,
                "data",
                ResourceState::Bound,
                Some(binding("data-id")),
            ),
            record(
                ResourceKind::Container,
                "container",
                if late_create {
                    ResourceState::Dispatched
                } else {
                    ResourceState::Bound
                },
                (!late_create).then(|| binding("container-id")),
            ),
        ];
        store.save(&journal).unwrap();
        let backend = FakeLifecycle::default();
        backend.set(
            "container",
            if late_create {
                vec![ResourceObservation::Absent]
            } else {
                vec![ResourceObservation::Owned {
                    binding: binding("container-id"),
                    foreign_attachments: Vec::new(),
                }]
            },
        );
        if !late_create {
            backend
                .fail_removal
                .borrow_mut()
                .insert("container".to_owned());
        }
        let report = cleanup(&store, &mut journal, &backend);
        assert!(!report.succeeded());
        let expected = if late_create {
            Vec::new()
        } else {
            vec!["container".to_owned()]
        };
        assert_eq!(backend.removals.borrow().as_slice(), expected);
        assert_eq!(journal.resources[0].state, ResourceState::Blocked);
        assert_eq!(journal.resources[1].state, ResourceState::Blocked);
    }
}

#[test]
fn cleanup_of_owned_containers_does_not_require_sql_availability_or_native_binding() {
    let directory = tempfile::tempdir().unwrap();
    let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
    let mut journal = store.create(sample_run(store.root())).unwrap();
    assert!(journal.native_binding.is_none());
    journal.resources.push(record(
        ResourceKind::Container,
        "sql-offline",
        ResourceState::Bound,
        Some(binding("offline-id")),
    ));
    store.save(&journal).unwrap();
    let backend = FakeLifecycle::default();
    backend.set(
        "sql-offline",
        [ResourceObservation::Owned {
            binding: binding("offline-id"),
            foreign_attachments: Vec::new(),
        }],
    );
    assert!(cleanup(&store, &mut journal, &backend).succeeded());
}

#[test]
fn ownership_mismatch_foreign_lookalikes_and_attachments_are_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
    for observation in [
        ResourceObservation::Foreign,
        ResourceObservation::Owned {
            binding: binding("replacement-id"),
            foreign_attachments: Vec::new(),
        },
        ResourceObservation::Owned {
            binding: binding("network-id"),
            foreign_attachments: vec!["foreign-container".to_owned()],
        },
    ] {
        let mut journal = OwnershipJournal::new(sample_run(store.root()));
        journal.resources.push(record(
            ResourceKind::Network,
            "network",
            ResourceState::Bound,
            Some(binding("network-id")),
        ));
        store.save(&journal).unwrap();
        let backend = FakeLifecycle::default();
        backend.set("network", [observation]);
        let report = cleanup(&store, &mut journal, &backend);
        assert!(!report.succeeded());
        assert!(backend.removals.borrow().is_empty());
        assert_eq!(journal.resources[0].state, ResourceState::Blocked);
    }
}

#[test]
fn partial_deletion_and_cleanup_failures_remain_durable_and_combined() {
    let directory = tempfile::tempdir().unwrap();
    let store = JournalStore::initialize(&directory.path().join("fixture")).unwrap();
    let mut journal = store.create(sample_run(store.root())).unwrap();
    journal.resources.push(record(
        ResourceKind::Container,
        "sql-1",
        ResourceState::Bound,
        Some(binding("container-id")),
    ));
    store.save(&journal).unwrap();
    let backend = FakeLifecycle::default();
    backend.set(
        "sql-1",
        [ResourceObservation::Owned {
            binding: binding("container-id"),
            foreign_attachments: Vec::new(),
        }],
    );
    backend.fail_removal.borrow_mut().insert("sql-1".to_owned());
    let report = cleanup(&store, &mut journal, &backend);
    assert!(!report.succeeded());
    assert_eq!(store.load().unwrap().unwrap().state, RunState::Blocked);
    let combined = combine_with_cleanup::<()>(
        Err(SanitizedFailure::new(
            FailureStage::Setup,
            FailureCategory::ContainerCreation,
        )),
        &report,
    )
    .unwrap_err();
    assert_eq!(
        combined.to_string(),
        "setup: container creation failed; cleanup: container removal failed"
    );
}
