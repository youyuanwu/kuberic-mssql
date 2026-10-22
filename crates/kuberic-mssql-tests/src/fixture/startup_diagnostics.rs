use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, Instant};

use rustix::fs::{Mode, OFlags, open, openat};
use serde::Serialize;

use super::docker::{ContainerRequest, DockerCli, SQL_SERVER_UID, StartupState};
use super::model::SanitizedFailure;
use super::ownership::DirectoryBinding;
use super::process::{ProcessRunner, SHUTDOWN_RESERVE};
use super::secrets::{PrivateFile, SecretValue};

const DIAGNOSTIC_BUDGET: Duration = Duration::from_secs(15);
const COMMAND_BUDGET: Duration = Duration::from_secs(5);
const MAX_TAIL_BYTES: usize = 8192;
const MAX_TAIL_LINES: usize = 64;
const READ_BYTES: u64 = 16 * 1024;

#[derive(Serialize)]
struct MemberDiagnostic {
    member: Option<u8>,
    state: Result<StartupState, String>,
    error_log: Result<Vec<String>, &'static str>,
}

pub(crate) fn with_startup_diagnostics<'a>(
    failure: SanitizedFailure,
    docker: &DockerCli<impl ProcessRunner>,
    image_id: &str,
    root: &Path,
    private_files: &[PrivateFile],
    containers: impl IntoIterator<
        Item = (
            &'a ContainerRequest,
            Option<&'a DirectoryBinding>,
            Option<&'a str>,
        ),
    >,
) -> SanitizedFailure {
    let reports = collect_reports(
        docker,
        image_id,
        root,
        private_files,
        containers,
        Instant::now() + DIAGNOSTIC_BUDGET,
    );
    if reports.is_empty() {
        failure
    } else {
        failure.with_context(format!(
            "SQL_STARTUP {}",
            serde_json::to_string(&reports).expect("startup diagnostics are serializable")
        ))
    }
}

fn collect_reports<'a>(
    docker: &DockerCli<impl ProcessRunner>,
    image_id: &str,
    root: &Path,
    private_files: &[PrivateFile],
    containers: impl IntoIterator<
        Item = (
            &'a ContainerRequest,
            Option<&'a DirectoryBinding>,
            Option<&'a str>,
        ),
    >,
    deadline: Instant,
) -> Vec<MemberDiagnostic> {
    let credential_directory = root.join("credentials");
    let credential_files: Vec<_> = private_files
        .iter()
        .filter(|file| {
            file.path().parent() == Some(credential_directory.as_path())
                && file
                    .path()
                    .file_name()
                    .is_some_and(|name| name != "container.env" && name != "sa-username")
        })
        .collect();
    let credentials = if credential_files.iter().any(|file| {
        file.path()
            .file_name()
            .is_some_and(|name| name == "sa-password")
    }) {
        credential_files
            .iter()
            .map(|file| file.read_secret())
            .collect::<Result<Vec<_>, _>>()
    } else {
        Err(super::secrets::SecretError::InvalidValue)
    };
    let mut reports = Vec::new();
    for (request, directory, id) in containers.into_iter().take(3) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let state = if remaining <= SHUTDOWN_RESERVE {
            Err("diagnostic deadline exceeded".to_owned())
        } else if let Some(id) = id {
            docker
                .startup_state(request, id, image_id, remaining.min(COMMAND_BUDGET))
                .map_err(|error| error.to_string())
        } else {
            Err("bound container identity unavailable".to_owned())
        };
        let error_log = if state.is_err() {
            Err("container ownership/state unavailable; SQL text suppressed")
        } else if let Ok(credentials) = &credentials {
            match directory {
                Some(directory) if directory.canonical_path == request.data_directory => {
                    read_error_log(directory, credentials, deadline)
                }
                _ => Err("owned data-directory binding unavailable"),
            }
        } else {
            Err("credential verification failed; SQL text suppressed")
        };
        reports.push(MemberDiagnostic {
            member: request.labels.member_ordinal,
            state,
            error_log,
        });
    }
    reports
}

fn read_error_log(
    binding: &DirectoryBinding,
    credentials: &[SecretValue],
    deadline: Instant,
) -> Result<Vec<String>, &'static str> {
    if Instant::now() >= deadline {
        return Err("diagnostic deadline exceeded");
    }
    let directory_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let directory = File::from(
        open(&binding.canonical_path, directory_flags, Mode::empty())
            .map_err(|_| "owned data directory unavailable")?,
    );
    let metadata = directory
        .metadata()
        .map_err(|_| "owned data directory unavailable")?;
    if metadata.dev() != binding.device
        || metadata.ino() != binding.inode
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err("owned data directory replaced");
    }
    let log_directory = openat(&directory, "log", directory_flags, Mode::empty())
        .map_err(|_| "SQL log directory unavailable or unsafe")?;
    let mut log = File::from(
        openat(
            &log_directory,
            "errorlog",
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|_| "SQL error log unavailable or unsafe")?,
    );
    let metadata = log.metadata().map_err(|_| "SQL error log unavailable")?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || ![SQL_SERVER_UID, unsafe { libc::geteuid() }].contains(&metadata.uid())
    {
        return Err("SQL error log is not an owned regular single-link file");
    }
    let offset = metadata.len().saturating_sub(READ_BYTES);
    log.seek(SeekFrom::Start(offset))
        .map_err(|_| "SQL error log seek failed")?;
    if Instant::now() >= deadline {
        return Err("diagnostic deadline exceeded");
    }
    let mut bytes = Vec::new();
    log.take(metadata.len() - offset)
        .read_to_end(&mut bytes)
        .map_err(|_| "SQL error log read failed")?;
    if Instant::now() >= deadline {
        return Err("diagnostic deadline exceeded");
    }
    // Complete lines avoid exposing credentials split at either read boundary.
    let bytes = if offset > 0 {
        let boundary = bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or("SQL error log has no complete bounded lines")?;
        &bytes[boundary + 1..]
    } else {
        bytes.as_slice()
    };
    let end = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .ok_or("SQL error log has no complete bounded lines")?;
    sanitized_tail(&bytes[..=end], credentials)
}

fn sanitized_tail(bytes: &[u8], credentials: &[SecretValue]) -> Result<Vec<String>, &'static str> {
    let text = std::str::from_utf8(bytes).map_err(|_| "unsupported SQL error log encoding")?;
    if text.contains('\0') {
        return Err("unsupported SQL error log encoding");
    }
    let mut text = text.to_owned();
    for credential in credentials {
        if !credential.expose().is_empty() {
            text = text.replace(credential.expose(), "<redacted>");
        }
    }
    let mut tail = Vec::new();
    for line in text.lines().rev().take(MAX_TAIL_LINES) {
        let line: String = line
            .chars()
            .map(|character| {
                if character.is_control() {
                    '?'
                } else {
                    character
                }
            })
            .collect();
        tail.insert(0, line);
        if serde_json::to_vec(&tail)
            .expect("log lines are serializable")
            .len()
            > MAX_TAIL_BYTES
        {
            tail.remove(0);
            if tail.is_empty() {
                return Err("SQL error log line exceeds diagnostic limit");
            }
            break;
        }
    }
    Ok(tail)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::sync::Mutex;

    use serde_json::{Value, json};

    use super::*;
    use crate::fixture::config::ResourcePolicy;
    use crate::fixture::docker::{OwnedLabels, SqlServerContainerSpec};
    use crate::fixture::model::{FailureCategory, FailureStage, ResourceBinding};
    use crate::fixture::ownership::AclEvidence;
    use crate::fixture::process::{
        ChildDisposition, CommandSpec, ProcessError, ProcessErrorKind, ProcessResult,
    };

    struct Runner {
        response: Value,
        fail: bool,
        timeouts: Mutex<Vec<Duration>>,
    }

    impl ProcessRunner for Runner {
        fn run(&self, command: &CommandSpec) -> Result<ProcessResult, ProcessError> {
            assert_eq!(command.arguments().last().unwrap(), "owned-id");
            self.timeouts.lock().unwrap().push(command.timeout());
            if self.fail {
                return Err(ProcessError::new(
                    ProcessErrorKind::Timeout,
                    "inspect SQL startup state",
                    None,
                    "unsafe fixture-secret command output",
                    ChildDisposition::default(),
                ));
            }
            Ok(ProcessResult {
                status: 0,
                stdout: self.response.to_string(),
                stderr: String::new(),
                child: ChildDisposition::default(),
            })
        }
    }

    struct Fixture {
        root: tempfile::TempDir,
        request: ContainerRequest,
        binding: DirectoryBinding,
        credentials: Vec<PrivateFile>,
        response: Value,
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let data = root.path().join("member-1");
            fs::create_dir(&data).unwrap();
            fs::create_dir(data.join("log")).unwrap();
            let metadata = fs::metadata(&data).unwrap();
            let binding = DirectoryBinding {
                canonical_path: data.clone(),
                device: metadata.dev(),
                inode: metadata.ino(),
                marker_device: metadata.dev(),
                marker_inode: 0,
                marker_sha256: String::new(),
                binding: ResourceBinding {
                    immutable_id: "directory".to_owned(),
                    attributes_sha256: String::new(),
                },
                acl: AclEvidence {
                    host_access: true,
                    sql_access: true,
                    host_default: true,
                    sql_default: true,
                    access_mask: true,
                    default_mask: true,
                },
            };
            let directory = root.path().join("credentials");
            fs::create_dir(&directory).unwrap();
            let credentials = vec![
                PrivateFile::create_text(directory.join("sa-password"), "fixture-secret").unwrap(),
                PrivateFile::create_text(directory.join("admin-username"), "km_admin_test")
                    .unwrap(),
            ];
            let request = ContainerRequest::sql_server(
                SqlServerContainerSpec {
                    name: "owned-name".to_owned(),
                    hostname: "sql-one".to_owned(),
                    network_name: "owned-network".to_owned(),
                    data_directory: data,
                    environment_file: directory.join("container.env"),
                    sa_password: SecretValue::from_test("fixture-secret"),
                },
                OwnedLabels::container("run-startup", 1),
                ResourcePolicy::default(),
                [("ACCEPT_EULA", "Y")],
            )
            .unwrap();
            let response = json!([{
                "Id": "owned-id", "Name": "/owned-name", "Image": "image-id",
                "Config": {
                    "Hostname": request.hostname, "User": "mssql",
                    "Labels": request.labels.as_map(),
                    "Env": request.environment_file_contents().lines().collect::<Vec<_>>(),
                },
                "State": {
                    "Running": false, "Restarting": false, "Dead": false,
                    "ExitCode": 137, "OOMKilled": true, "StartedAt": "test-start",
                    "Error": "must not print fixture-secret",
                },
                "Mounts": [{
                    "Source": request.data_directory,
                    "Destination": "/var/opt/mssql", "RW": true,
                }],
                "HostConfig": {
                    "RestartPolicy": {"Name": "no"}, "NetworkMode": "owned-network",
                    "Memory": request.limits.memory_bytes,
                    "MemorySwap": request.limits.memory_swap_bytes,
                    "NanoCpus": request.limits.nano_cpus,
                    "PortBindings": {"1433/tcp": [{"HostIp": "127.0.0.1", "HostPort": ""}]},
                },
                "NetworkSettings": {"Networks": {"owned-network": {}}, "Ports": {"1433/tcp": null}},
            }]);
            Self {
                root,
                request,
                binding,
                credentials,
                response,
            }
        }

        fn log(&self, text: &str) {
            fs::write(self.request.data_directory.join("log/errorlog"), text).unwrap();
        }

        fn docker(&self) -> DockerCli<Runner> {
            DockerCli::new(Runner {
                response: self.response.clone(),
                fail: false,
                timeouts: Mutex::new(Vec::new()),
            })
        }

        fn failure(&self, docker: &DockerCli<Runner>) -> SanitizedFailure {
            with_startup_diagnostics(
                SanitizedFailure::with_detail(
                    FailureStage::Setup,
                    FailureCategory::DeadlineExceeded,
                    "member initialization; last readiness error: TCP connection failed",
                ),
                docker,
                "image-id",
                self.root.path(),
                &self.credentials,
                [(&self.request, Some(&self.binding), Some("owned-id"))],
            )
        }
    }

    #[test]
    fn exited_oom_state_and_sanitized_tail_survive_source_cleanup() {
        let fixture = Fixture::new();
        fixture.log("SQL startup failed for km_admin_test: fixture-secret\n\x1b[31mreason\n");
        let docker = fixture.docker();
        let failure = fixture.failure(&docker);
        fs::remove_file(fixture.request.data_directory.join("log/errorlog")).unwrap();
        let rendered = failure.to_string();
        assert!(rendered.starts_with("setup: stage deadline exceeded"));
        assert!(rendered.contains("last readiness error: TCP connection failed"));
        assert!(rendered.contains("\"ExitCode\":137"));
        assert!(rendered.contains("\"OOMKilled\":true"));
        assert!(rendered.contains("SQL startup failed for <redacted>: <redacted>"));
        assert!(!rendered.contains("fixture-secret"));
        assert!(!rendered.contains("km_admin_test"));
        assert!(!rendered.contains("must not print"));
        assert!(!rendered.contains(['\x1b', '\n', '\r']));
        assert_eq!(*docker.runner().timeouts.lock().unwrap(), [COMMAND_BUDGET]);
    }

    #[test]
    fn foreign_identity_labels_and_malformed_state_never_publish_sql_text() {
        for (field, value) in [
            ("/0/Id", json!("replacement-id")),
            (
                "/0/Config/Labels/io.kuberic.mssql.run",
                json!("another-run"),
            ),
            ("/0/State/OOMKilled", json!("unknown")),
        ] {
            let mut fixture = Fixture::new();
            fixture.log("raw SQL text must be suppressed\n");
            *fixture.response.pointer_mut(field).unwrap() = value;
            let rendered = fixture.failure(&fixture.docker()).to_string();
            assert!(rendered.contains("container ownership/state unavailable"));
            assert!(!rendered.contains("raw SQL text"));
            assert!(rendered.contains("last readiness error"));
        }
    }

    #[test]
    fn missing_replaced_credentials_or_failed_commands_do_not_replace_primary_error() {
        for mode in [
            "missing-log",
            "replaced-credential",
            "no-credentials",
            "timeout",
        ] {
            let mut fixture = Fixture::new();
            if mode != "missing-log" {
                fixture.log("fixture-secret\n");
            }
            if mode == "replaced-credential" {
                fs::write(fixture.credentials[0].path(), "different-secret").unwrap();
            } else if mode == "no-credentials" {
                fixture.credentials.clear();
            }
            let docker = DockerCli::new(Runner {
                response: fixture.response.clone(),
                fail: mode == "timeout",
                timeouts: Mutex::new(Vec::new()),
            });
            let rendered = fixture.failure(&docker).to_string();
            assert!(rendered.contains("last readiness error"));
            assert!(rendered.contains("\"Err\":"));
            assert!(!rendered.contains("fixture-secret"));
            assert!(!rendered.contains("different-secret"));
            assert!(!rendered.contains("unsafe fixture-secret command output"));
        }
    }

    #[test]
    fn symlink_hardlink_fifo_and_replaced_directory_are_refused() {
        for mode in [
            "log-symlink",
            "directory-symlink",
            "hardlink",
            "fifo",
            "replaced",
        ] {
            let mut fixture = Fixture::new();
            let log_directory = fixture.request.data_directory.join("log");
            let log = log_directory.join("errorlog");
            let foreign = fixture.root.path().join("foreign");
            fs::write(&foreign, "unowned SQL text\n").unwrap();
            match mode {
                "log-symlink" => symlink(&foreign, &log).unwrap(),
                "directory-symlink" => {
                    fs::remove_dir(&log_directory).unwrap();
                    symlink(fixture.root.path(), &log_directory).unwrap();
                }
                "hardlink" => fs::hard_link(&foreign, &log).unwrap(),
                "fifo" => rustix::fs::mknodat(
                    rustix::fs::CWD,
                    &log,
                    rustix::fs::FileType::Fifo,
                    Mode::RUSR | Mode::WUSR,
                    0,
                )
                .unwrap(),
                "replaced" => fixture.binding.inode += 1,
                _ => unreachable!(),
            }
            let rendered = fixture.failure(&fixture.docker()).to_string();
            assert!(rendered.contains("\"error_log\":{\"Err\":"));
            assert!(!rendered.contains("unowned SQL text"));
        }
    }

    #[test]
    fn tail_bounds_drop_partial_credentials_and_keep_latest_complete_lines() {
        let fixture = Fixture::new();
        let prefix = format!("{}fixture-secret\n", "x".repeat(READ_BYTES as usize));
        fixture.log(&format!(
            "{prefix}{}partial fixture-se",
            (0..100)
                .map(|index| format!("line {index} \"quoted\"\n"))
                .collect::<String>(),
        ));
        let lines = read_error_log(
            &fixture.binding,
            &[SecretValue::from_test("fixture-secret")],
            Instant::now() + DIAGNOSTIC_BUDGET,
        )
        .unwrap();
        assert_eq!(lines.len(), MAX_TAIL_LINES);
        assert_eq!(lines.last().unwrap(), "line 99 \"quoted\"");
        assert!(!lines.iter().any(|line| line.contains("fixture-se")));
        assert!(serde_json::to_vec(&lines).unwrap().len() <= MAX_TAIL_BYTES);
        let wide = format!("{}\n", "\"\u{e9}".repeat(100));
        fixture.log(&wide.repeat(80));
        let lines =
            read_error_log(&fixture.binding, &[], Instant::now() + DIAGNOSTIC_BUDGET).unwrap();
        assert!(lines.len() < MAX_TAIL_LINES);
        assert!(serde_json::to_vec(&lines).unwrap().len() <= MAX_TAIL_BYTES);
    }

    #[test]
    fn unsupported_encoding_is_not_lossily_rendered_around_credentials() {
        for bytes in [
            b"fixture-secret\xff\n".to_vec(),
            "fixture-secret\n"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect(),
        ] {
            assert_eq!(
                sanitized_tail(&bytes, &[SecretValue::from_test("fixture-secret")]),
                Err("unsupported SQL error log encoding"),
            );
        }
    }

    #[test]
    fn exhausted_shared_budget_skips_all_commands_and_logs() {
        let fixture = Fixture::new();
        fixture.log("must not read\n");
        let docker = fixture.docker();
        let reports = collect_reports(
            &docker,
            "image-id",
            fixture.root.path(),
            &fixture.credentials,
            [(&fixture.request, Some(&fixture.binding), Some("owned-id")); 3],
            Instant::now(),
        );
        assert_eq!(reports.len(), 3);
        assert!(docker.runner().timeouts.lock().unwrap().is_empty());
        assert!(reports.iter().all(|report| {
            report.state.as_ref().map_err(String::as_str) == Err("diagnostic deadline exceeded")
                && report.error_log.is_err()
        }));
    }
}
