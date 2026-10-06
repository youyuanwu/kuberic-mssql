use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const MAX_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const WAIT_INTERVAL: Duration = Duration::from_millis(10);
const TERMINATION_GRACE: Duration = Duration::from_millis(250);
const PIPE_DRAIN_GRACE: Duration = Duration::from_millis(250);
const SHUTDOWN_RESERVE: Duration = Duration::from_millis(1500);

#[derive(Clone, PartialEq, Eq)]
pub struct CommandSpec {
    program: PathBuf,
    arguments: Vec<OsString>,
    diagnostic_name: String,
    timeout: Duration,
    secrets: Vec<Vec<u8>>,
}

impl fmt::Debug for CommandSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandSpec")
            .field("program", &self.program)
            .field("argument_count", &self.arguments.len())
            .field("diagnostic_name", &self.diagnostic_name)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl CommandSpec {
    pub fn new(
        program: impl Into<PathBuf>,
        diagnostic_name: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            program: program.into(),
            arguments: Vec::new(),
            diagnostic_name: diagnostic_name.into(),
            timeout,
            secrets: Vec::new(),
        }
    }

    pub fn arg(mut self, argument: impl AsRef<OsStr>) -> Self {
        self.arguments.push(argument.as_ref().to_os_string());
        self
    }

    pub fn args<I, S>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.arguments.extend(
            arguments
                .into_iter()
                .map(|argument| argument.as_ref().to_os_string()),
        );
        self
    }

    pub fn sensitive_arg(mut self, argument: impl AsRef<OsStr>) -> Self {
        let argument = argument.as_ref();
        self.secrets.push(argument.as_encoded_bytes().to_vec());
        self.arguments.push(argument.to_os_string());
        self
    }

    pub fn program(&self) -> &Path {
        &self.program
    }

    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    pub fn diagnostic_name(&self) -> &str {
        &self.diagnostic_name
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn sanitize_diagnostic(&self, bytes: &[u8]) -> String {
        sanitize(bytes, &self.secrets)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChildDisposition {
    pub pid: Option<u32>,
    pub terminated: bool,
    pub reaped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessResult {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
    pub child: ChildDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessErrorKind {
    Spawn,
    Timeout,
    Exit,
    Reap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessError {
    kind: ProcessErrorKind,
    command: String,
    status: Option<i32>,
    diagnostic: String,
    child: ChildDisposition,
}

impl ProcessError {
    pub fn new(
        kind: ProcessErrorKind,
        command: impl Into<String>,
        status: Option<i32>,
        diagnostic: impl Into<String>,
        child: ChildDisposition,
    ) -> Self {
        Self {
            kind,
            command: command.into(),
            status,
            diagnostic: diagnostic.into(),
            child,
        }
    }

    pub fn kind(&self) -> ProcessErrorKind {
        self.kind
    }

    pub fn status(&self) -> Option<i32> {
        self.status
    }

    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }

    pub fn child(&self) -> ChildDisposition {
        self.child
    }
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.command, self.diagnostic)
    }
}

impl Error for ProcessError {}

pub trait ProcessRunner: Send + Sync {
    fn run(&self, command: &CommandSpec) -> Result<ProcessResult, ProcessError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BoundedProcessRunner;

impl ProcessRunner for BoundedProcessRunner {
    fn run(&self, command: &CommandSpec) -> Result<ProcessResult, ProcessError> {
        let started_at = Instant::now();
        let overall_deadline = started_at
            .checked_add(command.timeout())
            .unwrap_or(started_at);
        let execution_deadline = overall_deadline
            .checked_sub(SHUTDOWN_RESERVE)
            .unwrap_or(started_at);
        let mut process = Command::new(command.program());
        process
            .args(command.arguments())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            process.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            });
        }
        let mut child = process.spawn().map_err(|_| {
            ProcessError::new(
                ProcessErrorKind::Spawn,
                command.diagnostic_name(),
                None,
                "process could not be started",
                ChildDisposition::default(),
            )
        })?;
        let pid = child.id();
        let mut stdout = child.stdout.take().expect("piped stdout must exist");
        let mut stderr = child.stderr.take().expect("piped stderr must exist");
        if set_nonblocking(stdout.as_raw_fd()).is_err()
            || set_nonblocking(stderr.as_raw_fd()).is_err()
        {
            let terminated = terminate_process_group(pid, overall_deadline);
            let reaped = reap_until(&mut child, overall_deadline).is_some();
            return Err(ProcessError::new(
                ProcessErrorKind::Spawn,
                command.diagnostic_name(),
                None,
                "process output could not be bounded",
                ChildDisposition {
                    pid: Some(pid),
                    terminated,
                    reaped,
                },
            ));
        }
        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        let mut stdout_open = true;
        let mut stderr_open = true;
        let status = loop {
            drain_pipe(&mut stdout, &mut stdout_bytes, &mut stdout_open);
            drain_pipe(&mut stderr, &mut stderr_bytes, &mut stderr_open);
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < execution_deadline => thread::sleep(WAIT_INTERVAL),
                Ok(None) => {
                    let terminated = terminate_process_group(pid, overall_deadline);
                    let status = reap_until(&mut child, overall_deadline);
                    drain_until(
                        &mut stdout,
                        &mut stderr,
                        &mut stdout_bytes,
                        &mut stderr_bytes,
                        &mut stdout_open,
                        &mut stderr_open,
                        overall_deadline,
                    );
                    let disposition = ChildDisposition {
                        pid: Some(pid),
                        terminated,
                        reaped: status.is_some(),
                    };
                    if status.is_none() {
                        return Err(ProcessError::new(
                            ProcessErrorKind::Reap,
                            command.diagnostic_name(),
                            None,
                            "timed-out process group could not be reaped within its deadline",
                            disposition,
                        ));
                    }
                    let diagnostic = command.sanitize_diagnostic(if stderr_bytes.is_empty() {
                        &stdout_bytes
                    } else {
                        &stderr_bytes
                    });
                    return Err(ProcessError::new(
                        ProcessErrorKind::Timeout,
                        command.diagnostic_name(),
                        status.and_then(|status| status.code()),
                        if diagnostic.is_empty() {
                            "deadline exceeded".to_owned()
                        } else {
                            format!("deadline exceeded: {diagnostic}")
                        },
                        disposition,
                    ));
                }
                Err(_) => {
                    let terminated = terminate_process_group(pid, overall_deadline);
                    let reaped = reap_until(&mut child, overall_deadline).is_some();
                    return Err(ProcessError::new(
                        ProcessErrorKind::Reap,
                        command.diagnostic_name(),
                        None,
                        "child status could not be observed",
                        ChildDisposition {
                            pid: Some(pid),
                            terminated,
                            reaped,
                        },
                    ));
                }
            }
        };

        // A successfully exited group leader may leave descendants holding the
        // inherited pipe descriptors. Terminate only this command's process
        // group, then drain for a bounded interval before dropping our handles.
        let terminated = terminate_process_group(pid, overall_deadline);
        drain_until(
            &mut stdout,
            &mut stderr,
            &mut stdout_bytes,
            &mut stderr_bytes,
            &mut stdout_open,
            &mut stderr_open,
            overall_deadline.min(Instant::now() + PIPE_DRAIN_GRACE),
        );

        let stdout = command.sanitize_diagnostic(&stdout_bytes);
        let stderr = command.sanitize_diagnostic(&stderr_bytes);
        let disposition = ChildDisposition {
            pid: Some(pid),
            terminated,
            reaped: true,
        };
        let code = status.code().unwrap_or(-1);
        if !status.success() {
            return Err(ProcessError::new(
                ProcessErrorKind::Exit,
                command.diagnostic_name(),
                Some(code),
                if stderr.is_empty() {
                    "process exited unsuccessfully".to_owned()
                } else {
                    stderr
                },
                disposition,
            ));
        }
        Ok(ProcessResult {
            status: code,
            stdout,
            stderr,
            child: disposition,
        })
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn drain_pipe(reader: &mut impl Read, retained: &mut Vec<u8>, open: &mut bool) {
    if !*open {
        return;
    }
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => {
                *open = false;
                break;
            }
            Ok(count) => {
                let remaining = MAX_DIAGNOSTIC_BYTES.saturating_sub(retained.len());
                retained.extend_from_slice(&buffer[..count.min(remaining)]);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(_) => {
                *open = false;
                break;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_until(
    stdout: &mut impl Read,
    stderr: &mut impl Read,
    stdout_bytes: &mut Vec<u8>,
    stderr_bytes: &mut Vec<u8>,
    stdout_open: &mut bool,
    stderr_open: &mut bool,
    deadline: Instant,
) {
    while (*stdout_open || *stderr_open) && Instant::now() < deadline {
        drain_pipe(stdout, stdout_bytes, stdout_open);
        drain_pipe(stderr, stderr_bytes, stderr_open);
        if *stdout_open || *stderr_open {
            thread::sleep(WAIT_INTERVAL);
        }
    }
}

fn terminate_process_group(pid: u32, deadline: Instant) -> bool {
    let Ok(group) = i32::try_from(pid) else {
        return false;
    };
    let mut signalled = signal_process_group(group, libc::SIGTERM);
    let graceful_deadline = deadline.min(Instant::now() + TERMINATION_GRACE);
    while process_group_exists(group) && Instant::now() < graceful_deadline {
        thread::sleep(WAIT_INTERVAL);
    }
    if process_group_exists(group) {
        signalled |= signal_process_group(group, libc::SIGKILL);
    }
    signalled
}

fn signal_process_group(group: i32, signal: i32) -> bool {
    (unsafe { libc::kill(-group, signal) }) == 0
}

fn process_group_exists(group: i32) -> bool {
    if unsafe { libc::kill(-group, 0) } == 0 {
        true
    } else {
        io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}

fn reap_until(
    child: &mut std::process::Child,
    deadline: Instant,
) -> Option<std::process::ExitStatus> {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(WAIT_INTERVAL),
            Ok(None) | Err(_) => return None,
        }
    }
}

fn sanitize(bytes: &[u8], secrets: &[Vec<u8>]) -> String {
    let mut text = String::from_utf8_lossy(bytes).into_owned();
    for secret in secrets {
        if !secret.is_empty() {
            text = text.replace(String::from_utf8_lossy(secret).as_ref(), "<redacted>");
        }
    }
    text.trim().to_owned()
}
