use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const MAX_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const WAIT_INTERVAL: Duration = Duration::from_millis(10);

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
        let mut child = Command::new(command.program())
            .args(command.arguments())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| {
                ProcessError::new(
                    ProcessErrorKind::Spawn,
                    command.diagnostic_name(),
                    None,
                    "process could not be started",
                    ChildDisposition::default(),
                )
            })?;
        let pid = child.id();
        let stdout = child.stdout.take().expect("piped stdout must exist");
        let stderr = child.stderr.take().expect("piped stderr must exist");
        let stdout_reader = thread::spawn(move || read_bounded(stdout));
        let stderr_reader = thread::spawn(move || read_bounded(stderr));
        let deadline = Instant::now()
            .checked_add(command.timeout())
            .unwrap_or_else(Instant::now);

        let (status, terminated) = loop {
            match child.try_wait() {
                Ok(Some(status)) => break (status, false),
                Ok(None) if Instant::now() < deadline => thread::sleep(WAIT_INTERVAL),
                Ok(None) => {
                    let terminated = child.kill().is_ok();
                    let status = child.wait().map_err(|_| {
                        ProcessError::new(
                            ProcessErrorKind::Reap,
                            command.diagnostic_name(),
                            None,
                            "timed-out child could not be reaped",
                            ChildDisposition {
                                pid: Some(pid),
                                terminated,
                                reaped: false,
                            },
                        )
                    })?;
                    let stdout = join_reader(stdout_reader);
                    let stderr = join_reader(stderr_reader);
                    let diagnostic = command.sanitize_diagnostic(if stderr.is_empty() {
                        &stdout
                    } else {
                        &stderr
                    });
                    return Err(ProcessError::new(
                        ProcessErrorKind::Timeout,
                        command.diagnostic_name(),
                        status.code(),
                        if diagnostic.is_empty() {
                            "deadline exceeded".to_owned()
                        } else {
                            format!("deadline exceeded: {diagnostic}")
                        },
                        ChildDisposition {
                            pid: Some(pid),
                            terminated,
                            reaped: true,
                        },
                    ));
                }
                Err(_) => {
                    let terminated = child.kill().is_ok();
                    let reaped = child.wait().is_ok();
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

        let stdout = command.sanitize_diagnostic(&join_reader(stdout_reader));
        let stderr = command.sanitize_diagnostic(&join_reader(stderr_reader));
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

fn read_bounded(mut reader: impl Read) -> Vec<u8> {
    let mut retained = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                let remaining = MAX_DIAGNOSTIC_BYTES.saturating_sub(retained.len());
                retained.extend_from_slice(&buffer[..count.min(remaining)]);
            }
        }
    }
    retained
}

fn join_reader(reader: thread::JoinHandle<Vec<u8>>) -> Vec<u8> {
    reader.join().unwrap_or_default()
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
