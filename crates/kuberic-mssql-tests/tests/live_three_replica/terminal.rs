use std::any::Any;
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::{HandledCancellationSignal, OwnershipJournal, ResourceState, RunState, TestError};

pub(super) const CONTEXT_ENV: &str = "KUBERIC_MSSQL_TERMINAL_CONTEXT";
pub(super) const EXPECTED_EXIT: i32 = 86;
const MAX_BYTES: u64 = 4096;
static CHILD: Mutex<Option<ChildControl>> = Mutex::new(None);
pub(super) type PanicPayload = Box<dyn Any + Send + 'static>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Checkpoint {
    FailAfterAg,
    PanicAfterAgentStart,
    FailDuringReport,
    FailAfterAgentRestart,
}

impl Checkpoint {
    pub(super) fn from_label(label: &str) -> Option<Self> {
        match label {
            "fail-after-ag" => Some(Self::FailAfterAg),
            "panic-after-agent-start" => Some(Self::PanicAfterAgentStart),
            "fail-during-report" => Some(Self::FailDuringReport),
            "fail-after-agent-restart" => Some(Self::FailAfterAgentRestart),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Signal {
    Interrupt,
    Terminate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Cause {
    Signal { signal: Signal },
    Checkpoint { checkpoint: Checkpoint },
}

impl Cause {
    pub(super) fn signal(signal: HandledCancellationSignal) -> Self {
        Self::Signal {
            signal: match signal {
                HandledCancellationSignal::Interrupt => Signal::Interrupt,
                HandledCancellationSignal::Terminate => Signal::Terminate,
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    path: PathBuf,
    attempt: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u8,
    attempt: String,
    process: u32,
    run: String,
    cause: Cause,
    teardown_completed: bool,
}

struct ChildControl {
    control: Control,
    ready: Option<Receipt>,
}

pub(super) struct Attempt {
    _directory: PathBuf,
    control: Control,
}

impl Attempt {
    pub(super) fn new(parent: &Path) -> io::Result<Self> {
        fs::create_dir_all(parent)?;
        let directory = tempfile::Builder::new()
            .prefix(".terminal-attempt-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(parent.canonicalize()?)?;
        let control = Control {
            path: directory.path().join("terminal.json"),
            attempt: directory
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| io::Error::other("invalid terminal attempt identity"))?
                .to_owned(),
        };
        Ok(Self {
            _directory: directory.keep(),
            control,
        })
    }

    pub(super) fn configure(&self, command: &mut Command) -> io::Result<()> {
        command.env(CONTEXT_ENV, serde_json::to_string(&self.control)?);
        Ok(())
    }

    pub(super) fn verify(
        &self,
        outcome: ChildExit,
        process: u32,
        run: &str,
        cause: Cause,
    ) -> Result<(), &'static str> {
        let status = outcome.observed()?;
        if status.code() != Some(EXPECTED_EXIT) {
            return Err("expected an observed orderly negative child exit");
        }
        let receipt = read_receipt(&self.control.path)
            .map_err(|_| "terminal child evidence missing or invalid")?;
        validate(&receipt, &self.control.attempt, process, run, cause)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ChildExit {
    Observed(ExitStatus),
    Escalated(ExitStatus),
}

impl ChildExit {
    pub(super) fn observed(self) -> Result<ExitStatus, &'static str> {
        match self {
            Self::Observed(status) => Ok(status),
            Self::Escalated(status) => {
                if status.success() {
                    Err("child exited only after its timeout")
                } else {
                    Err("child required timeout escalation")
                }
            }
        }
    }

    pub(super) fn killed(self) -> Result<(), &'static str> {
        if self.observed()?.signal() == Some(libc::SIGKILL) {
            Ok(())
        } else {
            Err("child did not die from the requested SIGKILL")
        }
    }
}

fn validate(
    receipt: &Receipt,
    attempt: &str,
    process: u32,
    run: &str,
    cause: Cause,
) -> Result<(), &'static str> {
    if receipt.version != 1 || !receipt.teardown_completed {
        return Err("child did not certify its applicable terminal teardown");
    }
    if receipt.attempt != attempt || receipt.process != process || receipt.run != run {
        return Err("terminal child evidence belongs to another attempt");
    }
    if receipt.cause != cause {
        return Err("child did not observe the expected terminal cause");
    }
    Ok(())
}

pub(super) fn install() -> io::Result<()> {
    let Some(value) = std::env::var_os(CONTEXT_ENV) else {
        return Ok(());
    };
    let value = value
        .to_str()
        .ok_or_else(|| io::Error::other("invalid terminal context"))?;
    if value.len() > MAX_BYTES as usize {
        return Err(io::Error::other("oversized terminal context"));
    }
    let control: Control =
        serde_json::from_str(value).map_err(|_| io::Error::other("invalid terminal context"))?;
    private_parent(&control.path)?;
    if control.attempt.is_empty() || control.attempt.len() > 128 || control.path.exists() {
        return Err(io::Error::other("terminal context is not fresh"));
    }
    let mut child = CHILD
        .lock()
        .map_err(|_| io::Error::other("terminal context poisoned"))?;
    if child.is_some() {
        return Err(io::Error::other("terminal context already installed"));
    }
    *child = Some(ChildControl {
        control,
        ready: None,
    });
    Ok(())
}

pub(super) fn mark_ready(root: &Path, cause: Cause) -> io::Result<()> {
    let mut child = CHILD
        .lock()
        .map_err(|_| io::Error::other("terminal context poisoned"))?;
    let Some(child) = child.as_mut() else {
        return Ok(());
    };
    let journal = OwnershipJournal::from_json(&fs::read(root.join("ownership.json"))?)
        .map_err(|_| io::Error::other("invalid terminal ownership journal"))?;
    if journal.state != RunState::Removed
        || journal
            .resources
            .iter()
            .any(|record| record.state != ResourceState::Removed)
        || child.ready.is_some()
    {
        return Err(io::Error::other(
            "terminal teardown is incomplete or repeated",
        ));
    }
    child.ready = Some(Receipt {
        version: 1,
        attempt: child.control.attempt.clone(),
        process: std::process::id(),
        run: journal.run.run_id,
        cause,
        teardown_completed: true,
    });
    Ok(())
}

pub(super) fn is_ready() -> bool {
    CHILD
        .lock()
        .expect("terminal child context")
        .as_ref()
        .is_some_and(|child| child.ready.is_some())
}

pub(super) fn publish_ready() -> io::Result<()> {
    let child = CHILD
        .lock()
        .map_err(|_| io::Error::other("terminal context poisoned"))?;
    let child = child
        .as_ref()
        .ok_or_else(|| io::Error::other("terminal context missing"))?;
    let receipt = child
        .ready
        .as_ref()
        .ok_or_else(|| io::Error::other("terminal outcome missing"))?;
    write_receipt(&child.control.path, receipt)
}

fn private_parent(path: &Path) -> io::Result<&Path> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("terminal directory missing"))?;
    let metadata = fs::symlink_metadata(parent)?;
    if !path.is_absolute()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || parent.canonicalize()? != parent
    {
        return Err(io::Error::other("unsafe terminal directory"));
    }
    Ok(parent)
}

fn write_receipt(path: &Path, receipt: &Receipt) -> io::Result<()> {
    let mut file = tempfile::Builder::new()
        .prefix(".terminal-tmp-")
        .tempfile_in(private_parent(path)?)?;
    serde_json::to_writer(file.as_file_mut(), receipt)?;
    file.as_file_mut().flush()?;
    file.as_file().sync_data()?;
    file.persist_noclobber(path).map_err(|error| error.error)?;
    Ok(())
}

fn read_receipt(path: &Path) -> io::Result<Receipt> {
    private_parent(path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > MAX_BYTES
    {
        return Err(io::Error::other("unsafe terminal record"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(io::Error::other("oversized terminal record"));
    }
    serde_json::from_slice(&bytes).map_err(|_| io::Error::other("malformed terminal record"))
}

#[derive(Debug)]
struct ControlledFailure {
    checkpoint: Checkpoint,
    message: &'static str,
}

impl fmt::Display for ControlledFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}
impl Error for ControlledFailure {}

pub(super) fn injected(checkpoint: Checkpoint, message: &'static str) -> TestError {
    Box::new(ControlledFailure {
        checkpoint,
        message,
    })
}

pub(super) fn error_cause(error: &TestError) -> Option<Cause> {
    error
        .downcast_ref::<ControlledFailure>()
        .map(|error| Cause::Checkpoint {
            checkpoint: error.checkpoint,
        })
}

struct ControlledPanic {
    payload: PanicPayload,
}

pub(super) fn tag_panic(
    result: Result<Result<(), TestError>, PanicPayload>,
    injected: bool,
) -> Result<Result<(), TestError>, PanicPayload> {
    match result {
        Err(payload) if injected => Err(Box::new(ControlledPanic { payload })),
        other => other,
    }
}

pub(super) fn unpack_panic(payload: PanicPayload) -> (PanicPayload, Option<Cause>) {
    match payload.downcast::<ControlledPanic>() {
        Ok(panic) => (
            panic.payload,
            Some(Cause::Checkpoint {
                checkpoint: Checkpoint::PanicAfterAgentStart,
            }),
        ),
        Err(payload) => (payload, None),
    }
}

pub(super) fn original_panic(payload: &PanicPayload) -> &PanicPayload {
    payload
        .downcast_ref::<ControlledPanic>()
        .map_or(payload, |panic| &panic.payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Receipt {
        Receipt {
            version: 1,
            attempt: "attempt".into(),
            process: 42,
            run: "run".into(),
            cause: Cause::Signal {
                signal: Signal::Interrupt,
            },
            teardown_completed: true,
        }
    }

    #[test]
    fn terminal_evidence_rejects_unrelated_or_incomplete_outcomes() {
        let report = sample();
        assert!(validate(&report, "attempt", 42, "run", report.cause).is_ok());
        for (attempt, process, run, cause) in [
            ("stale", 42, "run", report.cause),
            ("attempt", 43, "run", report.cause),
            ("attempt", 42, "other-run", report.cause),
            (
                "attempt",
                42,
                "run",
                Cause::Signal {
                    signal: Signal::Terminate,
                },
            ),
        ] {
            assert!(validate(&report, attempt, process, run, cause).is_err());
        }
        let mut incomplete = report.clone();
        incomplete.teardown_completed = false;
        assert!(validate(&incomplete, "attempt", 42, "run", report.cause).is_err());
        incomplete.teardown_completed = true;
        incomplete.version = 2;
        assert!(validate(&incomplete, "attempt", 42, "run", report.cause).is_err());
    }

    #[test]
    fn escalation_and_unrelated_exit_cannot_satisfy_negative_expectation() {
        let directory = tempfile::tempdir().unwrap();
        let attempt = Attempt::new(directory.path()).unwrap();
        let report = Receipt {
            attempt: attempt.control.attempt.clone(),
            ..sample()
        };
        write_receipt(&attempt.control.path, &report).unwrap();
        for outcome in [
            ChildExit::Observed(ExitStatus::from_raw(0)),
            ChildExit::Observed(ExitStatus::from_raw(101 << 8)),
            ChildExit::Escalated(ExitStatus::from_raw(libc::SIGKILL)),
            ChildExit::Observed(ExitStatus::from_raw(libc::SIGTERM)),
        ] {
            assert!(attempt.verify(outcome, 42, "run", report.cause).is_err());
        }
        assert!(
            attempt
                .verify(
                    ChildExit::Observed(ExitStatus::from_raw(EXPECTED_EXIT << 8)),
                    42,
                    "run",
                    report.cause
                )
                .is_ok()
        );
        assert!(
            ChildExit::Observed(ExitStatus::from_raw(libc::SIGKILL))
                .killed()
                .is_ok()
        );
        assert!(
            ChildExit::Escalated(ExitStatus::from_raw(libc::SIGKILL))
                .killed()
                .is_err()
        );
        assert!(
            ChildExit::Observed(ExitStatus::from_raw(101 << 8))
                .killed()
                .is_err()
        );
    }

    #[test]
    fn terminal_files_are_bounded_private_and_never_overwritten() {
        let directory = tempfile::tempdir().unwrap();
        let attempt = Attempt::new(directory.path()).unwrap();
        assert!(read_receipt(&attempt.control.path).is_err());
        write_receipt(&attempt.control.path, &sample()).unwrap();
        assert!(write_receipt(&attempt.control.path, &sample()).is_err());
        assert_eq!(
            read_receipt(&attempt.control.path).unwrap().cause,
            sample().cause
        );
        fs::write(&attempt.control.path, b"{\"selected\":true}").unwrap();
        assert!(read_receipt(&attempt.control.path).is_err());
        fs::write(&attempt.control.path, vec![b'x'; MAX_BYTES as usize + 1]).unwrap();
        assert!(read_receipt(&attempt.control.path).is_err());
        fs::remove_file(&attempt.control.path).unwrap();
        std::os::unix::fs::symlink("/dev/null", &attempt.control.path).unwrap();
        assert!(read_receipt(&attempt.control.path).is_err());
    }

    #[test]
    fn controlled_failures_are_not_inferred_from_error_or_panic_text() {
        let error: TestError = "injected failure during bracketed reporting".into();
        assert!(error_cause(&error).is_none());
        assert!(error_cause(&injected(Checkpoint::FailDuringReport, "controlled")).is_some());
        let ordinary: PanicPayload = Box::new("injected panic after bootstrap agent startup");
        assert!(unpack_panic(ordinary).1.is_none());
        let tagged = tag_panic(Err(Box::new("original panic")), true).unwrap_err();
        let (original, cause) = unpack_panic(tagged);
        assert_eq!(original.downcast_ref::<&str>(), Some(&"original panic"));
        assert_eq!(
            cause,
            Some(Cause::Checkpoint {
                checkpoint: Checkpoint::PanicAfterAgentStart
            })
        );
    }

    #[test]
    fn cleanup_failure_after_expected_cause_never_readies_terminal_evidence() {
        use super::super::RuntimeOutcome;
        use kuberic_mssql_tests::three_replica::{
            CombinedFixtureError, FailureCategory, FailureStage, SanitizedFailure,
        };
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                *CHILD.lock().unwrap() = None;
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let attempt = Attempt::new(directory.path()).unwrap();
        {
            let mut child = CHILD.lock().unwrap();
            assert!(child.is_none());
            *child = Some(ChildControl {
                control: attempt.control.clone(),
                ready: None,
            });
        }
        let _reset = Reset;
        for outcome in [
            RuntimeOutcome::Interrupted(HandledCancellationSignal::Interrupt),
            RuntimeOutcome::Completed(Ok(Err(injected(
                Checkpoint::FailDuringReport,
                "expected report failure",
            )))),
        ] {
            let cleanup = CombinedFixtureError::new(
                SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::DeadlineExceeded),
                Vec::new(),
            );
            let error =
                super::super::finish_runtime_cleanup(directory.path(), outcome, Err(cleanup))
                    .unwrap_err();
            assert!(error.to_string().contains("cleanup also failed"));
            assert!(!is_ready());
            assert!(publish_ready().is_err());
            assert!(!attempt.control.path.exists());
        }
    }
}
