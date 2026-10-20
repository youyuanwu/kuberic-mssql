use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

const DIRECTORY_ENV: &str = "KUBERIC_MSSQL_PHASE_TIMING_DIR";
const SCENARIO_ENV: &str = "KUBERIC_MSSQL_PHASE_TIMING_SCENARIO";
const ATTEMPT_ENV: &str = "KUBERIC_MSSQL_PHASE_TIMING_ATTEMPT";
const MAX_PHASES: usize = 256;
static ACTIVE: Mutex<Option<Arc<Shared>>> = Mutex::new(None);
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Scenario {
    LeaseExpiry,
    LeaseActiveRestart,
    SigintRecovery,
    SigtermRecovery,
    SigkillRecovery,
    RestartCheckpoint,
    TargetSqlRestart,
    AcceptedReplyLost,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Attempt {
    Main,
    InterruptedChild,
    FaultChild,
    RetryChild,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    FixtureLaunch,
    Preflight,
    Assets,
    ContainerCreation,
    ContainerStart,
    MemberOne,
    MemberTwo,
    MemberThree,
    PeerPreparation,
    PublicAssembly,
    HostStabilization,
    Reports,
    Build,
    SeedRetry,
    NativeConvergence,
    Marker,
    SqlRestart,
    FreshSql,
    CallbackRejection,
    HostDrain,
    Cleanup,
    InterruptedLaunch,
    SeparateCleanup,
    RecoveryRetry,
    FaultJourney,
    LeaseFormation,
    LeaseBaseline,
    LeaseRenewal,
    OriginalExpiryWait,
    ExtensionWrite,
    RenewedExpiryWait,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Outcome {
    Running,
    Succeeded,
    Failed,
    Incomplete,
    Panicked,
}

#[derive(Serialize)]
struct Record {
    phase: Phase,
    outcome: Outcome,
    elapsed_ms: u64,
}

#[derive(Serialize)]
struct Report {
    version: u8,
    scenario: Scenario,
    attempt: Attempt,
    process: u32,
    outcome: Outcome,
    phases: Vec<Record>,
}

struct Sink {
    directory: PathBuf,
    path: PathBuf,
}

struct Shared {
    report: Mutex<Report>,
    sink: Option<Sink>,
    failed: AtomicBool,
}

impl Shared {
    fn update(&self, update: impl FnOnce(&mut Report)) {
        let mut report = match self.report.lock() {
            Ok(report) => report,
            Err(error) => {
                self.diagnostic_failure();
                error.into_inner()
            }
        };
        update(&mut report);
        if self.persist(&report).is_err() {
            self.diagnostic_failure();
        }
    }

    fn diagnostic_failure(&self) {
        self.failed.store(true, Ordering::Relaxed);
        let _ = writeln!(
            io::stderr(),
            "MSSQL_PHASE_REPORT_ERROR: timing report failed"
        );
    }

    fn persist(&self, report: &Report) -> io::Result<()> {
        let Some(sink) = &self.sink else {
            return Ok(());
        };
        let mut file = tempfile::Builder::new()
            .prefix(".phase-tmp-")
            .tempfile_in(&sink.directory)?;
        serde_json::to_writer(file.as_file_mut(), report)?;
        file.as_file_mut().flush()?;
        file.as_file().sync_data()?;
        file.persist(&sink.path).map_err(|error| error.error)?;
        Ok(())
    }
}

pub struct Session {
    shared: Arc<Shared>,
    complete: bool,
}

impl Session {
    pub fn start(scenario: Scenario) -> io::Result<Self> {
        Self::new(scenario, Attempt::Main, directory()?.as_deref())
    }

    pub fn child() -> io::Result<Option<Self>> {
        let Some(scenario) = std::env::var_os(SCENARIO_ENV) else {
            return Ok(None);
        };
        let scenario = serde_json::from_str(scenario.to_str().ok_or_else(invalid_context)?)
            .map_err(|_| invalid_context())?;
        let attempt = std::env::var(ATTEMPT_ENV).map_err(|_| invalid_context())?;
        let attempt = serde_json::from_str(&attempt).map_err(|_| invalid_context())?;
        if matches!(attempt, Attempt::Main) {
            return Err(invalid_context());
        }
        Self::new(scenario, attempt, directory()?.as_deref()).map(Some)
    }

    fn new(scenario: Scenario, attempt: Attempt, directory: Option<&Path>) -> io::Result<Self> {
        let mut active = ACTIVE
            .lock()
            .map_err(|_| io::Error::other("timing context poisoned"))?;
        if active.is_some() {
            return Err(io::Error::other("overlapping live timing sessions"));
        }
        let session = Self::create(scenario, attempt, directory)?;
        *active = Some(session.shared.clone());
        Ok(session)
    }

    fn create(scenario: Scenario, attempt: Attempt, directory: Option<&Path>) -> io::Result<Self> {
        let sink = if let Some(directory) = directory {
            let metadata = fs::symlink_metadata(directory)?;
            if !metadata.is_dir()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.permissions().mode() & 0o077 != 0
                || directory.canonicalize()? != directory
            {
                return Err(io::Error::other("unsafe timing directory"));
            }
            let identity = format!(
                "{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            );
            let path = directory.join(format!("{identity}.json"));
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(directory.join(format!(".required-{identity}")))?;
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            Some(Sink {
                directory: directory.to_owned(),
                path,
            })
        } else {
            None
        };
        let shared = Arc::new(Shared {
            report: Mutex::new(Report {
                version: 1,
                scenario,
                attempt,
                process: std::process::id(),
                outcome: Outcome::Running,
                phases: Vec::new(),
            }),
            sink,
            failed: AtomicBool::new(false),
        });
        {
            let report = shared
                .report
                .lock()
                .map_err(|_| io::Error::other("timing context poisoned"))?;
            shared.persist(&report)?;
        }
        Ok(Self {
            shared,
            complete: false,
        })
    }

    pub fn complete(mut self) -> io::Result<()> {
        self.shared.update(|report| {
            if report
                .phases
                .iter()
                .all(|phase| phase.outcome != Outcome::Running)
            {
                report.outcome = Outcome::Succeeded;
            }
        });
        let succeeded = self
            .shared
            .report
            .lock()
            .map_err(|_| io::Error::other("timing context poisoned"))?
            .outcome
            == Outcome::Succeeded;
        if self.shared.failed.load(Ordering::Relaxed) || !succeeded {
            return Err(io::Error::other("incomplete or failed timing report"));
        }
        self.complete = true;
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if !self.complete {
            self.shared.update(|report| {
                report.outcome = if std::thread::panicking() {
                    Outcome::Panicked
                } else {
                    Outcome::Incomplete
                };
            });
        }
        match ACTIVE.lock() {
            Ok(mut active) => {
                if active
                    .as_ref()
                    .is_some_and(|shared| Arc::ptr_eq(shared, &self.shared))
                {
                    active.take();
                }
            }
            Err(_) => self.shared.diagnostic_failure(),
        }
    }
}

fn invalid_context() -> io::Error {
    io::Error::other("invalid child timing context")
}

fn directory() -> io::Result<Option<PathBuf>> {
    std::env::var_os(DIRECTORY_ENV)
        .map(PathBuf::from)
        .map_or(Ok(None), |path| {
            if path.is_absolute() {
                Ok(Some(path))
            } else {
                Err(io::Error::other("timing directory must be absolute"))
            }
        })
}

pub fn child_context(command: &mut Command, attempt: Attempt) {
    command.env_remove(SCENARIO_ENV).env_remove(ATTEMPT_ENV);
    let active = ACTIVE.lock().expect("live timing context");
    if let Some(shared) = active.as_ref() {
        let report = shared.report.lock().expect("live timing report");
        command
            .env(
                SCENARIO_ENV,
                serde_json::to_string(&report.scenario).expect("finite timing label"),
            )
            .env(
                ATTEMPT_ENV,
                serde_json::to_string(&attempt).expect("finite timing label"),
            );
    }
}

pub struct PhaseGuard {
    shared: Option<Arc<Shared>>,
    index: Option<usize>,
    started: Instant,
    finished: bool,
}

pub fn phase(phase: Phase) -> PhaseGuard {
    let shared = ACTIVE.lock().expect("live timing context").clone();
    phase_in(shared, phase)
}

fn phase_in(shared: Option<Arc<Shared>>, phase: Phase) -> PhaseGuard {
    let mut index = None;
    if let Some(shared) = &shared {
        shared.update(|report| {
            if report.phases.len() < MAX_PHASES {
                index = Some(report.phases.len());
                report.phases.push(Record {
                    phase,
                    outcome: Outcome::Running,
                    elapsed_ms: 0,
                });
            } else {
                shared.diagnostic_failure();
            }
        });
    }
    PhaseGuard {
        shared,
        index,
        started: Instant::now(),
        finished: false,
    }
}

impl PhaseGuard {
    pub fn succeeded(self) {
        self.finish(Outcome::Succeeded);
    }

    pub fn result<T, E>(self, result: &Result<T, E>) {
        self.finish(if result.is_ok() {
            Outcome::Succeeded
        } else {
            Outcome::Failed
        });
    }

    fn finish(mut self, outcome: Outcome) {
        self.record(outcome);
        self.finished = true;
    }

    fn record(&self, outcome: Outcome) {
        if let (Some(shared), Some(index)) = (&self.shared, self.index) {
            shared.update(|report| {
                let record = &mut report.phases[index];
                record.outcome = outcome;
                let Ok(elapsed_ms) = self.started.elapsed().as_millis().try_into() else {
                    shared.diagnostic_failure();
                    return;
                };
                record.elapsed_ms = elapsed_ms;
                if writeln!(
                    io::stderr(),
                    "MSSQL_PHASE scenario={:?} attempt={:?} phase={:?} outcome={:?} elapsed_ms={}",
                    report.scenario,
                    report.attempt,
                    record.phase,
                    outcome,
                    record.elapsed_ms,
                )
                .is_err()
                {
                    shared.diagnostic_failure();
                }
            });
        }
    }
}

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.record(if std::thread::panicking() {
                Outcome::Panicked
            } else {
                Outcome::Incomplete
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(directory: &Path) -> serde_json::Value {
        let file = fs::read_dir(directory)
            .unwrap()
            .map(|file| file.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .unwrap();
        serde_json::from_slice(&fs::read(file).unwrap()).unwrap()
    }

    #[test]
    fn committed_phases_survive_incomplete_session() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        {
            let session =
                Session::create(Scenario::LeaseExpiry, Attempt::Main, Some(directory.path()))
                    .unwrap();
            phase_in(Some(session.shared.clone()), Phase::Preflight).succeeded();
            drop(phase_in(Some(session.shared.clone()), Phase::FixtureLaunch));
        }
        let report = report(directory.path());
        assert_eq!(report["outcome"], "incomplete");
        assert_eq!(report["phases"][0]["outcome"], "succeeded");
        assert_eq!(report["phases"][1]["outcome"], "incomplete");
    }

    #[test]
    fn expected_failure_is_retained_without_overriding_scenario_success() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let session = Session::create(
            Scenario::AcceptedReplyLost,
            Attempt::RetryChild,
            Some(directory.path()),
        )
        .unwrap();
        phase_in(Some(session.shared.clone()), Phase::SeedRetry)
            .result(&Err::<(), _>("not published"));
        session.complete().unwrap();
        assert_eq!(report(directory.path())["outcome"], "succeeded");
        assert_eq!(report(directory.path())["phases"][0]["outcome"], "failed");
    }

    #[test]
    fn unwinding_retains_panicked_phase_without_private_diagnostic() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let result = std::panic::catch_unwind(|| {
            let session = Session::create(
                Scenario::RestartCheckpoint,
                Attempt::FaultChild,
                Some(directory.path()),
            )
            .unwrap();
            let _phase = phase_in(Some(session.shared.clone()), Phase::Build);
            panic!("PRIVATE_SENTINEL");
        });
        assert!(result.is_err());
        let value = report(directory.path());
        assert_eq!(value["outcome"], "panicked");
        assert_eq!(value["phases"][0]["outcome"], "panicked");
        assert!(!value.to_string().contains("PRIVATE_SENTINEL"));
    }

    #[test]
    fn report_io_failure_is_not_success_and_symlink_directory_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let sink = directory.path().join("sink");
        fs::create_dir(&sink).unwrap();
        fs::set_permissions(&sink, fs::Permissions::from_mode(0o700)).unwrap();
        let session = Session::create(Scenario::LeaseExpiry, Attempt::Main, Some(&sink)).unwrap();
        for file in fs::read_dir(&sink).unwrap() {
            fs::remove_file(file.unwrap().path()).unwrap();
        }
        fs::remove_dir(&sink).unwrap();
        phase_in(Some(session.shared.clone()), Phase::Cleanup).succeeded();
        assert!(session.complete().is_err());
        std::os::unix::fs::symlink(directory.path(), &sink).unwrap();
        assert!(Session::create(Scenario::LeaseExpiry, Attempt::Main, Some(&sink)).is_err());
    }
}
