use std::error::Error;
use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use super::model::{
    CombinedFixtureError, FailureCategory, FailureStage, OwnershipJournal, ResourceKind,
    ResourceRecord, ResourceState, RunState, SanitizedFailure,
};
use super::ownership::{JournalStore, OwnershipInspector, ReconcileError, ResourceObservation};

pub trait CleanupBackend: OwnershipInspector {
    fn inspect_cleanup(
        &self,
        resource: &ResourceRecord,
        _remaining: Duration,
    ) -> Result<ResourceObservation, ReconcileError> {
        self.inspect(resource)
    }

    fn remove_container(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError>;
    fn remove_network(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError>;
    fn remove_path(
        &self,
        resource: &ResourceRecord,
        remaining: Duration,
    ) -> Result<(), CleanupError>;
}

pub const CLEANUP_BUDGET: Duration = Duration::from_secs(180);

pub trait CleanupClock {
    fn now(&self) -> Duration;
}

pub struct OperationBudget<'a, C> {
    clock: &'a C,
    deadline: Duration,
}

impl<'a, C: CleanupClock> OperationBudget<'a, C> {
    pub fn new(clock: &'a C, budget: Duration) -> Self {
        Self {
            clock,
            deadline: clock.now().saturating_add(budget),
        }
    }

    pub fn remaining(&self) -> Option<Duration> {
        let remaining = self.deadline.saturating_sub(self.clock.now());
        (!remaining.is_zero()).then_some(remaining)
    }

    pub fn limit(&self, local: Duration) -> Option<Duration> {
        self.remaining().map(|remaining| remaining.min(local))
    }
}

#[derive(Debug, Clone)]
pub struct SystemCleanupClock {
    origin: Instant,
}

impl Default for SystemCleanupClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl CleanupClock for SystemCleanupClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandledCancellationSignal {
    Interrupt,
    Terminate,
}

pub struct CancellationSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl CancellationSignals {
    pub fn register() -> io::Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    pub async fn recv(&mut self) -> HandledCancellationSignal {
        tokio::select! {
            _ = self.interrupt.recv() => HandledCancellationSignal::Interrupt,
            _ = self.terminate.recv() => HandledCancellationSignal::Terminate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupCompletion<T> {
    Result(Result<T, SanitizedFailure>),
    CaughtPanic(SanitizedFailure),
    HandledSignal {
        signal: HandledCancellationSignal,
        failure: SanitizedFailure,
    },
}

pub struct CleanupCoordinator<C = SystemCleanupClock> {
    clock: C,
    started_at: Duration,
    budget: Duration,
}

impl Default for CleanupCoordinator<SystemCleanupClock> {
    fn default() -> Self {
        Self::new(SystemCleanupClock::default(), CLEANUP_BUDGET)
    }
}

impl<C: CleanupClock> CleanupCoordinator<C> {
    pub fn new(clock: C, budget: Duration) -> Self {
        let started_at = clock.now();
        Self {
            clock,
            started_at,
            budget,
        }
    }

    pub fn remaining(&self) -> Duration {
        self.budget
            .saturating_sub(self.clock.now().saturating_sub(self.started_at))
    }

    pub fn clock(&self) -> &C {
        &self.clock
    }

    pub fn cleanup(
        &self,
        store: &JournalStore,
        journal: &mut OwnershipJournal,
        backend: &impl CleanupBackend,
    ) -> CleanupReport {
        cleanup_with_coordinator(store, journal, backend, self)
    }

    pub fn coordinate<T>(
        &self,
        completion: CleanupCompletion<T>,
        store: &JournalStore,
        journal: &mut OwnershipJournal,
        backend: &impl CleanupBackend,
    ) -> Result<T, CombinedFixtureError> {
        let primary = match completion {
            CleanupCompletion::Result(result) => result,
            CleanupCompletion::CaughtPanic(failure)
            | CleanupCompletion::HandledSignal { failure, .. } => Err(failure),
        };
        combine_with_cleanup(primary, &self.cleanup(store, journal, backend))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupError {
    pub resource: String,
    pub failure: SanitizedFailure,
}

impl CleanupError {
    fn ownership(resource: &ResourceRecord) -> Self {
        Self {
            resource: resource.logical_name.clone(),
            failure: SanitizedFailure::new(
                FailureStage::Cleanup,
                FailureCategory::OwnershipMismatch,
            ),
        }
    }

    fn journal(resource: &ResourceRecord) -> Self {
        Self {
            resource: resource.logical_name.clone(),
            failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::Journal),
        }
    }
}

impl fmt::Display for CleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.resource, self.failure)
    }
}

impl Error for CleanupError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupReport {
    pub removed: Vec<String>,
    pub unresolved: Vec<String>,
    pub errors: Vec<CleanupError>,
}

impl CleanupReport {
    pub fn succeeded(&self) -> bool {
        self.unresolved.is_empty() && self.errors.is_empty()
    }
}

pub fn cleanup(
    store: &JournalStore,
    journal: &mut OwnershipJournal,
    backend: &impl CleanupBackend,
) -> CleanupReport {
    CleanupCoordinator::default().cleanup(store, journal, backend)
}

fn cleanup_with_coordinator(
    store: &JournalStore,
    journal: &mut OwnershipJournal,
    backend: &impl CleanupBackend,
    coordinator: &CleanupCoordinator<impl CleanupClock>,
) -> CleanupReport {
    let mut report = CleanupReport {
        removed: Vec::new(),
        unresolved: Vec::new(),
        errors: Vec::new(),
    };
    journal.state = RunState::Cleaning;
    if store.save(journal).is_err() {
        report.errors.push(CleanupError {
            resource: "ownership-journal".to_owned(),
            failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::Journal),
        });
        return report;
    }

    let mut cleanup_order = (0..journal.resources.len())
        .rev()
        .filter(|index| journal.resources[*index].kind == ResourceKind::Container)
        .collect::<Vec<_>>();
    cleanup_order.extend(
        (0..journal.resources.len())
            .rev()
            .filter(|index| journal.resources[*index].kind != ResourceKind::Container),
    );
    for index in cleanup_order {
        let record = journal.resources[index].clone();
        if record.state == ResourceState::Removed {
            continue;
        }
        if coordinator.remaining().is_zero() {
            exhaust_budget(journal, index, &record, &mut report);
            continue;
        }
        if record.state == ResourceState::Intended {
            journal.resources[index].state = ResourceState::Removed;
            report.removed.push(record.logical_name.clone());
            if store.save(journal).is_err() {
                report.errors.push(CleanupError::journal(&record));
            }
            continue;
        }
        if matches!(
            record.kind,
            ResourceKind::DataDirectory
                | ResourceKind::Directory
                | ResourceKind::SecretFile
                | ResourceKind::Network
        ) && !containers_proven_absent(journal)
        {
            block(journal, index, &record, &mut report);
            let _ = store.save(journal);
            continue;
        }
        if matches!(
            record.kind,
            ResourceKind::DataDirectory | ResourceKind::Directory
        ) && !descendants_proven_removed(journal, &record)
        {
            block(journal, index, &record, &mut report);
            let _ = store.save(journal);
            continue;
        }
        let observation = match backend.inspect_cleanup(&record, coordinator.remaining()) {
            Ok(observation) => observation,
            Err(_) => {
                block(journal, index, &record, &mut report);
                continue;
            }
        };
        if coordinator.remaining().is_zero() {
            exhaust_budget(journal, index, &record, &mut report);
            continue;
        }
        let binding = match observation {
            ResourceObservation::Absent => {
                if matches!(
                    record.state,
                    ResourceState::Dispatched | ResourceState::Blocked
                ) && record.binding.is_none()
                    && !matches!(
                        record.kind,
                        ResourceKind::DataDirectory
                            | ResourceKind::Directory
                            | ResourceKind::SecretFile
                    )
                {
                    block(journal, index, &record, &mut report);
                } else {
                    journal.resources[index].state = ResourceState::Removed;
                    report.removed.push(record.logical_name.clone());
                    if store.save(journal).is_err() {
                        report.errors.push(CleanupError::journal(&record));
                    }
                }
                continue;
            }
            ResourceObservation::Foreign => {
                block(journal, index, &record, &mut report);
                continue;
            }
            ResourceObservation::Owned {
                binding,
                foreign_attachments,
            } => {
                if !foreign_attachments.is_empty() {
                    block(journal, index, &record, &mut report);
                    continue;
                }
                binding
            }
        };
        if let Some(expected) = &record.binding {
            if expected != &binding {
                block(journal, index, &record, &mut report);
                continue;
            }
        } else if matches!(
            record.state,
            ResourceState::Dispatched | ResourceState::Blocked
        ) {
            journal.resources[index].binding = Some(binding);
        } else {
            block(journal, index, &record, &mut report);
            continue;
        }
        journal.resources[index].state = ResourceState::Cleaning;
        if store.save(journal).is_err() {
            report.errors.push(CleanupError::journal(&record));
            block(journal, index, &record, &mut report);
            continue;
        }
        if let Err(error) = remove(backend, &journal.resources[index], coordinator.remaining()) {
            journal.resources[index].state = ResourceState::Blocked;
            report.unresolved.push(record.logical_name.clone());
            report.errors.push(error);
            let _ = store.save(journal);
            continue;
        }
        if coordinator.remaining().is_zero() {
            exhaust_budget(journal, index, &record, &mut report);
            continue;
        }
        match backend.inspect_cleanup(&journal.resources[index], coordinator.remaining()) {
            Ok(ResourceObservation::Absent) => {
                journal.resources[index].state = ResourceState::Removed;
                report.removed.push(record.logical_name.clone());
                if store.save(journal).is_err() {
                    report.errors.push(CleanupError::journal(&record));
                }
            }
            Ok(ResourceObservation::Owned { .. }) | Ok(ResourceObservation::Foreign) | Err(_) => {
                block(journal, index, &record, &mut report)
            }
        }
    }

    journal.state = if journal
        .resources
        .iter()
        .all(|resource| resource.state == ResourceState::Removed)
        && report.errors.is_empty()
    {
        journal.blocked_owner = None;
        RunState::Removed
    } else {
        RunState::Blocked
    };
    if store.save(journal).is_err() {
        report.errors.push(CleanupError {
            resource: "ownership-journal".to_owned(),
            failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::Journal),
        });
    }
    report
}

pub fn combine_with_cleanup<T>(
    primary: Result<T, SanitizedFailure>,
    cleanup: &CleanupReport,
) -> Result<T, CombinedFixtureError> {
    let cleanup_failures = cleanup
        .errors
        .iter()
        .map(|error| {
            error
                .failure
                .clone()
                .with_context(format!("resource {}", error.resource))
        })
        .collect::<Vec<_>>();
    match primary {
        Ok(value) if cleanup.succeeded() => Ok(value),
        Ok(_) => {
            let primary = cleanup_failures.first().cloned().unwrap_or_else(|| {
                SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::OwnershipMismatch)
            });
            Err(CombinedFixtureError::new(
                primary,
                cleanup_failures.into_iter().skip(1).collect(),
            ))
        }
        Err(primary) => Err(CombinedFixtureError::new(primary, cleanup_failures)),
    }
}

fn remove(
    backend: &impl CleanupBackend,
    resource: &ResourceRecord,
    remaining: Duration,
) -> Result<(), CleanupError> {
    match resource.kind {
        ResourceKind::Container => backend.remove_container(resource, remaining),
        ResourceKind::Network => backend.remove_network(resource, remaining),
        ResourceKind::DataDirectory | ResourceKind::Directory | ResourceKind::SecretFile => {
            backend.remove_path(resource, remaining)
        }
        ResourceKind::AvailabilityGroup | ResourceKind::Database => Err(CleanupError {
            resource: resource.logical_name.clone(),
            failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::SqlUnavailable),
        }),
    }
}

fn containers_proven_absent(journal: &OwnershipJournal) -> bool {
    journal.resources.iter().all(|resource| {
        resource.kind != ResourceKind::Container || resource.state == ResourceState::Removed
    })
}

fn descendants_proven_removed(journal: &OwnershipJournal, directory: &ResourceRecord) -> bool {
    let Some(directory_path) = directory.path.as_ref() else {
        return false;
    };
    journal.resources.iter().all(|resource| {
        resource.logical_name == directory.logical_name
            || resource.path.as_ref().is_none_or(|path| {
                !path.starts_with(directory_path) || resource.state == ResourceState::Removed
            })
    })
}

fn exhaust_budget(
    journal: &mut OwnershipJournal,
    index: usize,
    record: &ResourceRecord,
    report: &mut CleanupReport,
) {
    journal.resources[index].state = ResourceState::Blocked;
    if !report.unresolved.contains(&record.logical_name) {
        report.unresolved.push(record.logical_name.clone());
    }
    report.errors.push(CleanupError {
        resource: record.logical_name.clone(),
        failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::DeadlineExceeded),
    });
}

fn block(
    journal: &mut OwnershipJournal,
    index: usize,
    record: &ResourceRecord,
    report: &mut CleanupReport,
) {
    journal.resources[index].state = ResourceState::Blocked;
    if !report.unresolved.contains(&record.logical_name) {
        report.unresolved.push(record.logical_name.clone());
    }
    report.errors.push(CleanupError::ownership(record));
}

impl From<ReconcileError> for CleanupError {
    fn from(_: ReconcileError) -> Self {
        Self {
            resource: "ownership-inspection".to_owned(),
            failure: SanitizedFailure::new(
                FailureStage::Cleanup,
                FailureCategory::OwnershipMismatch,
            ),
        }
    }
}
