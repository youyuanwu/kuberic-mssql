use std::error::Error;
use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use super::model::{
    CombinedFixtureError, FailureCategory, FailureStage, ResourceKind, ResourceRecord,
    ResourceState, RunState, SanitizedFailure,
};
use super::ownership::{OwnershipInspector, ReconcileError, ResourceObservation};

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

pub trait CleanupJournal {
    fn state_mut(&mut self) -> &mut RunState;
    fn resources(&self) -> &[ResourceRecord];
    fn resources_mut(&mut self) -> &mut Vec<ResourceRecord>;
    fn clear_blocked_owner(&mut self);
}

pub trait CleanupJournalStore<J> {
    fn save_cleanup_journal(&self, journal: &J) -> Result<(), ReconcileError>;
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

    pub fn cleanup<J, S>(
        &self,
        store: &S,
        journal: &mut J,
        backend: &impl CleanupBackend,
    ) -> CleanupReport
    where
        J: CleanupJournal,
        S: CleanupJournalStore<J>,
    {
        cleanup_with_coordinator(store, journal, backend, self)
    }

    pub fn coordinate<T, J, S>(
        &self,
        completion: CleanupCompletion<T>,
        store: &S,
        journal: &mut J,
        backend: &impl CleanupBackend,
    ) -> Result<T, CombinedFixtureError>
    where
        J: CleanupJournal,
        S: CleanupJournalStore<J>,
    {
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
        let detail = match resource.kind {
            ResourceKind::Container => "container identity or immutable attributes changed",
            ResourceKind::Network => "network identity, attributes, or attachments changed",
            ResourceKind::DataDirectory | ResourceKind::Directory | ResourceKind::SecretFile => {
                "path identity or attributes changed"
            }
            ResourceKind::AvailabilityGroup | ResourceKind::Database => {
                "native SQL resource ownership changed"
            }
        };
        Self::ownership_with_detail(resource, detail)
    }

    fn ownership_with_detail(resource: &ResourceRecord, detail: impl Into<String>) -> Self {
        Self {
            resource: resource.logical_name.clone(),
            failure: SanitizedFailure::with_detail(
                FailureStage::Cleanup,
                FailureCategory::OwnershipMismatch,
                detail,
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

pub fn cleanup<J, S>(store: &S, journal: &mut J, backend: &impl CleanupBackend) -> CleanupReport
where
    J: CleanupJournal,
    S: CleanupJournalStore<J>,
{
    CleanupCoordinator::default().cleanup(store, journal, backend)
}

fn cleanup_with_coordinator<J, S>(
    store: &S,
    journal: &mut J,
    backend: &impl CleanupBackend,
    coordinator: &CleanupCoordinator<impl CleanupClock>,
) -> CleanupReport
where
    J: CleanupJournal,
    S: CleanupJournalStore<J>,
{
    let mut report = CleanupReport {
        removed: Vec::new(),
        unresolved: Vec::new(),
        errors: Vec::new(),
    };
    *journal.state_mut() = RunState::Cleaning;
    if store.save_cleanup_journal(journal).is_err() {
        report.errors.push(CleanupError {
            resource: "ownership-journal".to_owned(),
            failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::Journal),
        });
        return report;
    }

    let mut cleanup_order = (0..journal.resources().len())
        .rev()
        .filter(|index| journal.resources()[*index].kind == ResourceKind::Container)
        .collect::<Vec<_>>();
    cleanup_order.extend(
        (0..journal.resources().len())
            .rev()
            .filter(|index| journal.resources()[*index].kind != ResourceKind::Container),
    );
    for index in cleanup_order {
        let record = journal.resources()[index].clone();
        if record.state == ResourceState::Removed {
            continue;
        }
        if coordinator.remaining().is_zero() {
            exhaust_budget(journal, index, &record, &mut report);
            continue;
        }
        if record.state == ResourceState::Intended {
            journal.resources_mut()[index].state = ResourceState::Removed;
            report.removed.push(record.logical_name.clone());
            if store.save_cleanup_journal(journal).is_err() {
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
            block_with_detail(
                journal,
                index,
                &record,
                &mut report,
                Some("owned container cleanup is not complete"),
            );
            let _ = store.save_cleanup_journal(journal);
            continue;
        }
        if matches!(
            record.kind,
            ResourceKind::DataDirectory | ResourceKind::Directory
        ) && !descendants_proven_removed(journal, &record)
        {
            block_with_detail(
                journal,
                index,
                &record,
                &mut report,
                Some("owned descendant cleanup is not complete"),
            );
            let _ = store.save_cleanup_journal(journal);
            continue;
        }
        let observation = match backend.inspect_cleanup(&record, coordinator.remaining()) {
            Ok(observation) => observation,
            Err(_) => {
                block_with_detail(
                    journal,
                    index,
                    &record,
                    &mut report,
                    Some("resource inspection could not prove exact ownership"),
                );
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
                    block_with_detail(
                        journal,
                        index,
                        &record,
                        &mut report,
                        Some("create dispatch outcome is ambiguous"),
                    );
                } else {
                    journal.resources_mut()[index].state = ResourceState::Removed;
                    report.removed.push(record.logical_name.clone());
                    if store.save_cleanup_journal(journal).is_err() {
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
                    block_with_detail(
                        journal,
                        index,
                        &record,
                        &mut report,
                        Some("resource has foreign attachments"),
                    );
                    continue;
                }
                binding
            }
        };
        if let Some(expected) = &record.binding {
            if expected != &binding {
                block_with_detail(
                    journal,
                    index,
                    &record,
                    &mut report,
                    Some("journaled immutable binding changed"),
                );
                continue;
            }
        } else if matches!(
            record.state,
            ResourceState::Dispatched | ResourceState::Blocked
        ) {
            journal.resources_mut()[index].binding = Some(binding);
        } else {
            block_with_detail(
                journal,
                index,
                &record,
                &mut report,
                Some("resource state lacks a dispatched binding"),
            );
            continue;
        }
        journal.resources_mut()[index].state = ResourceState::Cleaning;
        if store.save_cleanup_journal(journal).is_err() {
            report.errors.push(CleanupError::journal(&record));
            block(journal, index, &record, &mut report);
            continue;
        }
        if let Err(error) = remove(
            backend,
            &journal.resources()[index],
            coordinator.remaining(),
        ) {
            journal.resources_mut()[index].state = ResourceState::Blocked;
            report.unresolved.push(record.logical_name.clone());
            report.errors.push(error);
            let _ = store.save_cleanup_journal(journal);
            continue;
        }
        if coordinator.remaining().is_zero() {
            exhaust_budget(journal, index, &record, &mut report);
            continue;
        }
        match backend.inspect_cleanup(&journal.resources()[index], coordinator.remaining()) {
            Ok(ResourceObservation::Absent) => {
                journal.resources_mut()[index].state = ResourceState::Removed;
                report.removed.push(record.logical_name.clone());
                if store.save_cleanup_journal(journal).is_err() {
                    report.errors.push(CleanupError::journal(&record));
                }
            }
            Ok(ResourceObservation::Owned { .. }) | Ok(ResourceObservation::Foreign) | Err(_) => {
                block_with_detail(
                    journal,
                    index,
                    &record,
                    &mut report,
                    Some("resource remained present after removal"),
                )
            }
        }
    }

    let final_state = if journal
        .resources()
        .iter()
        .all(|resource| resource.state == ResourceState::Removed)
        && report.errors.is_empty()
    {
        journal.clear_blocked_owner();
        RunState::Removed
    } else {
        RunState::Blocked
    };
    *journal.state_mut() = final_state;
    if store.save_cleanup_journal(journal).is_err() {
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

fn containers_proven_absent(journal: &impl CleanupJournal) -> bool {
    journal.resources().iter().all(|resource| {
        resource.kind != ResourceKind::Container || resource.state == ResourceState::Removed
    })
}

fn descendants_proven_removed(journal: &impl CleanupJournal, directory: &ResourceRecord) -> bool {
    let Some(directory_path) = directory.path.as_ref() else {
        return false;
    };
    journal.resources().iter().all(|resource| {
        resource.logical_name == directory.logical_name
            || resource.path.as_ref().is_none_or(|path| {
                !path.starts_with(directory_path) || resource.state == ResourceState::Removed
            })
    })
}

fn exhaust_budget(
    journal: &mut impl CleanupJournal,
    index: usize,
    record: &ResourceRecord,
    report: &mut CleanupReport,
) {
    journal.resources_mut()[index].state = ResourceState::Blocked;
    if !report.unresolved.contains(&record.logical_name) {
        report.unresolved.push(record.logical_name.clone());
    }
    report.errors.push(CleanupError {
        resource: record.logical_name.clone(),
        failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::DeadlineExceeded),
    });
}

fn block(
    journal: &mut impl CleanupJournal,
    index: usize,
    record: &ResourceRecord,
    report: &mut CleanupReport,
) {
    block_with_detail(journal, index, record, report, None);
}

fn block_with_detail(
    journal: &mut impl CleanupJournal,
    index: usize,
    record: &ResourceRecord,
    report: &mut CleanupReport,
    detail: Option<&str>,
) {
    journal.resources_mut()[index].state = ResourceState::Blocked;
    if !report.unresolved.contains(&record.logical_name) {
        report.unresolved.push(record.logical_name.clone());
    }
    report.errors.push(match detail {
        Some(detail) => CleanupError::ownership_with_detail(record, detail),
        None => CleanupError::ownership(record),
    });
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
