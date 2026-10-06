use std::error::Error;
use std::fmt;

use super::model::{
    CombinedFixtureError, FailureCategory, FailureStage, OwnershipJournal, ResourceKind,
    ResourceRecord, ResourceState, RunState, SanitizedFailure,
};
use super::ownership::{JournalStore, OwnershipInspector, ReconcileError, ResourceObservation};

pub trait CleanupBackend: OwnershipInspector {
    fn remove_container(&self, resource: &ResourceRecord) -> Result<(), CleanupError>;
    fn remove_network(&self, resource: &ResourceRecord) -> Result<(), CleanupError>;
    fn remove_path(&self, resource: &ResourceRecord) -> Result<(), CleanupError>;
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

    for index in (0..journal.resources.len()).rev() {
        let record = journal.resources[index].clone();
        if record.state == ResourceState::Removed {
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
        let observation = match backend.inspect(&record) {
            Ok(observation) => observation,
            Err(_) => {
                block(journal, index, &record, &mut report);
                continue;
            }
        };
        let binding = match observation {
            ResourceObservation::Absent => {
                if matches!(
                    record.state,
                    ResourceState::Dispatched | ResourceState::Blocked
                ) && record.binding.is_none()
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
        if let Err(error) = remove(backend, &journal.resources[index]) {
            journal.resources[index].state = ResourceState::Blocked;
            report.unresolved.push(record.logical_name.clone());
            report.errors.push(error);
            let _ = store.save(journal);
            continue;
        }
        match backend.inspect(&journal.resources[index]) {
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
        .map(|error| error.failure)
        .collect::<Vec<_>>();
    match primary {
        Ok(value) if cleanup.succeeded() => Ok(value),
        Ok(_) => {
            let primary = cleanup_failures.first().copied().unwrap_or_else(|| {
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

fn remove(backend: &impl CleanupBackend, resource: &ResourceRecord) -> Result<(), CleanupError> {
    match resource.kind {
        ResourceKind::Container => backend.remove_container(resource),
        ResourceKind::Network => backend.remove_network(resource),
        ResourceKind::DataDirectory | ResourceKind::SecretFile => backend.remove_path(resource),
        ResourceKind::AvailabilityGroup | ResourceKind::Database => Err(CleanupError {
            resource: resource.logical_name.clone(),
            failure: SanitizedFailure::new(FailureStage::Cleanup, FailureCategory::SqlUnavailable),
        }),
    }
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
