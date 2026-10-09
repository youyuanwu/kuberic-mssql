use crate::{Guid, SqlIdentifier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeConvergenceState {
    Absent,
    Incomplete,
    Complete,
    Conflicting,
    Unsafe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePresence {
    Absent,
    Exact,
    Conflicting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeEffectEvidence {
    pub presence: NativePresence,
    pub postcondition_complete: bool,
    pub unsafe_state: bool,
}

impl NativeEffectEvidence {
    pub const fn classify(self) -> NativeConvergenceState {
        if self.unsafe_state {
            return NativeConvergenceState::Unsafe;
        }
        match self.presence {
            NativePresence::Absent => NativeConvergenceState::Absent,
            NativePresence::Conflicting => NativeConvergenceState::Conflicting,
            NativePresence::Exact if self.postcondition_complete => {
                NativeConvergenceState::Complete
            }
            NativePresence::Exact => NativeConvergenceState::Incomplete,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseBootstrapEvidence(pub NativeEffectEvidence);

impl DatabaseBootstrapEvidence {
    pub const fn classify(self) -> NativeConvergenceState {
        self.0.classify()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneDatabaseIdentity {
    pub name: SqlIdentifier,
    pub database_guid: Guid,
    pub family_guid: Guid,
    pub recovery_fork_guid: Guid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneDatabaseSnapshot {
    pub identity: Option<StandaloneDatabaseIdentity>,
    pub state: String,
    pub recovery_model: String,
}

pub fn classify_standalone_database(
    snapshot: Option<&StandaloneDatabaseSnapshot>,
    expected: Option<&StandaloneDatabaseIdentity>,
    unsafe_state: bool,
) -> NativeConvergenceState {
    if unsafe_state {
        return NativeConvergenceState::Unsafe;
    }
    let Some(snapshot) = snapshot else {
        return NativeConvergenceState::Absent;
    };
    let Some(expected) = expected else {
        return NativeConvergenceState::Conflicting;
    };
    let Some(actual) = &snapshot.identity else {
        return NativeConvergenceState::Incomplete;
    };
    if actual != expected || snapshot.recovery_model != "FULL" {
        return NativeConvergenceState::Conflicting;
    }
    if snapshot.state == "ONLINE" {
        NativeConvergenceState::Complete
    } else {
        NativeConvergenceState::Incomplete
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AvailabilityGroupBootstrapEvidence(pub NativeEffectEvidence);

impl AvailabilityGroupBootstrapEvidence {
    pub const fn classify(self) -> NativeConvergenceState {
        self.0.classify()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaJoinEvidence(pub NativeEffectEvidence);

impl ReplicaJoinEvidence {
    pub const fn classify(self) -> NativeConvergenceState {
        self.0.classify()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaSeedEvidence(pub NativeEffectEvidence);

impl ReplicaSeedEvidence {
    pub const fn classify(self) -> NativeConvergenceState {
        self.0.classify()
    }
}
