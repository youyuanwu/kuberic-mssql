use crate::observation::{AvailabilityGroupSnapshot, NativeProvenance, RecoveryLineageObservation};
use crate::{
    AvailabilityGroupIdentity, AvailabilityGroupName, DatabaseIdentity, DatabaseLineage, Guid,
    NativeRole, ReplicaDescriptor, ReplicaIdentity, ServerName, SqlIdentifier,
};

#[cfg(feature = "kuberic")]
use kuberic_runtime::protocol::types::{ReplicaIdentity as KubericReplicaIdentity, ReplicaRole};
#[cfg(feature = "kuberic")]
use kuberic_runtime::replicator::ReplicaSetConfiguration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeConvergenceState {
    Absent,
    Incomplete,
    Complete,
    Conflicting,
    Unsafe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneDatabaseIdentity {
    pub name: SqlIdentifier,
    pub database_guid: Guid,
    pub family_guid: Guid,
    pub recovery_fork_guid: Guid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneDatabaseOwnership {
    pub resource_id: String,
    pub configuration_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneDatabaseSnapshot {
    pub identity: Option<StandaloneDatabaseIdentity>,
    pub ownership: Option<StandaloneDatabaseOwnership>,
    pub state: String,
    pub recovery_model: String,
    pub accessible: bool,
}

pub fn classify_standalone_database(
    snapshot: Option<&StandaloneDatabaseSnapshot>,
    expected: Option<&StandaloneDatabaseIdentity>,
    expected_ownership: &StandaloneDatabaseOwnership,
    unsafe_state: bool,
) -> NativeConvergenceState {
    if unsafe_state {
        return NativeConvergenceState::Unsafe;
    }
    let Some(snapshot) = snapshot else {
        return NativeConvergenceState::Absent;
    };
    if !snapshot.accessible {
        return NativeConvergenceState::Unsafe;
    }
    if snapshot.ownership.as_ref() != Some(expected_ownership) {
        return NativeConvergenceState::Conflicting;
    }
    let Some(actual) = &snapshot.identity else {
        return NativeConvergenceState::Incomplete;
    };
    if expected.is_some_and(|expected| actual != expected) || snapshot.recovery_model != "FULL" {
        return NativeConvergenceState::Conflicting;
    }
    if snapshot.state == "ONLINE" {
        NativeConvergenceState::Complete
    } else {
        NativeConvergenceState::Incomplete
    }
}

pub fn classify_availability_group(
    group: Option<&AvailabilityGroupSnapshot>,
    expected_name: &AvailabilityGroupName,
    expected_group_id: Option<&Guid>,
    expected_replicas: &[ReplicaDescriptor],
    unsafe_state: bool,
) -> NativeConvergenceState {
    if unsafe_state {
        return NativeConvergenceState::Unsafe;
    }
    let Some(group) = group else {
        return NativeConvergenceState::Absent;
    };
    if group.identity.name != *expected_name
        || expected_group_id.is_some_and(|group_id| group.identity.group_id != *group_id)
        || group.cluster_type != "EXTERNAL"
        || group.required_synchronized_secondaries_to_commit != 1
        || group.basic_features
        || group.is_distributed
        || group.replicas.len() > expected_replicas.len()
    {
        return NativeConvergenceState::Conflicting;
    }
    let mut incomplete = group.replicas.len() != expected_replicas.len();
    for actual in &group.replicas {
        let Some(expected) = expected_replicas
            .iter()
            .find(|expected| expected.server_name == actual.server_name)
        else {
            return NativeConvergenceState::Conflicting;
        };
        if actual
            .endpoint_url
            .as_deref()
            .is_none_or(|endpoint| !endpoint.eq_ignore_ascii_case(&expected.endpoint.to_string()))
            || actual.availability_mode != "SYNCHRONOUS_COMMIT"
            || actual.failover_mode != "EXTERNAL"
            || actual.seeding_mode != "AUTOMATIC"
        {
            return NativeConvergenceState::Conflicting;
        }
    }
    for expected in expected_replicas {
        let Some(actual) = group
            .replicas
            .iter()
            .find(|replica| replica.server_name == expected.server_name)
        else {
            incomplete = true;
            continue;
        };
        if actual
            .endpoint_url
            .as_deref()
            .is_none_or(|endpoint| !endpoint.eq_ignore_ascii_case(&expected.endpoint.to_string()))
            || actual.availability_mode != "SYNCHRONOUS_COMMIT"
            || actual.failover_mode != "EXTERNAL"
            || actual.seeding_mode != "AUTOMATIC"
        {
            return NativeConvergenceState::Conflicting;
        }
    }
    if incomplete {
        NativeConvergenceState::Incomplete
    } else {
        NativeConvergenceState::Complete
    }
}

pub fn classify_replica_join(
    group: Option<&AvailabilityGroupSnapshot>,
    availability_group: &AvailabilityGroupIdentity,
    expected: &ReplicaIdentity,
    expected_server: &ServerName,
    expected_endpoint: &str,
    unsafe_state: bool,
) -> NativeConvergenceState {
    if unsafe_state {
        return NativeConvergenceState::Unsafe;
    }
    let Some(group) = group else {
        return NativeConvergenceState::Absent;
    };
    if group.identity != *availability_group {
        return NativeConvergenceState::Conflicting;
    }
    let Some(native_id) = expected.native_replica_id() else {
        return NativeConvergenceState::Conflicting;
    };
    let Some(replica) = group
        .replicas
        .iter()
        .find(|replica| &replica.replica_id == native_id)
    else {
        return NativeConvergenceState::Incomplete;
    };
    if replica.server_name != *expected_server
        || replica
            .endpoint_url
            .as_deref()
            .is_none_or(|endpoint| !endpoint.eq_ignore_ascii_case(expected_endpoint))
        || replica.availability_mode != "SYNCHRONOUS_COMMIT"
        || replica.failover_mode != "EXTERNAL"
        || replica.seeding_mode != "AUTOMATIC"
    {
        return NativeConvergenceState::Conflicting;
    }
    if group.local_replica.identity.native_replica_id() != Some(native_id) {
        return NativeConvergenceState::Conflicting;
    }
    match (
        group.local_replica.state_available,
        group.local_replica.role.as_ref(),
    ) {
        (true, Some(NativeRole::Secondary)) => NativeConvergenceState::Complete,
        (true, Some(NativeRole::Primary) | Some(NativeRole::Unknown(_))) => {
            NativeConvergenceState::Conflicting
        }
        _ => NativeConvergenceState::Incomplete,
    }
}

pub fn classify_replica_seed(
    group: Option<&AvailabilityGroupSnapshot>,
    availability_group: &AvailabilityGroupIdentity,
    database: &DatabaseIdentity,
    lineage: &DatabaseLineage,
    target_replica_id: &Guid,
    unsafe_state: bool,
) -> NativeConvergenceState {
    if unsafe_state {
        return NativeConvergenceState::Unsafe;
    }
    let Some(group) = group else {
        return NativeConvergenceState::Absent;
    };
    if group.identity != *availability_group {
        return NativeConvergenceState::Conflicting;
    }
    if group.automatic_seeding.iter().any(|seed| {
        seed.group_database_id == database.group_database_id
            && seed.remote_replica_id == *target_replica_id
            && (seed.current_state.as_deref() == Some("FAILED")
                || seed.performed_seeding == Some(false)
                || seed.failure_state.is_some_and(|value| value != 0)
                || seed.error_code.is_some_and(|value| value != 0))
    }) {
        return NativeConvergenceState::Unsafe;
    }
    let Some(database_snapshot) = group
        .databases
        .iter()
        .find(|candidate| candidate.identity.group_database_id == database.group_database_id)
    else {
        return NativeConvergenceState::Incomplete;
    };
    if database_snapshot.identity.name != database.name {
        return NativeConvergenceState::Conflicting;
    }
    let Some(target) = database_snapshot
        .replicas
        .iter()
        .find(|replica| &replica.replica_id == target_replica_id)
    else {
        return NativeConvergenceState::Incomplete;
    };
    match &target.lineage {
        RecoveryLineageObservation::Local { value } if value != lineage => {
            return NativeConvergenceState::Conflicting;
        }
        RecoveryLineageObservation::Local { .. } => {}
        RecoveryLineageObservation::LocalUnavailable
        | RecoveryLineageObservation::RemoteUnavailable => {
            return NativeConvergenceState::Incomplete;
        }
    }
    if target.provenance != NativeProvenance::Local
        || target.database_state.as_deref() != Some("ONLINE")
        || target.synchronization_state.as_deref() != Some("SYNCHRONIZED")
        || target.synchronization_health.as_deref() != Some("HEALTHY")
        || target.is_suspended != Some(false)
    {
        NativeConvergenceState::Incomplete
    } else {
        NativeConvergenceState::Complete
    }
}

#[cfg(feature = "kuberic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapOwnership {
    PrimaryTopology,
    LocalJoin,
}

#[cfg(feature = "kuberic")]
pub fn classify_bootstrap_ownership(
    local: &KubericReplicaIdentity,
    configuration: &ReplicaSetConfiguration,
) -> Result<BootstrapOwnership, BootstrapOwnershipError> {
    let member = configuration
        .configuration
        .members
        .iter()
        .find(|member| member.identity == *local)
        .ok_or(BootstrapOwnershipError::LocalMemberMissing)?;
    if member.identity.replica_id == configuration.configuration.primary_id
        && member.role == ReplicaRole::Primary
    {
        return Ok(BootstrapOwnership::PrimaryTopology);
    }
    if member.role == ReplicaRole::ActiveSecondary {
        return Ok(BootstrapOwnership::LocalJoin);
    }
    Err(BootstrapOwnershipError::UnsupportedRole)
}

#[cfg(feature = "kuberic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapOwnershipError {
    LocalMemberMissing,
    UnsupportedRole,
}
