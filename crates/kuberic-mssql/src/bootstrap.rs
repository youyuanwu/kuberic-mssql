#![cfg_attr(not(feature = "kuberic-testing"), allow(dead_code))]

use crate::observation::{AvailabilityGroupSnapshot, NativeProvenance, RecoveryLineageObservation};
use crate::{
    AvailabilityGroupIdentity, AvailabilityGroupName, DatabaseIdentity, DatabaseLineage, Guid,
    NativeRole, ReplicaDescriptor, ReplicaIdentity, ServerName, SqlIdentifier,
};

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
pub struct AvailabilityGroupOwnership {
    pub resource_id: String,
    pub configuration_id: String,
    pub name: AvailabilityGroupName,
    /// Identity captured atomically by the scoped server DDL trigger that
    /// authorizes exactly one application-created availability group.
    pub creation_group_id: Option<Guid>,
    /// `None` is the durable pre-create intent. A bound marker always freezes
    /// the SQL-generated availability-group identity.
    pub group_id: Option<Guid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneDatabaseSnapshot {
    pub identity: Option<StandaloneDatabaseIdentity>,
    /// Token written atomically by the scoped CREATE_DATABASE DDL trigger.
    pub creation_token: Option<String>,
    pub ownership: Option<StandaloneDatabaseOwnership>,
    pub availability_group_ownership: Option<AvailabilityGroupOwnership>,
    pub state: String,
    pub recovery_model: String,
    pub accessible: bool,
    pub marker_table_ready: bool,
    pub full_backup_ready: bool,
    pub log_chain_ready: bool,
    pub backup_verified: bool,
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
    let Some(actual) = &snapshot.identity else {
        return NativeConvergenceState::Incomplete;
    };
    let expected_token = source_database_creation_token(&actual.name, expected_ownership);
    if snapshot.creation_token.as_deref() != Some(expected_token.as_str()) {
        return NativeConvergenceState::Conflicting;
    }
    if expected.is_some_and(|expected| actual != expected) {
        return NativeConvergenceState::Conflicting;
    }
    if snapshot
        .ownership
        .as_ref()
        .is_some_and(|actual| actual != expected_ownership)
    {
        return NativeConvergenceState::Conflicting;
    }
    if snapshot.state == "ONLINE"
        && snapshot.recovery_model == "FULL"
        && snapshot.ownership.as_ref() == Some(expected_ownership)
        && snapshot.marker_table_ready
        && snapshot.full_backup_ready
        && snapshot.log_chain_ready
        && snapshot.backup_verified
    {
        NativeConvergenceState::Complete
    } else {
        NativeConvergenceState::Incomplete
    }
}

pub fn source_database_creation_token(
    database: &SqlIdentifier,
    ownership: &StandaloneDatabaseOwnership,
) -> String {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    digest.update(b"kuberic-mssql-source-database-v1\0");
    digest.update(ownership.resource_id.as_bytes());
    digest.update(b"\0");
    digest.update(ownership.configuration_id.as_bytes());
    digest.update(b"\0");
    digest.update(database.as_str().as_bytes());
    hex(&digest.finalize())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[usize::from(byte >> 4)] as char);
        value.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    value
}

pub fn classify_owned_availability_group(
    group: Option<&AvailabilityGroupSnapshot>,
    ownership: Option<&AvailabilityGroupOwnership>,
    expected_ownership: &AvailabilityGroupOwnership,
    expected_replicas: &[ReplicaDescriptor],
    expected_database_name: &SqlIdentifier,
    expected_database: Option<&StandaloneDatabaseIdentity>,
    unsafe_state: bool,
) -> NativeConvergenceState {
    if unsafe_state {
        return NativeConvergenceState::Unsafe;
    }
    let Some(group) = group else {
        return match ownership {
            None => NativeConvergenceState::Absent,
            Some(actual) if actual == expected_ownership => NativeConvergenceState::Incomplete,
            Some(_) => NativeConvergenceState::Conflicting,
        };
    };
    let Some(ownership) = ownership else {
        return NativeConvergenceState::Conflicting;
    };
    if ownership.resource_id != expected_ownership.resource_id
        || ownership.configuration_id != expected_ownership.configuration_id
        || ownership.name != expected_ownership.name
        || expected_ownership.group_id.is_some()
    {
        return NativeConvergenceState::Conflicting;
    }
    if ownership.creation_group_id.as_ref() != Some(&group.identity.group_id) {
        return NativeConvergenceState::Conflicting;
    }
    let Some(owned_group_id) = ownership.group_id.as_ref() else {
        return NativeConvergenceState::Incomplete;
    };
    classify_availability_group(
        Some(group),
        &expected_ownership.name,
        Some(owned_group_id),
        expected_replicas,
        expected_database_name,
        expected_database,
        false,
    )
}

pub fn classify_availability_group(
    group: Option<&AvailabilityGroupSnapshot>,
    expected_name: &AvailabilityGroupName,
    expected_group_id: Option<&Guid>,
    expected_replicas: &[ReplicaDescriptor],
    expected_database_name: &SqlIdentifier,
    expected_database: Option<&StandaloneDatabaseIdentity>,
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
        || !group.cluster_type.eq_ignore_ascii_case("EXTERNAL")
        || group.required_synchronized_secondaries_to_commit != 1
        || group.basic_features
        || group.is_distributed
        || group.replicas.len() > expected_replicas.len()
        || group.databases.len() > 1
    {
        return NativeConvergenceState::Conflicting;
    }
    let mut incomplete = group.replicas.len() != expected_replicas.len();
    if let Some(database) = group.databases.first() {
        let Some(expected_database) = expected_database else {
            return NativeConvergenceState::Conflicting;
        };
        if database.identity.name != *expected_database_name {
            return NativeConvergenceState::Conflicting;
        }
        let Some(local) = &database.local else {
            return NativeConvergenceState::Incomplete;
        };
        let Some(recovery) = &local.recovery else {
            return NativeConvergenceState::Incomplete;
        };
        if recovery.database_guid.as_ref() != Some(&expected_database.database_guid)
            || recovery.family_guid.as_ref() != Some(&expected_database.family_guid)
            || recovery.recovery_fork_guid.as_ref() != Some(&expected_database.recovery_fork_guid)
        {
            return NativeConvergenceState::Conflicting;
        }
    }
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
    // Automatic-seeding DMV rows describe individual processes. A failed
    // process can precede a later retry, while this target-local native state
    // is the terminal postcondition for the exact database and lineage.
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
