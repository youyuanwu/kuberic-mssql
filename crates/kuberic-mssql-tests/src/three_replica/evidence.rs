use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kuberic_mssql::instance::SqlServerInstanceManager;
use kuberic_mssql::observation::{
    AvailabilityGroupSnapshot, InstanceSnapshot, RecoveryLineageObservation,
};
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::tds::TdsExecutor;
use kuberic_mssql::{NativeRole, Observation};

use super::member::ReadyMember;
use super::model::TopologyRun;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaProfileEvidence {
    pub native_replica_id: String,
    pub server_name: String,
    pub endpoint_url: String,
    pub availability_mode: String,
    pub failover_mode: String,
    pub seeding_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseEvidence {
    pub name: String,
    pub group_database_id: String,
    pub local_database_id: u32,
    pub local_replica_id: String,
    pub database_guid: String,
    pub family_guid: String,
    pub recovery_fork_id: String,
    pub state: String,
    pub recovery_model: String,
    pub synchronization_state: String,
    pub synchronization_health: String,
    pub database_state: String,
    pub suspended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedingEvidence {
    pub group_database_id: String,
    pub remote_replica_id: String,
    pub operation_id: String,
    pub is_source: bool,
    pub current_state: Option<String>,
    pub performed_seeding: Option<bool>,
    pub failure_state: Option<i32>,
    pub error_code: Option<i32>,
    pub completion_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberEvidence {
    pub ordinal: u8,
    pub observed_at_unix_millis: u64,
    pub server_name: String,
    pub availability_group_name: String,
    pub availability_group_id: String,
    pub configuration_sequence: i64,
    pub cluster_type: String,
    pub required_synchronized_secondaries: u32,
    pub basic_features: bool,
    pub distributed: bool,
    pub local_replica_id: String,
    pub local_role: String,
    pub replica_profiles: Vec<ReplicaProfileEvidence>,
    pub database: DatabaseEvidence,
    pub automatic_seeding: Vec<SeedingEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedNativeEvidence {
    pub availability_group_name: String,
    pub availability_group_id: String,
    pub configuration_sequence: i64,
    pub database_name: String,
    pub group_database_id: String,
    pub database_guids: [String; 3],
    pub family_guid: String,
    pub recovery_fork_id: String,
    pub primary_ordinal: u8,
    pub members: [MemberEvidence; 3],
    pub seeding_operation_ids: [String; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceError {
    ObservationFailed,
    Stale,
    IdentityMismatch,
    SequenceMismatch,
    ProfileMismatch,
    RoleMismatch,
    DatabaseMismatch,
    Unsynchronized,
    Suspended,
    LineageMismatch,
    SeedingIncomplete,
    SeedingFailed,
}

impl EvidenceError {
    pub fn retryable(self) -> bool {
        !matches!(
            self,
            Self::IdentityMismatch
                | Self::ProfileMismatch
                | Self::LineageMismatch
                | Self::SeedingFailed
        )
    }
}

impl fmt::Display for EvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ObservationFailed => "direct production observation failed",
            Self::Stale => "direct production evidence is stale",
            Self::IdentityMismatch => "native availability-group identity does not match",
            Self::SequenceMismatch => "native availability-group sequence does not match",
            Self::ProfileMismatch => "native replica profile does not match",
            Self::RoleMismatch => "native local roles do not form one primary and two secondaries",
            Self::DatabaseMismatch => "native database identity or state does not match",
            Self::Unsynchronized => "native database is not synchronized and healthy",
            Self::Suspended => "native database participation is suspended",
            Self::LineageMismatch => "native database lineage does not match",
            Self::SeedingIncomplete => "automatic seeding has not completed",
            Self::SeedingFailed => "automatic seeding reported failure",
        })
    }
}

impl std::error::Error for EvidenceError {}

pub async fn observe_direct_members(
    run: &TopologyRun,
    members: &[ReadyMember; 3],
) -> Result<[MemberEvidence; 3], EvidenceError> {
    let mut observed = Vec::with_capacity(3);
    for (member, expected) in members.iter().zip(&run.members) {
        let config = ObserverConfig::read(&member.observer_config)
            .await
            .map_err(|_| EvidenceError::ObservationFailed)?;
        let manager =
            SqlServerInstanceManager::new(TdsExecutor::new(config.connection().clone()), config);
        let outer = manager
            .observe()
            .await
            .map_err(|_| EvidenceError::ObservationFailed)?;
        let snapshot = match outer {
            Observation::Present { value, .. } => value,
            Observation::Absent { .. } | Observation::Failed(_) => {
                return Err(EvidenceError::ObservationFailed);
            }
        };
        observed.push(extract_member_evidence(
            expected.ordinal,
            &snapshot,
            &format!("km_db_{}", run.run_id),
        )?);
    }
    observed
        .try_into()
        .map_err(|_| EvidenceError::ObservationFailed)
}

pub fn validate_native_evidence(
    run: &TopologyRun,
    evidence: [MemberEvidence; 3],
    now_unix_millis: u64,
    max_age: Duration,
) -> Result<ValidatedNativeEvidence, EvidenceError> {
    let expected_servers = run
        .members
        .iter()
        .map(|member| member.server_name.as_str())
        .collect::<BTreeSet<_>>();
    let expected_group_name = format!("km_ag_{}", run.run_id);
    let expected_database_name = format!("km_db_{}", run.run_id);
    let max_age_millis = u64::try_from(max_age.as_millis()).map_err(|_| EvidenceError::Stale)?;
    for member in &evidence {
        if member.observed_at_unix_millis > now_unix_millis
            || now_unix_millis - member.observed_at_unix_millis > max_age_millis
        {
            return Err(EvidenceError::Stale);
        }
        let expected = run
            .members
            .get(usize::from(member.ordinal.saturating_sub(1)))
            .ok_or(EvidenceError::IdentityMismatch)?;
        if member.server_name != expected.server_name
            || member.availability_group_name != expected_group_name
            || !member.cluster_type.eq_ignore_ascii_case("EXTERNAL")
            || member.required_synchronized_secondaries != 1
            || member.basic_features
            || member.distributed
        {
            return Err(EvidenceError::IdentityMismatch);
        }
        if member.replica_profiles.len() != 3
            || member
                .replica_profiles
                .iter()
                .map(|replica| replica.server_name.as_str())
                .collect::<BTreeSet<_>>()
                != expected_servers
            || member.replica_profiles.iter().any(|replica| {
                replica.availability_mode != "SYNCHRONOUS_COMMIT"
                    || replica.failover_mode != "EXTERNAL"
                    || replica.seeding_mode != "AUTOMATIC"
                    || replica.endpoint_url != format!("TCP://{}:5022", replica.server_name)
            })
        {
            return Err(EvidenceError::ProfileMismatch);
        }
        let local = member
            .replica_profiles
            .iter()
            .find(|replica| replica.server_name == member.server_name)
            .ok_or(EvidenceError::ProfileMismatch)?;
        if local.native_replica_id != member.local_replica_id {
            return Err(EvidenceError::IdentityMismatch);
        }
        if member.database.name != expected_database_name
            || member.database.local_replica_id != member.local_replica_id
            || member.database.state != "ONLINE"
            || member.database.recovery_model != "FULL"
            || member.database.database_state != "ONLINE"
        {
            return Err(EvidenceError::DatabaseMismatch);
        }
        if member.database.suspended {
            return Err(EvidenceError::Suspended);
        }
        if member.database.synchronization_state != "SYNCHRONIZED"
            || member.database.synchronization_health != "HEALTHY"
        {
            return Err(EvidenceError::Unsynchronized);
        }
    }

    let first = &evidence[0];
    let canonical_profiles = first
        .replica_profiles
        .iter()
        .map(|replica| {
            (
                replica.server_name.clone(),
                (
                    replica.native_replica_id.clone(),
                    replica.endpoint_url.clone(),
                    replica.availability_mode.clone(),
                    replica.failover_mode.clone(),
                    replica.seeding_mode.clone(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if evidence
        .iter()
        .any(|member| member.availability_group_id != first.availability_group_id)
    {
        return Err(EvidenceError::IdentityMismatch);
    }
    if evidence
        .iter()
        .any(|member| member.configuration_sequence != first.configuration_sequence)
    {
        return Err(EvidenceError::SequenceMismatch);
    }
    if evidence.iter().any(|member| {
        member
            .replica_profiles
            .iter()
            .map(|replica| {
                (
                    replica.server_name.clone(),
                    (
                        replica.native_replica_id.clone(),
                        replica.endpoint_url.clone(),
                        replica.availability_mode.clone(),
                        replica.failover_mode.clone(),
                        replica.seeding_mode.clone(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>()
            != canonical_profiles
            || member.database.group_database_id != first.database.group_database_id
    }) {
        return Err(EvidenceError::IdentityMismatch);
    }
    if evidence.iter().any(|member| {
        member.database.family_guid != first.database.family_guid
            || member.database.recovery_fork_id != first.database.recovery_fork_id
    }) {
        return Err(EvidenceError::LineageMismatch);
    }

    let primaries = evidence
        .iter()
        .filter(|member| member.local_role == "PRIMARY")
        .collect::<Vec<_>>();
    if primaries.len() != 1
        || evidence
            .iter()
            .filter(|member| member.local_role == "SECONDARY")
            .count()
            != 2
    {
        return Err(EvidenceError::RoleMismatch);
    }
    let primary = primaries[0];
    let secondary_ids = evidence
        .iter()
        .filter(|member| member.local_role == "SECONDARY")
        .map(|member| member.local_replica_id.as_str())
        .collect::<BTreeSet<_>>();
    if primary.automatic_seeding.iter().any(|seed| {
        secondary_ids.contains(seed.remote_replica_id.as_str())
            && (seed.current_state.as_deref() == Some("FAILED")
                || seed.performed_seeding == Some(false)
                || seed.failure_state.is_some_and(|value| value != 0)
                || seed.error_code.is_some_and(|value| value != 0))
    }) {
        return Err(EvidenceError::SeedingFailed);
    }
    let successful_seeds = primary
        .automatic_seeding
        .iter()
        .filter(|seed| {
            seed.group_database_id == first.database.group_database_id
                && secondary_ids.contains(seed.remote_replica_id.as_str())
                && seed.is_source
                && seed.current_state.as_deref() == Some("COMPLETED")
                && seed.performed_seeding == Some(true)
                && seed.failure_state.is_none_or(|value| value == 0)
                && seed.error_code.is_none_or(|value| value == 0)
                && seed.completion_time.is_some()
        })
        .collect::<Vec<_>>();
    if successful_seeds.len() != 2
        || successful_seeds
            .iter()
            .map(|seed| seed.remote_replica_id.as_str())
            .collect::<BTreeSet<_>>()
            != secondary_ids
    {
        return Err(EvidenceError::SeedingIncomplete);
    }
    let mut operation_ids = successful_seeds
        .iter()
        .map(|seed| seed.operation_id.clone())
        .collect::<Vec<_>>();
    operation_ids.sort();

    Ok(ValidatedNativeEvidence {
        availability_group_name: expected_group_name,
        availability_group_id: first.availability_group_id.clone(),
        configuration_sequence: first.configuration_sequence,
        database_name: expected_database_name,
        group_database_id: first.database.group_database_id.clone(),
        database_guids: evidence
            .iter()
            .map(|member| member.database.database_guid.clone())
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| EvidenceError::DatabaseMismatch)?,
        family_guid: first.database.family_guid.clone(),
        recovery_fork_id: first.database.recovery_fork_id.clone(),
        primary_ordinal: primary.ordinal,
        members: evidence,
        seeding_operation_ids: operation_ids
            .try_into()
            .map_err(|_| EvidenceError::SeedingIncomplete)?,
    })
}

pub fn unix_millis() -> Result<u64, EvidenceError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or(EvidenceError::Stale)
}

fn extract_member_evidence(
    ordinal: u8,
    snapshot: &InstanceSnapshot,
    expected_database_name: &str,
) -> Result<MemberEvidence, EvidenceError> {
    let group = match &snapshot.availability_group {
        Observation::Present { value, .. } => value,
        Observation::Absent { .. } | Observation::Failed(_) => {
            return Err(EvidenceError::ObservationFailed);
        }
    };
    extract_present_group(ordinal, snapshot, group, expected_database_name)
}

fn extract_present_group(
    ordinal: u8,
    snapshot: &InstanceSnapshot,
    group: &AvailabilityGroupSnapshot,
    expected_database_name: &str,
) -> Result<MemberEvidence, EvidenceError> {
    let database = group
        .databases
        .iter()
        .find(|database| database.identity.name.as_str() == expected_database_name)
        .ok_or(EvidenceError::DatabaseMismatch)?;
    if group.databases.len() != 1 {
        return Err(EvidenceError::DatabaseMismatch);
    }
    let local = database
        .local
        .as_ref()
        .ok_or(EvidenceError::DatabaseMismatch)?;
    let recovery = local
        .recovery
        .as_ref()
        .ok_or(EvidenceError::LineageMismatch)?;
    let state = database
        .replicas
        .iter()
        .find(|state| {
            group
                .local_replica
                .identity
                .native_replica_id()
                .is_some_and(|identity| state.replica_id == *identity)
        })
        .ok_or(EvidenceError::DatabaseMismatch)?;
    let lineage = match &state.lineage {
        RecoveryLineageObservation::Local { value } => value,
        RecoveryLineageObservation::LocalUnavailable
        | RecoveryLineageObservation::RemoteUnavailable => {
            return Err(EvidenceError::LineageMismatch);
        }
    };
    Ok(MemberEvidence {
        ordinal,
        observed_at_unix_millis: snapshot.observed_at_unix_millis,
        server_name: snapshot.instance.server_name.as_str().to_owned(),
        availability_group_name: group.identity.name.as_str().to_owned(),
        availability_group_id: group.identity.group_id.as_str().to_owned(),
        configuration_sequence: group.configuration_sequence.value(),
        cluster_type: group.cluster_type.clone(),
        required_synchronized_secondaries: group.required_synchronized_secondaries_to_commit,
        basic_features: group.basic_features,
        distributed: group.is_distributed,
        local_replica_id: group
            .local_replica
            .identity
            .native_replica_id()
            .ok_or(EvidenceError::IdentityMismatch)?
            .as_str()
            .to_owned(),
        local_role: role_name(group.local_replica.role.as_ref())?.to_owned(),
        replica_profiles: group
            .replicas
            .iter()
            .map(|replica| {
                Ok(ReplicaProfileEvidence {
                    native_replica_id: replica.replica_id.as_str().to_owned(),
                    server_name: replica.server_name.as_str().to_owned(),
                    endpoint_url: replica
                        .endpoint_url
                        .clone()
                        .ok_or(EvidenceError::ProfileMismatch)?,
                    availability_mode: replica.availability_mode.clone(),
                    failover_mode: replica.failover_mode.clone(),
                    seeding_mode: replica.seeding_mode.clone(),
                })
            })
            .collect::<Result<Vec<_>, EvidenceError>>()?,
        database: DatabaseEvidence {
            name: database.identity.name.as_str().to_owned(),
            group_database_id: database.identity.group_database_id.as_str().to_owned(),
            local_database_id: local.database_id,
            local_replica_id: local.replica_id.as_str().to_owned(),
            database_guid: recovery
                .database_guid
                .as_ref()
                .ok_or(EvidenceError::LineageMismatch)?
                .as_str()
                .to_owned(),
            family_guid: recovery
                .family_guid
                .as_ref()
                .ok_or(EvidenceError::LineageMismatch)?
                .as_str()
                .to_owned(),
            recovery_fork_id: lineage.recovery_fork_id.as_str().to_owned(),
            state: local.state.clone().ok_or(EvidenceError::DatabaseMismatch)?,
            recovery_model: local
                .recovery_model
                .clone()
                .ok_or(EvidenceError::DatabaseMismatch)?,
            synchronization_state: state
                .synchronization_state
                .clone()
                .ok_or(EvidenceError::Unsynchronized)?,
            synchronization_health: state
                .synchronization_health
                .clone()
                .ok_or(EvidenceError::Unsynchronized)?,
            database_state: state
                .database_state
                .clone()
                .ok_or(EvidenceError::DatabaseMismatch)?,
            suspended: state.is_suspended.ok_or(EvidenceError::Suspended)?,
        },
        automatic_seeding: group
            .automatic_seeding
            .iter()
            .map(|seed| SeedingEvidence {
                group_database_id: seed.group_database_id.as_str().to_owned(),
                remote_replica_id: seed.remote_replica_id.as_str().to_owned(),
                operation_id: seed.operation_id.as_str().to_owned(),
                is_source: seed.is_source,
                current_state: seed.current_state.clone(),
                performed_seeding: seed.performed_seeding,
                failure_state: seed.failure_state,
                error_code: seed.error_code,
                completion_time: seed.completion_time.clone(),
            })
            .collect(),
    })
}

fn role_name(role: Option<&NativeRole>) -> Result<&'static str, EvidenceError> {
    match role {
        Some(NativeRole::Primary) => Ok("PRIMARY"),
        Some(NativeRole::Secondary) => Ok("SECONDARY"),
        _ => Err(EvidenceError::RoleMismatch),
    }
}
