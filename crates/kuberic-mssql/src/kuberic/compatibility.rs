use std::sync::Arc;

use kuberic_runtime::protocol::types::{
    ConfigurationDescriptor, ProcessSessionId, ReplicaIdentity as KubericReplicaIdentity,
    ReplicaRole,
};
use kuberic_runtime::replicator::{ReplicaInformation, ReplicaSetConfiguration};

use super::{HealthyTopologyBinding, HealthyTopologyMemberBinding, KubericAdapterError};
use crate::runtime_host::seed_state::SeedDispatchTarget;
use crate::topology_config::SqlServerTopologyExpectation;
use crate::{
    Endpoint, ReplicaDescriptor, ReplicaIdentity as SqlReplicaIdentity, SUPPORTED_REPLICA_COUNT,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapOwnership {
    PrimaryTopology,
    LocalJoin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapOwnershipError {
    LocalMemberMissing,
    UnsupportedRole,
}

#[derive(Debug, Clone)]
pub(super) struct CurrentMemberFacts {
    pub(super) identity: KubericReplicaIdentity,
    pub(super) process_session_id: ProcessSessionId,
    pub(super) replication_address: String,
    pub(super) role: ReplicaRole,
    described_role: ReplicaRole,
}

#[derive(Debug, Clone)]
pub(super) struct CurrentTopologyFacts {
    pub(super) configuration: ConfigurationDescriptor,
    pub(super) members: Vec<CurrentMemberFacts>,
}

impl CurrentTopologyFacts {
    pub(super) fn translate(
        runtime_identity: &KubericReplicaIdentity,
        process_session_id: &ProcessSessionId,
        expectation: &SqlServerTopologyExpectation,
        replication_address: &str,
        current: &ReplicaSetConfiguration,
    ) -> Result<Self, KubericAdapterError> {
        validate_configuration_shape(
            runtime_identity,
            process_session_id,
            expectation,
            replication_address,
            current,
        )?;
        Ok(Self {
            configuration: current.configuration.clone(),
            members: current
                .configuration
                .members
                .iter()
                .map(|member| {
                    let description = current
                        .replicas
                        .iter()
                        .find(|replica| replica.identity == member.identity);
                    CurrentMemberFacts {
                        identity: member.identity.clone(),
                        process_session_id: description.map_or_else(
                            || process_session_id.clone(),
                            |description| description.process_session_id.clone(),
                        ),
                        replication_address: description.map_or_else(
                            || replication_address.to_owned(),
                            |description| description.replication_address.clone(),
                        ),
                        role: member.role,
                        described_role: description
                            .map_or(member.role, |description| description.role),
                    }
                })
                .collect(),
        })
    }

    pub(super) fn bootstrap_ownership(
        &self,
        local: &KubericReplicaIdentity,
    ) -> Result<BootstrapOwnership, BootstrapOwnershipError> {
        classify_bootstrap_ownership_from_facts(local, &self.configuration, &self.members)
    }

    pub(super) fn bootstrap_descriptors(
        &self,
        expectation: &SqlServerTopologyExpectation,
    ) -> Result<Vec<ReplicaDescriptor>, KubericAdapterError> {
        expectation
            .members()
            .iter()
            .map(|member| {
                let configured = self
                    .members
                    .iter()
                    .find(|configured| configured.identity.replica_id == member.replica_id())
                    .ok_or(KubericAdapterError::TopologyBindingMismatch(
                        "bootstrap member has no current configuration identity",
                    ))?;
                let endpoint = member
                    .endpoint_url()
                    .get(6..)
                    .and_then(|value| value.rsplit_once(':'))
                    .ok_or(KubericAdapterError::InvalidConfiguration(
                        "bootstrap SQL endpoint is invalid",
                    ))?;
                if !member.endpoint_url()[..6].eq_ignore_ascii_case("tcp://")
                    || !endpoint
                        .0
                        .eq_ignore_ascii_case(member.server_name().as_str())
                {
                    return Err(KubericAdapterError::InvalidConfiguration(
                        "bootstrap SQL endpoint must name the configured SQL member",
                    ));
                }
                let port = endpoint
                    .1
                    .parse::<u16>()
                    .ok()
                    .filter(|port| *port != 0)
                    .ok_or(KubericAdapterError::InvalidConfiguration(
                        "bootstrap SQL endpoint port is invalid",
                    ))?;
                Ok(ReplicaDescriptor {
                    identity: SqlReplicaIdentity::desired(
                        configured.identity.replica_id.to_string(),
                        configured.identity.agent_generation.to_string(),
                    )
                    .map_err(|_| {
                        KubericAdapterError::InvalidConfiguration(
                            "bootstrap SQL replica identity is invalid",
                        )
                    })?,
                    server_name: member.server_name().clone(),
                    endpoint: Endpoint::new(member.server_name().as_str(), port).map_err(|_| {
                        KubericAdapterError::InvalidConfiguration(
                            "bootstrap SQL endpoint is invalid",
                        )
                    })?,
                })
            })
            .collect()
    }

    pub(super) fn validate_binding(
        &self,
        binding: &HealthyTopologyBinding,
    ) -> Result<(), KubericAdapterError> {
        if self.configuration != binding.configuration
            || self.members.len() != binding.members.len()
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica descriptions are incomplete",
            ));
        }
        for member in &binding.members {
            let matching = self
                .members
                .iter()
                .filter(|candidate| candidate.identity == member.kuberic_identity)
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "current replica identities differ from the frozen members",
                ));
            }
            let current = matching[0];
            if current.process_session_id != member.process_session_id
                || current.replication_address != member.replication_address
                || (!matches!(current.described_role, ReplicaRole::None)
                    && current.described_role != member.stable_role)
            {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "current replica session, address, or role differs from the frozen member",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "kuberic-testing")]
pub fn classify_bootstrap_ownership(
    local: &KubericReplicaIdentity,
    configuration: &ReplicaSetConfiguration,
) -> Result<BootstrapOwnership, BootstrapOwnershipError> {
    let members = configuration
        .configuration
        .members
        .iter()
        .map(|member| CurrentMemberFacts {
            identity: member.identity.clone(),
            process_session_id: ProcessSessionId::default(),
            replication_address: String::new(),
            role: member.role,
            described_role: ReplicaRole::None,
        })
        .collect::<Vec<_>>();
    classify_bootstrap_ownership_from_facts(local, &configuration.configuration, &members)
}

fn classify_bootstrap_ownership_from_facts(
    local: &KubericReplicaIdentity,
    configuration: &ConfigurationDescriptor,
    members: &[CurrentMemberFacts],
) -> Result<BootstrapOwnership, BootstrapOwnershipError> {
    let member = members
        .iter()
        .find(|member| member.identity == *local)
        .ok_or(BootstrapOwnershipError::LocalMemberMissing)?;
    if member.identity.replica_id == configuration.primary_id && member.role == ReplicaRole::Primary
    {
        return Ok(BootstrapOwnership::PrimaryTopology);
    }
    if member.role == ReplicaRole::ActiveSecondary {
        return Ok(BootstrapOwnership::LocalJoin);
    }
    Err(BootstrapOwnershipError::UnsupportedRole)
}

pub(super) fn normalize_bootstrap_configuration(
    current: ReplicaSetConfiguration,
) -> Result<ReplicaSetConfiguration, KubericAdapterError> {
    if current.replicas.len() <= usize::from(SUPPORTED_REPLICA_COUNT) {
        return Ok(current);
    }
    let mut base = Vec::with_capacity(current.configuration.members.len());
    let mut described = 0;
    for member in &current.configuration.members {
        let matches = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == member.identity)
            .collect::<Vec<_>>();
        described += matches.len();
        let base_descriptions = matches
            .iter()
            .filter(|replica| replica.build_id.is_empty())
            .collect::<Vec<_>>();
        if base_descriptions.len() != 1
            || matches.iter().any(|replica| {
                !replica.build_id.is_empty()
                    && (member.role != ReplicaRole::ActiveSecondary
                        || !matches!(
                            replica.role,
                            ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary
                        ))
            })
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "bootstrap build-enriched configuration is ambiguous",
            ));
        }
        base.push((**base_descriptions.first().expect("one base description")).clone());
    }
    if described != current.replicas.len()
        || current.replicas.len() > current.configuration.members.len() + 2
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "bootstrap build-enriched configuration has unknown descriptions",
        ));
    }
    Ok(ReplicaSetConfiguration {
        configuration: current.configuration,
        replicas: base,
    })
}

pub(super) fn validate_incomplete_bootstrap_configuration(
    runtime_identity: &KubericReplicaIdentity,
    process_session_id: &ProcessSessionId,
    expectation: &SqlServerTopologyExpectation,
    replication_address: &str,
    current: &ReplicaSetConfiguration,
) -> Result<bool, KubericAdapterError> {
    if current.replicas.len() >= usize::from(SUPPORTED_REPLICA_COUNT) {
        return Ok(false);
    }
    let mut normalized = current.clone();
    for member in &current.configuration.members {
        if normalized
            .replicas
            .iter()
            .any(|replica| replica.identity == member.identity)
        {
            continue;
        }
        let mut replica = ReplicaInformation::new(
            kuberic_runtime::protocol::types::OperationId::default(),
            member.identity.clone(),
            if member.identity == *runtime_identity {
                replication_address.to_owned()
            } else {
                String::new()
            },
        );
        replica.role = member.role;
        normalized.replicas.push(replica);
    }
    validate_configuration_shape(
        runtime_identity,
        process_session_id,
        expectation,
        replication_address,
        &normalized,
    )?;
    Ok(true)
}

pub(super) fn validate_bootstrap_configuration_replay(
    binding: &HealthyTopologyBinding,
    current: &ReplicaSetConfiguration,
) -> Result<(), KubericAdapterError> {
    validate_configuration_replay(binding, current, true)
}

fn validate_configuration_replay(
    binding: &HealthyTopologyBinding,
    current: &ReplicaSetConfiguration,
    allow_remote_omissions: bool,
) -> Result<(), KubericAdapterError> {
    if current.configuration != binding.configuration {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current descriptor differs from the frozen descriptor",
        ));
    }
    if current.replicas.len() != binding.members.len() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current replica descriptions are incomplete",
        ));
    }
    for member in &binding.members {
        let matching = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == member.kuberic_identity)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica identities differ from the frozen members",
            ));
        }
        let replica = matching[0];
        let remote_omission =
            allow_remote_omissions && member.kuberic_identity != binding.local_identity;
        if (replica.process_session_id != member.process_session_id
            && !(remote_omission && replica.process_session_id.is_empty()))
            || (replica.replication_address != member.replication_address
                && !(remote_omission && replica.replication_address.is_empty()))
            || (!matches!(replica.role, ReplicaRole::None) && replica.role != member.stable_role)
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica session, address, or role differs from the frozen member",
            ));
        }
    }
    Ok(())
}

pub(super) fn build_enriched_configuration(
    binding: &HealthyTopologyBinding,
    current: &ReplicaSetConfiguration,
) -> Result<bool, KubericAdapterError> {
    if current.configuration != binding.configuration {
        return Ok(false);
    }
    if current.replicas.len() <= binding.members.len() {
        return Ok(false);
    }
    if current.replicas.len() > binding.members.len() + 2 {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "build-enriched current configuration has an invalid description count",
        ));
    }
    let mut base = Vec::with_capacity(binding.members.len());
    let mut builds = Vec::new();
    for member in &binding.members {
        let matches = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == member.kuberic_identity)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [only] => base.push((*only).clone()),
            [first, second] => {
                let (base_replica, build) =
                    match (first.build_id.is_empty(), second.build_id.is_empty()) {
                        (true, false) => (*first, *second),
                        (false, true) => (*second, *first),
                        _ => {
                            return Err(KubericAdapterError::TopologyBindingMismatch(
                                "build-enriched target descriptions are ambiguous",
                            ));
                        }
                    };
                base.push(base_replica.clone());
                builds.push(build.clone());
            }
            _ => {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "build-enriched current configuration has invalid member descriptions",
                ));
            }
        }
    }
    if builds.is_empty() {
        return Ok(false);
    }
    if base.len() + builds.len() != current.replicas.len() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "build-enriched current configuration contains an unknown replica",
        ));
    }
    validate_bootstrap_configuration_replay(
        binding,
        &ReplicaSetConfiguration {
            configuration: current.configuration.clone(),
            replicas: base,
        },
    )?;
    for build in builds {
        let target = binding
            .members
            .iter()
            .find(|member| member.kuberic_identity == build.identity)
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "build-enriched current configuration has an unknown target",
            ))?;
        if target.stable_role != ReplicaRole::ActiveSecondary
            || build.process_session_id != target.process_session_id
            || (!build.replication_address.is_empty()
                && build.replication_address != target.replication_address)
            || !matches!(
                build.role,
                ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary
            )
            || build.current_progress < 0
            || build.catch_up_capability < 0
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "build-enriched current configuration differs from the admitted target",
            ));
        }
    }
    Ok(true)
}

pub(super) fn configuration_enriches(
    previous: &ReplicaSetConfiguration,
    current: &ReplicaSetConfiguration,
) -> bool {
    if previous.configuration != current.configuration
        || previous.replicas.len() != current.replicas.len()
    {
        return false;
    }

    previous.replicas.iter().all(|old| {
        let matching = current
            .replicas
            .iter()
            .filter(|new| new.identity == old.identity)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return false;
        }
        let new = matching[0];
        (old.process_session_id.is_empty() || old.process_session_id == new.process_session_id)
            && (old.replication_address.is_empty()
                || old.replication_address == new.replication_address)
            && (matches!(old.role, ReplicaRole::None) || old.role == new.role)
            && old.current_progress == new.current_progress
            && old.catch_up_capability == new.catch_up_capability
    })
}

pub(super) fn configuration_is_complete(configuration: &ReplicaSetConfiguration) -> bool {
    configuration.replicas.iter().all(|replica| {
        !replica.process_session_id.is_empty()
            && validate_replication_address(&replica.replication_address).is_ok()
            && !matches!(replica.role, ReplicaRole::None)
    })
}

pub(super) fn topology_enriches(
    previous: &HealthyTopologyBinding,
    current: &HealthyTopologyBinding,
) -> bool {
    if previous.local_identity != current.local_identity
        || previous.configuration != current.configuration
        || (previous.database_lineage.is_some()
            && previous.native_configuration_sequence != current.native_configuration_sequence)
        || previous.availability_group != current.availability_group
        || previous
            .database_lineage
            .as_ref()
            .is_some_and(|lineage| current.database_lineage.as_ref() != Some(lineage))
        || previous.bootstrap_database_name != current.bootstrap_database_name
        || previous
            .bootstrap_database
            .as_ref()
            .is_some_and(|database| current.bootstrap_database.as_ref() != Some(database))
        || previous.local_sql_replica_identity != current.local_sql_replica_identity
        || previous.local_sql_server_start_incarnation != current.local_sql_server_start_incarnation
        || previous.members.len() != current.members.len()
    {
        return false;
    }
    previous.members.iter().all(|old| {
        let matching = current
            .members
            .iter()
            .filter(|new| new.kuberic_identity == old.kuberic_identity)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return false;
        }
        let new = matching[0];
        old.native_replica_id == new.native_replica_id
            && old.sql_endpoint == new.sql_endpoint
            && old.stable_role == new.stable_role
            && (old.process_session_id.is_empty()
                || old.process_session_id == new.process_session_id)
            && (old.replication_address.is_empty()
                || old.replication_address == new.replication_address)
    })
}

pub(super) struct CurrentBuildFacts {
    pub(super) source: SeedDispatchTarget,
    pub(super) targets: [SeedDispatchTarget; 2],
    pub(super) target: HealthyTopologyMemberBinding,
    pub(super) winning_target_replica_id: String,
    pub(super) build_id: String,
}

impl CurrentBuildFacts {
    pub(super) fn translate(
        binding: &Arc<HealthyTopologyBinding>,
        replica: &ReplicaInformation,
        published_role: Option<ReplicaRole>,
    ) -> Result<Self, KubericAdapterError> {
        replica.validate_public_progress().map_err(|_| {
            KubericAdapterError::TopologyBindingMismatch(
                "target build must be idle with INVALID_LSN progress",
            )
        })?;
        let storage = replica.storage_incarnation.as_ref().ok_or(
            KubericAdapterError::TopologyBindingMismatch(
                "target build lacks an exact storage incarnation",
            ),
        )?;
        storage.validate().map_err(|_| {
            KubericAdapterError::TopologyBindingMismatch(
                "target build storage incarnation is incomplete",
            )
        })?;
        if storage.replica != replica.identity {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "target build storage incarnation differs from its identity",
            ));
        }
        let source = binding.local_member();
        if source.kuberic_identity.replica_id != binding.configuration.primary_id
            || source.stable_role != ReplicaRole::Primary
            || published_role != Some(ReplicaRole::Primary)
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "build-owned seeding requires the exact configured primary",
            ));
        }
        let target = binding
            .members
            .iter()
            .find(|member| member.kuberic_identity == replica.identity)
            .ok_or(KubericAdapterError::TopologyBindingMismatch(
                "target build identity is not admitted",
            ))?;
        if target.stable_role != ReplicaRole::ActiveSecondary
            || target.process_session_id.is_empty()
            || replica.process_session_id != target.process_session_id
            || replica.replication_address != target.replication_address
            || replica.role != ReplicaRole::IdleSecondary
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "target build session, address, or role differs from the admitted configuration",
            ));
        }
        let source = seed_member(source)?;
        let targets: [SeedDispatchTarget; 2] = binding
            .members
            .iter()
            .filter(|member| member.stable_role == ReplicaRole::ActiveSecondary)
            .map(seed_member)
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| {
                KubericAdapterError::TopologyBindingMismatch(
                    "seed dispatch requires exactly two configured targets",
                )
            })?;
        if targets
            .iter()
            .any(|target| target.process_session_id().is_empty())
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "both configured target sessions must be current before seed dispatch",
            ));
        }
        Ok(Self {
            source,
            targets,
            target: target.clone(),
            winning_target_replica_id: replica.identity.replica_id.to_string(),
            build_id: replica.build_id.to_string(),
        })
    }
}

fn seed_member(
    member: &HealthyTopologyMemberBinding,
) -> Result<SeedDispatchTarget, KubericAdapterError> {
    SeedDispatchTarget::new(
        member.kuberic_identity.replica_id.to_string(),
        member.kuberic_identity.instance_id.to_string(),
        member.kuberic_identity.agent_generation.to_string(),
        member.process_session_id.to_string(),
        member.native_replica_id.as_str(),
    )
    .map_err(KubericAdapterError::SeedState)
}

pub(super) fn validate_admitted_build_configuration(
    binding: &HealthyTopologyBinding,
    admitted_configuration: &ReplicaSetConfiguration,
) -> Result<(), KubericAdapterError> {
    if admitted_configuration.configuration != binding.configuration {
        Err(KubericAdapterError::TopologyBindingMismatch(
            "admitted build configuration differs from the topology binding",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn validate_build_id(replica: &ReplicaInformation) -> Result<(), KubericAdapterError> {
    if replica.build_id.is_empty() {
        Err(KubericAdapterError::TopologyBindingMismatch(
            "target build identity is empty",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn validate_replication_address(address: &str) -> Result<(), KubericAdapterError> {
    if address.is_empty()
        || address.len() > 1024
        || address.chars().any(char::is_control)
        || address.trim() != address
    {
        return Err(KubericAdapterError::InvalidConfiguration(
            "replication address must be nonempty, bounded, and contain no control or surrounding whitespace",
        ));
    }
    Ok(())
}

fn validate_configuration_shape(
    runtime_identity: &KubericReplicaIdentity,
    process_session_id: &ProcessSessionId,
    expectation: &SqlServerTopologyExpectation,
    replication_address: &str,
    current: &ReplicaSetConfiguration,
) -> Result<(), KubericAdapterError> {
    let configuration = &current.configuration;
    if configuration.configuration_id != configuration.expected_id()
        || configuration.members.len() != usize::from(SUPPORTED_REPLICA_COUNT)
        || !matches!(current.replicas.len(), 2 | 3)
        || configuration.write_quorum != 2
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current configuration requires exactly three members and quorum two",
        ));
    }
    let primary_count = configuration
        .members
        .iter()
        .filter(|member| member.role == ReplicaRole::Primary)
        .count();
    let secondary_count = configuration
        .members
        .iter()
        .filter(|member| member.role == ReplicaRole::ActiveSecondary)
        .count();
    if primary_count != 1
        || secondary_count != 2
        || configuration.members.iter().any(|member| {
            !matches!(
                member.role,
                ReplicaRole::Primary | ReplicaRole::ActiveSecondary
            )
        })
        || configuration
            .members
            .iter()
            .find(|member| member.role == ReplicaRole::Primary)
            .is_none_or(|member| member.identity.replica_id != configuration.primary_id)
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current configuration requires one Primary and two ActiveSecondary members",
        ));
    }
    if configuration
        .members
        .iter()
        .enumerate()
        .any(|(index, member)| {
            configuration.members[..index].iter().any(|other| {
                other.identity == member.identity
                    || other.identity.replica_id == member.identity.replica_id
            })
        })
        || expectation.members().iter().any(|expected| {
            !configuration
                .members
                .iter()
                .any(|member| member.identity.replica_id == expected.replica_id())
        })
    {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current member identities differ from the topology expectation",
        ));
    }
    let local = configuration
        .members
        .iter()
        .find(|member| member.identity == *runtime_identity)
        .ok_or(KubericAdapterError::TopologyBindingMismatch(
            "current configuration does not include the runtime identity",
        ))?;
    if local.identity.replica_id != expectation.local_replica_id() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "runtime identity differs from the local topology association",
        ));
    }
    if process_session_id.is_empty() {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "runtime process session is empty",
        ));
    }

    for configuration_member in &configuration.members {
        if configuration_member.identity.instance_id.is_empty()
            || configuration_member.identity.agent_generation.is_empty()
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current member identities must be complete",
            ));
        }
        let descriptions = current
            .replicas
            .iter()
            .filter(|replica| replica.identity == configuration_member.identity)
            .collect::<Vec<_>>();
        let is_local = configuration_member.identity == *runtime_identity;
        if descriptions.len() != usize::from(!is_local || current.replicas.len() == 3) {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica identity differs from the descriptor",
            ));
        }
        if descriptions.is_empty() {
            if configuration_member.role != ReplicaRole::Primary {
                return Err(KubericAdapterError::TopologyBindingMismatch(
                    "only the local Primary may be omitted from remote-active descriptions",
                ));
            }
            continue;
        }
        if !descriptions[0].replication_address.is_empty()
            && validate_replication_address(&descriptions[0].replication_address).is_err()
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica address is invalid",
            ));
        }
        if !matches!(descriptions[0].role, ReplicaRole::None)
            && descriptions[0].role != configuration_member.role
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "current replica role differs from the descriptor",
            ));
        }
        if configuration_member.identity == *runtime_identity
            && descriptions[0].replication_address != replication_address
        {
            return Err(KubericAdapterError::TopologyBindingMismatch(
                "local replication address differs from the runtime",
            ));
        }
    }
    if current.replicas.iter().enumerate().any(|(index, replica)| {
        current.replicas[..index].iter().any(|other| {
            other.identity == replica.identity
                || (!replica.process_session_id.is_empty()
                    && other.process_session_id == replica.process_session_id)
                || (!replica.replication_address.is_empty()
                    && other.replication_address == replica.replication_address)
        })
    }) {
        return Err(KubericAdapterError::TopologyBindingMismatch(
            "current identities, sessions, and addresses must be unique",
        ));
    }
    Ok(())
}

#[cfg(feature = "kuberic-testing")]
#[doc(hidden)]
pub fn testing_current_pin_bootstrap_descriptors(
    runtime_identity: &KubericReplicaIdentity,
    expectation: &SqlServerTopologyExpectation,
    replication_address: &str,
    current: &ReplicaSetConfiguration,
) -> Result<Vec<ReplicaDescriptor>, KubericAdapterError> {
    CurrentTopologyFacts::translate(
        runtime_identity,
        &ProcessSessionId::new("testing-local-process-session"),
        expectation,
        replication_address,
        current,
    )?
    .bootstrap_descriptors(expectation)
}
