#![cfg_attr(not(feature = "kuberic-testing"), allow(dead_code))]

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use crate::bootstrap::StandaloneDatabaseSnapshot;
use crate::runtime_error::RuntimeError;
use crate::{
    AvailabilityGroupName, OperationPayload, OperationRequest, ReplicaDescriptor,
    SUPPORTED_REPLICA_COUNT, SqlIdentifier,
};

pub const STANDALONE_DATABASE_QUERY: &str = r#"
SELECT
    CONVERT(nvarchar(60), d.state_desc) AS state_desc,
    CONVERT(nvarchar(60), d.recovery_model_desc) AS recovery_model_desc,
    CONVERT(nvarchar(36), drs.database_guid) AS database_guid,
    CONVERT(nvarchar(36), drs.family_guid) AS family_guid,
    CONVERT(nvarchar(36), drs.recovery_fork_guid) AS recovery_fork_guid
FROM sys.databases AS d
LEFT JOIN sys.database_recovery_status AS drs
    ON drs.database_id = d.database_id
WHERE d.name = @P1;"#;

#[allow(dead_code)]
pub(crate) const DATABASE_OWNERSHIP_QUERY: &str = r#"
SELECT
    CONVERT(nvarchar(512), ep_resource.value) AS kuberic_resource_id,
    CONVERT(nvarchar(512), ep_configuration.value) AS kuberic_configuration_id,
    CONVERT(nvarchar(512), ep_ag_resource.value) AS kuberic_ag_resource_id,
    CONVERT(nvarchar(512), ep_ag_configuration.value) AS kuberic_ag_configuration_id,
    CONVERT(nvarchar(128), ep_ag_name.value) AS kuberic_ag_name,
    CONVERT(nvarchar(36), ep_ag_id.value) AS kuberic_ag_id
FROM (VALUES (1)) AS singleton(value)
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_resource_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_resource
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_configuration_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_configuration
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_resource_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_resource
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_configuration_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_configuration
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_name', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_name
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_id;"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationEffect(MutationKind);

#[derive(Debug, Clone, PartialEq, Eq)]
enum MutationKind {
    CreateSourceDatabase {
        database: SqlIdentifier,
        resource_id: String,
        configuration_id: String,
    },
    RetainAvailabilityGroupIntent {
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        resource_id: String,
        configuration_id: String,
    },
    CreateAvailabilityGroup {
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        resource_id: String,
        configuration_id: String,
        replicas: Vec<ReplicaDescriptor>,
    },
    JoinAvailabilityGroup {
        availability_group: AvailabilityGroupName,
    },
    AddDatabase {
        availability_group: AvailabilityGroupName,
        database: SqlIdentifier,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapMutationPlan {
    effects: Vec<MutationEffect>,
}

impl BootstrapMutationPlan {
    pub fn from_operation(request: &OperationRequest) -> Result<Self, MutationContractError> {
        let effects = match request.payload() {
            OperationPayload::EnsureAvailabilityGroup {
                name,
                database_name,
                replicas,
                ..
            } => vec![
                MutationEffect::create_source_database(
                    database_name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                )?,
                MutationEffect::retain_availability_group_intent(
                    database_name.clone(),
                    name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                )?,
                MutationEffect::create_availability_group(
                    database_name.clone(),
                    name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                    replicas.clone(),
                )?,
            ],
            OperationPayload::EnsureReplicaJoined {
                availability_group, ..
            } => vec![MutationEffect::join_availability_group(
                availability_group.name.clone(),
            )],
            OperationPayload::EnsureReplicaSeeded {
                availability_group,
                database,
                ..
            } => vec![MutationEffect::add_database(
                availability_group.name.clone(),
                database.name.clone(),
            )],
            _ => return Err(MutationContractError::UnsupportedOperation),
        };
        Ok(Self { effects })
    }

    pub fn effects(&self) -> &[MutationEffect] {
        &self.effects
    }
}

impl MutationEffect {
    pub fn create_source_database(
        database: SqlIdentifier,
        resource_id: &str,
        configuration_id: &str,
    ) -> Result<Self, MutationContractError> {
        validate_marker_value(resource_id)?;
        validate_marker_value(configuration_id)?;
        Ok(Self(MutationKind::CreateSourceDatabase {
            database,
            resource_id: resource_id.to_owned(),
            configuration_id: configuration_id.to_owned(),
        }))
    }

    pub fn create_availability_group(
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        resource_id: &str,
        configuration_id: &str,
        replicas: Vec<ReplicaDescriptor>,
    ) -> Result<Self, MutationContractError> {
        validate_marker_value(resource_id)?;
        validate_marker_value(configuration_id)?;
        if replicas.len() != usize::from(SUPPORTED_REPLICA_COUNT) {
            return Err(MutationContractError::ReplicaCount);
        }
        if replicas
            .iter()
            .any(|replica| replica.identity.native_replica_id().is_some())
        {
            return Err(MutationContractError::NativeBootstrapIdentity);
        }
        let mut logical_ids = BTreeSet::new();
        let mut server_names = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for replica in &replicas {
            if !logical_ids.insert(replica.identity.logical_id()) {
                return Err(MutationContractError::DuplicateReplica);
            }
            if !server_names.insert(&replica.server_name) {
                return Err(MutationContractError::DuplicateReplica);
            }
            if !endpoints.insert(&replica.endpoint) {
                return Err(MutationContractError::DuplicateReplica);
            }
        }
        Ok(Self(MutationKind::CreateAvailabilityGroup {
            database,
            availability_group,
            resource_id: resource_id.to_owned(),
            configuration_id: configuration_id.to_owned(),
            replicas,
        }))
    }

    pub fn retain_availability_group_intent(
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        resource_id: &str,
        configuration_id: &str,
    ) -> Result<Self, MutationContractError> {
        validate_marker_value(resource_id)?;
        validate_marker_value(configuration_id)?;
        Ok(Self(MutationKind::RetainAvailabilityGroupIntent {
            database,
            availability_group,
            resource_id: resource_id.to_owned(),
            configuration_id: configuration_id.to_owned(),
        }))
    }

    pub fn join_availability_group(availability_group: AvailabilityGroupName) -> Self {
        Self(MutationKind::JoinAvailabilityGroup { availability_group })
    }

    pub(crate) fn add_database(
        availability_group: AvailabilityGroupName,
        database: SqlIdentifier,
    ) -> Self {
        Self(MutationKind::AddDatabase {
            availability_group,
            database,
        })
    }

    pub const fn label(&self) -> &'static str {
        match &self.0 {
            MutationKind::CreateSourceDatabase { .. } => "create source database",
            MutationKind::RetainAvailabilityGroupIntent { .. } => {
                "retain availability group intent"
            }
            MutationKind::CreateAvailabilityGroup { .. } => "create availability group",
            MutationKind::JoinAvailabilityGroup { .. } => "join availability group",
            MutationKind::AddDatabase { .. } => "add database to availability group",
        }
    }

    pub(crate) fn is_retryable_propagation_error(&self, error: &RuntimeError) -> bool {
        matches!(
            (&self.0, error.server_code),
            (
                MutationKind::JoinAvailabilityGroup { .. },
                Some(35250 | 41158 | 41160)
            )
        )
    }

    pub fn statement(&self) -> String {
        match &self.0 {
            MutationKind::CreateSourceDatabase {
                database,
                resource_id,
                configuration_id,
            } => {
                let backup_literal = format!(
                    "/var/opt/mssql/data/kuberic-{}.bak",
                    hex(&Sha256::digest(database.as_str().as_bytes()))
                );
                let database_name = sql_literal(database.as_str());
                let database = database.quoted();
                let resource_id = sql_literal(resource_id);
                let configuration_id = sql_literal(configuration_id);
                let database_setup = sql_literal(&format!(
                    "USE {database};\n\
                     IF NOT EXISTS (SELECT 1 FROM sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_resource_id') \
                     EXEC sys.sp_addextendedproperty \
                     @name=N'kuberic_resource_id', @value=N'{resource_id}';\n\
                     IF NOT EXISTS (SELECT 1 FROM sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_configuration_id') \
                     EXEC sys.sp_addextendedproperty \
                     @name=N'kuberic_configuration_id', @value=N'{configuration_id}';\n\
                     IF OBJECT_ID(N'dbo.kuberic_marker', N'U') IS NULL \
                     CREATE TABLE dbo.kuberic_marker(\
                     id uniqueidentifier NOT NULL PRIMARY KEY, value nvarchar(128) NOT NULL);"
                ));
                format!(
                    "IF DB_ID(N'{database_name}') IS NULL CREATE DATABASE {database};\n\
                     ALTER DATABASE {database} SET RECOVERY FULL;\n\
                     EXEC(N'{database_setup}');\n\
                     BACKUP DATABASE {database} TO DISK = N'{backup_literal}' WITH INIT, CHECKSUM;\n\
                     RESTORE VERIFYONLY FROM DISK = N'{backup_literal}' WITH CHECKSUM;"
                )
            }
            MutationKind::RetainAvailabilityGroupIntent {
                database,
                availability_group,
                resource_id,
                configuration_id,
            } => {
                let database = database.quoted();
                let resource_id = sql_literal(resource_id);
                let configuration_id = sql_literal(configuration_id);
                let availability_group = sql_literal(availability_group.as_str());
                format!(
                    "IF NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_resource_id' \
                     AND CONVERT(nvarchar(512), value) = N'{resource_id}') \
                     OR NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_configuration_id' \
                     AND CONVERT(nvarchar(512), value) = N'{configuration_id}') \
                     THROW 51000, N'source database ownership is not exact', 1;\n\
                     IF EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name IN (N'kuberic_ag_resource_id', \
                     N'kuberic_ag_configuration_id', N'kuberic_ag_name', N'kuberic_ag_id')) \
                     THROW 51000, N'availability-group ownership marker already exists', 1;\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_resource_id', @value=N'{resource_id}';\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_configuration_id', @value=N'{configuration_id}';\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_name', @value=N'{availability_group}';"
                )
            }
            MutationKind::CreateAvailabilityGroup {
                database,
                availability_group,
                resource_id,
                configuration_id,
                replicas,
            } => {
                let replicas = replicas
                    .iter()
                    .map(|replica| {
                        format!(
                            "N'{}' WITH (\
                             ENDPOINT_URL = N'{}', \
                             AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, \
                             FAILOVER_MODE = EXTERNAL, \
                             SEEDING_MODE = AUTOMATIC, \
                             SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL))",
                            sql_literal(replica.server_name.as_str()),
                            sql_literal(&format!(
                                "TCP://{}:{}",
                                replica.endpoint.host(),
                                replica.endpoint.port()
                            ))
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",\n");
                let database = database.quoted();
                let availability_group_name = sql_literal(availability_group.as_str());
                let availability_group = availability_group.quoted();
                let resource_id = sql_literal(resource_id);
                let configuration_id = sql_literal(configuration_id);
                format!(
                    "IF NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                    WHERE class = 0 AND name = N'kuberic_resource_id' \
                    AND CONVERT(nvarchar(512), value) = N'{resource_id}') \
                    OR NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                    WHERE class = 0 AND name = N'kuberic_configuration_id' \
                    AND CONVERT(nvarchar(512), value) = N'{configuration_id}') \
                    THROW 51000, N'source database ownership is not exact', 1;\n\
                    IF NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                    WHERE class = 0 AND name = N'kuberic_ag_resource_id' \
                    AND CONVERT(nvarchar(512), value) = N'{resource_id}') \
                    OR NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                    WHERE class = 0 AND name = N'kuberic_ag_configuration_id' \
                    AND CONVERT(nvarchar(512), value) = N'{configuration_id}') \
                    OR NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                    WHERE class = 0 AND name = N'kuberic_ag_name' \
                    AND CONVERT(nvarchar(128), value) = N'{availability_group_name}') \
                    OR EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                    WHERE class = 0 AND name = N'kuberic_ag_id') \
                    THROW 51000, N'availability-group ownership intent is not exact', 1;\n\
                    CREATE AVAILABILITY GROUP {availability_group}\n\
                    WITH (CLUSTER_TYPE = EXTERNAL, \
                    REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1)\n\
                    FOR REPLICA ON {replicas};\n\
                    DECLARE @kuberic_ag_id nvarchar(36) = (\
                    SELECT CONVERT(nvarchar(36), group_id) FROM sys.availability_groups \
                    WHERE name = N'{availability_group_name}');\n\
                    IF @kuberic_ag_id IS NULL \
                    THROW 51000, N'created availability-group identity is unavailable', 1;\n\
                    EXEC {database}.sys.sp_addextendedproperty \
                    @name=N'kuberic_ag_id', @value=@kuberic_ag_id;\n\
                    ALTER AVAILABILITY GROUP {availability_group} GRANT CREATE ANY DATABASE;"
                )
            }
            MutationKind::JoinAvailabilityGroup { availability_group } => {
                let availability_group = availability_group.quoted();
                format!(
                    "ALTER AVAILABILITY GROUP {availability_group} \
                     JOIN WITH (CLUSTER_TYPE = EXTERNAL);\n\
                     ALTER AVAILABILITY GROUP {availability_group} GRANT CREATE ANY DATABASE;"
                )
            }
            MutationKind::AddDatabase {
                availability_group,
                database,
            } => format!(
                "ALTER AVAILABILITY GROUP {} ADD DATABASE {};",
                availability_group.quoted(),
                database.quoted()
            ),
        }
    }
}

#[async_trait]
pub trait SqlMutationExecutor: Send + Sync {
    async fn observe_standalone_database(
        &self,
        database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError>;

    async fn observe_availability_group_ownership(
        &self,
        database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        self.observe_standalone_database(database).await
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationContractError {
    ReplicaCount,
    NativeBootstrapIdentity,
    DuplicateReplica,
    InvalidMarker,
    UnsupportedOperation,
}

impl std::fmt::Display for MutationContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReplicaCount => formatter.write_str("mutation requires exactly three replicas"),
            Self::NativeBootstrapIdentity => {
                formatter.write_str("bootstrap mutation cannot accept native replica identities")
            }
            Self::DuplicateReplica => formatter.write_str(
                "bootstrap mutation requires unique replica identities, server names and endpoints",
            ),
            Self::InvalidMarker => formatter.write_str("bootstrap mutation marker is invalid"),
            Self::UnsupportedOperation => {
                formatter.write_str("operation is outside bootstrap convergence")
            }
        }
    }
}

impl std::error::Error for MutationContractError {}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn validate_marker_value(value: &str) -> Result<(), MutationContractError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        Err(MutationContractError::InvalidMarker)
    } else {
        Ok(())
    }
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
