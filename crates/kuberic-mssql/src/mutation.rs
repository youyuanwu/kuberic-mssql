use async_trait::async_trait;
use std::collections::BTreeSet;

use crate::runtime_error::RuntimeError;
use crate::{
    AvailabilityGroupName, ReplicaDescriptor, SUPPORTED_REPLICA_COUNT, SqlIdentifier,
    StandaloneDatabaseSnapshot,
};

pub const STANDALONE_DATABASE_QUERY: &str = r#"
SELECT
    CONVERT(nvarchar(20), d.database_id) AS database_id,
    CONVERT(nvarchar(60), d.state_desc) AS state_desc,
    CONVERT(nvarchar(60), d.recovery_model_desc) AS recovery_model_desc,
    CONVERT(nvarchar(36), drs.database_guid) AS database_guid,
    CONVERT(nvarchar(36), drs.family_guid) AS family_guid,
    CONVERT(nvarchar(36), drs.recovery_fork_guid) AS recovery_fork_guid
FROM sys.databases AS d
LEFT JOIN sys.database_recovery_status AS drs
    ON drs.database_id = d.database_id
WHERE d.name = @P1;"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationEffect(MutationKind);

#[derive(Debug, Clone, PartialEq, Eq)]
enum MutationKind {
    CreateSourceDatabase {
        database: SqlIdentifier,
    },
    CreateAvailabilityGroup {
        availability_group: AvailabilityGroupName,
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

impl MutationEffect {
    pub fn create_source_database(database: SqlIdentifier) -> Self {
        Self(MutationKind::CreateSourceDatabase { database })
    }

    pub fn create_availability_group(
        availability_group: AvailabilityGroupName,
        replicas: Vec<ReplicaDescriptor>,
    ) -> Result<Self, MutationContractError> {
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
            availability_group,
            replicas,
        }))
    }

    pub fn join_availability_group(availability_group: AvailabilityGroupName) -> Self {
        Self(MutationKind::JoinAvailabilityGroup { availability_group })
    }

    pub fn add_database(
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
            MutationKind::CreateAvailabilityGroup { .. } => "create availability group",
            MutationKind::JoinAvailabilityGroup { .. } => "join availability group",
            MutationKind::AddDatabase { .. } => "add database to availability group",
        }
    }

    pub fn statement(&self) -> String {
        match &self.0 {
            MutationKind::CreateSourceDatabase { database } => {
                let backup_literal = sql_literal(&format!("{}.bak", database.as_str()));
                let database = database.quoted();
                format!(
                    "CREATE DATABASE {database};\n\
                     ALTER DATABASE {database} SET RECOVERY FULL;\n\
                     BACKUP DATABASE {database} TO DISK = N'{backup_literal}' WITH INIT, CHECKSUM;\n\
                     RESTORE VERIFYONLY FROM DISK = N'{backup_literal}' WITH CHECKSUM;\n\
                     CREATE TABLE {database}.dbo.kuberic_marker(\
                     marker_id int NOT NULL PRIMARY KEY, marker_value nvarchar(128) NOT NULL);"
                )
            }
            MutationKind::CreateAvailabilityGroup {
                availability_group,
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
                let availability_group = availability_group.quoted();
                format!(
                    "CREATE AVAILABILITY GROUP {availability_group}\n\
                     WITH (CLUSTER_TYPE = EXTERNAL, \
                     REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1)\n\
                     FOR REPLICA ON {replicas};\n\
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

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationContractError {
    ReplicaCount,
    NativeBootstrapIdentity,
    DuplicateReplica,
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
        }
    }
}

impl std::error::Error for MutationContractError {}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
