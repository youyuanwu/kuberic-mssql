#![cfg_attr(not(feature = "kuberic-testing"), allow(dead_code))]

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use crate::bootstrap::StandaloneDatabaseSnapshot;
use crate::runtime_error::RuntimeError;
use crate::{
    AvailabilityGroupName, Guid, OperationPayload, OperationRequest, ReplicaDescriptor,
    SUPPORTED_REPLICA_COUNT, SqlIdentifier,
};

pub const STANDALONE_DATABASE_QUERY: &str = r#"
SELECT
    CONVERT(nvarchar(60), d.state_desc) AS state_desc,
    CONVERT(nvarchar(60), d.recovery_model_desc) AS recovery_model_desc,
    CONVERT(nvarchar(36), drs.database_guid) AS database_guid,
    CONVERT(nvarchar(36), drs.family_guid) AS family_guid,
    CONVERT(nvarchar(36), drs.recovery_fork_guid) AS recovery_fork_guid,
    CONVERT(int, CASE WHEN OBJECT_ID(QUOTENAME(@P1) + N'.dbo.kuberic_marker', N'U') IS NOT NULL
        THEN 1 ELSE 0 END) AS marker_table_ready,
    CONVERT(int, CASE WHEN full_backup.backup_set_id IS NOT NULL
        THEN 1 ELSE 0 END) AS full_backup_ready,
    CONVERT(int, CASE WHEN log_backup.backup_set_id IS NOT NULL
        AND drs.last_log_backup_lsn IS NOT NULL THEN 1 ELSE 0 END) AS log_chain_ready,
    CONVERT(int, CASE WHEN verified.backup_set_id IS NOT NULL
        THEN 1 ELSE 0 END) AS backup_verified
FROM sys.databases AS d
LEFT JOIN sys.database_recovery_status AS drs
    ON drs.database_id = d.database_id
OUTER APPLY (
    SELECT TOP (1) bs.backup_set_id
    FROM msdb.dbo.backupset AS bs
    INNER JOIN msdb.dbo.backupmediafamily AS bmf
        ON bmf.media_set_id = bs.media_set_id
    WHERE bs.database_name = @P1
      AND bs.database_guid = drs.database_guid
      AND bs.type = N'D'
      AND bs.is_copy_only = 0
      AND bs.is_damaged = 0
      AND bs.has_backup_checksums = 1
      AND bs.recovery_model = N'FULL'
      AND bmf.physical_device_name = @P2
    ORDER BY bs.backup_finish_date DESC, bs.backup_set_id DESC
) AS full_backup
OUTER APPLY (
    SELECT TOP (1) bs.backup_set_id
    FROM msdb.dbo.backupset AS bs
    INNER JOIN msdb.dbo.backupmediafamily AS bmf
        ON bmf.media_set_id = bs.media_set_id
    WHERE bs.database_name = @P1
      AND bs.database_guid = drs.database_guid
      AND bs.type = N'L'
      AND bs.is_copy_only = 0
      AND bs.is_damaged = 0
      AND bs.has_backup_checksums = 1
      AND bs.recovery_model = N'FULL'
      AND bs.database_backup_lsn IS NOT NULL
      AND bs.last_lsn > bs.first_lsn
      AND bmf.physical_device_name = @P3
    ORDER BY bs.backup_finish_date DESC, bs.backup_set_id DESC
) AS log_backup
OUTER APPLY (
    SELECT TOP (1) rh.backup_set_id
    FROM msdb.dbo.restorehistory AS rh
    WHERE rh.destination_database_name = @P1
      AND rh.restore_type = N'V'
      AND rh.backup_set_id = full_backup.backup_set_id
    ORDER BY rh.restore_date DESC, rh.restore_history_id DESC
) AS verified
WHERE d.name = @P1;"#;

#[allow(dead_code)]
pub(crate) const DATABASE_OWNERSHIP_QUERY: &str = r#"
SELECT
    CONVERT(nvarchar(512), ep_resource.value) AS kuberic_resource_id,
    CONVERT(nvarchar(512), ep_configuration.value) AS kuberic_configuration_id,
    CONVERT(nvarchar(512), ep_activation.value) AS kuberic_activation_id,
    CONVERT(nvarchar(128), ep_creation.value) AS kuberic_creation_token,
    CONVERT(nvarchar(512), ep_ag_resource.value) AS kuberic_ag_resource_id,
    CONVERT(nvarchar(512), ep_ag_configuration.value) AS kuberic_ag_configuration_id,
    CONVERT(nvarchar(512), ep_ag_activation.value) AS kuberic_ag_activation_id,
    CONVERT(nvarchar(128), ep_ag_name.value) AS kuberic_ag_name,
    CONVERT(nvarchar(36), ep_ag_created_id.value) AS kuberic_ag_created_id,
    CONVERT(nvarchar(36), ep_ag_id.value) AS kuberic_ag_id
FROM (VALUES (1)) AS singleton(value)
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_resource_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_resource
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_configuration_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_configuration
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_activation_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_activation
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_creation_token', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_creation
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_resource_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_resource
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_configuration_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_configuration
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_activation_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_activation
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_name', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_name
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_created_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_created_id
OUTER APPLY sys.fn_listextendedproperty(
    N'kuberic_ag_id', DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT
) AS ep_ag_id;"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationEffect(MutationKind);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedDispatchEvidence {
    Absent,
    Submitting,
    Accepted,
    Terminal,
    Ambiguous,
    FailedRetryable,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedDispatchRequest {
    availability_group: AvailabilityGroupName,
    availability_group_id: Guid,
    database: SqlIdentifier,
    database_guid: Guid,
    operation_token: String,
    terminal_token: String,
}

impl SeedDispatchRequest {
    pub fn new(
        availability_group: AvailabilityGroupName,
        availability_group_id: Guid,
        database: SqlIdentifier,
        database_guid: Guid,
        operation_token: impl Into<String>,
        terminal_token: impl Into<String>,
    ) -> Result<Self, MutationContractError> {
        let operation_token = operation_token.into();
        let terminal_token = terminal_token.into();
        if [&operation_token, &terminal_token]
            .iter()
            .any(|token| token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(MutationContractError::InvalidMarker);
        }
        Ok(Self {
            availability_group,
            availability_group_id,
            database,
            database_guid,
            operation_token,
            terminal_token,
        })
    }

    pub fn effect(&self) -> MutationEffect {
        MutationEffect::add_database(self.availability_group.clone(), self.database.clone())
    }

    pub fn operation_token(&self) -> &str {
        &self.operation_token
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_reconciliation_statement(&self) -> String {
        self.reconciliation_statement()
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_submission_statement(&self, inject_definite_rejection: bool) -> String {
        self.submission_statement(inject_definite_rejection)
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_terminal_marker(&self) -> (String, String) {
        (self.terminal_marker_name(), self.terminal_marker_value())
    }

    pub(crate) fn reconciliation_statement(&self) -> String {
        let table = format!("{}.dbo.kuberic_seed_dispatch", self.database.quoted());
        let token = sql_literal(&self.operation_token);
        let marker_name = sql_literal(&self.terminal_marker_name());
        let marker_value = sql_literal(&self.terminal_marker_value());
        let lock = sql_literal(&self.terminal_lock_name());
        let association = self.exact_association_predicate();
        format!(
            "DECLARE @terminal_value nvarchar(128) = (SELECT CONVERT(nvarchar(128), value) \
             FROM master.sys.extended_properties WHERE class=0 AND name=N'{marker_name}');\n\
             IF @terminal_value IS NOT NULL\n\
             BEGIN\n\
             SELECT CASE WHEN @terminal_value=N'{marker_value}' \
             THEN N'terminal' ELSE N'conflict' END AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF OBJECT_ID(N'{table}', N'U') IS NULL\n\
             BEGIN\n\
             IF {association}\n\
             BEGIN\n\
             DECLARE @missing_lock_result int;\n\
             EXEC @missing_lock_result = sys.sp_getapplock @Resource=N'{lock}', \
             @LockMode=N'Exclusive', @LockOwner=N'Session', @LockTimeout=0;\n\
             IF @missing_lock_result < 0\n\
             BEGIN\n\
             SELECT N'ambiguous' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF EXISTS (SELECT 1 FROM master.sys.extended_properties WHERE class=0 \
             AND name=N'{marker_name}' AND CONVERT(nvarchar(128), value)<>N'{marker_value}')\n\
             BEGIN\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             SELECT N'conflict' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF NOT EXISTS (SELECT 1 FROM master.sys.extended_properties \
             WHERE class=0 AND name=N'{marker_name}')\n\
             EXEC master.sys.sp_addextendedproperty \
             @name=N'{marker_name}', @value=N'{marker_value}';\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             SELECT N'terminal' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             SELECT N'absent' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF (SELECT COUNT_BIG(*) FROM {table}) > 1\n\
             BEGIN\n\
             SELECT N'conflict' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF NOT EXISTS (SELECT 1 FROM {table})\n\
             BEGIN\n\
             SELECT N'absent' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF NOT EXISTS (SELECT 1 FROM {table} WHERE singleton = 1 \
             AND operation_token = N'{token}' \
             AND availability_group_id = CONVERT(uniqueidentifier, N'{group_id}') \
             AND database_guid = CONVERT(uniqueidentifier, N'{database_guid}'))\n\
             BEGIN\n\
             SELECT N'conflict' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             DECLARE @phase nvarchar(32) = (SELECT phase FROM {table} WHERE singleton = 1);\n\
             DECLARE @lock_result int;\n\
             EXEC @lock_result = sys.sp_getapplock @Resource=N'{lock}', \
             @LockMode=N'Exclusive', @LockOwner=N'Session', @LockTimeout=0;\n\
             IF @lock_result < 0\n\
             BEGIN\n\
             SELECT N'ambiguous' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             SET @terminal_value = (SELECT CONVERT(nvarchar(128), value) \
             FROM master.sys.extended_properties WHERE class=0 AND name=N'{marker_name}');\n\
             IF @terminal_value IS NOT NULL AND @terminal_value<>N'{marker_value}'\n\
             BEGIN\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             SELECT N'conflict' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF {association} OR @phase=N'accepted'\n\
             BEGIN\n\
             IF @terminal_value IS NULL\n\
             EXEC master.sys.sp_addextendedproperty \
             @name=N'{marker_name}', @value=N'{marker_value}';\n\
             UPDATE {table} SET phase=N'accepted', error_code=NULL WHERE singleton=1;\n\
             SET @phase=N'terminal';\n\
             END\n\
             ELSE IF @phase IN (N'intent', N'submitting', N'ambiguous')\n\
             BEGIN\n\
             UPDATE {table} SET phase=N'failed_retryable' WHERE singleton=1;\n\
             SET @phase=N'failed_retryable';\n\
             END;\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             SELECT @phase AS dispatch_phase;",
            group_id = self.availability_group_id.as_str(),
            database_guid = self.database_guid.as_str(),
        )
    }

    pub(crate) fn submission_statement(&self, inject_definite_rejection: bool) -> String {
        let table = format!("{}.dbo.kuberic_seed_dispatch", self.database.quoted());
        let token = sql_literal(&self.operation_token);
        let marker_name = sql_literal(&self.terminal_marker_name());
        let marker_value = sql_literal(&self.terminal_marker_value());
        let lock = sql_literal(&self.terminal_lock_name());
        let association = self.exact_association_predicate();
        let rejection = if inject_definite_rejection {
            "THROW 51001, N'injected definite seed dispatch rejection', 1;"
        } else {
            ""
        };
        format!(
            "DECLARE @terminal_value nvarchar(128) = (SELECT CONVERT(nvarchar(128), value) \
             FROM master.sys.extended_properties WHERE class=0 AND name=N'{marker_name}');\n\
             IF @terminal_value IS NOT NULL\n\
             BEGIN\n\
             SELECT CASE WHEN @terminal_value=N'{marker_value}' \
             THEN N'terminal' ELSE N'conflict' END AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF OBJECT_ID(N'{table}', N'U') IS NULL\n\
             BEGIN\n\
             CREATE TABLE {table}(\
             singleton tinyint NOT NULL CONSTRAINT PK_kuberic_seed_dispatch PRIMARY KEY \
             CONSTRAINT CK_kuberic_seed_dispatch_singleton CHECK (singleton = 1),\
             operation_token char(64) NOT NULL,\
             availability_group_id uniqueidentifier NOT NULL,\
             database_guid uniqueidentifier NOT NULL,\
             phase nvarchar(32) NOT NULL \
             CONSTRAINT CK_kuberic_seed_dispatch_phase CHECK \
             (phase IN (N'intent',N'submitting',N'accepted',N'ambiguous',N'failed_retryable')),\
             error_code int NULL);\n\
             END;\n\
             IF EXISTS (SELECT 1 FROM {table} WHERE singleton <> 1 \
             OR operation_token <> N'{token}' \
             OR availability_group_id <> CONVERT(uniqueidentifier, N'{group_id}') \
             OR database_guid <> CONVERT(uniqueidentifier, N'{database_guid}'))\n\
             THROW 51000, N'seed dispatch metadata conflicts with the requested operation', 1;\n\
             IF NOT EXISTS (SELECT 1 FROM {table} WHERE singleton=1)\n\
             INSERT {table}(singleton, operation_token, availability_group_id, database_guid, phase) \
             VALUES(1, N'{token}', CONVERT(uniqueidentifier, N'{group_id}'), \
             CONVERT(uniqueidentifier, N'{database_guid}'), N'intent');\n\
             DECLARE @lock_result int;\n\
             EXEC @lock_result = sys.sp_getapplock @Resource=N'{lock}', \
             @LockMode=N'Exclusive', @LockOwner=N'Session', @LockTimeout=0;\n\
             IF @lock_result < 0 THROW 51000, N'seed dispatch operation is already submitting', 1;\n\
             SET @terminal_value = (SELECT CONVERT(nvarchar(128), value) \
             FROM master.sys.extended_properties WHERE class=0 AND name=N'{marker_name}');\n\
             IF @terminal_value IS NOT NULL\n\
             BEGIN\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             SELECT CASE WHEN @terminal_value=N'{marker_value}' \
             THEN N'terminal' ELSE N'conflict' END AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             IF EXISTS (SELECT 1 FROM {table} WHERE singleton=1 AND phase=N'accepted')\n\
             BEGIN\n\
             EXEC master.sys.sp_addextendedproperty \
             @name=N'{marker_name}', @value=N'{marker_value}';\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             SELECT N'terminal' AS dispatch_phase;\n\
             RETURN;\n\
             END;\n\
             UPDATE {table} SET phase=N'submitting', error_code=NULL WHERE singleton=1;\n\
             SELECT N'submitting' AS dispatch_phase;\n\
             BEGIN TRY\n\
             {rejection}\n\
             ALTER AVAILABILITY GROUP {availability_group} ADD DATABASE {database};\n\
             EXEC master.sys.sp_addextendedproperty \
             @name=N'{marker_name}', @value=N'{marker_value}';\n\
             UPDATE {table} SET phase=N'accepted', error_code=NULL WHERE singleton=1;\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             SELECT N'terminal' AS dispatch_phase;\n\
             END TRY\n\
             BEGIN CATCH\n\
             DECLARE @error_number int = ERROR_NUMBER();\n\
             IF {association}\n\
             BEGIN\n\
             IF NOT EXISTS (SELECT 1 FROM master.sys.extended_properties \
             WHERE class=0 AND name=N'{marker_name}')\n\
             EXEC master.sys.sp_addextendedproperty \
             @name=N'{marker_name}', @value=N'{marker_value}';\n\
             UPDATE {table} SET phase=N'accepted', error_code=NULL WHERE singleton=1;\n\
             END\n\
             ELSE\n\
             UPDATE {table} SET phase=N'failed_retryable', error_code=@error_number WHERE singleton=1;\n\
             EXEC sys.sp_releaseapplock @Resource=N'{lock}', @LockOwner=N'Session';\n\
             THROW;\n\
             END CATCH;",
            group_id = self.availability_group_id.as_str(),
            database_guid = self.database_guid.as_str(),
            availability_group = self.availability_group.quoted(),
            database = self.database.quoted(),
        )
    }

    fn terminal_marker_name(&self) -> String {
        format!("kuberic_seed_terminal_v2_{}", self.terminal_scope_digest())
    }

    fn terminal_marker_value(&self) -> String {
        format!("v2:{}", self.terminal_token)
    }

    fn terminal_lock_name(&self) -> String {
        format!("kuberic-seed-terminal-{}", self.terminal_scope_digest())
    }

    fn terminal_scope_digest(&self) -> String {
        let mut digest = Sha256::new();
        for value in [
            "kuberic-mssql-seed-terminal-scope-v1",
            self.availability_group.as_str(),
            self.database.as_str(),
        ] {
            digest.update(value.as_bytes());
            digest.update(b"\0");
        }
        hex(&digest.finalize())
    }

    fn exact_association_predicate(&self) -> String {
        format!(
            "EXISTS (SELECT 1 FROM sys.availability_databases_cluster AS adc \
             INNER JOIN sys.availability_groups AS ag ON ag.group_id = adc.group_id \
             INNER JOIN sys.databases AS d ON d.name = adc.database_name \
             INNER JOIN sys.database_recovery_status AS drs ON drs.database_id = d.database_id \
             WHERE ag.name = N'{}' \
             AND ag.group_id = CONVERT(uniqueidentifier, N'{}') \
             AND adc.database_name = N'{}' \
             AND drs.database_guid = CONVERT(uniqueidentifier, N'{}'))",
            sql_literal(self.availability_group.as_str()),
            self.availability_group_id.as_str(),
            sql_literal(self.database.as_str()),
            self.database_guid.as_str(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MutationKind {
    RetainSourceDatabaseCreationIntent {
        database: SqlIdentifier,
        token: String,
    },
    CreateSourceDatabase {
        database: SqlIdentifier,
        token: String,
    },
    RemoveSourceDatabaseCreationIntent {
        token: String,
    },
    ConfigureSourceDatabaseRecovery {
        database: SqlIdentifier,
    },
    RetainSourceDatabaseOwnership {
        database: SqlIdentifier,
        resource_id: String,
        configuration_id: String,
        activation_id: String,
    },
    CreateSourceMarkerTable {
        database: SqlIdentifier,
    },
    BackupSourceDatabase {
        database: SqlIdentifier,
    },
    BackupSourceLog {
        database: SqlIdentifier,
    },
    VerifySourceBackup {
        database: SqlIdentifier,
    },
    RetainAvailabilityGroupIntent {
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        resource_id: String,
        configuration_id: String,
        activation_id: String,
    },
    RetainAvailabilityGroupCreationIntent {
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        token: String,
    },
    CreateAvailabilityGroup {
        availability_group: AvailabilityGroupName,
        token: String,
        replicas: Vec<ReplicaDescriptor>,
    },
    BindAvailabilityGroupOwnership {
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
    },
    RemoveAvailabilityGroupCreationIntent {
        token: String,
    },
    JoinAvailabilityGroup {
        availability_group: AvailabilityGroupName,
    },
    GrantCreateAnyDatabase {
        availability_group: AvailabilityGroupName,
    },
    RetainCreateAnyDatabaseEvidence {
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
        // This legacy typed-operation seam is deliberately fenced away from
        // Public V1 activation identity. Its markers can never be adopted by
        // the runtime-host bootstrap path.
        let activation_id = hex(&Sha256::digest(request.canonical_input()));
        let effects = match request.payload() {
            OperationPayload::EnsureAvailabilityGroup {
                name,
                database_name,
                replicas,
                ..
            } => vec![
                MutationEffect::retain_source_database_creation_intent(
                    database_name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                    &activation_id,
                )?,
                MutationEffect::create_source_database(
                    database_name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                    &activation_id,
                )?,
                MutationEffect::configure_source_database_recovery(database_name.clone()),
                MutationEffect::retain_source_database_ownership(
                    database_name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                    &activation_id,
                )?,
                MutationEffect::create_source_marker_table(database_name.clone()),
                MutationEffect::backup_source_database(database_name.clone()),
                MutationEffect::backup_source_log(database_name.clone()),
                MutationEffect::verify_source_backup(database_name.clone()),
                MutationEffect::retain_availability_group_intent(
                    database_name.clone(),
                    name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                    &activation_id,
                )?,
                MutationEffect::retain_availability_group_creation_intent(
                    database_name.clone(),
                    name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                    &activation_id,
                )?,
                MutationEffect::create_availability_group(
                    name.clone(),
                    request.resource_id(),
                    request.source_configuration_id(),
                    &activation_id,
                    replicas.clone(),
                )?,
                MutationEffect::bind_availability_group_ownership(
                    database_name.clone(),
                    name.clone(),
                ),
                MutationEffect::grant_create_any_database(name.clone()),
                MutationEffect::retain_create_any_database_evidence(name.clone()),
            ],
            OperationPayload::EnsureReplicaJoined {
                availability_group, ..
            } => vec![
                MutationEffect::join_availability_group(availability_group.name.clone()),
                MutationEffect::grant_create_any_database(availability_group.name.clone()),
                MutationEffect::retain_create_any_database_evidence(
                    availability_group.name.clone(),
                ),
            ],
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
    pub fn retain_source_database_creation_intent(
        database: SqlIdentifier,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
    ) -> Result<Self, MutationContractError> {
        let token = creation_token(
            b"kuberic-mssql-source-database-v1",
            resource_id,
            configuration_id,
            activation_id,
            database.as_str(),
        )?;
        Ok(Self(MutationKind::RetainSourceDatabaseCreationIntent {
            database,
            token,
        }))
    }

    pub fn create_source_database(
        database: SqlIdentifier,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
    ) -> Result<Self, MutationContractError> {
        let token = creation_token(
            b"kuberic-mssql-source-database-v1",
            resource_id,
            configuration_id,
            activation_id,
            database.as_str(),
        )?;
        Ok(Self(MutationKind::CreateSourceDatabase { database, token }))
    }

    pub fn remove_source_database_creation_intent(
        database: &SqlIdentifier,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
    ) -> Result<Self, MutationContractError> {
        let token = creation_token(
            b"kuberic-mssql-source-database-v1",
            resource_id,
            configuration_id,
            activation_id,
            database.as_str(),
        )?;
        Ok(Self(MutationKind::RemoveSourceDatabaseCreationIntent {
            token,
        }))
    }

    pub fn configure_source_database_recovery(database: SqlIdentifier) -> Self {
        Self(MutationKind::ConfigureSourceDatabaseRecovery { database })
    }

    pub fn retain_source_database_ownership(
        database: SqlIdentifier,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
    ) -> Result<Self, MutationContractError> {
        validate_marker_value(resource_id)?;
        validate_marker_value(configuration_id)?;
        validate_marker_value(activation_id)?;
        Ok(Self(MutationKind::RetainSourceDatabaseOwnership {
            database,
            resource_id: resource_id.to_owned(),
            configuration_id: configuration_id.to_owned(),
            activation_id: activation_id.to_owned(),
        }))
    }

    pub fn create_source_marker_table(database: SqlIdentifier) -> Self {
        Self(MutationKind::CreateSourceMarkerTable { database })
    }

    pub fn backup_source_database(database: SqlIdentifier) -> Self {
        Self(MutationKind::BackupSourceDatabase { database })
    }

    pub fn backup_source_log(database: SqlIdentifier) -> Self {
        Self(MutationKind::BackupSourceLog { database })
    }

    pub fn verify_source_backup(database: SqlIdentifier) -> Self {
        Self(MutationKind::VerifySourceBackup { database })
    }

    pub fn create_availability_group(
        availability_group: AvailabilityGroupName,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
        replicas: Vec<ReplicaDescriptor>,
    ) -> Result<Self, MutationContractError> {
        let token = creation_token(
            b"kuberic-mssql-availability-group-v1",
            resource_id,
            configuration_id,
            activation_id,
            availability_group.as_str(),
        )?;
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
            token,
            replicas,
        }))
    }

    pub fn retain_availability_group_creation_intent(
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
    ) -> Result<Self, MutationContractError> {
        let token = creation_token(
            b"kuberic-mssql-availability-group-v1",
            resource_id,
            configuration_id,
            activation_id,
            availability_group.as_str(),
        )?;
        Ok(Self(MutationKind::RetainAvailabilityGroupCreationIntent {
            database,
            availability_group,
            token,
        }))
    }

    pub fn bind_availability_group_ownership(
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
    ) -> Self {
        Self(MutationKind::BindAvailabilityGroupOwnership {
            database,
            availability_group,
        })
    }

    pub fn remove_availability_group_creation_intent(
        availability_group: &AvailabilityGroupName,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
    ) -> Result<Self, MutationContractError> {
        let token = creation_token(
            b"kuberic-mssql-availability-group-v1",
            resource_id,
            configuration_id,
            activation_id,
            availability_group.as_str(),
        )?;
        Ok(Self(MutationKind::RemoveAvailabilityGroupCreationIntent {
            token,
        }))
    }

    pub fn retain_availability_group_intent(
        database: SqlIdentifier,
        availability_group: AvailabilityGroupName,
        resource_id: &str,
        configuration_id: &str,
        activation_id: &str,
    ) -> Result<Self, MutationContractError> {
        validate_marker_value(resource_id)?;
        validate_marker_value(configuration_id)?;
        validate_marker_value(activation_id)?;
        Ok(Self(MutationKind::RetainAvailabilityGroupIntent {
            database,
            availability_group,
            resource_id: resource_id.to_owned(),
            configuration_id: configuration_id.to_owned(),
            activation_id: activation_id.to_owned(),
        }))
    }

    pub fn join_availability_group(availability_group: AvailabilityGroupName) -> Self {
        Self(MutationKind::JoinAvailabilityGroup { availability_group })
    }

    pub fn grant_create_any_database(availability_group: AvailabilityGroupName) -> Self {
        Self(MutationKind::GrantCreateAnyDatabase { availability_group })
    }

    pub fn retain_create_any_database_evidence(availability_group: AvailabilityGroupName) -> Self {
        Self(MutationKind::RetainCreateAnyDatabaseEvidence { availability_group })
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
            MutationKind::RetainSourceDatabaseCreationIntent { .. } => {
                "retain source database creation intent"
            }
            MutationKind::CreateSourceDatabase { .. } => "create source database",
            MutationKind::RemoveSourceDatabaseCreationIntent { .. } => {
                "remove source database creation intent"
            }
            MutationKind::ConfigureSourceDatabaseRecovery { .. } => {
                "configure source database recovery"
            }
            MutationKind::RetainSourceDatabaseOwnership { .. } => {
                "retain source database ownership"
            }
            MutationKind::CreateSourceMarkerTable { .. } => "create source marker table",
            MutationKind::BackupSourceDatabase { .. } => "backup source database",
            MutationKind::BackupSourceLog { .. } => "backup source log",
            MutationKind::VerifySourceBackup { .. } => "verify source backup",
            MutationKind::RetainAvailabilityGroupIntent { .. } => {
                "retain availability group intent"
            }
            MutationKind::RetainAvailabilityGroupCreationIntent { .. } => {
                "retain availability group creation intent"
            }
            MutationKind::CreateAvailabilityGroup { .. } => "create availability group",
            MutationKind::BindAvailabilityGroupOwnership { .. } => {
                "bind availability group ownership"
            }
            MutationKind::RemoveAvailabilityGroupCreationIntent { .. } => {
                "remove availability group creation intent"
            }
            MutationKind::JoinAvailabilityGroup { .. } => "join availability group",
            MutationKind::GrantCreateAnyDatabase { .. } => "grant create any database",
            MutationKind::RetainCreateAnyDatabaseEvidence { .. } => {
                "retain create any database evidence"
            }
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

    pub fn completion_query(&self) -> Option<String> {
        match &self.0 {
            MutationKind::RetainSourceDatabaseCreationIntent { token, .. } => Some(
                trigger_completion_query("kuberic_create_database", token, true),
            ),
            MutationKind::RemoveSourceDatabaseCreationIntent { token } => Some(
                trigger_completion_query("kuberic_create_database", token, false),
            ),
            MutationKind::RetainAvailabilityGroupCreationIntent { token, .. } => {
                Some(trigger_completion_query("kuberic_create_ag", token, true))
            }
            MutationKind::RemoveAvailabilityGroupCreationIntent { token } => {
                Some(trigger_completion_query("kuberic_create_ag", token, false))
            }
            MutationKind::GrantCreateAnyDatabase { availability_group } => {
                let name = sql_literal(&seeding_permission_marker(availability_group));
                let availability_group = sql_literal(availability_group.as_str());
                Some(format!(
                    "SELECT CONVERT(int, CASE WHEN EXISTS (\
                     SELECT 1 FROM sys.extended_properties AS marker \
                     INNER JOIN sys.availability_groups AS ag \
                     ON CONVERT(nvarchar(36), marker.value) = CONVERT(nvarchar(36), ag.group_id) \
                     WHERE marker.class = 0 AND marker.name = N'{name}' \
                     AND ag.name = N'{availability_group}') \
                     THEN 1 ELSE 0 END) AS is_complete;"
                ))
            }
            _ => None,
        }
    }

    pub fn statement(&self) -> String {
        match &self.0 {
            MutationKind::RetainSourceDatabaseCreationIntent { database, token } => {
                let trigger = trigger_name("kuberic_create_database", token);
                let database_name = sql_literal(database.as_str());
                let database = database.quoted();
                let token_literal = sql_literal(token);
                let definition = format!(
                    "CREATE TRIGGER {trigger} ON ALL SERVER FOR CREATE_DATABASE AS\n\
                     BEGIN\n\
                     SET NOCOUNT ON;\n\
                     DECLARE @event xml = EVENTDATA();\n\
                     IF @event.value('(/EVENT_INSTANCE/DatabaseName)[1]', 'nvarchar(128)') = N'{database_name}'\n\
                     AND CONVERT(nvarchar(128), SESSION_CONTEXT(N'kuberic_creation_token')) = N'{token_literal}'\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_creation_token', @value=N'{token_literal}';\n\
                     END;"
                );
                format!("EXEC(N'{}');", sql_literal(&definition))
            }
            MutationKind::CreateSourceDatabase { database, token } => {
                format!(
                    "EXEC sys.sp_set_session_context @key=N'kuberic_creation_token', \
                     @value=N'{}', @read_only=1;\n\
                     CREATE DATABASE {};",
                    sql_literal(token),
                    database.quoted(),
                )
            }
            MutationKind::RemoveSourceDatabaseCreationIntent { token } => {
                format!(
                    "DROP TRIGGER {} ON ALL SERVER;",
                    trigger_name("kuberic_create_database", token)
                )
            }
            MutationKind::ConfigureSourceDatabaseRecovery { database } => {
                format!("ALTER DATABASE {} SET RECOVERY FULL;", database.quoted())
            }
            MutationKind::RetainSourceDatabaseOwnership {
                database,
                resource_id,
                configuration_id,
                activation_id,
            } => {
                let database = database.quoted();
                let resource_id = sql_literal(resource_id);
                let configuration_id = sql_literal(configuration_id);
                let activation_id = sql_literal(activation_id);
                format!(
                    "BEGIN TRANSACTION;\n\
                     BEGIN TRY\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_resource_id', @value=N'{resource_id}';\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_configuration_id', @value=N'{configuration_id}';\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_activation_id', @value=N'{activation_id}';\n\
                     COMMIT TRANSACTION;\n\
                     END TRY\n\
                     BEGIN CATCH\n\
                     IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;\n\
                     THROW;\n\
                     END CATCH;"
                )
            }
            MutationKind::CreateSourceMarkerTable { database } => format!(
                "CREATE TABLE {}.dbo.kuberic_marker(\
                 id uniqueidentifier NOT NULL PRIMARY KEY, value nvarchar(128) NOT NULL);",
                database.quoted()
            ),
            MutationKind::BackupSourceDatabase { database } => {
                let backup_literal = source_backup_path(database);
                let database = database.quoted();
                format!(
                    "BACKUP DATABASE {database} TO DISK = N'{backup_literal}' WITH INIT, CHECKSUM;"
                )
            }
            MutationKind::BackupSourceLog { database } => {
                let backup_literal = source_log_backup_path(database);
                format!(
                    "BACKUP LOG {} TO DISK = N'{backup_literal}' WITH INIT, CHECKSUM;",
                    database.quoted()
                )
            }
            MutationKind::VerifySourceBackup { database } => {
                let backup_literal = source_backup_path(database);
                format!(
                    "RESTORE VERIFYONLY FROM DISK = N'{backup_literal}' WITH CHECKSUM, LOADHISTORY;"
                )
            }
            MutationKind::RetainAvailabilityGroupIntent {
                database,
                availability_group,
                resource_id,
                configuration_id,
                activation_id,
            } => {
                let database = database.quoted();
                let resource_id = sql_literal(resource_id);
                let configuration_id = sql_literal(configuration_id);
                let activation_id = sql_literal(activation_id);
                let availability_group = sql_literal(availability_group.as_str());
                format!(
                    "IF NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_resource_id' \
                     AND CONVERT(nvarchar(512), value) = N'{resource_id}') \
                     OR NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_configuration_id' \
                     AND CONVERT(nvarchar(512), value) = N'{configuration_id}') \
                     OR NOT EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_activation_id' \
                     AND CONVERT(nvarchar(512), value) = N'{activation_id}') \
                     THROW 51000, N'source database ownership is not exact', 1;\n\
                     IF EXISTS (SELECT 1 FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name IN (N'kuberic_ag_resource_id', \
                     N'kuberic_ag_configuration_id', N'kuberic_ag_activation_id', \
                     N'kuberic_ag_name', N'kuberic_ag_id')) \
                     THROW 51000, N'availability-group ownership marker already exists', 1;\n\
                     BEGIN TRANSACTION;\n\
                     BEGIN TRY\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_resource_id', @value=N'{resource_id}';\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_configuration_id', @value=N'{configuration_id}';\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_activation_id', @value=N'{activation_id}';\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_name', @value=N'{availability_group}';\n\
                     COMMIT TRANSACTION;\n\
                     END TRY\n\
                     BEGIN CATCH\n\
                     IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;\n\
                     THROW;\n\
                     END CATCH;"
                )
            }
            MutationKind::RetainAvailabilityGroupCreationIntent {
                database,
                availability_group,
                token,
            } => {
                let trigger = trigger_name("kuberic_create_ag", token);
                let database = database.quoted();
                let group_name = sql_literal(availability_group.as_str());
                let token_literal = sql_literal(token);
                let definition = format!(
                    "CREATE TRIGGER {trigger} ON ALL SERVER FOR CREATE_AVAILABILITY_GROUP AS\n\
                     BEGIN\n\
                     SET NOCOUNT ON;\n\
                     IF CONVERT(nvarchar(128), SESSION_CONTEXT(N'kuberic_creation_token')) = N'{token_literal}'\n\
                     BEGIN\n\
                     DECLARE @group_id nvarchar(36) = (SELECT CONVERT(nvarchar(36), group_id) \
                     FROM sys.availability_groups WHERE name = N'{group_name}');\n\
                     IF @group_id IS NULL THROW 51000, N'created availability-group identity is unavailable', 1;\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_created_id', @value=@group_id;\n\
                     END\n\
                     END;"
                );
                format!("EXEC(N'{}');", sql_literal(&definition))
            }
            MutationKind::CreateAvailabilityGroup {
                availability_group,
                token,
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
                    "EXEC sys.sp_set_session_context @key=N'kuberic_creation_token', \
                    @value=N'{}', @read_only=1;\n\
                    CREATE AVAILABILITY GROUP {availability_group}\n\
                    WITH (CLUSTER_TYPE = EXTERNAL, \
                    REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1)\n\
                    FOR REPLICA ON {replicas};",
                    sql_literal(token),
                )
            }
            MutationKind::BindAvailabilityGroupOwnership {
                database,
                availability_group,
            } => {
                let database = database.quoted();
                let group_name = sql_literal(availability_group.as_str());
                format!(
                    "DECLARE @created_id nvarchar(36) = (SELECT CONVERT(nvarchar(36), value) \
                     FROM {database}.sys.extended_properties \
                     WHERE class = 0 AND name = N'kuberic_ag_created_id');\n\
                     DECLARE @actual_id nvarchar(36) = (SELECT CONVERT(nvarchar(36), group_id) \
                     FROM sys.availability_groups WHERE name = N'{group_name}');\n\
                     IF @created_id IS NULL OR @actual_id IS NULL OR @created_id <> @actual_id \
                     THROW 51000, N'availability-group creation receipt is not exact', 1;\n\
                     EXEC {database}.sys.sp_addextendedproperty \
                     @name=N'kuberic_ag_id', @value=@actual_id;"
                )
            }
            MutationKind::RemoveAvailabilityGroupCreationIntent { token } => format!(
                "DROP TRIGGER {} ON ALL SERVER;",
                trigger_name("kuberic_create_ag", token)
            ),
            MutationKind::JoinAvailabilityGroup { availability_group } => {
                format!(
                    "ALTER AVAILABILITY GROUP {} JOIN WITH (CLUSTER_TYPE = EXTERNAL);",
                    availability_group.quoted()
                )
            }
            MutationKind::GrantCreateAnyDatabase { availability_group } => format!(
                "ALTER AVAILABILITY GROUP {} GRANT CREATE ANY DATABASE;",
                availability_group.quoted()
            ),
            MutationKind::RetainCreateAnyDatabaseEvidence { availability_group } => {
                let group_name = sql_literal(availability_group.as_str());
                let marker_name = sql_literal(&seeding_permission_marker(availability_group));
                format!(
                    "DECLARE @group_id nvarchar(36) = (SELECT CONVERT(nvarchar(36), group_id) \
                     FROM sys.availability_groups WHERE name = N'{group_name}');\n\
                     IF @group_id IS NULL THROW 51000, N'availability-group identity is unavailable', 1;\n\
                     IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 \
                     AND name = N'{marker_name}' AND CONVERT(nvarchar(36), value) <> @group_id) \
                     THROW 51000, N'seeding-permission evidence conflicts with the availability group', 1;\n\
                     IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 \
                     AND name = N'{marker_name}') \
                     EXEC master.sys.sp_addextendedproperty @name=N'{marker_name}', @value=@group_id;"
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

    async fn observe_effect_completion(
        &self,
        _effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        Ok(false)
    }

    async fn reconcile_seed_dispatch(
        &self,
        _request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        Ok(SeedDispatchEvidence::Absent)
    }

    async fn submit_seed_dispatch(
        &self,
        request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        self.execute(&request.effect()).await?;
        Ok(SeedDispatchEvidence::Accepted)
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

pub(crate) fn source_backup_path(database: &SqlIdentifier) -> String {
    format!(
        "/var/opt/mssql/data/kuberic-{}.bak",
        hex(&Sha256::digest(database.as_str().as_bytes()))
    )
}

pub(crate) fn source_log_backup_path(database: &SqlIdentifier) -> String {
    format!(
        "/var/opt/mssql/data/kuberic-{}.trn",
        hex(&Sha256::digest(database.as_str().as_bytes()))
    )
}

fn validate_marker_value(value: &str) -> Result<(), MutationContractError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        Err(MutationContractError::InvalidMarker)
    } else {
        Ok(())
    }
}

fn creation_token(
    domain: &[u8],
    resource_id: &str,
    configuration_id: &str,
    activation_id: &str,
    native_name: &str,
) -> Result<String, MutationContractError> {
    validate_marker_value(resource_id)?;
    validate_marker_value(configuration_id)?;
    validate_marker_value(activation_id)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(b"\0");
    digest.update(resource_id.as_bytes());
    digest.update(b"\0");
    digest.update(configuration_id.as_bytes());
    digest.update(b"\0");
    digest.update(activation_id.as_bytes());
    digest.update(b"\0");
    digest.update(native_name.as_bytes());
    Ok(hex(&digest.finalize()))
}

fn trigger_name(prefix: &str, token: &str) -> String {
    format!("[{prefix}_{}]", &token[..32])
}

fn seeding_permission_marker(availability_group: &AvailabilityGroupName) -> String {
    format!(
        "kuberic_ag_seed_permission_{}",
        &hex(&Sha256::digest(availability_group.as_str().as_bytes()))[..32]
    )
}

fn trigger_completion_query(prefix: &str, token: &str, expected_present: bool) -> String {
    let name = format!("{prefix}_{}", &token[..32]);
    let predicate = if expected_present {
        "EXISTS"
    } else {
        "NOT EXISTS"
    };
    format!(
        "SELECT CONVERT(int, CASE WHEN {predicate} (\
         SELECT 1 FROM sys.server_triggers \
         WHERE name = N'{}' AND is_disabled = 0 \
         AND OBJECT_DEFINITION(object_id) LIKE N'%{}%') \
         THEN 1 ELSE 0 END) AS is_complete;",
        sql_literal(&name),
        sql_literal(token)
    )
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
