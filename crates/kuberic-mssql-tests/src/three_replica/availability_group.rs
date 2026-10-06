use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant as StdInstant};

use sha2::{Digest, Sha256};
use tiberius::ToSql;
use tokio::time::{Instant, sleep};

use super::admin::{
    AdminDeadlines, AdminEndpoint, AdminError, AdminSession, LoginFiles, MemberReadinessEvidence,
    required_text, single_row, validated_identifier,
};
use super::config::StageDeadlines;
use super::data::{DataContext, verify_direct_read_connectivity};
use super::deadline::{BoundedOperationError, complete_before, earlier};
use super::evidence::{
    EvidenceError, ValidatedNativeEvidence, observe_direct_members, unix_millis,
    validate_native_evidence,
};
use super::member::ReadyMember;
use super::model::{
    IncarnationError, NativeMemberBinding, NativeMemberIntent, NativeTopologyBinding,
    NativeTopologyIntent, OwnershipJournal, SqlMemberIncarnation, TopologyRun,
};
use super::ownership::JournalStore;
use super::process::{CommandSpec, ProcessError, ProcessRunner};
use super::secrets::{CredentialFiles, SecretError, SecretValue};
use super::tls::TlsAssets;

pub const HADR_ENDPOINT_NAME: &str = "kuberic_hadr";
pub const HADR_ENDPOINT_PORT: u16 = 5022;
const POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointEvidence {
    pub ordinal: u8,
    pub endpoint_name: String,
    pub state: String,
    pub port: u16,
    pub certificate_name: String,
    pub certificate_thumbprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionedAvailabilityGroup {
    pub evidence: ValidatedNativeEvidence,
    pub binding: NativeTopologyBinding,
}

#[derive(Debug)]
pub enum AvailabilityGroupError {
    Deadline,
    Intent,
    Journal,
    Admin(AdminError),
    Secret(SecretError),
    CertificateExchange,
    Helper,
    FrozenMember,
    Evidence(EvidenceError),
    Incarnation(IncarnationError),
}

impl fmt::Display for AvailabilityGroupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Deadline => "native availability-group deadline exceeded",
            Self::Intent => "native availability-group intent is invalid",
            Self::Journal => "native availability-group journal update failed",
            Self::Admin(_) => "native availability-group SQL operation failed",
            Self::Secret(_) => "native availability-group credential changed",
            Self::CertificateExchange => "native endpoint certificate exchange failed",
            Self::Helper => "native endpoint certificate permission helper failed",
            Self::FrozenMember => "native frozen SQL Server member evidence changed",
            Self::Evidence(_) => "native availability-group evidence validation failed",
            Self::Incarnation(_) => "native SQL Server incarnation changed",
        })
    }
}

impl std::error::Error for AvailabilityGroupError {}

pub(crate) struct ProvisionContext<'a, R> {
    pub run: &'a TopologyRun,
    pub members: &'a [ReadyMember; 3],
    pub credentials: &'a CredentialFiles,
    pub tls: &'a TlsAssets,
    pub deadlines: StageDeadlines,
    pub complete_deadline: StdInstant,
    pub store: &'a JournalStore,
    pub journal: &'a mut OwnershipJournal,
    pub runner: &'a R,
    pub frozen_member_verifier: &'a dyn FrozenMemberVerifier,
}

pub(crate) trait FrozenMemberVerifier {
    fn verify(
        &self,
        parent_deadline: StdInstant,
        operation_timeout: Duration,
    ) -> Result<(), AvailabilityGroupError>;
}

pub(crate) async fn provision<R: ProcessRunner>(
    context: ProvisionContext<'_, R>,
) -> Result<ProvisionedAvailabilityGroup, AvailabilityGroupError> {
    let intent = native_intent(context.run, context.members)?;
    if context.journal.native_intent.is_some() || context.journal.native_binding.is_some() {
        return Err(AvailabilityGroupError::Intent);
    }
    context.journal.native_intent = Some(intent);
    context
        .store
        .save(context.journal)
        .map_err(|_| AvailabilityGroupError::Journal)?;

    let formation_deadline = earlier(
        Instant::from_std(context.complete_deadline),
        Instant::now() + context.deadlines.availability_group,
    );
    let mut endpoint_evidence = Vec::with_capacity(3);
    for index in 0..3 {
        let mut session =
            connect_admin(&context, index, "master", false, formation_deadline).await?;
        create_endpoint(
            &context,
            &mut session,
            context.run,
            index,
            context.credentials,
            formation_deadline,
        )
        .await?;
        endpoint_evidence.push(
            verify_endpoint(
                &context,
                &mut session,
                context.run,
                index,
                formation_deadline,
            )
            .await?,
        );
    }
    exchange_endpoint_certificates(&context, formation_deadline.into_std())?;
    for target in 0..3 {
        let mut session =
            connect_admin(&context, target, "master", false, formation_deadline).await?;
        for peer in 0..3 {
            if peer != target {
                authorize_peer(
                    &context,
                    &mut session,
                    context.run,
                    target,
                    peer,
                    formation_deadline,
                )
                .await?;
            }
        }
    }

    create_and_join_group(&context, formation_deadline).await?;
    create_database(&context, formation_deadline).await?;
    verify_direct_read_connectivity(
        &DataContext {
            run: context.run,
            members: context.members,
            credentials: context.credentials,
            tls: context.tls,
            deadlines: context.deadlines,
            complete_deadline: context.complete_deadline,
        },
        formation_deadline,
    )
    .await
    .map_err(|error| match error {
        super::data::DataError::Deadline => AvailabilityGroupError::Deadline,
        _ => AvailabilityGroupError::Evidence(EvidenceError::ObservationFailed),
    })?;
    let evidence = loop {
        let observations = observe_direct_members(
            context.run,
            context.members,
            formation_deadline,
            context.deadlines.sql_batch,
        )
        .await;
        let validation = match observations {
            Ok(observations) => {
                let result = unix_millis().and_then(|now| {
                    validate_native_evidence(
                        context.run,
                        observations.clone(),
                        now,
                        Duration::from_secs(30),
                    )
                });
                if result.as_ref().is_err_and(|error| !error.retryable()) {
                    eprintln!(
                        "native availability-group evidence rejected: {:?}; observations: {observations:#?}",
                        result.as_ref().unwrap_err()
                    );
                }
                result
            }
            Err(error) => Err(error),
        };
        match validation {
            Ok(evidence) if Instant::now() < formation_deadline => break evidence,
            Ok(_) | Err(EvidenceError::Deadline) => {
                return Err(AvailabilityGroupError::Deadline);
            }
            Err(error) if error.retryable() && Instant::now() < formation_deadline => {
                sleep(
                    POLL_INTERVAL.min(formation_deadline.saturating_duration_since(Instant::now())),
                )
                .await;
            }
            Err(error) => return Err(AvailabilityGroupError::Evidence(error)),
        }
        if Instant::now() >= formation_deadline {
            return Err(AvailabilityGroupError::Deadline);
        }
    };
    let endpoint_evidence: [EndpointEvidence; 3] = endpoint_evidence
        .try_into()
        .map_err(|_| AvailabilityGroupError::Intent)?;
    let fresh_instances = observe_fresh_instances(&context, formation_deadline).await?;
    context.frozen_member_verifier.verify(
        formation_deadline.into_std(),
        context.deadlines.docker_command,
    )?;
    let frozen_incarnations = context
        .journal
        .sql_member_incarnations
        .as_ref()
        .ok_or(AvailabilityGroupError::Intent)?;
    let binding_incarnations =
        validate_binding_incarnations(context.members, frozen_incarnations, &fresh_instances)?;
    let binding = build_binding(
        context.run,
        &binding_incarnations,
        &endpoint_evidence,
        &evidence,
    )?;
    if Instant::now() >= formation_deadline {
        return Err(AvailabilityGroupError::Deadline);
    }
    context.journal.native_binding = Some(binding.clone());
    context
        .store
        .save(context.journal)
        .map_err(|_| AvailabilityGroupError::Journal)?;
    Ok(ProvisionedAvailabilityGroup { evidence, binding })
}

fn native_intent(
    run: &TopologyRun,
    members: &[ReadyMember; 3],
) -> Result<NativeTopologyIntent, AvailabilityGroupError> {
    let native_members = (0..3)
        .map(|index| {
            let peers = (0..3).filter(|peer| *peer != index).collect::<Vec<_>>();
            Ok(NativeMemberIntent {
                ordinal: run.members[index].ordinal,
                server_name: run.members[index].server_name.clone(),
                container_id: members[index].container_id.clone(),
                sql_start_unix_millis: members[index].sql_start_unix_millis,
                endpoint_certificate_name: endpoint_certificate_name(run, index)?,
                peer_login_names: [
                    endpoint_login_name(run, peers[0])?,
                    endpoint_login_name(run, peers[1])?,
                ],
                peer_user_names: [
                    endpoint_user_name(run, peers[0])?,
                    endpoint_user_name(run, peers[1])?,
                ],
            })
        })
        .collect::<Result<Vec<_>, AvailabilityGroupError>>()?
        .try_into()
        .map_err(|_| AvailabilityGroupError::Intent)?;
    Ok(NativeTopologyIntent {
        session_id: run.run_id.clone(),
        availability_group_name: availability_group_name(run),
        database_name: database_name(run),
        endpoint_name: HADR_ENDPOINT_NAME.to_owned(),
        endpoint_port: HADR_ENDPOINT_PORT,
        members: native_members,
    })
}

async fn create_endpoint(
    context: &ProvisionContext<'_, impl ProcessRunner>,
    session: &mut AdminSession,
    run: &TopologyRun,
    index: usize,
    credentials: &CredentialFiles,
    parent_deadline: Instant,
) -> Result<(), AvailabilityGroupError> {
    let certificate = endpoint_certificate_name(run, index)?;
    let backup_path = endpoint_certificate_container_path(run, index)?;
    let master_password = credentials.endpoint_master_key_passwords[index]
        .read_secret()
        .map_err(AvailabilityGroupError::Secret)?;
    let batch = format!(
        r#"
SET NOCOUNT ON;
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = N'##MS_DatabaseMasterKey##')
BEGIN
    DECLARE @master nvarchar(max) =
        N'CREATE MASTER KEY ENCRYPTION BY PASSWORD = ' + QUOTENAME(@P1, NCHAR(39));
    EXEC (@master);
END;
CREATE CERTIFICATE [{certificate}] WITH SUBJECT = N'kuberic endpoint member {ordinal}';
BACKUP CERTIFICATE [{certificate}] TO FILE = N'{backup_path}';
CREATE ENDPOINT [{endpoint}]
    STATE = STARTED
    AS TCP (LISTENER_PORT = {port}, LISTENER_IP = ALL)
    FOR DATA_MIRRORING (
        ROLE = ALL,
        AUTHENTICATION = CERTIFICATE [{certificate}],
        ENCRYPTION = REQUIRED ALGORITHM AES
    );"#,
        ordinal = index + 1,
        endpoint = HADR_ENDPOINT_NAME,
        port = HADR_ENDPOINT_PORT,
    );
    execute_admin(
        context,
        session,
        &batch,
        &[&master_password.expose() as &dyn ToSql],
        parent_deadline,
    )
    .await
}

async fn verify_endpoint(
    context: &ProvisionContext<'_, impl ProcessRunner>,
    session: &mut AdminSession,
    run: &TopologyRun,
    index: usize,
    parent_deadline: Instant,
) -> Result<EndpointEvidence, AvailabilityGroupError> {
    const QUERY: &str = r#"
SET NOCOUNT ON;
SELECT
    CONVERT(nvarchar(128), endpoint.name),
    CONVERT(nvarchar(60), endpoint.state_desc),
    CONVERT(int, tcp.port),
    CONVERT(nvarchar(128), certificate.name),
    CONVERT(varchar(128), certificate.thumbprint, 2)
FROM sys.endpoints AS endpoint
INNER JOIN sys.tcp_endpoints AS tcp ON tcp.endpoint_id = endpoint.endpoint_id
INNER JOIN sys.database_mirroring_endpoints AS mirroring
    ON mirroring.endpoint_id = endpoint.endpoint_id
INNER JOIN sys.certificates AS certificate
    ON certificate.certificate_id = mirroring.certificate_id
WHERE endpoint.name = @P1;"#;
    let row = single_row(
        query_admin(
            context,
            session,
            QUERY,
            &[&HADR_ENDPOINT_NAME as &dyn ToSql],
            parent_deadline,
        )
        .await?,
    )
    .map_err(AvailabilityGroupError::Admin)?;
    let evidence = EndpointEvidence {
        ordinal: (index + 1) as u8,
        endpoint_name: required_text(&row, 0).map_err(AvailabilityGroupError::Admin)?,
        state: required_text(&row, 1).map_err(AvailabilityGroupError::Admin)?,
        port: row
            .get::<i32, _>(2)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or(AvailabilityGroupError::Admin(AdminError::MalformedResult))?,
        certificate_name: required_text(&row, 3).map_err(AvailabilityGroupError::Admin)?,
        certificate_thumbprint: required_text(&row, 4).map_err(AvailabilityGroupError::Admin)?,
    };
    validate_endpoint_evidence(&evidence, &endpoint_certificate_name(run, index)?)?;
    Ok(evidence)
}

pub fn validate_endpoint_evidence(
    evidence: &EndpointEvidence,
    expected_certificate_name: &str,
) -> Result<(), AvailabilityGroupError> {
    if evidence.endpoint_name != HADR_ENDPOINT_NAME
        || evidence.state != "STARTED"
        || evidence.port != HADR_ENDPOINT_PORT
        || evidence.certificate_name != expected_certificate_name
        || evidence.certificate_thumbprint.len() != 40
        || !evidence
            .certificate_thumbprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(AvailabilityGroupError::Intent);
    }
    Ok(())
}

fn exchange_endpoint_certificates<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    parent_deadline: StdInstant,
) -> Result<(), AvailabilityGroupError> {
    for source in 0..3 {
        let source_path = endpoint_certificate_host_path(context.run, source)?;
        let source_bytes =
            fs::read(&source_path).map_err(|_| AvailabilityGroupError::CertificateExchange)?;
        if source_bytes.is_empty() || source_bytes.len() > 64 * 1024 {
            return Err(AvailabilityGroupError::CertificateExchange);
        }
        let expected_digest = Sha256::digest(&source_bytes);
        for target in 0..3 {
            if source == target {
                continue;
            }
            let target_path = peer_certificate_host_path(context.run, target, source)?;
            if target_path.exists() {
                return Err(AvailabilityGroupError::CertificateExchange);
            }
            fs::write(&target_path, &source_bytes)
                .map_err(|_| AvailabilityGroupError::CertificateExchange)?;
            fs::set_permissions(&target_path, fs::Permissions::from_mode(0o640))
                .map_err(|_| AvailabilityGroupError::CertificateExchange)?;
            let remaining = parent_deadline.saturating_duration_since(StdInstant::now());
            if remaining.is_zero() {
                return Err(AvailabilityGroupError::Deadline);
            }
            context
                .runner
                .run(
                    &CommandSpec::new(
                        "setfacl",
                        "share endpoint public certificate with SQL Server",
                        remaining.min(context.deadlines.tls_helper),
                    )
                    .args(["-m", "u:10001:r,m:r"])
                    .arg(&target_path),
                )
                .map_err(map_helper)?;
            if StdInstant::now() >= parent_deadline {
                return Err(AvailabilityGroupError::Deadline);
            }
            let copied =
                fs::read(&target_path).map_err(|_| AvailabilityGroupError::CertificateExchange)?;
            if Sha256::digest(copied) != expected_digest {
                return Err(AvailabilityGroupError::CertificateExchange);
            }
        }
    }
    Ok(())
}

async fn authorize_peer(
    context: &ProvisionContext<'_, impl ProcessRunner>,
    session: &mut AdminSession,
    run: &TopologyRun,
    target: usize,
    peer: usize,
    parent_deadline: Instant,
) -> Result<(), AvailabilityGroupError> {
    let login = endpoint_login_name(run, peer)?;
    let user = endpoint_user_name(run, peer)?;
    let certificate = endpoint_certificate_name(run, peer)?;
    let path = peer_certificate_container_path(run, target, peer)?;
    let password = SecretValue::generate_password().map_err(AvailabilityGroupError::Secret)?;
    let batch = format!(
        r#"
SET NOCOUNT ON;
DECLARE @login nvarchar(max) =
    N'CREATE LOGIN [{login}] WITH PASSWORD = ' + QUOTENAME(@P1, NCHAR(39)) +
    N', CHECK_POLICY = ON';
EXEC (@login);
CREATE USER [{user}] FOR LOGIN [{login}];
CREATE CERTIFICATE [{certificate}] AUTHORIZATION [{user}] FROM FILE = N'{path}';
GRANT CONNECT ON ENDPOINT::[{endpoint}] TO [{login}];"#,
        endpoint = HADR_ENDPOINT_NAME,
    );
    execute_admin(
        context,
        session,
        &batch,
        &[&password.expose() as &dyn ToSql],
        parent_deadline,
    )
    .await
}

async fn create_and_join_group<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    parent_deadline: Instant,
) -> Result<(), AvailabilityGroupError> {
    let group = availability_group_name(context.run);
    validated_identifier(&group).map_err(AvailabilityGroupError::Admin)?;
    let clauses = context
        .run
        .members
        .iter()
        .map(|member| {
            format!(
                "N'{server}' WITH (ENDPOINT_URL = N'TCP://{server}:{port}', \
                 AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, FAILOVER_MODE = EXTERNAL, \
                 SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL))",
                server = member.server_name,
                port = HADR_ENDPOINT_PORT,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let batch = format!(
        r#"
SET NOCOUNT ON;
CREATE AVAILABILITY GROUP [{group}]
WITH (
    CLUSTER_TYPE = EXTERNAL,
    REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1
)
FOR REPLICA ON
{clauses};
ALTER AVAILABILITY GROUP [{group}] GRANT CREATE ANY DATABASE;"#
    );
    let mut primary = connect_admin(context, 0, "master", false, parent_deadline).await?;
    execute_admin(context, &mut primary, &batch, &[], parent_deadline).await?;
    drop(primary);
    for index in 1..3 {
        let mut secondary = connect_admin(context, index, "master", false, parent_deadline).await?;
        execute_admin(
            context,
            &mut secondary,
            &format!(
                r#"
SET NOCOUNT ON;
ALTER AVAILABILITY GROUP [{group}] JOIN WITH (CLUSTER_TYPE = EXTERNAL);
ALTER AVAILABILITY GROUP [{group}] GRANT CREATE ANY DATABASE;"#
            ),
            &[],
            parent_deadline,
        )
        .await?;
    }
    Ok(())
}

async fn create_database<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    parent_deadline: Instant,
) -> Result<(), AvailabilityGroupError> {
    let group = availability_group_name(context.run);
    let database = database_name(context.run);
    validated_identifier(&database).map_err(AvailabilityGroupError::Admin)?;
    let backup = format!("/var/opt/mssql/data/{database}.bak");
    let batch = format!(
        r#"
SET NOCOUNT ON;
CREATE DATABASE [{database}];
ALTER DATABASE [{database}] SET RECOVERY FULL;
BACKUP DATABASE [{database}] TO DISK = N'{backup}' WITH INIT, CHECKSUM;
RESTORE VERIFYONLY FROM DISK = N'{backup}' WITH CHECKSUM;
EXEC(N'USE [{database}];
CREATE TABLE dbo.kuberic_marker(
    id uniqueidentifier NOT NULL PRIMARY KEY,
    value nvarchar(128) NOT NULL
);');
ALTER AVAILABILITY GROUP [{group}] ADD DATABASE [{database}];"#
    );
    let mut primary = connect_admin(context, 0, "master", false, parent_deadline).await?;
    execute_admin(context, &mut primary, &batch, &[], parent_deadline).await
}

fn build_binding(
    run: &TopologyRun,
    incarnations: &[SqlMemberIncarnation; 3],
    endpoints: &[EndpointEvidence; 3],
    evidence: &ValidatedNativeEvidence,
) -> Result<NativeTopologyBinding, AvailabilityGroupError> {
    let members = evidence
        .members
        .iter()
        .map(|member| {
            let index = usize::from(member.ordinal.saturating_sub(1));
            let incarnation = incarnations
                .get(index)
                .ok_or(AvailabilityGroupError::Intent)?;
            let endpoint = endpoints.get(index).ok_or(AvailabilityGroupError::Intent)?;
            if incarnation.ordinal != member.ordinal
                || incarnation.server_name != member.server_name
                || endpoint.ordinal != member.ordinal
            {
                return Err(AvailabilityGroupError::Intent);
            }
            let profile = member
                .replica_profiles
                .iter()
                .find(|profile| profile.server_name == member.server_name)
                .ok_or(AvailabilityGroupError::Intent)?;
            Ok(NativeMemberBinding {
                ordinal: member.ordinal,
                server_name: member.server_name.clone(),
                container_id: incarnation.container_id.clone(),
                sql_start_unix_millis: incarnation.sql_start_unix_millis,
                native_replica_id: member.local_replica_id.clone(),
                local_database_id: member.database.local_database_id,
                database_guid: member.database.database_guid.clone(),
                role: member.local_role.clone(),
                endpoint_url: profile.endpoint_url.clone(),
                endpoint_name: endpoint.endpoint_name.clone(),
                endpoint_port: endpoint.port,
                endpoint_certificate_name: endpoint.certificate_name.clone(),
                endpoint_certificate_thumbprint: endpoint.certificate_thumbprint.clone(),
            })
        })
        .collect::<Result<Vec<_>, AvailabilityGroupError>>()?
        .try_into()
        .map_err(|_| AvailabilityGroupError::Intent)?;
    Ok(NativeTopologyBinding {
        session_id: run.run_id.clone(),
        availability_group_name: evidence.availability_group_name.clone(),
        availability_group_id: evidence.availability_group_id.clone(),
        configuration_sequence: evidence.configuration_sequence,
        database_name: evidence.database_name.clone(),
        group_database_id: evidence.group_database_id.clone(),
        family_guid: evidence.family_guid.clone(),
        recovery_fork_id: evidence.recovery_fork_id.clone(),
        seeding_operation_ids: evidence.seeding_operation_ids.clone(),
        members,
    })
}

pub fn validate_binding_incarnations(
    launched: &[ReadyMember; 3],
    frozen: &[SqlMemberIncarnation; 3],
    fresh: &[MemberReadinessEvidence; 3],
) -> Result<[SqlMemberIncarnation; 3], AvailabilityGroupError> {
    (0..3)
        .map(|index| {
            let launched = &launched[index];
            let frozen = &frozen[index];
            let fresh = &fresh[index];
            fresh
                .verify(&launched.server_name)
                .map_err(AvailabilityGroupError::Admin)?;
            if launched.ordinal != (index + 1) as u8
                || frozen.ordinal != launched.ordinal
                || frozen.server_name != launched.server_name
            {
                return Err(AvailabilityGroupError::Intent);
            }
            frozen
                .verify(&launched.container_id, launched.sql_start_unix_millis)
                .map_err(AvailabilityGroupError::Incarnation)?;
            frozen
                .verify(&launched.container_id, fresh.sql_start_unix_millis)
                .map_err(AvailabilityGroupError::Incarnation)?;
            Ok(SqlMemberIncarnation {
                ordinal: launched.ordinal,
                server_name: fresh.server_name.clone(),
                container_id: launched.container_id.clone(),
                sql_start_unix_millis: fresh.sql_start_unix_millis,
            })
        })
        .collect::<Result<Vec<_>, AvailabilityGroupError>>()?
        .try_into()
        .map_err(|_| AvailabilityGroupError::Intent)
}

async fn observe_fresh_instances<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    parent_deadline: Instant,
) -> Result<[MemberReadinessEvidence; 3], AvailabilityGroupError> {
    let mut observations = Vec::with_capacity(3);
    for index in 0..3 {
        let mut session = connect_admin(context, index, "master", false, parent_deadline).await?;
        let observation = readiness_admin(context, &mut session, parent_deadline).await?;
        observation
            .verify(&context.run.members[index].server_name)
            .map_err(AvailabilityGroupError::Admin)?;
        observations.push(observation);
    }
    observations
        .try_into()
        .map_err(|_| AvailabilityGroupError::Intent)
}

async fn connect_admin<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    index: usize,
    database: &str,
    read_only: bool,
    parent_deadline: Instant,
) -> Result<AdminSession, AvailabilityGroupError> {
    let endpoint = AdminEndpoint {
        tcp_host: "127.0.0.1".to_owned(),
        tls_hostname: "localhost".to_owned(),
        port: context.members[index].host_port,
        ca_certificate: context.tls.ca_certificate.path().to_path_buf(),
    };
    let login = LoginFiles {
        username: context.credentials.admin_username.path().to_path_buf(),
        password: context.credentials.admin_password.path().to_path_buf(),
    };
    let remaining = remaining_until(parent_deadline)?;
    complete_before(
        parent_deadline,
        context.deadlines.sql_connect,
        AdminSession::connect_database(
            &endpoint,
            &login,
            AdminDeadlines {
                connect: context.deadlines.sql_connect.min(remaining),
                query: context.deadlines.sql_batch.min(remaining),
            },
            database,
            read_only,
        ),
    )
    .await
    .map_err(map_connect_limit)?
    .map_err(AvailabilityGroupError::Admin)
}

async fn execute_admin<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    session: &mut AdminSession,
    sql: &str,
    parameters: &[&dyn ToSql],
    parent_deadline: Instant,
) -> Result<(), AvailabilityGroupError> {
    session
        .set_query_timeout(
            context
                .deadlines
                .sql_batch
                .min(remaining_until(parent_deadline)?),
        )
        .map_err(AvailabilityGroupError::Admin)?;
    complete_before(
        parent_deadline,
        context.deadlines.sql_batch,
        session.execute(sql, parameters),
    )
    .await
    .map_err(map_query_limit)?
    .map_err(AvailabilityGroupError::Admin)
}

async fn query_admin<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    session: &mut AdminSession,
    sql: &str,
    parameters: &[&dyn ToSql],
    parent_deadline: Instant,
) -> Result<Vec<tiberius::Row>, AvailabilityGroupError> {
    session
        .set_query_timeout(
            context
                .deadlines
                .sql_batch
                .min(remaining_until(parent_deadline)?),
        )
        .map_err(AvailabilityGroupError::Admin)?;
    complete_before(
        parent_deadline,
        context.deadlines.sql_batch,
        session.query_rows(sql, parameters),
    )
    .await
    .map_err(map_query_limit)?
    .map_err(AvailabilityGroupError::Admin)
}

async fn readiness_admin<R: ProcessRunner>(
    context: &ProvisionContext<'_, R>,
    session: &mut AdminSession,
    parent_deadline: Instant,
) -> Result<MemberReadinessEvidence, AvailabilityGroupError> {
    session
        .set_query_timeout(
            context
                .deadlines
                .sql_batch
                .min(remaining_until(parent_deadline)?),
        )
        .map_err(AvailabilityGroupError::Admin)?;
    complete_before(
        parent_deadline,
        context.deadlines.sql_batch,
        session.readiness(),
    )
    .await
    .map_err(map_query_limit)?
    .map_err(AvailabilityGroupError::Admin)
}

fn availability_group_name(run: &TopologyRun) -> String {
    format!("km_ag_{}", run.run_id)
}

fn database_name(run: &TopologyRun) -> String {
    format!("km_db_{}", run.run_id)
}

fn endpoint_certificate_name(
    run: &TopologyRun,
    index: usize,
) -> Result<String, AvailabilityGroupError> {
    let value = format!("km_ep_{}_{}", &run.run_id[..8], index + 1);
    validated_identifier(&value).map_err(AvailabilityGroupError::Admin)?;
    Ok(value)
}

fn endpoint_login_name(run: &TopologyRun, peer: usize) -> Result<String, AvailabilityGroupError> {
    let value = format!("km_ep_login_{}_{}", &run.run_id[..8], peer + 1);
    validated_identifier(&value).map_err(AvailabilityGroupError::Admin)?;
    Ok(value)
}

fn endpoint_user_name(run: &TopologyRun, peer: usize) -> Result<String, AvailabilityGroupError> {
    let value = format!("km_ep_user_{}_{}", &run.run_id[..8], peer + 1);
    validated_identifier(&value).map_err(AvailabilityGroupError::Admin)?;
    Ok(value)
}

fn endpoint_certificate_container_path(
    run: &TopologyRun,
    index: usize,
) -> Result<String, AvailabilityGroupError> {
    Ok(format!(
        "/var/opt/mssql/data/{}.cer",
        endpoint_certificate_name(run, index)?
    ))
}

fn endpoint_certificate_host_path(
    run: &TopologyRun,
    index: usize,
) -> Result<PathBuf, AvailabilityGroupError> {
    Ok(run.members[index]
        .data_directory
        .join("data")
        .join(format!("{}.cer", endpoint_certificate_name(run, index)?)))
}

fn peer_certificate_container_path(
    run: &TopologyRun,
    _target: usize,
    peer: usize,
) -> Result<String, AvailabilityGroupError> {
    Ok(format!(
        "/var/opt/mssql/data/peer-{}.cer",
        endpoint_certificate_name(run, peer)?
    ))
}

fn peer_certificate_host_path(
    run: &TopologyRun,
    target: usize,
    peer: usize,
) -> Result<PathBuf, AvailabilityGroupError> {
    Ok(run.members[target]
        .data_directory
        .join("data")
        .join(format!(
            "peer-{}.cer",
            endpoint_certificate_name(run, peer)?
        )))
}

fn remaining_until(deadline: Instant) -> Result<Duration, AvailabilityGroupError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(AvailabilityGroupError::Deadline)
    } else {
        Ok(remaining)
    }
}

fn map_connect_limit(error: BoundedOperationError) -> AvailabilityGroupError {
    match error {
        BoundedOperationError::Deadline => AvailabilityGroupError::Deadline,
        BoundedOperationError::OperationTimeout => {
            AvailabilityGroupError::Admin(AdminError::ConnectDeadline)
        }
    }
}

fn map_query_limit(error: BoundedOperationError) -> AvailabilityGroupError {
    match error {
        BoundedOperationError::Deadline => AvailabilityGroupError::Deadline,
        BoundedOperationError::OperationTimeout => {
            AvailabilityGroupError::Admin(AdminError::QueryDeadline)
        }
    }
}

fn map_helper(_: ProcessError) -> AvailabilityGroupError {
    AvailabilityGroupError::Helper
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn evidence_poll_rejects_success_ready_at_parent_deadline() {
        let deadline = Instant::now() + Duration::from_millis(25);
        let result = complete_before(deadline, Duration::from_secs(1), async {
            sleep(Duration::from_millis(25)).await;
            7
        })
        .await
        .map_err(map_query_limit);
        assert!(matches!(result, Err(AvailabilityGroupError::Deadline)));
    }
}
