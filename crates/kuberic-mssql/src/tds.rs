mod panic_boundary;

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use tiberius::{AuthMethod, Client, Column, ColumnType, Config, EncryptionLevel, QueryItem};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use zeroize::Zeroizing;

use crate::bootstrap::{
    AvailabilityGroupOwnership, StandaloneDatabaseIdentity, StandaloneDatabaseOwnership,
    StandaloneDatabaseSnapshot,
};
use crate::executor::{QueryRow, SqlExecutor, SqlSession};
use crate::mutation::{
    DATABASE_OWNERSHIP_QUERY, MutationEffect, STANDALONE_DATABASE_QUERY, SeedDispatchEvidence,
    SeedDispatchRequest, SqlMutationExecutor, source_backup_path, source_log_backup_path,
};
use crate::query::ReadQuery;
use crate::runtime_config::{ConnectionSettings, read_bounded};
use crate::runtime_error::RuntimeError;
use crate::{AvailabilityGroupName, Guid, ObservationFailureKind, SqlIdentifier};

const MAX_SECRET_BYTES: u64 = 4096;
const MAX_QUERY_ROWS: usize = 4096;
const MAX_CELL_BYTES: usize = 4096;

pub struct TdsExecutor {
    settings: ConnectionSettings,
}

#[allow(dead_code)]
pub struct TdsMutationExecutor {
    settings: ConnectionSettings,
    #[cfg(feature = "kuberic-testing")]
    seed_probe: Option<std::sync::Arc<dyn SeedSubmissionProbe>>,
}

impl TdsExecutor {
    pub fn new(settings: ConnectionSettings) -> Self {
        Self { settings }
    }
}

#[allow(dead_code)]
impl TdsMutationExecutor {
    pub(crate) fn new(settings: ConnectionSettings) -> Self {
        Self {
            settings,
            #[cfg(feature = "kuberic-testing")]
            seed_probe: None,
        }
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_new(settings: ConnectionSettings) -> Self {
        Self::new(settings)
    }

    #[cfg(feature = "kuberic-testing")]
    #[doc(hidden)]
    pub fn testing_new_with_seed_probe(
        settings: ConnectionSettings,
        seed_probe: std::sync::Arc<dyn SeedSubmissionProbe>,
    ) -> Self {
        Self {
            settings,
            seed_probe: Some(seed_probe),
        }
    }
}

#[cfg(feature = "kuberic-testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedSubmissionCheckpoint {
    BeforeRequestSubmission,
    AfterRequestSubmission,
    AfterAcceptedBeforeReply,
}

#[cfg(feature = "kuberic-testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedSubmissionAction {
    Continue,
    InjectDefiniteRejection,
}

#[cfg(feature = "kuberic-testing")]
#[async_trait]
pub trait SeedSubmissionProbe: Send + Sync {
    async fn checkpoint(
        &self,
        checkpoint: SeedSubmissionCheckpoint,
    ) -> Result<SeedSubmissionAction, RuntimeError>;
}

struct TdsSession {
    client: Option<Client<Compat<TcpStream>>>,
    query_timeout: Duration,
}

#[async_trait]
impl SqlExecutor for TdsExecutor {
    async fn connect(&self) -> Result<Box<dyn SqlSession>, RuntimeError> {
        let client = connect_client(
            &self.settings,
            "kuberic-sqlserver-observer",
            "observer username",
            "observer password",
            None,
        )
        .await?;
        Ok(Box::new(TdsSession {
            client: Some(client),
            query_timeout: self.settings.query_timeout,
        }) as Box<dyn SqlSession>)
    }
}

#[async_trait]
impl SqlMutationExecutor for TdsMutationExecutor {
    async fn observe_standalone_database(
        &self,
        database: &SqlIdentifier,
    ) -> Result<Option<StandaloneDatabaseSnapshot>, RuntimeError> {
        const STAGE: &str = "standalone database preflight";
        let mut master = connect_client(
            &self.settings,
            "kuberic-sqlserver-bootstrap",
            "mutation username",
            "mutation password",
            None,
        )
        .await?;
        let name = database.as_str().to_owned();
        let backup_path = source_backup_path(database);
        let log_backup_path = source_log_backup_path(database);
        let snapshot = timeout(
            self.settings.query_timeout,
            panic_boundary::contain(STAGE, async move {
                let mut stream = master
                    .query(
                        STANDALONE_DATABASE_QUERY,
                        &[&name, &backup_path, &log_backup_path],
                    )
                    .await
                    .map_err(|error| driver_error(STAGE, error))?;
                let mut result = None;
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|error| driver_error(STAGE, error))?
                {
                    if let QueryItem::Row(row) = item {
                        if result.is_some() {
                            return Err(malformed(
                                STAGE,
                                "standalone database query returned multiple rows",
                            ));
                        }
                        let state = required_text(&row, "state_desc", STAGE)?;
                        let recovery_model = required_text(&row, "recovery_model_desc", STAGE)?;
                        let identity = match (
                            optional_guid(&row, "database_guid", STAGE)?,
                            optional_guid(&row, "family_guid", STAGE)?,
                            optional_guid(&row, "recovery_fork_guid", STAGE)?,
                        ) {
                            (Some(database_guid), Some(family_guid), Some(recovery_fork_guid)) => {
                                Some(StandaloneDatabaseIdentity {
                                    name: database.clone(),
                                    database_guid,
                                    family_guid,
                                    recovery_fork_guid,
                                })
                            }
                            (None, None, None) => None,
                            _ => {
                                return Err(malformed(
                                    STAGE,
                                    "standalone database recovery identity is partial",
                                ));
                            }
                        };
                        result = Some(StandaloneDatabaseSnapshot {
                            identity,
                            creation_token: None,
                            ownership: None,
                            availability_group_ownership: None,
                            state,
                            recovery_model,
                            accessible: false,
                            marker_table_ready: required_bool(&row, "marker_table_ready", STAGE)?,
                            full_backup_ready: required_bool(&row, "full_backup_ready", STAGE)?,
                            log_chain_ready: required_bool(&row, "log_chain_ready", STAGE)?,
                            backup_verified: required_bool(&row, "backup_verified", STAGE)?,
                        });
                    }
                }
                Ok(result)
            }),
        )
        .await
        .map_err(|_| timed_out(STAGE))??;
        let Some(mut snapshot) = snapshot else {
            return Ok(None);
        };
        if snapshot.state != "ONLINE" {
            return Ok(Some(snapshot));
        }
        let mut database_client = match connect_client(
            &self.settings,
            "kuberic-sqlserver-bootstrap",
            "mutation username",
            "mutation password",
            Some(database.as_str()),
        )
        .await
        {
            Ok(client) => client,
            Err(error) => return existing_database_connect_error(snapshot, error).map(Some),
        };
        let ownership = timeout(
            self.settings.query_timeout,
            panic_boundary::contain(STAGE, async move {
                let mut stream = database_client
                    .query(DATABASE_OWNERSHIP_QUERY, &[])
                    .await
                    .map_err(|error| driver_error(STAGE, error))?;
                let mut ownership = None;
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|error| driver_error(STAGE, error))?
                {
                    if let QueryItem::Row(row) = item {
                        if ownership.is_some() {
                            return Err(malformed(
                                STAGE,
                                "database ownership query returned multiple rows",
                            ));
                        }

                        let database_ownership = match (
                            optional_text(&row, "kuberic_resource_id", STAGE)?,
                            optional_text(&row, "kuberic_configuration_id", STAGE)?,
                        ) {
                            (Some(resource_id), Some(configuration_id)) => {
                                Some(StandaloneDatabaseOwnership {
                                    resource_id,
                                    configuration_id,
                                })
                            }
                            (None, None) => None,
                            _ => {
                                return Err(malformed(
                                    STAGE,
                                    "standalone database ownership marker is partial",
                                ));
                            }
                        };
                        let creation_token = optional_text(&row, "kuberic_creation_token", STAGE)?;
                        let availability_group_ownership = match (
                            optional_text(&row, "kuberic_ag_resource_id", STAGE)?,
                            optional_text(&row, "kuberic_ag_configuration_id", STAGE)?,
                            optional_text(&row, "kuberic_ag_name", STAGE)?,
                            optional_guid(&row, "kuberic_ag_created_id", STAGE)?,
                            optional_guid(&row, "kuberic_ag_id", STAGE)?,
                        ) {
                            (None, None, None, None, None) => None,
                            (
                                Some(resource_id),
                                Some(configuration_id),
                                Some(name),
                                creation_group_id,
                                group_id,
                            ) => Some(AvailabilityGroupOwnership {
                                resource_id,
                                configuration_id,
                                name: AvailabilityGroupName::new(name).map_err(|_| {
                                    malformed(
                                        STAGE,
                                        "availability-group ownership marker has an invalid name",
                                    )
                                })?,
                                creation_group_id,
                                group_id,
                            }),
                            _ => {
                                return Err(malformed(
                                    STAGE,
                                    "availability-group ownership marker is partial",
                                ));
                            }
                        };
                        ownership = Some((
                            creation_token,
                            database_ownership,
                            availability_group_ownership,
                        ));
                    }
                }
                Ok(ownership)
            }),
        )
        .await
        .map_err(|_| timed_out(STAGE))??;
        if let Some((creation_token, database_ownership, availability_group_ownership)) = ownership
        {
            snapshot.creation_token = creation_token;
            snapshot.ownership = database_ownership;
            snapshot.availability_group_ownership = availability_group_ownership;
        }
        snapshot.accessible = true;
        Ok(Some(snapshot))
    }

    async fn observe_effect_completion(
        &self,
        effect: &MutationEffect,
    ) -> Result<bool, RuntimeError> {
        const STAGE: &str = "bootstrap effect evidence";
        let Some(statement) = effect.completion_query() else {
            return Ok(false);
        };
        let mut client = connect_client(
            &self.settings,
            "kuberic-sqlserver-bootstrap",
            "mutation username",
            "mutation password",
            None,
        )
        .await?;
        timeout(
            self.settings.query_timeout,
            panic_boundary::contain(STAGE, async move {
                let mut stream = client
                    .query(statement, &[])
                    .await
                    .map_err(|error| driver_error(STAGE, error))?;
                let mut complete = None;
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|error| driver_error(STAGE, error))?
                {
                    if let QueryItem::Row(row) = item {
                        if complete.is_some() {
                            return Err(malformed(STAGE, "effect evidence returned multiple rows"));
                        }
                        complete = Some(required_bool(&row, "is_complete", STAGE)?);
                    }
                }
                complete.ok_or_else(|| malformed(STAGE, "effect evidence returned no row"))
            }),
        )
        .await
        .map_err(|_| timed_out(STAGE))?
    }

    async fn reconcile_seed_dispatch(
        &self,
        request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        #[cfg(feature = "kuberic-testing")]
        if let Some(probe) = &self.seed_probe
            && matches!(
                probe
                    .checkpoint(SeedSubmissionCheckpoint::AfterAcceptedBeforeReply)
                    .await?,
                SeedSubmissionAction::InjectDefiniteRejection
            )
        {
            return Err(RuntimeError::new(
                ObservationFailureKind::Unsupported,
                "seed dispatch reconciliation",
                "invalid seed submission probe action",
            ));
        }
        match self
            .execute_seed_protocol(
                "seed dispatch reconciliation",
                request.reconciliation_statement(),
                None,
            )
            .await
        {
            Err(error) if error.server_code == Some(988) => Ok(SeedDispatchEvidence::Ambiguous),
            result => result,
        }
    }

    async fn submit_seed_dispatch(
        &self,
        request: &SeedDispatchRequest,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        const STAGE: &str = "seed ADD DATABASE submission";
        let mut client = connect_client(
            &self.settings,
            "kuberic-sqlserver-bootstrap",
            "mutation username",
            "mutation password",
            None,
        )
        .await?;
        #[cfg(feature = "kuberic-testing")]
        let inject_definite_rejection = if let Some(probe) = &self.seed_probe {
            matches!(
                probe
                    .checkpoint(SeedSubmissionCheckpoint::BeforeRequestSubmission)
                    .await?,
                SeedSubmissionAction::InjectDefiniteRejection
            )
        } else {
            false
        };
        #[cfg(not(feature = "kuberic-testing"))]
        let inject_definite_rejection = false;
        let statement = request.submission_statement(inject_definite_rejection);
        let query_timeout = self.settings.query_timeout;
        #[cfg(feature = "kuberic-testing")]
        let seed_probe = self.seed_probe.clone();
        timeout(
            query_timeout,
            panic_boundary::contain(STAGE, async {
                let mut stream = client
                    .query(statement, &[])
                    .await
                    .map_err(|error| driver_error(STAGE, error))?;
                #[cfg(feature = "kuberic-testing")]
                if let Some(probe) = &seed_probe {
                    let _ = probe
                        .checkpoint(SeedSubmissionCheckpoint::AfterRequestSubmission)
                        .await?;
                }
                let evidence = read_seed_dispatch_evidence(STAGE, &mut stream).await?;
                #[cfg(feature = "kuberic-testing")]
                if let Some(probe) = &seed_probe {
                    let _ = probe
                        .checkpoint(SeedSubmissionCheckpoint::AfterAcceptedBeforeReply)
                        .await?;
                }
                Ok(evidence)
            }),
        )
        .await
        .map_err(|_| timed_out(STAGE))?
    }

    async fn execute(&self, effect: &MutationEffect) -> Result<(), RuntimeError> {
        let stage = effect.label();
        let mut client = connect_client(
            &self.settings,
            "kuberic-sqlserver-bootstrap",
            "mutation username",
            "mutation password",
            None,
        )
        .await?;
        let statement = effect.statement();
        timeout(
            self.settings.query_timeout,
            panic_boundary::contain(stage, async move {
                let mut stream = client
                    .query(statement, &[])
                    .await
                    .map_err(|error| driver_error(stage, error))?;
                while stream
                    .try_next()
                    .await
                    .map_err(|error| driver_error(stage, error))?
                    .is_some()
                {}
                Ok(())
            }),
        )
        .await
        .map_err(|_| timed_out(stage))?
    }
}

impl TdsMutationExecutor {
    async fn execute_seed_protocol(
        &self,
        stage: &'static str,
        statement: String,
        database: Option<&str>,
    ) -> Result<SeedDispatchEvidence, RuntimeError> {
        let mut client = connect_client(
            &self.settings,
            "kuberic-sqlserver-bootstrap",
            "mutation username",
            "mutation password",
            database,
        )
        .await?;
        timeout(
            self.settings.query_timeout,
            panic_boundary::contain(stage, async move {
                let mut stream = client
                    .query(statement, &[])
                    .await
                    .map_err(|error| driver_error(stage, error))?;
                read_seed_dispatch_evidence(stage, &mut stream).await
            }),
        )
        .await
        .map_err(|_| timed_out(stage))?
    }
}

async fn read_seed_dispatch_evidence(
    stage: &'static str,
    stream: &mut tiberius::QueryStream<'_>,
) -> Result<SeedDispatchEvidence, RuntimeError> {
    let mut evidence = None;
    while let Some(item) = stream
        .try_next()
        .await
        .map_err(|error| driver_error(stage, error))?
    {
        if let QueryItem::Row(row) = item
            && let Some(phase) = optional_text(&row, "dispatch_phase", stage)?
        {
            evidence = Some(match phase.as_str() {
                "absent" => SeedDispatchEvidence::Absent,
                "submitting" => SeedDispatchEvidence::Submitting,
                "accepted" => SeedDispatchEvidence::Accepted,
                "ambiguous" => SeedDispatchEvidence::Ambiguous,
                "failed_retryable" => SeedDispatchEvidence::FailedRetryable,
                "conflict" => SeedDispatchEvidence::Conflict,
                _ => return Err(malformed(stage, "seed dispatch phase is invalid")),
            });
        }
    }
    evidence.ok_or_else(|| malformed(stage, "seed dispatch evidence returned no phase"))
}

#[allow(dead_code)]
fn existing_database_connect_error(
    snapshot: StandaloneDatabaseSnapshot,
    error: RuntimeError,
) -> Result<StandaloneDatabaseSnapshot, RuntimeError> {
    if error.server_code == Some(4060) {
        Ok(snapshot)
    } else {
        Err(error)
    }
}

#[allow(dead_code)]
fn required_text(
    row: &tiberius::Row,
    column: &'static str,
    stage: &'static str,
) -> Result<String, RuntimeError> {
    row.try_get::<&str, _>(column)
        .map_err(|_| malformed(stage, "standalone database row has an invalid text column"))?
        .map(str::to_owned)
        .ok_or_else(|| {
            malformed(
                stage,
                "standalone database row is missing a required column",
            )
        })
}

fn required_bool(
    row: &tiberius::Row,
    column: &'static str,
    stage: &'static str,
) -> Result<bool, RuntimeError> {
    match row
        .try_get::<i32, _>(column)
        .map_err(|_| malformed(stage, "bootstrap evidence has an invalid boolean column"))?
    {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        _ => Err(malformed(
            stage,
            "bootstrap evidence has a non-boolean value",
        )),
    }
}

#[allow(dead_code)]
fn optional_guid(
    row: &tiberius::Row,
    column: &'static str,
    stage: &'static str,
) -> Result<Option<Guid>, RuntimeError> {
    row.try_get::<&str, _>(column)
        .map_err(|_| malformed(stage, "standalone database row has an invalid GUID column"))?
        .map(|value| {
            Guid::parse(column, value)
                .map_err(|_| malformed(stage, "standalone database row has a malformed GUID"))
        })
        .transpose()
}

#[allow(dead_code)]
fn optional_text(
    row: &tiberius::Row,
    column: &'static str,
    stage: &'static str,
) -> Result<Option<String>, RuntimeError> {
    row.try_get::<&str, _>(column)
        .map_err(|_| {
            malformed(
                stage,
                "standalone database row has an invalid marker column",
            )
        })
        .map(|value| value.map(str::to_owned))
}

async fn connect_client(
    settings: &ConnectionSettings,
    application_name: &'static str,
    username_stage: &'static str,
    password_stage: &'static str,
    database: Option<&str>,
) -> Result<Client<Compat<TcpStream>>, RuntimeError> {
    timeout(settings.connect_timeout, async {
        let username = read_secret(&settings.username_file, username_stage).await?;
        let password = read_secret(&settings.password_file, password_stage).await?;
        let config = connection_config(settings, &username, &password, application_name, database);
        let tcp = TcpStream::connect((settings.endpoint.host(), settings.endpoint.port()))
            .await
            .map_err(|_| {
                RuntimeError::new(
                    ObservationFailureKind::Unreachable,
                    "TCP connect",
                    "cannot reach the configured SQL Server endpoint",
                )
            })?;
        tcp.set_nodelay(true).map_err(|_| {
            RuntimeError::new(
                ObservationFailureKind::Unreachable,
                "TCP connect",
                "cannot configure the SQL Server connection",
            )
        })?;
        panic_boundary::contain("TLS/TDS login", async {
            Client::connect(config, tcp.compat_write())
                .await
                .map_err(|error| driver_error("TLS/TDS login", error))
        })
        .await
    })
    .await
    .map_err(|_| timed_out("TLS/TDS connect"))?
}

fn connection_config(
    settings: &ConnectionSettings,
    username: &str,
    password: &str,
    application_name: &'static str,
    database: Option<&str>,
) -> Config {
    let mut config = Config::new();
    config.host(settings.endpoint.host());
    config.port(settings.endpoint.port());
    config.database(database.unwrap_or("master"));
    config.application_name(application_name);
    // Required avoids both plaintext fallback and the driver's On/Off panic.
    config.encryption(EncryptionLevel::Required);
    if let Some(path) = &settings.ca_certificate_file {
        // Config validation rejects non-UTF-8 paths before this point.
        config.trust_cert_ca(path.to_string_lossy());
    }
    config.authentication(AuthMethod::sql_server(username, password));
    // ApplicationIntent is not a write fence and can trigger replica routing.
    // Connect to the named instance directly.
    config
}

#[async_trait]
impl SqlSession for TdsSession {
    async fn query(
        &mut self,
        query: ReadQuery,
        availability_group: &AvailabilityGroupName,
    ) -> Result<Vec<QueryRow>, RuntimeError> {
        // The future owns the client, so an error, panic, or cancellation cannot
        // leave a partially decoded session available for another query.
        let mut client = self.client.take().ok_or_else(|| {
            malformed(
                query.label(),
                "TDS session is closed; reconnect before observing",
            )
        })?;
        let (client, rows) = timeout(
            self.query_timeout,
            panic_boundary::contain(query.label(), async move {
                let name = availability_group.as_str();
                let mut stream = client
                    .query(query.sql(), &[&name])
                    .await
                    .map_err(|error| driver_error(query.label(), error))?;
                let mut rows = Vec::new();
                let mut result_sets = 0;
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|error| driver_error(query.label(), error))?
                {
                    match item {
                        QueryItem::Metadata(metadata) => {
                            result_sets += 1;
                            if result_sets != 1 {
                                return Err(malformed(
                                    query.label(),
                                    "unexpected multiple result sets",
                                ));
                            }
                            validate_columns(query, metadata.columns())?;
                        }
                        QueryItem::Row(row) => {
                            if rows.len() == MAX_QUERY_ROWS {
                                return Err(malformed(
                                    query.label(),
                                    "query exceeds the 4096-row observation limit",
                                ));
                            }
                            let mut values = QueryRow::new();
                            for (index, column) in row.columns().iter().enumerate() {
                                let value = row.try_get::<&str, _>(index).map_err(|_| {
                                    malformed(query.label(), "expected a text or NULL DMV column")
                                })?;
                                if value.is_some_and(|text| text.len() > MAX_CELL_BYTES) {
                                    return Err(malformed(
                                        query.label(),
                                        "DMV column exceeds the size limit",
                                    ));
                                }
                                if values
                                    .insert(column.name().to_owned(), value.map(str::to_owned))
                                    .is_some()
                                {
                                    return Err(malformed(
                                        query.label(),
                                        "duplicate DMV column name",
                                    ));
                                }
                            }
                            rows.push(values);
                        }
                    }
                }
                if result_sets != 1 {
                    return Err(malformed(query.label(), "missing DMV result set"));
                }
                drop(stream);
                Ok((client, rows))
            }),
        )
        .await
        .map_err(|_| timed_out(query.label()))??;
        self.client = Some(client);
        Ok(rows)
    }
}

fn validate_columns(query: ReadQuery, columns: &[Column]) -> Result<(), RuntimeError> {
    if columns.len() != query.columns().len()
        || columns
            .iter()
            .zip(query.columns())
            .any(|(actual, expected)| {
                actual.name() != *expected || actual.column_type() != ColumnType::NVarchar
            })
    {
        return Err(malformed(
            query.label(),
            "DMV result schema does not match the predefined query",
        ));
    }
    Ok(())
}

async fn read_secret(path: &Path, stage: &'static str) -> Result<Zeroizing<String>, RuntimeError> {
    let bytes = Zeroizing::new(read_bounded(path, MAX_SECRET_BYTES, stage).await?);
    let text =
        std::str::from_utf8(&bytes).map_err(|_| malformed(stage, "Secret must be UTF-8 text"))?;
    if text.is_empty() || text.contains(['\0', '\r', '\n']) {
        return Err(malformed(
            stage,
            "Secret must be nonempty text without NUL or line endings",
        ));
    }
    Ok(Zeroizing::new(text.to_owned()))
}

fn driver_error(stage: &'static str, error: tiberius::error::Error) -> RuntimeError {
    use tiberius::error::Error;

    let code = error.code();
    let (kind, message) = match &error {
        Error::Server(_) => server_error(code),
        Error::Tls(_) => (
            ObservationFailureKind::Tls,
            "TLS certificate or handshake validation failed",
        ),
        Error::Io { kind, .. } if *kind == std::io::ErrorKind::TimedOut => (
            ObservationFailureKind::TimedOut,
            "SQL Server transport timed out",
        ),
        Error::Io { kind, .. }
            if stage == "TLS/TDS login"
                && matches!(
                    kind,
                    std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput
                ) =>
        {
            (
                ObservationFailureKind::Tls,
                "TLS certificate or handshake validation failed",
            )
        }
        Error::Io { .. } => (
            ObservationFailureKind::Unreachable,
            "SQL Server transport failed",
        ),
        Error::Routing { .. } => (
            ObservationFailureKind::Unsupported,
            "SQL Server routing is not allowed for an instance-bound observer",
        ),
        _ => (
            ObservationFailureKind::Malformed,
            "invalid TDS response or unsupported column encoding",
        ),
    };
    let mut failure = RuntimeError::new(kind, stage, message);
    failure.server_code = code;
    failure
}

fn server_error(code: Option<u32>) -> (ObservationFailureKind, &'static str) {
    match code {
        Some(18452 | 18456 | 18470 | 18486 | 18487 | 18488) => (
            ObservationFailureKind::Authentication,
            "SQL Server authentication failed",
        ),
        Some(229 | 230 | 262 | 297 | 300 | 916) => (
            ObservationFailureKind::PermissionDenied,
            "SQL Server denied the observation permission",
        ),
        _ => (
            ObservationFailureKind::Unsupported,
            "SQL Server rejected the observation query",
        ),
    }
}

fn timed_out(stage: &'static str) -> RuntimeError {
    RuntimeError::new(
        ObservationFailureKind::TimedOut,
        stage,
        "observation deadline exceeded",
    )
}

fn malformed(stage: &'static str, message: &'static str) -> RuntimeError {
    RuntimeError::new(ObservationFailureKind::Malformed, stage, message)
}

#[cfg(feature = "kuberic-testing")]
#[doc(hidden)]
pub fn testing_classify_server_error(code: u32) -> RuntimeError {
    let (kind, message) = server_error(Some(code));
    let mut error = RuntimeError::new(kind, "SQL Server", message);
    error.server_code = Some(code);
    error
}

#[cfg(feature = "kuberic-testing")]
#[doc(hidden)]
pub fn testing_classify_driver_error(error: tiberius::error::Error) -> RuntimeError {
    driver_error("TLS/TDS login", error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_errors_never_echo_server_text() {
        let marker = "secret-password-and-connection-string";
        let errors = [
            tiberius::error::Error::Protocol(marker.into()),
            tiberius::error::Error::Tls(marker.to_owned()),
            tiberius::error::Error::Io {
                kind: std::io::ErrorKind::ConnectionReset,
                message: marker.to_owned(),
            },
            tiberius::error::Error::Routing {
                host: marker.to_owned(),
                port: 1433,
            },
        ];
        for error in errors {
            let sanitized = driver_error("test", error);
            assert!(!sanitized.to_string().contains(marker));
            assert!(!format!("{sanitized:?}").contains(marker));
        }
    }

    #[test]
    fn permission_and_login_failures_are_distinct() {
        assert_eq!(
            server_error(Some(297)).0,
            ObservationFailureKind::PermissionDenied
        );
        assert_eq!(
            server_error(Some(18456)).0,
            ObservationFailureKind::Authentication
        );
        assert_eq!(
            server_error(Some(208)).0,
            ObservationFailureKind::Unsupported
        );
    }

    #[test]
    fn existing_database_error_4060_is_not_collapsed_into_absence() {
        let snapshot = StandaloneDatabaseSnapshot {
            identity: None,
            creation_token: None,
            ownership: None,
            availability_group_ownership: None,
            state: "RESTORING".into(),
            recovery_model: "FULL".into(),
            accessible: false,
            marker_table_ready: false,
            full_backup_ready: false,
            log_chain_ready: false,
            backup_verified: false,
        };
        let mut unavailable = RuntimeError::new(
            ObservationFailureKind::Unsupported,
            "TLS/TDS login",
            "SQL Server rejected the database connection",
        );
        unavailable.server_code = Some(4060);
        assert_eq!(
            existing_database_connect_error(snapshot.clone(), unavailable),
            Ok(snapshot)
        );

        let denied = RuntimeError::new(
            ObservationFailureKind::Authentication,
            "TLS/TDS login",
            "SQL Server authentication failed",
        );
        assert_eq!(
            existing_database_connect_error(
                StandaloneDatabaseSnapshot {
                    identity: None,
                    creation_token: None,
                    ownership: None,
                    availability_group_ownership: None,
                    state: "ONLINE".into(),
                    recovery_model: "FULL".into(),
                    accessible: false,
                    marker_table_ready: false,
                    full_backup_ready: false,
                    log_chain_ready: false,
                    backup_verified: false,
                },
                denied.clone(),
            ),
            Err(denied)
        );
    }

    #[tokio::test]
    async fn discarded_tds_sessions_require_a_new_connection() {
        let mut session = TdsSession {
            client: None,
            query_timeout: Duration::from_secs(1),
        };
        let error = session
            .query(
                ReadQuery::Permissions,
                &AvailabilityGroupName::new("test-ag").unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ObservationFailureKind::Malformed);
        assert_eq!(
            error.message,
            "TDS session is closed; reconnect before observing"
        );
    }

    #[test]
    fn result_metadata_is_validated_even_when_no_rows_are_returned() {
        for query in ReadQuery::ALL {
            let expected: Vec<Column> = query
                .columns()
                .iter()
                .map(|name| Column::new((*name).to_owned(), ColumnType::NVarchar))
                .collect();
            validate_columns(query, &expected).unwrap();
            assert!(validate_columns(query, &[]).is_err());
            assert!(validate_columns(query, &expected[1..]).is_err());
            let mut reordered = expected.clone();
            reordered.swap(0, 1);
            assert!(validate_columns(query, &reordered).is_err());
            let mut wrong_type = expected.clone();
            wrong_type[0] = Column::new(query.columns()[0].to_owned(), ColumnType::Int8);
            assert!(validate_columns(query, &wrong_type).is_err());
            let mut duplicate = expected.clone();
            duplicate.push(expected[0].clone());
            assert!(validate_columns(query, &duplicate).is_err());
        }
    }
}
