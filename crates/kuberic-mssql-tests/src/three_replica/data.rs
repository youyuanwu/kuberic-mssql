use std::fmt;
use std::fs::File;
use std::io::Read;
use std::time::{Duration, Instant};

use tiberius::ToSql;
use tokio::time::sleep;

use super::admin::{
    AdminDeadlines, AdminEndpoint, AdminError, AdminSession, LoginFiles, required_text, single_row,
};
use super::config::StageDeadlines;
use super::evidence::ValidatedNativeEvidence;
use super::member::ReadyMember;
use super::model::TopologyRun;
use super::secrets::{CredentialFiles, SecretError};
use super::tls::TlsAssets;

const MARKER_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerEvidence {
    pub marker_id: String,
    pub marker_value: String,
    pub primary_ordinal: u8,
    pub readable_ordinals: [u8; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataError {
    Deadline,
    Random,
    Secret(SecretError),
    Admin(AdminError),
    WrongDatabase,
    MarkerMismatch,
}

impl fmt::Display for DataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Deadline => "replicated marker deadline exceeded",
            Self::Random => "replicated marker nonce generation failed",
            Self::Secret(_) => "replicated marker credential changed",
            Self::Admin(_) => "replicated marker SQL operation failed",
            Self::WrongDatabase => "direct member connection selected the wrong database",
            Self::MarkerMismatch => "replicated marker did not converge on all members",
        })
    }
}

impl std::error::Error for DataError {}

pub(crate) struct DataContext<'a> {
    pub run: &'a TopologyRun,
    pub members: &'a [ReadyMember; 3],
    pub credentials: &'a CredentialFiles,
    pub tls: &'a TlsAssets,
    pub deadlines: StageDeadlines,
    pub complete_deadline: Instant,
}

pub(crate) async fn verify_direct_read_connectivity(
    context: &DataContext<'_>,
    deadline: Instant,
) -> Result<(), DataError> {
    let database = database_name(context.run);
    loop {
        let mut complete = true;
        for index in 1..3 {
            match connect(context, index, &database, true).await {
                Ok(mut session) => {
                    let query = r#"
SET NOCOUNT ON;
SELECT
    CONVERT(nvarchar(128), DB_NAME()),
    CONVERT(nvarchar(60), DATABASEPROPERTYEX(DB_NAME(), N'Status')),
    CONVERT(bit, CASE
        WHEN DATABASEPROPERTYEX(DB_NAME(), N'Updateability') = N'READ_ONLY' THEN 1
        ELSE 0
    END);"#;
                    match session.query_rows(query, &[]).await {
                        Ok(rows) => {
                            let row = single_row(rows).map_err(DataError::Admin)?;
                            if required_text(&row, 0).map_err(DataError::Admin)? != database
                                || required_text(&row, 1).map_err(DataError::Admin)? != "ONLINE"
                                || row.get::<bool, _>(2) != Some(true)
                            {
                                complete = false;
                            }
                        }
                        Err(_) => complete = false,
                    }
                }
                Err(_) => complete = false,
            }
        }
        if complete {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(DataError::Deadline);
        }
        sleep(MARKER_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now()))).await;
    }
}

pub(crate) async fn prove_replicated_marker(
    context: &DataContext<'_>,
    topology: &ValidatedNativeEvidence,
) -> Result<MarkerEvidence, DataError> {
    let database = database_name(context.run);
    let primary_index = usize::from(topology.primary_ordinal.saturating_sub(1));
    if primary_index >= 3 {
        return Err(DataError::MarkerMismatch);
    }
    let marker_id = random_guid()?;
    let marker_value = format!("three-replica-{}", context.run.run_id);
    let mut primary = connect(context, primary_index, &database, false).await?;
    primary
        .execute(
            r#"
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRANSACTION;
INSERT INTO dbo.kuberic_marker(id, value)
VALUES (CONVERT(uniqueidentifier, @P1), @P2);
COMMIT TRANSACTION;"#,
            &[
                &marker_id.as_str() as &dyn ToSql,
                &marker_value.as_str() as &dyn ToSql,
            ],
        )
        .await
        .map_err(DataError::Admin)?;
    drop(primary);

    let deadline = Instant::now() + remaining(context)?.min(context.deadlines.marker_convergence);
    loop {
        let mut readable = Vec::with_capacity(3);
        for index in 0..3 {
            let result = async {
                let mut session = connect(context, index, &database, true).await?;
                let row = single_row(
                    session
                        .query_rows(
                            r#"
SET NOCOUNT ON;
SELECT CONVERT(nvarchar(128), value)
FROM dbo.kuberic_marker
WHERE id = CONVERT(uniqueidentifier, @P1);"#,
                            &[&marker_id.as_str() as &dyn ToSql],
                        )
                        .await
                        .map_err(DataError::Admin)?,
                )
                .map_err(DataError::Admin)?;
                let value = required_text(&row, 0).map_err(DataError::Admin)?;
                if value != marker_value {
                    return Err(DataError::MarkerMismatch);
                }
                Ok::<u8, DataError>((index + 1) as u8)
            }
            .await;
            if let Ok(ordinal) = result {
                readable.push(ordinal);
            }
        }
        if let Ok(readable_ordinals) =
            validate_marker_observations(&readable, Instant::now(), deadline)
        {
            return Ok(MarkerEvidence {
                marker_id,
                marker_value,
                primary_ordinal: topology.primary_ordinal,
                readable_ordinals,
            });
        }
        if Instant::now() >= deadline {
            return Err(DataError::Deadline);
        }

        sleep(MARKER_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now()))).await;
    }
}

pub fn validate_marker_observations(
    readable_ordinals: &[u8],
    now: Instant,
    deadline: Instant,
) -> Result<[u8; 3], DataError> {
    if readable_ordinals == [1, 2, 3] {
        return Ok([1, 2, 3]);
    }
    if now >= deadline {
        Err(DataError::Deadline)
    } else {
        Err(DataError::MarkerMismatch)
    }
}

async fn connect(
    context: &DataContext<'_>,
    index: usize,
    database: &str,
    read_only: bool,
) -> Result<AdminSession, DataError> {
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
    AdminSession::connect_database(
        &endpoint,
        &login,
        AdminDeadlines {
            connect: remaining(context)?.min(context.deadlines.sql_connect),
            query: remaining(context)?.min(context.deadlines.sql_batch),
        },
        database,
        read_only,
    )
    .await
    .map_err(DataError::Admin)
}

fn database_name(run: &TopologyRun) -> String {
    format!("km_db_{}", run.run_id)
}

fn random_guid() -> Result<String, DataError> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|_| DataError::Random)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    ))
}

fn remaining(context: &DataContext<'_>) -> Result<Duration, DataError> {
    let remaining = context
        .complete_deadline
        .saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(DataError::Deadline)
    } else {
        Ok(remaining)
    }
}
