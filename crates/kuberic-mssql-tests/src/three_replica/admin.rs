use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::TryStreamExt;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, QueryItem, Row, ToSql};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::secrets::{PrivateFile, SecretValue};

pub const EXPECTED_SQL_SERVER_VERSION: &str = "17.0.5005.3";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminEndpoint {
    pub tcp_host: String,
    pub tls_hostname: String,
    pub port: u16,
    pub ca_certificate: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginFiles {
    pub username: PathBuf,
    pub password: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminDeadlines {
    pub connect: Duration,
    pub query: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberReadinessEvidence {
    pub server_name: String,
    pub product_version: String,
    pub edition: String,
    pub engine_edition: i32,
    pub hadr_enabled: bool,
    pub sql_start_unix_millis: i64,
}

impl MemberReadinessEvidence {
    pub fn verify(&self, expected_server_name: &str) -> Result<(), AdminError> {
        let edition = self
            .edition
            .strip_suffix(" (64-bit)")
            .unwrap_or(&self.edition);
        if self.server_name != expected_server_name {
            return Err(AdminError::WrongIdentity);
        }
        if self.product_version != EXPECTED_SQL_SERVER_VERSION {
            return Err(AdminError::WrongVersion);
        }
        if self.engine_edition != 3
            || !matches!(
                edition,
                "Developer Enterprise"
                    | "Developer Enterprise Edition"
                    | "Enterprise Developer"
                    | "Enterprise Developer Edition"
                    | "Enterprise"
                    | "Enterprise Edition"
                    | "Enterprise Edition: Core-based Licensing"
            )
        {
            return Err(AdminError::WrongEdition);
        }
        if !self.hadr_enabled {
            return Err(AdminError::HadrDisabled);
        }
        if self.sql_start_unix_millis <= 0 {
            return Err(AdminError::InvalidStartIdentity);
        }
        Ok(())
    }
}

pub struct AdminSession {
    client: Client<Compat<TcpStream>>,
    query_timeout: Duration,
}

impl AdminSession {
    pub async fn connect(
        endpoint: &AdminEndpoint,
        login: &LoginFiles,
        deadlines: AdminDeadlines,
    ) -> Result<Self, AdminError> {
        validate_endpoint(endpoint)?;
        if deadlines.connect.is_zero() || deadlines.query.is_zero() {
            return Err(AdminError::InvalidDeadline);
        }
        let username = read_private_value(&login.username).await?;
        let password = read_private_value(&login.password).await?;
        timeout(deadlines.connect, async {
            let mut config = Config::new();
            config.host(&endpoint.tls_hostname);
            config.port(endpoint.port);
            config.database("master");
            config.application_name("kuberic-mssql-three-replica-test-admin");
            config.encryption(EncryptionLevel::Required);
            config.trust_cert_ca(endpoint.ca_certificate.to_string_lossy());
            config.authentication(AuthMethod::sql_server(username.expose(), password.expose()));
            let tcp = TcpStream::connect((endpoint.tcp_host.as_str(), endpoint.port))
                .await
                .map_err(|_| AdminError::Connect)?;
            tcp.set_nodelay(true).map_err(|_| AdminError::Connect)?;
            let client = Client::connect(config, tcp.compat_write())
                .await
                .map_err(|_| AdminError::TlsOrLogin)?;
            Ok(Self {
                client,
                query_timeout: deadlines.query,
            })
        })
        .await
        .map_err(|_| AdminError::ConnectDeadline)?
    }

    pub async fn readiness(&mut self) -> Result<MemberReadinessEvidence, AdminError> {
        const QUERY: &str = r#"
SET NOCOUNT ON;
SELECT
    CONVERT(nvarchar(128), @@SERVERNAME),
    CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
    CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition')),
    CONVERT(int, SERVERPROPERTY(N'EngineEdition')),
    CONVERT(bit, SERVERPROPERTY(N'IsHadrEnabled')),
    CONVERT(bigint, DATEDIFF_BIG(millisecond, CONVERT(datetime2, '1970-01-01T00:00:00'), info.sqlserver_start_time))
FROM sys.dm_os_sys_info AS info;"#;
        let rows = self.query_rows(QUERY, &[]).await?;
        let row = single_row(rows)?;
        Ok(MemberReadinessEvidence {
            server_name: required_text(&row, 0)?,
            product_version: required_text(&row, 1)?,
            edition: required_text(&row, 2)?,
            engine_edition: row.get::<i32, _>(3).ok_or(AdminError::MalformedResult)?,
            hadr_enabled: row.get::<bool, _>(4).ok_or(AdminError::MalformedResult)?,
            sql_start_unix_millis: row.get::<i64, _>(5).ok_or(AdminError::MalformedResult)?,
        })
    }

    pub async fn bootstrap_admin(
        &mut self,
        username: &str,
        password: &SecretValue,
    ) -> Result<(), AdminError> {
        let identifier = validated_identifier(username)?;
        let batch = format!(
            r#"
SET NOCOUNT ON;
IF SUSER_ID(@P1) IS NULL
BEGIN
    DECLARE @create nvarchar(max) =
        N'CREATE LOGIN ' + QUOTENAME(@P1) + N' WITH PASSWORD = ' +
        QUOTENAME(@P2, NCHAR(39)) + N', CHECK_POLICY = ON';
    EXEC (@create);
END;
IF IS_SRVROLEMEMBER(N'sysadmin', @P1) <> 1
    ALTER SERVER ROLE [sysadmin] ADD MEMBER [{identifier}];"#
        );
        self.execute(
            &batch,
            &[&username as &dyn ToSql, &password.expose() as &dyn ToSql],
        )
        .await
    }

    pub async fn bootstrap_observer(
        &mut self,
        username: &str,
        password: &SecretValue,
    ) -> Result<(), AdminError> {
        let identifier = validated_identifier(username)?;
        let batch = format!(
            r#"
SET NOCOUNT ON;
IF SUSER_ID(@P1) IS NULL
BEGIN
    DECLARE @create nvarchar(max) =
        N'CREATE LOGIN ' + QUOTENAME(@P1) + N' WITH PASSWORD = ' +
        QUOTENAME(@P2, NCHAR(39)) + N', CHECK_POLICY = ON';
    EXEC (@create);
END;
GRANT VIEW SERVER STATE TO [{identifier}];
GRANT VIEW SERVER PERFORMANCE STATE TO [{identifier}];
GRANT VIEW ANY DEFINITION TO [{identifier}];
GRANT VIEW ANY DATABASE TO [{identifier}];"#
        );
        self.execute(
            &batch,
            &[&username as &dyn ToSql, &password.expose() as &dyn ToSql],
        )
        .await
    }

    pub async fn verify_observer_permissions(&mut self) -> Result<(), AdminError> {
        const QUERY: &str = r#"
SET NOCOUNT ON;
SELECT
    CONVERT(bit, HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER STATE')),
    CONVERT(bit, HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER PERFORMANCE STATE')),
    CONVERT(bit, HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW ANY DEFINITION')),
    CONVERT(bit, HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW ANY DATABASE'));"#;
        let row = single_row(self.query_rows(QUERY, &[]).await?)?;
        if (0..4).all(|index| row.get::<bool, _>(index) == Some(true)) {
            Ok(())
        } else {
            Err(AdminError::ObserverPermissions)
        }
    }

    async fn execute(&mut self, sql: &str, parameters: &[&dyn ToSql]) -> Result<(), AdminError> {
        let timeout_value = self.query_timeout;
        timeout(timeout_value, async {
            let mut stream = self
                .client
                .query(sql, parameters)
                .await
                .map_err(|_| AdminError::Query)?;
            while stream
                .try_next()
                .await
                .map_err(|_| AdminError::Query)?
                .is_some()
            {}
            Ok(())
        })
        .await
        .map_err(|_| AdminError::QueryDeadline)?
    }

    async fn query_rows(
        &mut self,
        sql: &str,
        parameters: &[&dyn ToSql],
    ) -> Result<Vec<Row>, AdminError> {
        let timeout_value = self.query_timeout;
        timeout(timeout_value, async {
            let mut stream = self
                .client
                .query(sql, parameters)
                .await
                .map_err(|_| AdminError::Query)?;
            let mut rows = Vec::new();
            while let Some(item) = stream.try_next().await.map_err(|_| AdminError::Query)? {
                if let QueryItem::Row(row) = item {
                    if rows.len() == 16 {
                        return Err(AdminError::MalformedResult);
                    }
                    rows.push(row);
                }
            }
            Ok(rows)
        })
        .await
        .map_err(|_| AdminError::QueryDeadline)?
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminError {
    InvalidEndpoint,
    InvalidDeadline,
    InvalidIdentifier,
    SecretFile,
    Connect,
    ConnectDeadline,
    TlsOrLogin,
    Query,
    QueryDeadline,
    MalformedResult,
    WrongIdentity,
    WrongVersion,
    WrongEdition,
    HadrDisabled,
    InvalidStartIdentity,
    ObserverPermissions,
}

impl fmt::Display for AdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidEndpoint => "administrator endpoint is invalid",
            Self::InvalidDeadline => "administrator deadline is invalid",
            Self::InvalidIdentifier => "administrator SQL identifier is invalid",
            Self::SecretFile => "administrator credential file is invalid",
            Self::Connect => "administrator TCP connection failed",
            Self::ConnectDeadline => "administrator connection deadline exceeded",
            Self::TlsOrLogin => "verified TLS administrator login failed",
            Self::Query => "administrator SQL operation failed",
            Self::QueryDeadline => "administrator SQL deadline exceeded",
            Self::MalformedResult => "administrator SQL result is malformed",
            Self::WrongIdentity => "SQL Server identity does not match the member",
            Self::WrongVersion => "SQL Server version does not match the pinned image",
            Self::WrongEdition => "SQL Server Enterprise feature profile is unavailable",
            Self::HadrDisabled => "SQL Server HADR is disabled",
            Self::InvalidStartIdentity => "SQL Server start identity is invalid",
            Self::ObserverPermissions => "observer login permissions are incomplete",
        })
    }
}

impl std::error::Error for AdminError {}

pub fn validated_identifier(value: &str) -> Result<&str, AdminError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || !value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic())
    {
        return Err(AdminError::InvalidIdentifier);
    }
    Ok(value)
}

fn validate_endpoint(endpoint: &AdminEndpoint) -> Result<(), AdminError> {
    if endpoint.tcp_host != "127.0.0.1"
        || endpoint.tls_hostname != "localhost"
        || endpoint.port == 0
        || !endpoint.ca_certificate.is_absolute()
        || endpoint
            .ca_certificate
            .extension()
            .and_then(|value| value.to_str())
            != Some("crt")
    {
        return Err(AdminError::InvalidEndpoint);
    }
    PrivateFile::inspect(endpoint.ca_certificate.clone()).map_err(|_| AdminError::SecretFile)?;
    Ok(())
}

async fn read_private_value(path: &Path) -> Result<SecretValue, AdminError> {
    PrivateFile::inspect(path.to_path_buf()).map_err(|_| AdminError::SecretFile)?;
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|_| AdminError::SecretFile)?;
    if bytes.is_empty()
        || bytes.len() > 4096
        || bytes
            .iter()
            .any(|byte| matches!(byte, b'\0' | b'\n' | b'\r'))
    {
        return Err(AdminError::SecretFile);
    }
    let value = String::from_utf8(bytes).map_err(|_| AdminError::SecretFile)?;
    Ok(SecretValue::from_test(value))
}

fn single_row(mut rows: Vec<Row>) -> Result<Row, AdminError> {
    if rows.len() != 1 {
        return Err(AdminError::MalformedResult);
    }
    Ok(rows.remove(0))
}

fn required_text(row: &Row, index: usize) -> Result<String, AdminError> {
    row.get::<&str, _>(index)
        .map(str::to_owned)
        .ok_or(AdminError::MalformedResult)
}
