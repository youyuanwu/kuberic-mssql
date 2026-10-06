use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::docker::SQL_SERVER_UID;
use super::process::{CommandSpec, ProcessRunner};
use super::secrets::{PrivateFile, SecretError, create_private_directory};

const MAX_OPENSSL_ARGUMENTS: usize = 32;
const MAX_OPENSSL_ARGUMENT_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberTlsAssets {
    pub server_certificate: PrivateFile,
    pub server_private_key: PrivateFile,
    pub endpoint_exchange_directory: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsAssets {
    pub ca_certificate: PrivateFile,
    pub ca_private_key: PrivateFile,
    pub members: [MemberTlsAssets; 3],
}

impl TlsAssets {
    pub fn generate(
        root: &Path,
        hostnames: [&str; 3],
        member_data_directories: [&Path; 3],
        timeout: Duration,
        runner: &impl ProcessRunner,
    ) -> Result<Self, TlsError> {
        for hostname in hostnames {
            validate_hostname(hostname)?;
        }
        let tls_root = root.join("tls");
        create_private_directory(&tls_root).map_err(TlsError::Secret)?;
        let exchange_root = root.join("endpoint-exchange");
        create_private_directory(&exchange_root).map_err(TlsError::Secret)?;

        let ca_key = tls_root.join("ca.key");
        let ca_certificate = tls_root.join("ca.crt");
        run_openssl(
            runner,
            timeout,
            "generate TLS CA private key",
            [
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
                path_text(&ca_key)?,
            ],
        )?;
        run_openssl(
            runner,
            timeout,
            "generate TLS CA certificate",
            [
                "req",
                "-x509",
                "-new",
                "-sha256",
                "-days",
                "2",
                "-key",
                path_text(&ca_key)?,
                "-subj",
                "/CN=Kuberic three replica test CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
                "-out",
                path_text(&ca_certificate)?,
            ],
        )?;
        make_private(&ca_key)?;
        make_private(&ca_certificate)?;

        let mut members = Vec::with_capacity(3);
        for index in 0..3 {
            let secrets = member_data_directories[index].join("secrets");
            create_sql_directory(&secrets, timeout, runner)?;
            let kuberic = secrets.join("kuberic");
            create_sql_directory(&kuberic, timeout, runner)?;
            let key = kuberic.join("server.key");
            let request = tls_root.join(format!("server-{}.csr", index + 1));
            let extensions = tls_root.join(format!("server-{}.ext", index + 1));
            let certificate = kuberic.join("server.crt");
            PrivateFile::create_text(
                &extensions,
                format!(
                    "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:{}\n\
                     extendedKeyUsage=serverAuth\n\
                     keyUsage=critical,digitalSignature,keyEncipherment\n\
                     basicConstraints=critical,CA:FALSE\n",
                    hostnames[index]
                ),
            )
            .map_err(TlsError::Secret)?;
            run_openssl(
                runner,
                timeout,
                "generate SQL Server TLS private key",
                [
                    "genpkey",
                    "-algorithm",
                    "RSA",
                    "-pkeyopt",
                    "rsa_keygen_bits:2048",
                    "-out",
                    path_text(&key)?,
                ],
            )?;
            run_openssl(
                runner,
                timeout,
                "generate SQL Server TLS request",
                [
                    "req",
                    "-new",
                    "-sha256",
                    "-key",
                    path_text(&key)?,
                    "-subj",
                    &format!("/CN={}", hostnames[index]),
                    "-out",
                    path_text(&request)?,
                ],
            )?;
            let serial = tls_root.join("ca.srl");
            let mut arguments = vec![
                "x509".to_owned(),
                "-req".to_owned(),
                "-sha256".to_owned(),
                "-days".to_owned(),
                "2".to_owned(),
                "-in".to_owned(),
                path_text(&request)?.to_owned(),
                "-CA".to_owned(),
                path_text(&ca_certificate)?.to_owned(),
                "-CAkey".to_owned(),
                path_text(&ca_key)?.to_owned(),
            ];
            if index == 0 {
                arguments.push("-CAcreateserial".to_owned());
            } else {
                arguments.extend(["-CAserial".to_owned(), path_text(&serial)?.to_owned()]);
            }
            arguments.extend([
                "-extfile".to_owned(),
                path_text(&extensions)?.to_owned(),
                "-out".to_owned(),
                path_text(&certificate)?.to_owned(),
            ]);
            run_openssl_vec(
                runner,
                timeout,
                "sign SQL Server TLS certificate",
                &arguments,
            )?;
            make_private(&key)?;
            make_private(&certificate)?;
            make_private(&request)?;
            share_file_with_sql(&key, timeout, runner)?;
            share_file_with_sql(&certificate, timeout, runner)?;
            let exchange = exchange_root.join(format!("member-{}", index + 1));
            create_private_directory(&exchange).map_err(TlsError::Secret)?;
            members.push(MemberTlsAssets {
                server_certificate: PrivateFile::inspect_sql_shared(certificate)
                    .map_err(TlsError::Secret)?,
                server_private_key: PrivateFile::inspect_sql_shared(key)
                    .map_err(TlsError::Secret)?,
                endpoint_exchange_directory: exchange,
            });
        }
        let members = members.try_into().map_err(|_| TlsError::Helper)?;
        Ok(Self {
            ca_certificate: PrivateFile::inspect(ca_certificate).map_err(TlsError::Secret)?,
            ca_private_key: PrivateFile::inspect(ca_key).map_err(TlsError::Secret)?,
            members,
        })
    }

    pub fn private_files(&self) -> impl Iterator<Item = &PrivateFile> {
        [
            &self.ca_certificate,
            &self.ca_private_key,
            &self.members[0].server_certificate,
            &self.members[0].server_private_key,
            &self.members[1].server_certificate,
            &self.members[1].server_private_key,
            &self.members[2].server_certificate,
            &self.members[2].server_private_key,
        ]
        .into_iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsError {
    InvalidHostname,
    InvalidPath,
    UnsafeRequest,
    Helper,
    Secret(SecretError),
}

impl fmt::Display for TlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidHostname => "TLS hostname is invalid",
            Self::InvalidPath => "TLS asset path is invalid",
            Self::UnsafeRequest => "OpenSSL request exceeds the fixed safety bounds",
            Self::Helper => "OpenSSL TLS helper failed",
            Self::Secret(_) => "TLS private asset operation failed",
        })
    }
}

impl std::error::Error for TlsError {}

fn run_openssl<I, S>(
    runner: &impl ProcessRunner,
    timeout: Duration,
    diagnostic: &str,
    arguments: I,
) -> Result<(), TlsError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let arguments = arguments
        .into_iter()
        .map(|argument| argument.as_ref().to_owned())
        .collect::<Vec<_>>();
    run_openssl_vec(runner, timeout, diagnostic, &arguments)
}

fn run_openssl_vec(
    runner: &impl ProcessRunner,
    timeout: Duration,
    diagnostic: &str,
    arguments: &[String],
) -> Result<(), TlsError> {
    if arguments.is_empty()
        || arguments.len() > MAX_OPENSSL_ARGUMENTS
        || arguments
            .iter()
            .any(|argument| argument.len() > MAX_OPENSSL_ARGUMENT_BYTES || argument.contains('\0'))
    {
        return Err(TlsError::UnsafeRequest);
    }
    runner
        .run(&CommandSpec::new("openssl", diagnostic, timeout).args(arguments))
        .map(|_| ())
        .map_err(|_| TlsError::Helper)
}

fn validate_hostname(hostname: &str) -> Result<(), TlsError> {
    if hostname.is_empty()
        || hostname.len() > 15
        || hostname.starts_with('-')
        || hostname.ends_with('-')
        || !hostname
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(TlsError::InvalidHostname);
    }
    Ok(())
}

fn path_text(path: &Path) -> Result<&str, TlsError> {
    path.to_str().ok_or(TlsError::InvalidPath)
}

fn make_private(path: &Path) -> Result<(), TlsError> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|_| TlsError::Helper)
}

fn create_sql_directory(
    path: &Path,
    timeout: Duration,
    runner: &impl ProcessRunner,
) -> Result<(), TlsError> {
    fs::create_dir(path).map_err(|_| TlsError::Helper)?;
    runner
        .run(
            &CommandSpec::new("setfacl", "configure SQL TLS directory ACL", timeout)
                .args([
                    "-m",
                    &format!("u:{SQL_SERVER_UID}:rwx,d:u:{SQL_SERVER_UID}:rwx,m:rwx,d:m:rwx"),
                ])
                .arg(path),
        )
        .map(|_| ())
        .map_err(|_| TlsError::Helper)
}

fn share_file_with_sql(
    path: &Path,
    timeout: Duration,
    runner: &impl ProcessRunner,
) -> Result<(), TlsError> {
    runner
        .run(
            &CommandSpec::new("setfacl", "configure SQL TLS file ACL", timeout)
                .args(["-m", &format!("u:{SQL_SERVER_UID}:r,m:r")])
                .arg(path),
        )
        .map(|_| ())
        .map_err(|_| TlsError::Helper)
}
