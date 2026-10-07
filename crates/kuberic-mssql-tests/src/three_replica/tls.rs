use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::cleanup::{CleanupClock, OperationBudget, SystemCleanupClock};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SharedTlsAssets {
    pub ca_certificate: PrivateFile,
    pub ca_private_key: PrivateFile,
    pub members: Vec<MemberTlsAssets>,
}

pub(crate) struct TlsGenerationRequest<'a> {
    pub root: &'a Path,
    pub hostnames: &'a [&'a str],
    pub member_data_directories: &'a [&'a Path],
    pub ca_common_name: &'a str,
    pub timeout: Duration,
}

pub trait TlsAssetRecorder {
    fn create_directory(&mut self, logical_name: &str, path: &Path) -> Result<(), TlsError>;
    fn create_text_file(
        &mut self,
        logical_name: &str,
        path: &Path,
        contents: &str,
    ) -> Result<PrivateFile, TlsError>;
    fn dispatch_file(&mut self, logical_name: &str, path: &Path) -> Result<usize, TlsError>;
    fn bind_file(
        &mut self,
        record_index: usize,
        path: &Path,
        sql_shared: bool,
    ) -> Result<PrivateFile, TlsError>;
}

struct DirectTlsAssetRecorder;

impl TlsAssetRecorder for DirectTlsAssetRecorder {
    fn create_directory(&mut self, _: &str, path: &Path) -> Result<(), TlsError> {
        create_private_directory(path).map_err(TlsError::Secret)
    }

    fn create_text_file(
        &mut self,
        _: &str,
        path: &Path,
        contents: &str,
    ) -> Result<PrivateFile, TlsError> {
        PrivateFile::create_text(path, contents).map_err(TlsError::Secret)
    }

    fn dispatch_file(&mut self, _: &str, _: &Path) -> Result<usize, TlsError> {
        Ok(0)
    }

    fn bind_file(
        &mut self,
        _: usize,
        path: &Path,
        sql_shared: bool,
    ) -> Result<PrivateFile, TlsError> {
        if sql_shared {
            PrivateFile::inspect_sql_shared(path).map_err(TlsError::Secret)
        } else {
            PrivateFile::inspect(path).map_err(TlsError::Secret)
        }
    }
}

impl TlsAssets {
    pub fn generate(
        root: &Path,
        hostnames: [&str; 3],
        member_data_directories: [&Path; 3],
        timeout: Duration,
        runner: &impl ProcessRunner,
    ) -> Result<Self, TlsError> {
        Self::generate_recorded(
            root,
            hostnames,
            member_data_directories,
            timeout,
            runner,
            &mut DirectTlsAssetRecorder,
        )
    }

    pub fn generate_recorded(
        root: &Path,
        hostnames: [&str; 3],
        member_data_directories: [&Path; 3],
        timeout: Duration,
        runner: &impl ProcessRunner,
        recorder: &mut impl TlsAssetRecorder,
    ) -> Result<Self, TlsError> {
        Self::generate_recorded_with_clock(
            root,
            hostnames,
            member_data_directories,
            timeout,
            runner,
            recorder,
            &SystemCleanupClock::default(),
        )
    }

    fn generate_recorded_with_clock<C: CleanupClock>(
        root: &Path,
        hostnames: [&str; 3],
        member_data_directories: [&Path; 3],
        timeout: Duration,
        runner: &impl ProcessRunner,
        recorder: &mut impl TlsAssetRecorder,
        clock: &C,
    ) -> Result<Self, TlsError> {
        let shared = SharedTlsAssets::generate_recorded_with_clock(
            TlsGenerationRequest {
                root,
                hostnames: &hostnames,
                member_data_directories: &member_data_directories,
                ca_common_name: "Kuberic three replica test CA",
                timeout,
            },
            runner,
            recorder,
            clock,
        )?;
        Ok(Self {
            ca_certificate: shared.ca_certificate,
            ca_private_key: shared.ca_private_key,
            members: shared.members.try_into().map_err(|_| TlsError::Helper)?,
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

impl SharedTlsAssets {
    pub(crate) fn generate_recorded_with_clock<C: CleanupClock>(
        request: TlsGenerationRequest<'_>,
        runner: &impl ProcessRunner,
        recorder: &mut impl TlsAssetRecorder,
        clock: &C,
    ) -> Result<Self, TlsError> {
        if request.hostnames.is_empty()
            || request.hostnames.len() != request.member_data_directories.len()
        {
            return Err(TlsError::Helper);
        }
        let budget = OperationBudget::new(clock, request.timeout);
        for hostname in request.hostnames {
            validate_hostname(hostname)?;
        }
        let tls_root = request.root.join("tls");
        recorder.create_directory("tls-directory", &tls_root)?;
        let exchange_root = request.root.join("endpoint-exchange");
        recorder.create_directory("endpoint-exchange-directory", &exchange_root)?;

        let ca_key = tls_root.join("ca.key");
        let ca_certificate = tls_root.join("ca.crt");
        let ca_key_record = recorder.dispatch_file("tls-ca-private-key", &ca_key)?;
        run_openssl(
            runner,
            &budget,
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
        make_private(&ca_key)?;
        let ca_private_key = recorder.bind_file(ca_key_record, &ca_key, false)?;
        let ca_certificate_record =
            recorder.dispatch_file("tls-ca-certificate", &ca_certificate)?;
        run_openssl(
            runner,
            &budget,
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
                &format!("/CN={}", request.ca_common_name),
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
                "-out",
                path_text(&ca_certificate)?,
            ],
        )?;
        make_private(&ca_certificate)?;
        let ca_certificate_file =
            recorder.bind_file(ca_certificate_record, &ca_certificate, false)?;

        let serial = tls_root.join("ca.srl");
        let serial_record = recorder.dispatch_file("tls-ca-serial", &serial)?;
        let mut members = Vec::with_capacity(request.hostnames.len());
        for index in 0..request.hostnames.len() {
            let secrets = request.member_data_directories[index].join("secrets");
            recorder
                .create_directory(&format!("member-{}-secrets-directory", index + 1), &secrets)?;
            configure_sql_directory(&secrets, &budget, runner)?;
            let kuberic = secrets.join("kuberic");
            recorder.create_directory(&format!("member-{}-tls-directory", index + 1), &kuberic)?;
            configure_sql_directory(&kuberic, &budget, runner)?;
            let key = kuberic.join("server.key");
            let certificate_request = tls_root.join(format!("server-{}.csr", index + 1));
            let extensions = tls_root.join(format!("server-{}.ext", index + 1));
            let certificate = kuberic.join("server.crt");
            let extension_file = recorder.create_text_file(
                &format!("tls-server-{}-extensions", index + 1),
                &extensions,
                &format!(
                    "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:{}\n\
                     extendedKeyUsage=serverAuth\n\
                     keyUsage=critical,digitalSignature,keyEncipherment\n\
                     basicConstraints=critical,CA:FALSE\n",
                    request.hostnames[index]
                ),
            )?;
            let key_record =
                recorder.dispatch_file(&format!("tls-server-{}-private-key", index + 1), &key)?;
            run_openssl(
                runner,
                &budget,
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
            make_private(&key)?;
            share_file_with_sql(&key, &budget, runner)?;
            let key_file = recorder.bind_file(key_record, &key, true)?;
            let request_record = recorder.dispatch_file(
                &format!("tls-server-{}-request", index + 1),
                &certificate_request,
            )?;
            run_openssl(
                runner,
                &budget,
                "generate SQL Server TLS request",
                [
                    "req",
                    "-new",
                    "-sha256",
                    "-key",
                    path_text(&key)?,
                    "-subj",
                    &format!("/CN={}", request.hostnames[index]),
                    "-out",
                    path_text(&certificate_request)?,
                ],
            )?;
            make_private(&certificate_request)?;
            let _request_file = recorder.bind_file(request_record, &certificate_request, false)?;
            let certificate_record = recorder.dispatch_file(
                &format!("tls-server-{}-certificate", index + 1),
                &certificate,
            )?;
            let mut arguments = vec![
                "x509".to_owned(),
                "-req".to_owned(),
                "-sha256".to_owned(),
                "-days".to_owned(),
                "2".to_owned(),
                "-in".to_owned(),
                path_text(&certificate_request)?.to_owned(),
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
                &budget,
                "sign SQL Server TLS certificate",
                &arguments,
            )?;
            make_private(&certificate)?;
            share_file_with_sql(&certificate, &budget, runner)?;
            let certificate_file = recorder.bind_file(certificate_record, &certificate, true)?;
            drop(extension_file);
            let exchange = exchange_root.join(format!("member-{}", index + 1));
            recorder.create_directory(
                &format!("endpoint-exchange-member-{}", index + 1),
                &exchange,
            )?;
            members.push(MemberTlsAssets {
                server_certificate: certificate_file,
                server_private_key: key_file,
                endpoint_exchange_directory: exchange,
            });
        }
        make_private(&serial)?;
        let _serial_file = recorder.bind_file(serial_record, &serial, false)?;
        Ok(Self {
            ca_certificate: ca_certificate_file,
            ca_private_key,
            members,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsError {
    InvalidHostname,
    InvalidPath,
    UnsafeRequest,
    Deadline,
    Helper,
    Journal,
    Secret(SecretError),
}

impl fmt::Display for TlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidHostname => "TLS hostname is invalid",
            Self::InvalidPath => "TLS asset path is invalid",
            Self::UnsafeRequest => "OpenSSL request exceeds the fixed safety bounds",
            Self::Deadline => "TLS helper deadline exceeded",
            Self::Helper => "OpenSSL TLS helper failed",
            Self::Journal => "TLS ownership journal update failed",
            Self::Secret(error) => {
                return write!(formatter, "TLS private asset operation failed: {error}");
            }
        })
    }
}

impl std::error::Error for TlsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Secret(error) => Some(error),
            _ => None,
        }
    }
}

fn run_openssl<I, S>(
    runner: &impl ProcessRunner,
    budget: &OperationBudget<'_, impl CleanupClock>,
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
    run_openssl_vec(runner, budget, diagnostic, &arguments)
}

fn run_openssl_vec(
    runner: &impl ProcessRunner,
    budget: &OperationBudget<'_, impl CleanupClock>,
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
    let timeout = budget.remaining().ok_or(TlsError::Deadline)?;
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

fn configure_sql_directory(
    path: &Path,
    budget: &OperationBudget<'_, impl CleanupClock>,
    runner: &impl ProcessRunner,
) -> Result<(), TlsError> {
    let timeout = budget.remaining().ok_or(TlsError::Deadline)?;
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
    budget: &OperationBudget<'_, impl CleanupClock>,
    runner: &impl ProcessRunner,
) -> Result<(), TlsError> {
    let timeout = budget.remaining().ok_or(TlsError::Deadline)?;
    runner
        .run(
            &CommandSpec::new("setfacl", "configure SQL TLS file ACL", timeout)
                .args(["-m", &format!("u:{SQL_SERVER_UID}:r,m:r")])
                .arg(path),
        )
        .map(|_| ())
        .map_err(|_| TlsError::Helper)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::three_replica::process::{ChildDisposition, ProcessError, ProcessResult};

    #[derive(Clone)]
    struct FakeClock {
        now: Arc<Mutex<Duration>>,
    }

    impl CleanupClock for FakeClock {
        fn now(&self) -> Duration {
            *self.now.lock().unwrap()
        }
    }

    struct AdvancingRunner {
        clock: FakeClock,
        elapsed_per_call: Duration,
        timeouts: Arc<Mutex<Vec<Duration>>>,
    }

    impl ProcessRunner for AdvancingRunner {
        fn run(&self, command: &CommandSpec) -> Result<ProcessResult, ProcessError> {
            self.timeouts.lock().unwrap().push(command.timeout());
            let mut now = self.clock.now.lock().unwrap();
            *now += self.elapsed_per_call;
            Ok(ProcessResult {
                status: 0,
                stdout: String::new(),
                stderr: String::new(),
                child: ChildDisposition::default(),
            })
        }
    }

    #[test]
    fn sequential_tls_helpers_receive_decreasing_parent_budget() {
        let clock = FakeClock {
            now: Arc::new(Mutex::new(Duration::ZERO)),
        };
        let timeouts = Arc::new(Mutex::new(Vec::new()));
        let runner = AdvancingRunner {
            clock: clock.clone(),
            elapsed_per_call: Duration::from_secs(7),
            timeouts: timeouts.clone(),
        };
        let budget = OperationBudget::new(&clock, Duration::from_secs(30));

        for diagnostic in ["first", "second", "third"] {
            run_openssl(&runner, &budget, diagnostic, ["version"]).unwrap();
        }

        assert_eq!(
            timeouts.lock().unwrap().as_slice(),
            [
                Duration::from_secs(30),
                Duration::from_secs(23),
                Duration::from_secs(16),
            ]
        );
    }
}
