use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    kuberic_mssql::runtime_host::run_from_env().await
}
