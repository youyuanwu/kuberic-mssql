use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    kuberic_mssql::observer::run_from_env().await
}
