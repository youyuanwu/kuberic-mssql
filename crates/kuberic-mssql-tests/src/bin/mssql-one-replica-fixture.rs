use std::path::PathBuf;
use std::process::ExitCode;

use kuberic_mssql_tests::one_replica::cleanup_one_replica_fixture;

fn main() -> ExitCode {
    match run(std::env::args_os().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: Vec<std::ffi::OsString>) -> Result<(), String> {
    if arguments.len() != 3 || arguments[0] != "cleanup" || arguments[1] != "--root" {
        return Err("usage: mssql-one-replica-fixture cleanup --root <fixture-root>".to_owned());
    }
    let root = PathBuf::from(&arguments[2]);
    let evidence = cleanup_one_replica_fixture(root).map_err(|error| error.to_string())?;
    println!(
        "one-replica cleanup complete: removed={}, unresolved={}, journal={}",
        evidence.report.removed.len(),
        evidence.report.unresolved.len(),
        evidence
            .journal_path
            .as_deref()
            .map_or_else(|| "absent".to_owned(), |path| path.display().to_string())
    );
    Ok(())
}
