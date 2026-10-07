use std::path::PathBuf;

use kuberic_mssql_tests::three_replica::cleanup_three_replica_fixture;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("cleanup"))
        || arguments.next().as_deref() != Some(std::ffi::OsStr::new("--root"))
    {
        return Err("usage: mssql-three-replica-fixture cleanup --root <fixture-root>".to_owned());
    }
    let root = arguments.next().map(PathBuf::from).ok_or_else(|| {
        "usage: mssql-three-replica-fixture cleanup --root <fixture-root>".to_owned()
    })?;
    if arguments.next().is_some() {
        return Err("usage: mssql-three-replica-fixture cleanup --root <fixture-root>".to_owned());
    }
    let root = if root.is_absolute() {
        root
    } else {
        std::env::current_dir()
            .map_err(|error| error.to_string())?
            .join(root)
    };
    let evidence = cleanup_three_replica_fixture(&root).map_err(|error| error.to_string())?;
    println!(
        "three-replica cleanup complete: containers={}, journal={}",
        evidence.removed_container_ids.len(),
        evidence.journal_path.display()
    );
    Ok(())
}
