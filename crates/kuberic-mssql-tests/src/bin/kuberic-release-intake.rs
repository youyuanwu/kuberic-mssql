use std::process::ExitCode;

use kuberic_mssql_tests::release_intake::{
    EXIT_INVALID, outcome_exit_code, parse_and_validate_json, render_summary,
};

fn main() -> ExitCode {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(path) = arguments.next() else {
        eprintln!("usage: kuberic-release-intake <record.json>");
        return exit_code(EXIT_INVALID);
    };
    if arguments.next().is_some() {
        eprintln!("usage: kuberic-release-intake <record.json>");
        return exit_code(EXIT_INVALID);
    }
    let input = match std::fs::read(&path) {
        Ok(input) => input,
        Err(error) => {
            eprintln!("release intake invalid: cannot read {:?}: {error}", path);
            return exit_code(EXIT_INVALID);
        }
    };
    match parse_and_validate_json(&input) {
        Ok((record, validated)) => {
            print!("{}", render_summary(&record, &validated));
            exit_code(outcome_exit_code(validated.outcome()))
        }
        Err(error) => {
            eprintln!("release intake invalid: {error}");
            exit_code(EXIT_INVALID)
        }
    }
}

fn exit_code(code: i32) -> ExitCode {
    ExitCode::from(u8::try_from(code).expect("release intake exit codes fit in u8"))
}
