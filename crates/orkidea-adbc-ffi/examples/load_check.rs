//! Release smoke test: loads the built driver through the ADBC driver manager
//! and exercises it without a database.
//!
//! ```text
//! load_check --path target/release/libadbc_driver_orkidea.so
//! load_check --name orkidea      # resolve an installed driver manifest
//! ```
//!
//! It checks that the shared library loads, that its entrypoint initialises,
//! that driver code runs (an unknown option is rejected), and that a
//! connection attempt to a closed port fails with an I/O error rather than
//! crashing. Exits non-zero on any failure.

use std::process::ExitCode;

use adbc_core::error::Status;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database, Driver, Optionable, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut driver = match args.as_slice() {
        [flag, path] if flag == "--path" => ManagedDriver::load_dynamic_from_filename(
            path,
            Some(b"OrkideaDriverInit"),
            AdbcVersion::V110,
        ),
        [flag, name] if flag == "--name" => {
            ManagedDriver::load_from_name(name, None, AdbcVersion::V110, LOAD_FLAG_DEFAULT, None)
        }
        _ => return Err("usage: load_check --path <library> | --name <driver>".into()),
    }
    .map_err(|e| format!("loading the driver failed: {e}"))?;
    println!("driver loaded");

    let mut database = driver
        .new_database()
        .map_err(|e| format!("creating a database failed: {e}"))?;
    match database.set_option(
        OptionDatabase::Other("oracle.no_such_option".into()),
        "x".into(),
    ) {
        Err(e) if e.status == Status::InvalidArguments => println!("option validation works"),
        other => return Err(format!("unknown option was not rejected: {other:?}")),
    }

    // Port 9 (discard) is closed on CI runners: the connect must fail cleanly.
    for (key, value) in [
        (
            OptionDatabase::Uri,
            "oracle://probe:probe@127.0.0.1:9/probe",
        ),
        (
            OptionDatabase::Other("oracle.connect_timeout_ms".into()),
            "3000",
        ),
    ] {
        database
            .set_option(key, value.into())
            .map_err(|e| format!("setting an option failed: {e}"))?;
    }
    match database.new_connection() {
        Err(e) if e.status == Status::IO => {
            println!(
                "connection to a closed port failed as expected: {}",
                e.message
            );
            Ok(())
        }
        Err(e) => Err(format!("unexpected connection error: {e:?}")),
        Ok(_) => Err("connected to a closed port".into()),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => {
            println!("load check passed");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("load check failed: {message}");
            ExitCode::FAILURE
        }
    }
}
