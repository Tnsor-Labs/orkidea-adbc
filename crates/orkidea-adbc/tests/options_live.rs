//! Live integration tests for database and connection options.
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test options_live
//! ```

use adbc_core::options::{OptionConnection, OptionDatabase};
use adbc_core::{Database, Driver, Optionable};
use arrow_array::cast::AsArray;
use arrow_array::Array;
use orkidea_adbc::{OracleConnection, OracleDriver};

mod common;
use common::*;

fn uri() -> Option<String> {
    std::env::var("ORKIDEA_ADBC_URI").ok()
}

fn session_value(conn: &mut OracleConnection, expression: &str) -> Option<String> {
    let row = single_row(conn, &format!("SELECT {expression} AS v FROM dual"));
    let column = row.column(0).as_string::<i32>();
    (!column.is_null(0)).then(|| column.value(0).to_string())
}

#[test]
fn secrets_are_write_only() {
    let Some(uri) = uri() else { return };
    let mut db = OracleDriver.new_database().unwrap();
    db.set_option(OptionDatabase::Uri, uri.clone().into())
        .unwrap();
    db.set_option(OptionDatabase::Password, "secret".into())
        .unwrap();
    assert!(db.get_option_string(OptionDatabase::Password).is_err());
    let shown = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert!(shown.contains(":***@"), "{shown}");
    let password = uri
        .split("://")
        .nth(1)
        .unwrap()
        .split('@')
        .next()
        .unwrap()
        .split(':')
        .nth(1)
        .unwrap();
    assert!(!shown.contains(password));
}

#[test]
fn session_options_apply_to_new_connections() {
    let Some(uri) = uri() else { return };
    let mut db = OracleDriver.new_database().unwrap();
    db.set_option(OptionDatabase::Uri, uri.into()).unwrap();
    for (key, value) in [
        ("oracle.time_zone", "-03:30"),
        ("oracle.module", "orkidea-tests"),
        ("oracle.action", "options_live"),
        ("oracle.client_identifier", "tester@example"),
        ("oracle.client_info", "ci"),
    ] {
        db.set_option(OptionDatabase::Other(key.into()), value.into())
            .unwrap();
    }
    let mut conn = db.new_connection().unwrap();
    assert_eq!(
        session_value(&mut conn, "SESSIONTIMEZONE").as_deref(),
        Some("-03:30")
    );
    assert_eq!(
        session_value(&mut conn, "SYS_CONTEXT('USERENV', 'MODULE')").as_deref(),
        Some("orkidea-tests")
    );
    assert_eq!(
        session_value(&mut conn, "SYS_CONTEXT('USERENV', 'ACTION')").as_deref(),
        Some("options_live")
    );
    assert_eq!(
        session_value(&mut conn, "SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')").as_deref(),
        Some("tester@example")
    );
    assert_eq!(
        session_value(&mut conn, "SYS_CONTEXT('USERENV', 'CLIENT_INFO')").as_deref(),
        Some("ci")
    );

    // Connection-level changes take effect on the next round trip.
    conn.set_option(
        OptionConnection::Other("oracle.action".into()),
        "step-2".into(),
    )
    .unwrap();
    assert_eq!(
        session_value(&mut conn, "SYS_CONTEXT('USERENV', 'ACTION')").as_deref(),
        Some("step-2")
    );
    assert_eq!(
        conn.get_option_string(OptionConnection::Other("oracle.action".into()))
            .unwrap(),
        "step-2"
    );
}

/// A SYSDBA connection for tests that need administrative privileges, from
/// `ORKIDEA_ADBC_ADMIN_URI` (for example `oracle://sys:...@host:1521/FREEPDB1`).
fn admin() -> Option<OracleConnection> {
    let Ok(uri) = std::env::var("ORKIDEA_ADBC_ADMIN_URI") else {
        eprintln!("skipping admin test: ORKIDEA_ADBC_ADMIN_URI is not set");
        return None;
    };
    let mut db = OracleDriver.new_database().unwrap();
    db.set_option(OptionDatabase::Uri, uri.into()).unwrap();
    db.set_option(
        OptionDatabase::Other("oracle.auth_mode".into()),
        "sysdba".into(),
    )
    .unwrap();
    Some(db.new_connection().unwrap())
}

#[test]
fn sysdba_auth_mode_connects_with_dba_privileges() {
    let Some(mut sys) = admin() else { return };
    assert_eq!(
        session_value(&mut sys, "SYS_CONTEXT('USERENV', 'ISDBA')").as_deref(),
        Some("TRUE")
    );
    // The driver reports its name and version to the server.
    let driver = session_value(
        &mut sys,
        "(SELECT MAX(client_driver) FROM v$session_connect_info \
          WHERE sid = SYS_CONTEXT('USERENV', 'SID'))",
    )
    .unwrap_or_default();
    assert!(driver.starts_with("orkidea"), "client driver {driver:?}");
}

#[test]
fn invalid_auth_mode_is_rejected_at_set_time() {
    let mut db = OracleDriver.new_database().unwrap();
    assert!(db
        .set_option(
            OptionDatabase::Other("oracle.auth_mode".into()),
            "root".into()
        )
        .is_err());
    db.set_option(
        OptionDatabase::Other("oracle.auth_mode".into()),
        "sysdba".into(),
    )
    .unwrap();
}
