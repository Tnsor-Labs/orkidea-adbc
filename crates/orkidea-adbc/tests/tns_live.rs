//! Connecting through a `tnsnames.ora` alias (`oracle.connect_string` with
//! `oracle.config_dir`).

mod common;

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use arrow_array::cast::AsArray;
use orkidea_adbc::OracleDriver;

/// The host, port, and service of an `oracle://user:password@host:port/service` URI.
fn address(uri: &str) -> Option<(String, String, String)> {
    let rest = uri.strip_prefix("oracle://")?;
    let address = rest.rsplit_once('@')?.1.split('?').next()?;
    let (host_port, service) = address.split_once('/')?;
    let (host, port) = host_port.rsplit_once(':').unwrap_or((host_port, "1521"));
    Some((host.to_string(), port.to_string(), service.to_string()))
}

#[test]
fn connects_through_an_alias_written_after_a_comment() {
    let Ok(uri) = std::env::var("ORKIDEA_ADBC_URI") else {
        eprintln!("skipping live test: ORKIDEA_ADBC_URI is not set");
        return;
    };
    let Some((host, port, service)) = address(&uri) else {
        eprintln!("skipping live test: ORKIDEA_ADBC_URI is not a plain oracle:// URI");
        return;
    };

    // The layout most tnsnames.ora files use: a comment, then a descriptor
    // over several lines.
    let dir = std::env::temp_dir().join(format!("orkidea-tns-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("tnsnames.ora"),
        format!(
            "# Orkidea test alias\n\
             ORKIDEA_TNS =\n  (DESCRIPTION =\n    \
             (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port}))\n    \
             (CONNECT_DATA = (SERVICE_NAME = {service})))\n"
        ),
    )
    .unwrap();

    let mut database = OracleDriver.new_database().unwrap();
    // The URI supplies the credentials; the alias overrides its address.
    database
        .set_option(OptionDatabase::Uri, uri.into())
        .unwrap();
    database
        .set_option(
            OptionDatabase::Other("oracle.connect_string".into()),
            OptionValue::String("orkidea_tns".into()),
        )
        .unwrap();
    database
        .set_option(
            OptionDatabase::Other("oracle.config_dir".into()),
            OptionValue::String(dir.to_str().unwrap().into()),
        )
        .unwrap();
    let mut conn = database.new_connection().unwrap();
    let row = common::single_row(
        &mut conn,
        "SELECT SYS_CONTEXT('USERENV', 'SERVICE_NAME') FROM dual",
    );
    std::fs::remove_dir_all(&dir).unwrap();

    let connected = row.column(0).as_string::<i32>().value(0);
    assert!(
        connected.eq_ignore_ascii_case(&service),
        "connected to {connected}, not {service}"
    );
}
