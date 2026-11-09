//! Helpers shared by the live integration tests.
//!
//! Tests are skipped when `ORKIDEA_ADBC_URI` is not set.

#![allow(dead_code)]

use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use orkidea_adbc::{OracleConnection, OracleDatabase, OracleDriver, ORACLE_TYPE_NAME_KEY};

pub fn database() -> Option<OracleDatabase> {
    let Ok(uri) = std::env::var("ORKIDEA_ADBC_URI") else {
        eprintln!("skipping live test: ORKIDEA_ADBC_URI is not set");
        return None;
    };
    let mut database = OracleDriver.new_database().unwrap();
    database
        .set_option(OptionDatabase::Uri, uri.into())
        .unwrap();
    Some(database)
}

pub fn connect() -> Option<OracleConnection> {
    database().map(|db| db.new_connection().unwrap())
}

pub fn statement_option(key: &str) -> OptionStatement {
    OptionStatement::Other(key.to_string())
}

/// Executes `sql` and returns the schema and every batch.
pub fn query(conn: &mut OracleConnection, sql: &str) -> (SchemaRef, Vec<RecordBatch>) {
    query_with(conn, sql, &[])
}

pub fn query_with(
    conn: &mut OracleConnection,
    sql: &str,
    options: &[(&str, OptionValue)],
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut stmt = conn.new_statement().unwrap();
    for (key, value) in options {
        stmt.set_option(statement_option(key), value.clone())
            .unwrap();
    }
    stmt.set_sql_query(sql).unwrap();
    let reader = stmt.execute().unwrap();
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    (schema, batches)
}

pub fn single_row(conn: &mut OracleConnection, sql: &str) -> RecordBatch {
    let (_, batches) = query(conn, sql);
    assert_eq!(batches.len(), 1, "expected one batch for {sql}");
    assert_eq!(batches[0].num_rows(), 1, "expected one row for {sql}");
    batches[0].clone()
}

pub fn exec(conn: &mut OracleConnection, sql: &str) {
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(sql).unwrap();
    stmt.execute_update().unwrap();
}

pub fn drop_table(conn: &mut OracleConnection, table: &str) {
    exec(
        conn,
        &format!(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table} PURGE'; \
             EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        ),
    );
}

pub fn field<'a>(schema: &'a Schema, name: &str) -> &'a Field {
    schema.field_with_name(name).unwrap()
}

pub fn type_name(schema: &Schema, name: &str) -> String {
    field(schema, name)
        .metadata()
        .get(ORACLE_TYPE_NAME_KEY)
        .cloned()
        .unwrap_or_default()
}
