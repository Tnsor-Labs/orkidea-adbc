use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::options::{InfoCode, IngestMode, OptionDatabase, OptionStatement};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use orkidea_adbc::OracleDriver;

fn database() -> Option<orkidea_adbc::OracleDatabase> {
    let Ok(uri) = std::env::var("ORKIDEA_ADBC_URI") else {
        eprintln!("skipping Oracle smoke test: ORKIDEA_ADBC_URI is not set");
        return None;
    };
    let mut driver = OracleDriver;
    let mut database = driver.new_database().unwrap();
    database
        .set_option(OptionDatabase::Uri, uri.into())
        .unwrap();
    Some(database)
}

#[test]
fn query_arrow_against_free() {
    let Some(database) = database() else { return };
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query(
            "SELECT 1 AS value, 'hello' AS text FROM dual UNION ALL \
             SELECT 2, CAST(NULL AS VARCHAR2(20)) FROM dual",
        )
        .unwrap();
    let mut reader = statement.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(1),
        2.0
    );
    assert!(batch.column(1).is_null(1));
}

#[test]
fn get_info_against_free() {
    let Some(database) = database() else { return };
    let connection = database.new_connection().unwrap();
    let mut reader = connection
        .get_info(Some(HashSet::from([InfoCode::DriverName])))
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).len(), 1);
}

#[test]
fn ingest_arrow_against_free() {
    let Some(database) = database() else { return };
    let mut connection = database.new_connection().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("safe"), Some("value")])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut statement = connection.new_statement().unwrap();
    statement
        .set_option(OptionStatement::IngestMode, IngestMode::Replace.into())
        .unwrap();
    statement
        .set_option(
            OptionStatement::TargetTable,
            "ADBC_ORACLE_ARROW_SMOKE".into(),
        )
        .unwrap();
    statement.bind(batch).unwrap();
    assert_eq!(statement.execute_update().unwrap(), Some(2));

    let mut query = connection.new_statement().unwrap();
    query
        .set_sql_query("SELECT id, text FROM ADBC_ORACLE_ARROW_SMOKE ORDER BY id")
        .unwrap();
    let mut reader = query.execute().unwrap();
    let result = reader.next().unwrap().unwrap();
    assert_eq!(result.num_rows(), 2);
}
