//! Live integration tests for SQL that uses a bind placeholder more than
//! once. In SQL each use is a bind of its own; bound by name, one parameter
//! field supplies every use of its placeholder.
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test repeated_binds_live
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::options::OptionValue;
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{
    Array, ArrayRef, DurationSecondArray, Int64Array, NullArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use orkidea_adbc::{OracleConnection, ORACLE_BIND_TYPE_KEY};

mod common;
use common::*;

const BIND_BY_NAME: &str = "adbc.statement.bind_by_name";

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// Runs `sql` with `params`, by name when `by_name` says so (and in the
/// default mode otherwise), and returns every result batch.
fn run(
    conn: &mut OracleConnection,
    sql: &str,
    params: RecordBatch,
    by_name: Option<bool>,
) -> Vec<RecordBatch> {
    let mut stmt = conn.new_statement().unwrap();
    if let Some(by_name) = by_name {
        stmt.set_option(
            statement_option(BIND_BY_NAME),
            OptionValue::String(by_name.to_string()),
        )
        .unwrap();
    }
    stmt.set_sql_query(sql).unwrap();
    stmt.bind(params).unwrap();
    stmt.execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn update(conn: &mut OracleConnection, sql: &str, params: Option<RecordBatch>) -> Option<i64> {
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        statement_option(BIND_BY_NAME),
        OptionValue::String("true".into()),
    )
    .unwrap();
    stmt.set_sql_query(sql).unwrap();
    if let Some(params) = params {
        stmt.bind(params).unwrap();
    }
    stmt.execute_update().unwrap()
}

fn numbers(batches: &[RecordBatch], column: usize) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|b| {
            let values = b.column(column).as_primitive::<Float64Type>();
            (0..values.len())
                .map(|i| values.value(i))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn texts(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let values = b.column(column).as_string::<i32>();
            (0..values.len())
                .map(|i| values.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn a_query_binds_one_field_to_every_use_of_its_name() {
    let Some(mut conn) = connect() else { return };
    let sql = "SELECT :v + :v AS twice, :s || '-' || :s AS pair, :v * 3 AS thrice FROM dual";
    let params = batch(
        vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ],
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![2, 5])),
        ],
    );
    for by_name in [Some(true), None] {
        let batches = run(&mut conn, sql, params.clone(), by_name);
        assert_eq!(numbers(&batches, 0), [4.0, 10.0], "by_name {by_name:?}");
        assert_eq!(texts(&batches, 1), ["a-a", "b-b"], "by_name {by_name:?}");
        assert_eq!(numbers(&batches, 2), [6.0, 15.0], "by_name {by_name:?}");
    }
}

#[test]
fn by_position_takes_one_field_per_use() {
    let Some(mut conn) = connect() else { return };
    let params = batch(
        vec![
            Field::new("first", DataType::Int64, true),
            Field::new("second", DataType::Int64, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    );
    let batches = run(
        &mut conn,
        "SELECT :v - :v AS d FROM dual",
        params,
        Some(false),
    );
    assert_eq!(numbers(&batches, 0), [-9.0]);
}

#[test]
fn literals_and_comments_are_not_uses() {
    let Some(mut conn) = connect() else { return };
    let sql = "SELECT ':v' || q'[:v]' || :v /* :v */ AS s, -- :v\n :v AS t FROM dual";
    let params = batch(
        vec![Field::new("v", DataType::Utf8, true)],
        vec![Arc::new(StringArray::from(vec!["x"]))],
    );
    let batches = run(&mut conn, sql, params, Some(true));
    assert_eq!(texts(&batches, 0), [":v:vx"]);
    assert_eq!(texts(&batches, 1), ["x"]);
}

#[test]
fn dml_binds_a_repeated_name_on_every_path() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_ADBC_REPEAT";
    let _ = update(
        &mut conn,
        &format!(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        ),
        None,
    );
    update(
        &mut conn,
        &format!(
            "CREATE TABLE {table} (id NUMBER(9), a VARCHAR2(10), b VARCHAR2(10), wait INTERVAL DAY TO SECOND)"
        ),
        None,
    );

    // Array DML through the Arrow bind path.
    let rows = batch(
        vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
        ],
        vec![
            Arc::new(StringArray::from(vec!["x", "y"])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    );
    let affected = update(
        &mut conn,
        &format!("INSERT INTO {table} (id, a, b) VALUES (:id, :v, :v)"),
        Some(rows),
    );
    assert_eq!(affected, Some(2));

    // An interval is bound row by row, not through the Arrow path.
    let rows = batch(
        vec![
            Field::new("id", DataType::Int64, true),
            Field::new("w", DataType::Duration(TimeUnit::Second), true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(DurationSecondArray::from(vec![60, 120])),
        ],
    );
    let affected = update(
        &mut conn,
        &format!("UPDATE {table} SET wait = :w, b = b || :id WHERE id = :id"),
        Some(rows),
    );
    assert_eq!(affected, Some(2));

    let (_, batches) = query(
        &mut conn,
        &format!(
            "SELECT a, b, CAST(EXTRACT(MINUTE FROM wait) AS BINARY_DOUBLE) FROM {table} ORDER BY id"
        ),
    );
    assert_eq!(texts(&batches, 0), ["x", "y"]);
    assert_eq!(texts(&batches, 1), ["x1", "y2"]);
    assert_eq!(numbers(&batches, 2), [1.0, 2.0]);

    // DML RETURNING binds its inputs the same way.
    let params = batch(
        vec![
            Field::new("id", DataType::Int64, true),
            Field::new("out", DataType::Null, true).with_metadata(HashMap::from([(
                ORACLE_BIND_TYPE_KEY.to_string(),
                "VARCHAR2".to_string(),
            )])),
        ],
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(NullArray::new(1)),
        ],
    );
    let returned = run(
        &mut conn,
        &format!("UPDATE {table} SET a = a || :id WHERE id = :id RETURNING a INTO :out"),
        params,
        Some(true),
    );
    assert_eq!(texts(&returned, 0), ["y2"]);

    update(&mut conn, &format!("DROP TABLE {table} PURGE"), None);
}
