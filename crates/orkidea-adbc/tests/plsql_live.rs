//! Live integration tests for PL/SQL, REF CURSOR results, DML RETURNING and
//! vector parameters.
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test plsql_live
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::options::OptionConnection;
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, NullArray, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use orkidea_adbc::{OracleConnection, ORACLE_BIND_TYPE_KEY};

mod common;
use common::*;

fn bind_type(field: Field, oracle_type: &str) -> Field {
    field.with_metadata(HashMap::from([(
        ORACLE_BIND_TYPE_KEY.to_string(),
        oracle_type.to_string(),
    )]))
}

fn run(conn: &mut OracleConnection, sql: &str, params: Option<RecordBatch>) -> Vec<RecordBatch> {
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(sql).unwrap();
    if let Some(params) = params {
        stmt.bind(params).unwrap();
    }
    stmt.execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn plsql_out_binds_become_a_result_row() {
    let Some(mut conn) = connect() else { return };
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            bind_type(Field::new("doubled", DataType::Null, true), "NUMBER"),
            Field::new("label", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![21, 5])) as ArrayRef,
            Arc::new(NullArray::new(2)) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>, None])) as ArrayRef,
        ],
    )
    .unwrap();
    let batches = run(
        &mut conn,
        "BEGIN :doubled := :n * 2; :label := 'n=' || :n; END;",
        Some(params),
    );
    let batch = &batches[0];
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["DOUBLED", "LABEL"], "IN-only binds are not outputs");
    assert_eq!(batch.num_rows(), 2, "one output row per parameter row");
    let doubled = batch.column(0).as_primitive::<Float64Type>();
    assert_eq!((doubled.value(0), doubled.value(1)), (42.0, 10.0));
    let label = batch.column(1).as_string::<i32>();
    assert_eq!((label.value(0), label.value(1)), ("n=21", "n=5"));
}

#[test]
fn plsql_without_outputs_returns_an_empty_result() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_P_SIDE_EFFECT";
    drop_table(&mut conn, table);
    exec(&mut conn, &format!("CREATE TABLE {table} (id NUMBER(5))"));
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(format!(
        "BEGIN INSERT INTO {table} VALUES (1); INSERT INTO {table} VALUES (2); END;"
    ))
    .unwrap();
    let reader = stmt.execute().unwrap();
    assert!(reader.schema().fields().is_empty());
    assert_eq!(reader.count(), 0);
    drop(stmt);
    let row = single_row(
        &mut conn,
        &format!("SELECT CAST(COUNT(*) AS NUMBER(5)) AS c FROM {table}"),
    );
    assert_eq!(row.column(0).as_primitive::<Int32Type>().value(0), 2);
    drop_table(&mut conn, table);
}

#[test]
fn ref_cursor_out_bind_streams_its_rows() {
    let Some(mut conn) = connect() else { return };
    exec(
        &mut conn,
        "CREATE OR REPLACE PROCEDURE orkidea_p_numbers(p_count IN NUMBER, p_rows OUT SYS_REFCURSOR) AS
         BEGIN
             OPEN p_rows FOR
                 SELECT CAST(level AS NUMBER(10)) AS n, 'row ' || level AS label
                   FROM dual CONNECT BY level <= p_count;
         END;",
    );
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            bind_type(Field::new("rows", DataType::Null, true), "CURSOR"),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3, 2])) as ArrayRef,
            Arc::new(NullArray::new(2)) as ArrayRef,
        ],
    )
    .unwrap();
    let batches = run(
        &mut conn,
        "BEGIN orkidea_p_numbers(:1, :2); END;",
        Some(params),
    );
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "N");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        total_rows(&batches),
        5,
        "cursors of both executions are concatenated"
    );
    let labels: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let c = b.column(1).as_string::<i32>();
            (0..c.len())
                .map(|i| c.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(labels, ["row 1", "row 2", "row 3", "row 1", "row 2"]);
    exec(&mut conn, "DROP PROCEDURE orkidea_p_numbers");
}

#[test]
fn dml_returning_returns_rows() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_P_RETURNING";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE {table} (id NUMBER(10) GENERATED ALWAYS AS IDENTITY, name VARCHAR2(20))"
        ),
    );
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            bind_type(Field::new("new_id", DataType::Null, true), "NUMBER"),
            Field::new("upper_name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["ada", "grace"])) as ArrayRef,
            Arc::new(NullArray::new(2)) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>, None])) as ArrayRef,
        ],
    )
    .unwrap();
    let batches = run(
        &mut conn,
        &format!("INSERT INTO {table} (name) VALUES (:1) RETURNING id, UPPER(name) INTO :2, :3"),
        Some(params),
    );
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    let ids = batch.column(0).as_primitive::<Float64Type>();
    assert!(ids.value(1) > ids.value(0));
    let names = batch.column(1).as_string::<i32>();
    assert_eq!((names.value(0), names.value(1)), ("ADA", "GRACE"));
    // Autocommit committed the inserts.
    let row = single_row(
        &mut conn,
        &format!("SELECT CAST(COUNT(*) AS NUMBER(5)) AS c FROM {table}"),
    );
    assert_eq!(row.column(0).as_primitive::<Int32Type>().value(0), 2);
    drop_table(&mut conn, table);
}

#[test]
fn vector_parameters_bind_from_arrow_lists() {
    let Some(mut conn) = connect() else { return };
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let vectors = FixedSizeListArray::try_new(
        item.clone(),
        3,
        Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 1.0, 2.0, 4.0])),
        None,
    )
    .unwrap();
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::FixedSizeList(item, 3),
            false,
        )])),
        vec![Arc::new(vectors) as ArrayRef],
    )
    .unwrap();
    let batches = run(
        &mut conn,
        "SELECT CAST(VECTOR_DISTANCE(TO_VECTOR('[1, 2, 3]', 3, FLOAT32), :1, EUCLIDEAN) \
         AS BINARY_DOUBLE) AS d FROM dual",
        Some(params),
    );
    let d: Vec<f64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Float64Type>().values().to_vec())
        .collect();
    assert_eq!(d, [0.0, 1.0]);
}

#[test]
fn read_only_connections_refuse_plsql() {
    let Some(mut conn) = connect() else { return };
    conn.set_option(OptionConnection::ReadOnly, "true".into())
        .unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("BEGIN NULL; END;").unwrap();
    assert!(stmt.execute().is_err());
    drop(stmt);
    conn.set_option(OptionConnection::ReadOnly, "false".into())
        .unwrap();
}

fn minus_params() -> RecordBatch {
    // Fields in the opposite order of the placeholders.
    RecordBatch::try_from_iter([
        ("b", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
        ("a", Arc::new(Int64Array::from(vec![10])) as ArrayRef),
    ])
    .unwrap()
}

fn minus(
    conn: &mut OracleConnection,
    bind_by_name: Option<&str>,
    params: RecordBatch,
) -> adbc_core::error::Result<f64> {
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT :a - :b AS d FROM dual").unwrap();
    if let Some(value) = bind_by_name {
        stmt.set_option(
            statement_option("adbc.statement.bind_by_name"),
            value.into(),
        )
        .unwrap();
    }
    stmt.bind(params).unwrap();
    let batches = stmt.execute()?.collect::<Result<Vec<_>, _>>().unwrap();
    Ok(batches[0].column(0).as_primitive::<Float64Type>().value(0))
}

/// `adbc.statement.bind_by_name`, which the Python DB-API sets for dict
/// parameters, makes the matching explicit.
#[test]
fn bind_by_name_option_controls_matching() {
    let Some(mut conn) = connect() else { return };
    assert_eq!(minus(&mut conn, None, minus_params()).unwrap(), 8.0);
    assert_eq!(minus(&mut conn, Some("true"), minus_params()).unwrap(), 8.0);
    assert_eq!(
        minus(&mut conn, Some("false"), minus_params()).unwrap(),
        -8.0
    );

    let unmatched = RecordBatch::try_from_iter([
        ("a", Arc::new(Int64Array::from(vec![10])) as ArrayRef),
        ("c", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
    ])
    .unwrap();
    let err = minus(&mut conn, Some("true"), unmatched).unwrap_err();
    assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);
    assert!(
        err.message.contains("no bound parameter is named \"B\""),
        "{}",
        err.message
    );
}

/// Parameters bound for one query are not sent with the next one.
#[test]
fn a_new_query_drops_parameters_bound_for_the_previous_one() {
    let Some(mut conn) = connect() else { return };
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT :a AS v FROM dual").unwrap();
    stmt.bind(
        RecordBatch::try_from_iter([("a", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap(),
    )
    .unwrap();
    let batches = stmt
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(total_rows(&batches), 2);

    stmt.set_sql_query("SELECT 1 AS v FROM dual").unwrap();
    let batches = stmt
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(total_rows(&batches), 1);
}
