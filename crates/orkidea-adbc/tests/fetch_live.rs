//! Live integration tests for the streaming fetch engine and type mapping.
//!
//! Run against a real Oracle Database (23ai or later for BOOLEAN, JSON and
//! VECTOR columns):
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test fetch_live
//! ```
//!
//! Every test is skipped when `ORKIDEA_ADBC_URI` is not set.

use std::sync::Arc;

use adbc_core::options::OptionValue;
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, DurationMicrosecondType, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, IntervalYearMonthType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};

mod common;
use common::*;
use orkidea_adbc::OracleConnection;

#[test]
fn streams_large_results_in_bounded_batches() {
    let Some(mut conn) = connect() else { return };
    let (schema, batches) = query_with(
        &mut conn,
        "SELECT CAST(level AS NUMBER(10)) AS n FROM dual CONNECT BY level <= 25000",
        &[
            ("oracle.batch_size", OptionValue::Int(1000)),
            ("oracle.fetch_array_size", OptionValue::Int(500)),
            ("oracle.prefetch_rows", OptionValue::Int(500)),
        ],
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(batches.len(), 25);
    assert!(batches.iter().all(|b| b.num_rows() == 1000));
    let last = batches
        .last()
        .unwrap()
        .column(0)
        .as_primitive::<Int64Type>();
    assert_eq!(last.value(999), 25000);
}

#[test]
fn empty_result_keeps_schema() {
    let Some(mut conn) = connect() else { return };
    let (schema, batches) = query(
        &mut conn,
        "SELECT CAST(1 AS NUMBER(5)) AS a, 'x' AS b FROM dual WHERE 1 = 0",
    );
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert!(batches.is_empty());
}

#[test]
fn numeric_type_mapping() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_T_NUMERIC";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE {table} (
                n2 NUMBER(2), n4 NUMBER(4), n9 NUMBER(9), n18 NUMBER(18),
                n38 NUMBER(38), d NUMBER(10,2), tiny NUMBER(3,5), neg NUMBER(5,-2),
                unc NUMBER, f FLOAT, bf BINARY_FLOAT, bd BINARY_DOUBLE)"
        ),
    );
    exec(
        &mut conn,
        &format!(
            "INSERT INTO {table} VALUES (
                -99, 9999, 123456789, 123456789012345678,
                12345678901234567890123456789012345678, -12345678.91, 0.00123, 1234500,
                123.456, 2.5, 1.5, 2.25)"
        ),
    );
    let batch = single_row(&mut conn, &format!("SELECT * FROM {table}"));
    let schema = batch.schema();
    let expect = [
        ("N2", DataType::Int8),
        ("N4", DataType::Int16),
        ("N9", DataType::Int32),
        ("N18", DataType::Int64),
        ("N38", DataType::Decimal128(38, 0)),
        ("D", DataType::Decimal128(10, 2)),
        ("TINY", DataType::Decimal128(5, 5)),
        ("NEG", DataType::Decimal128(7, 0)),
        ("UNC", DataType::Float64),
        ("F", DataType::Float64),
        ("BF", DataType::Float32),
        ("BD", DataType::Float64),
    ];
    for (name, ty) in &expect {
        assert_eq!(field(&schema, name).data_type(), ty, "column {name}");
    }
    let col = |name: &str| batch.column(schema.index_of(name).unwrap()).clone();
    assert_eq!(col("N2").as_primitive::<Int8Type>().value(0), -99);
    assert_eq!(col("N4").as_primitive::<Int16Type>().value(0), 9999);
    assert_eq!(col("N9").as_primitive::<Int32Type>().value(0), 123_456_789);
    assert_eq!(
        col("N18").as_primitive::<Int64Type>().value(0),
        123_456_789_012_345_678
    );
    assert_eq!(
        col("N38")
            .as_primitive::<Decimal128Type>()
            .value_as_string(0),
        "12345678901234567890123456789012345678"
    );
    assert_eq!(
        col("D").as_primitive::<Decimal128Type>().value_as_string(0),
        "-12345678.91"
    );
    assert_eq!(
        col("TINY")
            .as_primitive::<Decimal128Type>()
            .value_as_string(0),
        "0.00123"
    );
    assert_eq!(
        col("NEG")
            .as_primitive::<Decimal128Type>()
            .value_as_string(0),
        "1234500"
    );
    assert_eq!(col("UNC").as_primitive::<Float64Type>().value(0), 123.456);
    assert_eq!(type_name(&schema, "D"), "NUMBER(10,2)");
    drop_table(&mut conn, table);
}

#[test]
fn unconstrained_number_modes() {
    let Some(mut conn) = connect() else { return };
    let sql = "SELECT CAST(12345678901234567890.123 AS NUMBER) AS v FROM dual";

    let (schema, batches) = query_with(
        &mut conn,
        sql,
        &[("oracle.unconstrained_number", "decimal".into())],
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(38, 10));
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<Decimal128Type>()
            .value_as_string(0),
        "12345678901234567890.1230000000"
    );

    let (schema, batches) = query_with(
        &mut conn,
        sql,
        &[("oracle.unconstrained_number", "string".into())],
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(0),
        "12345678901234567890.123"
    );

    // Too many fractional digits for the configured scale is an error, never
    // a silent rounding.
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(
        statement_option("oracle.unconstrained_number"),
        "decimal".into(),
    )
    .unwrap();
    stmt.set_option(
        statement_option("oracle.unconstrained_number_scale"),
        OptionValue::Int(2),
    )
    .unwrap();
    stmt.set_sql_query(sql).unwrap();
    let mut reader = stmt.execute().unwrap();
    let err = reader.next().unwrap().unwrap_err();
    assert!(err.to_string().contains("fractional digits"), "{err}");
}

#[test]
fn character_binary_and_lob_mapping() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_T_CHARS";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE {table} (
                c CHAR(3), nc NCHAR(3), v VARCHAR2(20), nv NVARCHAR2(20),
                cl CLOB, ncl NCLOB, r RAW(4), bl BLOB)"
        ),
    );
    exec(
        &mut conn,
        &format!(
            "INSERT INTO {table} VALUES ('ab', N'çé', 'héllo', N'日本', \
             TO_CLOB(RPAD('x', 4000, 'x')) || RPAD('y', 4000, 'y'), N'ünï', \
             HEXTORAW('DEADBEEF'), TO_BLOB(HEXTORAW('0102030405')))"
        ),
    );
    let batch = single_row(&mut conn, &format!("SELECT * FROM {table}"));
    let schema = batch.schema();
    for (name, ty) in [
        ("C", DataType::Utf8),
        ("NC", DataType::Utf8),
        ("V", DataType::Utf8),
        ("NV", DataType::Utf8),
        ("CL", DataType::LargeUtf8),
        ("NCL", DataType::LargeUtf8),
        ("R", DataType::Binary),
        ("BL", DataType::LargeBinary),
    ] {
        assert_eq!(field(&schema, name).data_type(), &ty, "column {name}");
    }
    let col = |name: &str| batch.column(schema.index_of(name).unwrap()).clone();
    assert_eq!(col("C").as_string::<i32>().value(0), "ab ");
    assert_eq!(col("NV").as_string::<i32>().value(0), "日本");
    assert_eq!(col("CL").as_string::<i64>().value(0).len(), 8000);
    assert_eq!(col("NCL").as_string::<i64>().value(0), "ünï");
    assert_eq!(
        col("R").as_binary::<i32>().value(0),
        &[0xDE, 0xAD, 0xBE, 0xEF]
    );
    assert_eq!(col("BL").as_binary::<i64>().value(0), &[1, 2, 3, 4, 5]);

    // get_table_schema reports the declared LOB types, not LONG.
    let declared = conn.get_table_schema(None, None, table).unwrap();
    assert_eq!(type_name(&declared, "CL"), "CLOB");
    assert_eq!(type_name(&declared, "BL"), "BLOB");
    assert_eq!(field(&declared, "CL").data_type(), &DataType::LargeUtf8);
    drop_table(&mut conn, table);
}

#[test]
fn datetime_and_interval_mapping() {
    let Some(mut conn) = connect() else { return };
    // Declared columns: Oracle reports maximum precision (9) for
    // expressions, so precision mapping is tested on real columns.
    let table = "ORKIDEA_T_DATETIME";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE {table} (
                d DATE, t0 TIMESTAMP(0), t3 TIMESTAMP(3), t6 TIMESTAMP(6), t9 TIMESTAMP(9),
                tz TIMESTAMP(6) WITH TIME ZONE, ltz TIMESTAMP WITH LOCAL TIME ZONE,
                ids INTERVAL DAY TO SECOND, iym INTERVAL YEAR TO MONTH)"
        ),
    );
    let ts = "TIMESTAMP '2025-01-02 03:04:05.123456789'";
    exec(
        &mut conn,
        &format!(
            "INSERT INTO {table} VALUES (
                DATE '2025-01-02' + 3/24, {ts}, {ts}, {ts}, {ts},
                TIMESTAMP '2025-01-02 03:04:05.5 +02:00',
                TIMESTAMP '2025-01-02 03:04:05.5 +02:00',
                INTERVAL '1 02:03:04.5' DAY TO SECOND, INTERVAL '-2-3' YEAR TO MONTH)"
        ),
    );
    let batch = single_row(&mut conn, &format!("SELECT * FROM {table}"));
    let schema = batch.schema();
    let col = |name: &str| batch.column(schema.index_of(name).unwrap()).clone();

    assert_eq!(
        field(&schema, "D").data_type(),
        &DataType::Timestamp(TimeUnit::Second, None)
    );
    assert_eq!(
        col("D").as_primitive::<TimestampSecondType>().value(0),
        1_735_786_800
    );
    for (name, unit) in [
        ("T0", TimeUnit::Second),
        ("T3", TimeUnit::Millisecond),
        ("T6", TimeUnit::Microsecond),
        ("T9", TimeUnit::Nanosecond),
    ] {
        assert_eq!(
            field(&schema, name).data_type(),
            &DataType::Timestamp(unit, None),
            "column {name}"
        );
    }
    assert_eq!(
        col("T0").as_primitive::<TimestampSecondType>().value(0),
        1_735_787_045
    );
    assert_eq!(
        col("T3")
            .as_primitive::<TimestampMillisecondType>()
            .value(0),
        1_735_787_045_123
    );
    assert_eq!(
        col("T6")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_735_787_045_123_457
    );
    assert_eq!(
        col("T9").as_primitive::<TimestampNanosecondType>().value(0),
        1_735_787_045_123_456_789
    );
    // WITH TIME ZONE: a UTC instant (03:04:05.5 at +02:00 is 01:04:05.5 UTC).
    assert_eq!(
        field(&schema, "TZ").data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        col("TZ")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_735_779_845_500_000
    );
    // WITH LOCAL TIME ZONE: the same instant, in UTC, whatever the database
    // and session time zones are.
    assert_eq!(
        field(&schema, "LTZ").data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        col("LTZ")
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        1_735_779_845_500_000
    );
    assert_eq!(
        field(&schema, "IDS").data_type(),
        &DataType::Duration(TimeUnit::Microsecond)
    );
    assert_eq!(
        col("IDS")
            .as_primitive::<DurationMicrosecondType>()
            .value(0),
        ((86_400 + 2 * 3600 + 3 * 60 + 4) * 1_000_000) + 500_000
    );
    assert_eq!(
        field(&schema, "IYM").data_type(),
        &DataType::Interval(IntervalUnit::YearMonth)
    );
    assert_eq!(
        col("IYM").as_primitive::<IntervalYearMonthType>().value(0),
        -27
    );
    drop_table(&mut conn, table);
}

/// Historical dates, including the Julian/Gregorian switch and BC years,
/// must match Oracle's own date arithmetic.
#[test]
fn historical_dates_match_oracle_arithmetic() {
    let Some(mut conn) = connect() else { return };
    let (_, batches) = query(
        &mut conn,
        "SELECT d, CAST(ROUND((d - DATE '1970-01-01') * 86400) AS NUMBER(18)) AS secs
         FROM (
            SELECT TO_DATE('-4712-01-01', 'SYYYY-MM-DD') AS d FROM dual UNION ALL
            SELECT TO_DATE('-0001-12-31', 'SYYYY-MM-DD') FROM dual UNION ALL
            SELECT TO_DATE('0001-01-01', 'YYYY-MM-DD') FROM dual UNION ALL
            SELECT TO_DATE('1582-10-04', 'YYYY-MM-DD') FROM dual UNION ALL
            SELECT TO_DATE('1582-10-15', 'YYYY-MM-DD') FROM dual UNION ALL
            SELECT TO_DATE('1900-02-28 23:59:59', 'YYYY-MM-DD HH24:MI:SS') FROM dual UNION ALL
            SELECT TO_DATE('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') FROM dual
         )",
    );
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 7);
    let ours = batch.column(0).as_primitive::<TimestampSecondType>();
    let oracle = batch.column(1).as_primitive::<Int64Type>();
    for i in 0..batch.num_rows() {
        assert_eq!(ours.value(i), oracle.value(i), "row {i}");
    }
}

#[test]
fn oracle_23ai_types() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_T_23AI";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE {table} (
                b BOOLEAN, j JSON, v3 VECTOR(3, FLOAT32), vflex VECTOR(*, *), v8 VECTOR(4, INT8))"
        ),
    );
    exec(
        &mut conn,
        &format!(
            "INSERT INTO {table} VALUES (TRUE, JSON('{{\"b\":[1,true,null],\"a\":\"x\"}}'), \
             VECTOR('[1.5, 2, -3]', 3, FLOAT32), VECTOR('[1, 2]', *, FLOAT64), \
             VECTOR('[1, -2, 3, 4]', 4, INT8))"
        ),
    );
    exec(
        &mut conn,
        &format!("INSERT INTO {table} VALUES (NULL, NULL, NULL, NULL, NULL)"),
    );
    let (schema, batches) = query(&mut conn, &format!("SELECT * FROM {table} ORDER BY b"));
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(field(&schema, "B").data_type(), &DataType::Boolean);
    assert!(batch.column(0).as_boolean().value(0));
    assert!(batch.column(0).is_null(1));

    let json = field(&schema, "J");
    assert_eq!(json.data_type(), &DataType::Utf8);
    assert_eq!(
        json.metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.json")
    );
    assert_eq!(
        batch.column(1).as_string::<i32>().value(0),
        r#"{"a":"x","b":[1,true,null]}"#
    );

    let v3 = field(&schema, "V3").data_type();
    assert!(
        matches!(v3, DataType::FixedSizeList(item, 3) if item.data_type() == &DataType::Float32),
        "{v3:?}"
    );
    let list = batch.column(2).as_fixed_size_list();
    let values = list.value(0);
    let values = values.as_primitive::<arrow_array::types::Float32Type>();
    assert_eq!(values.values().as_ref(), &[1.5, 2.0, -3.0]);
    assert!(list.is_null(1));

    let flex = field(&schema, "VFLEX").data_type();
    assert!(matches!(flex, DataType::List(_)), "{flex:?}");
    let flex = batch.column(3).as_list::<i32>();
    assert_eq!(flex.value(0).len(), 2);

    let v8 = field(&schema, "V8").data_type();
    assert!(
        matches!(v8, DataType::FixedSizeList(item, 4) if item.data_type() == &DataType::Int8),
        "{v8:?}"
    );
    drop_table(&mut conn, table);
}

#[test]
fn rowid_is_text() {
    let Some(mut conn) = connect() else { return };
    let batch = single_row(&mut conn, "SELECT ROWID AS rid FROM dual");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(batch.column(0).as_string::<i32>().value(0).len(), 18);
}

#[test]
fn unsupported_types_fail_with_a_hint() {
    let Some(mut conn) = connect() else { return };
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT XMLTYPE('<a/>') AS x FROM dual")
        .unwrap();
    let err = stmt.execute().err().expect("XMLTYPE should be rejected");
    assert!(err.message.contains("XMLSERIALIZE"), "{}", err.message);
}

#[test]
fn bound_parameters_execute_once_per_row() {
    let Some(mut conn) = connect() else { return };
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(
        "SELECT CAST(:1 * 10 AS NUMBER(10)) AS n, CAST(:2 AS VARCHAR2(10)) AS s FROM dual",
    )
    .unwrap();
    stmt.bind(params).unwrap();
    let batches = stmt
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let batch = arrow_select_concat(&batches);
    assert_eq!(batch.num_rows(), 3);
    let n = batch.column(0).as_primitive::<Int64Type>();
    assert_eq!((n.value(0), n.value(1)), (10, 20));
    assert!(n.is_null(2));
    let s = batch.column(1).as_string::<i32>();
    assert_eq!(s.value(0), "a");
    assert!(s.is_null(1));
}

/// A prepared statement runs with its parameters even when the same SQL ran
/// to the end before on the connection, as with a second DB-API cursor. The
/// prepare used to parse the cached statement and leave it without binds
/// (ORA-01008).
#[test]
fn prepare_after_the_same_query_ran_keeps_its_binds() {
    let Some(mut conn) = connect() else { return };
    let run = |conn: &mut OracleConnection, prepare: bool| -> i64 {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("SELECT CAST(:n * 2 AS NUMBER(9)) AS doubled FROM dual")
            .unwrap();
        if prepare {
            stmt.prepare().unwrap();
        }
        let params = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![21]))],
        )
        .unwrap();
        stmt.bind(params).unwrap();
        let batches = stmt
            .execute()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        arrow_select_concat(&batches)
            .column(0)
            .as_primitive::<Int32Type>()
            .value(0)
            .into()
    };
    for prepare in [false, true, true, false, true] {
        assert_eq!(run(&mut conn, prepare), 42, "prepare = {prepare}");
    }
}

fn arrow_select_concat(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches[0].schema();
    arrow_select::concat::concat_batches(&schema, batches).unwrap()
}

#[test]
fn execute_on_non_query_returns_empty_result() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_T_EXEC";
    drop_table(&mut conn, table);
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(format!("CREATE TABLE {table} (id NUMBER(5))"))
        .unwrap();
    let reader = stmt.execute().unwrap();
    assert!(reader.schema().fields().is_empty());
    assert_eq!(reader.count(), 0);
    let batch = single_row(&mut conn, &format!("SELECT COUNT(*) AS c FROM {table}"));
    assert_eq!(batch.num_rows(), 1);
    drop_table(&mut conn, table);
}

#[test]
fn execute_schema_matches_execute() {
    let Some(mut conn) = connect() else { return };
    let sql = "SELECT CAST(1 AS NUMBER(4)) AS a, SYSTIMESTAMP AS b, 'x' AS c, \
               INTERVAL '1' DAY AS d FROM dual";
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(sql).unwrap();
    let described = stmt.execute_schema().unwrap();
    let (executed, _) = query(&mut conn, sql);
    assert_eq!(&described, executed.as_ref());
}

#[test]
fn get_info_reports_server_version() {
    use adbc_core::options::InfoCode;
    use std::collections::HashSet;
    let Some(conn) = connect() else { return };
    let mut reader = conn
        .get_info(Some(HashSet::from([InfoCode::VendorVersion])))
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    let union = batch.column(1).as_union();
    let version = union.value(0);
    let version = version.as_string::<i32>().value(0).to_string();
    assert!(
        version.chars().next().is_some_and(|c| c.is_ascii_digit()) && version.contains('.'),
        "unexpected server version {version:?}"
    );
}

/// Oracle executes DDL at parse time; prepare must not run it.
#[test]
fn prepare_does_not_execute_ddl() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_T_PREPARE";
    drop_table(&mut conn, table);
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(format!("CREATE TABLE {table} (id NUMBER)"))
        .unwrap();
    stmt.prepare().unwrap();
    let (_, batches) = query(
        &mut conn,
        &format!("SELECT table_name FROM user_tables WHERE table_name = '{table}'"),
    );
    assert!(
        batches.iter().all(|b| b.num_rows() == 0),
        "prepare created the table"
    );
    stmt.execute_update().unwrap();
    drop(stmt);
    drop_table(&mut conn, table);

    // Queries are still validated on the server by prepare.
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT no_such_column FROM dual")
        .unwrap();
    assert_eq!(stmt.prepare().unwrap_err().vendor_code, 904);
}
