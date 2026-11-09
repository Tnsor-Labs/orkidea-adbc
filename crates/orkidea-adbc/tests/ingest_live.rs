//! Live integration tests for bulk ingestion.
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test ingest_live
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use adbc_core::error::Status;
use adbc_core::options::{IngestMode, OptionConnection, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{
    ArrayRef, BooleanArray, Decimal256Array, Int32Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use orkidea_adbc::OracleConnection;

mod common;
use common::*;

fn count(conn: &mut OracleConnection, sql: &str) -> i64 {
    let batch = single_row(
        conn,
        &format!("SELECT CAST(COUNT(*) AS NUMBER(18)) AS c FROM ({sql})"),
    );
    batch.column(0).as_primitive::<Int64Type>().value(0)
}

fn table_exists(conn: &mut OracleConnection, table: &str) -> bool {
    count(
        conn,
        &format!("SELECT 1 FROM user_tables WHERE table_name = '{table}'"),
    ) > 0
}

fn ids_batch(ids: &[i32], names: &[Option<&str>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_statement(
    conn: &mut OracleConnection,
    mode: IngestMode,
    table: &str,
    extra: &[(OptionStatement, OptionValue)],
) -> orkidea_adbc::OracleStatement {
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(OptionStatement::IngestMode, mode.into())
        .unwrap();
    stmt.set_option(OptionStatement::TargetTable, table.into())
        .unwrap();
    for (key, value) in extra {
        stmt.set_option(key.clone(), value.clone()).unwrap();
    }
    stmt
}

fn ingest(
    conn: &mut OracleConnection,
    mode: IngestMode,
    table: &str,
    batch: RecordBatch,
) -> adbc_core::error::Result<Option<i64>> {
    let mut stmt = ingest_statement(conn, mode, table, &[]);
    stmt.bind(batch).unwrap();
    stmt.execute_update()
}

/// A string longer than VARCHAR2(4000): inserting it fails with ORA-12899.
fn too_long() -> String {
    "x".repeat(4001)
}

#[test]
fn create_append_and_create_append() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_MODES";
    drop_table(&mut conn, table);
    let batch = ids_batch(&[1, 2], &[Some("a"), None]);
    assert_eq!(
        ingest(&mut conn, IngestMode::Create, table, batch.clone()).unwrap(),
        Some(2)
    );
    let err = ingest(&mut conn, IngestMode::Create, table, batch.clone()).unwrap_err();
    assert_eq!(err.status, Status::AlreadyExists);
    assert_eq!(
        ingest(&mut conn, IngestMode::Append, table, batch.clone()).unwrap(),
        Some(2)
    );
    assert_eq!(
        ingest(&mut conn, IngestMode::CreateAppend, table, batch).unwrap(),
        Some(2)
    );
    assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 6);
    let err = ingest(
        &mut conn,
        IngestMode::Append,
        "ORKIDEA_I_MISSING",
        ids_batch(&[1], &[None]),
    )
    .unwrap_err();
    assert_eq!(err.status, Status::NotFound);
    drop_table(&mut conn, table);
}

#[test]
fn append_matches_columns_by_name() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_BYNAME";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE {table} (id NUMBER(10), name VARCHAR2(20), created DATE DEFAULT SYSDATE)"
        ),
    );
    // Reordered subset of the table's columns.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("NAME", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["z"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(
        ingest(&mut conn, IngestMode::Append, table, batch).unwrap(),
        Some(1)
    );
    let row = single_row(
        &mut conn,
        &format!("SELECT id, name, CASE WHEN created IS NOT NULL THEN 'y' END AS d FROM {table}"),
    );
    assert_eq!(row.column(0).as_primitive::<Int64Type>().value(0), 7);
    assert_eq!(row.column(1).as_string::<i32>().value(0), "z");
    assert_eq!(row.column(2).as_string::<i32>().value(0), "y");

    // A column the table lacks, and a type that cannot be stored.
    let unknown = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("nope", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let err = ingest(&mut conn, IngestMode::Append, table, unknown).unwrap_err();
    assert_eq!(err.status, Status::AlreadyExists);
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["1"])) as ArrayRef],
    )
    .unwrap();
    let err = ingest(&mut conn, IngestMode::Append, table, wrong).unwrap_err();
    assert_eq!(err.status, Status::AlreadyExists);
    assert!(err.message.contains("NUMBER(10)"), "{}", err.message);
    drop_table(&mut conn, table);
}

#[test]
fn failed_append_rolls_back_every_row() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_ROLLBACK";
    drop_table(&mut conn, table);
    ingest(
        &mut conn,
        IngestMode::Create,
        table,
        ids_batch(&[1], &[Some("seed")]),
    )
    .unwrap();
    // Chunks of 2 rows: the first chunk succeeds, the second fails.
    let long = too_long();
    let batch = ids_batch(
        &[2, 3, 4, 5],
        &[Some("ok"), Some("ok"), Some(&long), Some("ok")],
    );
    let mut stmt = ingest_statement(
        &mut conn,
        IngestMode::Append,
        table,
        &[(statement_option("oracle.batch_size"), OptionValue::Int(2))],
    );
    stmt.bind(batch).unwrap();
    let err = stmt.execute_update().unwrap_err();
    assert!(err.message.contains("rows 2..4"), "{}", err.message);
    assert_eq!(err.vendor_code, 12899);
    // Another statement commits under autocommit; no partial rows may appear.
    exec(&mut conn, "SELECT 1 FROM dual");
    assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 1);
    drop_table(&mut conn, table);
}

#[test]
fn failed_create_leaves_no_table() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_FAILCREATE";
    drop_table(&mut conn, table);
    let long = too_long();
    let err = ingest(
        &mut conn,
        IngestMode::Create,
        table,
        ids_batch(&[1, 2], &[Some("ok"), Some(&long)]),
    )
    .unwrap_err();
    assert_eq!(err.vendor_code, 12899);
    assert!(!table_exists(&mut conn, table));
}

#[test]
fn replace_swaps_only_after_a_complete_load() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_REPLACE";
    drop_table(&mut conn, table);
    ingest(
        &mut conn,
        IngestMode::Create,
        table,
        ids_batch(&[1, 2, 3], &[None, None, None]),
    )
    .unwrap();

    // A failing replace keeps the original table and data, and leaves no
    // staging table behind.
    let long = too_long();
    let err = ingest(
        &mut conn,
        IngestMode::Replace,
        table,
        ids_batch(&[9, 9], &[Some("ok"), Some(&long)]),
    )
    .unwrap_err();
    assert_eq!(err.vendor_code, 12899);
    assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 3);
    assert_eq!(
        count(
            &mut conn,
            "SELECT 1 FROM user_tables WHERE table_name LIKE 'ORKIDEA$STAGE%'"
        ),
        0
    );

    // A successful replace installs the new schema and rows.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("code", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["new"])) as ArrayRef],
    )
    .unwrap();
    assert_eq!(
        ingest(&mut conn, IngestMode::Replace, table, batch).unwrap(),
        Some(1)
    );
    let row = single_row(&mut conn, &format!("SELECT * FROM {table}"));
    assert_eq!(row.schema().field(0).name(), "CODE");
    assert_eq!(row.column(0).as_string::<i32>().value(0), "new");
    drop_table(&mut conn, table);
}

#[test]
fn global_temporary_rows_survive_commit_and_stay_private() {
    let Some(database) = database() else { return };
    let mut conn = database.new_connection().unwrap();
    let table = "ORKIDEA_I_GTT";
    exec(
        &mut conn,
        &format!(
            "BEGIN EXECUTE IMMEDIATE 'TRUNCATE TABLE {table}'; EXECUTE IMMEDIATE 'DROP TABLE {table}'; \
             EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        ),
    );
    let mut stmt = ingest_statement(
        &mut conn,
        IngestMode::Create,
        table,
        &[(OptionStatement::Temporary, "true".into())],
    );
    stmt.bind(ids_batch(&[1, 2], &[None, None])).unwrap();
    assert_eq!(stmt.execute_update().unwrap(), Some(2));
    drop(stmt);
    // Autocommit has committed; ON COMMIT PRESERVE ROWS keeps the data.
    assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 2);
    assert_eq!(
        count(
            &mut conn,
            &format!("SELECT 1 FROM user_tables WHERE table_name = '{table}' AND temporary = 'Y' AND duration = 'SYS$SESSION'")
        ),
        1
    );
    let mut other = database.new_connection().unwrap();
    assert_eq!(count(&mut other, &format!("SELECT * FROM {table}")), 0);

    // Replace on a temporary table works even while it holds session rows.
    let mut stmt = ingest_statement(
        &mut conn,
        IngestMode::Replace,
        table,
        &[(OptionStatement::Temporary, "true".into())],
    );
    stmt.bind(ids_batch(&[3], &[None])).unwrap();
    assert_eq!(stmt.execute_update().unwrap(), Some(1));
    drop(stmt);
    assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 1);
    exec(&mut conn, &format!("TRUNCATE TABLE {table}"));
    exec(&mut conn, &format!("DROP TABLE {table}"));
}

#[test]
fn private_temporary_tables_are_session_scoped() {
    let Some(database) = database() else { return };
    let table = "ORA$PTT_ORKIDEA";
    {
        let mut conn = database.new_connection().unwrap();
        let mut stmt = ingest_statement(
            &mut conn,
            IngestMode::Create,
            table,
            &[(OptionStatement::Temporary, "true".into())],
        );
        stmt.bind(ids_batch(&[1, 2, 3], &[None, None, None]))
            .unwrap();
        assert_eq!(stmt.execute_update().unwrap(), Some(3));
        drop(stmt);
        assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 3);
    }
    // A new session does not see it.
    let mut conn = database.new_connection().unwrap();

    // LOB columns are rejected up front: Oracle does not support them in
    // private temporary tables.
    let lob = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "doc",
            DataType::LargeUtf8,
            true,
        )])),
        vec![Arc::new(arrow_array::LargeStringArray::from(vec!["x"])) as ArrayRef],
    )
    .unwrap();
    let mut stmt = ingest_statement(
        &mut conn,
        IngestMode::Create,
        "ORA$PTT_ORKIDEA_LOB",
        &[(OptionStatement::Temporary, "true".into())],
    );
    stmt.bind(lob).unwrap();
    let err = stmt.execute_update().unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    drop(stmt);

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(format!("SELECT * FROM {table}"))
        .unwrap();
    let err = stmt
        .execute()
        .err()
        .expect("table must not exist in a new session");
    assert_eq!(err.vendor_code, 942);
}

#[test]
fn bind_stream_is_consumed_lazily() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_STREAM";
    drop_table(&mut conn, table);
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let pulled = Arc::new(AtomicUsize::new(0));
    let counter = pulled.clone();
    let stream_schema = schema.clone();
    let batches = (0..5).map(move |i| -> Result<RecordBatch, ArrowError> {
        counter.fetch_add(1, Ordering::SeqCst);
        let values: Vec<i64> = (i * 1000..(i + 1) * 1000).collect();
        RecordBatch::try_new(
            stream_schema.clone(),
            vec![Arc::new(Int64Array::from(values))],
        )
    });
    let reader = RecordBatchIterator::new(batches, schema);
    let mut stmt = ingest_statement(&mut conn, IngestMode::Create, table, &[]);
    stmt.bind_stream(Box::new(reader)).unwrap();
    assert_eq!(
        pulled.load(Ordering::SeqCst),
        0,
        "bind_stream must not read the stream"
    );
    assert_eq!(stmt.execute_update().unwrap(), Some(5000));
    assert_eq!(pulled.load(Ordering::SeqCst), 5);
    drop(stmt);
    let row = single_row(
        &mut conn,
        &format!("SELECT CAST(SUM(n) AS NUMBER(18)) AS s FROM {table}"),
    );
    assert_eq!(
        row.column(0).as_primitive::<Int64Type>().value(0),
        4999 * 5000 / 2
    );
    drop_table(&mut conn, table);
}

#[test]
fn zone_aware_timestamps_store_the_right_instant() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_TZ";
    drop_table(&mut conn, table);
    // A non-UTC session time zone must not shift the stored instant.
    exec(&mut conn, "ALTER SESSION SET TIME_ZONE = '+05:00'");
    let instant = 1_735_787_045_123_456_i64; // 2025-01-02 03:04:05.123456 UTC
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![instant]).with_timezone("UTC"))
                as ArrayRef,
        ],
    )
    .unwrap();
    ingest(&mut conn, IngestMode::Create, table, batch).unwrap();
    let row = single_row(
        &mut conn,
        &format!(
            "SELECT TO_CHAR(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF6') AS utc, ts FROM {table}"
        ),
    );
    assert_eq!(
        row.column(0).as_string::<i32>().value(0),
        "2025-01-02 03:04:05.123456"
    );
    assert_eq!(
        row.column(1)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        instant
    );
    drop_table(&mut conn, table);
}

#[test]
fn booleans_use_native_boolean_on_23ai() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_BOOL";
    drop_table(&mut conn, table);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )])),
        vec![Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef],
    )
    .unwrap();
    ingest(&mut conn, IngestMode::Create, table, batch).unwrap();
    let declared = conn.get_table_schema(None, None, table).unwrap();
    assert_eq!(declared.field(0).data_type(), &DataType::Boolean);
    assert_eq!(
        count(&mut conn, &format!("SELECT 1 FROM {table} WHERE flag")),
        1
    );
    drop_table(&mut conn, table);
}

#[test]
fn unbindable_types_fail_before_any_ddl() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_UNBINDABLE";
    drop_table(&mut conn, table);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal256(50, 0),
            true,
        )])),
        vec![Arc::new(
            Decimal256Array::from(vec![arrow_buffer::i256::from_i128(1)])
                .with_precision_and_scale(50, 0)
                .unwrap(),
        ) as ArrayRef],
    )
    .unwrap();
    let err = ingest(&mut conn, IngestMode::Create, table, batch).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert!(!table_exists(&mut conn, table));
}

#[test]
fn failed_array_dml_has_no_partial_effects() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_DML";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!("CREATE TABLE {table} (id NUMBER(10), name VARCHAR2(10))"),
    );
    let long = "y".repeat(11);
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(format!("INSERT INTO {table} VALUES (:1, :2)"))
        .unwrap();
    stmt.bind(ids_batch(&[1, 2, 3], &[Some("a"), Some("b"), Some(&long)]))
        .unwrap();
    let err = stmt.execute_update().unwrap_err();
    assert_eq!(err.vendor_code, 12899);
    exec(&mut conn, "SELECT 1 FROM dual");
    assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 0);

    // A manual transaction keeps its earlier work when a later DML fails.
    conn.set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();
    exec(
        &mut conn,
        &format!("INSERT INTO {table} VALUES (100, 'kept')"),
    );
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(format!("INSERT INTO {table} VALUES (:1, :2)"))
        .unwrap();
    stmt.bind(ids_batch(&[1, 2], &[Some("a"), Some(&long)]))
        .unwrap();
    assert!(stmt.execute_update().is_err());
    drop(stmt);
    conn.commit().unwrap();
    assert_eq!(count(&mut conn, &format!("SELECT * FROM {table}")), 1);
    conn.set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();
    drop_table(&mut conn, table);
}

#[test]
fn identifiers_follow_oracle_name_resolution() {
    let Some(mut conn) = connect() else { return };
    // Simple names are upper-cased; other names are kept exactly.
    let table = "Orkidea Mixed Case";
    exec(
        &mut conn,
        &format!(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"{table}\" PURGE'; \
             EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        ),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("plain", DataType::Int32, true),
            Field::new("Total Amount", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
        ],
    )
    .unwrap();
    ingest(&mut conn, IngestMode::Create, table, batch.clone()).unwrap();
    ingest(&mut conn, IngestMode::Append, table, batch).unwrap();
    let schema = conn.get_table_schema(None, None, table).unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["PLAIN", "Total Amount"]);
    assert_eq!(count(&mut conn, &format!("SELECT * FROM \"{table}\"")), 2);
    exec(&mut conn, &format!("DROP TABLE \"{table}\" PURGE"));
}

/// DB-API cursors reuse one statement for ingest and queries.
#[test]
fn statement_switches_from_ingest_to_query() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_REUSE";
    drop_table(&mut conn, table);
    let mut stmt = ingest_statement(&mut conn, IngestMode::Create, table, &[]);
    stmt.bind(ids_batch(&[1, 2], &[None, None])).unwrap();
    assert_eq!(stmt.execute_update().unwrap(), Some(2));
    stmt.set_sql_query(format!("SELECT COUNT(*) AS c FROM {table}"))
        .unwrap();
    let batches = stmt
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    drop(stmt);
    drop_table(&mut conn, table);
}

/// Vectors, durations and intervals use the row-bind path (one round trip
/// per chunk) and get matching Oracle column types.
#[test]
fn ingests_vectors_and_intervals() {
    use arrow_array::types::{DurationMicrosecondType, Float32Type, IntervalYearMonthType};
    use arrow_array::Array;
    use arrow_array::{
        DurationMicrosecondArray, FixedSizeListArray, Float32Array, IntervalYearMonthArray,
    };
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_VECTORS";
    drop_table(&mut conn, table);
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let embeddings = FixedSizeListArray::try_new(
        item.clone(),
        3,
        Arc::new(Float32Array::from(vec![
            1.0, 2.0, 3.0, 0.0, 0.0, 0.0, 4.0, 5.0, 6.0,
        ])),
        Some(vec![true, false, true].into()),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("embedding", DataType::FixedSizeList(item, 3), true),
            Field::new("elapsed", DataType::Duration(TimeUnit::Microsecond), true),
            Field::new(
                "term",
                DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
                true,
            ),
        ])),
        vec![
            Arc::new(embeddings) as ArrayRef,
            Arc::new(DurationMicrosecondArray::from(vec![
                Some(90_061_500_000),
                None,
                Some(-1),
            ])) as ArrayRef,
            Arc::new(IntervalYearMonthArray::from(vec![Some(27), Some(-3), None])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(
        ingest(&mut conn, IngestMode::Create, table, batch).unwrap(),
        Some(3)
    );

    let declared = conn.get_table_schema(None, None, table).unwrap();
    let type_name = |name: &str| {
        declared
            .field_with_name(name)
            .unwrap()
            .metadata()
            .get(orkidea_adbc::ORACLE_TYPE_NAME_KEY)
            .cloned()
            .unwrap()
    };
    assert_eq!(type_name("EMBEDDING"), "VECTOR(3,float32)");
    assert!(type_name("ELAPSED").starts_with("INTERVAL DAY(9) TO SECOND(6)"));
    assert!(type_name("TERM").starts_with("INTERVAL YEAR(9) TO MONTH"));

    let (_, batches) = query(&mut conn, &format!("SELECT * FROM {table}"));
    let b = &batches[0];
    let vectors = b.column(0).as_fixed_size_list();
    assert!(vectors.is_null(1));
    let first = vectors.value(0);
    assert_eq!(
        first.as_primitive::<Float32Type>().values().as_ref(),
        &[1.0, 2.0, 3.0]
    );
    let elapsed = b.column(1).as_primitive::<DurationMicrosecondType>();
    assert_eq!(elapsed.value(0), 90_061_500_000);
    assert!(elapsed.is_null(1));
    assert_eq!(elapsed.value(2), -1);
    let term = b.column(2).as_primitive::<IntervalYearMonthType>();
    assert_eq!((term.value(0), term.value(1)), (27, -3));
    drop_table(&mut conn, table);
}

/// Named placeholders bind parameters by field name, in any order.
#[test]
fn dml_binds_by_name_when_names_match() {
    let Some(mut conn) = connect() else { return };
    let table = "ORKIDEA_I_NAMED";
    drop_table(&mut conn, table);
    exec(
        &mut conn,
        &format!("CREATE TABLE {table} (id NUMBER(10), name VARCHAR2(20))"),
    );
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(format!(
        "INSERT INTO {table} (id, name) VALUES (:id, :name)"
    ))
    .unwrap();
    // Fields deliberately in the opposite order of the placeholders.
    stmt.bind(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(stmt.execute_update().unwrap(), Some(2));
    drop(stmt);
    let row = single_row(&mut conn, &format!("SELECT name FROM {table} WHERE id = 2"));
    assert_eq!(row.column(0).as_string::<i32>().value(0), "y");
    drop_table(&mut conn, table);
}
