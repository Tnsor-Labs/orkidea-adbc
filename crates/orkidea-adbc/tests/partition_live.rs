//! Live integration tests for partitioned execution.
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test partition_live
//! ```
//!
//! The consistency test needs `GRANT EXECUTE ON DBMS_FLASHBACK`.

use std::collections::HashSet;
use std::sync::Mutex;

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use adbc_core::{Connection, Database, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use orkidea_adbc::{OracleConnection, OracleDatabase};

mod common;
use common::*;

const TABLE: &str = "ORKIDEA_PT_ROWS";
static FIXTURE: Mutex<()> = Mutex::new(());

/// 10,000 rows with ids 1..=10000; every 97th id is NULL.
fn setup(conn: &mut OracleConnection) {
    drop_table(conn, TABLE);
    exec(
        conn,
        &format!("CREATE TABLE {TABLE} (id NUMBER(18), seq NUMBER(18) NOT NULL)"),
    );
    exec(
        conn,
        &format!(
            "INSERT INTO {TABLE} SELECT CASE WHEN MOD(level, 97) = 0 THEN NULL ELSE level END, level \
             FROM dual CONNECT BY level <= 10000"
        ),
    );
}

fn partitions(
    conn: &mut OracleConnection,
    sql: &str,
    options: &[(&str, OptionValue)],
) -> adbc_core::error::Result<adbc_core::PartitionedResult> {
    let mut stmt = conn.new_statement().unwrap();
    for (key, value) in options {
        stmt.set_option(statement_option(key), value.clone())?;
    }
    stmt.set_sql_query(sql).unwrap();
    stmt.execute_partitions()
}

fn read_all(conn: &OracleConnection, partition: &[u8]) -> Vec<RecordBatch> {
    conn.read_partition(partition)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn seqs(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(column)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn single_partition_is_the_whole_query() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut conn) = connect() else { return };
    setup(&mut conn);
    let result = partitions(&mut conn, &format!("SELECT seq FROM {TABLE}"), &[]).unwrap();
    assert_eq!(result.partitions.len(), 1);
    assert_eq!(result.rows_affected, -1);
    assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);
    let batches = read_all(&conn, &result.partitions[0]);
    assert_eq!(seqs(&batches, 0).len(), 10_000);
    assert_eq!(batches[0].schema().as_ref(), &result.schema);
}

#[test]
fn hash_partitions_cover_every_row_exactly_once_in_parallel() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    let Some(database) = database() else { return };
    let mut conn = database.new_connection().unwrap();
    setup(&mut conn);
    let result = partitions(
        &mut conn,
        &format!("SELECT id, seq FROM {TABLE};"),
        &[
            ("oracle.partition.count", OptionValue::Int(8)),
            ("oracle.partition.column", "id".into()),
        ],
    )
    .unwrap();
    assert_eq!(result.partitions.len(), 8);

    // Read every partition on its own connection, concurrently.
    let database: &OracleDatabase = &database;
    let schema = &result.schema;
    let per_partition: Vec<Vec<i64>> = std::thread::scope(|scope| {
        let handles: Vec<_> = result
            .partitions
            .iter()
            .map(|p| {
                scope.spawn(move || {
                    let reader = database.new_connection().unwrap();
                    let batches = read_all(&reader, p);
                    for b in &batches {
                        assert_eq!(b.schema().as_ref(), schema);
                    }
                    seqs(&batches, 1)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(
        per_partition.iter().filter(|p| !p.is_empty()).count() >= 6,
        "rows should spread across partitions: {:?}",
        per_partition.iter().map(Vec::len).collect::<Vec<_>>()
    );
    let all: Vec<i64> = per_partition.into_iter().flatten().collect();
    let unique: HashSet<i64> = all.iter().copied().collect();
    assert_eq!(all.len(), 10_000, "no row may be read twice or missed");
    assert_eq!(unique.len(), 10_000);
}

#[test]
fn consistent_partitions_read_one_snapshot() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut conn) = connect() else { return };
    setup(&mut conn);
    // Oracle cannot flash back to an SCN within seconds of a table's DDL
    // (ORA-01466), so retry until the freshly created table is old enough.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let (result, first) = loop {
        let result = partitions(
            &mut conn,
            &format!("SELECT id, seq FROM {TABLE}"),
            &[
                ("oracle.partition.count", OptionValue::Int(4)),
                ("oracle.partition.column", "seq".into()),
                ("oracle.partition.consistent", "true".into()),
            ],
        );
        let result = match result {
            Err(err) if err.status == Status::Unauthorized => {
                eprintln!("skipping: {}", err.message);
                return;
            }
            other => other.unwrap(),
        };
        match conn.read_partition(&result.partitions[0]) {
            Ok(reader) => {
                let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
                break (result, seqs(&batches, 1).len());
            }
            Err(err) if err.vendor_code == 1466 && std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
            Err(err) => panic!("{err:?}"),
        }
    };
    // Rows committed after the partitions were created are not visible.
    exec(
        &mut conn,
        &format!(
            "INSERT INTO {TABLE} SELECT level, 10000 + level FROM dual CONNECT BY level <= 500"
        ),
    );
    let rest: usize = result.partitions[1..]
        .iter()
        .map(|p| seqs(&read_all(&conn, p), 1).len())
        .sum();
    assert_eq!(first + rest, 10_000);
    // Flashback is disabled again after each read: DML works.
    exec(&mut conn, &format!("DELETE FROM {TABLE} WHERE seq > 10000"));
}

#[test]
fn descriptors_carry_type_options() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    let Some(database) = database() else { return };
    let mut conn = database.new_connection().unwrap();
    let result = partitions(
        &mut conn,
        "SELECT CAST(1.5 AS NUMBER) AS v FROM dual",
        &[("oracle.unconstrained_number", "string".into())],
    )
    .unwrap();
    // A different connection, with no statement options, reads the same schema.
    let other = database.new_connection().unwrap();
    let batches = read_all(&other, &result.partitions[0]);
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "1.5");
}

#[test]
fn invalid_partitioning_is_rejected() {
    let Some(mut conn) = connect() else { return };
    let err = partitions(
        &mut conn,
        "SELECT 1 AS a FROM dual",
        &[("oracle.partition.count", OptionValue::Int(4))],
    )
    .unwrap_err();
    assert!(
        err.message.contains("oracle.partition.column"),
        "{}",
        err.message
    );
    let err = partitions(
        &mut conn,
        "SELECT 1 AS a FROM dual",
        &[
            ("oracle.partition.count", OptionValue::Int(4)),
            ("oracle.partition.column", "missing".into()),
        ],
    )
    .unwrap_err();
    assert!(err.message.contains("MISSING"), "{}", err.message);
    assert!(partitions(&mut conn, "BEGIN NULL; END;", &[]).is_err());
    assert!(partitions(
        &mut conn,
        "SELECT 1 FROM dual",
        &[("oracle.partition.count", OptionValue::Int(0))]
    )
    .is_err());
    let err = conn.read_partition(b"not a partition").err().unwrap();
    assert_eq!(err.status, Status::InvalidArguments);
}
