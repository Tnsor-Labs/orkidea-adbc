//! Live regression tests for wide rows of mixed types.
//!
//! Oracle compresses a fetched row by sending only the columns that differ
//! from the previous row, plus a bit vector with one bit per column of the
//! statement. When an execute response carried no describe info (after a
//! prepare, or on a statement cache hit), `oracledb` 26.0.0-beta.5 sized the
//! bit vector from the number of columns sent instead. Rows wider than eight
//! columns were then misparsed ("unknown TTC message type"), and the
//! connection was left unusable.
//!
//! The tables are generated from a fixed seed, for the table layout and for
//! Oracle's `DBMS_RANDOM`, so every run builds the same data. They create
//! and drop tables named `ORKIDEA_W_*`.
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test wide_rows_live
//! ```

use adbc_core::{Connection, Statement};
use arrow_array::RecordBatch;
use orkidea_adbc::OracleConnection;

mod common;
use common::*;

const TABLES: usize = 30;

/// Column types and the SQL that generates a value of each.
const TYPES: &[(&str, &str)] = &[
    ("NUMBER(10)", "TRUNC(DBMS_RANDOM.VALUE(-1e9, 1e9))"),
    ("NUMBER(12,2)", "ROUND(DBMS_RANDOM.VALUE(-1e6, 1e6), 2)"),
    ("NUMBER", "DBMS_RANDOM.VALUE"),
    ("BINARY_DOUBLE", "DBMS_RANDOM.VALUE"),
    (
        "VARCHAR2({n})",
        "DBMS_RANDOM.STRING('p', TRUNC(DBMS_RANDOM.VALUE(0, {n})))",
    ),
    ("DATE", "DATE '2000-01-01' + DBMS_RANDOM.VALUE(0, 9000)"),
    (
        "TIMESTAMP(6)",
        "TIMESTAMP '2000-01-01 00:00:00' + NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0, 1e8), 'SECOND')",
    ),
    (
        "TIMESTAMP(6) WITH TIME ZONE",
        "TIMESTAMP '2030-01-01 00:00:00 +02:00' - NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0, 1e8), 'SECOND')",
    ),
    (
        "CLOB",
        "TO_CLOB(DBMS_RANDOM.STRING('a', TRUNC(DBMS_RANDOM.VALUE(0, 400))))",
    ),
    (
        "BLOB",
        "UTL_RAW.CAST_TO_RAW(DBMS_RANDOM.STRING('x', TRUNC(DBMS_RANDOM.VALUE(1, 300))))",
    ),
    (
        "RAW(100)",
        "UTL_RAW.CAST_TO_RAW(DBMS_RANDOM.STRING('x', TRUNC(DBMS_RANDOM.VALUE(1, 100))))",
    ),
    (
        "INTERVAL DAY(3) TO SECOND(6)",
        "NUMTODSINTERVAL(DBMS_RANDOM.VALUE(-1e6, 1e6), 'SECOND')",
    ),
    (
        "BOOLEAN",
        "CASE WHEN DBMS_RANDOM.VALUE < 0.5 THEN TRUE ELSE FALSE END",
    ),
];

/// A small deterministic generator (xorshift64*), so the layout of every
/// table is the same on every run.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33) as usize % n
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// Creates table `name` with 5 to 40 random columns (15% NULLs) and returns
/// its column and row counts.
fn create_table(
    conn: &mut OracleConnection,
    rng: &mut Rng,
    name: &str,
    seed: usize,
) -> (usize, usize) {
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for c in 0..5 + rng.below(36) {
        let (ddl, value) = rng.pick(TYPES);
        let n = rng.pick(&[1, 10, 50, 200, 1000, 4000]).to_string();
        columns.push(format!("c{c} {}", ddl.replace("{n}", &n)));
        values.push(format!(
            "CASE WHEN DBMS_RANDOM.VALUE < 0.15 THEN NULL ELSE {} END",
            value.replace("{n}", &n)
        ));
    }
    let rows = *rng.pick(&[3, 50, 200, 600]);
    drop_table(conn, name);
    exec(
        conn,
        &format!(
            "CREATE TABLE {name} (id NUMBER(10), {})",
            columns.join(", ")
        ),
    );
    exec(conn, &format!("BEGIN DBMS_RANDOM.SEED({seed}); END;"));
    exec(
        conn,
        &format!(
            "INSERT INTO {name} SELECT level, {} FROM dual CONNECT BY level <= {rows}",
            values.join(", ")
        ),
    );
    (columns.len(), rows)
}

fn read(
    conn: &mut OracleConnection,
    sql: &str,
    prepare: bool,
) -> adbc_core::error::Result<Vec<RecordBatch>> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    if prepare {
        stmt.prepare()?;
    }
    Ok(stmt.execute()?.collect::<Result<Vec<_>, _>>()?)
}

/// Each table is read three ways, which must agree: a plain execute on a
/// new connection, a prepared execute, and the same query again on the same
/// connection (a statement cache hit).
#[test]
fn wide_random_tables_read_identically_on_every_path() {
    let Some(mut conn) = connect() else { return };
    let mut rng = Rng(0x6f72_6b69_6465_6121);
    let mut failures = Vec::new();
    for t in 0..TABLES {
        let name = format!("ORKIDEA_W_{t}");
        let (columns, rows) = create_table(&mut conn, &mut rng, &name, t);
        let sql = format!("SELECT * FROM {name} ORDER BY id");
        let outcome = read(&mut connect().unwrap(), &sql, false).and_then(|reference| {
            let mut reader = connect().unwrap();
            let prepared = read(&mut reader, &sql, true)?;
            let cached = read(&mut reader, &sql, false)?;
            Ok((reference, prepared, cached))
        });
        let label = format!("{name} ({columns} columns, {rows} rows)");
        match outcome {
            Ok((reference, prepared, cached)) => {
                let read_rows: usize = reference.iter().map(|b| b.num_rows()).sum();
                assert_eq!(read_rows, rows, "{label}");
                if prepared != reference || cached != reference {
                    failures.push(format!("{label}: results differ"));
                }
            }
            Err(e) => failures.push(format!("{label}: {}", e.message)),
        }
        drop_table(&mut conn, &name);
    }
    assert!(
        failures.is_empty(),
        "{} of {TABLES} tables failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
