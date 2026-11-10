// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Columnar fetching (`oracle.columnar_fetch`, on by default) must read every
//! result as reading rows does.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Optionable};
use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use orkidea_adbc::OracleConnection;

mod common;
use common::*;

fn connect_with(columnar: bool) -> Option<OracleConnection> {
    let mut database = database()?;
    database
        .set_option(
            OptionDatabase::Other("oracle.columnar_fetch".into()),
            OptionValue::String(columnar.to_string()),
        )
        .unwrap();
    Some(database.new_connection().unwrap())
}

/// Reads `sql` with columnar fetching on and off, with the same options,
/// and checks the results are equal, batch by batch.
fn read_both_ways(sql: &str, options: &[(&str, OptionValue)]) -> Vec<RecordBatch> {
    let (Some(mut columnar), Some(mut rows)) = (connect_with(true), connect_with(false)) else {
        return Vec::new();
    };
    let (schema, by_columns) = query_with(&mut columnar, sql, options);
    let (row_schema, by_rows) = query_with(&mut rows, sql, options);
    assert_eq!(schema, row_schema);
    let sizes = |b: &[RecordBatch]| b.iter().map(RecordBatch::num_rows).collect::<Vec<_>>();
    assert_eq!(sizes(&by_columns), sizes(&by_rows), "batch sizes");
    for (a, b) in by_columns.iter().zip(&by_rows) {
        assert_eq!(a, b);
    }
    by_columns
}

const ALL_TYPES: &str = "
    SELECT
        CAST(level AS NUMBER(10)) AS id,
        CAST(MOD(level, 100) - 50 AS NUMBER(2)) AS small,
        CAST(MOD(level, 100) * 1000 AS NUMBER(5)) AS medium,
        CAST(-level * 7 AS NUMBER(9)) AS wide,
        CASE WHEN MOD(level, 3) = 0 THEN NULL ELSE CAST(level * 1.25 AS NUMBER(12, 2)) END AS money,
        CAST(level / 7 AS NUMBER(38, 10)) AS precise,
        CAST(level / 3 AS NUMBER) AS free,
        CAST(level / 8 AS BINARY_DOUBLE) AS bd,
        CAST(level / 4 AS BINARY_FLOAT) AS bf,
        CASE WHEN MOD(level, 4) = 0 THEN NULL ELSE 'row ' || level END AS vc,
        CAST('fixed' AS CHAR(8)) AS ch,
        CAST(N'nät ' || level AS NVARCHAR2(20)) AS nvc,
        HEXTORAW('0A0B' || TO_CHAR(MOD(level, 256), 'FM0X')) AS rw,
        DATE '2025-01-01' + MOD(level, 40) AS dt,
        TIMESTAMP '2025-01-01 10:00:00.123456' + NUMTODSINTERVAL(level, 'SECOND') AS ts,
        FROM_TZ(TIMESTAMP '2025-01-01 10:00:00', '+02:00') + NUMTODSINTERVAL(level, 'MINUTE') AS tstz,
        MOD(level, 2) = 0 AS flag,
        CASE WHEN MOD(level, 6) = 0 THEN NULL
            ELSE NUMTODSINTERVAL(level * 1.5 - 2000, 'SECOND') END AS ids,
        CAST(NUMTODSINTERVAL(level, 'DAY') AS INTERVAL DAY(9) TO SECOND(0)) AS days,
        NUMTOYMINTERVAL(MOD(level, 30) - 15, 'MONTH') AS iym,
        CASE WHEN MOD(level, 5) = 0 THEN NULL ELSE TO_CLOB('clob ' || level) END AS cl,
        TO_BLOB(HEXTORAW('FF' || TO_CHAR(MOD(level, 256), 'FM0X'))) AS bl,
        'same' AS constant
    FROM dual CONNECT BY level <= 3000
    ORDER BY id";

#[test]
fn every_type_reads_as_rows_do() {
    let batches = read_both_ways(
        ALL_TYPES,
        &[
            ("oracle.batch_size", OptionValue::Int(1000)),
            ("oracle.fetch_array_size", OptionValue::Int(700)),
            ("oracle.prefetch_rows", OptionValue::Int(13)),
        ],
    );
    if batches.is_empty() {
        return;
    }
    let all = concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(all.num_rows(), 3000);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).max(), Some(1000));
}

#[test]
fn fetches_larger_than_batches_carry_over() {
    // Each fetch holds several batches, and the execute's rows come first.
    read_both_ways(
        ALL_TYPES,
        &[
            ("oracle.batch_size", OptionValue::Int(64)),
            ("oracle.fetch_array_size", OptionValue::Int(1000)),
            ("oracle.prefetch_rows", OptionValue::Int(2)),
        ],
    );
}

#[test]
fn defaults_and_one_batch() {
    read_both_ways(ALL_TYPES, &[]);
    read_both_ways("SELECT CAST(42 AS NUMBER(5)) AS n, 'x' AS s FROM dual", &[]);
    read_both_ways(
        "SELECT CAST(1 AS NUMBER(5)) AS n FROM dual WHERE 1 = 0",
        &[],
    );
}

#[test]
fn numbers_at_the_edges_of_their_types() {
    read_both_ways(
        "SELECT * FROM (
            SELECT CAST(9999999999 AS NUMBER(10)) AS i, CAST(-99 AS NUMBER(2)) AS s,
                   CAST(0.01 AS NUMBER(12, 2)) AS d, CAST(-0.5 AS NUMBER(3, 1)) AS h,
                   CAST(99999999999999999999999999999999999999 AS NUMBER(38)) AS big,
                   CAST(-1e-130 AS NUMBER) AS tiny, CAST(0 AS NUMBER(9)) AS z FROM dual
            UNION ALL
            SELECT -9999999999, 99, -0.01, 0.5, -99999999999999999999999999999999999999,
                   1e125, NULL FROM dual
        )",
        &[],
    );
}

#[test]
fn bound_queries_and_several_executions() {
    use adbc_core::Statement;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let (Some(mut columnar), Some(mut rows)) = (connect_with(true), connect_with(false)) else {
        return;
    };
    let read = |conn: &mut OracleConnection| {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(
            "SELECT level + :base AS n, 'v' || level AS s FROM dual CONNECT BY level <= 1500",
        )
        .unwrap();
        let params = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "base",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![0, 10_000, 20_000]))],
        )
        .unwrap();
        stmt.bind(params).unwrap();
        stmt.execute()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let (a, b) = (read(&mut columnar), read(&mut rows));
    assert_eq!(a, b);
    assert_eq!(a.iter().map(RecordBatch::num_rows).sum::<usize>(), 4500);
}

#[test]
fn the_option_takes_true_or_false() {
    let Some(mut database) = database() else {
        return;
    };
    let key = OptionDatabase::Other("oracle.columnar_fetch".into());
    assert!(database
        .set_option(key.clone(), OptionValue::String("maybe".into()))
        .is_err());
    database
        .set_option(key, OptionValue::String("false".into()))
        .unwrap();
}

#[test]
fn rowids_of_heap_and_index_organized_tables() {
    let Some(mut conn) = connect() else {
        return;
    };
    for (table, options) in [
        ("ORKIDEA_COLUMNAR_HEAP", ""),
        ("ORKIDEA_COLUMNAR_IOT", " ORGANIZATION INDEX"),
    ] {
        drop_table(&mut conn, table);
        exec(
            &mut conn,
            &format!("CREATE TABLE {table} (id NUMBER(9) PRIMARY KEY, name VARCHAR2(30)){options}"),
        );
        exec(
            &mut conn,
            &format!(
                "INSERT INTO {table} SELECT level, 'name ' || level FROM dual CONNECT BY level <= 2500"
            ),
        );
        // Physical ROWIDs, logical ones (UROWID), and NULLs.
        let batches = read_both_ways(
            &format!(
                "SELECT ROWID AS rid, id, CASE WHEN MOD(id, 7) = 0 THEN NULL ELSE ROWID END AS maybe \
                 FROM {table} ORDER BY id"
            ),
            &[("oracle.batch_size", OptionValue::Int(1000))],
        );
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2500
        );
        drop_table(&mut conn, table);
    }
}

#[test]
fn json_and_vectors() {
    read_both_ways(
        "SELECT CAST(level AS NUMBER(9)) AS id,
                CASE WHEN MOD(level, 5) = 0 THEN NULL
                     ELSE JSON('{\"n\":' || level || ',\"tags\":[\"a\",\"b\"],\"x\":1.5,\"nested\":{\"ok\":true}}')
                END AS doc,
                CASE WHEN MOD(level, 7) = 0 THEN NULL
                     ELSE TO_VECTOR('[' || level || ',2.5,-1]', 3, FLOAT32) END AS v32,
                TO_VECTOR('[' || level || ',1]', 2, FLOAT64) AS v64,
                TO_VECTOR('[' || MOD(level, 100) || ',-3,7,0]', 4, INT8) AS v8,
                TO_VECTOR('[' || level || ',' || (level + 1) || ']', *, FLOAT64) AS flexible,
                TO_VECTOR('[' || MOD(level, 256) || ',' || MOD(level * 7, 256) || ']', 16, BINARY)
                    AS bits,
                CASE WHEN MOD(level, 3) = 0 THEN NULL
                     ELSE TO_NCLOB(N'nclob ä ' || level) END AS ncl
         FROM dual CONNECT BY level <= 2500 ORDER BY id",
        &[("oracle.batch_size", OptionValue::Int(1000))],
    );
}
