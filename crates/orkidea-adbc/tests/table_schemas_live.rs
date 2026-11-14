//! Live integration tests for `oracle.table_schemas`, which reads the schemas
//! of a schema's tables from the data dictionary.
//!
//! ```bash
//! ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
//!   cargo test -p orkidea-adbc --test table_schemas_live
//! ```

use std::sync::Mutex;

use adbc_core::options::OptionValue;
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Fields, Schema};
use orkidea_adbc::OracleConnection;

mod common;
use common::*;

/// The fixtures are shared, so tests that create them run one at a time.
static FIXTURE: Mutex<()> = Mutex::new(());

const TABLES: [&str; 7] = [
    "ORKIDEA_TS_TYPES",
    "ORKIDEA_TS_VIRTUAL",
    "ORKIDEA_TS_LONG",
    "ORKIDEA_TS_LONG_RAW",
    "ORKIDEA_TS_VECTOR",
    "ORKIDEA_TS_XML",
    "orkidea_ts_Mixed",
];

const VIEWS: [&str; 2] = ["ORKIDEA_TS_VIEW", "ORKIDEA_TS_INVALID"];

fn drop_view(conn: &mut OracleConnection, view: &str) {
    exec(
        conn,
        &format!(
            "BEGIN EXECUTE IMMEDIATE 'DROP VIEW \"{view}\"'; \
             EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        ),
    );
}

fn cleanup(conn: &mut OracleConnection) {
    for view in VIEWS {
        drop_view(conn, view);
    }
    for table in TABLES.iter().chain(["ORKIDEA_TS_GONE"].iter()) {
        drop_table(conn, &format!("\"{table}\""));
    }
}

fn setup(conn: &mut OracleConnection) {
    cleanup(conn);
    exec(
        conn,
        "CREATE TABLE orkidea_ts_types (
            n NUMBER, n5 NUMBER(5), n102 NUMBER(10,2), nstar NUMBER(*,0), nint INTEGER,
            nneg NUMBER(5,-2), n35 NUMBER(3,5), n38 NUMBER(38), n40 NUMBER(38,10),
            f FLOAT, f63 FLOAT(63), bf BINARY_FLOAT, bd BINARY_DOUBLE,
            v VARCHAR2(10), vc VARCHAR2(10 CHAR), vb VARCHAR2(10 BYTE), nv NVARCHAR2(10),
            c CHAR(3), cc CHAR(3 CHAR), nc NCHAR(3), r RAW(16),
            d DATE, ts TIMESTAMP, ts0 TIMESTAMP(0), ts3 TIMESTAMP(3), ts9 TIMESTAMP(9),
            tz TIMESTAMP(3) WITH TIME ZONE, ltz TIMESTAMP WITH LOCAL TIME ZONE,
            ids INTERVAL DAY TO SECOND, ids2 INTERVAL DAY(2) TO SECOND(0),
            iym INTERVAL YEAR TO MONTH, iym4 INTERVAL YEAR(4) TO MONTH,
            cl CLOB, ncl NCLOB, bl BLOB, b BOOLEAN, j JSON, rid ROWID, urid UROWID,
            nn NUMBER NOT NULL, vnn VARCHAR2(5) DEFAULT 'x' NOT NULL, hid NUMBER INVISIBLE,
            vv AS (n5 * 2), idc NUMBER GENERATED ALWAYS AS IDENTITY,
            ckn NUMBER(4) CHECK (ckn IS NOT NULL)
        )",
    );
    exec(
        conn,
        "CREATE TABLE orkidea_ts_virtual (name VARCHAR2(10), d DATE,
            total NUMBER(10,2) AS (LENGTH(name) * 2), ts TIMESTAMP(3) AS (CAST(d AS TIMESTAMP(6))))",
    );
    exec(conn, "CREATE TABLE orkidea_ts_long (id NUMBER(3), l LONG)");
    exec(conn, "CREATE TABLE orkidea_ts_long_raw (lr LONG RAW)");
    exec(
        conn,
        "CREATE TABLE orkidea_ts_vector (id NUMBER(3), v VECTOR(3, FLOAT32))",
    );
    exec(
        conn,
        "CREATE TABLE orkidea_ts_xml (id NUMBER(3), x XMLTYPE)",
    );
    exec(
        conn,
        "CREATE TABLE \"orkidea_ts_Mixed\" (\"Lower\" NUMBER(9), \"lower\" VARCHAR2(4))",
    );
    exec(
        conn,
        "CREATE VIEW orkidea_ts_view AS SELECT n5 + 1 AS e1, TRIM(v) AS e2,
            CAST(v AS VARCHAR2(7)) AS e3, nn, vnn, SYSDATE AS e4, SYSTIMESTAMP AS e5,
            CAST(NULL AS NUMBER(6,1)) AS e6, n102 * 2 AS e7, COUNT(*) OVER () AS e8,
            ts - ts AS e10, 'abc' AS e11, UPPER(nv) AS e12, idc
          FROM orkidea_ts_types",
    );
    exec(conn, "CREATE TABLE orkidea_ts_gone (x NUMBER)");
    exec(
        conn,
        "CREATE VIEW orkidea_ts_invalid AS SELECT x FROM orkidea_ts_gone",
    );
    drop_table(conn, "orkidea_ts_gone");
}

fn table_schemas(
    conn: &mut OracleConnection,
    owner: &str,
    options: &[(&str, OptionValue)],
) -> Schema {
    let mut stmt = conn.new_statement().unwrap();
    for (key, value) in options {
        stmt.set_option(statement_option(key), value.clone())
            .unwrap();
    }
    stmt.set_option(statement_option("oracle.table_schemas"), owner.into())
        .unwrap();
    stmt.execute_schema().unwrap()
}

fn described(conn: &mut OracleConnection, table: &str, options: &[(&str, OptionValue)]) -> Schema {
    let mut stmt = conn.new_statement().unwrap();
    for (key, value) in options {
        stmt.set_option(statement_option(key), value.clone())
            .unwrap();
    }
    stmt.set_sql_query(format!("SELECT * FROM \"{table}\" WHERE 1 = 0"))
        .unwrap();
    stmt.execute_schema().unwrap()
}

fn columns(schema: &Schema, table: &str) -> Option<Fields> {
    let field = schema.field_with_name(table).ok()?;
    match field.data_type() {
        DataType::Struct(fields) => Some(fields.clone()),
        other => panic!("{table} is a {other}, not a struct"),
    }
}

fn current_user(conn: &mut OracleConnection) -> String {
    let row = single_row(conn, "SELECT USER AS u FROM dual");
    row.column(0).as_string::<i32>().value(0).to_string()
}

#[test]
fn every_table_reads_as_a_query_describes_it() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut conn) = connect() else { return };
    setup(&mut conn);
    let owner = current_user(&mut conn);
    for options in [
        vec![],
        vec![("oracle.unconstrained_number", OptionValue::from("decimal"))],
    ] {
        let schema = table_schemas(&mut conn, &owner, &options);
        for table in [
            "ORKIDEA_TS_TYPES",
            "ORKIDEA_TS_LONG",
            "ORKIDEA_TS_LONG_RAW",
            "orkidea_ts_Mixed",
        ] {
            assert!(columns(&schema, table).is_some(), "{table} is missing");
        }
        // Every table the dictionary describes, the fixtures and any other
        // in the schema, has the fields a query of it has.
        for field in schema.fields() {
            let table = field.name();
            let listed = columns(&schema, table).unwrap();
            let queried = described(&mut conn, table, &options);
            assert_eq!(&listed, queried.fields(), "{table} with {options:?}");
        }
    }
    cleanup(&mut conn);
}

#[test]
fn tables_the_dictionary_cannot_describe_are_left_out() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    let Some(mut conn) = connect() else { return };
    setup(&mut conn);
    let owner = current_user(&mut conn);
    let schema = table_schemas(&mut conn, &owner, &[]);
    // A VECTOR's shape and an XMLTYPE are not read from the dictionary; a
    // query describes a virtual NUMBER(10,2) as NUMBER, and so does a view
    // over it, which the dictionary gives as NUMBER(10,2); views are
    // described.
    for table in [
        "ORKIDEA_TS_VECTOR",
        "ORKIDEA_TS_XML",
        "ORKIDEA_TS_VIRTUAL",
        "ORKIDEA_TS_VIEW",
        "ORKIDEA_TS_INVALID",
    ] {
        assert!(columns(&schema, table).is_none(), "{table} is listed");
    }
    // The invisible column, which SELECT * leaves out, is not listed.
    let types = columns(&schema, "ORKIDEA_TS_TYPES").unwrap();
    assert!(types.find("HID").is_none());
    cleanup(&mut conn);
}

#[test]
fn the_option_names_a_schema_as_sql_does() {
    let Some(mut conn) = connect() else { return };
    let owner = current_user(&mut conn);
    let key = statement_option("oracle.table_schemas");

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(key.clone(), owner.to_lowercase().into())
        .unwrap();
    assert_eq!(stmt.get_option_string(key.clone()).unwrap(), owner);
    stmt.set_option(key.clone(), "\"lower\"".into()).unwrap();
    assert_eq!(stmt.get_option_string(key.clone()).unwrap(), "lower");
    // A schema with no tables, or none the user can see, has no fields.
    assert!(stmt.execute_schema().unwrap().fields().is_empty());

    // It only gives a schema.
    let message = stmt.execute().err().unwrap().message;
    assert!(message.contains("only gives a schema"), "{message}");

    // A query replaces it.
    stmt.set_sql_query("SELECT 1 AS one FROM dual").unwrap();
    assert!(stmt.get_option_string(key).is_err());
    assert_eq!(stmt.execute_schema().unwrap().field(0).name(), "ONE");
}
