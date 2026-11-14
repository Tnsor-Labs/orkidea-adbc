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

//! The schemas of a schema's tables, read from the data dictionary
//! (`oracle.table_schemas`).
//!
//! Describing a table takes a query that Oracle parses, and a round trip;
//! one dictionary query gives the columns of every table in a schema. Each
//! column is planned as a query's describe of it would be (see
//! [`crate::types`]), and is given as that describe gives it: `NUMBER(*,0)`
//! as `NUMBER(38)`, the length of character columns in characters, and
//! LOBs, which the driver reads inline, as `LONG`, `LONG NVARCHAR`, and
//! `LONG RAW`. A table is left out when the dictionary cannot say exactly
//! how a query describes one of its columns: a `VECTOR`, `XMLTYPE`, or
//! object type, or a virtual `NUMBER` or `FLOAT` column with a declared
//! precision, which a query describes as the type of its expression. Views
//! are left out: their columns are expressions, typed as Oracle compiled
//! them, and a view over such a virtual column has the declared type in the
//! dictionary too. Describe those with a query of each.

use std::collections::{BTreeMap, HashSet};

use arrow_schema::{DataType, Field, Fields, Schema};
use oracledb::{Connection, DbType, Row};

use crate::catalog::{int, required, text, DictQuery};
use crate::error::Result;
use crate::types::{plan_type, ColumnType, TypeOptions};

/// Invisible columns, which `SELECT *` leaves out, have no `column_id`.
/// Views are left out by name, read with a query of their own: as a
/// subquery here, Oracle can take seconds to check each column.
const COLUMNS_SQL: &str = "
SELECT c.table_name, c.column_name, c.data_type, c.data_type_owner, c.data_precision,
       c.data_scale, c.data_length, c.char_length, c.nullable, c.virtual_column
  FROM all_tab_cols c
 WHERE c.hidden_column = 'NO' AND c.column_id IS NOT NULL";

const VIEWS_SQL: &str = "SELECT view_name FROM all_views WHERE 1 = 1";

/// The type a query's describe gives a column of this dictionary row, or
/// `None` when the row does not say exactly.
fn described_type(row: &Row) -> Result<Option<(&'static DbType, u8, i8, String)>> {
    use oracledb::*;
    if text(row, 3)?.is_some() {
        // XMLTYPE and object types, which have an owner.
        return Ok(None);
    }
    let data_type = required(row, 2)?;
    let precision = int(row, 4)?;
    if precision.is_some() && required(row, 9)? == "YES" {
        // A virtual column declared with a precision, such as NUMBER(10,2):
        // a query describes its expression's type.
        return Ok(None);
    }
    let scale = int(row, 5)?;
    let data_length = int(row, 6)?;
    let char_length = int(row, 7)?;
    let digits = |v: Option<i64>| v.and_then(|v| u8::try_from(v).ok());
    let signed = |v: Option<i64>| v.and_then(|v| i8::try_from(v).ok());
    let sized = |db_type: &'static DbType, name: &str, size: Option<i64>| {
        size.map(|size| (db_type, 0, 0, format!("{name}({size})")))
    };
    let plain = |db_type: &'static DbType, name: &str| Some((db_type, 0, 0, name.to_string()));
    let described = match data_type.as_str() {
        "NUMBER" => match (precision, scale) {
            (None, None) => Some((DB_TYPE_NUMBER, 0, -127, "NUMBER".to_string())),
            (None, Some(0)) => Some((DB_TYPE_NUMBER, 38, 0, "NUMBER(38)".to_string())),
            (Some(_), Some(_)) => match (digits(precision), signed(scale)) {
                (Some(p), Some(0)) => Some((DB_TYPE_NUMBER, p, 0, format!("NUMBER({p})"))),
                (Some(p), Some(s)) => Some((DB_TYPE_NUMBER, p, s, format!("NUMBER({p},{s})"))),
                _ => None,
            },
            _ => None,
        },
        "FLOAT" => digits(precision).map(|p| (DB_TYPE_NUMBER, p, -127, format!("FLOAT({p})"))),
        "BINARY_FLOAT" => plain(DB_TYPE_BINARY_FLOAT, "BINARY_FLOAT"),
        "BINARY_DOUBLE" => plain(DB_TYPE_BINARY_DOUBLE, "BINARY_DOUBLE"),
        "BOOLEAN" => plain(DB_TYPE_BOOLEAN, "BOOLEAN"),
        "DATE" => plain(DB_TYPE_DATE, "DATE"),
        "JSON" => plain(DB_TYPE_JSON, "JSON"),
        "ROWID" => plain(DB_TYPE_ROWID, "ROWID"),
        "UROWID" => plain(DB_TYPE_UROWID, "UROWID"),
        "VARCHAR2" => sized(DB_TYPE_VARCHAR, "VARCHAR2", char_length),
        "NVARCHAR2" => sized(DB_TYPE_NVARCHAR, "NVARCHAR2", char_length),
        "CHAR" => sized(DB_TYPE_CHAR, "CHAR", char_length),
        "NCHAR" => sized(DB_TYPE_NCHAR, "NCHAR", char_length),
        "RAW" => sized(DB_TYPE_RAW, "RAW", data_length),
        "LONG" | "CLOB" => plain(DB_TYPE_LONG, "LONG"),
        "NCLOB" => plain(DB_TYPE_LONG_NVARCHAR, "LONG NVARCHAR"),
        "LONG RAW" | "BLOB" => plain(DB_TYPE_LONG_RAW, "LONG RAW"),
        name if name.starts_with("TIMESTAMP(") => signed(scale).and_then(|s| {
            let (db_type, zone) = if name.ends_with(") WITH LOCAL TIME ZONE") {
                (DB_TYPE_TIMESTAMP_LTZ, " WITH LOCAL TIME ZONE")
            } else if name.ends_with(") WITH TIME ZONE") {
                (DB_TYPE_TIMESTAMP_TZ, " WITH TIME ZONE")
            } else if name.ends_with(')') {
                (DB_TYPE_TIMESTAMP, "")
            } else {
                return None;
            };
            Some((db_type, 0, s, format!("TIMESTAMP({s}){zone}")))
        }),
        name if name.starts_with("INTERVAL DAY(") => match (digits(precision), signed(scale)) {
            (Some(p), Some(s)) => Some((
                DB_TYPE_INTERVAL_DS,
                p,
                s,
                format!("INTERVAL DAY({p}) TO SECOND({s})"),
            )),
            _ => None,
        },
        name if name.starts_with("INTERVAL YEAR(") => digits(precision).map(|p| {
            (
                DB_TYPE_INTERVAL_YM,
                p,
                0,
                format!("INTERVAL YEAR({p}) TO MONTH"),
            )
        }),
        _ => None,
    };
    Ok(described)
}

/// The schema of every table of `owner` that the dictionary describes
/// exactly: one struct field per table, named after it and holding its
/// columns.
pub(crate) fn table_schemas(
    conn: &Connection,
    owner: &str,
    options: &TypeOptions,
) -> Result<Schema> {
    let owner = [owner.to_string()];
    let mut views = HashSet::new();
    DictQuery::new(VIEWS_SQL)
        .one_of("owner", Some(&owner))
        .for_each(conn, |row| {
            views.insert(required(row, 0)?);
            Ok(())
        })?;
    // Each table's columns in order, or `None` once one cannot be planned.
    let mut tables: BTreeMap<String, Option<Vec<Field>>> = BTreeMap::new();
    DictQuery::new(COLUMNS_SQL)
        .one_of("c.owner", Some(&owner))
        .order_by("c.table_name, c.column_id")
        .for_each(conn, |row| {
            let table = required(row, 0)?;
            if views.contains(&table) {
                return Ok(());
            }
            let columns = tables.entry(table).or_insert_with(|| Some(Vec::new()));
            let Some(fields) = columns else { return Ok(()) };
            let Some((db_type, precision, scale, type_name)) = described_type(row)? else {
                *columns = None;
                return Ok(());
            };
            let name = required(row, 1)?;
            let col = ColumnType {
                name: &name,
                db_type,
                precision,
                scale,
                nullable: required(row, 8)? == "Y",
                type_name,
                vector: None,
            };
            // A type the driver cannot read, as describing the table finds.
            match plan_type(col, options) {
                Ok(plan) => fields.push(plan.field),
                Err(_) => *columns = None,
            }
            Ok(())
        })?;
    let fields: Vec<Field> = tables
        .into_iter()
        .filter_map(|(table, columns)| {
            columns.map(|columns| Field::new(table, DataType::Struct(Fields::from(columns)), false))
        })
        .collect();
    Ok(Schema::new(fields))
}
