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

//! `GetObjects` and `GetStatistics` from the Oracle data dictionary.
//!
//! Oracle has no catalogs; the driver reports a single catalog named after
//! the current container (`SYS_CONTEXT('USERENV', 'CON_NAME')`, for example
//! `FREEPDB1`). Schemas are database users (`ALL_USERS`). Filters are pushed
//! into the dictionary queries as `LIKE` patterns, so only matching objects
//! are read.

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::constants::{
    ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY, ADBC_STATISTIC_DISTINCT_COUNT_KEY,
    ADBC_STATISTIC_NULL_COUNT_KEY, ADBC_STATISTIC_ROW_COUNT_KEY,
};
use adbc_core::error::{Result, Status};
use adbc_core::options::ObjectDepth;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array,
    ListArray, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, StructArray,
    UInt64Array, UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, UnionFields, UnionMode};
use oracledb::{Connection, Row, ToDbValue};

use crate::connection::ConnState;
use crate::error::{error, from_oracle};

// ---------------------------------------------------------------------------
// Schemas (field definitions shared by the declared schema and the arrays)
// ---------------------------------------------------------------------------

fn item(data_type: DataType) -> FieldRef {
    Arc::new(Field::new("item", data_type, true))
}

fn list_type(data_type: DataType) -> DataType {
    DataType::List(item(data_type))
}

fn usage_fields() -> Fields {
    Fields::from(vec![
        Field::new("fk_catalog", DataType::Utf8, true),
        Field::new("fk_db_schema", DataType::Utf8, true),
        Field::new("fk_table", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
    ])
}

fn constraint_fields() -> Fields {
    Fields::from(vec![
        Field::new("constraint_name", DataType::Utf8, true),
        Field::new("constraint_type", DataType::Utf8, false),
        Field::new("constraint_column_names", list_type(DataType::Utf8), false),
        Field::new(
            "constraint_column_usage",
            list_type(DataType::Struct(usage_fields())),
            true,
        ),
    ])
}

fn column_fields() -> Fields {
    Fields::from(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, true),
        Field::new("remarks", DataType::Utf8, true),
        Field::new("xdbc_data_type", DataType::Int16, true),
        Field::new("xdbc_type_name", DataType::Utf8, true),
        Field::new("xdbc_column_size", DataType::Int32, true),
        Field::new("xdbc_decimal_digits", DataType::Int16, true),
        Field::new("xdbc_num_prec_radix", DataType::Int16, true),
        Field::new("xdbc_nullable", DataType::Int16, true),
        Field::new("xdbc_column_def", DataType::Utf8, true),
        Field::new("xdbc_sql_data_type", DataType::Int16, true),
        Field::new("xdbc_datetime_sub", DataType::Int16, true),
        Field::new("xdbc_char_octet_length", DataType::Int32, true),
        Field::new("xdbc_is_nullable", DataType::Utf8, true),
        Field::new("xdbc_scope_catalog", DataType::Utf8, true),
        Field::new("xdbc_scope_schema", DataType::Utf8, true),
        Field::new("xdbc_scope_table", DataType::Utf8, true),
        Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
        Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
    ])
}

fn table_fields() -> Fields {
    Fields::from(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new(
            "table_columns",
            list_type(DataType::Struct(column_fields())),
            true,
        ),
        Field::new(
            "table_constraints",
            list_type(DataType::Struct(constraint_fields())),
            true,
        ),
    ])
}

fn db_schema_fields() -> Fields {
    Fields::from(vec![
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new(
            "db_schema_tables",
            list_type(DataType::Struct(table_fields())),
            true,
        ),
    ])
}

pub(crate) fn objects_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            list_type(DataType::Struct(db_schema_fields())),
            true,
        ),
    ]))
}

fn statistic_value_fields() -> UnionFields {
    UnionFields::try_new(
        [0, 1, 2, 3],
        [
            Field::new("int64", DataType::Int64, true),
            Field::new("uint64", DataType::UInt64, true),
            Field::new("float64", DataType::Float64, true),
            Field::new("binary", DataType::Binary, true),
        ],
    )
    .expect("static statistics union fields")
}

fn statistic_fields() -> Fields {
    Fields::from(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, true),
        Field::new("statistic_key", DataType::Int16, false),
        Field::new(
            "statistic_value",
            DataType::Union(statistic_value_fields(), UnionMode::Dense),
            false,
        ),
        Field::new("statistic_is_approximate", DataType::Boolean, false),
    ])
}

fn statistics_db_schema_fields() -> Fields {
    Fields::from(vec![
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new(
            "db_schema_statistics",
            list_type(DataType::Struct(statistic_fields())),
            false,
        ),
    ])
}

pub(crate) fn statistics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            list_type(DataType::Struct(statistics_db_schema_fields())),
            false,
        ),
    ]))
}

// ---------------------------------------------------------------------------
// Dictionary access
// ---------------------------------------------------------------------------

/// A dictionary query with optional `LIKE` filters appended as binds.
struct DictQuery {
    sql: String,
    params: Vec<String>,
}

impl DictQuery {
    fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// Adds `AND column LIKE :n` when a pattern is given.
    fn like(mut self, column: &str, pattern: Option<&str>) -> Self {
        if let Some(pattern) = pattern {
            self.params.push(pattern.to_string());
            self.sql
                .push_str(&format!(" AND {column} LIKE :{}", self.params.len()));
        }
        self
    }

    /// Adds `AND column IN (...)` when values are given.
    fn one_of(mut self, column: &str, values: Option<&[String]>) -> Self {
        if let Some(values) = values {
            if values.is_empty() {
                self.sql.push_str(" AND 1 = 0");
                return self;
            }
            let binds: Vec<String> = values
                .iter()
                .map(|v| {
                    self.params.push(v.clone());
                    format!(":{}", self.params.len())
                })
                .collect();
            self.sql
                .push_str(&format!(" AND {column} IN ({})", binds.join(", ")));
        }
        self
    }

    fn order_by(mut self, clause: &str) -> Self {
        self.sql.push_str(" ORDER BY ");
        self.sql.push_str(clause);
        self
    }

    fn for_each(&self, conn: &Connection, mut f: impl FnMut(&Row) -> Result<()>) -> Result<()> {
        let params: Vec<&dyn ToDbValue> = self.params.iter().map(|p| p as &dyn ToDbValue).collect();
        let cursor = conn.query(&self.sql, &params).map_err(from_oracle)?;
        for row in cursor {
            f(&row.map_err(from_oracle)?)?;
        }
        Ok(())
    }
}

fn text(row: &Row, index: usize) -> Result<Option<String>> {
    row.get::<Option<String>>(index).map_err(from_oracle)
}

fn required(row: &Row, index: usize) -> Result<String> {
    text(row, index)?.ok_or_else(|| error("unexpected NULL in data dictionary", Status::Internal))
}

fn int(row: &Row, index: usize) -> Result<Option<i64>> {
    row.get::<Option<i64>>(index).map_err(from_oracle)
}

fn float(row: &Row, index: usize) -> Result<Option<f64>> {
    row.get::<Option<f64>>(index).map_err(from_oracle)
}

/// The driver's single catalog: the current container (PDB) name.
pub(crate) fn current_catalog(conn: &Connection) -> Result<String> {
    let mut name = None;
    DictQuery::new("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM dual WHERE 1 = 1").for_each(
        conn,
        |row| {
            name = text(row, 0)?;
            Ok(())
        },
    )?;
    name.ok_or_else(|| error("could not determine the container name", Status::Internal))
}

/// Matches an ADBC search pattern (`%` any run, `_` any one character).
/// `None` matches everything; an empty pattern matches nothing, because the
/// catalog always has a name.
pub(crate) fn like_match(pattern: Option<&str>, value: &str) -> bool {
    fn matches(p: &[char], v: &[char]) -> bool {
        match (p.first(), v.first()) {
            (None, None) => true,
            (None, Some(_)) => false,
            (Some('%'), _) => matches(&p[1..], v) || (!v.is_empty() && matches(p, &v[1..])),
            (Some('_'), Some(_)) => matches(&p[1..], &v[1..]),
            (Some(a), Some(b)) => a == b && matches(&p[1..], &v[1..]),
            (Some(_), None) => false,
        }
    }
    match pattern {
        None => true,
        Some(p) => {
            let p: Vec<char> = p.chars().collect();
            let v: Vec<char> = value.chars().collect();
            matches(&p, &v)
        }
    }
}

// ---------------------------------------------------------------------------
// ODBC-style column metadata
// ---------------------------------------------------------------------------

/// The `xdbc_*` fields for one column.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Xdbc {
    pub(crate) data_type: Option<i16>,
    pub(crate) sql_data_type: Option<i16>,
    pub(crate) datetime_sub: Option<i16>,
    pub(crate) column_size: Option<i32>,
    pub(crate) decimal_digits: Option<i16>,
    pub(crate) radix: Option<i16>,
    pub(crate) char_octet_length: Option<i32>,
}

// ODBC SQL type codes.
const SQL_CHAR: i16 = 1;
const SQL_DECIMAL: i16 = 3;
const SQL_FLOAT: i16 = 6;
const SQL_REAL: i16 = 7;
const SQL_DOUBLE: i16 = 8;
const SQL_DATETIME: i16 = 9;
const SQL_INTERVAL: i16 = 10;
const SQL_VARCHAR: i16 = 12;
const SQL_TYPE_TIMESTAMP: i16 = 93;
const SQL_INTERVAL_YEAR_TO_MONTH: i16 = 107;
const SQL_INTERVAL_DAY_TO_SECOND: i16 = 110;
const SQL_LONGVARCHAR: i16 = -1;
const SQL_VARBINARY: i16 = -3;
const SQL_LONGVARBINARY: i16 = -4;
const SQL_BIT: i16 = -7;
const SQL_WCHAR: i16 = -8;
const SQL_WVARCHAR: i16 = -9;
const SQL_WLONGVARCHAR: i16 = -10;
const SQL_CODE_TIMESTAMP: i16 = 3;
const SQL_CODE_YEAR_TO_MONTH: i16 = 7;
const SQL_CODE_DAY_TO_SECOND: i16 = 10;

/// Dictionary facts about a column (`ALL_TAB_COLS`).
pub(crate) struct ColumnFacts<'a> {
    pub(crate) data_type: &'a str,
    pub(crate) precision: Option<i64>,
    pub(crate) scale: Option<i64>,
    pub(crate) char_length: Option<i64>,
    pub(crate) data_length: Option<i64>,
}

fn narrow16(v: Option<i64>) -> Option<i16> {
    v.and_then(|v| i16::try_from(v).ok())
}

fn narrow32(v: Option<i64>) -> Option<i32> {
    v.and_then(|v| i32::try_from(v).ok())
}

/// Maps an Oracle column to ODBC type metadata, following Oracle's ODBC
/// driver conventions (`DATE` is a timestamp because it has a time part).
pub(crate) fn xdbc_for(c: &ColumnFacts<'_>) -> Xdbc {
    let base = c.data_type.split('(').next().unwrap_or(c.data_type).trim();
    let simple = |code: i16| Xdbc {
        data_type: Some(code),
        sql_data_type: Some(code),
        ..Xdbc::default()
    };
    let character = |code: i16| Xdbc {
        column_size: narrow32(c.char_length),
        char_octet_length: narrow32(c.data_length),
        ..simple(code)
    };
    let timestamp = |fraction: Option<i64>| {
        let fraction = fraction.unwrap_or(0);
        Xdbc {
            data_type: Some(SQL_TYPE_TIMESTAMP),
            sql_data_type: Some(SQL_DATETIME),
            datetime_sub: Some(SQL_CODE_TIMESTAMP),
            column_size: Some(19 + if fraction > 0 { fraction as i32 + 1 } else { 0 }),
            decimal_digits: narrow16(Some(fraction)),
            ..Xdbc::default()
        }
    };
    if c.data_type.starts_with("TIMESTAMP") {
        return timestamp(c.scale);
    }
    if c.data_type.starts_with("INTERVAL YEAR") {
        return Xdbc {
            data_type: Some(SQL_INTERVAL_YEAR_TO_MONTH),
            sql_data_type: Some(SQL_INTERVAL),
            datetime_sub: Some(SQL_CODE_YEAR_TO_MONTH),
            column_size: narrow32(c.precision),
            ..Xdbc::default()
        };
    }
    if c.data_type.starts_with("INTERVAL DAY") {
        return Xdbc {
            data_type: Some(SQL_INTERVAL_DAY_TO_SECOND),
            sql_data_type: Some(SQL_INTERVAL),
            datetime_sub: Some(SQL_CODE_DAY_TO_SECOND),
            column_size: narrow32(c.precision),
            decimal_digits: narrow16(c.scale),
            ..Xdbc::default()
        };
    }
    match base {
        "NUMBER" => Xdbc {
            column_size: Some(narrow32(c.precision).unwrap_or(38)),
            decimal_digits: narrow16(c.scale),
            radix: Some(10),
            ..simple(SQL_DECIMAL)
        },
        "FLOAT" => Xdbc {
            column_size: narrow32(c.precision),
            radix: Some(2),
            ..simple(SQL_FLOAT)
        },
        "BINARY_FLOAT" => Xdbc {
            column_size: Some(24),
            radix: Some(2),
            ..simple(SQL_REAL)
        },
        "BINARY_DOUBLE" => Xdbc {
            column_size: Some(53),
            radix: Some(2),
            ..simple(SQL_DOUBLE)
        },
        "CHAR" => character(SQL_CHAR),
        "NCHAR" => character(SQL_WCHAR),
        "VARCHAR2" | "VARCHAR" => character(SQL_VARCHAR),
        "NVARCHAR2" => character(SQL_WVARCHAR),
        "ROWID" | "UROWID" => Xdbc {
            column_size: narrow32(c.data_length),
            ..simple(SQL_VARCHAR)
        },
        "CLOB" | "LONG" | "JSON" | "XMLTYPE" => simple(SQL_LONGVARCHAR),
        "NCLOB" => simple(SQL_WLONGVARCHAR),
        "RAW" => Xdbc {
            column_size: narrow32(c.data_length),
            char_octet_length: narrow32(c.data_length),
            ..simple(SQL_VARBINARY)
        },
        "BLOB" | "LONG RAW" | "BFILE" => simple(SQL_LONGVARBINARY),
        "DATE" => timestamp(Some(0)),
        "BOOLEAN" => simple(SQL_BIT),
        // VECTOR, object types and other types have no ODBC equivalent.
        _ => Xdbc::default(),
    }
}

// ---------------------------------------------------------------------------
// GetObjects
// ---------------------------------------------------------------------------

struct ColumnInfo {
    name: String,
    ordinal: Option<i32>,
    remarks: Option<String>,
    type_name: String,
    xdbc: Xdbc,
    nullable: bool,
    default: Option<String>,
    identity: bool,
    virtual_column: bool,
}

struct ConstraintInfo {
    name: String,
    kind: &'static str,
    columns: Vec<String>,
    /// Referenced (schema, table, column) for foreign keys.
    references: Vec<(String, String, String)>,
}

struct TableInfo {
    name: String,
    kind: String,
    columns: Vec<ColumnInfo>,
    constraints: Vec<ConstraintInfo>,
}

type Tables = BTreeMap<String, TableInfo>;

const TABLES_SQL: &str = "
SELECT owner, table_name, table_type FROM (
    SELECT t.owner, t.table_name,
           CASE WHEN t.temporary = 'Y' THEN 'TEMPORARY TABLE' ELSE 'TABLE' END AS table_type
      FROM all_tables t
     WHERE t.dropped = 'NO' AND t.secondary = 'N' AND t.nested = 'NO'
       AND (t.iot_type IS NULL OR t.iot_type = 'IOT')
       AND NOT EXISTS (SELECT 1 FROM all_mviews m
                        WHERE m.owner = t.owner AND m.mview_name = t.table_name)
    UNION ALL
    SELECT owner, view_name, 'VIEW' FROM all_views
    UNION ALL
    SELECT owner, mview_name, 'MATERIALIZED VIEW' FROM all_mviews
) WHERE 1 = 1";

const COLUMNS_SQL: &str = "
SELECT c.owner, c.table_name, c.column_name, c.column_id, c.data_type, c.data_precision,
       c.data_scale, c.char_length, c.data_length, c.nullable, c.identity_column,
       c.virtual_column, cc.comments, c.data_default
  FROM all_tab_cols c
  LEFT JOIN all_col_comments cc
    ON cc.owner = c.owner AND cc.table_name = c.table_name AND cc.column_name = c.column_name
 WHERE c.hidden_column = 'NO'";

// NOT NULL constraints are reported through xdbc_nullable, so the implicit
// system-named `"COL" IS NOT NULL` check constraints are left out.
const CONSTRAINTS_SQL: &str = "
SELECT c.owner, c.table_name, c.constraint_name, c.constraint_type, cc.column_name,
       r.owner, r.table_name, rc.column_name
  FROM all_constraints c
  JOIN all_cons_columns cc
    ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
   AND cc.table_name = c.table_name
  LEFT JOIN all_constraints r
    ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name
  LEFT JOIN all_cons_columns rc
    ON rc.owner = r.owner AND rc.constraint_name = r.constraint_name
   AND rc.position = cc.position
 WHERE c.constraint_type IN ('P', 'U', 'R', 'C')
   AND c.table_name NOT LIKE 'BIN$%'
   AND NOT (c.constraint_type = 'C' AND c.generated = 'GENERATED NAME'
            AND c.search_condition_vc LIKE '\"%\" IS NOT NULL')";

fn constraint_kind(code: &str) -> &'static str {
    match code {
        "P" => "PRIMARY KEY",
        "U" => "UNIQUE",
        "R" => "FOREIGN KEY",
        _ => "CHECK",
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn get_objects(
    state: &ConnState,
    depth: ObjectDepth,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<Vec<&str>>,
    column_name: Option<&str>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let conn = state.lock()?;
    let catalog_name = current_catalog(&conn)?;
    let schema = objects_schema();
    if !like_match(catalog, &catalog_name) {
        let batch = RecordBatch::new_empty(schema.clone());
        return Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)));
    }

    let want_schemas = !matches!(depth, ObjectDepth::Catalogs);
    let want_tables = matches!(
        depth,
        ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
    );
    let want_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

    let mut schemas: BTreeMap<String, Tables> = BTreeMap::new();
    if want_schemas {
        DictQuery::new("SELECT username FROM all_users WHERE 1 = 1")
            .like("username", db_schema)
            .order_by("username")
            .for_each(&conn, |row| {
                schemas.insert(required(row, 0)?, Tables::new());
                Ok(())
            })?;
    }

    if want_tables {
        let types: Option<Vec<String>> =
            table_type.map(|t| t.iter().map(|s| s.to_string()).collect());
        DictQuery::new(TABLES_SQL)
            .like("owner", db_schema)
            .like("table_name", table_name)
            .one_of("table_type", types.as_deref())
            .order_by("owner, table_name")
            .for_each(&conn, |row| {
                let owner = required(row, 0)?;
                let name = required(row, 1)?;
                if let Some(tables) = schemas.get_mut(&owner) {
                    tables.insert(
                        name.clone(),
                        TableInfo {
                            name,
                            kind: required(row, 2)?,
                            columns: Vec::new(),
                            constraints: Vec::new(),
                        },
                    );
                }
                Ok(())
            })?;
    }

    if want_columns {
        DictQuery::new(COLUMNS_SQL)
            .like("c.owner", db_schema)
            .like("c.table_name", table_name)
            .like("c.column_name", column_name)
            .order_by("c.owner, c.table_name, c.column_id")
            .for_each(&conn, |row| {
                let owner = required(row, 0)?;
                let table = required(row, 1)?;
                let Some(info) = schemas.get_mut(&owner).and_then(|t| t.get_mut(&table)) else {
                    return Ok(());
                };
                let data_type = required(row, 4)?;
                let xdbc = xdbc_for(&ColumnFacts {
                    data_type: &data_type,
                    precision: int(row, 5)?,
                    scale: int(row, 6)?,
                    char_length: int(row, 7)?.filter(|v| *v > 0),
                    data_length: int(row, 8)?,
                });
                info.columns.push(ColumnInfo {
                    name: required(row, 2)?,
                    ordinal: narrow32(int(row, 3)?),
                    nullable: text(row, 9)?.as_deref() != Some("N"),
                    identity: text(row, 10)?.as_deref() == Some("YES"),
                    virtual_column: text(row, 11)?.as_deref() == Some("YES"),
                    remarks: text(row, 12)?,
                    default: text(row, 13)?
                        .map(|d| d.trim().to_string())
                        .filter(|d| !d.is_empty()),
                    type_name: data_type,
                    xdbc,
                });
                Ok(())
            })?;

        DictQuery::new(CONSTRAINTS_SQL)
            .like("c.owner", db_schema)
            .like("c.table_name", table_name)
            .order_by("c.owner, c.table_name, c.constraint_name, cc.position, cc.column_name")
            .for_each(&conn, |row| {
                let owner = required(row, 0)?;
                let table = required(row, 1)?;
                let Some(info) = schemas.get_mut(&owner).and_then(|t| t.get_mut(&table)) else {
                    return Ok(());
                };
                let name = required(row, 2)?;
                let kind = constraint_kind(&required(row, 3)?);
                if info.constraints.last().map(|c| &c.name) != Some(&name) {
                    info.constraints.push(ConstraintInfo {
                        name,
                        kind,
                        columns: Vec::new(),
                        references: Vec::new(),
                    });
                }
                let constraint = info.constraints.last_mut().expect("just pushed");
                constraint.columns.push(required(row, 4)?);
                if let (Some(r_owner), Some(r_table), Some(r_column)) =
                    (text(row, 5)?, text(row, 6)?, text(row, 7)?)
                {
                    constraint.references.push((r_owner, r_table, r_column));
                }
                Ok(())
            })?;
    }
    drop(conn);

    let catalog_schemas = want_schemas.then_some(schemas);
    let batch = objects_batch(
        &schema,
        &catalog_name,
        catalog_schemas.as_ref(),
        want_tables,
        want_columns,
    )?;
    Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
}

// ---------------------------------------------------------------------------
// Nested Arrow assembly
// ---------------------------------------------------------------------------

fn internal(e: impl std::fmt::Display) -> adbc_core::error::Error {
    error(e.to_string(), Status::Internal)
}

/// Wraps `values` into a list array; `lengths[i] == None` is a null list.
fn list(item: FieldRef, values: ArrayRef, lengths: &[Option<usize>]) -> Result<ArrayRef> {
    let offsets = OffsetBuffer::from_lengths(lengths.iter().map(|l| l.unwrap_or(0)));
    let nulls = lengths
        .iter()
        .any(Option::is_none)
        .then(|| NullBuffer::from(lengths.iter().map(Option::is_some).collect::<Vec<_>>()));
    Ok(Arc::new(
        ListArray::try_new(item, offsets, values, nulls).map_err(internal)?,
    ))
}

fn struct_array(fields: Fields, columns: Vec<ArrayRef>, len: usize) -> Result<ArrayRef> {
    let array = StructArray::try_new(fields, columns, None).map_err(internal)?;
    debug_assert_eq!(array.len(), len);
    Ok(Arc::new(array))
}

fn item_of(fields: &Fields, name: &str) -> FieldRef {
    match fields.find(name).map(|(_, f)| f.data_type()) {
        Some(DataType::List(item)) => item.clone(),
        _ => unreachable!("{name} is a list field"),
    }
}

fn columns_array(columns: &[&ColumnInfo]) -> Result<ArrayRef> {
    let strings = |f: &dyn Fn(&ColumnInfo) -> Option<String>| -> ArrayRef {
        Arc::new(StringArray::from(
            columns.iter().map(|c| f(c)).collect::<Vec<_>>(),
        ))
    };
    let i16s = |f: &dyn Fn(&ColumnInfo) -> Option<i16>| -> ArrayRef {
        Arc::new(Int16Array::from(
            columns.iter().map(|c| f(c)).collect::<Vec<_>>(),
        ))
    };
    let i32s = |f: &dyn Fn(&ColumnInfo) -> Option<i32>| -> ArrayRef {
        Arc::new(Int32Array::from(
            columns.iter().map(|c| f(c)).collect::<Vec<_>>(),
        ))
    };
    let bools = |f: &dyn Fn(&ColumnInfo) -> Option<bool>| -> ArrayRef {
        Arc::new(BooleanArray::from(
            columns.iter().map(|c| f(c)).collect::<Vec<_>>(),
        ))
    };
    let arrays = vec![
        strings(&|c| Some(c.name.clone())),
        i32s(&|c| c.ordinal),
        strings(&|c| c.remarks.clone()),
        i16s(&|c| c.xdbc.data_type),
        strings(&|c| Some(c.type_name.clone())),
        i32s(&|c| c.xdbc.column_size),
        i16s(&|c| c.xdbc.decimal_digits),
        i16s(&|c| c.xdbc.radix),
        i16s(&|c| Some(i16::from(c.nullable))),
        strings(&|c| c.default.clone()),
        i16s(&|c| c.xdbc.sql_data_type),
        i16s(&|c| c.xdbc.datetime_sub),
        i32s(&|c| c.xdbc.char_octet_length),
        strings(&|c| Some(if c.nullable { "YES" } else { "NO" }.to_string())),
        strings(&|_| None),
        strings(&|_| None),
        strings(&|_| None),
        bools(&|c| Some(c.identity)),
        bools(&|c| Some(c.virtual_column)),
    ];
    struct_array(column_fields(), arrays, columns.len())
}

fn constraints_array(constraints: &[&ConstraintInfo], catalog: &str) -> Result<ArrayRef> {
    let names: ArrayRef = Arc::new(StringArray::from(
        constraints
            .iter()
            .map(|c| Some(c.name.as_str()))
            .collect::<Vec<_>>(),
    ));
    let kinds: ArrayRef = Arc::new(StringArray::from(
        constraints.iter().map(|c| c.kind).collect::<Vec<_>>(),
    ));
    let column_values: ArrayRef = Arc::new(StringArray::from(
        constraints
            .iter()
            .flat_map(|c| c.columns.iter().map(String::as_str))
            .collect::<Vec<_>>(),
    ));
    let column_lists = list(
        item(DataType::Utf8),
        column_values,
        &constraints
            .iter()
            .map(|c| Some(c.columns.len()))
            .collect::<Vec<_>>(),
    )?;
    let refs: Vec<&(String, String, String)> = constraints
        .iter()
        .flat_map(|c| c.references.iter())
        .collect();
    let usage = struct_array(
        usage_fields(),
        vec![
            Arc::new(StringArray::from(vec![Some(catalog); refs.len()])),
            Arc::new(StringArray::from(
                refs.iter().map(|r| Some(r.0.as_str())).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                refs.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                refs.iter().map(|r| r.2.as_str()).collect::<Vec<_>>(),
            )),
        ],
        refs.len(),
    )?;
    let usage_lists = list(
        item(DataType::Struct(usage_fields())),
        usage,
        &constraints
            .iter()
            .map(|c| (c.kind == "FOREIGN KEY").then_some(c.references.len()))
            .collect::<Vec<_>>(),
    )?;
    struct_array(
        constraint_fields(),
        vec![names, kinds, column_lists, usage_lists],
        constraints.len(),
    )
}

fn tables_array(tables: &[&TableInfo], with_columns: bool, catalog: &str) -> Result<ArrayRef> {
    let fields = table_fields();
    let names: ArrayRef = Arc::new(StringArray::from(
        tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
    ));
    let kinds: ArrayRef = Arc::new(StringArray::from(
        tables.iter().map(|t| t.kind.as_str()).collect::<Vec<_>>(),
    ));
    let all_columns: Vec<&ColumnInfo> = if with_columns {
        tables.iter().flat_map(|t| t.columns.iter()).collect()
    } else {
        Vec::new()
    };
    let all_constraints: Vec<&ConstraintInfo> = if with_columns {
        tables.iter().flat_map(|t| t.constraints.iter()).collect()
    } else {
        Vec::new()
    };
    let column_lists = list(
        item_of(&fields, "table_columns"),
        columns_array(&all_columns)?,
        &tables
            .iter()
            .map(|t| with_columns.then_some(t.columns.len()))
            .collect::<Vec<_>>(),
    )?;
    let constraint_lists = list(
        item_of(&fields, "table_constraints"),
        constraints_array(&all_constraints, catalog)?,
        &tables
            .iter()
            .map(|t| with_columns.then_some(t.constraints.len()))
            .collect::<Vec<_>>(),
    )?;
    struct_array(
        fields,
        vec![names, kinds, column_lists, constraint_lists],
        tables.len(),
    )
}

fn objects_batch(
    schema: &SchemaRef,
    catalog: &str,
    schemas: Option<&BTreeMap<String, Tables>>,
    with_tables: bool,
    with_columns: bool,
) -> Result<RecordBatch> {
    let schema_fields = db_schema_fields();
    let empty = BTreeMap::new();
    let schemas_ref = schemas.unwrap_or(&empty);
    let schema_names: ArrayRef = Arc::new(StringArray::from(
        schemas_ref
            .keys()
            .map(|k| Some(k.as_str()))
            .collect::<Vec<_>>(),
    ));
    let all_tables: Vec<&TableInfo> = if with_tables {
        schemas_ref.values().flat_map(|t| t.values()).collect()
    } else {
        Vec::new()
    };
    let table_lists = list(
        item_of(&schema_fields, "db_schema_tables"),
        tables_array(&all_tables, with_columns, catalog)?,
        &schemas_ref
            .values()
            .map(|t| with_tables.then_some(t.len()))
            .collect::<Vec<_>>(),
    )?;
    let db_schemas = struct_array(
        schema_fields,
        vec![schema_names, table_lists],
        schemas_ref.len(),
    )?;
    let catalog_lists = list(
        item_of(schema.fields(), "catalog_db_schemas"),
        db_schemas,
        &[schemas.map(BTreeMap::len)],
    )?;
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![catalog])), catalog_lists],
    )
    .map_err(internal)
}

// ---------------------------------------------------------------------------
// GetStatistics
// ---------------------------------------------------------------------------

enum StatValue {
    Int(i64),
    Float(f64),
}

struct Statistic {
    table: String,
    column: Option<String>,
    key: i16,
    value: StatValue,
}

/// Returns Oracle optimizer statistics (`ALL_TAB_STATISTICS`,
/// `ALL_TAB_COL_STATISTICS`) for matching tables: row count and average row
/// length per table; distinct count, null count and average length per
/// column. Values are estimates from the last statistics gathering, so they
/// are always flagged approximate, whatever `approximate` requests. Tables
/// without gathered statistics are omitted.
pub(crate) fn get_statistics(
    state: &ConnState,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    _approximate: bool,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let conn = state.lock()?;
    let catalog_name = current_catalog(&conn)?;
    let schema = statistics_schema();
    if !like_match(catalog, &catalog_name) {
        let batch = RecordBatch::new_empty(schema.clone());
        return Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)));
    }
    let mut by_schema: BTreeMap<String, Vec<Statistic>> = BTreeMap::new();
    DictQuery::new(
        "SELECT owner, table_name, num_rows, avg_row_len FROM all_tab_statistics \
         WHERE object_type = 'TABLE' AND partition_name IS NULL AND num_rows IS NOT NULL \
         AND table_name NOT LIKE 'BIN$%'",
    )
    .like("owner", db_schema)
    .like("table_name", table_name)
    .order_by("owner, table_name")
    .for_each(&conn, |row| {
        let stats = by_schema.entry(required(row, 0)?).or_default();
        let table = required(row, 1)?;
        if let Some(rows) = int(row, 2)? {
            stats.push(Statistic {
                table: table.clone(),
                column: None,
                key: ADBC_STATISTIC_ROW_COUNT_KEY,
                value: StatValue::Int(rows),
            });
        }
        if let Some(width) = float(row, 3)? {
            stats.push(Statistic {
                table,
                column: None,
                key: ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY,
                value: StatValue::Float(width),
            });
        }
        Ok(())
    })?;
    DictQuery::new(
        "SELECT owner, table_name, column_name, num_distinct, num_nulls, avg_col_len \
         FROM all_tab_col_statistics WHERE table_name NOT LIKE 'BIN$%'",
    )
    .like("owner", db_schema)
    .like("table_name", table_name)
    .order_by("owner, table_name, column_name")
    .for_each(&conn, |row| {
        let stats = by_schema.entry(required(row, 0)?).or_default();
        let table = required(row, 1)?;
        let column = Some(required(row, 2)?);
        for (index, key) in [
            (3, ADBC_STATISTIC_DISTINCT_COUNT_KEY),
            (4, ADBC_STATISTIC_NULL_COUNT_KEY),
        ] {
            if let Some(v) = int(row, index)? {
                stats.push(Statistic {
                    table: table.clone(),
                    column: column.clone(),
                    key,
                    value: StatValue::Int(v),
                });
            }
        }
        if let Some(v) = float(row, 5)? {
            stats.push(Statistic {
                table,
                column,
                key: ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY,
                value: StatValue::Float(v),
            });
        }
        Ok(())
    })?;
    drop(conn);
    let batch = statistics_batch(&schema, &catalog_name, &by_schema)?;
    Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
}

fn statistics_batch(
    schema: &SchemaRef,
    catalog: &str,
    by_schema: &BTreeMap<String, Vec<Statistic>>,
) -> Result<RecordBatch> {
    let all: Vec<&Statistic> = by_schema.values().flatten().collect();
    let mut type_ids = Vec::with_capacity(all.len());
    let mut offsets = Vec::with_capacity(all.len());
    let (mut ints, mut floats) = (Vec::new(), Vec::new());
    for stat in &all {
        match stat.value {
            StatValue::Int(v) => {
                type_ids.push(0_i8);
                offsets.push(ints.len() as i32);
                ints.push(v);
            }
            StatValue::Float(v) => {
                type_ids.push(2_i8);
                offsets.push(floats.len() as i32);
                floats.push(v);
            }
        }
    }
    let values = UnionArray::try_new(
        statistic_value_fields(),
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        vec![
            Arc::new(Int64Array::from(ints)),
            Arc::new(UInt64Array::from(Vec::<u64>::new())),
            Arc::new(Float64Array::from(floats)),
            Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
        ],
    )
    .map_err(internal)?;
    let statistics = struct_array(
        statistic_fields(),
        vec![
            Arc::new(StringArray::from(
                all.iter().map(|s| s.table.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                all.iter().map(|s| s.column.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(Int16Array::from(
                all.iter().map(|s| s.key).collect::<Vec<_>>(),
            )),
            Arc::new(values),
            Arc::new(BooleanArray::from(vec![true; all.len()])),
        ],
        all.len(),
    )?;
    let schema_fields = statistics_db_schema_fields();
    let stat_lists = list(
        item_of(&schema_fields, "db_schema_statistics"),
        statistics,
        &by_schema
            .values()
            .map(|s| Some(s.len()))
            .collect::<Vec<_>>(),
    )?;
    let db_schemas = struct_array(
        schema_fields,
        vec![
            Arc::new(StringArray::from(
                by_schema
                    .keys()
                    .map(|k| Some(k.as_str()))
                    .collect::<Vec<_>>(),
            )),
            stat_lists,
        ],
        by_schema.len(),
    )?;
    let catalog_lists = list(
        item_of(schema.fields(), "catalog_db_schemas"),
        db_schemas,
        &[Some(by_schema.len())],
    )?;
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![catalog])), catalog_lists],
    )
    .map_err(internal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_patterns() {
        assert!(like_match(None, "FREEPDB1"));
        assert!(like_match(Some("FREE%"), "FREEPDB1"));
        assert!(like_match(Some("FREEPDB_"), "FREEPDB1"));
        assert!(like_match(Some("%"), ""));
        assert!(!like_match(Some(""), "FREEPDB1"));
        assert!(
            !like_match(Some("free%"), "FREEPDB1"),
            "patterns are case-sensitive"
        );
    }

    #[test]
    fn xdbc_mapping() {
        let facts = |data_type, precision, scale, char_length, data_length| ColumnFacts {
            data_type,
            precision,
            scale,
            char_length,
            data_length,
        };
        let number = xdbc_for(&facts("NUMBER", Some(10), Some(2), None, Some(22)));
        assert_eq!(
            (
                number.data_type,
                number.column_size,
                number.decimal_digits,
                number.radix
            ),
            (Some(SQL_DECIMAL), Some(10), Some(2), Some(10))
        );
        let unconstrained = xdbc_for(&facts("NUMBER", None, None, None, Some(22)));
        assert_eq!(unconstrained.column_size, Some(38));
        let varchar = xdbc_for(&facts("VARCHAR2", None, None, Some(20), Some(80)));
        assert_eq!(
            (
                varchar.data_type,
                varchar.column_size,
                varchar.char_octet_length
            ),
            (Some(SQL_VARCHAR), Some(20), Some(80))
        );
        let ts = xdbc_for(&facts(
            "TIMESTAMP(6) WITH TIME ZONE",
            None,
            Some(6),
            None,
            Some(13),
        ));
        assert_eq!(
            (
                ts.data_type,
                ts.sql_data_type,
                ts.datetime_sub,
                ts.column_size
            ),
            (
                Some(SQL_TYPE_TIMESTAMP),
                Some(SQL_DATETIME),
                Some(SQL_CODE_TIMESTAMP),
                Some(26)
            )
        );
        let date = xdbc_for(&facts("DATE", None, None, None, Some(7)));
        assert_eq!(
            (date.data_type, date.column_size),
            (Some(SQL_TYPE_TIMESTAMP), Some(19))
        );
        let ids = xdbc_for(&facts(
            "INTERVAL DAY(2) TO SECOND(6)",
            Some(2),
            Some(6),
            None,
            None,
        ));
        assert_eq!(
            (ids.data_type, ids.sql_data_type, ids.datetime_sub),
            (
                Some(SQL_INTERVAL_DAY_TO_SECOND),
                Some(SQL_INTERVAL),
                Some(SQL_CODE_DAY_TO_SECOND)
            )
        );
        assert_eq!(
            xdbc_for(&facts("VECTOR", None, None, None, None)),
            Xdbc::default()
        );
    }

    #[test]
    fn empty_objects_batch_matches_schema() {
        let schema = objects_schema();
        let batch = objects_batch(&schema, "PDB", None, false, false).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column(1).is_null(0));
        let stats = statistics_batch(&statistics_schema(), "PDB", &BTreeMap::new()).unwrap();
        assert_eq!(stats.num_rows(), 1);
    }
}
