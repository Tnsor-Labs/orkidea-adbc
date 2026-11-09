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

//! Bulk ingestion (`adbc.ingest.*`) using `oracledb`'s Arrow array-DML bind
//! path.
//!
//! Guarantees:
//! - Every Arrow type is validated before any DDL runs.
//! - Rows are inserted under a savepoint; a failure rolls back every row of
//!   the ingest and drops a table the ingest itself created.
//! - `replace` loads a staging table and swaps it in only after all rows are
//!   loaded; the previous table goes to the recycle bin, not `PURGE`.
//! - Bound streams are consumed chunk by chunk; memory is bounded by one
//!   input batch.
//!
//! Oracle DDL commits implicitly, so creating or replacing a table commits
//! any transaction already open on the connection.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::IngestMode;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use oracledb::Connection;

use crate::connection::ConnState;
use crate::error::{error, from_oracle};
use crate::metadata;

const SAVEPOINT: &str = "ORKIDEA_INGEST";
/// Oracle's default `PRIVATE_TEMP_TABLE_PREFIX`.
const PRIVATE_TEMP_PREFIX: &str = "ORA$PTT_";
const ORA_TABLE_NOT_FOUND: i32 = 942;

/// Parameters of one ingest call.
pub(crate) struct IngestRequest<'a> {
    pub(crate) mode: IngestMode,
    pub(crate) target_table: &'a str,
    pub(crate) target_schema: Option<&'a str>,
    pub(crate) temporary: bool,
    /// Rows per `execute_batch` round trip.
    pub(crate) chunk_rows: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TableKind {
    Permanent,
    /// `GLOBAL TEMPORARY ... ON COMMIT PRESERVE ROWS`: rows are private to
    /// the session and survive commits; the definition is permanent.
    GlobalTemporary,
    /// `PRIVATE TEMPORARY ... ON COMMIT PRESERVE DEFINITION` (18c+): table
    /// and rows are private to the session and dropped when it ends.
    PrivateTemporary,
}

/// A table name as Oracle stores it, plus its quoted SQL form.
#[derive(Clone, Debug)]
struct TableName {
    schema: Option<String>,
    table: String,
}

impl TableName {
    fn sql(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", quote(schema), quote(&self.table)),
            None => quote(&self.table),
        }
    }
}

/// Quotes an identifier already normalised by [`normalize_identifier`].
fn quote(name: &str) -> String {
    format!("\"{name}\"")
}

use crate::metadata::normalize_identifier;

fn resolve_name(schema: Option<&str>, table: &str) -> Result<TableName> {
    let (schema, table) = match (schema, table.rsplit_once('.')) {
        (Some(schema), _) => (Some(schema), table),
        (None, Some((schema, table))) => (Some(schema), table),
        (None, None) => (None, table),
    };
    Ok(TableName {
        schema: schema
            .map(|s| normalize_identifier(s, "schema name"))
            .transpose()?,
        table: normalize_identifier(table, "table name")?,
    })
}

/// Checks that every column can be bound and stored, before any DDL runs.
fn validate_bindable(schema: &Schema) -> Result<()> {
    if schema.fields().is_empty() {
        return Err(error(
            "cannot ingest a batch with no columns",
            Status::InvalidArguments,
        ));
    }
    for field in schema.fields() {
        let data_type = field.data_type();
        let precision = match data_type {
            DataType::Decimal128(p, _) | DataType::Decimal256(p, _) => Some(*p),
            _ => None,
        };
        if precision.is_some_and(|p| p > 38) {
            return Err(error(
                format!(
                    "column {:?} has precision {}; Oracle NUMBER holds at most 38 digits",
                    field.name(),
                    precision.unwrap_or_default()
                ),
                Status::InvalidArguments,
            ));
        }
        if !crate::params::row_bindable(data_type) {
            let hint = match data_type {
                DataType::Dictionary(_, _) => "cast it to its value type",
                DataType::FixedSizeList(_, _) | DataType::List(_) | DataType::LargeList(_) => {
                    "lists are stored as VECTOR and need Float32, Float64, Int8 or UInt8 elements"
                }
                _ => "cast it to a supported type",
            };
            return Err(error(
                format!(
                    "cannot ingest column {:?} of Arrow type {data_type:?}; {hint}",
                    field.name()
                ),
                Status::NotImplemented,
            ));
        }
    }
    Ok(())
}

fn is_vector_item(item: &arrow_schema::FieldRef) -> bool {
    matches!(
        item.data_type(),
        DataType::Float32 | DataType::Float64 | DataType::Int8 | DataType::UInt8
    )
}

fn ddl_type(data_type: &DataType, native_boolean: bool) -> String {
    use DataType::*;
    match data_type {
        Int8 | UInt8 => "NUMBER(3)".into(),
        Int16 | UInt16 => "NUMBER(5)".into(),
        Int32 | UInt32 => "NUMBER(10)".into(),
        Int64 => "NUMBER(19)".into(),
        UInt64 => "NUMBER(20)".into(),
        Float16 | Float32 => "BINARY_FLOAT".into(),
        Float64 => "BINARY_DOUBLE".into(),
        Decimal128(precision, scale) | Decimal256(precision, scale) => {
            format!("NUMBER({precision},{scale})")
        }
        Duration(unit) => format!(
            "INTERVAL DAY(9) TO SECOND({})",
            match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            }
        ),
        Interval(_) => "INTERVAL YEAR(9) TO MONTH".into(),
        FixedSizeList(item, _) | List(item) | LargeList(item) if is_vector_item(item) => {
            let dims = match data_type {
                FixedSizeList(_, n) if item.data_type() == &UInt8 => (n * 8).to_string(),
                FixedSizeList(_, n) => n.to_string(),
                _ => "*".into(),
            };
            let format = match item.data_type() {
                Float32 => "FLOAT32",
                Float64 => "FLOAT64",
                Int8 => "INT8",
                _ => "BINARY",
            };
            format!("VECTOR({dims}, {format})")
        }
        Utf8 | Utf8View => "VARCHAR2(4000)".into(),
        LargeUtf8 => "CLOB".into(),
        Binary | BinaryView => "RAW(2000)".into(),
        FixedSizeBinary(size) if (1..=2000).contains(size) => format!("RAW({size})"),
        FixedSizeBinary(_) | LargeBinary => "BLOB".into(),
        Date32 | Date64 => "DATE".into(),
        Timestamp(unit, tz) => {
            let precision = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            match tz {
                Some(_) => format!("TIMESTAMP({precision}) WITH TIME ZONE"),
                None => format!("TIMESTAMP({precision})"),
            }
        }
        Boolean if native_boolean => "BOOLEAN".into(),
        Boolean => "NUMBER(1)".into(),
        // Only Null remains after validate_bindable.
        _ => "VARCHAR2(1)".into(),
    }
}

fn column_names(schema: &Schema) -> Result<Vec<String>> {
    let mut seen = HashMap::new();
    let mut names = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = normalize_identifier(field.name(), "column name")?;
        if let Some(previous) = seen.insert(name.clone(), field.name().clone()) {
            return Err(error(
                format!(
                    "columns {previous:?} and {:?} both map to Oracle column {name:?}",
                    field.name()
                ),
                Status::InvalidArguments,
            ));
        }
        names.push(name);
    }
    Ok(names)
}

fn create_table(
    conn: &Connection,
    name: &TableName,
    kind: TableKind,
    schema: &Schema,
    columns: &[String],
    native_boolean: bool,
) -> Result<()> {
    let definitions: Vec<String> = schema
        .fields()
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            // Private temporary tables do not support constraints.
            let nullability = if field.is_nullable() || kind == TableKind::PrivateTemporary {
                ""
            } else {
                " NOT NULL"
            };
            format!(
                "{} {}{nullability}",
                quote(column),
                ddl_type(field.data_type(), native_boolean)
            )
        })
        .collect();
    let body = definitions.join(", ");
    let ddl = match kind {
        TableKind::Permanent => format!("CREATE TABLE {} ({body})", name.sql()),
        TableKind::GlobalTemporary => format!(
            "CREATE GLOBAL TEMPORARY TABLE {} ({body}) ON COMMIT PRESERVE ROWS",
            name.sql()
        ),
        TableKind::PrivateTemporary => format!(
            "CREATE PRIVATE TEMPORARY TABLE {} ({body}) ON COMMIT PRESERVE DEFINITION",
            name.sql()
        ),
    };
    conn.execute(&ddl, &[]).map_err(from_oracle)?;
    Ok(())
}

/// Whether a dropped permanent table is kept in the recycle bin.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DropMode {
    /// The user's table: recoverable with `FLASHBACK TABLE ... TO BEFORE DROP`.
    Recoverable,
    /// A table this ingest created and is cleaning up.
    Purge,
}

/// Drops a table. A missing table is not an error.
fn drop_table(conn: &Connection, name: &TableName, kind: TableKind, mode: DropMode) -> Result<()> {
    if kind == TableKind::GlobalTemporary {
        // A session cannot drop a global temporary table that holds its own
        // rows (ORA-14452); truncating clears only this session's rows.
        ignore_missing(conn.execute(&format!("TRUNCATE TABLE {}", name.sql()), &[]))?;
    }
    let purge = if kind == TableKind::Permanent && mode == DropMode::Purge {
        " PURGE"
    } else {
        ""
    };
    ignore_missing(conn.execute(&format!("DROP TABLE {}{purge}", name.sql()), &[]))
}

fn ignore_missing<T>(result: std::result::Result<T, oracledb::Error>) -> Result<()> {
    match result.map_err(from_oracle) {
        Ok(_) => Ok(()),
        Err(err) if err.vendor_code == ORA_TABLE_NOT_FOUND => Ok(()),
        Err(err) => Err(err),
    }
}

/// Describes an existing table, or `None` when it does not exist.
fn existing_schema(state: &ConnState, name: &TableName) -> Result<Option<Schema>> {
    match metadata::get_table_schema(state, None, name.schema.as_deref(), &name.table) {
        Ok(schema) => Ok(Some(schema)),
        Err(err) if err.status == Status::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Arrow types that can be appended into a column the driver reads as
/// `existing`.
fn types_compatible(incoming: &DataType, existing: &DataType) -> bool {
    use DataType::*;
    let numeric = |t: &DataType| {
        matches!(
            t,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
        )
    };
    let text = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
    let binary = |t: &DataType| matches!(t, Binary | LargeBinary | BinaryView | FixedSizeBinary(_));
    let datetime = |t: &DataType| matches!(t, Date32 | Date64 | Timestamp(_, _));
    match (incoming, existing) {
        (Null, _) => true,
        (a, b) if numeric(a) => numeric(b) || *b == Boolean,
        (Boolean, b) => *b == Boolean || numeric(b),
        (a, b) if text(a) => text(b),
        (a, b) if binary(a) => binary(b),
        (a, b) if datetime(a) => datetime(b),
        _ => false,
    }
}

/// Checks an append against an existing table: every incoming column must
/// exist (matched by Oracle name) with a compatible type. Columns the batch
/// omits receive their defaults.
fn check_compatible(
    name: &TableName,
    incoming: &Schema,
    columns: &[String],
    existing: &Schema,
) -> Result<()> {
    for (field, column) in incoming.fields().iter().zip(columns) {
        let Ok(target) = existing.field_with_name(column) else {
            return Err(error(
                format!(
                    "column {:?} (Oracle name {column:?}) does not exist in {}",
                    field.name(),
                    name.sql()
                ),
                Status::AlreadyExists,
            ));
        };
        if !types_compatible(field.data_type(), target.data_type()) {
            return Err(error(
                format!(
                    "column {:?} has Arrow type {:?}, which cannot be stored in {} column {column:?} ({})",
                    field.name(),
                    field.data_type(),
                    name.sql(),
                    target
                        .metadata()
                        .get(crate::types::ORACLE_TYPE_NAME_KEY)
                        .map(String::as_str)
                        .unwrap_or("unknown type")
                ),
                Status::AlreadyExists,
            ));
        }
    }
    Ok(())
}

fn insert_sql(name: &TableName, schema: &Schema, columns: &[String]) -> String {
    let targets: Vec<String> = columns.iter().map(|c| quote(c)).collect();
    let values: Vec<String> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| match field.data_type() {
            // oracledb binds Arrow timestamps as their UTC wall-clock time;
            // FROM_TZ pins that to UTC so the stored instant is correct
            // regardless of the session time zone.
            DataType::Timestamp(_, Some(_)) => {
                format!("FROM_TZ(CAST(:{} AS TIMESTAMP(9)), 'UTC')", i + 1)
            }
            _ => format!(":{}", i + 1),
        })
        .collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        name.sql(),
        targets.join(", "),
        values.join(", ")
    )
}

/// Inserts every batch, in chunks, under a savepoint. On failure all rows
/// inserted by this call are rolled back.
fn insert_all(
    conn: &Connection,
    sql: &str,
    expected: &SchemaRef,
    batches: &mut dyn Iterator<Item = Result<RecordBatch>>,
    chunk_rows: usize,
) -> Result<i64> {
    conn.execute(&format!("SAVEPOINT {SAVEPOINT}"), &[])
        .map_err(from_oracle)?;
    let result = (|| {
        let mut total: usize = 0;
        for batch in batches {
            let batch = batch?;
            if batch.schema().fields() != expected.fields() {
                return Err(error(
                    "every batch in a bound stream must have the stream's schema",
                    Status::InvalidArguments,
                ));
            }
            let rows = batch.num_rows();
            let mut offset = 0;
            while offset < rows {
                let len = chunk_rows.min(rows - offset);
                crate::params::execute_chunk(conn, sql, batch.slice(offset, len)).map_err(
                    |mut err| {
                        err.message = format!(
                            "inserting rows {}..{} failed: {}",
                            total + offset,
                            total + offset + len,
                            err.message
                        );
                        err
                    },
                )?;
                offset += len;
            }
            total += rows;
        }
        Ok(total)
    })();
    match result {
        Ok(total) => Ok(total.min(i64::MAX as usize) as i64),
        Err(err) => {
            // Best effort: the original error is the one worth reporting.
            let _ = conn.execute(&format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}"), &[]);
            Err(err)
        }
    }
}

fn staging_name(target: &TableName) -> TableName {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    TableName {
        schema: target.schema.clone(),
        table: format!("ORKIDEA$STAGE_{:X}_{:X}", std::process::id(), nanos),
    }
}

/// Performs an ingest and returns the number of rows inserted.
pub(crate) fn ingest(
    state: &ConnState,
    request: &IngestRequest<'_>,
    schema: SchemaRef,
    batches: &mut dyn Iterator<Item = Result<RecordBatch>>,
) -> Result<i64> {
    validate_bindable(&schema)?;
    let name = resolve_name(request.target_schema, request.target_table)?;
    let columns = column_names(&schema)?;
    let kind = match (
        request.temporary,
        name.table
            .to_ascii_uppercase()
            .starts_with(PRIVATE_TEMP_PREFIX),
    ) {
        (false, _) => TableKind::Permanent,
        (true, true) => TableKind::PrivateTemporary,
        (true, false) => TableKind::GlobalTemporary,
    };
    if kind == TableKind::PrivateTemporary && name.schema.is_some() {
        return Err(error(
            "private temporary tables cannot be created in another schema",
            Status::InvalidArguments,
        ));
    }
    if kind == TableKind::PrivateTemporary {
        if let Some(field) = schema
            .fields()
            .iter()
            .find(|f| matches!(ddl_type(f.data_type(), true).as_str(), "CLOB" | "BLOB"))
        {
            return Err(error(
                format!(
                    "column {:?} would need a LOB column, which private temporary tables do \
                     not support; use a name without the {PRIVATE_TEMP_PREFIX} prefix for a \
                     global temporary table",
                    field.name()
                ),
                Status::InvalidArguments,
            ));
        }
    }
    let chunk_rows = request.chunk_rows.max(1);
    let existing = existing_schema(state, &name)?;
    let conn = state.lock()?;
    let native_boolean = conn.version().map_err(from_oracle)?.0 >= 23;
    // The INSERT lists VECTOR columns last: oracledb corrupts binds that
    // follow a VECTOR bind (see params::vector_as_text), so this keeps every
    // vector on the native bind path. The table keeps the batch's column
    // order.
    let order: Vec<usize> = (0..schema.fields().len())
        .filter(|&i| !crate::params::is_vector(schema.field(i).data_type()))
        .chain(
            (0..schema.fields().len())
                .filter(|&i| crate::params::is_vector(schema.field(i).data_type())),
        )
        .collect();
    let insert_schema: SchemaRef = Arc::new(
        schema
            .project(&order)
            .map_err(|e| error(e.to_string(), Status::Internal))?,
    );
    let insert_columns: Vec<String> = order.iter().map(|&i| columns[i].clone()).collect();
    let insert = |target: &TableName, batches: &mut dyn Iterator<Item = Result<RecordBatch>>| {
        let mut projected = batches.map(|batch| {
            batch?
                .project(&order)
                .map_err(|e| error(e.to_string(), Status::Internal))
        });
        insert_all(
            &conn,
            &insert_sql(target, &insert_schema, &insert_columns),
            &insert_schema,
            &mut projected,
            chunk_rows,
        )
    };
    // Creates `target`, loads it, and drops it again if loading fails.
    let create_and_load =
        |target: &TableName, batches: &mut dyn Iterator<Item = Result<RecordBatch>>| {
            create_table(&conn, target, kind, &schema, &columns, native_boolean)?;
            insert(target, batches).inspect_err(|_| {
                let _ = drop_table(&conn, target, kind, DropMode::Purge);
            })
        };

    let rows = match (request.mode, existing) {
        (IngestMode::Create, Some(_)) => {
            return Err(error(
                format!("table {} already exists", name.sql()),
                Status::AlreadyExists,
            ))
        }
        (IngestMode::Append, None) => {
            return Err(error(
                format!("table {} does not exist", name.sql()),
                Status::NotFound,
            ))
        }
        (IngestMode::Append | IngestMode::CreateAppend, Some(existing)) => {
            check_compatible(&name, &schema, &columns, &existing)?;
            insert(&name, batches)?
        }
        (IngestMode::Create | IngestMode::CreateAppend | IngestMode::Replace, None) => {
            create_and_load(&name, batches)?
        }
        (IngestMode::Replace, Some(_)) if kind != TableKind::Permanent => {
            // Temporary data is session-scoped; nothing durable is at risk.
            drop_table(&conn, &name, kind, DropMode::Purge)?;
            create_and_load(&name, batches)?
        }
        (IngestMode::Replace, Some(_)) => {
            let staging = staging_name(&name);
            let rows = create_and_load(&staging, batches)?;
            // The DROP commits the staged rows. Until then a failure leaves
            // the original table untouched.
            if let Err(err) = drop_table(&conn, &name, kind, DropMode::Recoverable) {
                let _ = drop_table(&conn, &staging, kind, DropMode::Purge);
                return Err(err);
            }
            let rename = format!(
                "ALTER TABLE {} RENAME TO {}",
                staging.sql(),
                quote(&name.table)
            );
            conn.execute(&rename, &[]).map_err(|e| {
                let mut err = from_oracle(e);
                err.message = format!(
                    "{}: the new data is in {} and the previous table is in the recycle bin",
                    err.message,
                    staging.sql()
                );
                err
            })?;
            rows
        }
    };
    state.commit_if_autocommit(&conn)?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    #[test]
    fn identifiers_follow_oracle_rules() {
        assert_eq!(normalize_identifier("customers", "t").unwrap(), "CUSTOMERS");
        assert_eq!(
            normalize_identifier("Mixed Case", "t").unwrap(),
            "Mixed Case"
        );
        assert!(normalize_identifier("bad\"name", "t").is_err());
        assert!(normalize_identifier("  ", "t").is_err());
        let name = resolve_name(None, "hr.emp").unwrap();
        assert_eq!(name.sql(), "\"HR\".\"EMP\"");
    }

    #[test]
    fn rejects_unbindable_types_before_ddl() {
        let schema = Schema::new(vec![Field::new("d", DataType::Decimal256(50, 0), true)]);
        let err = validate_bindable(&schema).unwrap_err();
        assert!(err.message.contains("38 digits"), "{}", err.message);
        let schema = Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]);
        assert!(validate_bindable(&schema).is_err());
    }

    #[test]
    fn duplicate_oracle_column_names_are_rejected() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("ID", DataType::Int32, true),
        ]);
        assert!(column_names(&schema).is_err());
    }

    #[test]
    fn zone_aware_timestamps_are_pinned_to_utc() {
        let schema = Schema::new(vec![
            Field::new(
                "a",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("b", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);
        let columns = column_names(&schema).unwrap();
        let name = resolve_name(None, "t").unwrap();
        assert_eq!(
            insert_sql(&name, &schema, &columns),
            "INSERT INTO \"T\" (\"A\", \"B\") VALUES (FROM_TZ(CAST(:1 AS TIMESTAMP(9)), 'UTC'), :2)"
        );
    }

    #[test]
    fn boolean_ddl_depends_on_server_version() {
        assert_eq!(ddl_type(&DataType::Boolean, true), "BOOLEAN");
        assert_eq!(ddl_type(&DataType::Boolean, false), "NUMBER(1)");
    }
}
