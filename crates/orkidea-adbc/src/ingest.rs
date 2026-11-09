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

//! Bulk ingestion (`adbc.ingest.*`) implemented with `oracledb`'s native Arrow
//! bind path.

use std::collections::HashSet;

use adbc_core::error::{Result, Status};
use adbc_core::options::IngestMode;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, TimeUnit};
use oracledb::BindParameters;

use crate::connection::ConnState;
use crate::error::{error, from_oracle};
use crate::metadata;

/// Performs an ingest. Returns the number of rows inserted.
#[allow(clippy::too_many_arguments)]
pub(crate) fn ingest(
    state: &ConnState,
    mode: IngestMode,
    target_table: &str,
    target_schema: Option<&str>,
    temporary: bool,
    batch: &RecordBatch,
    batch_size: usize,
) -> Result<i64> {
    let schema = batch.schema();
    let qualified = qualified_name(target_schema, target_table)?;

    let existing = match metadata::get_table_schema(state, None, target_schema, target_table) {
        Ok(schema) => Some(schema),
        Err(err) if err.status == Status::NotFound => None,
        Err(err) => return Err(err),
    };

    match mode {
        IngestMode::Create => {
            if existing.is_some() {
                return Err(error(
                    format!("table {qualified} already exists"),
                    Status::AlreadyExists,
                ));
            }
            create_table(state, &qualified, &schema, temporary)?;
        }
        IngestMode::Replace => {
            if existing.is_some() {
                drop_table(state, &qualified)?;
            }
            create_table(state, &qualified, &schema, temporary)?;
        }
        IngestMode::Append => {
            let existing = existing.ok_or_else(|| {
                error(
                    format!("table {qualified} does not exist"),
                    Status::NotFound,
                )
            })?;
            check_compatible(&qualified, &schema, &existing)?;
        }
        IngestMode::CreateAppend => {
            if let Some(existing) = existing {
                check_compatible(&qualified, &schema, &existing)?;
            } else {
                create_table(state, &qualified, &schema, temporary)?;
            }
        }
    }

    let rows = batch.num_rows();
    if rows == 0 {
        return Ok(0);
    }

    let insert_sql = insert_sql(&qualified, &schema)?;
    let size = batch_size.max(1);
    let conn = state.lock()?;
    let mut offset = 0;
    while offset < rows {
        let len = size.min(rows - offset);
        let chunk = batch.slice(offset, len);
        conn.execute_batch(&insert_sql, BindParameters::Arrow(chunk))
            .map_err(from_oracle)?;
        offset += len;
    }
    state.commit_if_autocommit(&conn)?;
    Ok(rows.min(i64::MAX as usize) as i64)
}

fn qualified_name(schema: Option<&str>, table: &str) -> Result<String> {
    let (schema, table) = match (schema, table.rsplit_once('.')) {
        (Some(schema), _) => (Some(schema), table),
        (None, Some((schema, table))) => (Some(schema), table),
        (None, None) => (None, table),
    };
    let table = quote_identifier(table, "table name")?;
    match schema {
        Some(schema) => Ok(format!(
            "{}.{}",
            quote_identifier(schema, "schema name")?,
            table
        )),
        None => Ok(table),
    }
}

fn quote_identifier(value: &str, what: &str) -> Result<String> {
    if value.trim().is_empty() {
        return Err(error(
            format!("{what} must not be empty"),
            Status::InvalidArguments,
        ));
    }
    // Rejects embedded double quotes, which is the only way to break out of a
    // quoted identifier. Normalize to uppercase to match Oracle's default
    // unquoted identifier behavior.
    oracledb::enquote_name(value, true).map_err(from_oracle)
}

fn create_table(
    state: &ConnState,
    qualified: &str,
    schema: &Schema,
    temporary: bool,
) -> Result<()> {
    let mut seen = HashSet::new();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = quote_identifier(field.name(), "column name")?;
        if !seen.insert(name.clone()) {
            return Err(error(
                format!("duplicate column name {name} in ingest schema"),
                Status::InvalidArguments,
            ));
        }
        let ty = ddl_type(field.data_type())?;
        let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
        columns.push(format!("{name} {ty}{nullability}"));
    }
    if columns.is_empty() {
        return Err(error(
            "cannot ingest a batch with no columns",
            Status::InvalidArguments,
        ));
    }
    let temporary = if temporary { "GLOBAL TEMPORARY " } else { "" };
    let ddl = format!(
        "CREATE {temporary}TABLE {qualified} ({})",
        columns.join(", ")
    );
    state.lock()?.execute(&ddl, &[]).map_err(from_oracle)?;
    Ok(())
}

fn drop_table(state: &ConnState, qualified: &str) -> Result<()> {
    let ddl = format!("DROP TABLE {qualified} PURGE");
    match state.lock()?.execute(&ddl, &[]) {
        Ok(_) => Ok(()),
        // ORA-00942: table or view does not exist
        Err(err) if err.to_string().contains("ORA-00942") => Ok(()),
        Err(err) => Err(from_oracle(err)),
    }
}

fn insert_sql(qualified: &str, schema: &Schema) -> Result<String> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    let mut binds = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        columns.push(quote_identifier(field.name(), "column name")?);
        binds.push(format!(":{}", i + 1));
    }
    Ok(format!(
        "INSERT INTO {qualified} ({}) VALUES ({})",
        columns.join(", "),
        binds.join(", ")
    ))
}

fn check_compatible(qualified: &str, incoming: &Schema, existing: &Schema) -> Result<()> {
    let mut mismatch = None;
    if incoming.fields().len() != existing.fields().len() {
        mismatch = Some(format!(
            "incoming batch has {} columns but {qualified} has {}",
            incoming.fields().len(),
            existing.fields().len()
        ));
    } else {
        for (i, (a, b)) in incoming
            .fields()
            .iter()
            .zip(existing.fields().iter())
            .enumerate()
        {
            if !a.name().eq_ignore_ascii_case(b.name()) {
                mismatch = Some(format!(
                    "column {i} is named {:?} in the batch but {:?} in {qualified}",
                    a.name(),
                    b.name()
                ));
                break;
            }
            if !types_compatible(a.data_type(), b.data_type()) {
                mismatch = Some(format!(
                    "column {:?} has type {:?} in the batch but {:?} in {qualified}; \
                     cast the Arrow column to a matching type",
                    a.name(),
                    a.data_type(),
                    b.data_type()
                ));
                break;
            }
        }
    }
    match mismatch {
        Some(message) => Err(error(message, Status::AlreadyExists)),
        None => Ok(()),
    }
}

fn types_compatible(incoming: &DataType, existing: &DataType) -> bool {
    use DataType::*;
    if incoming == existing {
        return true;
    }
    match (incoming, existing) {
        (Null, _) | (_, Null) => true,
        (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Boolean, other) => {
            is_numeric(other)
        }
        (Decimal128(_, _) | Decimal256(_, _), other) => is_numeric(other),
        (other, Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64) => {
            is_numeric(other)
        }
        (Utf8 | LargeUtf8 | Utf8View | Dictionary(_, _), Utf8 | LargeUtf8 | Utf8View) => true,
        (Dictionary(_, value), other) => types_compatible(value, other),
        (Binary | LargeBinary | FixedSizeBinary(_), Binary | LargeBinary | FixedSizeBinary(_)) => {
            true
        }
        (Date32 | Date64 | Timestamp(_, _), Timestamp(_, _)) => true,
        (Timestamp(_, _), Date32 | Date64) => true,
        _ => false,
    }
}

fn is_numeric(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        data_type,
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
            | Boolean
    )
}

fn ddl_type(data_type: &DataType) -> Result<String> {
    use DataType::*;
    let ty = match data_type {
        Int8 => "NUMBER(3,0)".to_string(),
        Int16 => "NUMBER(5,0)".to_string(),
        Int32 => "NUMBER(10,0)".to_string(),
        Int64 => "NUMBER(19,0)".to_string(),
        UInt8 => "NUMBER(3,0)".to_string(),
        UInt16 => "NUMBER(5,0)".to_string(),
        UInt32 => "NUMBER(10,0)".to_string(),
        UInt64 => "NUMBER(20,0)".to_string(),
        Float32 => "BINARY_FLOAT".to_string(),
        Float64 => "BINARY_DOUBLE".to_string(),
        Decimal128(precision, scale) | Decimal256(precision, scale) => {
            format!("NUMBER({precision},{scale})")
        }
        Utf8 | Utf8View => "VARCHAR2(4000)".to_string(),
        LargeUtf8 => "CLOB".to_string(),
        Binary => "RAW(2000)".to_string(),
        LargeBinary => "BLOB".to_string(),
        FixedSizeBinary(size) => {
            if *size == 0 || *size > 2000 {
                return Err(error(
                    format!("FixedSizeBinary({size}) cannot be stored in Oracle RAW(2000)"),
                    Status::NotImplemented,
                ));
            }
            format!("RAW({size})")
        }
        Date32 | Date64 => "DATE".to_string(),
        Timestamp(unit, tz) => {
            let precision = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            if tz.is_some() {
                format!("TIMESTAMP({precision}) WITH TIME ZONE")
            } else {
                format!("TIMESTAMP({precision})")
            }
        }
        Boolean => "NUMBER(1,0)".to_string(),
        Null => "VARCHAR2(1)".to_string(),
        other => {
            return Err(error(
                format!("Orkidea cannot ingest Arrow type {other:?}"),
                Status::NotImplemented,
            ));
        }
    };
    Ok(ty)
}
