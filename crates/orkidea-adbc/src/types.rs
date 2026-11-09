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

//! Oracle column metadata → Arrow type planning.
//!
//! [`plan_column`] decides, for every result column, both the Arrow field the
//! driver reports and how each value is read. `execute_schema`,
//! `get_table_schema`, and `execute` all go through it, so the reported
//! schema always matches the data.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use adbc_core::error::Status;
use arrow_schema::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use oracledb::{Metadata, VectorStorageFormat};

use crate::convert::time_unit_for_scale;
use crate::error::{error, Result};

/// Field metadata key carrying the Oracle type name (for example
/// `NUMBER(10,2)`).
pub const ORACLE_TYPE_NAME_KEY: &str = "oracle.type-name";

/// Oracle reports this scale for `FLOAT` and unconstrained `NUMBER`.
const FLOATING_SCALE: i8 = -127;

/// How `NUMBER` columns without a declared precision (and `FLOAT`) are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnconstrainedNumber {
    /// `Float64`. Fast and convenient; values beyond ~15 significant digits
    /// lose precision. Matches python-oracledb's default.
    Float64,
    /// `Decimal128(38, scale)`. Exact; values that need more fractional
    /// digits than `scale`, or more than 38 digits in total, raise an error
    /// instead of being rounded.
    Decimal,
    /// `Utf8` holding the exact decimal text.
    String,
}

impl FromStr for UnconstrainedNumber {
    type Err = adbc_core::error::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "float64" | "double" => Ok(Self::Float64),
            "decimal" => Ok(Self::Decimal),
            "string" | "utf8" => Ok(Self::String),
            other => Err(error(
                format!(
                    "invalid oracle.unconstrained_number value {other:?}; \
                     expected float64, decimal, or string"
                ),
                Status::InvalidArguments,
            )),
        }
    }
}

impl UnconstrainedNumber {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Float64 => "float64",
            Self::Decimal => "decimal",
            Self::String => "string",
        }
    }
}

/// Options that influence type planning.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TypeOptions {
    pub(crate) unconstrained_number: UnconstrainedNumber,
    pub(crate) unconstrained_number_scale: i8,
}

impl Default for TypeOptions {
    fn default() -> Self {
        Self {
            unconstrained_number: UnconstrainedNumber::Float64,
            unconstrained_number_scale: 10,
        }
    }
}

/// How one column's values are read from an Oracle row.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Reader {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    /// Any value read through its exact text form (`NUMBER` as string).
    Text,
    Boolean,
    Utf8,
    /// ROWID / UROWID, formatted as their text form.
    RowId,
    LargeUtf8,
    Binary,
    LargeBinary,
    Timestamp(TimeUnit),
    IntervalDayToSecond(TimeUnit),
    IntervalYearToMonth,
    Json,
    Vector(VectorKind),
}

/// Element type and shape of a `VECTOR` column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VectorKind {
    pub(crate) element: VectorElement,
    /// Fixed dimension count, when the column declares one.
    pub(crate) dimensions: Option<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VectorElement {
    Float32,
    Float64,
    Int8,
    /// Packed bits, one `UInt8` per 8 dimensions.
    Binary,
}

impl VectorElement {
    fn data_type(self) -> DataType {
        match self {
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Int8 => DataType::Int8,
            Self::Binary => DataType::UInt8,
        }
    }
}

impl VectorKind {
    pub(crate) fn item_field(self) -> Arc<Field> {
        Arc::new(Field::new("item", self.element.data_type(), false))
    }

    pub(crate) fn data_type(self) -> DataType {
        match self.dimensions {
            Some(n) => DataType::FixedSizeList(self.item_field(), n),
            None => DataType::List(self.item_field()),
        }
    }
}

/// A planned result column.
#[derive(Clone, Debug)]
pub(crate) struct ColumnPlan {
    pub(crate) field: Field,
    pub(crate) reader: Reader,
}

/// Workaround advice for an Oracle type the driver cannot return as Arrow.
pub(crate) fn unsupported_type_hint(db_type: &oracledb::DbType) -> &'static str {
    use oracledb::*;
    if db_type == DB_TYPE_XMLTYPE {
        "select it with XMLSERIALIZE(CONTENT col AS CLOB) or col.getClobVal()"
    } else if db_type == DB_TYPE_BFILE {
        "read the file contents in SQL (for example with DBMS_LOB) instead"
    } else if db_type == DB_TYPE_OBJECT {
        "object and collection types are not supported yet; select their attributes \
         or convert with JSON_OBJECT/JSON_ARRAYAGG"
    } else if db_type == DB_TYPE_CURSOR {
        "cursor expressions are not supported yet; query the inner SELECT directly"
    } else {
        "cast it to a supported type in SQL"
    }
}

fn unsupported(col: &Metadata, hint: &str) -> adbc_core::error::Error {
    error(
        format!(
            "Oracle column {:?} has type {}, which Orkidea cannot return as Arrow; {hint}",
            col.name(),
            col.data_type()
        ),
        Status::NotImplemented,
    )
}

/// Declared dimension count of a `VECTOR` column, parsed from its type name
/// (`VECTOR(3,FLOAT32)`; `VECTOR(*,...)` is flexible).
///
/// `Metadata::vector_dimensions` in oracledb 26.0.0-beta.5 tests the
/// flexible-dimension flag the wrong way round (it returns `None` for fixed
/// dimensions), so the type name is the reliable source. Remove once
/// <https://github.com/oracle/rust-oracledb/issues/78> is fixed upstream.
fn declared_vector_dimensions(col: &Metadata) -> Option<usize> {
    let name = col.data_type();
    let inner = name.strip_prefix("VECTOR(")?.strip_suffix(')')?;
    let dims = inner.split(',').next()?.trim();
    dims.parse::<usize>().ok().filter(|d| *d > 0)
}

/// Smallest exact Arrow representation for a decimal with the given digits.
fn decimal_reader(precision: u8, scale: i8) -> Reader {
    if precision <= 38 {
        Reader::Decimal128 { precision, scale }
    } else if precision <= 76 {
        Reader::Decimal256 { precision, scale }
    } else {
        Reader::Text
    }
}

fn number_reader(col: &Metadata, options: &TypeOptions) -> Reader {
    let precision = col.precision();
    let scale = col.scale();
    if scale == FLOATING_SCALE || precision == 0 {
        // Unconstrained NUMBER, FLOAT(b), or a computed expression.
        return match options.unconstrained_number {
            UnconstrainedNumber::Float64 => Reader::Float64,
            UnconstrainedNumber::String => Reader::Text,
            UnconstrainedNumber::Decimal => Reader::Decimal128 {
                precision: 38,
                scale: options.unconstrained_number_scale,
            },
        };
    }
    match scale {
        0 => match precision {
            1..=2 => Reader::Int8,
            3..=4 => Reader::Int16,
            5..=9 => Reader::Int32,
            10..=18 => Reader::Int64,
            _ => decimal_reader(precision, 0),
        },
        // NUMBER(p,s) with s <= p holds p digits, s of them fractional.
        s if s > 0 && s as u8 <= precision => decimal_reader(precision, s),
        // NUMBER(p,s) with s > p (e.g. NUMBER(3,5) = 0.00xyz) needs s digits.
        s if s > 0 => decimal_reader(s as u8, s),
        // Negative scale rounds to the left of the decimal point: the value
        // is an integer with up to p - s digits.
        s => decimal_reader(precision.saturating_add(s.unsigned_abs()), 0),
    }
}

fn vector_reader(col: &Metadata) -> Result<Reader> {
    if col.is_sparse_vector() {
        return Err(unsupported(
            col,
            "sparse vectors are not supported yet; convert with FROM_VECTOR(col RETURNING CLOB) \
             or TO_VECTOR(col, *, *, DENSE)",
        ));
    }
    let element = match col.vector_storage_format() {
        Some(VectorStorageFormat::Float32) => VectorElement::Float32,
        Some(VectorStorageFormat::Int8) => VectorElement::Int8,
        Some(VectorStorageFormat::Binary) => VectorElement::Binary,
        // Float64 and flexible (*) storage are read as Float64.
        Some(VectorStorageFormat::Float64) | None => VectorElement::Float64,
    };
    let dimensions = match (col.vector_storage_format(), declared_vector_dimensions(col)) {
        (_, None) => None,
        (Some(VectorStorageFormat::Binary), Some(dims)) => i32::try_from(dims / 8).ok(),
        (_, Some(dims)) => i32::try_from(dims).ok(),
    };
    Ok(Reader::Vector(VectorKind {
        element,
        dimensions,
    }))
}

fn reader_for(col: &Metadata, options: &TypeOptions) -> Result<Reader> {
    use oracledb::*;
    let db_type = col.db_type();
    let reader = if db_type == DB_TYPE_NUMBER {
        number_reader(col, options)
    } else if db_type == DB_TYPE_BINARY_INTEGER {
        Reader::Int64
    } else if db_type == DB_TYPE_BINARY_FLOAT {
        Reader::Float32
    } else if db_type == DB_TYPE_BINARY_DOUBLE {
        Reader::Float64
    } else if db_type == DB_TYPE_BOOLEAN {
        Reader::Boolean
    } else if db_type == DB_TYPE_VARCHAR
        || db_type == DB_TYPE_NVARCHAR
        || db_type == DB_TYPE_CHAR
        || db_type == DB_TYPE_NCHAR
    {
        Reader::Utf8
    } else if db_type == DB_TYPE_ROWID || db_type == DB_TYPE_UROWID {
        Reader::RowId
    } else if db_type == DB_TYPE_LONG
        || db_type == DB_TYPE_LONG_NVARCHAR
        || db_type == DB_TYPE_CLOB
        || db_type == DB_TYPE_NCLOB
    {
        Reader::LargeUtf8
    } else if db_type == DB_TYPE_RAW {
        Reader::Binary
    } else if db_type == DB_TYPE_LONG_RAW || db_type == DB_TYPE_BLOB {
        Reader::LargeBinary
    } else if db_type == DB_TYPE_DATE {
        Reader::Timestamp(TimeUnit::Second)
    } else if db_type == DB_TYPE_TIMESTAMP
        || db_type == DB_TYPE_TIMESTAMP_TZ
        || db_type == DB_TYPE_TIMESTAMP_LTZ
    {
        Reader::Timestamp(time_unit_for_scale(col.scale()))
    } else if db_type == DB_TYPE_INTERVAL_DS {
        Reader::IntervalDayToSecond(time_unit_for_scale(col.scale()))
    } else if db_type == DB_TYPE_INTERVAL_YM {
        Reader::IntervalYearToMonth
    } else if db_type == DB_TYPE_JSON {
        Reader::Json
    } else if db_type == DB_TYPE_VECTOR {
        vector_reader(col)?
    } else {
        return Err(unsupported(col, unsupported_type_hint(db_type)));
    };
    Ok(reader)
}

fn data_type_for(reader: &Reader, db_type: &oracledb::DbType) -> DataType {
    match reader {
        Reader::Int8 => DataType::Int8,
        Reader::Int16 => DataType::Int16,
        Reader::Int32 => DataType::Int32,
        Reader::Int64 => DataType::Int64,
        Reader::Float32 => DataType::Float32,
        Reader::Float64 => DataType::Float64,
        Reader::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
        Reader::Decimal256 { precision, scale } => DataType::Decimal256(*precision, *scale),
        Reader::Text | Reader::Utf8 | Reader::RowId | Reader::Json => DataType::Utf8,
        Reader::Boolean => DataType::Boolean,
        Reader::LargeUtf8 => DataType::LargeUtf8,
        Reader::Binary => DataType::Binary,
        Reader::LargeBinary => DataType::LargeBinary,
        Reader::Timestamp(unit) => {
            // TIMESTAMP WITH TIME ZONE values arrive normalised to UTC.
            // TIMESTAMP WITH LOCAL TIME ZONE values arrive as wall-clock
            // time in the session time zone, so they stay zone-less.
            let tz = (db_type == oracledb::DB_TYPE_TIMESTAMP_TZ).then(|| "UTC".into());
            DataType::Timestamp(*unit, tz)
        }
        Reader::IntervalDayToSecond(unit) => DataType::Duration(*unit),
        Reader::IntervalYearToMonth => DataType::Interval(IntervalUnit::YearMonth),
        Reader::Vector(kind) => kind.data_type(),
    }
}

/// Plans one result column.
pub(crate) fn plan_column(col: &Metadata, options: &TypeOptions) -> Result<ColumnPlan> {
    let reader = reader_for(col, options)?;
    let data_type = data_type_for(&reader, col.db_type());
    let mut metadata = HashMap::from([(ORACLE_TYPE_NAME_KEY.to_string(), col.data_type())]);
    if reader == Reader::Json {
        metadata.insert("ARROW:extension:name".to_string(), "arrow.json".to_string());
        metadata.insert("ARROW:extension:metadata".to_string(), String::new());
    }
    let field = Field::new(col.name(), data_type, col.nullable()).with_metadata(metadata);
    Ok(ColumnPlan { field, reader })
}

/// Plans every column of a result set.
pub(crate) fn plan_columns(columns: &[Metadata], options: &TypeOptions) -> Result<Vec<ColumnPlan>> {
    columns.iter().map(|c| plan_column(c, options)).collect()
}

pub(crate) fn schema_of(plans: &[ColumnPlan]) -> SchemaRef {
    Arc::new(Schema::new(
        plans.iter().map(|p| p.field.clone()).collect::<Vec<_>>(),
    ))
}

/// Convenience: the Arrow schema for a set of Oracle columns.
pub(crate) fn schema_from_metadata(
    columns: &[Metadata],
    options: &TypeOptions,
) -> Result<SchemaRef> {
    Ok(schema_of(&plan_columns(columns, options)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_widths() {
        assert_eq!(
            decimal_reader(38, 2),
            Reader::Decimal128 {
                precision: 38,
                scale: 2
            }
        );
        assert_eq!(
            decimal_reader(39, 0),
            Reader::Decimal256 {
                precision: 39,
                scale: 0
            }
        );
        assert_eq!(decimal_reader(77, 0), Reader::Text);
    }

    #[test]
    fn unconstrained_number_option_parses() {
        assert_eq!(
            "Decimal".parse::<UnconstrainedNumber>().unwrap(),
            UnconstrainedNumber::Decimal
        );
        assert!("money".parse::<UnconstrainedNumber>().is_err());
    }

    #[test]
    fn vector_shapes() {
        let kind = VectorKind {
            element: VectorElement::Float32,
            dimensions: Some(3),
        };
        assert!(matches!(kind.data_type(), DataType::FixedSizeList(_, 3)));
        let kind = VectorKind {
            element: VectorElement::Int8,
            dimensions: None,
        };
        assert!(matches!(kind.data_type(), DataType::List(_)));
    }
}
