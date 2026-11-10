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

//! Columnar fetching: query values decoded from the network buffer straight
//! into column buffers, without building `oracledb` rows.
//!
//! `oracledb`'s [`Cursor::fetch_into`] hands each value of a fetched row to a
//! [`RowSink`] as the bytes it arrived as. [`Columns`] decodes them into one
//! buffer per column, which become Arrow arrays. It takes the columns of the
//! common types (numbers, text, raw, booleans, binary floats, dates, and
//! timestamps); a query with any other column is read as rows.
//!
//! Rows the cursor already holds (those that came with the execute) are
//! appended from [`Row`]s, with the conversions of the row reader, so a
//! result is one sequence of rows whichever way each arrived.

use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::{
    make_array, Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray,
};
use arrow_buffer::{i256, BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, TimeUnit};
use oracledb::{Cursor, Metadata, OracleNumber, OracleTimestamp, Row, RowSink, WireValue};

use crate::convert;
use crate::error::{error, from_oracle, Result};
use crate::types::{ColumnPlan, Reader};

/// What a column's values become.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal128(u8, i8),
    Decimal256(u8, i8),
    /// A `NUMBER` as its exact text.
    NumberText,
    Boolean,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Timestamp(TimeUnit),
}

/// The kind of column a plan reads, when values of its Oracle type can be
/// fetched into it.
fn kind(reader: &Reader, db_type: &oracledb::DbType) -> Option<Kind> {
    use oracledb::*;
    let number = db_type == DB_TYPE_NUMBER || db_type == DB_TYPE_BINARY_INTEGER;
    let text = db_type == DB_TYPE_VARCHAR
        || db_type == DB_TYPE_CHAR
        || db_type == DB_TYPE_NVARCHAR
        || db_type == DB_TYPE_NCHAR
        || db_type == DB_TYPE_LONG;
    let raw = db_type == DB_TYPE_RAW || db_type == DB_TYPE_LONG_RAW;
    let datetime = db_type == DB_TYPE_DATE
        || db_type == DB_TYPE_TIMESTAMP
        || db_type == DB_TYPE_TIMESTAMP_TZ
        || db_type == DB_TYPE_TIMESTAMP_LTZ;
    let kind = match reader {
        Reader::Int8 if number => Kind::Int8,
        Reader::Int16 if number => Kind::Int16,
        Reader::Int32 if number => Kind::Int32,
        Reader::Int64 if number => Kind::Int64,
        Reader::Float32 if number || db_type == DB_TYPE_BINARY_FLOAT => Kind::Float32,
        Reader::Float64 if number || db_type == DB_TYPE_BINARY_DOUBLE => Kind::Float64,
        Reader::Decimal128 { precision, scale } if number => Kind::Decimal128(*precision, *scale),
        Reader::Decimal256 { precision, scale } if number => Kind::Decimal256(*precision, *scale),
        Reader::Text if number => Kind::NumberText,
        Reader::Boolean if db_type == DB_TYPE_BOOLEAN => Kind::Boolean,
        Reader::Utf8 if text => Kind::Utf8,
        Reader::LargeUtf8 if text => Kind::LargeUtf8,
        Reader::Binary if raw => Kind::Binary,
        Reader::LargeBinary if raw => Kind::LargeBinary,
        Reader::Timestamp(unit) if datetime => Kind::Timestamp(*unit),
        _ => return None,
    };
    Some(kind)
}

/// Decodes Oracle `NUMBER` bytes into an integer scaled by `10^scale`, when
/// the number has at most `scale` fractional digits and the result fits in
/// an `i128`. `None` for anything else, including the infinities, which the
/// caller then converts through [`OracleNumber`].
///
/// The first byte holds the sign and the base-100 exponent; each byte after
/// it is one base-100 digit, stored as `digit + 1` for a positive number and
/// as `101 - digit` (followed by a byte of 102) for a negative one.
fn scaled_number(bytes: &[u8], scale: u32) -> Option<i128> {
    let (&first, rest) = bytes.split_first()?;
    if rest.is_empty() {
        // Zero is a single byte of 0x80; other single bytes are infinities.
        return (first == 0x80).then_some(0);
    }
    let positive = first & 0x80 != 0;
    let (exponent, digits) = if positive {
        (i32::from(first & 0x7f) - 65, rest)
    } else {
        let digits = rest.strip_suffix(&[102]).unwrap_or(rest);
        (i32::from(!first & 0x7f) - 65, digits)
    };
    let scale = i32::try_from(scale).ok()?;
    let mut value: i128 = 0;
    for (i, &byte) in digits.iter().enumerate() {
        let digit = if positive {
            i32::from(byte) - 1
        } else {
            101 - i32::from(byte)
        };
        if !(0..100).contains(&digit) {
            return None;
        }
        // The digit's power of ten once scaled: 2 * (exponent - i) + scale.
        let power = 2 * (exponent - i32::try_from(i).ok()?) + scale;
        let term = match power {
            // The digit's tens fall within the scale, its units beyond it:
            // exact only if the units digit is zero.
            -1 if digit % 10 == 0 => i128::from(digit / 10),
            // A fractional digit beyond the scale: exact only if it is zero.
            ..0 if digit == 0 => 0,
            ..0 => return None,
            _ => i128::from(digit).checked_mul(10_i128.checked_pow(power.try_into().ok()?)?)?,
        };
        value = value.checked_add(term)?;
    }
    Some(if positive { value } else { -value })
}

/// One column's values for the rows not yet returned.
enum Values {
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    I128(Vec<i128>),
    I256(Vec<i256>),
    Bool(Vec<bool>),
    /// Text or binary values: their bytes, one after the other, and where
    /// each ends.
    Bytes {
        data: Vec<u8>,
        ends: Vec<usize>,
    },
}

impl Values {
    fn new(kind: Kind, capacity: usize) -> Self {
        match kind {
            Kind::Int8 | Kind::Int16 | Kind::Int32 | Kind::Int64 | Kind::Timestamp(_) => {
                Self::I64(Vec::with_capacity(capacity))
            }
            Kind::Float32 => Self::F32(Vec::with_capacity(capacity)),
            Kind::Float64 => Self::F64(Vec::with_capacity(capacity)),
            Kind::Decimal128(..) => Self::I128(Vec::with_capacity(capacity)),
            Kind::Decimal256(..) => Self::I256(Vec::with_capacity(capacity)),
            Kind::Boolean => Self::Bool(Vec::with_capacity(capacity)),
            Kind::NumberText | Kind::Utf8 | Kind::LargeUtf8 | Kind::Binary | Kind::LargeBinary => {
                Self::Bytes {
                    data: Vec::new(),
                    ends: Vec::with_capacity(capacity),
                }
            }
        }
    }

    fn push_null(&mut self) {
        match self {
            Self::I64(v) => v.push(0),
            Self::F32(v) => v.push(0.0),
            Self::F64(v) => v.push(0.0),
            Self::I128(v) => v.push(0),
            Self::I256(v) => v.push(i256::ZERO),
            Self::Bool(v) => v.push(false),
            Self::Bytes { data, ends } => ends.push(data.len()),
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        if let Self::Bytes { data, ends } = self {
            data.extend_from_slice(bytes);
            ends.push(data.len());
        }
    }

    /// Appends a copy of the last value.
    fn repeat(&mut self) {
        match self {
            Self::I64(v) => v.push(*v.last().unwrap_or(&0)),
            Self::F32(v) => v.push(*v.last().unwrap_or(&0.0)),
            Self::F64(v) => v.push(*v.last().unwrap_or(&0.0)),
            Self::I128(v) => v.push(*v.last().unwrap_or(&0)),
            Self::I256(v) => v.push(*v.last().unwrap_or(&i256::ZERO)),
            Self::Bool(v) => v.push(*v.last().unwrap_or(&false)),
            Self::Bytes { data, ends } => {
                let end = ends.last().copied().unwrap_or(0);
                let start = ends.len().checked_sub(2).map_or(0, |i| ends[i]);
                data.extend_from_within(start..end);
                ends.push(data.len());
            }
        }
    }

    fn truncate(&mut self, rows: usize) {
        match self {
            Self::I64(v) => v.truncate(rows),
            Self::F32(v) => v.truncate(rows),
            Self::F64(v) => v.truncate(rows),
            Self::I128(v) => v.truncate(rows),
            Self::I256(v) => v.truncate(rows),
            Self::Bool(v) => v.truncate(rows),
            Self::Bytes { data, ends } => {
                ends.truncate(rows);
                data.truncate(ends.last().copied().unwrap_or(0));
            }
        }
    }

    /// Removes the first `rows` values and returns them.
    fn take_front(&mut self, rows: usize) -> Self {
        fn split<T>(v: &mut Vec<T>, rows: usize) -> Vec<T> {
            let rest = v.split_off(rows);
            std::mem::replace(v, rest)
        }
        match self {
            Self::I64(v) => Self::I64(split(v, rows)),
            Self::F32(v) => Self::F32(split(v, rows)),
            Self::F64(v) => Self::F64(split(v, rows)),
            Self::I128(v) => Self::I128(split(v, rows)),
            Self::I256(v) => Self::I256(split(v, rows)),
            Self::Bool(v) => Self::Bool(split(v, rows)),
            Self::Bytes { data, ends } => {
                let cut = if rows == 0 { 0 } else { ends[rows - 1] };
                let front_ends = split(ends, rows);
                for end in ends.iter_mut() {
                    *end -= cut;
                }
                Self::Bytes {
                    data: split(data, cut),
                    ends: front_ends,
                }
            }
        }
    }
}

/// One column of [`Columns`].
struct Column {
    kind: Kind,
    name: String,
    values: Values,
    valid: Vec<bool>,
}

impl Column {
    fn push(&mut self, value: WireValue<'_>) -> Result<()> {
        if matches!(value, WireValue::Null) {
            self.values.push_null();
            self.valid.push(false);
            return Ok(());
        }
        match (&mut self.values, self.kind, value) {
            (
                Values::I64(v),
                Kind::Int8 | Kind::Int16 | Kind::Int32 | Kind::Int64,
                WireValue::Number(bytes),
            ) => {
                let (min, max) = int_range(self.kind);
                match scaled_number(bytes, 0).filter(|n| (min..=max).contains(n)) {
                    Some(n) => v.push(n as i64),
                    None => {
                        return Err(error(
                            format!(
                                "column {:?}: NUMBER value {} does not fit in {:?}",
                                self.name,
                                WireValue::number(bytes),
                                self.kind
                            ),
                            Status::InvalidData,
                        ))
                    }
                }
            }
            (Values::F32(v), _, WireValue::BinaryFloat(f)) => v.push(f),
            (Values::F32(v), _, WireValue::Number(bytes)) => {
                v.push(number_text(bytes).parse().map_err(|e| {
                    error(
                        format!("column {:?}: invalid NUMBER: {e}", self.name),
                        Status::InvalidData,
                    )
                })?)
            }
            (Values::F64(v), _, WireValue::BinaryDouble(f)) => v.push(f),
            (Values::F64(v), _, WireValue::Number(bytes)) => {
                v.push(number_text(bytes).parse().map_err(|e| {
                    error(
                        format!("column {:?}: invalid NUMBER: {e}", self.name),
                        Status::InvalidData,
                    )
                })?)
            }
            (Values::I128(v), Kind::Decimal128(precision, scale), WireValue::Number(bytes)) => {
                let fits = |n: &i128| n.unsigned_abs() < 10_u128.pow(u32::from(precision));
                let value = match u32::try_from(scale)
                    .ok()
                    .and_then(|s| scaled_number(bytes, s))
                    .filter(fits)
                {
                    Some(n) => n,
                    // Not exact at this scale, or too wide: the row reader's
                    // conversion, and its error.
                    None => {
                        convert::number_to_decimal128(&WireValue::number(bytes), precision, scale)
                            .map_err(|e| with_column(e, &self.name))?
                    }
                };
                v.push(value);
            }
            (Values::I256(v), Kind::Decimal256(precision, scale), WireValue::Number(bytes)) => v
                .push(
                    convert::number_to_decimal256(&WireValue::number(bytes), precision, scale)
                        .map_err(|e| with_column(e, &self.name))?,
                ),
            (Values::Bool(v), _, WireValue::Boolean(b)) => v.push(b),
            (values @ Values::Bytes { .. }, Kind::NumberText, WireValue::Number(bytes)) => {
                values.push_bytes(number_text(bytes).as_bytes())
            }
            (
                values @ Values::Bytes { .. },
                Kind::Utf8 | Kind::LargeUtf8,
                WireValue::Text(bytes),
            ) => values.push_bytes(bytes),
            (
                values @ Values::Bytes { .. },
                Kind::Utf8 | Kind::LargeUtf8,
                WireValue::NationalText(bytes),
            ) => {
                let units: Vec<u16> = bytes
                    .chunks_exact(2)
                    .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
                    .collect();
                let text = String::from_utf16(&units).map_err(|e| {
                    error(
                        format!("column {:?}: invalid national text: {e}", self.name),
                        Status::InvalidData,
                    )
                })?;
                values.push_bytes(text.as_bytes())
            }
            (
                values @ Values::Bytes { .. },
                Kind::Binary | Kind::LargeBinary,
                WireValue::Raw(bytes),
            ) => values.push_bytes(bytes),
            (Values::I64(v), Kind::Timestamp(unit), WireValue::Timestamp(bytes)) => v.push(
                convert::timestamp_to_arrow(&WireValue::timestamp(bytes), unit)
                    .map_err(|e| with_column(e, &self.name))?,
            ),
            (_, kind, _) => {
                return Err(error(
                    format!("column {:?}: unexpected value for {kind:?}", self.name),
                    Status::Internal,
                ))
            }
        }
        self.valid.push(true);
        Ok(())
    }

    /// Appends the value of a row read the usual way.
    fn push_row(&mut self, row: &Row, index: usize) -> Result<()> {
        macro_rules! get {
            ($t:ty) => {
                row.get::<Option<$t>>(index).map_err(from_oracle)?
            };
        }
        let valid = match (&mut self.values, self.kind) {
            (Values::I64(v), Kind::Int8 | Kind::Int16 | Kind::Int32 | Kind::Int64) => {
                let (min, max) = int_range(self.kind);
                let value = get!(i64);
                if let Some(n) = value.filter(|n| !(min..=max).contains(&i128::from(*n))) {
                    return Err(error(
                        format!(
                            "column {:?}: NUMBER value {n} does not fit in {:?}",
                            self.name, self.kind
                        ),
                        Status::InvalidData,
                    ));
                }
                push_option(v, value)
            }
            (Values::F32(v), _) => push_option(v, get!(f32)),
            (Values::F64(v), _) => push_option(v, get!(f64)),
            (Values::I128(v), Kind::Decimal128(p, s)) => {
                let value = get!(OracleNumber)
                    .map(|n| convert::number_to_decimal128(&n, p, s))
                    .transpose()
                    .map_err(|e| with_column(e, &self.name))?;
                push_option(v, value)
            }
            (Values::I256(v), Kind::Decimal256(p, s)) => {
                let value = get!(OracleNumber)
                    .map(|n| convert::number_to_decimal256(&n, p, s))
                    .transpose()
                    .map_err(|e| with_column(e, &self.name))?;
                push_option(v, value)
            }
            (Values::Bool(v), _) => push_option(v, get!(bool)),
            (values @ Values::Bytes { .. }, Kind::NumberText) => push_bytes_option(
                values,
                get!(OracleNumber)
                    .map(|n| n.to_string())
                    .as_deref()
                    .map(str::as_bytes),
            ),
            (values @ Values::Bytes { .. }, Kind::Utf8 | Kind::LargeUtf8) => push_bytes_option(
                values,
                row.get::<Option<&str>>(index)
                    .map_err(from_oracle)?
                    .map(str::as_bytes),
            ),
            (values @ Values::Bytes { .. }, Kind::Binary | Kind::LargeBinary) => push_bytes_option(
                values,
                row.get::<Option<&[u8]>>(index).map_err(from_oracle)?,
            ),
            (Values::I64(v), Kind::Timestamp(unit)) => {
                let value = get!(OracleTimestamp)
                    .map(|ts| convert::timestamp_to_arrow(&ts, unit))
                    .transpose()
                    .map_err(|e| with_column(e, &self.name))?;
                push_option(v, value)
            }
            (_, kind) => {
                return Err(error(
                    format!("column {:?}: unexpected {kind:?} column", self.name),
                    Status::Internal,
                ))
            }
        };
        self.valid.push(valid);
        Ok(())
    }

    fn repeat(&mut self) {
        self.values.repeat();
        self.valid.push(self.valid.last().copied().unwrap_or(false));
    }

    fn truncate(&mut self, rows: usize) {
        self.values.truncate(rows);
        self.valid.truncate(rows);
    }

    /// Takes the first `rows` values as an Arrow array of `data_type`.
    fn take_array(&mut self, rows: usize, data_type: &DataType) -> Result<ArrayRef> {
        let values = self.values.take_front(rows);
        let rest = self.valid.split_off(rows);
        let valid = std::mem::replace(&mut self.valid, rest);
        let nulls = valid.contains(&false).then(|| NullBuffer::from(valid));
        let internal = |e: arrow_schema::ArrowError| error(e.to_string(), Status::Internal);
        let array: ArrayRef = match (values, self.kind) {
            (Values::I64(v), Kind::Int64) => Arc::new(Int64Array::new(v.into(), nulls)),
            (Values::I64(v), Kind::Int32) => Arc::new(Int32Array::new(
                v.into_iter().map(|n| n as i32).collect(),
                nulls,
            )),
            (Values::I64(v), Kind::Int16) => Arc::new(Int16Array::new(
                v.into_iter().map(|n| n as i16).collect(),
                nulls,
            )),
            (Values::I64(v), Kind::Int8) => Arc::new(Int8Array::new(
                v.into_iter().map(|n| n as i8).collect(),
                nulls,
            )),
            (Values::I64(v), Kind::Timestamp(_)) => {
                let data = Int64Array::new(v.into(), nulls)
                    .into_data()
                    .into_builder()
                    .data_type(data_type.clone())
                    .build()
                    .map_err(internal)?;
                make_array(data)
            }
            (Values::F32(v), _) => Arc::new(Float32Array::new(v.into(), nulls)),
            (Values::F64(v), _) => Arc::new(Float64Array::new(v.into(), nulls)),
            (Values::I128(v), Kind::Decimal128(p, s)) => Arc::new(
                Decimal128Array::new(ScalarBuffer::from(v), nulls)
                    .with_precision_and_scale(p, s)
                    .map_err(internal)?,
            ),
            (Values::I256(v), Kind::Decimal256(p, s)) => Arc::new(
                Decimal256Array::new(ScalarBuffer::from(v), nulls)
                    .with_precision_and_scale(p, s)
                    .map_err(internal)?,
            ),
            (Values::Bool(v), _) => Arc::new(BooleanArray::new(BooleanBuffer::from(v), nulls)),
            (Values::Bytes { data, ends }, kind) => {
                let data = Buffer::from_vec(data);
                match kind {
                    Kind::NumberText | Kind::Utf8 => {
                        Arc::new(StringArray::try_new(offsets(&ends)?, data, nulls).map_err(
                            |e| error(format!("column {:?}: {e}", self.name), Status::InvalidData),
                        )?)
                    }
                    Kind::LargeUtf8 => Arc::new(
                        LargeStringArray::try_new(offsets(&ends)?, data, nulls).map_err(|e| {
                            error(format!("column {:?}: {e}", self.name), Status::InvalidData)
                        })?,
                    ),
                    Kind::Binary => Arc::new(
                        BinaryArray::try_new(offsets(&ends)?, data, nulls).map_err(internal)?,
                    ),
                    _ => Arc::new(
                        LargeBinaryArray::try_new(offsets(&ends)?, data, nulls)
                            .map_err(internal)?,
                    ),
                }
            }
            (_, kind) => {
                return Err(error(
                    format!("column {:?}: unexpected {kind:?} values", self.name),
                    Status::Internal,
                ))
            }
        };
        Ok(array)
    }
}

/// Offsets from the end of each value; an error when they do not fit `O`.
fn offsets<O: arrow_array::OffsetSizeTrait>(ends: &[usize]) -> Result<OffsetBuffer<O>> {
    let mut out = Vec::with_capacity(ends.len() + 1);
    out.push(O::usize_as(0));
    for &end in ends {
        out.push(O::from_usize(end).ok_or_else(|| {
            error(
                "a batch holds more than 2 GiB of text or binary data; lower oracle.batch_size",
                Status::InvalidData,
            )
        })?);
    }
    Ok(OffsetBuffer::new(ScalarBuffer::from(out)))
}

fn int_range(kind: Kind) -> (i128, i128) {
    match kind {
        Kind::Int8 => (i8::MIN.into(), i8::MAX.into()),
        Kind::Int16 => (i16::MIN.into(), i16::MAX.into()),
        Kind::Int32 => (i32::MIN.into(), i32::MAX.into()),
        _ => (i64::MIN.into(), i64::MAX.into()),
    }
}

/// The exact text of a `NUMBER`, as the row reader writes it.
fn number_text(bytes: &[u8]) -> String {
    WireValue::number(bytes).to_string()
}

fn push_option<T: Default>(values: &mut Vec<T>, value: Option<T>) -> bool {
    let valid = value.is_some();
    values.push(value.unwrap_or_default());
    valid
}

fn push_bytes_option(values: &mut Values, value: Option<&[u8]>) -> bool {
    match value {
        Some(bytes) => {
            values.push_bytes(bytes);
            true
        }
        None => {
            values.push_null();
            false
        }
    }
}

fn with_column(err: adbc_core::error::Error, column: &str) -> adbc_core::error::Error {
    let mut err = err;
    err.message = format!("column {column:?}: {}", err.message);
    err
}

/// The rows of a result not yet returned, column by column.
pub(crate) struct Columns {
    columns: Vec<Column>,
    /// Complete rows.
    rows: usize,
    /// The first error decoding a fetched value. `RowSink` methods return
    /// `oracledb` errors, so a conversion error waits here, and the fetch
    /// returns it once the response is read.
    error: Option<adbc_core::error::Error>,
}

impl Columns {
    /// Columns for `plans`, when every one can be read this way.
    pub(crate) fn for_plans(
        plans: &[ColumnPlan],
        metadata: &[Metadata],
        capacity: usize,
    ) -> Option<Self> {
        if plans.len() != metadata.len() {
            return None;
        }
        let columns = plans
            .iter()
            .zip(metadata)
            .map(|(plan, meta)| {
                let kind = kind(&plan.reader, meta.db_type())?;
                Some(Column {
                    kind,
                    name: plan.field.name().clone(),
                    values: Values::new(kind, capacity),
                    valid: Vec::with_capacity(capacity),
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            columns,
            rows: 0,
            error: None,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.rows
    }

    /// Appends a row read the usual way.
    pub(crate) fn push_row(&mut self, row: &Row) -> Result<()> {
        for (index, column) in self.columns.iter_mut().enumerate() {
            column.push_row(row, index)?;
        }
        self.rows += 1;
        Ok(())
    }

    /// Fetches the cursor's next rows into the columns, decoded as they
    /// arrive, when every column of the cursor can be: otherwise, and while
    /// the cursor holds rows, appends its next row. Returns how many rows
    /// were added; 0 when the cursor has no more.
    pub(crate) fn fetch(&mut self, cursor: &mut Cursor) -> Result<usize> {
        if cursor.buffered_rows() > 0 || !cursor.columns().iter().all(oracledb::row_sink_supports) {
            return match cursor.next() {
                Some(row) => {
                    self.push_row(&row.map_err(from_oracle)?)?;
                    Ok(1)
                }
                None => Ok(0),
            };
        }
        let before = self.rows;
        let fetched = cursor.fetch_into(self);
        if let Some(err) = self.error.take() {
            return Err(err);
        }
        fetched.map_err(from_oracle)?;
        Ok(self.rows - before)
    }

    /// Takes the first `rows` rows as Arrow arrays of the plans' types.
    pub(crate) fn take_batch(
        &mut self,
        rows: usize,
        plans: &[ColumnPlan],
    ) -> Result<Vec<ArrayRef>> {
        let arrays = self
            .columns
            .iter_mut()
            .zip(plans)
            .map(|(column, plan)| column.take_array(rows, plan.field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        self.rows -= rows;
        Ok(arrays)
    }
}

impl RowSink for Columns {
    fn append(
        &mut self,
        column: usize,
        value: WireValue<'_>,
    ) -> std::result::Result<(), oracledb::Error> {
        let column = &mut self.columns[column];
        if self.error.is_none() {
            if let Err(err) = column.push(value) {
                self.error = Some(err);
            } else {
                return Ok(());
            }
        }
        // After an error, the response is still read to its end; values keep
        // their place so that the columns stay aligned.
        column.values.push_null();
        column.valid.push(false);
        Ok(())
    }

    fn repeat(&mut self, column: usize) -> std::result::Result<(), oracledb::Error> {
        self.columns[column].repeat();
        Ok(())
    }

    fn end_row(&mut self) -> std::result::Result<(), oracledb::Error> {
        self.rows += 1;
        Ok(())
    }

    fn discard(&mut self, rows: usize) -> std::result::Result<(), oracledb::Error> {
        self.rows -= rows;
        for column in &mut self.columns {
            column.truncate(self.rows);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Oracle's NUMBER encoding of an integer or decimal string.
    fn encode(text: &str) -> Vec<u8> {
        let number: OracleNumber = text.parse().unwrap();
        let mut buf = Vec::new();
        // OracleNumber's wire form, as the database sends it.
        buf.extend_from_slice(&oracle_bytes(&number));
        buf
    }

    /// The wire bytes of a number, built as the database does: base-100
    /// digits after an exponent byte.
    fn oracle_bytes(number: &OracleNumber) -> Vec<u8> {
        let text = number.to_string();
        let (negative, digits) = match text.strip_prefix('-') {
            Some(rest) => (true, rest.to_string()),
            None => (false, text),
        };
        if digits.chars().all(|c| c == '0' || c == '.') {
            return vec![0x80];
        }
        let (int, frac) = digits.split_once('.').unwrap_or((&digits, ""));
        let int = int.trim_start_matches('0');
        // Pad to whole base-100 digits on both sides of the point.
        let int = if int.len() % 2 == 1 {
            format!("0{int}")
        } else {
            int.to_string()
        };
        let frac = if frac.len() % 2 == 1 {
            format!("{frac}0")
        } else {
            frac.to_string()
        };
        let mut pairs: Vec<u8> = int
            .as_bytes()
            .chunks(2)
            .chain(frac.as_bytes().chunks(2))
            .map(|p| (p[0] - b'0') * 10 + (p[1] - b'0'))
            .collect();
        let mut exponent = (int.len() / 2) as i32 - 1;
        while pairs.first() == Some(&0) {
            pairs.remove(0);
            exponent -= 1;
        }
        while pairs.last() == Some(&0) {
            pairs.pop();
        }
        let mut out = Vec::new();
        if negative {
            out.push(!((exponent + 65) as u8 | 0x80));
            out.extend(pairs.iter().map(|d| 101 - d));
            if out.len() < 21 {
                out.push(102);
            }
        } else {
            out.push((exponent + 65) as u8 | 0x80);
            out.extend(pairs.iter().map(|d| d + 1));
        }
        out
    }

    #[test]
    fn encoding_matches_oracledb() {
        for text in [
            "1",
            "-1",
            "100",
            "123.45",
            "-0.001",
            "99999999999999999999",
            "0.5",
        ] {
            let bytes = encode(text);
            assert_eq!(
                WireValue::number(&bytes).to_string(),
                text,
                "{text}: {bytes:?}"
            );
        }
    }

    #[test]
    fn scaled_number_decodes_exactly() {
        let cases: &[(&str, u32, Option<i128>)] = &[
            ("0", 0, Some(0)),
            ("0", 4, Some(0)),
            ("1", 0, Some(1)),
            ("-1", 0, Some(-1)),
            ("100", 0, Some(100)),
            ("1000000", 0, Some(1_000_000)),
            ("-987654321", 0, Some(-987_654_321)),
            ("123.45", 2, Some(12345)),
            ("123.45", 4, Some(1_234_500)),
            ("123.45", 1, None),
            ("-0.001", 3, Some(-1)),
            ("-0.001", 2, None),
            ("0.1", 1, Some(1)),
            ("0.1", 0, None),
            ("12.3", 1, Some(123)),
            ("-12.34", 3, Some(-12340)),
            ("0.5", 0, None),
            ("99999999999999999999", 0, Some(99_999_999_999_999_999_999)),
            (
                "12345678901234567890123456789012345678",
                0,
                Some(12345678901234567890123456789012345678),
            ),
        ];
        for &(text, scale, expected) in cases {
            assert_eq!(
                scaled_number(&encode(text), scale),
                expected,
                "{text} at scale {scale}"
            );
        }
    }

    #[test]
    fn scaled_number_leaves_infinities_and_overflow_to_oracle_number() {
        assert_eq!(scaled_number(&[0xff, 0x65], 0), None);
        assert_eq!(scaled_number(&[0x00], 0), None);
        // 39 digits: more than an i128 holds.
        let wide = "999999999999999999999999999999999999999";
        assert_eq!(scaled_number(&encode(wide), 0), None);
    }

    #[test]
    fn bytes_repeat_truncate_and_take_front() {
        let mut values = Values::new(Kind::Utf8, 4);
        values.push_bytes(b"ab");
        values.push_null();
        values.push_bytes(b"cde");
        values.repeat();
        values.truncate(3);
        values.push_bytes(b"f");
        let Values::Bytes { data, ends } = values.take_front(2) else {
            panic!()
        };
        assert_eq!(
            (data.as_slice(), ends.as_slice()),
            (&b"ab"[..], &[2, 2][..])
        );
        let Values::Bytes { data, ends } = values else {
            panic!()
        };
        assert_eq!(
            (data.as_slice(), ends.as_slice()),
            (&b"cdef"[..], &[3, 4][..])
        );
    }
}
