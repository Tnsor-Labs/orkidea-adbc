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

//! Streaming Oracle → Arrow result reader.
//!
//! Rows are pulled from `oracledb` row sources: query cursors (which fetch
//! `fetch_array_size` rows per round trip) and appended to per-column Arrow
//! builders; a `RecordBatch` is emitted every `batch_size` rows. Memory use is
//! bounded by one batch regardless of the size of the result set.

use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Decimal128Builder, Decimal256Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, LargeBinaryBuilder,
    LargeStringBuilder, StringBuilder,
};
use arrow_array::{
    make_array, Array, ArrayRef, FixedSizeListArray, ListArray, RecordBatch, RecordBatchOptions,
    RecordBatchReader,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};
use oracledb::{
    Cursor, JsonValue, OracleIntervalDS, OracleIntervalYM, OracleNumber, OracleTimestamp, Row,
    Vector, VectorData,
};

use crate::convert;
use crate::error::{error, from_oracle, Result};
use crate::types::{ColumnPlan, Reader, VectorElement, VectorKind};

/// Accumulates values of one `VECTOR` column.
struct VectorBuilder {
    kind: VectorKind,
    f32s: Vec<f32>,
    f64s: Vec<f64>,
    i8s: Vec<i8>,
    u8s: Vec<u8>,
    offsets: Vec<i32>,
    validity: Vec<bool>,
}

impl VectorBuilder {
    fn new(kind: VectorKind) -> Self {
        Self {
            kind,
            f32s: Vec::new(),
            f64s: Vec::new(),
            i8s: Vec::new(),
            u8s: Vec::new(),
            offsets: vec![0],
            validity: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        match self.kind.element {
            VectorElement::Float32 => self.f32s.len(),
            VectorElement::Float64 => self.f64s.len(),
            VectorElement::Int8 => self.i8s.len(),
            VectorElement::Binary => self.u8s.len(),
        }
    }

    fn append(&mut self, value: Option<Vector>, column: &str) -> Result<()> {
        let before = self.len();
        match value {
            None => {
                self.validity.push(false);
                if let Some(n) = self.kind.dimensions {
                    // Fixed-size lists reserve child slots for null entries.
                    for _ in 0..n {
                        self.push_default();
                    }
                }
            }
            Some(Vector::Sparse(_)) => {
                return Err(error(
                    format!("column {column:?} returned a sparse vector"),
                    Status::NotImplemented,
                ))
            }
            Some(Vector::Dense(data)) => {
                self.validity.push(true);
                match (self.kind.element, data) {
                    (VectorElement::Float32, VectorData::Float32(v)) => self.f32s.extend(v),
                    (VectorElement::Float64, VectorData::Float64(v)) => self.f64s.extend(v),
                    (VectorElement::Float64, VectorData::Float32(v)) => {
                        self.f64s.extend(v.into_iter().map(f64::from))
                    }
                    (VectorElement::Float64, VectorData::Int8(v)) => {
                        self.f64s.extend(v.into_iter().map(f64::from))
                    }
                    (VectorElement::Int8, VectorData::Int8(v)) => self.i8s.extend(v),
                    (VectorElement::Binary, VectorData::Binary(v)) => self.u8s.extend(v),
                    (_, other) => {
                        return Err(error(
                            format!(
                                "column {column:?} returned a {} vector where {:?} was expected",
                                vector_data_name(&other),
                                self.kind.element
                            ),
                            Status::InvalidData,
                        ))
                    }
                }
                if let Some(n) = self.kind.dimensions {
                    let got = self.len() - before;
                    if got != n as usize {
                        return Err(error(
                            format!(
                                "column {column:?} returned a vector of length {got}, expected {n}"
                            ),
                            Status::InvalidData,
                        ));
                    }
                }
            }
        }
        self.offsets.push(self.len() as i32);
        Ok(())
    }

    fn push_default(&mut self) {
        match self.kind.element {
            VectorElement::Float32 => self.f32s.push(0.0),
            VectorElement::Float64 => self.f64s.push(0.0),
            VectorElement::Int8 => self.i8s.push(0),
            VectorElement::Binary => self.u8s.push(0),
        }
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        let values: ArrayRef = match self.kind.element {
            VectorElement::Float32 => Arc::new(arrow_array::Float32Array::from(std::mem::take(
                &mut self.f32s,
            ))),
            VectorElement::Float64 => Arc::new(arrow_array::Float64Array::from(std::mem::take(
                &mut self.f64s,
            ))),
            VectorElement::Int8 => {
                Arc::new(arrow_array::Int8Array::from(std::mem::take(&mut self.i8s)))
            }
            VectorElement::Binary => {
                Arc::new(arrow_array::UInt8Array::from(std::mem::take(&mut self.u8s)))
            }
        };
        let nulls = Some(NullBuffer::from(std::mem::take(&mut self.validity)));
        let offsets = std::mem::replace(&mut self.offsets, vec![0]);
        let field = self.kind.item_field();
        let array: ArrayRef = match self.kind.dimensions {
            Some(n) => Arc::new(
                FixedSizeListArray::try_new(field, n, values, nulls)
                    .map_err(|e| error(e.to_string(), Status::Internal))?,
            ),
            None => Arc::new(
                ListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    values,
                    nulls,
                )
                .map_err(|e| error(e.to_string(), Status::Internal))?,
            ),
        };
        Ok(array)
    }
}

fn vector_data_name(data: &VectorData) -> &'static str {
    match data {
        VectorData::Float32(_) => "FLOAT32",
        VectorData::Float64(_) => "FLOAT64",
        VectorData::Int8(_) => "INT8",
        VectorData::Binary(_) => "BINARY",
    }
}

/// An Arrow builder for one planned column.
enum ColumnBuilder {
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal128(Decimal128Builder, u8, i8),
    Decimal256(Decimal256Builder, u8, i8),
    Text(StringBuilder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    RowId(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Binary(BinaryBuilder),
    LargeBinary(LargeBinaryBuilder),
    /// Timestamp and duration values are accumulated as i64 and retyped on
    /// finish (identical physical layout).
    Timestamp(Int64Builder, TimeUnit),
    Duration(Int64Builder, TimeUnit),
    IntervalYearToMonth(Int32Builder),
    Json(StringBuilder),
    Vector(VectorBuilder),
}

impl ColumnBuilder {
    fn new(reader: &Reader, capacity: usize) -> Result<Self> {
        Ok(match reader {
            Reader::Int8 => Self::Int8(Int8Builder::with_capacity(capacity)),
            Reader::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            Reader::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            Reader::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            Reader::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            Reader::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            Reader::Decimal128 { precision, scale } => Self::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| error(e.to_string(), Status::Internal))?,
                *precision,
                *scale,
            ),
            Reader::Decimal256 { precision, scale } => Self::Decimal256(
                Decimal256Builder::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| error(e.to_string(), Status::Internal))?,
                *precision,
                *scale,
            ),
            Reader::Text => Self::Text(StringBuilder::new()),
            Reader::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            Reader::Utf8 => Self::Utf8(StringBuilder::new()),
            Reader::RowId => Self::RowId(StringBuilder::new()),
            Reader::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::new()),
            Reader::Binary => Self::Binary(BinaryBuilder::new()),
            Reader::LargeBinary => Self::LargeBinary(LargeBinaryBuilder::new()),
            Reader::Timestamp(unit) => {
                Self::Timestamp(Int64Builder::with_capacity(capacity), *unit)
            }
            Reader::IntervalDayToSecond(unit) => {
                Self::Duration(Int64Builder::with_capacity(capacity), *unit)
            }
            Reader::IntervalYearToMonth => {
                Self::IntervalYearToMonth(Int32Builder::with_capacity(capacity))
            }
            Reader::Json => Self::Json(StringBuilder::new()),
            Reader::Vector(kind) => Self::Vector(VectorBuilder::new(*kind)),
        })
    }

    fn append(&mut self, row: &Row, index: usize, column: &str) -> Result<()> {
        macro_rules! get {
            ($t:ty) => {
                row.get::<Option<$t>>(index).map_err(from_oracle)?
            };
        }
        match self {
            Self::Int8(b) => b.append_option(get!(i8)),
            Self::Int16(b) => b.append_option(get!(i16)),
            Self::Int32(b) => b.append_option(get!(i32)),
            Self::Int64(b) => b.append_option(get!(i64)),
            Self::Float32(b) => b.append_option(get!(f32)),
            Self::Float64(b) => b.append_option(get!(f64)),
            Self::Decimal128(b, p, s) => {
                let value = get!(OracleNumber)
                    .map(|n| convert::number_to_decimal128(&n, *p, *s))
                    .transpose()
                    .map_err(|e| with_column(e, column))?;
                b.append_option(value)
            }
            Self::Decimal256(b, p, s) => {
                let value = get!(OracleNumber)
                    .map(|n| convert::number_to_decimal256(&n, *p, *s))
                    .transpose()
                    .map_err(|e| with_column(e, column))?;
                b.append_option(value)
            }
            Self::Text(b) => b.append_option(get!(OracleNumber).map(|n| n.to_string())),
            Self::Boolean(b) => b.append_option(get!(bool)),
            Self::Utf8(b) => b.append_option(row.get::<Option<&str>>(index).map_err(from_oracle)?),
            Self::RowId(b) => b.append_option(get!(String)),
            Self::LargeUtf8(b) => {
                b.append_option(row.get::<Option<&str>>(index).map_err(from_oracle)?)
            }
            Self::Binary(b) => {
                b.append_option(row.get::<Option<&[u8]>>(index).map_err(from_oracle)?)
            }
            Self::LargeBinary(b) => {
                b.append_option(row.get::<Option<&[u8]>>(index).map_err(from_oracle)?)
            }
            Self::Timestamp(b, unit) => {
                let value = get!(OracleTimestamp)
                    .map(|ts| convert::timestamp_to_arrow(&ts, *unit))
                    .transpose()
                    .map_err(|e| with_column(e, column))?;
                b.append_option(value)
            }
            Self::Duration(b, unit) => {
                let value = get!(OracleIntervalDS)
                    .map(|v| convert::interval_ds_to_arrow(&v, *unit))
                    .transpose()
                    .map_err(|e| with_column(e, column))?;
                b.append_option(value)
            }
            Self::IntervalYearToMonth(b) => {
                let value = get!(OracleIntervalYM)
                    .map(|v| convert::interval_ym_to_months(&v))
                    .transpose()
                    .map_err(|e| with_column(e, column))?;
                b.append_option(value)
            }
            Self::Json(b) => b.append_option(get!(JsonValue).map(|v| convert::json_to_string(&v))),
            Self::Vector(b) => b.append(get!(Vector), column)?,
        }
        Ok(())
    }

    fn finish(&mut self, data_type: &DataType) -> Result<ArrayRef> {
        fn retype(array: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
            let data = array
                .into_data()
                .into_builder()
                .data_type(data_type.clone())
                .build()
                .map_err(|e| error(e.to_string(), Status::Internal))?;
            Ok(make_array(data))
        }
        Ok(match self {
            Self::Int8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Decimal128(b, _, _) => Arc::new(b.finish()),
            Self::Decimal256(b, _, _) => Arc::new(b.finish()),
            Self::Text(b) | Self::Utf8(b) | Self::RowId(b) | Self::Json(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::LargeUtf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::LargeBinary(b) => Arc::new(b.finish()),
            Self::Timestamp(b, _) | Self::Duration(b, _) => {
                retype(Arc::new(b.finish()), data_type)?
            }
            Self::IntervalYearToMonth(b) => retype(Arc::new(b.finish()), data_type)?,
            Self::Vector(b) => b.finish()?,
        })
    }
}

fn with_column(err: adbc_core::error::Error, column: &str) -> adbc_core::error::Error {
    let mut err = err;
    err.message = format!("column {column:?}: {}", err.message);
    err
}

/// A stream of result rows: a query cursor, a REF CURSOR returned by PL/SQL,
/// or rows already returned by an execution (OUT binds, DML RETURNING).
pub(crate) type Rows = Box<dyn Iterator<Item = std::result::Result<Row, oracledb::Error>> + Send>;

pub(crate) fn cursor_rows(cursor: Cursor) -> Rows {
    Box::new(cursor)
}

pub(crate) fn fetched_rows(rows: Vec<Row>) -> Rows {
    Box::new(rows.into_iter().map(Ok))
}

/// Supplies further row streams once the current one is exhausted: one per
/// execution when a multi-row parameter batch is bound.
pub(crate) trait RowSource: Send {
    /// Returns the next stream, or `None` when all executions are done.
    fn next_rows(&mut self) -> Result<Option<Rows>>;
}

/// A fixed sequence of row streams.
pub(crate) struct QueuedRows(pub(crate) std::collections::VecDeque<Rows>);

impl RowSource for QueuedRows {
    fn next_rows(&mut self) -> Result<Option<Rows>> {
        Ok(self.0.pop_front())
    }
}

/// Streams a query result as Arrow record batches.
pub(crate) struct OracleBatchReader {
    schema: SchemaRef,
    plans: Vec<ColumnPlan>,
    builders: Vec<ColumnBuilder>,
    batch_size: usize,
    rows: Option<Rows>,
    source: Box<dyn RowSource>,
    finished: bool,
}

impl OracleBatchReader {
    pub(crate) fn new(
        schema: SchemaRef,
        plans: Vec<ColumnPlan>,
        first: Option<Rows>,
        source: Box<dyn RowSource>,
        batch_size: usize,
    ) -> Result<Self> {
        let batch_size = batch_size.max(1);
        let builders = plans
            .iter()
            .map(|p| ColumnBuilder::new(&p.reader, batch_size))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            plans,
            builders,
            batch_size,
            finished: first.is_none(),
            rows: first,
            source,
        })
    }

    /// A reader with no rows.
    pub(crate) fn empty(schema: SchemaRef, plans: Vec<ColumnPlan>) -> Result<Self> {
        Self::new(schema, plans, None, Box::new(NoMoreRows), 1)
    }

    fn next_row(&mut self) -> Result<Option<Row>> {
        loop {
            let Some(rows) = self.rows.as_mut() else {
                return Ok(None);
            };
            match rows.next() {
                Some(row) => return row.map(Some).map_err(from_oracle),
                None => self.rows = self.source.next_rows()?,
            }
        }
    }

    fn fill_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut rows = 0;
        while rows < self.batch_size {
            let Some(row) = self.next_row()? else {
                self.finished = true;
                break;
            };
            for (index, (builder, plan)) in self.builders.iter_mut().zip(&self.plans).enumerate() {
                builder.append(&row, index, plan.field.name())?;
            }
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        let columns = self
            .builders
            .iter_mut()
            .zip(&self.plans)
            .map(|(b, p)| b.finish(p.field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)
            .map(Some)
            .map_err(|e| error(e.to_string(), Status::Internal))
    }
}

impl Iterator for OracleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.fill_batch() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => {
                // Stop after an error; release the cursor.
                self.finished = true;
                self.rows = None;
                Some(Err(ArrowError::ExternalError(Box::new(e))))
            }
        }
    }
}

impl RecordBatchReader for OracleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// A source that yields no further rows.
pub(crate) struct NoMoreRows;

impl RowSource for NoMoreRows {
    fn next_rows(&mut self) -> Result<Option<Rows>> {
        Ok(None)
    }
}
