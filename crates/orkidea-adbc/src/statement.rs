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

use std::str::FromStr;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use oracledb::{BindParameters, Cursor, ToDbValue};

use crate::config::{
    validate_number_scale, OPT_BATCH_SIZE, OPT_FETCH_ARRAY_SIZE, OPT_PREFETCH_ROWS,
    OPT_UNCONSTRAINED_NUMBER, OPT_UNCONSTRAINED_NUMBER_SCALE,
};
use crate::connection::ConnState;
use crate::error::{error, expect_bool, expect_int, expect_string, from_oracle};
use crate::ingest;
use crate::params::param_row;
use crate::reader::{CursorSource, NoMoreCursors, OracleBatchReader};
use crate::types::{plan_columns, schema_of, TypeOptions, UnconstrainedNumber};

pub(crate) const DEFAULT_BATCH_SIZE: usize = 8192;

/// Round-trip tuning applied to each Oracle cursor.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct FetchTuning {
    /// Rows fetched per round trip (`None`: oracledb default, 100).
    pub(crate) fetch_array_size: Option<u32>,
    /// Rows returned with the execute response (`None`: oracledb default, 2).
    pub(crate) prefetch_rows: Option<u32>,
}

/// Opens a cursor for `sql`, applying the fetch tuning.
pub(crate) fn open_cursor(
    state: &ConnState,
    sql: &str,
    params: &[&dyn ToDbValue],
    tuning: FetchTuning,
) -> Result<Cursor> {
    let conn = state.lock()?;
    let mut builder = conn.statement(sql).map_err(from_oracle)?;
    if let Some(n) = tuning.fetch_array_size {
        builder = builder.fetch_array_size(n);
    }
    if let Some(n) = tuning.prefetch_rows {
        builder = builder.prefetch_rows(n);
    }
    builder
        .build()
        .map_err(from_oracle)?
        .query(params)
        .map_err(from_oracle)
}

/// Runs array DML for every bound batch, in chunks, under a savepoint so a
/// failure leaves no partial effects. Returns the total rows affected.
fn execute_batches(
    conn: &oracledb::Connection,
    sql: &str,
    batches: BatchIter,
    chunk_rows: usize,
) -> Result<u64> {
    const SAVEPOINT: &str = "ORKIDEA_DML";
    let chunk_rows = chunk_rows.max(1);
    conn.execute(&format!("SAVEPOINT {SAVEPOINT}"), &[])
        .map_err(from_oracle)?;
    let result = (|| {
        let mut affected = 0u64;
        let mut offset_total = 0usize;
        for batch in batches {
            let batch = batch?;
            let rows = batch.num_rows();
            let mut offset = 0;
            while offset < rows {
                let len = chunk_rows.min(rows - offset);
                let result = conn
                    .execute_batch(sql, BindParameters::Arrow(batch.slice(offset, len)))
                    .map_err(|e| {
                        let mut err = from_oracle(e);
                        err.message = format!(
                            "executing parameter rows {}..{} failed: {}",
                            offset_total + offset,
                            offset_total + offset + len,
                            err.message
                        );
                        err
                    })?;
                affected += result.rows_affected();
                offset += len;
            }
            offset_total += rows;
        }
        Ok(affected)
    })();
    if result.is_err() {
        let _ = conn.execute(&format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}"), &[]);
    }
    result
}

/// Executes a query once per row of a bound parameter batch.
struct ParamCursors {
    state: Arc<ConnState>,
    sql: String,
    params: RecordBatch,
    next_row: usize,
    tuning: FetchTuning,
}

impl CursorSource for ParamCursors {
    fn next_cursor(&mut self) -> Result<Option<Cursor>> {
        if self.next_row >= self.params.num_rows() {
            return Ok(None);
        }
        let row = param_row(&self.params, self.next_row)?;
        self.next_row += 1;
        open_cursor(&self.state, &self.sql, &row.as_refs(), self.tuning).map(Some)
    }
}

/// Data bound to a statement: one batch (reusable) or a stream (consumed by
/// the next execution).
pub(crate) enum Bound {
    Batch(RecordBatch),
    Stream(Box<dyn RecordBatchReader + Send>),
}

impl Bound {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Batch(batch) => batch.schema(),
            Self::Stream(reader) => reader.schema(),
        }
    }
}

type BatchIter = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

/// An ADBC statement bound to one Oracle session.
pub struct OracleStatement {
    pub(crate) state: Arc<ConnState>,
    pub(crate) sql: Option<String>,
    pub(crate) bound: Option<Bound>,
    pub(crate) prepared: bool,
    pub(crate) bind_names: Vec<String>,
    pub(crate) ingest_mode: Option<IngestMode>,
    pub(crate) target_table: Option<String>,
    pub(crate) target_catalog: Option<String>,
    pub(crate) target_schema: Option<String>,
    pub(crate) temporary: bool,
    pub(crate) batch_size: usize,
    pub(crate) tuning: FetchTuning,
    pub(crate) type_options: TypeOptions,
}

fn positive_u32(defaults: &crate::config::DbOptions, key: &str, allow_zero: bool) -> Option<u32> {
    defaults
        .get_i64(key)
        .ok()
        .flatten()
        .filter(|v| if allow_zero { *v >= 0 } else { *v > 0 })
        .and_then(|v| u32::try_from(v).ok())
}

impl OracleStatement {
    pub(crate) fn new(state: Arc<ConnState>) -> Result<Self> {
        let defaults = &state.defaults;
        let batch_size = defaults
            .get_i64(OPT_BATCH_SIZE)?
            .filter(|v| *v > 0)
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_BATCH_SIZE);
        let tuning = FetchTuning {
            fetch_array_size: positive_u32(defaults, OPT_FETCH_ARRAY_SIZE, false),
            prefetch_rows: positive_u32(defaults, OPT_PREFETCH_ROWS, true),
        };
        let type_options = defaults.type_options()?;
        Ok(Self {
            state,
            sql: None,
            bound: None,
            prepared: false,
            bind_names: Vec::new(),
            ingest_mode: None,
            target_table: None,
            target_catalog: None,
            target_schema: None,
            temporary: false,
            batch_size,
            tuning,
            type_options,
        })
    }

    fn clear_ingest(&mut self) {
        self.ingest_mode = None;
        self.target_table = None;
        self.target_catalog = None;
        self.target_schema = None;
        self.temporary = false;
    }

    fn sql(&self) -> Result<String> {
        self.sql.clone().ok_or_else(|| {
            error(
                "no SQL query or ingest target has been set on this statement",
                Status::InvalidState,
            )
        })
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.state.read_only() {
            return Err(error(
                "connection is read-only; writes are not permitted",
                Status::Unauthorized,
            ));
        }
        Ok(())
    }

    /// The bound data as a sequence of batches. A bound batch stays bound
    /// for the next execution; a bound stream is consumed.
    fn bound_batches(&mut self) -> Option<(SchemaRef, BatchIter)> {
        match self.bound.take()? {
            Bound::Batch(batch) => {
                self.bound = Some(Bound::Batch(batch.clone()));
                Some((batch.schema(), Box::new(std::iter::once(Ok(batch)))))
            }
            Bound::Stream(reader) => {
                let schema = reader.schema();
                let iter = reader
                    .map(|b| b.map_err(|e| error(format!("bound stream failed: {e}"), Status::IO)));
                Some((schema, Box::new(iter)))
            }
        }
    }

    /// The bound parameters as a single batch, for queries (which run once
    /// per parameter row). A bound stream is collected and kept.
    fn bound_params(&mut self) -> Result<Option<RecordBatch>> {
        match self.bound.take() {
            None => Ok(None),
            Some(Bound::Batch(batch)) => {
                self.bound = Some(Bound::Batch(batch.clone()));
                Ok(Some(batch))
            }
            Some(Bound::Stream(reader)) => {
                let schema = reader.schema();
                let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
                let batch =
                    arrow_select::concat::concat_batches(&schema, &batches).map_err(|e| {
                        error(
                            format!("failed to concatenate bound parameters: {e}"),
                            Status::InvalidData,
                        )
                    })?;
                self.bound = Some(Bound::Batch(batch.clone()));
                Ok(Some(batch))
            }
        }
    }

    /// Classifies the statement (query, DML, ...) with oracledb's client-side
    /// SQL parser. No round trip to the server.
    fn is_query(&self, sql: &str) -> Result<bool> {
        let conn = self.state.lock()?;
        let stmt = conn
            .statement(sql)
            .map_err(from_oracle)?
            .build()
            .map_err(from_oracle)?;
        Ok(stmt.is_query())
    }

    /// Parses and describes the statement on the server without executing it.
    fn describe(&self, sql: &str) -> Result<oracledb::Statement> {
        let conn = self.state.lock()?;
        let mut stmt = conn
            .statement(sql)
            .map_err(from_oracle)?
            .build()
            .map_err(from_oracle)?;
        if stmt.is_query() {
            stmt.ensure_fully_parsed().map_err(from_oracle)?;
        }
        Ok(stmt)
    }

    fn execute_query(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let sql = self.sql()?;
        if !self.is_query(&sql)? {
            // ADBC allows execute() on any statement; non-queries yield an
            // empty result with an empty schema.
            self.execute_non_query(&sql)?;
            let schema = Arc::new(Schema::empty());
            return Ok(Box::new(OracleBatchReader::new(
                schema,
                Vec::new(),
                None,
                Box::new(NoMoreCursors),
                self.batch_size,
            )?));
        }

        let params = self.bound_params()?;
        let (first, source): (Option<Cursor>, Box<dyn CursorSource>) = match params {
            None => (
                Some(open_cursor(&self.state, &sql, &[], self.tuning)?),
                Box::new(NoMoreCursors),
            ),
            Some(params) => {
                let mut source = ParamCursors {
                    state: self.state.clone(),
                    sql: sql.clone(),
                    params,
                    next_row: 0,
                    tuning: self.tuning,
                };
                (source.next_cursor()?, Box::new(source))
            }
        };
        let plans = match &first {
            Some(cursor) => plan_columns(cursor.columns(), &self.type_options)?,
            // Zero parameter rows: nothing executes, but the schema is known.
            None => plan_columns(self.describe(&sql)?.out_metadata(), &self.type_options)?,
        };
        let schema = schema_of(&plans);
        Ok(Box::new(OracleBatchReader::new(
            schema,
            plans,
            first,
            source,
            self.batch_size,
        )?))
    }

    fn execute_non_query(&mut self, sql: &str) -> Result<i64> {
        self.ensure_writable()?;
        let bound = self.bound_batches();
        let conn = self.state.lock()?;
        let affected = match bound {
            None => conn
                .execute(sql, &[] as &[&dyn ToDbValue])
                .map_err(from_oracle)?
                .rows_affected(),
            Some((_, batches)) => execute_batches(&conn, sql, batches, self.batch_size)?,
        };
        self.state.commit_if_autocommit(&conn)?;
        Ok(affected.min(i64::MAX as u64) as i64)
    }

    fn execute_ingest(&mut self) -> Result<i64> {
        self.ensure_writable()?;
        let mode = self
            .ingest_mode
            .ok_or_else(|| error("ingest mode was not set", Status::InvalidState))?;
        let target = self.target_table.clone().ok_or_else(|| {
            error(
                "ingest requires the `adbc.ingest.target_table` option",
                Status::InvalidState,
            )
        })?;
        if self.target_catalog.is_some() {
            return Err(error(
                "Oracle does not support catalogs; use adbc.ingest.target_db_schema",
                Status::NotImplemented,
            ));
        }
        let (schema, mut batches) = self.bound_batches().ok_or_else(|| {
            error(
                "ingest requires bound data (bind or bind_stream before execute_update)",
                Status::InvalidState,
            )
        })?;
        let request = ingest::IngestRequest {
            mode,
            target_table: &target,
            target_schema: self.target_schema.as_deref(),
            temporary: self.temporary,
            chunk_rows: self.batch_size,
        };
        ingest::ingest(&self.state, &request, schema, &mut batches)
    }
}

impl Optionable for OracleStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let key = key.as_ref();
        match key {
            adbc_core::constants::ADBC_INGEST_OPTION_MODE => {
                let mode = IngestMode::from_str(&expect_string(&value, key)?)?;
                self.ingest_mode = Some(mode);
                Ok(())
            }
            adbc_core::constants::ADBC_INGEST_OPTION_TARGET_TABLE => {
                // Choosing an ingest target replaces any SQL query.
                self.target_table = Some(expect_string(&value, key)?);
                self.sql = None;
                self.prepared = false;
                self.bind_names.clear();
                Ok(())
            }
            adbc_core::constants::ADBC_INGEST_OPTION_TARGET_CATALOG => {
                self.target_catalog = Some(expect_string(&value, key)?);
                Ok(())
            }
            adbc_core::constants::ADBC_INGEST_OPTION_TARGET_DB_SCHEMA => {
                self.target_schema = Some(expect_string(&value, key)?);
                Ok(())
            }
            adbc_core::constants::ADBC_INGEST_OPTION_TEMPORARY => {
                self.temporary = expect_bool(&value, key)?;
                Ok(())
            }
            OPT_BATCH_SIZE => {
                let v = expect_int(&value, key)?;
                if v <= 0 {
                    return Err(error(
                        "oracle.batch_size must be positive",
                        Status::InvalidArguments,
                    ));
                }
                self.batch_size = v as usize;
                Ok(())
            }
            OPT_FETCH_ARRAY_SIZE => {
                let v = expect_int(&value, key)?;
                self.tuning.fetch_array_size =
                    Some(u32::try_from(v).ok().filter(|v| *v > 0).ok_or_else(|| {
                        error(
                            "oracle.fetch_array_size must be a positive 32-bit integer",
                            Status::InvalidArguments,
                        )
                    })?);
                Ok(())
            }
            OPT_PREFETCH_ROWS => {
                let v = expect_int(&value, key)?;
                self.tuning.prefetch_rows = Some(u32::try_from(v).map_err(|_| {
                    error(
                        "oracle.prefetch_rows must be a non-negative 32-bit integer",
                        Status::InvalidArguments,
                    )
                })?);
                Ok(())
            }
            OPT_UNCONSTRAINED_NUMBER => {
                self.type_options.unconstrained_number =
                    expect_string(&value, key)?.parse::<UnconstrainedNumber>()?;
                Ok(())
            }
            OPT_UNCONSTRAINED_NUMBER_SCALE => {
                self.type_options.unconstrained_number_scale =
                    validate_number_scale(expect_int(&value, key)?)?;
                Ok(())
            }
            _ => Err(error(
                format!("unknown Orkidea statement option {key:?}"),
                Status::InvalidArguments,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key = key.as_ref();
        match key {
            adbc_core::constants::ADBC_INGEST_OPTION_MODE => self
                .ingest_mode
                .map(String::from)
                .ok_or_else(|| error("ingest mode was not set", Status::NotFound)),
            adbc_core::constants::ADBC_INGEST_OPTION_TARGET_TABLE => self
                .target_table
                .clone()
                .ok_or_else(|| error("target table was not set", Status::NotFound)),
            OPT_UNCONSTRAINED_NUMBER => Ok(self.type_options.unconstrained_number.as_str().into()),
            _ => Err(error(
                format!("unknown Orkidea string option {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(error(
            format!("option {:?} is not a bytes option", key.as_ref()),
            Status::InvalidArguments,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key = key.as_ref();
        match key {
            OPT_BATCH_SIZE => Ok(self.batch_size as i64),
            OPT_FETCH_ARRAY_SIZE => {
                self.tuning.fetch_array_size.map(i64::from).ok_or_else(|| {
                    error("oracle.fetch_array_size has not been set", Status::NotFound)
                })
            }
            OPT_PREFETCH_ROWS => self
                .tuning
                .prefetch_rows
                .map(i64::from)
                .ok_or_else(|| error("oracle.prefetch_rows has not been set", Status::NotFound)),
            OPT_UNCONSTRAINED_NUMBER_SCALE => {
                Ok(i64::from(self.type_options.unconstrained_number_scale))
            }
            _ => Err(error(
                format!("unknown Orkidea integer option {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(error(
            format!("option {:?} is not a float option", key.as_ref()),
            Status::InvalidArguments,
        ))
    }
}

impl Statement for OracleStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = Some(Bound::Batch(batch));
        Ok(())
    }

    /// Binds a stream without reading it; ingest and DML consume it batch
    /// by batch at execution time.
    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.bound = Some(Bound::Stream(reader));
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        if self.ingest_mode.is_some() {
            return Err(error(
                "ingest statements must be executed with execute_update",
                Status::InvalidState,
            ));
        }
        self.execute_query()
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.ingest_mode.is_some() {
            return Ok(Some(self.execute_ingest()?));
        }
        let sql = self.sql()?;
        self.execute_non_query(&sql).map(Some)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        if self.ingest_mode.is_some() {
            return Err(error(
                "ingest statements do not produce a result schema",
                Status::NotImplemented,
            ));
        }
        let sql = self.sql()?;
        let stmt = self.describe(&sql)?;
        if !stmt.is_query() {
            return Ok(Schema::empty());
        }
        let plans = plan_columns(stmt.out_metadata(), &self.type_options)?;
        Ok((*schema_of(&plans)).clone())
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(error(
            "Orkidea does not support partitioned execution",
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        // Oracle does not describe bind types, so prepared parameters are
        // reported by name with type Null, as ADBC specifies for unknown
        // types. Without a prepared statement, the bound data's schema is
        // the best available answer.
        if self.bind_names.is_empty() {
            return Ok(match &self.bound {
                Some(bound) => (*bound.schema()).clone(),
                None => Schema::empty(),
            });
        }
        let fields: Vec<Field> = self
            .bind_names
            .iter()
            .map(|name| Field::new(name, DataType::Null, true))
            .collect();
        Ok(Schema::new(fields))
    }

    fn prepare(&mut self) -> Result<()> {
        let sql = self.sql()?;
        let conn = self.state.lock()?;
        let mut stmt = conn
            .statement(&sql)
            .map_err(from_oracle)?
            .build()
            .map_err(from_oracle)?;
        // Oracle executes DDL when it is parsed, so only queries and DML are
        // parsed on the server here (catching SQL errors early). Bind names
        // come from oracledb's client-side parser and need no round trip.
        if stmt.is_query() || stmt.is_dml() {
            stmt.ensure_fully_parsed().map_err(from_oracle)?;
        }
        self.bind_names = stmt.bind_names().to_vec();
        drop(conn);
        self.prepared = true;
        Ok(())
    }

    /// Setting a query switches the statement out of ingest mode. Data bound
    /// for a previous ingest is dropped so it is not mistaken for query
    /// parameters (DB-API cursors reuse one statement for both).
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        if self.ingest_mode.take().is_some() || self.target_table.is_some() {
            self.clear_ingest();
            self.bound = None;
        }
        self.sql = Some(query.as_ref().to_string());
        self.prepared = false;
        self.bind_names.clear();
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(error(
            "Substrait is not supported by the Orkidea driver",
            Status::NotImplemented,
        ))
    }
}
