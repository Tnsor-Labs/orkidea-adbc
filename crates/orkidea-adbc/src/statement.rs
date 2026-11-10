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

use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use oracledb::{Cursor, Row, ToDbValue};

use crate::config::{
    validate_number_scale, OPT_BATCH_SIZE, OPT_FETCH_ARRAY_SIZE, OPT_PREFETCH_ROWS,
    OPT_UNCONSTRAINED_NUMBER, OPT_UNCONSTRAINED_NUMBER_SCALE,
};
use crate::connection::ConnState;
use crate::error::{error, expect_bool, expect_int, expect_string, from_oracle};
use crate::ingest;
use crate::params::{for_binds, param_row, BindMode};
use crate::placeholders::Placeholders;
use crate::reader::{
    cursor_rows, fetched_rows, NoMoreRows, OracleBatchReader, QueuedRows, RowSource, Rows,
};
use crate::types::{plan_columns, schema_of, TypeOptions, UnconstrainedNumber};

pub(crate) const DEFAULT_BATCH_SIZE: usize = 8192;

pub(crate) const OPT_PARTITION_COUNT: &str = "oracle.partition.count";
pub(crate) const OPT_PARTITION_COLUMN: &str = "oracle.partition.column";
pub(crate) const OPT_PARTITION_CONSISTENT: &str = "oracle.partition.consistent";
/// ADBC 1.1 option: bind parameters by field name (`true`) or by position
/// (`false`). The ADBC Python DB-API sets it for dict parameters.
pub(crate) const OPT_BIND_BY_NAME: &str = "adbc.statement.bind_by_name";

/// Upper bound of the default fetch array size, which otherwise follows the
/// batch size.
pub(crate) const MAX_DEFAULT_FETCH_ARRAY_SIZE: usize = 10_000;

/// Round-trip tuning applied to each Oracle cursor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FetchTuning {
    /// Rows fetched per round trip (`None`: oracledb default, 100). A
    /// statement fills it in from its batch size when it is not set; see
    /// [`OracleStatement::fetch_tuning`].
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
                let result = crate::params::execute_chunk(conn, sql, batch.slice(offset, len))
                    .map_err(|mut err| {
                        err.message = format!(
                            "executing parameter rows {}..{} failed: {}",
                            offset_total + offset,
                            offset_total + offset + len,
                            err.message
                        );
                        err
                    })?;
                affected += result;
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

impl ParamCursors {
    fn next_cursor(&mut self) -> Result<Option<Cursor>> {
        if self.next_row >= self.params.num_rows() {
            return Ok(None);
        }
        let row = param_row(&self.params, self.next_row)?;
        self.next_row += 1;
        open_cursor(&self.state, &self.sql, &row.as_refs(), self.tuning).map(Some)
    }
}

impl RowSource for ParamCursors {
    fn next_rows(&mut self) -> Result<Option<Rows>> {
        Ok(self.next_cursor()?.map(cursor_rows))
    }
}

/// How a statement is executed by `execute()`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Query,
    /// PL/SQL blocks: OUT binds (or a single REF CURSOR) form the result.
    PlSql,
    /// DML with `RETURNING ... INTO`: the returned rows form the result.
    DmlReturning,
    /// Other DML and DDL: an empty result.
    Other,
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
    pub(crate) bind_mode: BindMode,
    pub(crate) ingest_mode: Option<IngestMode>,
    pub(crate) target_table: Option<String>,
    pub(crate) target_catalog: Option<String>,
    pub(crate) target_schema: Option<String>,
    pub(crate) temporary: bool,
    pub(crate) batch_size: usize,
    pub(crate) tuning: FetchTuning,
    pub(crate) type_options: TypeOptions,
    pub(crate) partition_count: u32,
    pub(crate) partition_column: Option<String>,
    pub(crate) partition_consistent: bool,
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
            bind_mode: BindMode::Auto,
            ingest_mode: None,
            target_table: None,
            target_catalog: None,
            target_schema: None,
            temporary: false,
            batch_size,
            tuning,
            type_options,
            partition_count: 1,
            partition_column: None,
            partition_consistent: false,
        })
    }

    /// The fetch tuning for cursors this statement opens. Unless
    /// `oracle.fetch_array_size` is set, each round trip fetches one batch
    /// (at most [`MAX_DEFAULT_FETCH_ARRAY_SIZE`] rows) instead of oracledb's
    /// default of 100 rows, which makes large reads several times slower.
    pub(crate) fn fetch_tuning(&self) -> FetchTuning {
        let default = self.batch_size.min(MAX_DEFAULT_FETCH_ARRAY_SIZE) as u32;
        FetchTuning {
            fetch_array_size: Some(self.tuning.fetch_array_size.unwrap_or(default)),
            ..self.tuning
        }
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

    /// The statement's bind placeholders (client-side; no round trip).
    fn placeholders(&self, sql: &str) -> Result<Placeholders> {
        let conn = self.state.lock()?;
        let stmt = conn
            .statement(sql)
            .map_err(from_oracle)?
            .build()
            .map_err(from_oracle)?;
        Ok(Placeholders::of(&stmt))
    }

    /// The bound parameters ordered for the statement's placeholders.
    fn ordered_params(&mut self, sql: &str) -> Result<Option<RecordBatch>> {
        match self.bound_params()? {
            None => Ok(None),
            Some(batch) => Ok(Some(for_binds(
                &self.placeholders(sql)?,
                batch,
                self.bind_mode,
            )?)),
        }
    }

    /// Classifies the statement with oracledb's client-side SQL parser. No
    /// round trip to the server.
    fn classify(&self, sql: &str) -> Result<Kind> {
        let conn = self.state.lock()?;
        let stmt = conn
            .statement(sql)
            .map_err(from_oracle)?
            .build()
            .map_err(from_oracle)?;
        Ok(if stmt.is_query() {
            Kind::Query
        } else if stmt.is_plsql() {
            Kind::PlSql
        } else if stmt.is_dml_returning() {
            Kind::DmlReturning
        } else {
            Kind::Other
        })
    }

    /// Parses and describes the statement on the server without executing it.
    /// Parses a statement on the server and returns it with its metadata.
    /// It bypasses the statement cache: a cached statement counts as parsed
    /// and keeps the columns it had then, so after the table is re-created
    /// (DROP and CREATE, or ALTER) the same SQL would describe the old table.
    fn describe(&self, sql: &str) -> Result<oracledb::Statement> {
        let conn = self.state.lock()?;
        let mut stmt = conn
            .statement(sql)
            .map_err(from_oracle)?
            .exclude_from_cache()
            .build()
            .map_err(from_oracle)?;
        if stmt.is_query() {
            stmt.ensure_fully_parsed().map_err(from_oracle)?;
        }
        Ok(stmt)
    }

    fn execute_query(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let sql = self.sql()?;
        match self.classify(&sql)? {
            Kind::Query => {}
            kind @ (Kind::PlSql | Kind::DmlReturning) => {
                return self.execute_with_outputs(&sql, kind)
            }
            Kind::Other => {
                // ADBC allows execute() on any statement; non-queries yield
                // an empty result with an empty schema.
                self.execute_non_query(&sql)?;
                return Ok(Box::new(OracleBatchReader::empty(
                    Arc::new(Schema::empty()),
                    Vec::new(),
                )?));
            }
        }

        let params = self.ordered_params(&sql)?;
        let (first, source): (Option<Cursor>, Box<dyn RowSource>) = match params {
            None => (
                Some(open_cursor(&self.state, &sql, &[], self.fetch_tuning())?),
                Box::new(NoMoreRows),
            ),
            Some(params) => {
                let mut source = ParamCursors {
                    state: self.state.clone(),
                    sql: sql.clone(),
                    params,
                    next_row: 0,
                    tuning: self.fetch_tuning(),
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
            first.map(cursor_rows),
            source,
            self.batch_size,
        )?))
    }

    /// Executes a PL/SQL block or a DML RETURNING statement once per bound
    /// parameter row and returns its outputs:
    ///
    /// - PL/SQL with exactly one OUT bind of type REF CURSOR: the cursor's
    ///   rows (one cursor per execution, concatenated);
    /// - other PL/SQL: one row of OUT/IN OUT values per execution, columns
    ///   named after the binds; no OUT binds gives an empty result;
    /// - DML RETURNING: the returned rows.
    fn execute_with_outputs(
        &mut self,
        sql: &str,
        kind: Kind,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.ensure_writable()?;
        let params = self.ordered_params(sql)?;
        let executions = params.as_ref().map_or(1, RecordBatch::num_rows);
        let mut out_rows: Vec<Row> = Vec::new();
        let mut cursors: VecDeque<Cursor> = VecDeque::new();
        let mut metadata: Vec<oracledb::Metadata> = Vec::new();
        {
            let conn = self.state.lock()?;
            for index in 0..executions {
                let row = params.as_ref().map(|p| param_row(p, index)).transpose()?;
                let refs = row.as_ref().map(|r| r.as_refs()).unwrap_or_default();
                let mut stmt = conn
                    .statement(sql)
                    .map_err(from_oracle)?
                    .build()
                    .map_err(from_oracle)?;
                let mut result = stmt.execute(&refs).map_err(|e| {
                    let mut err = from_oracle(e);
                    if executions > 1 {
                        err.message = format!("parameter row {index}: {}", err.message);
                    }
                    err
                })?;
                if metadata.is_empty() {
                    metadata = stmt.out_metadata().to_vec();
                }
                match kind {
                    Kind::DmlReturning => out_rows.extend(result.returned_data()),
                    _ => {
                        let mut out = result.out_bind_data();
                        let columns = out.columns().to_vec();
                        if columns.is_empty() {
                            continue;
                        }
                        let cursor_columns = columns
                            .iter()
                            .filter(|c| c.db_type() == oracledb::DB_TYPE_CURSOR)
                            .count();
                        match (columns.len(), cursor_columns) {
                            (_, 0) => out_rows.push(out),
                            (1, 1) => {
                                cursors.push_back(out.take::<Cursor>(0).map_err(from_oracle)?)
                            }
                            _ => {
                                return Err(error(
                                    "a PL/SQL block can return either one REF CURSOR OUT bind \
                                     or scalar OUT binds, not both",
                                    Status::NotImplemented,
                                ))
                            }
                        }
                    }
                }
            }
            self.state.commit_if_autocommit(&conn)?;
        }

        // OUT bind metadata carries no meaningful nullability: any output
        // can be NULL.
        let nullable = |plans: Vec<crate::types::ColumnPlan>| -> Vec<crate::types::ColumnPlan> {
            plans
                .into_iter()
                .map(|mut p| {
                    p.field = p.field.with_nullable(true);
                    p
                })
                .collect()
        };
        if let Some(first) = cursors.pop_front() {
            let plans = plan_columns(first.columns(), &self.type_options)?;
            let rest = cursors.into_iter().map(cursor_rows).collect();
            return Ok(Box::new(OracleBatchReader::new(
                schema_of(&plans),
                plans,
                Some(cursor_rows(first)),
                Box::new(QueuedRows(rest)),
                self.batch_size,
            )?));
        }
        let columns = out_rows
            .first()
            .map(|r| r.columns().to_vec())
            .unwrap_or(metadata);
        if kind == Kind::PlSql && out_rows.is_empty() {
            return Ok(Box::new(OracleBatchReader::empty(
                Arc::new(Schema::empty()),
                Vec::new(),
            )?));
        }
        let plans = nullable(plan_columns(&columns, &self.type_options)?);
        let schema = schema_of(&plans);
        if out_rows.is_empty() {
            return Ok(Box::new(OracleBatchReader::empty(schema, plans)?));
        }
        Ok(Box::new(OracleBatchReader::new(
            schema,
            plans,
            Some(fetched_rows(out_rows)),
            Box::new(NoMoreRows),
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
            Some((_, batches)) => {
                let placeholders = {
                    let stmt = conn
                        .statement(sql)
                        .map_err(from_oracle)?
                        .build()
                        .map_err(from_oracle)?;
                    Placeholders::of(&stmt)
                };
                let mode = self.bind_mode;
                let batches: BatchIter = Box::new(
                    batches.map(move |b| b.and_then(|b| for_binds(&placeholders, b, mode))),
                );
                execute_batches(&conn, sql, batches, self.batch_size)?
            }
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
            OPT_PARTITION_COUNT => {
                let v = expect_int(&value, key)?;
                self.partition_count = u32::try_from(v)
                    .ok()
                    .filter(|v| (1..=crate::partition::MAX_PARTITIONS).contains(v))
                    .ok_or_else(|| {
                        error(
                            format!(
                                "{OPT_PARTITION_COUNT} must be between 1 and {}",
                                crate::partition::MAX_PARTITIONS
                            ),
                            Status::InvalidArguments,
                        )
                    })?;
                Ok(())
            }
            OPT_PARTITION_COLUMN => {
                self.partition_column = Some(crate::metadata::normalize_identifier(
                    &expect_string(&value, key)?,
                    "partition column",
                )?);
                Ok(())
            }
            OPT_PARTITION_CONSISTENT => {
                self.partition_consistent = expect_bool(&value, key)?;
                Ok(())
            }
            OPT_BIND_BY_NAME => {
                self.bind_mode = if expect_bool(&value, key)? {
                    BindMode::ByName
                } else {
                    BindMode::ByPosition
                };
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
            OPT_PARTITION_COLUMN => self
                .partition_column
                .clone()
                .ok_or_else(|| error("partition column was not set", Status::NotFound)),
            OPT_PARTITION_CONSISTENT => Ok(self.partition_consistent.to_string()),
            OPT_BIND_BY_NAME => match self.bind_mode {
                BindMode::ByName => Ok("true".into()),
                BindMode::ByPosition => Ok("false".into()),
                BindMode::Auto => Err(error(
                    format!(
                        "{OPT_BIND_BY_NAME} was not set: parameters bind by name when every \
                         placeholder matches a field name, otherwise by position"
                    ),
                    Status::NotFound,
                )),
            },
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
            OPT_FETCH_ARRAY_SIZE => Ok(self
                .fetch_tuning()
                .fetch_array_size
                .map(i64::from)
                .unwrap_or_default()),
            OPT_PREFETCH_ROWS => self
                .tuning
                .prefetch_rows
                .map(i64::from)
                .ok_or_else(|| error("oracle.prefetch_rows has not been set", Status::NotFound)),
            OPT_UNCONSTRAINED_NUMBER_SCALE => {
                Ok(i64::from(self.type_options.unconstrained_number_scale))
            }
            OPT_PARTITION_COUNT => Ok(i64::from(self.partition_count)),
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

    /// Splits a query into `oracle.partition.count` hash partitions on
    /// `oracle.partition.column` (see [`crate::partition`]). Nothing is
    /// fetched: each descriptor is read with `read_partition`, on this or any
    /// other connection to the same database.
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        if self.ingest_mode.is_some() {
            return Err(error(
                "ingest statements cannot be partitioned",
                Status::InvalidState,
            ));
        }
        if self.bound.is_some() {
            return Err(error(
                "partitioned execution does not support bound parameters",
                Status::NotImplemented,
            ));
        }
        let sql = self.sql()?;
        if self.classify(&sql)? != Kind::Query {
            return Err(error(
                "only queries can be executed as partitions",
                Status::InvalidArguments,
            ));
        }
        let described = self.describe(&sql)?;
        let plans = plan_columns(described.out_metadata(), &self.type_options)?;
        let schema = (*schema_of(&plans)).clone();
        drop(described);

        let count = self.partition_count;
        let column = match (count, &self.partition_column) {
            (1, _) => None,
            (_, None) => {
                return Err(error(
                    format!("{OPT_PARTITION_COUNT} > 1 needs {OPT_PARTITION_COLUMN}"),
                    Status::InvalidArguments,
                ))
            }
            (_, Some(column)) => {
                if schema.field_with_name(column).is_err() {
                    return Err(error(
                        format!(
                            "partition column {column:?} is not a column of the query result \
                             (columns: {})",
                            schema
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        Status::InvalidArguments,
                    ));
                }
                Some(column.clone())
            }
        };
        let scn = if self.partition_consistent {
            Some(crate::partition::current_scn(&*self.state.lock()?)?)
        } else {
            None
        };
        let partitions = (0..count)
            .map(|index| {
                crate::partition::Partition {
                    sql: sql.clone(),
                    column: column.clone(),
                    count,
                    index,
                    scn,
                    type_options: self.type_options,
                    batch_size: u32::try_from(self.batch_size).unwrap_or(u32::MAX),
                    tuning: self.fetch_tuning(),
                }
                .encode()
            })
            .collect();
        Ok(PartitionedResult {
            partitions,
            schema,
            rows_affected: -1,
        })
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
        // Parsed outside the statement cache: oracledb 26.0.0-beta.5 closes
        // a query's cursor once it is fetched to the end, and a parse of the
        // cached statement then leaves it with a cursor but no binds, so its
        // next execution fails with ORA-01008 (fixed upstream after beta.5).
        // Keeping prepare out of the cache also leaves the statement that
        // executes untouched.
        let mut stmt = conn
            .statement(&sql)
            .map_err(from_oracle)?
            .exclude_from_cache()
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

    /// Setting a query starts over: it switches the statement out of ingest
    /// mode and drops bound data, so parameters bound for a previous query or
    /// ingest are not sent with this one (DB-API cursors reuse one statement
    /// for everything). Bind parameters after setting the query.
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.clear_ingest();
        self.bound = None;
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
