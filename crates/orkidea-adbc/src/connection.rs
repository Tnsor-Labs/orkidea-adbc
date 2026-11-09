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

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use adbc_core::error::{Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use oracledb::Connection as OracleConn;

use crate::config::DbOptions;
use crate::error::{error, expect_bool, expect_string, from_oracle};
use crate::metadata;
use crate::statement::OracleStatement;

pub(crate) struct ConnState {
    pub(crate) conn: Mutex<OracleConn>,
    pub(crate) autocommit: AtomicBool,
    pub(crate) read_only: AtomicBool,
    pub(crate) current_schema: Mutex<Option<String>>,
    pub(crate) isolation_level: Mutex<Option<String>>,
    pub(crate) defaults: DbOptions,
}

impl ConnState {
    pub(crate) fn lock(&self) -> Result<std::sync::MutexGuard<'_, OracleConn>> {
        self.conn
            .lock()
            .map_err(|_| error("Oracle connection lock poisoned", Status::Internal))
    }

    pub(crate) fn autocommit(&self) -> bool {
        self.autocommit.load(Ordering::Relaxed)
    }

    pub(crate) fn read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    /// Commits on `conn` when autocommit is enabled. Takes the already-held
    /// guard so the write and its commit happen under one lock acquisition.
    pub(crate) fn commit_if_autocommit(&self, conn: &OracleConn) -> Result<()> {
        if self.autocommit() {
            conn.commit().map_err(from_oracle)?;
        }
        Ok(())
    }
}

/// An ADBC connection backed by a single Oracle session.
pub struct OracleConnection {
    pub(crate) state: Arc<ConnState>,
}

impl OracleConnection {
    pub(crate) fn new(conn: OracleConn, defaults: &DbOptions) -> Result<Self> {
        Ok(Self {
            state: Arc::new(ConnState {
                conn: Mutex::new(conn),
                autocommit: AtomicBool::new(true),
                read_only: AtomicBool::new(
                    defaults
                        .get_string("oracle.read_only")
                        .ok()
                        .flatten()
                        .as_deref()
                        .is_some_and(|v| v.eq_ignore_ascii_case("true")),
                ),
                current_schema: Mutex::new(None),
                isolation_level: Mutex::new(None),
                defaults: defaults.clone(),
            }),
        })
    }

    pub(crate) fn autocommit(&self) -> bool {
        self.state.autocommit()
    }

    pub(crate) fn read_only(&self) -> bool {
        self.state.read_only()
    }
}

impl Optionable for OracleConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let key = key.as_ref();
        match key {
            adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT => {
                let enabled = expect_bool(&value, key)?;
                if !enabled {
                    // Ending autocommit means the session controls the
                    // transaction. Make sure no implicit transaction from a
                    // previous statement leaks in.
                    self.state.autocommit.store(false, Ordering::Relaxed);
                } else {
                    // Switching back to autocommit should flush pending work,
                    // matching the semantics of a driver-managed transaction.
                    self.state.lock()?.commit().map_err(from_oracle)?;
                    self.state.autocommit.store(true, Ordering::Relaxed);
                }
                Ok(())
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_READ_ONLY => {
                let enabled = expect_bool(&value, key)?;
                self.state.read_only.store(enabled, Ordering::Relaxed);
                Ok(())
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => {
                let schema = expect_string(&value, key)?;
                let quoted = oracledb::enquote_name(&schema, false).map_err(from_oracle)?;
                let sql = format!("ALTER SESSION SET CURRENT_SCHEMA = {quoted}");
                self.state.lock()?.execute(&sql, &[]).map_err(from_oracle)?;
                *self
                    .state
                    .current_schema
                    .lock()
                    .map_err(|_| error("connection lock poisoned", Status::Internal))? =
                    Some(schema);
                Ok(())
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => Err(error(
                "Oracle does not support ADBC current catalog; use current schema",
                Status::NotImplemented,
            )),
            adbc_core::constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL => {
                let level = expect_string(&value, key)?;
                let sql = match level.as_str() {
                    "default" | "read_committed" => {
                        "ALTER SESSION SET ISOLATION_LEVEL = READ COMMITTED"
                    }
                    "serializable" => "ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE",
                    other => {
                        return Err(error(
                            format!(
                                "unsupported Oracle isolation level {other:?}; \
                                 supported levels: default, read_committed, serializable"
                            ),
                            Status::InvalidArguments,
                        ));
                    }
                };
                self.state.lock()?.execute(sql, &[]).map_err(from_oracle)?;
                *self
                    .state
                    .isolation_level
                    .lock()
                    .map_err(|_| error("connection lock poisoned", Status::Internal))? =
                    Some(level);
                Ok(())
            }
            _ => Err(error(
                format!("unknown Orkidea connection option {key:?}"),
                Status::InvalidArguments,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key = key.as_ref();
        match key {
            adbc_core::constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => self
                .state
                .current_schema
                .lock()
                .map_err(|_| error("connection lock poisoned", Status::Internal))?
                .clone()
                .ok_or_else(|| error("current schema has not been set", Status::NotFound)),
            adbc_core::constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL => self
                .state
                .isolation_level
                .lock()
                .map_err(|_| error("connection lock poisoned", Status::Internal))?
                .clone()
                .ok_or_else(|| error("isolation level has not been set", Status::NotFound)),
            adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT => {
                Ok(self.autocommit().to_string())
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_READ_ONLY => {
                Ok(self.read_only().to_string())
            }
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
        Err(error(
            format!("option {:?} is not an integer option", key.as_ref()),
            Status::InvalidArguments,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(error(
            format!("option {:?} is not a float option", key.as_ref()),
            Status::InvalidArguments,
        ))
    }
}

impl Connection for OracleConnection {
    type StatementType = OracleStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        OracleStatement::new(self.state.clone())
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_info(&self.state, codes)
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_objects(
            &self.state,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        metadata::get_table_schema(&self.state, catalog, db_schema, table_name)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_table_types()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_statistic_names()
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_statistics(catalog, db_schema, table_name, approximate)
    }

    fn commit(&mut self) -> Result<()> {
        self.state.lock()?.commit().map_err(from_oracle)
    }

    fn rollback(&mut self) -> Result<()> {
        self.state.lock()?.rollback().map_err(from_oracle)
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(error(
            "Orkidea does not implement partitioned execution; use execute()",
            Status::NotImplemented,
        ))
    }
}

fn _assert_send() {
    fn assert_send<T: Send>() {}
    assert_send::<OracleConnection>();
    assert_send::<OracleStatement>();
}
