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

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};
use oracledb::Connection;

use crate::config::{DbOptions, OPT_CALL_TIMEOUT_MS};
use crate::connection::OracleConnection;
use crate::error::{error, from_oracle};

/// An ADBC database handle: a reusable Oracle connection configuration.
#[derive(Clone, Default)]
pub struct OracleDatabase {
    pub(crate) options: DbOptions,
}

impl OracleDatabase {
    fn connect(&self) -> Result<Connection> {
        let config = self.options.build_config()?;
        let connection = oracledb::connect(config).map_err(from_oracle)?;
        if let Some(timeout_ms) = self.options.get_i64(OPT_CALL_TIMEOUT_MS)? {
            connection
                .set_call_timeout(Some(std::time::Duration::from_millis(timeout_ms as u64)))
                .map_err(from_oracle)?;
        }
        Ok(connection)
    }
}

impl Optionable for OracleDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.options.set(key.as_ref(), value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key = key.as_ref();
        match self.options.get(key) {
            Some(OptionValue::String(v)) => Ok(v.clone()),
            Some(other) => Err(error(
                format!("option {key:?} does not hold a string: {other:?}"),
                Status::InvalidArguments,
            )),
            None => Err(error(
                format!("option {key:?} was not set"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let key = key.as_ref();
        match self.options.get(key) {
            Some(OptionValue::Bytes(v)) => Ok(v.clone()),
            Some(other) => Err(error(
                format!("option {key:?} does not hold bytes: {other:?}"),
                Status::InvalidArguments,
            )),
            None => Err(error(
                format!("option {key:?} was not set"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key = key.as_ref();
        match self.options.get(key) {
            Some(OptionValue::Int(v)) => Ok(*v),
            Some(other) => Err(error(
                format!("option {key:?} does not hold an integer: {other:?}"),
                Status::InvalidArguments,
            )),
            None => Err(error(
                format!("option {key:?} was not set"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let key = key.as_ref();
        match self.options.get(key) {
            Some(OptionValue::Double(v)) => Ok(*v),
            Some(other) => Err(error(
                format!("option {key:?} does not hold a float: {other:?}"),
                Status::InvalidArguments,
            )),
            None => Err(error(
                format!("option {key:?} was not set"),
                Status::NotFound,
            )),
        }
    }
}

impl Database for OracleDatabase {
    type ConnectionType = OracleConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let connection = self.connect()?;
        OracleConnection::new(connection, &self.options)
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut conn = self.new_connection()?;
        for (key, value) in opts {
            conn.set_option(key, value)?;
        }
        Ok(conn)
    }
}

// The ADBC FFI exporter requires database handles to be Send. The handle only
// owns an option map; connections are created on demand.
fn _assert_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OracleDatabase>();
}

#[allow(dead_code)]
fn _assert_error_is_send(_: Error) {}
