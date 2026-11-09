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

//! Oracle ADBC driver that converts query results to Arrow record batches.
//!
//! This crate implements the ADBC abstract API ([`adbc_core`]) on top of
//! Oracle's official pure-Rust [`oracledb`] driver. Query results are produced
//! directly as Arrow [`arrow_array::RecordBatch`]es by `oracledb`'s native
//! Arrow fetch path; no OCI, Instant Client, ODBC driver manager, or libarrow
//! dependency is required.
//!
//! The companion crate `orkidea-adbc-ffi` exports this driver through the ADBC
//! C ABI so it can be loaded by the ADBC driver manager from Python, C/C++,
//! Go, R, and any other ADBC client.

mod arrow_schema;
mod config;
mod connection;
mod database;
mod driver;
mod error;
mod ingest;
mod metadata;
mod statement;

pub use connection::OracleConnection;
pub use database::OracleDatabase;
pub use driver::OracleDriver;
pub use statement::OracleStatement;

/// Human readable driver name advertised through `get_info`.
pub const DRIVER_NAME: &str = "orkidea";

/// Driver version advertised through `get_info`.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Arrow implementation version used by this build.
pub const ARROW_VERSION: &str = "59.1.0";
