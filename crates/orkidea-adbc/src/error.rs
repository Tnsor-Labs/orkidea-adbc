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

use adbc_core::error::{Error as AdbcError, Status};
use oracledb::{Error as OracleError, ErrorKind};

pub(crate) type Result<T> = std::result::Result<T, AdbcError>;

pub(crate) fn error(message: impl Into<String>, status: Status) -> AdbcError {
    AdbcError::with_message_and_status(message, status)
}

/// Map an `oracledb` error into an ADBC error.
///
/// The mapping is intentionally conservative: anything that is not clearly an
/// argument, state, or authentication problem is reported as an internal/IO
/// error so that clients do not mistake it for a retryable user error.
///
/// Note: the message is the `oracledb` display message. It never contains the
/// password because `oracledb` stores passwords in a secret wrapper and does
/// not include credentials in its error messages.
pub(crate) fn from_oracle(err: OracleError) -> AdbcError {
    let message = err.to_string();
    let (status, vendor_code) = match err.kind() {
        ErrorKind::DbError(db) => {
            let code = db.code();
            let status = match code {
                942 => Status::NotFound,
                1 => Status::AlreadyExists,
                54 | 55 | 56 | 1013 => Status::Timeout,
                1017 | 28000 | 28001 => Status::Unauthenticated,
                1031 | 1034 | 1045 => Status::Unauthorized,
                2290..=2292 => Status::Integrity,
                _ => Status::InvalidData,
            };
            (status, code as i32)
        }
        ErrorKind::NoDataFound => (Status::NotFound, 0),
        ErrorKind::NoCredentials => (Status::Unauthenticated, 0),
        ErrorKind::NoConnectString
        | ErrorKind::InvalidConnectString(_, _)
        | ErrorKind::InvalidServiceName(_, _, _, _)
        | ErrorKind::InvalidSid(_, _, _, _)
        | ErrorKind::InvalidNetworkName(_)
        | ErrorKind::InvalidDescriptorNode(_, _)
        | ErrorKind::TnsAliasNotFound(_, _)
        | ErrorKind::NoConfigDir
        | ErrorKind::IfileCycleDetected(_, _) => (Status::InvalidArguments, 0),
        ErrorKind::ListenerRefusedConnection(_, _, _, _) => (Status::IO, 0),
        ErrorKind::CallTimeoutExceeded => (Status::Timeout, 0),
        ErrorKind::DeadConnection | ErrorKind::UnableToRecover => (Status::IO, 0),
        ErrorKind::TlsOperation | ErrorKind::StreamOperation | ErrorKind::PemFileOperation => {
            (Status::IO, 0)
        }
        ErrorKind::UnsupportedArrowType(_)
        | ErrorKind::UnsupportedConversion(_, _)
        | ErrorKind::UnsupportedDbType(_)
        | ErrorKind::CannotConvertToArrowDecimal128(_, _, _)
        | ErrorKind::WrongArrowSchemaLength(_, _)
        | ErrorKind::ValueWasNull
        | ErrorKind::IntegerTooLarge(_, _)
        | ErrorKind::ColumnTruncated(_, _) => (Status::InvalidData, 0),
        ErrorKind::WrongNumPositionalBinds(_, _)
        | ErrorKind::InvalidBindName(_)
        | ErrorKind::MissingBindValue(_)
        | ErrorKind::InvalidColumnIndex(_)
        | ErrorKind::InvalidColumnName(_)
        | ErrorKind::NameHasEmbeddedQuotes => (Status::InvalidArguments, 0),
        ErrorKind::NotImplemented(_)
        | ErrorKind::OperationNotSupportedOnBfile
        | ErrorKind::OperationOnlySupportedOnBfile => (Status::NotImplemented, 0),
        ErrorKind::LockPoisoned => (Status::Internal, 0),
        ErrorKind::NotConnected | ErrorKind::PoolNotOpen => (Status::InvalidState, 0),
        _ => (Status::Internal, 0),
    };
    let mut mapped = error(message, status);
    mapped.vendor_code = vendor_code;
    mapped
}

/// Convenience for validating that an option was not set to an unsupported
/// value type.
pub(crate) fn expect_string(value: &adbc_core::options::OptionValue, key: &str) -> Result<String> {
    match value {
        adbc_core::options::OptionValue::String(v) => Ok(v.clone()),
        other => Err(error(
            format!("option {key:?} expects a string, got {other:?}"),
            Status::InvalidArguments,
        )),
    }
}

pub(crate) fn expect_int(value: &adbc_core::options::OptionValue, key: &str) -> Result<i64> {
    match value {
        adbc_core::options::OptionValue::Int(v) => Ok(*v),
        other => Err(error(
            format!("option {key:?} expects an integer, got {other:?}"),
            Status::InvalidArguments,
        )),
    }
}

pub(crate) fn expect_bool(value: &adbc_core::options::OptionValue, key: &str) -> Result<bool> {
    bool::try_from(value).map_err(|_| {
        error(
            format!("option {key:?} expects \"true\" or \"false\""),
            Status::InvalidArguments,
        )
    })
}
