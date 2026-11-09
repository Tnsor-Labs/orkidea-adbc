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

//! Mapping from Oracle column metadata to Arrow schemas.
//!
//! This mirrors the default mapping used by `oracledb`'s native Arrow fetch
//! path so that `execute_schema` reports exactly the schema that `execute`
//! will produce.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Status;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use oracledb::{Metadata, DB_TYPE_LONG, DB_TYPE_LONG_NVARCHAR, DB_TYPE_LONG_RAW};

use crate::error::{error, Result};

pub(crate) fn schema_from_metadata(columns: &[Metadata]) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let data_type = arrow_type(col)?;
        let metadata = HashMap::from([("oracle.type-name".to_string(), col.data_type())]);
        fields.push(Field::new(col.name(), data_type, col.nullable()).with_metadata(metadata));
    }
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn arrow_type(col: &Metadata) -> Result<DataType> {
    let db_type = col.db_type();

    if db_type == DB_TYPE_LONG || db_type == DB_TYPE_LONG_NVARCHAR {
        return Ok(DataType::LargeUtf8);
    }
    if db_type == DB_TYPE_LONG_RAW {
        return Ok(DataType::LargeBinary);
    }
    if db_type.is_string_type() {
        return Ok(DataType::Utf8);
    }
    if db_type.is_binary_type() {
        return Ok(DataType::Binary);
    }
    if db_type.is_date_type() {
        let unit = match col.scale() {
            0 => TimeUnit::Second,
            1..=3 => TimeUnit::Millisecond,
            4..=6 => TimeUnit::Microsecond,
            _ => TimeUnit::Nanosecond,
        };
        return Ok(DataType::Timestamp(unit, None));
    }

    if db_type == oracledb::DB_TYPE_BINARY_DOUBLE {
        return Ok(DataType::Float64);
    }
    if db_type == oracledb::DB_TYPE_BINARY_FLOAT {
        return Ok(DataType::Float32);
    }
    if db_type == oracledb::DB_TYPE_BOOLEAN {
        return Ok(DataType::Boolean);
    }
    if db_type == oracledb::DB_TYPE_NUMBER {
        let precision = col.precision();
        let scale = col.scale();
        return if precision == 0 || scale.unsigned_abs() > precision {
            Ok(DataType::Float64)
        } else if scale != 0 || precision > 18 {
            Ok(DataType::Decimal128(precision, scale))
        } else if precision <= 2 {
            Ok(DataType::Int8)
        } else if precision <= 4 {
            Ok(DataType::Int16)
        } else if precision <= 9 {
            Ok(DataType::Int32)
        } else {
            Ok(DataType::Int64)
        };
    }

    Err(error(
        format!(
            "Oracle column {:?} has type {} which oracledb cannot map to Arrow; \
             cast it in SQL (for example TO_CHAR/TO_NUMBER) or select a supported type",
            col.name(),
            col.data_type()
        ),
        Status::NotImplemented,
    ))
}
