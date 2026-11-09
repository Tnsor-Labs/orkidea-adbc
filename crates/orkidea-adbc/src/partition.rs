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

//! Partitioned execution (`ExecutePartitions` / `ReadPartition`).
//!
//! A query is split into `oracle.partition.count` hash partitions on
//! `oracle.partition.column`: partition `k` of `n` reads the rows where
//! `ORA_HASH(column, n - 1) = k`; rows where the column is NULL belong to
//! partition 0. Every row of the result lands in exactly one partition.
//!
//! With `oracle.partition.consistent`, every partition reads the database as
//! of the same SCN, captured when the partitions are created, so concurrent
//! changes cannot duplicate or drop rows. That uses `DBMS_FLASHBACK` and
//! needs `GRANT EXECUTE ON DBMS_FLASHBACK`.
//!
//! Descriptors are self-contained: they carry the SQL, the partitioning,
//! the SCN, and the type and fetch options, but no credentials. Any
//! connection to the same database can read them.

use adbc_core::error::Status;

use crate::error::{error, from_oracle, Result};
use crate::statement::FetchTuning;
use crate::types::{TypeOptions, UnconstrainedNumber};

const MAGIC: &[u8; 4] = b"ORKP";
const VERSION: u8 = 1;
/// Upper bound on `oracle.partition.count`.
pub(crate) const MAX_PARTITIONS: u32 = 4096;

/// One partition of a query.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Partition {
    pub(crate) sql: String,
    /// Normalised (Oracle-stored) name of the hash column; `None` when the
    /// query is a single partition.
    pub(crate) column: Option<String>,
    pub(crate) count: u32,
    pub(crate) index: u32,
    pub(crate) scn: Option<u64>,
    pub(crate) type_options: TypeOptions,
    pub(crate) batch_size: u32,
    pub(crate) tuning: FetchTuning,
}

impl Partition {
    /// The SQL this partition executes.
    pub(crate) fn sql(&self) -> String {
        match &self.column {
            None => self.sql.clone(),
            Some(column) => {
                let column = format!("orkidea_p.\"{column}\"");
                let null_rows = if self.index == 0 {
                    format!(" OR {column} IS NULL")
                } else {
                    String::new()
                };
                let inner = self.sql.trim().trim_end_matches(';').trim_end();
                format!(
                    "SELECT orkidea_p.* FROM ({inner}) orkidea_p WHERE ORA_HASH({column}, {}) = {}{null_rows}",
                    self.count - 1,
                    self.index
                )
            }
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.sql.len() + 64);
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        put_str(&mut out, &self.sql);
        put_opt_str(&mut out, self.column.as_deref());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        match self.scn {
            Some(scn) => {
                out.push(1);
                out.extend_from_slice(&scn.to_le_bytes());
            }
            None => out.push(0),
        }
        out.push(match self.type_options.unconstrained_number {
            UnconstrainedNumber::Float64 => 0,
            UnconstrainedNumber::Decimal => 1,
            UnconstrainedNumber::String => 2,
        });
        out.push(self.type_options.unconstrained_number_scale as u8);
        out.extend_from_slice(&self.batch_size.to_le_bytes());
        put_opt_u32(&mut out, self.tuning.fetch_array_size);
        put_opt_u32(&mut out, self.tuning.prefetch_rows);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader { bytes, pos: 0 };
        if r.take(4)? != MAGIC {
            return Err(invalid("not an Orkidea partition descriptor"));
        }
        let version = r.u8()?;
        if version != VERSION {
            return Err(invalid(&format!(
                "unsupported partition descriptor version {version}"
            )));
        }
        let sql = r.string()?;
        let column = r.opt_string()?;
        let count = r.u32()?;
        let index = r.u32()?;
        let scn = match r.u8()? {
            0 => None,
            1 => Some(r.u64()?),
            _ => return Err(invalid("corrupt partition descriptor")),
        };
        let unconstrained_number = match r.u8()? {
            0 => UnconstrainedNumber::Float64,
            1 => UnconstrainedNumber::Decimal,
            2 => UnconstrainedNumber::String,
            _ => return Err(invalid("corrupt partition descriptor")),
        };
        let unconstrained_number_scale = r.u8()? as i8;
        let batch_size = r.u32()?;
        let fetch_array_size = r.opt_u32()?;
        let prefetch_rows = r.opt_u32()?;
        if r.pos != bytes.len()
            || count == 0
            || count > MAX_PARTITIONS
            || index >= count
            || (count > 1) != column.is_some()
            || !(0..=38).contains(&unconstrained_number_scale)
        {
            return Err(invalid("corrupt partition descriptor"));
        }
        Ok(Self {
            sql,
            column,
            count,
            index,
            scn,
            type_options: TypeOptions {
                unconstrained_number,
                unconstrained_number_scale,
            },
            batch_size,
            tuning: FetchTuning {
                fetch_array_size,
                prefetch_rows,
            },
        })
    }
}

fn invalid(message: &str) -> adbc_core::error::Error {
    error(message, Status::InvalidArguments)
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn put_opt_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(v) => {
            out.push(1);
            put_str(out, v);
        }
        None => out.push(0),
    }
}

fn put_opt_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        None => out.push(0),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid("truncated partition descriptor"))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn string(&mut self) -> Result<String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| invalid("corrupt partition descriptor"))
    }

    fn opt_string(&mut self) -> Result<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.string()?)),
            _ => Err(invalid("corrupt partition descriptor")),
        }
    }

    fn opt_u32(&mut self) -> Result<Option<u32>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u32()?)),
            _ => Err(invalid("corrupt partition descriptor")),
        }
    }
}

/// The current SCN, for consistent partitions.
pub(crate) fn current_scn(conn: &oracledb::Connection) -> Result<u64> {
    let row = conn
        .query_row(
            "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM dual",
            &[],
        )
        .map_err(|e| {
            let mut err = from_oracle(e);
            if matches!(err.vendor_code, 904 | 6550 | 6576) {
                err.message = format!(
                    "{}; oracle.partition.consistent needs GRANT EXECUTE ON DBMS_FLASHBACK",
                    err.message
                );
                err.status = Status::Unauthorized;
            }
            err
        })?;
    let scn: oracledb::OracleNumber = row.get(0).map_err(from_oracle)?;
    scn.to_string()
        .parse::<u64>()
        .map_err(|e| error(format!("invalid SCN {scn}: {e}"), Status::Internal))
}

/// Opens the cursor for a partition. With an SCN, the cursor is opened in
/// flashback mode and flashback is disabled again straight away: an open
/// cursor keeps reading the snapshot it was opened at.
pub(crate) fn open(
    state: &crate::connection::ConnState,
    partition: &Partition,
) -> Result<oracledb::Cursor> {
    let sql = partition.sql();
    let Some(scn) = partition.scn else {
        return crate::statement::open_cursor(state, &sql, &[], partition.tuning);
    };
    let scn: oracledb::OracleNumber = scn.to_string().parse().map_err(from_oracle)?;
    {
        let conn = state.lock()?;
        conn.execute(
            "BEGIN DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER(:1); END;",
            &[&scn],
        )
        .map_err(from_oracle)?;
    }
    let cursor =
        crate::statement::open_cursor(state, &sql, &[], partition.tuning).map_err(|mut err| {
            if err.vendor_code == 1466 {
                err.message = format!(
                    "{}; a consistent partition cannot read a table created or altered after \
                 (or within a few seconds before) its snapshot SCN",
                    err.message
                );
            }
            err
        });
    let disabled = state
        .lock()
        .and_then(|conn| {
            conn.execute("BEGIN DBMS_FLASHBACK.DISABLE; END;", &[])
                .map_err(from_oracle)
        })
        .map(|_| ());
    let cursor = cursor?;
    disabled?;
    Ok(cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(count: u32, index: u32, column: Option<&str>) -> Partition {
        Partition {
            sql: "SELECT id, name FROM t".into(),
            column: column.map(String::from),
            count,
            index,
            scn: Some(123_456_789),
            type_options: TypeOptions {
                unconstrained_number: UnconstrainedNumber::Decimal,
                unconstrained_number_scale: 4,
            },
            batch_size: 1000,
            tuning: FetchTuning {
                fetch_array_size: Some(500),
                prefetch_rows: None,
            },
        }
    }

    #[test]
    fn descriptors_round_trip() {
        let p = sample(4, 3, Some("ID"));
        assert_eq!(Partition::decode(&p.encode()).unwrap(), p);
        let single = Partition {
            scn: None,
            ..sample(1, 0, None)
        };
        assert_eq!(Partition::decode(&single.encode()).unwrap(), single);
    }

    #[test]
    fn corrupt_descriptors_are_rejected() {
        let bytes = sample(4, 1, Some("ID")).encode();
        assert!(Partition::decode(b"nope").is_err());
        assert!(Partition::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(Partition::decode(&extra).is_err());
        assert!(Partition::decode(&sample(4, 4, Some("ID")).encode()).is_err());
        assert!(Partition::decode(&sample(4, 0, None).encode()).is_err());
    }

    #[test]
    fn partition_sql() {
        assert_eq!(
            sample(4, 0, Some("ID")).sql(),
            "SELECT orkidea_p.* FROM (SELECT id, name FROM t) orkidea_p \
             WHERE ORA_HASH(orkidea_p.\"ID\", 3) = 0 OR orkidea_p.\"ID\" IS NULL"
        );
        assert_eq!(
            sample(4, 2, Some("ID")).sql(),
            "SELECT orkidea_p.* FROM (SELECT id, name FROM t) orkidea_p \
             WHERE ORA_HASH(orkidea_p.\"ID\", 3) = 2"
        );
        assert_eq!(sample(1, 0, None).sql(), "SELECT id, name FROM t");
    }
}
