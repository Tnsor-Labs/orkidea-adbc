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

//! Value conversions between Oracle and Arrow representations.
//!
//! Calendar handling matches Oracle's own date arithmetic, so that the
//! difference between two converted values always equals `d2 - d1` computed
//! in Oracle (verified against Oracle Database in the live tests):
//!
//! - dates before 1582-10-15 use the Julian calendar, later ones Gregorian;
//! - BC years are numbered astronomically: year -1 is followed by a leap
//!   year 0 that Oracle counts but cannot represent;
//! - Oracle's first year, -4712, has no 29 February, so its January and
//!   February sit one day later than the standard Julian Day Number.
//!
//! Arrow timestamps count from the Unix epoch; all conversions go through
//! the Julian Day Number.

use std::fmt::Write;

use adbc_core::error::Status;
use arrow_buffer::i256;
use arrow_schema::TimeUnit;
use oracledb::{
    JsonScalar, JsonValue, OracleIntervalDS, OracleIntervalYM, OracleNumber, OracleTimestamp,
};

use crate::error::{error, Result};

/// Julian Day Number of 1970-01-01.
const UNIX_EPOCH_JDN: i64 = 2_440_588;
/// Julian Day Number of 1582-10-15, the first Gregorian day in Oracle.
const GREGORIAN_START_JDN: i64 = 2_299_161;
const SECONDS_PER_DAY: i64 = 86_400;

/// Julian Day Number of -4712-03-01, the first day of Oracle's calendar that
/// agrees with the standard Julian Day Number.
const ORACLE_FIRST_REGULAR_JDN: i64 = 60;

/// Julian Day Number of an Oracle calendar date.
fn oracle_date_to_jdn(year: i64, month: i64, day: i64) -> i64 {
    let jdn = date_to_jdn(year, month, day);
    if year == -4712 && month <= 2 {
        // Oracle's year -4712 is not a leap year.
        jdn + 1
    } else {
        jdn
    }
}

/// Oracle calendar date of a Julian Day Number.
fn jdn_to_oracle_date(jdn: i64) -> Result<(i16, u8, u8)> {
    let (year, month, day) = if (1..ORACLE_FIRST_REGULAR_JDN).contains(&jdn) {
        jdn_to_date(jdn - 1)
    } else {
        jdn_to_date(jdn)
    };
    if jdn < 1 || year == 0 || year > 9999 {
        return Err(error(
            format!(
                "date (Julian Day {jdn}) is outside the Oracle date range \
                 -4712-01-01 to 9999-12-31 or falls in year 0"
            ),
            Status::InvalidArguments,
        ));
    }
    Ok((year as i16, month as u8, day as u8))
}

/// Julian Day Number for a calendar date, using the Julian calendar before
/// 1582-10-15 and the Gregorian calendar from then on (Oracle's convention).
fn date_to_jdn(year: i64, month: i64, day: i64) -> i64 {
    let a = (14 - month) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    let gregorian = (year, month, day) >= (1582, 10, 15);
    if gregorian {
        day + (153 * m + 2) / 5 + 365 * y + y.div_euclid(4) - y.div_euclid(100) + y.div_euclid(400)
            - 32045
    } else {
        day + (153 * m + 2) / 5 + 365 * y + y.div_euclid(4) - 32083
    }
}

/// Inverse of [`date_to_jdn`]: returns (astronomical year, month, day).
fn jdn_to_date(jdn: i64) -> (i64, i64, i64) {
    let (b, c) = if jdn >= GREGORIAN_START_JDN {
        let a = jdn + 32044;
        let b = (4 * a + 3).div_euclid(146_097);
        (b, a - (146_097 * b).div_euclid(4))
    } else {
        (0, jdn + 32082)
    };
    let d = (4 * c + 3).div_euclid(1461);
    let e = c - (1461 * d).div_euclid(4);
    let m = (5 * e + 2).div_euclid(153);
    let day = e - (153 * m + 2).div_euclid(5) + 1;
    let month = m + 3 - 12 * m.div_euclid(10);
    let year = 100 * b + d - 4800 + m.div_euclid(10);
    (year, month, day)
}

fn units_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Arrow time unit able to hold `fractional_digits` digits of sub-second
/// precision without loss.
pub(crate) fn time_unit_for_scale(fractional_digits: i8) -> TimeUnit {
    match fractional_digits {
        i8::MIN..=0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

fn overflow(what: &str, unit: TimeUnit) -> adbc_core::error::Error {
    error(
        format!(
            "{what} does not fit in an Arrow {unit:?} value; select it with lower \
             fractional precision (for example CAST(col AS TIMESTAMP(6)))"
        ),
        Status::InvalidData,
    )
}

/// Converts an Oracle timestamp to an Arrow timestamp value in `unit`.
pub(crate) fn timestamp_to_arrow(ts: &OracleTimestamp, unit: TimeUnit) -> Result<i64> {
    let jdn = oracle_date_to_jdn(
        i64::from(ts.year()),
        i64::from(ts.month()),
        i64::from(ts.day()),
    );
    let seconds = (jdn - UNIX_EPOCH_JDN) * SECONDS_PER_DAY
        + i64::from(ts.hour()) * 3600
        + i64::from(ts.minute()) * 60
        + i64::from(ts.second());
    let per_second = units_per_second(unit);
    let fraction = i64::from(ts.nanoseconds()) / (1_000_000_000 / per_second);
    seconds
        .checked_mul(per_second)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(|| overflow(&format!("timestamp {ts}"), unit))
}

/// Converts an Arrow timestamp value to an Oracle timestamp (wall-clock
/// fields; `utc` marks the value as UTC).
pub(crate) fn arrow_to_timestamp(value: i64, unit: TimeUnit, utc: bool) -> Result<OracleTimestamp> {
    let per_second = units_per_second(unit);
    let seconds = value.div_euclid(per_second);
    let nanos = value.rem_euclid(per_second) * (1_000_000_000 / per_second);
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let secs_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let (year, month, day) = jdn_to_oracle_date(days + UNIX_EPOCH_JDN)?;
    let (hour, minute, second) = (
        (secs_of_day / 3600) as u8,
        ((secs_of_day % 3600) / 60) as u8,
        (secs_of_day % 60) as u8,
    );
    let build = if utc {
        OracleTimestamp::new_utc_timestamp
    } else {
        OracleTimestamp::new_timestamp
    };
    Ok(build(year, month, day, hour, minute, second, nanos as u32))
}

/// Converts an Arrow `Date32` (days since the epoch) to an Oracle date.
pub(crate) fn date32_to_timestamp(days: i32) -> Result<OracleTimestamp> {
    arrow_to_timestamp(i64::from(days) * SECONDS_PER_DAY, TimeUnit::Second, false)
}

/// Converts an INTERVAL DAY TO SECOND value to an Arrow duration in `unit`.
pub(crate) fn interval_ds_to_arrow(value: &OracleIntervalDS, unit: TimeUnit) -> Result<i64> {
    let per_second = units_per_second(unit);
    let seconds = i64::from(value.days()) * SECONDS_PER_DAY
        + i64::from(value.hours()) * 3600
        + i64::from(value.minutes()) * 60
        + i64::from(value.seconds());
    let fraction = i64::from(value.nanoseconds()) / (1_000_000_000 / per_second);
    seconds
        .checked_mul(per_second)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(|| overflow("INTERVAL DAY TO SECOND value", unit))
}

/// Converts an INTERVAL YEAR TO MONTH value to a number of months.
pub(crate) fn interval_ym_to_months(value: &OracleIntervalYM) -> Result<i32> {
    value
        .years()
        .checked_mul(12)
        .and_then(|v| v.checked_add(i32::from(value.months())))
        .ok_or_else(|| {
            error(
                "INTERVAL YEAR TO MONTH value does not fit in 32-bit months",
                Status::InvalidData,
            )
        })
}

/// Parses a plain decimal string (as produced by `OracleNumber`'s `Display`)
/// into an unscaled integer digit string at `scale`. Fails rather than
/// rounding when the value has more fractional digits than `scale`, or more
/// total digits than `precision`.
fn unscaled_digits(text: &str, precision: u8, scale: i8) -> Result<String> {
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
    let frac_part = frac_part.trim_end_matches('0');
    let mut digits = String::with_capacity(body.len() + 2);
    if scale >= 0 {
        let scale = scale as usize;
        if frac_part.len() > scale {
            return Err(error(
                format!(
                    "NUMBER value {text} has more than {scale} fractional digits; \
                     raise oracle.unconstrained_number_scale or select it as a string"
                ),
                Status::InvalidData,
            ));
        }
        digits.push_str(int_part.trim_start_matches('0'));
        digits.push_str(frac_part);
        digits.extend(std::iter::repeat_n('0', scale - frac_part.len()));
    } else {
        return Err(error(
            "negative Arrow decimal scales are not produced by this driver",
            Status::Internal,
        ));
    }
    let significant = digits.trim_start_matches('0');
    if significant.len() > precision as usize {
        return Err(error(
            format!("NUMBER value {text} does not fit in DECIMAL({precision},{scale})"),
            Status::InvalidData,
        ));
    }
    let mut out = String::with_capacity(significant.len() + 1);
    if negative && !significant.is_empty() {
        out.push('-');
    }
    out.push_str(if significant.is_empty() {
        "0"
    } else {
        significant
    });
    Ok(out)
}

pub(crate) fn number_to_decimal128(value: &OracleNumber, precision: u8, scale: i8) -> Result<i128> {
    let text = value.to_string();
    unscaled_digits(&text, precision, scale)?
        .parse::<i128>()
        .map_err(|e| error(format!("invalid NUMBER {text}: {e}"), Status::InvalidData))
}

pub(crate) fn number_to_decimal256(value: &OracleNumber, precision: u8, scale: i8) -> Result<i256> {
    let text = value.to_string();
    let digits = unscaled_digits(&text, precision, scale)?;
    i256::from_string(&digits)
        .ok_or_else(|| error(format!("invalid NUMBER {text}"), Status::InvalidData))
}

fn write_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_hex(out: &mut String, bytes: &[u8]) {
    out.push('"');
    for b in bytes {
        let _ = write!(out, "{b:02X}");
    }
    out.push('"');
}

fn write_json_float(out: &mut String, value: f64) {
    if value.is_finite() {
        let _ = write!(out, "{value}");
    } else {
        // JSON has no representation for NaN/Infinity.
        out.push_str("null");
    }
}

/// Serializes an Oracle JSON value to JSON text.
///
/// Object keys are emitted in sorted order: `oracledb` decodes objects into a
/// hash map, so the original key order is not available and sorting keeps the
/// output deterministic. Non-JSON scalar types use the same text forms Oracle's
/// `JSON_SERIALIZE` produces: timestamps as ISO-8601 strings, RAW and ids as
/// hex strings, intervals as ISO-8601 durations.
pub(crate) fn json_to_string(value: &JsonValue) -> String {
    let mut out = String::new();
    write_json(&mut out, value);
    out
}

fn write_json(out: &mut String, value: &JsonValue) {
    match value {
        JsonValue::JsonArray(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(out, item);
            }
            out.push(']');
        }
        JsonValue::JsonObject(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(out, key);
                out.push(':');
                write_json(out, &map[key]);
            }
            out.push('}');
        }
        JsonValue::Null => write_json_scalar(out, &JsonScalar::Null),
        JsonValue::Boolean(b) => write_json_scalar(out, &JsonScalar::Boolean(*b)),
        JsonValue::Number(n) => write_json_scalar(out, &JsonScalar::Number(*n)),
        JsonValue::BinaryDouble(v) => write_json_scalar(out, &JsonScalar::BinaryDouble(*v)),
        JsonValue::BinaryFloat(v) => write_json_scalar(out, &JsonScalar::BinaryFloat(*v)),
        JsonValue::String(s) => write_json_scalar(out, &JsonScalar::String(s)),
        JsonValue::Raw(bytes) | JsonValue::JsonId(bytes) => {
            write_json_scalar(out, &JsonScalar::Raw(bytes))
        }
        JsonValue::Timestamp(ts) => write_json_scalar(out, &JsonScalar::Timestamp(*ts)),
        JsonValue::IntervalDS(v) => write_json_scalar(out, &JsonScalar::IntervalDS(*v)),
        JsonValue::IntervalYM(v) => write_json_scalar(out, &JsonScalar::IntervalYM(*v)),
        JsonValue::Vector(vector) => write_vector(out, vector),
    }
}

/// Writes a JSON scalar as [`json_to_string`] does.
pub(crate) fn write_json_scalar(out: &mut String, value: &JsonScalar<'_>) {
    match value {
        JsonScalar::Null => out.push_str("null"),
        JsonScalar::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        JsonScalar::Number(n) => {
            let _ = write!(out, "{n}");
        }
        JsonScalar::BinaryDouble(v) => write_json_float(out, *v),
        JsonScalar::BinaryFloat(v) => write_json_float(out, f64::from(*v)),
        JsonScalar::String(s) => write_json_string(out, s),
        JsonScalar::Raw(bytes) => write_hex(out, bytes),
        JsonScalar::Timestamp(ts) => {
            let text = ts.to_string();
            write_json_string(out, &text);
        }
        JsonScalar::IntervalDS(v) => {
            let sign = if v.days() < 0
                || v.hours() < 0
                || v.minutes() < 0
                || v.seconds() < 0
                || v.nanoseconds() < 0
            {
                "-"
            } else {
                ""
            };
            let text = format!(
                "{sign}P{}DT{}H{}M{}.{:09}S",
                v.days().unsigned_abs(),
                v.hours().unsigned_abs(),
                v.minutes().unsigned_abs(),
                v.seconds().unsigned_abs(),
                v.nanoseconds().unsigned_abs()
            );
            write_json_string(out, &text);
        }
        JsonScalar::IntervalYM(v) => {
            let sign = if v.years() < 0 || v.months() < 0 {
                "-"
            } else {
                ""
            };
            let text = format!(
                "{sign}P{}Y{}M",
                v.years().unsigned_abs(),
                v.months().unsigned_abs()
            );
            write_json_string(out, &text);
        }
        JsonScalar::Vector(vector) => write_vector(out, vector),
    }
}

/// Writes JSON text as an OSON decoder visits a document, as
/// [`json_to_string`] writes the decoded [`JsonValue`]: object members are
/// sorted by name (and, as in the value's map, the last of a repeated name
/// wins), so both give the same text.
#[derive(Default)]
pub(crate) struct JsonTextWriter {
    pub(crate) out: String,
    containers: Vec<Container>,
}

enum Container {
    Array { items: usize },
    Object { start: usize, members: Vec<Member> },
}

/// One member of an object being written: its name, and where its text
/// (`"name":value`) starts and ends in the output.
struct Member {
    name: String,
    start: usize,
    end: usize,
}

impl JsonTextWriter {
    /// Before a value: the comma between array items.
    fn value(&mut self) {
        if let Some(Container::Array { items }) = self.containers.last_mut() {
            if *items > 0 {
                self.out.push(',');
            }
            *items += 1;
        }
    }

    /// Closes the last member of the innermost object.
    fn close_member(&mut self) {
        let end = self.out.len();
        if let Some(Container::Object { members, .. }) = self.containers.last_mut() {
            if let Some(last) = members.last_mut() {
                last.end = end;
            }
        }
    }
}

impl oracledb::JsonVisitor for JsonTextWriter {
    fn begin_object(&mut self, fields: usize) -> std::result::Result<(), oracledb::Error> {
        self.value();
        self.out.push('{');
        self.containers.push(Container::Object {
            start: self.out.len(),
            members: Vec::with_capacity(fields),
        });
        Ok(())
    }

    fn field_name(&mut self, name: &str) -> std::result::Result<(), oracledb::Error> {
        self.close_member();
        let Some(Container::Object { members, .. }) = self.containers.last_mut() else {
            return Ok(());
        };
        if !members.is_empty() {
            self.out.push(',');
        }
        let start = self.out.len();
        members.push(Member {
            name: name.to_string(),
            start,
            end: start,
        });
        write_json_string(&mut self.out, name);
        self.out.push(':');
        Ok(())
    }

    fn end_object(&mut self) -> std::result::Result<(), oracledb::Error> {
        self.close_member();
        if let Some(Container::Object { start, mut members }) = self.containers.pop() {
            let in_order = members.windows(2).all(|pair| pair[0].name < pair[1].name);
            if !in_order {
                // Stable, so that among repeated names the last stays last.
                members.sort_by(|a, b| a.name.cmp(&b.name));
                let mut text = String::with_capacity(self.out.len() - start);
                let mut first = true;
                for (i, member) in members.iter().enumerate() {
                    if members
                        .get(i + 1)
                        .is_some_and(|next| next.name == member.name)
                    {
                        continue;
                    }
                    if !first {
                        text.push(',');
                    }
                    first = false;
                    text.push_str(&self.out[member.start..member.end]);
                }
                self.out.truncate(start);
                self.out.push_str(&text);
            }
        }
        self.out.push('}');
        Ok(())
    }

    fn begin_array(&mut self, _items: usize) -> std::result::Result<(), oracledb::Error> {
        self.value();
        self.out.push('[');
        self.containers.push(Container::Array { items: 0 });
        Ok(())
    }

    fn end_array(&mut self) -> std::result::Result<(), oracledb::Error> {
        self.containers.pop();
        self.out.push(']');
        Ok(())
    }

    fn scalar(&mut self, value: JsonScalar<'_>) -> std::result::Result<(), oracledb::Error> {
        self.value();
        write_json_scalar(&mut self.out, &value);
        Ok(())
    }
}

pub(crate) fn write_vector(out: &mut String, vector: &oracledb::Vector) {
    use oracledb::{Vector, VectorData};
    fn list<T: std::fmt::Display>(out: &mut String, items: &[T]) {
        out.push('[');
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{item}");
        }
        out.push(']');
    }
    match vector {
        Vector::Dense(VectorData::Float32(v)) => list(out, v),
        Vector::Dense(VectorData::Float64(v)) => list(out, v),
        Vector::Dense(VectorData::Int8(v)) => list(out, v),
        Vector::Dense(VectorData::Binary(v)) => list(out, v),
        Vector::Sparse(_) => out.push_str("null"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Puts an object's member names in the order an OSON decoder visits them.
    type MemberOrder = dyn Fn(&mut Vec<&String>);

    /// Visits a JsonValue as an OSON decoder would, with object members in
    /// the given order.
    fn visit(writer: &mut JsonTextWriter, value: &JsonValue, order: &MemberOrder) {
        use oracledb::JsonVisitor;
        match value {
            JsonValue::JsonObject(map) => {
                writer.begin_object(map.len()).unwrap();
                let mut keys: Vec<&String> = map.keys().collect();
                order(&mut keys);
                for key in keys {
                    writer.field_name(key).unwrap();
                    visit(writer, &map[key], order);
                }
                writer.end_object().unwrap();
            }
            JsonValue::JsonArray(items) => {
                writer.begin_array(items.len()).unwrap();
                for item in items {
                    visit(writer, item, order);
                }
                writer.end_array().unwrap();
            }
            JsonValue::String(s) => writer.scalar(JsonScalar::String(s)).unwrap(),
            JsonValue::Number(n) => writer.scalar(JsonScalar::Number(*n)).unwrap(),
            JsonValue::Boolean(b) => writer.scalar(JsonScalar::Boolean(*b)).unwrap(),
            JsonValue::Null => writer.scalar(JsonScalar::Null).unwrap(),
            JsonValue::BinaryDouble(v) => writer.scalar(JsonScalar::BinaryDouble(*v)).unwrap(),
            JsonValue::Raw(b) => writer.scalar(JsonScalar::Raw(b)).unwrap(),
            JsonValue::Timestamp(t) => writer.scalar(JsonScalar::Timestamp(*t)).unwrap(),
            other => panic!("not used here: {other:?}"),
        }
    }

    #[test]
    fn json_text_writer_writes_what_json_to_string_does() {
        use std::collections::HashMap;
        let n = |s: &str| JsonValue::Number(s.parse().unwrap());
        let object = |pairs: Vec<(&str, JsonValue)>| {
            JsonValue::JsonObject(
                pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect::<HashMap<_, _>>(),
            )
        };
        let doc = object(vec![
            ("zeta", n("1")),
            (
                "alpha",
                JsonValue::String("quote \" back \\ tab \t é 😀".into()),
            ),
            (
                "mid",
                JsonValue::JsonArray(vec![
                    object(vec![
                        ("b", JsonValue::Null),
                        ("a", JsonValue::Boolean(true)),
                    ]),
                    JsonValue::JsonArray(vec![]),
                    object(vec![]),
                    n("-12.5"),
                    JsonValue::BinaryDouble(1.25),
                    JsonValue::Raw(vec![0, 255, 16]),
                    JsonValue::Timestamp(OracleTimestamp::new_timestamp(2025, 3, 4, 5, 6, 7, 8)),
                ]),
            ),
            ("", JsonValue::String(String::new())),
            ("Beta", n("0.000000000000000000000000000000001")),
        ]);
        let expected = json_to_string(&doc);
        // Members in the order the map gives them, reversed, and sorted.
        let orders: [&MemberOrder; 3] = [&|_| {}, &|keys| keys.reverse(), &|keys| keys.sort()];
        for order in orders {
            let mut writer = JsonTextWriter::default();
            visit(&mut writer, &doc, order);
            assert_eq!(writer.out, expected);
        }
        // A scalar document.
        let mut writer = JsonTextWriter::default();
        visit(&mut writer, &n("42"), &|_| {});
        assert_eq!(writer.out, "42");
    }

    #[test]
    fn json_text_writer_keeps_the_last_of_a_repeated_name() {
        use oracledb::JsonVisitor;
        let mut writer = JsonTextWriter::default();
        writer.begin_object(3).unwrap();
        for (name, value) in [("b", "1"), ("a", "2"), ("b", "3")] {
            writer.field_name(name).unwrap();
            writer
                .scalar(JsonScalar::Number(value.parse().unwrap()))
                .unwrap();
        }
        writer.end_object().unwrap();
        assert_eq!(writer.out, r#"{"a":2,"b":3}"#);
    }

    #[test]
    fn epoch_and_modern_dates() {
        let ts = OracleTimestamp::new_timestamp(1970, 1, 1, 0, 0, 0, 0);
        assert_eq!(timestamp_to_arrow(&ts, TimeUnit::Second).unwrap(), 0);
        let ts = OracleTimestamp::new_timestamp(2025, 1, 2, 3, 4, 5, 123_456_789);
        assert_eq!(
            timestamp_to_arrow(&ts, TimeUnit::Microsecond).unwrap(),
            1_735_787_045_123_456
        );
        assert_eq!(
            timestamp_to_arrow(&ts, TimeUnit::Nanosecond).unwrap(),
            1_735_787_045_123_456_789
        );
    }

    #[test]
    fn julian_calendar_before_gregorian_reform() {
        // 1582-10-04 (Julian) is the day before 1582-10-15 (Gregorian).
        let before = OracleTimestamp::new_date(1582, 10, 4);
        let after = OracleTimestamp::new_date(1582, 10, 15);
        let b = timestamp_to_arrow(&before, TimeUnit::Second).unwrap();
        let a = timestamp_to_arrow(&after, TimeUnit::Second).unwrap();
        assert_eq!(a - b, SECONDS_PER_DAY);
    }

    /// Days since 1970-01-01 as computed by Oracle Database itself
    /// (`TO_DATE(s, 'SYYYY-MM-DD') - DATE '1970-01-01'`).
    const ORACLE_DAYS: &[((i16, u8, u8), i64)] = &[
        ((-4712, 1, 1), -2_440_587),
        ((-4712, 3, 1), -2_440_528),
        ((-4711, 1, 1), -2_440_222),
        ((-5, 3, 1), -721_297),
        ((-4, 2, 28), -720_933),
        ((-4, 3, 1), -720_931),
        ((-1, 1, 1), -719_895),
        ((-1, 12, 31), -719_531),
        ((1, 1, 1), -719_164),
        ((4, 3, 1), -718_009),
        ((1582, 10, 4), -141_428),
        ((1582, 10, 15), -141_427),
        ((1970, 1, 1), 0),
    ];

    #[test]
    fn matches_oracle_date_arithmetic() {
        for &((y, m, d), days) in ORACLE_DAYS {
            let ts = OracleTimestamp::new_date(y, m, d);
            assert_eq!(
                timestamp_to_arrow(&ts, TimeUnit::Second).unwrap(),
                days * SECONDS_PER_DAY,
                "{y}-{m}-{d}"
            );
            let back = arrow_to_timestamp(days * SECONDS_PER_DAY, TimeUnit::Second, false).unwrap();
            assert_eq!((back.year(), back.month(), back.day()), (y, m, d));
        }
    }

    #[test]
    fn rejects_dates_outside_oracle_range() {
        let before_first = (ORACLE_DAYS[0].1 - 1) * SECONDS_PER_DAY;
        assert!(arrow_to_timestamp(before_first, TimeUnit::Second, false).is_err());
        // Year 0 exists in Oracle arithmetic but cannot be bound.
        let year_zero = (ORACLE_DAYS[7].1 + 1) * SECONDS_PER_DAY;
        assert!(arrow_to_timestamp(year_zero, TimeUnit::Second, false).is_err());
    }

    #[test]
    fn timestamp_round_trips() {
        for (y, m, d) in [
            (-4712, 1, 1),
            (-4712, 2, 28),
            (-4712, 3, 1),
            (-1, 12, 31),
            (1, 1, 1),
            (1582, 10, 4),
            (1582, 10, 15),
            (1900, 2, 28),
            (2000, 2, 29),
            (9999, 12, 31),
        ] {
            let ts = OracleTimestamp::new_timestamp(y, m, d, 23, 59, 58, 999_999_000);
            let v = timestamp_to_arrow(&ts, TimeUnit::Microsecond).unwrap();
            let back = arrow_to_timestamp(v, TimeUnit::Microsecond, false).unwrap();
            assert_eq!(
                (
                    back.year(),
                    back.month(),
                    back.day(),
                    back.hour(),
                    back.second()
                ),
                (y, m, d, 23, 58),
                "round trip of {y}-{m}-{d}"
            );
            assert_eq!(back.nanoseconds(), 999_999_000);
        }
    }

    #[test]
    fn nanosecond_overflow_is_an_error() {
        let ts = OracleTimestamp::new_timestamp(9999, 12, 31, 0, 0, 0, 0);
        assert!(timestamp_to_arrow(&ts, TimeUnit::Nanosecond).is_err());
    }

    #[test]
    fn decimal_conversion_is_exact() {
        assert_eq!(unscaled_digits("123.45", 10, 2).unwrap(), "12345");
        assert_eq!(unscaled_digits("-0.5", 10, 3).unwrap(), "-500");
        assert_eq!(unscaled_digits("0", 5, 2).unwrap(), "0");
        assert_eq!(unscaled_digits("1200", 4, 0).unwrap(), "1200");
        assert_eq!(unscaled_digits("7.100", 3, 1).unwrap(), "71");
        assert!(unscaled_digits("1.234", 10, 2).is_err(), "would round");
        assert!(unscaled_digits("12345", 4, 0).is_err(), "overflow");
        let big = "9".repeat(38);
        assert_eq!(unscaled_digits(&big, 38, 0).unwrap(), big);
    }

    #[test]
    fn json_serialization() {
        use std::collections::HashMap;
        let value = JsonValue::JsonObject(HashMap::from([
            (
                "b".to_string(),
                JsonValue::JsonArray(vec![JsonValue::Boolean(true), JsonValue::Null]),
            ),
            ("a".to_string(), JsonValue::String("x\"y\n".to_string())),
            ("n".to_string(), JsonValue::Number("1.5".parse().unwrap())),
            ("f".to_string(), JsonValue::BinaryDouble(f64::NAN)),
        ]));
        assert_eq!(
            json_to_string(&value),
            r#"{"a":"x\"y\n","b":[true,null],"f":null,"n":1.5}"#
        );
    }

    #[test]
    fn intervals() {
        let ds = OracleIntervalDS::new(1, 2, 3, 4, 500_000_000);
        assert_eq!(
            interval_ds_to_arrow(&ds, TimeUnit::Millisecond).unwrap(),
            ((86_400 + 2 * 3600 + 3 * 60 + 4) * 1000) + 500
        );
        let ym = OracleIntervalYM::new(2, 3);
        assert_eq!(interval_ym_to_months(&ym).unwrap(), 27);
    }
}
