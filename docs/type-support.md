# Type Support

## Query results (Oracle → Arrow)

Results are streamed from Oracle cursors into Arrow record batches of at most
`oracle.batch_size` rows (default 8192). Memory use is bounded by one batch,
whatever the size of the result.

Every field carries the Oracle declared type in its `oracle.type-name`
metadata (for example `NUMBER(10,2)`). `execute_schema` and
`get_table_schema` use the same mapping as `execute`, so they always report the
schema that `execute` produces.

| Oracle type | Arrow type | Notes |
| --- | --- | --- |
| `NUMBER(p)` with p ≤ 2 / 4 / 9 / 18 | `Int8` / `Int16` / `Int32` / `Int64` | |
| `NUMBER(p)` with p > 18 | `Decimal128(p, 0)` | `Decimal256` above 38 digits |
| `NUMBER(p,s)` with 0 < s ≤ p | `Decimal128(p, s)` | Exact |
| `NUMBER(p,s)` with s > p | `Decimal128(s, s)` | For example `NUMBER(3,5)` holds `0.00123` |
| `NUMBER(p,s)` with s < 0 | `Decimal128(p - s, 0)` | Integers rounded to the left of the decimal point |
| `NUMBER`, `FLOAT`, computed numbers | `Float64` by default | Configurable; see below |
| `BINARY_FLOAT` / `BINARY_DOUBLE` | `Float32` / `Float64` | |
| `BOOLEAN` (23ai) | `Boolean` | |
| `VARCHAR2`, `NVARCHAR2`, `CHAR`, `NCHAR` | `Utf8` | `CHAR` keeps Oracle's blank padding |
| `CLOB`, `NCLOB`, `LONG` | `LargeUtf8` | |
| `RAW` | `Binary` | |
| `BLOB`, `LONG RAW` | `LargeBinary` | |
| `ROWID`, `UROWID` | `Utf8` | Oracle's text form |
| `DATE` | `Timestamp(s)` | Oracle `DATE` has a time-of-day component |
| `TIMESTAMP(f)` | `Timestamp(unit)` | f = 0 → s, 1–3 → ms, 4–6 → µs, 7–9 → ns |
| `TIMESTAMP(f) WITH TIME ZONE` | `Timestamp(unit, "UTC")` | The UTC instant; the original offset is not preserved |
| `TIMESTAMP(f) WITH LOCAL TIME ZONE` | `Timestamp(unit)` | Wall-clock time in the session time zone |
| `INTERVAL DAY TO SECOND` | `Duration(unit)` | Unit from the fractional-second precision |
| `INTERVAL YEAR TO MONTH` | `Interval(YearMonth)` | |
| `JSON` (21c+) | `Utf8` with the `arrow.json` extension | Object keys are emitted in sorted order |
| `VECTOR(n, FLOAT32/FLOAT64/INT8)` | `FixedSizeList<Float32/Float64/Int8>(n)` | |
| `VECTOR(n, BINARY)` | `FixedSizeList<UInt8>(n / 8)` | Packed bits |
| `VECTOR(*, ...)`, `VECTOR(*, *)` | `List<...>`, `List<Float64>` | Flexible vectors |

Oracle reports maximum precision for expressions. For example,
`CAST(x AS TIMESTAMP(0))` in a `SELECT` list is described as `TIMESTAMP(9)`,
so precision-based mappings apply to columns as declared.

### Dates before 1582 and BC dates

Conversions follow Oracle's own date arithmetic, so the difference between two
converted values always equals `d2 - d1` computed in Oracle:

- Dates before 1582-10-15 use the Julian calendar.
- BC years are numbered astronomically.
- Oracle's year -4712 has no 29 February.

The test suite checks this against Oracle Database for dates from
-4712-01-01 to 9999-12-31.

`TIMESTAMP(7..9)` values outside 1677–2262 do not fit Arrow nanosecond
timestamps. They raise an error asking you to select a lower precision, rather
than wrapping around.

### Unconstrained `NUMBER`

`NUMBER` columns without a declared precision, `FLOAT`, and computed numeric
expressions (`COUNT(*)`, `SUM(x)`, `1 + 1`) carry no precision metadata.
`oracle.unconstrained_number` selects how they are read:

| Value | Arrow type | Behaviour |
| --- | --- | --- |
| `float64` (default) | `Float64` | Fast; values beyond ~15 significant digits lose precision. Matches python-oracledb. |
| `decimal` | `Decimal128(38, oracle.unconstrained_number_scale)` | Exact. A value that would need rounding or more than 38 digits raises an error instead of being altered. Scale defaults to 10. |
| `string` | `Utf8` | The exact decimal text. |

The option can be set on the database (the default for all statements) or per
statement. Casting in SQL, such as `CAST(x AS NUMBER(20,4))`, gives exact
control per column.

### Not supported yet

`XMLTYPE`, `BFILE`, object and collection types, cursor expressions, and
sparse vectors fail before any data is fetched. The error message suggests a
SQL workaround, for example `XMLSERIALIZE(CONTENT col AS CLOB)` for `XMLTYPE`.

## Query parameters (Arrow → Oracle)

A batch bound to a query is executed once per row, and the results are
concatenated. Supported parameter types:

- integers, floats, `Boolean`, `Decimal128`/`Decimal256`
- `Utf8`, `LargeUtf8`, `Utf8View`
- `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary`
- `Date32`, `Date64`, `Timestamp`
- `Duration` (as `INTERVAL DAY TO SECOND`) and `Interval(YearMonth)`
- `Null`

Timestamps are bound as their UTC wall-clock value. A zone-aware Arrow
timestamp compared with a `TIMESTAMP WITH TIME ZONE` column is therefore
interpreted in the session time zone; normalise the session time zone to UTC
when this matters.

## Fetch tuning

| Option | Default | Effect |
| --- | --- | --- |
| `oracle.batch_size` | 8192 | Maximum rows per Arrow record batch |
| `oracle.fetch_array_size` | 100 (oracledb) | Rows fetched per network round trip |
| `oracle.prefetch_rows` | 2 (oracledb) | Rows returned with the execute response |

All three can be set on the database or per statement. Integer options may
be given as integers or as strings, since many ADBC clients pass every option
as a string.

## DuckDB Integration

The DuckDB table function maps primitive Arrow values, strings, large strings,
binary values, timestamps, and decimals to DuckDB vectors.
