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
| `CLOB`, `NCLOB`, `LONG` | `LargeUtf8` | An empty LOB reads as NULL; see below |
| `RAW` | `Binary` | |
| `BLOB`, `LONG RAW` | `LargeBinary` | An empty LOB reads as NULL; see below |
| `ROWID`, `UROWID` | `Utf8` | Oracle's text form |
| `DATE` | `Timestamp(s)` | Oracle `DATE` has a time-of-day component |
| `TIMESTAMP(f)` | `Timestamp(unit)` | f = 0 → s, 1–3 → ms, 4–6 → µs, 7–9 → ns |
| `TIMESTAMP(f) WITH TIME ZONE` | `Timestamp(unit, "UTC")` | The UTC instant; the original offset is not preserved |
| `TIMESTAMP(f) WITH LOCAL TIME ZONE` | `Timestamp(unit, "UTC")` | The instant, in UTC; needs a database time zone set as an offset (`DBTIMEZONE` such as `+00:00`), not a region name |
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

### Empty LOBs

LOB values are fetched inline, with the row, which is much faster than
fetching a LOB locator and reading each value separately. An inline value
cannot tell an empty LOB (`EMPTY_CLOB()`, `EMPTY_BLOB()`) from NULL, so an
empty LOB reads as NULL, as with python-oracledb's default. To tell them
apart, select `DBMS_LOB.GETLENGTH(col)` alongside the column: it is `0` for
an empty LOB and NULL for a NULL one.

### Not supported yet

`XMLTYPE`, `BFILE`, object and collection types, cursor expressions, and
sparse vectors fail before any data is fetched. The error message suggests a
SQL workaround, for example `XMLSERIALIZE(CONTENT col AS CLOB)` for `XMLTYPE`.

## Query parameters (Arrow → Oracle)

A batch bound to a query is executed once per row, and the results are
concatenated. Parameters bind by name when every placeholder name matches a
field name, otherwise by position; see [plsql.md](plsql.md#binding-parameters).
Supported parameter types:

- integers, floats, `Boolean`, `Decimal128`/`Decimal256`
- `Utf8`, `LargeUtf8`, `Utf8View`
- `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary`
- `Date32`, `Date64`, `Timestamp`
- `Duration` (as `INTERVAL DAY TO SECOND`) and `Interval(YearMonth)`
- `FixedSizeList` / `List` of `Float32`, `Float64`, `Int8`, `UInt8` (as `VECTOR`)
- `Null`, optionally typed with `oracle.bind_type` field metadata

Timestamps are bound as their UTC wall-clock value. A zone-aware Arrow
timestamp compared with a `TIMESTAMP WITH TIME ZONE` column is therefore
interpreted in the session time zone; normalise the session time zone to UTC
when this matters.

## Bulk ingestion (Arrow → Oracle)

`adbc.ingest.mode` supports `create`, `append`, `create_append`, and `replace`.
Data can be bound as one batch (`bind`) or as a stream (`bind_stream`).
Streams are read batch by batch while rows are inserted, never all at once.
Rows go to Oracle in array-DML chunks of `oracle.batch_size` rows.

### Guarantees

- **No partial ingests.** Rows are inserted under a savepoint. If any row
  fails, every row of that ingest is rolled back, and the error names the
  failing range (for example `inserting rows 8192..16384 failed: ORA-12899
  ...`). An earlier transaction on the connection is not affected.
- **No empty leftovers.** If `create` fails while loading, the table it just
  created is dropped again, so a retry does not fail with "already exists".
- **Safe `replace`.** The new data is loaded into a staging table first and
  swapped in only once every row is loaded. If loading fails, the original
  table and its data are untouched. The replaced table goes to the recycle
  bin (`FLASHBACK TABLE ... TO BEFORE DROP`), not `PURGE`. Grants, indexes,
  and triggers on the old table are not carried over, as with any
  drop-and-recreate.
- **Type checks first.** Arrow types are validated before any DDL runs.
- Oracle DDL commits implicitly. `create` and `replace` therefore commit any
  transaction already open on the connection, even with autocommit off.

### Temporary tables

With `adbc.ingest.temporary = true`:

| Target table name | Oracle object | Lifetime |
| --- | --- | --- |
| Starts with `ORA$PTT_` | Private temporary table (18c+), `ON COMMIT PRESERVE DEFINITION` | Table and rows are private to the session and dropped when it ends. No LOB columns and no NOT NULL constraints (an Oracle restriction). |
| Any other name | Global temporary table, `ON COMMIT PRESERVE ROWS` | Rows are private to the session and survive commits. The table definition is permanent. |

### Column names and appends

Table and column names follow Oracle's name resolution. A name that is valid
unquoted (`customers`, `order_id`) is upper-cased, so it can be queried without
quotes. Any other name (`Total Amount`) is kept exactly. A name in double
quotes (`"MyTable"`, `"Id"`) is taken exactly too, as in Oracle SQL: this is
how to target a mixed-case table or column whose name would also be valid
unquoted. The same applies to the schema, and to the table name given to
`get_table_schema`. `append` matches
columns by these names, in any order. Table columns missing from the batch
receive their defaults. A batch column the table lacks, or a type that cannot
be stored in the target column, fails with `ALREADY_EXISTS` before any row is
inserted, as ADBC specifies.

### Column types for `create`

| Arrow type | Oracle column |
| --- | --- |
| `Int8`/`UInt8`, `Int16`/`UInt16`, `Int32`/`UInt32` | `NUMBER(3)`, `NUMBER(5)`, `NUMBER(10)` |
| `Int64`, `UInt64` | `NUMBER(19)`, `NUMBER(20)` |
| `Float16`/`Float32`, `Float64` | `BINARY_FLOAT`, `BINARY_DOUBLE` |
| `Decimal128(p, s)` | `NUMBER(p, s)` |
| `Boolean` | `BOOLEAN` on 23ai and later, otherwise `NUMBER(1)` |
| `Utf8`, `Utf8View` | `VARCHAR2(4000)` |
| `LargeUtf8` | `CLOB` |
| `Binary`, `BinaryView` | `RAW(2000)` |
| `FixedSizeBinary(n)` | `RAW(n)`, or `BLOB` when n > 2000 |
| `LargeBinary` | `BLOB` |
| `Date32`, `Date64` | `DATE` |
| `Timestamp(unit)` | `TIMESTAMP(0/3/6/9)` |
| `Timestamp(unit, tz)` | `TIMESTAMP(0/3/6/9) WITH TIME ZONE` |
| `Duration(unit)` | `INTERVAL DAY(9) TO SECOND(0/3/6/9)` |
| `Interval(YearMonth)` | `INTERVAL YEAR(9) TO MONTH` |
| `FixedSizeList<Float32/Float64/Int8>(n)` | `VECTOR(n, FLOAT32/FLOAT64/INT8)` |
| `FixedSizeList<UInt8>(n)` | `VECTOR(n × 8, BINARY)` |
| `List<...>` of those types | `VECTOR(*, ...)` |

Zone-aware timestamps are inserted through `FROM_TZ(..., 'UTC')`, so the
stored instant is correct whatever the session time zone. Columns the Arrow
bind path cannot carry (vectors, intervals, durations) are bound row by row
but still sent in one round trip per chunk. Decimals above 38 digits,
dictionary columns, and other nested types are rejected before any DDL; cast
them first.

## Fetch tuning

| Option | Default | Effect |
| --- | --- | --- |
| `oracle.batch_size` | 8192 | Maximum rows per Arrow record batch |
| `oracle.fetch_array_size` | `oracle.batch_size`, at most 10000 | Rows fetched per network round trip |
| `oracle.prefetch_rows` | 2 (oracledb) | Rows returned with the execute response |

All three can be set on the database or per statement. Integer options may
be given as integers or as strings, since many ADBC clients pass every option
as a string.

## DuckDB Integration

The DuckDB table function maps primitive Arrow values, strings, large strings,
binary values, timestamps, and decimals to DuckDB vectors.
