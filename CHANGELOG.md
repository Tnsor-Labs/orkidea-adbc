# Changelog

All notable changes to Orkidea ADBC are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/). Before
1.0, a minor release may change the public API or a type mapping; such
changes are listed under "Behaviour changes".

Every entry names its owner by GitHub handle. The section for a version is
published as its GitHub release notes, so it is written before the tag is
pushed (see [docs/release.md](docs/release.md)).

## [Unreleased]

### Queries and types

- **Intervals and ROWIDs are decoded straight into Arrow columns too** --
  @hc12r. `INTERVAL DAY TO SECOND`, `INTERVAL YEAR TO MONTH`, `ROWID`, and
  `UROWID` columns no longer make a query read row by row. `JSON`, `VECTOR`,
  and object columns still do.

## [0.3.0] - 2026-11-10

Orkidea 0.3.0 reads results two to three times as fast: values go from the
network buffer straight into Arrow columns instead of through a row object
each. It reads the same batches as 0.2.1, which a new test checks type by
type, and is tested in Rust and Python against Oracle Database 23ai Free.

### Behaviour changes to read before upgrading

None: results, types, batch sizes, and errors are those of 0.2.1. If you
suspect the new reader, `oracle.columnar_fetch = false` reads results as
0.2.1 did.

### Queries and types

- **Results are decoded straight into Arrow columns** -- @hc12r. Query
  values were decoded into a row object each, then converted one by one,
  integers and decimals through their text. They are now decoded from the
  network buffer into column buffers, and `NUMBER` into integers and
  decimals from its bytes. Reading the lab's million-row, seven-column
  `ORDERS` table takes 1.2 s instead of 2.9 s, and 0.7 s of CPU instead of
  2.3 s. This covers numbers, text, `RAW`, `BOOLEAN`, binary floats, dates,
  timestamps, and LOBs; a query with any other column is read as before.
  Results are the same: a test reads every covered type both ways and
  compares the batches. `oracle.columnar_fetch = false` turns it off. It
  needs `oracledb`'s row sinks (`Cursor::fetch_into`), in the Tnsor Labs
  fork until an Oracle release has them.

## [0.2.1] - 2026-11-10

Orkidea 0.2.1 fixes one bug: a second DB-API cursor running a query with
parameters, after a first had read it to the end, failed with ORA-01008.
Only clients that prepare statements are affected; the DuckDB extension is
not. The lab of the DuckDB extension found it, and it is tested in Rust and
Python against Oracle Database 23ai Free.

### Behaviour changes to read before upgrading

None.

### Fixed

- **A second cursor can run the same query with parameters** -- @hc12r.
  Once a query with parameters had been fetched to the end, preparing the
  same SQL again on the connection, as every new DB-API cursor does, made
  its execution fail with "ORA-01008: value for bind variable placeholder
  was not provided". `oracledb` 26.0.0-beta.5 closes the cursor of a fully
  fetched query, and the prepare then parsed the cached statement without
  its binds; this is fixed in `oracledb` after beta.5. `prepare()` now
  parses outside the statement cache, so the statement that executes is
  never changed by it. Present since 0.1.0.

### Documentation

- **Positional binding is described as it works** -- @hc12r. The Python
  package's README and `dbapi` docstring said a sequence binds in order of
  each placeholder's first appearance. In SQL it takes one value per
  placeholder in the order they appear, so a repeated name takes a value for
  each use; in PL/SQL, one value per name.

## [0.2.0] - 2026-11-10

Orkidea 0.2.0 reads faster and fixes what the DuckDB extension built on it
found: wide rows that could break a connection, schemas described from a
stale cache, quoted names, `tnsnames.ora` aliases after a comment, and SQL
that binds a placeholder more than once. Two of the fixes are in `oracledb`
itself; until a release of it includes them, Orkidea builds with `oracledb`
26.0.0-beta.5 plus those fixes, from
[Tnsor-Labs/rust-oracledb](https://github.com/Tnsor-Labs/rust-oracledb),
pinned by commit.

It is tested as 0.1.0 was, against Oracle Database Free 23ai, with new live
tests for each fix, including a seeded test of 30 random wide tables.

### Behaviour changes to read before upgrading

- **Reads fetch a batch per round trip** -- @hc12r. Unless
  `oracle.fetch_array_size` is set, each round trip now fetches
  `oracle.batch_size` rows (at most 10000) instead of `oracledb`'s default of
  100. Reading 2 million rows takes 5.1 s instead of 7.9 s. Each round
  trip holds up to one batch of rows in memory; set
  `oracle.fetch_array_size` to fetch less at a time. Reading the option
  back returns the value in effect instead of `NOT_FOUND`.

### Queries and types

- **A re-created table is described as it is now** -- @hc12r.
  `execute_schema`, `prepare`, and `get_table_schema` reused the column
  metadata of a cached statement with the same SQL, so after a table was
  dropped and created again, or altered, they described its old columns.
  Describing now bypasses the statement cache; executing a query was not
  affected, because Oracle re-parses an invalidated cursor. Reported to
  `oracledb` as
  [oracle/rust-oracledb#84](https://github.com/oracle/rust-oracledb/issues/84).
- **Wide rows no longer corrupt the connection** -- @hc12r. Reading a row of
  more than eight columns whose values partly repeat the previous row's could
  fail with "unknown TTC message type", return an error naming the wrong
  type, or panic, and left the connection unusable. It happened after
  `prepare()` (which every DB-API `execute` calls), on a statement cache hit,
  and in about one table in four of a random sample. The cause was in
  `oracledb`, which sized the row's bit vector from the wrong column count
  ([oracle/rust-oracledb#80](https://github.com/oracle/rust-oracledb/issues/80),
  fixed upstream). A seeded test of 30 random wide tables now runs in CI.
- **Empty LOBs read as NULL** -- @hc12r. LOBs are fetched inline, which
  cannot tell `EMPTY_CLOB()` from NULL. This is not new; it is now
  documented in [docs/type-support.md](docs/type-support.md#empty-lobs).

### Ingestion and catalog

- **Quoted names target exact objects** -- @hc12r. A table, schema, or column
  name in double quotes (`"MyTable"`) is taken exactly by ingest and
  `get_table_schema`, as in Oracle SQL. Before, quoted names failed with
  "name has embedded quotes", so a mixed-case table whose name would also be
  valid unquoted could not be appended to. A quoted name is no longer split
  at a dot inside the quotes (`"a.b"`). Unquoted names are upper-cased, as
  before.

### Parameters and PL/SQL

- **A placeholder used twice in SQL binds by name** -- @hc12r. Bound by name
  (a DB-API `dict`, or `adbc.statement.bind_by_name`), SQL that used a
  placeholder more than once, as in `WHERE a = :v OR b = :v`, failed with
  "2 positional bind values are required but 1 were provided". In SQL each
  use is a bind of its own, and the driver supplied one value per name. It
  now binds the field to every use, for queries, DML, and `RETURNING`.
  Binding by position, and PL/SQL, are unchanged.

### Connections

- **`tnsnames.ora` aliases after a comment are found** -- @hc12r. An alias
  written on the line after a `#` comment, as in most `tnsnames.ora` files,
  failed with "unable to find" the alias: `oracledb`'s parser dropped the
  first character of the line after a comment
  ([oracle/rust-oracledb#83](https://github.com/oracle/rust-oracledb/issues/83)).
  A live test connects through an alias written that way.

### Known limitations

Unchanged from 0.1.0.

## [0.1.0] - 2026-11-09

The first release of Orkidea, an ADBC driver for Oracle Database written in
Rust. Query results arrive as Arrow record batches, streamed in bounded
batches. The driver speaks Oracle's network protocol directly, so it needs no
Oracle Instant Client, OCI, or ODBC. It is published for Linux, macOS, and
Windows on x86_64 and arm64, as release archives for any ADBC client and as
Python wheels on PyPI (`pip install "adbc-driver-orkidea[dbapi]"`).

The driver is tested against Oracle Database Free 23ai: the Rust suite, and
the Python suite with the oldest and newest supported Python dependencies.
Every release build is also loaded and tested on its own platform, and the
Linux builds on Rocky Linux 8, Debian 11, and Amazon Linux 2023.

### Behaviour changes to read before upgrading

None: this is the first release. Until 1.0, type mappings and options may
still change between minor releases; each change will be listed here.

### Queries and types

- **Streaming results** -- @hc12r. A query is read in batches of
  `oracle.batch_size` rows (default 8192) as the client consumes them, so a
  large result is never held in memory at once. Fetch round trips are tuned
  with `oracle.fetch_array_size` and `oracle.prefetch_rows`.
- **Exact type mapping** -- @hc12r. `NUMBER(p,s)` becomes the narrowest
  integer or an exact decimal; `TIMESTAMP WITH TIME ZONE` becomes a UTC
  instant; intervals, `JSON` (with the `arrow.json` extension), `VECTOR`,
  `BOOLEAN`, LOBs, and `ROWID` all have a mapping. Dates before 1582 and BC
  dates follow Oracle's own calendar, checked against the database's date
  arithmetic. Every column carries its declared Oracle type in field metadata
  (`oracle.type-name`). See [docs/type-support.md](docs/type-support.md).
- **Unconstrained `NUMBER`** -- @hc12r. `NUMBER` without a precision reads as
  `float64` by default, like python-oracledb, or as an exact `decimal` or as
  `string` with `oracle.unconstrained_number`. The decimal mode fails rather
  than round a value.
- **Unsupported types fail early** -- @hc12r. `XMLTYPE`, `BFILE`, object and
  collection types fail before any row is fetched, with a SQL workaround in
  the message.
- **Schema without execution** -- @hc12r. `execute_schema` reports exactly the
  schema `execute` would produce.

### Parameters and PL/SQL

- **Parameterized statements** -- @hc12r. A bound batch runs the statement
  once per row; DML runs as array DML in chunks. Parameters bind by name when
  every placeholder matches a field name, otherwise by position, and
  `adbc.statement.bind_by_name` makes the choice explicit.
- **PL/SQL outputs** -- @hc12r. OUT and IN OUT parameters come back as a
  result row, a REF CURSOR OUT parameter streams as the result set, and
  `RETURNING ... INTO` returns the affected rows. Field metadata
  `oracle.bind_type` gives a NULL OUT parameter its Oracle type. See
  [docs/plsql.md](docs/plsql.md).
- **Vectors** -- @hc12r. `VECTOR` parameters bind from Arrow lists, for
  similarity search and embedding loads.

### Bulk ingestion

- **All four ingest modes** -- @hc12r. `create`, `append`, `create_append`,
  and `replace`, from a batch or a stream that is read as rows are inserted.
- **All or nothing** -- @hc12r. A failed ingest rolls back every row it
  inserted and names the failing row range. A failed `create` drops the table
  it created, and `replace` swaps the new table in only after a complete
  load, leaving the original untouched on failure.
- **Temporary tables** -- @hc12r. Temporary ingests use global temporary
  tables, or private temporary tables for `ORA$PTT_` names.

### Catalog metadata

- **`get_objects` at every depth** -- @hc12r. Schemas, tables, views,
  materialized views, and temporary tables; columns with ODBC-style metadata
  (sizes, defaults, comments, identity and virtual columns); and primary key,
  unique, foreign key, and check constraints. See
  [docs/catalog.md](docs/catalog.md).
- **`get_statistics`** -- @hc12r, from Oracle optimizer statistics: row count,
  distinct count, null count, and average width.
- **`get_info`, `get_table_schema`, and `get_table_types`** -- @hc12r,
  including the server version and the connected container as the current
  catalog.

### Partitioned reads

- **`execute_partitions`** -- @hc12r. A query splits into up to 4096 hash
  partitions on a chosen column, and any connection, thread, process, or
  machine can read a partition. With `oracle.partition.consistent`, every
  partition reads one snapshot through Flashback Query. See
  [docs/partitions.md](docs/partitions.md).

### Connections and options

- **Connecting** -- @hc12r. `oracle://` and `oracle+tcps://` URIs, Easy
  Connect strings, TNS descriptors and `tnsnames.ora` aliases, PEM wallets
  for TLS and mutual TLS, DRCP connection classes, privileged connections
  (`sysdba` and others), and password change on connect.
- **Session and tracing** -- @hc12r. Session time zone, edition, client
  identity in `V$SESSION`, end-to-end tracing attributes that can change at
  any time, call and connect timeouts, transactions with autocommit,
  read-only connections, and isolation levels. See
  [docs/options.md](docs/options.md).
- **Secrets stay secret** -- @hc12r. Passwords are write-only options, and
  reading `uri` back masks its password.

### Python

- **`adbc-driver-orkidea` on PyPI** -- @hc12r. A DB-API 2.0 interface
  (`adbc_driver_orkidea.dbapi`, with Oracle's `:name` placeholders) and the
  low-level `connect()`, with the driver bundled in the wheel. Option enums
  cover every driver option. One wheel per platform works with any Python
  3.10 or newer. The DB-API needs adbc-driver-manager 1.8 and pyarrow 16 or
  newer. See [python/README.md](python/README.md).

### Packaging and supply chain

- **Six platforms** -- @hc12r. Linux (glibc 2.28 and newer), macOS 11 and
  newer (ad-hoc signed), and Windows, each on x86_64 and arm64, built on
  native runners.
- **Installers** -- @hc12r. `install.sh` and `install.ps1` download a
  release, verify it, and register driver `orkidea` with the ADBC driver
  manager.
- **Verifiable releases** -- @hc12r. `SHA256SUMS` is signed keylessly with
  Sigstore, every asset has a GitHub build-provenance attestation and a
  CycloneDX SBOM, and the wheels carry PEP 740 attestations from PyPI.
  Archives and wheels include the license and notice files of every crate
  compiled into the driver. Debug symbols ship as separate archives.

### Known limitations

- The driver is built on the `oracledb` crate 26.0.0-beta.5, a beta.
- Statement cancellation and Substrait are not implemented; use
  `oracle.call_timeout_ms` to bound a call.
- Proxy authentication is unavailable because of an upstream logon bug
  ([oracle/rust-oracledb#56](https://github.com/oracle/rust-oracledb/issues/56)).
  Token, IAM, Kerberos, and external authentication, `cwallet.sso` wallets,
  and Easy Connect `?key=value` parameters are not supported by `oracledb`
  yet.
- A `VECTOR` parameter must follow every other placeholder unless it is small
  enough to send as text, because of an upstream bind bug
  ([oracle/rust-oracledb#79](https://github.com/oracle/rust-oracledb/issues/79)).
  Ingest orders vector columns itself.
- macOS libraries are not notarized. Installs through the installers or pip
  are not affected; a library downloaded with a browser may be blocked by
  Gatekeeper.
- pyarrow publishes no Windows arm64 wheels, so there the Python DB-API needs
  a pyarrow built from source. The low-level API works without it.

[Unreleased]: https://github.com/Tnsor-Labs/orkidea-adbc/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/Tnsor-Labs/orkidea-adbc/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/Tnsor-Labs/orkidea-adbc/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Tnsor-Labs/orkidea-adbc/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Tnsor-Labs/orkidea-adbc/releases/tag/v0.1.0
