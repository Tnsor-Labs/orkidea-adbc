# Development

## Local Build

```bash
cargo fmt --all
cargo check --workspace
cargo build --release -p orkidea-adbc-ffi
./scripts/build_duckdb_orkidea.sh
```

The Rust shared library is `target/release/libadbc_driver_orkidea.so`. The
DuckDB extension is produced under the DuckDB build directory as
`orkidea_adbc.duckdb_extension`.

## Tests

Rust tests do not require Oracle when `ORKIDEA_ADBC_URI` is absent; the
live integration tests (`oracle_smoke`, `fetch_live`) are skipped in that case.

```bash
cargo test --workspace
ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
  cargo test --workspace
```

`catalog_live` covers `get_objects` at every depth, column metadata,
constraints, filters, optimizer statistics, and the catalog option. It
creates and drops `ORKIDEA_C_*` objects and gathers statistics on them.

`plsql_live` covers OUT binds, REF CURSOR results, DML RETURNING and vector
parameters (needs `CREATE PROCEDURE`). `options_live` covers session and
tracing options and secret handling. Its SYSDBA test runs only when
`ORKIDEA_ADBC_ADMIN_URI` points at a SYS account. `partition_live` covers
partitioned reads; its consistency test needs
`GRANT EXECUTE ON DBMS_FLASHBACK`.

`ingest_live` covers every ingest mode, rollback on failure, safe `replace`,
global and private temporary tables, streaming `bind_stream`, time-zone
handling, and Oracle name resolution. It creates and drops tables named
`ORKIDEA_I_*`.

`fetch_live` creates and drops tables named `ORKIDEA_T_*` and needs
Oracle Database 23ai or later (BOOLEAN, JSON, and VECTOR columns). It covers
streaming, every supported type mapping, historical dates checked against
Oracle's own date arithmetic, parameterized queries, and schema consistency.

CI runs the full suite against Oracle Database Free in a container that is
started with a random, masked password. To reproduce it locally:

```bash
password="$(openssl rand -hex 16)"
docker run -d --rm --name orkidea-oracle -p 127.0.0.1:1521:1521 \
  -e ORACLE_PASSWORD="$password" \
  -e APP_USER=orkidea_test -e APP_USER_PASSWORD="$password" \
  gvenzl/oracle-free:23-slim-faststart
# wait for "DATABASE IS READY TO USE!" in `docker logs orkidea-oracle`
ORKIDEA_ADBC_URI="oracle://orkidea_test:$password@127.0.0.1:1521/FREEPDB1" \
  cargo test --workspace
```

## Python Package

The `adbc-driver-orkidea` package lives in `python/`. Its build backend
bundles the library from `target/release` (or `ORKIDEA_ADBC_LIBRARY`):

```bash
cargo build --release -p orkidea-adbc-ffi
python3 -m venv .venv
.venv/bin/pip install "./python[test]"
.venv/bin/python -I -m pytest python/tests
```

Run pytest with `-I`, so the tests import the installed package (with its
bundled library) rather than the source tree. `test_live.py` runs when
`ORKIDEA_ADBC_URI` is set; it creates and drops tables named
`ORKIDEA_PY_*`. There is no editable install: reinstall after changing the
package or rebuilding the library.

## DuckDB

The DuckDB integration test expects a user-supplied Oracle URI and a built
extension:

```bash
ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service' \
  ./scripts/test_duckdb_orkidea.sh
```

## Cancellation and Timeouts

`oracle.call_timeout_ms` configures the Oracle driver call timeout and is the
current safe cancellation mechanism. ADBC cancellation remains best-effort
until the underlying Rust Oracle driver exposes a non-blocking cancel handle.
Every query path releases its Arrow stream, statement, connection, and driver
handles on normal completion and error.
