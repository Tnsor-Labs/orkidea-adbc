# Development

## Local Build

```bash
cargo fmt --all
cargo check --workspace
cargo build --release -p orkidea-adbc-ffi
./scripts/build_duckdb_oracle_adbc.sh
```

The Rust shared library is `target/release/libadbc_driver_orkidea.so`. The
DuckDB extension is produced under the DuckDB build directory as
`orkidea_adbc.duckdb_extension`.

## Tests

Rust tests do not require Oracle when `ORACLE_ADBC_URI` is absent; the Oracle
integration test is skipped in that case.

```bash
cargo test --workspace
ORACLE_ADBC_URI='oracle://user:password@host:1521/service' \
  cargo test -p orkidea-adbc --test oracle_smoke -- --nocapture
```

The DuckDB integration test expects a user-supplied Oracle URI and a built
extension:

```bash
ORACLE_ADBC_URI='oracle://user:password@host:1521/service' \
  ./scripts/test_duckdb_orkidea.sh
```

## Cancellation and Timeouts

`oracle.call_timeout_ms` configures the Oracle driver call timeout and is the
current safe cancellation mechanism. ADBC cancellation remains best-effort
until the underlying Rust Oracle driver exposes a non-blocking cancel handle.
Every query path releases its Arrow stream, statement, connection, and driver
handles on normal completion and error.
