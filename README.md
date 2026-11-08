# Orkidea ADBC

Arrow-native Oracle ADBC driver built on the pure-Rust `oracledb` driver.

## Identifiers

- Package and repository: `orkidea-adbc`
- ADBC manifest short name: `orkidea`
- Shared library: `libadbc_driver_orkidea.so`
- ADBC entrypoint: `OrkideaDriverInit`

The driver exports Oracle query results as Arrow record batches and supports
ADBC metadata and bulk ingestion APIs. The DuckDB integration exposes the
`orkidea_adbc_query(sql, uri)` table function.

For ADBC driver-manager installation, use the manifest short name `orkidea`
and set its manifest entrypoint to `OrkideaDriverInit`.

## Build

```bash
cargo build --release -p orkidea-adbc-ffi
./scripts/build_duckdb_oracle_adbc.sh
```

The Oracle ADBC shared library is produced at
`target/release/libadbc_driver_orkidea.so`.

## DuckDB

Set the library and URI without committing credentials:

```bash
export ORACLE_ADBC_LIBRARY="$PWD/target/release/libadbc_driver_orkidea.so"
export ORACLE_ADBC_URI='oracle://user:password@host:1521/service'
```

Then query through the extension:

```sql
LOAD '/path/to/orkidea_adbc.duckdb_extension';
SELECT * FROM orkidea_adbc_query(
  'SELECT * FROM my_table',
  getenv('ORACLE_ADBC_URI')
);
```

## Local Demo

The Oracle container demo is intentionally local-only and is not part of the
release artifact:

```bash
./scripts/seed_oracle_adbc_demo.sh
```

See `docs/legal-release-checklist.md` before distributing binaries or images.
