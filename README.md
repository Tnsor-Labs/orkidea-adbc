# Orkidea ADBC

Orkidea is an ADBC driver that converts Oracle query results into Apache Arrow
record batches using the Rust `oracledb` driver. It is not affiliated with,
sponsored by, or endorsed by Oracle Corporation.

**Status:** beta. The public API and type mappings may change before 1.0.

## Identifiers

- Package and repository: `orkidea-adbc`
- ADBC manifest short name: `orkidea`
- Linux shared library: `libadbc_driver_orkidea.so`
- macOS shared library: `libadbc_driver_orkidea.dylib`
- Windows library: `adbc_driver_orkidea.dll`
- ADBC entrypoint: `OrkideaDriverInit`

## Requirements

- Rust 1.89 or newer
- Oracle Database with a service reachable by the client
- No Oracle Instant Client, OCI, ODBC driver manager, or `libarrow` runtime
- A Rust toolchain and a C++ toolchain for the optional DuckDB extension

The driver is built on [`rust-oracledb`](https://github.com/oracle/rust-oracledb),
currently pinned to the beta dependency `26.0.0-beta.5`. Review that dependency
and its license terms before adopting Orkidea.

Current validation is Linux amd64 against Oracle Database Free 23.26.3.0.0.
macOS, Windows, Linux arm64, and macOS arm64 are build targets but are not yet
covered by CI or release validation.

## Implemented ADBC Surface

- Streaming query execution: results arrive as bounded Arrow record batches
  (`oracle.batch_size`, default 8192 rows) instead of being loaded into memory
- Exact type mapping for every common Oracle type, including `NUMBER` as
  integers or decimals, `TIMESTAMP WITH TIME ZONE` as UTC instants, intervals,
  `JSON`, `VECTOR`, and `BOOLEAN`. See [docs/type-support.md](docs/type-support.md)
- Configurable handling of unconstrained `NUMBER` (`float64`, exact
  `decimal`, or `string`)
- Parameterized queries: a bound batch runs once per row
- `get_info` for vendor (including the server version), driver, Arrow, SQL,
  Substrait, and ADBC version fields
- `get_table_schema` for an individual Oracle table or view, reporting the
  declared Oracle type of each column
- `get_table_types` for tables and views
- `get_statistic_names` and `get_statistics` return valid empty result sets
- `get_objects` returns the standard ADBC schema but does not yet enumerate
  Oracle catalogs, schemas, tables, columns, or constraints
- Schema discovery with `execute_schema`, matching `execute` exactly
- Ingestion modes `create`, `append`, `create_append`, and `replace`
- Arrow batch ingestion with Oracle transaction/autocommit handling
- Oracle call timeouts through `oracle.call_timeout_ms`, plus fetch tuning
  through `oracle.fetch_array_size` and `oracle.prefetch_rows`

Partitioned execution, Substrait, Oracle procedures, scheduler jobs, and other
server-management features are not implemented. Procedures and jobs are also
outside the relational query and ingestion model exposed by ADBC.

## Build

```bash
cargo build --locked --release -p orkidea-adbc-ffi
```

The shared library is emitted under `target/release/` using the platform's
native extension. The ADBC manifest example is
`packaging/orkidea.toml.example`:

```toml
manifest_version = 1
name = "orkidea"
version = "0.1.0"

[Driver]
shared = "/absolute/path/to/libadbc_driver_orkidea.so"
entrypoint = "OrkideaDriverInit"
```

The path and extension must be changed for the target platform and install
location.

## ADBC Usage

Install the manifest and library in the locations used by your ADBC driver
manager, then use the short name `orkidea`.

Python:

```python
import adbc_driver_manager.dbapi

with adbc_driver_manager.dbapi.connect(
    driver="orkidea",
    db_kwargs={
        "uri": "oracle://user:password@host:1521/service",
    },
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id, name FROM customers")
        print(cur.fetchall())
```

Prefer separate `username` and `password` options or a driver-manager profile
when available. If credentials must be placed in a URI, percent-encode URI
special characters. Do not put production credentials in shell history,
source-controlled files, or diagnostic logs.

## DuckDB Integration

The optional custom DuckDB extension is useful for direct SQL-console and UI
testing, and for exposing the Rust driver's Arrow stream without installing a
separate ADBC client binding. It is not required for Python, C++, or other
ADBC clients. DuckDB community ADBC extensions can load the same driver through
the manifest and should be preferred when they meet your use case; keeping this
extension is justified only where the direct table-function/UI workflow is
valuable.

Build it with:

```bash
./scripts/build_duckdb_orkidea.sh
```

The local build uses DuckDB's unsigned-extension development mode. Do not use
that setting as a production security boundary. Release builds should use a
DuckDB-approved signing and verification process; signed extension packaging
is not yet published.

Configure it without embedding credentials in the extension:

```bash
export ORKIDEA_ADBC_LIBRARY="$PWD/target/release/libadbc_driver_orkidea.so"
export ORKIDEA_ADBC_URI='oracle://user:password@host:1521/service'
```

```sql
LOAD '/path/to/orkidea_adbc.duckdb_extension';
SELECT * FROM orkidea_adbc_query(
  'SELECT * FROM my_table',
  getenv('ORKIDEA_ADBC_URI')
);
```

## Security

Report security issues privately to the repository owners through GitHub
Security Advisories. Do not open a public issue containing credentials,
wallets, exploit details, or customer data.

Supply-chain controls:

- Dependencies are pinned in `Cargo.lock`; every CI and release build uses
  `--locked`.
- CI runs `rustfmt`, `clippy` with warnings denied, tests, an MSRV (1.89)
  build, and a credential-hygiene scan.
- CI runs [`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny)
  (advisories, license allow-list, banned wildcards, crates.io-only sources;
  see `deny.toml`) and
  [`cargo-audit`](https://github.com/rustsec/rustsec) with warnings denied.
- A CycloneDX SBOM is generated on every CI run and attached to each release.
- Tagged releases publish `SHA256SUMS`, signed keylessly with Sigstore
  `cosign`. Verify with:

  ```bash
  cosign verify-blob --bundle SHA256SUMS.sigstore.json \
    --certificate-identity-regexp 'https://github.com/Tnsor-Labs/orkidea-adbc/' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    SHA256SUMS
  sha256sum -c SHA256SUMS
  ```

Release artifacts contain only the driver library, the SBOM, license and
third-party notice files, and checksums. They never include Oracle client
libraries, database images, wallets, credentials, or local DuckDB build
directories.

## Local Demo

The Oracle container demo is local-only. It is not downloaded, bundled, or
included in release artifacts:

```bash
./scripts/seed_orkidea_demo.sh
```

See `docs/legal-release-checklist.md` before distributing binaries or images.
