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
currently pinned to the beta dependency `26.0.0-beta.5`, plus fixes for
[oracle/rust-oracledb#80](https://github.com/oracle/rust-oracledb/issues/80)
and for `tnsnames.ora` comments, built from the commit pinned in `Cargo.toml`
until a release includes them.
Review that dependency and its license terms before adopting Orkidea.

CI runs the Rust and Python test suites on Linux amd64 against Oracle
Database Free 23ai, with the oldest and newest supported Python
dependencies. Every release build is also loaded and tested on its own
platform (Linux, macOS, and Windows, on x86_64 and arm64) without a database.

## Implemented ADBC Surface

- Streaming query execution: results arrive as bounded Arrow record batches
  (`oracle.batch_size`, default 8192 rows) instead of being loaded into memory,
  decoded from the network buffer straight into Arrow columns
  ([`oracle.columnar_fetch`](docs/options.md#reading-results))
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
- `get_objects` at every depth: schemas, tables, views, materialized views,
  temporary tables, columns with ODBC-style metadata (types, sizes,
  defaults, comments, identity and virtual columns), and primary key,
  unique, foreign key, and check constraints. See
  [docs/catalog.md](docs/catalog.md)
- `get_statistics` from Oracle optimizer statistics: row count, distinct
  count, null count, and average width
- `get_table_types`: `TABLE`, `VIEW`, `MATERIALIZED VIEW`, `TEMPORARY TABLE`
- `current_catalog` reports the connected container (PDB) name
- Schema discovery with `execute_schema`, matching `execute` exactly
- Bulk ingestion with `create`, `append`, `create_append`, and `replace`
  from a batch or a stream. Ingests are all-or-nothing, `replace` swaps the
  table in only after a complete load, and temporary ingests use Oracle
  global or private temporary tables. See
  [docs/type-support.md](docs/type-support.md#bulk-ingestion-arrow--oracle)
- PL/SQL and stored procedures: OUT / IN OUT parameters come back as a
  result row, a REF CURSOR OUT parameter streams as the result set, and DML
  `RETURNING ... INTO` returns the affected rows. Parameters bind by name
  when the names match. See [docs/plsql.md](docs/plsql.md)
- `VECTOR` parameters and ingest from Arrow lists, for similarity search and
  embedding loads
- Connection options: privileged connections (`sysdba` and others), password
  change on connect, session time zone, edition, end-to-end tracing
  attributes, call and connect timeouts, and fetch tuning. Secret options are
  write-only. See [docs/options.md](docs/options.md)

- Partitioned reads: `execute_partitions` splits a query into hash
  partitions that any connection can read in parallel. Optionally, every
  partition reads one consistent snapshot through Flashback Query. See
  [docs/partitions.md](docs/partitions.md)

Substrait, statement cancellation, and proxy authentication are not
implemented. Scheduler jobs and other
server-management features are outside the model ADBC exposes.

## Install

Prebuilt drivers are published for Linux (glibc 2.28+), macOS 11+, and
Windows, on x86_64 and arm64.

### Python

```bash
pip install "adbc-driver-orkidea[dbapi]"
```

The wheel bundles the driver. The `dbapi` extra installs `pyarrow` (16 or
newer), which the DB-API interface needs. pyarrow publishes no wheels for
Windows arm64, so there the DB-API needs a pyarrow built from source. The
low-level `adbc_driver_orkidea.connect()` API works without it.

### Other ADBC clients

The installers download a release, verify it, and register driver `orkidea`
with the ADBC driver manager:

```bash
# Linux and macOS
curl -fsSL https://github.com/Tnsor-Labs/orkidea-adbc/releases/latest/download/install.sh | sh
```

```powershell
# Windows
irm https://github.com/Tnsor-Labs/orkidea-adbc/releases/latest/download/install.ps1 | iex
```

Pass `--version v0.4.1` (`-Version` on Windows) to pin a release. On Linux
and macOS, `--system` installs for all users. Every download is checked
against the release's `SHA256SUMS`, and its Sigstore signature is verified
when `cosign` is installed. See [docs/release.md](docs/release.md) for the
release assets and how to verify them yourself.

The installer writes a driver manifest to the per-user ADBC directory
(`~/.config/adbc/drivers`, `~/Library/Application Support/ADBC/Drivers`, or
`%LOCALAPPDATA%\ADBC\Drivers`).

## Build

```bash
cargo build --locked --release -p orkidea-adbc-ffi
```

The shared library is emitted under `target/release/`, with the platform's
native extension. To use a local build, write a manifest like
`packaging/orkidea.toml.example` with the absolute path of the library. The
manifest can list one library per platform:

```toml
manifest_version = 1
name = "orkidea"
version = "0.4.1"

[Driver]
entrypoint = "OrkideaDriverInit"

[Driver.shared]
linux_amd64 = "/absolute/path/to/libadbc_driver_orkidea.so"
macos_arm64 = "/absolute/path/to/libadbc_driver_orkidea.dylib"
windows_amd64 = 'C:\path\to\adbc_driver_orkidea.dll'
```

Put the manifest in one of the directories above, or point
`ADBC_DRIVER_PATH` at the directory that contains it. Relative library paths
are not resolved against the manifest's location.

## ADBC Usage

Python, with the `adbc-driver-orkidea` package:

```python
from adbc_driver_orkidea import dbapi

with dbapi.connect(
    "oracle://user@host:1521/service",
    db_kwargs={"password": "..."},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id, name FROM customers WHERE region = :region",
                    {"region": "EMEA"})
        table = cur.fetch_arrow_table()
```

A `dict` of parameters binds by name and a sequence by position. See
[python/README.md](python/README.md) for ingestion and driver options.

Any other ADBC client can load the driver through a manifest registered by
the installers, under the short name `orkidea`:

```python
import adbc_driver_manager.dbapi

with adbc_driver_manager.dbapi.connect(
    driver="orkidea",
    db_kwargs={"uri": "oracle://user@host:1521/service", "password": "..."},
) as conn:
    ...
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
- GitHub Actions are pinned to commit SHAs and container images to digests;
  Dependabot proposes updates.
- Python wheels are published to PyPI with trusted publishing (no stored
  API token) and carry PEP 740 attestations. Each wheel bundles the same
  library as the release archive for its platform.
- Every release asset has a per-platform CycloneDX SBOM, a SHA-256 checksum
  in `SHA256SUMS` (signed keylessly with Sigstore `cosign`), and a GitHub
  build-provenance attestation. Verify with:

  ```bash
  cosign verify-blob --bundle SHA256SUMS.sigstore.json \
    --certificate-identity-regexp '^https://github.com/Tnsor-Labs/orkidea-adbc/\.github/workflows/release\.yml@refs/tags/v' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    SHA256SUMS
  sha256sum --ignore-missing -c SHA256SUMS
  gh attestation verify <asset> --repo Tnsor-Labs/orkidea-adbc
  ```

Release archives and wheels contain only the driver library, license and
third-party notice files, and a README or the Python package. They never include Oracle client
libraries, database images, wallets, credentials, or local DuckDB build
directories.

## Local Demo

The Oracle container demo is local-only. It is not downloaded, bundled, or
included in release artifacts:

```bash
./scripts/seed_orkidea_demo.sh
```

See `docs/legal-release-checklist.md` before distributing binaries or images.
