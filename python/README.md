# adbc-driver-orkidea

[Orkidea](https://github.com/Tnsor-Labs/orkidea-adbc) is an
[ADBC](https://arrow.apache.org/adbc/) driver for Oracle Database, written in
Rust. Query results arrive as Apache Arrow data. The driver speaks Oracle's
network protocol directly, so it needs no Oracle Instant Client, OCI, or ODBC.

This package bundles the driver library and connects it to the ADBC driver
manager's DB-API 2.0 interface. Wheels are available for Linux (glibc 2.28+),
macOS 11+, and Windows, on x86_64 and arm64.

Orkidea is not affiliated with, sponsored by, or endorsed by Oracle
Corporation.

## Install

```bash
pip install "adbc-driver-orkidea[dbapi]"
```

The `dbapi` extra installs `pyarrow`, which the DB-API interface needs.

## Usage

```python
from adbc_driver_orkidea import dbapi

with dbapi.connect(
    "oracle://scott@db.example.com:1521/FREEPDB1",
    db_kwargs={"password": "..."},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM employees WHERE department_id = :dept", {"dept": 10})
        table = cur.fetch_arrow_table()  # a pyarrow.Table
```

Placeholders use Oracle's `:name` syntax. A `dict` of parameters binds by
name, and a placeholder used more than once takes the same value each time. A
sequence binds by position: in SQL, one value per placeholder in the order
they appear; in PL/SQL, one value per name. Like every DB-API connection, it
starts a transaction: call `commit()`, or pass `autocommit=True`.

Load an Arrow table, a pandas or polars DataFrame converted to Arrow, or a
stream of record batches into a table:

```python
with conn.cursor() as cur:
    cur.adbc_ingest("measurements", table, mode="create_append")
conn.commit()
```

Driver-specific options are listed in `DatabaseOptions`, `ConnectionOptions`,
and `StatementOptions`:

```python
import adbc_driver_orkidea

conn = dbapi.connect(
    "oracle://scott@db.example.com:1521/FREEPDB1",
    db_kwargs={
        "password": "...",
        adbc_driver_orkidea.DatabaseOptions.CALL_TIMEOUT_MS.value: "30000",
        adbc_driver_orkidea.DatabaseOptions.TIME_ZONE.value: "UTC",
    },
)
```

`adbc_driver_orkidea.driver_path()` returns the bundled library, for ADBC
clients that load drivers by path (entrypoint `OrkideaDriverInit`).

## Documentation

- [Type mapping](https://github.com/Tnsor-Labs/orkidea-adbc/blob/main/docs/type-support.md)
- [Options](https://github.com/Tnsor-Labs/orkidea-adbc/blob/main/docs/options.md)
- [PL/SQL, OUT parameters, and REF CURSORs](https://github.com/Tnsor-Labs/orkidea-adbc/blob/main/docs/plsql.md)
- [Catalog metadata](https://github.com/Tnsor-Labs/orkidea-adbc/blob/main/docs/catalog.md)
- [Partitioned reads](https://github.com/Tnsor-Labs/orkidea-adbc/blob/main/docs/partitions.md)

## License

Apache-2.0. The wheel's license files include the licenses of the Rust
crates compiled into the driver.
