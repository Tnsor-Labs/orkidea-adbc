# Options

Options are set with the ADBC option API. Integer and boolean options accept
strings (`"1000"`, `"true"`), because many clients pass every option as a
string.

## Database options

Database options apply to every connection created from the database
handle.

### Connecting

| Option | Description |
| --- | --- |
| `uri` | `oracle://user:password@host:port/service` or `oracle+tcps://...`. Query parameters: `config_dir`, `wallet_location`, `wallet_password`, `stmt_cache_size`. A value without `://` is used as a connect string or `tnsnames.ora` alias. |
| `username`, `password` | Credentials. They override the values in `uri`. Prefer these over embedding credentials in the URI. |
| `oracle.connect_string` | Easy Connect string (`host:port/service`), full TNS descriptor, or `tnsnames.ora` alias. Overrides the URI host. |
| `oracle.config_dir` | Directory containing `tnsnames.ora`. Defaults to `$TNS_ADMIN`. |
| `oracle.wallet_location`, `oracle.wallet_password` | PEM wallet (`ewallet.pem`) for TLS and mutual TLS. |
| `oracle.auth_mode` | Privileged connection: `sysdba`, `sysoper`, `sysasm`, `sysbackup`, `sysdg`, `syskm`, `sysrac` (default: none). |
| `oracle.new_password` | Changes the password while connecting, for example when it has expired. |
| `oracle.connect_timeout_ms` | Network connect timeout. |
| `oracle.call_timeout_ms` | Timeout for each round trip to the database. When it expires, the call is interrupted and the connection stays usable. |
| `oracle.stmt_cache_size` | Statement cache size (default 20). |
| `oracle.cclass` | DRCP connection class, for `(SERVER=POOLED)` connect strings. |

`password`, `oracle.wallet_password`, and `oracle.new_password` are
write-only: reading them back returns `NOT_FOUND`. Reading `uri` returns it
with its password masked.

### Session

These options are applied when each connection is opened.

| Option | Description |
| --- | --- |
| `oracle.time_zone` | Session time zone, for example `UTC`, `+02:00`, `Europe/Paris` (`ALTER SESSION SET TIME_ZONE`). It determines how `TIMESTAMP WITH LOCAL TIME ZONE` values are presented. |
| `oracle.edition` | Edition for edition-based redefinition (`ALTER SESSION SET EDITION`). |
| `oracle.program`, `oracle.machine`, `oracle.terminal` | Client identity recorded in `V$SESSION`. |

The driver identifies itself to the server as `orkidea : <version>`, visible
in `V$SESSION_CONNECT_INFO.CLIENT_DRIVER`.

### Tracing

`oracle.module`, `oracle.action`, `oracle.client_identifier`,
`oracle.client_info`, and `oracle.dbop` set the end-to-end tracing attributes
(`V$SESSION.MODULE`, `ACTION`, `CLIENT_IDENTIFIER`, `CLIENT_INFO`, and the
database operation). Set them on the database to apply them to every
connection, or on a connection to change them at any time. The new value is
sent with the next round trip, and `get_option` returns the current value.

### Reading results

| Option | Description |
| --- | --- |
| `oracle.columnar_fetch` | `true` (default) decodes query results from the network buffer straight into Arrow columns. `false` builds each row first, as before 0.3.0; set it to rule this out if you suspect a problem in it. |

Columnar fetching takes the columns of the common types: numbers, text,
`RAW`, `BOOLEAN`, `BINARY_FLOAT` and `BINARY_DOUBLE`, `DATE` and timestamps,
intervals, `ROWID` and `UROWID`, and LOBs (which are fetched inline). A query
with any other column, such as `JSON`, `VECTOR`, or an object, is read row by
row.
Both give the same batches. Reading a wide table is two to three times as
fast this way, and takes a third of the CPU.

### Defaults for statements

`oracle.batch_size`, `oracle.fetch_array_size`, `oracle.prefetch_rows`,
`oracle.unconstrained_number`, and `oracle.unconstrained_number_scale` set
the defaults for every statement. See [type-support.md](type-support.md).

## Connection options

| Option | Description |
| --- | --- |
| `adbc.connection.autocommit` | Default `true`. Setting it back to `true` commits pending work. |
| `adbc.connection.readonly` | When `true`, the driver refuses DML, ingest, and PL/SQL. |
| `adbc.connection.db_schema` | Current schema (`ALTER SESSION SET CURRENT_SCHEMA`). |
| `adbc.connection.catalog` | The connected container. Only the current name is accepted; see [catalog.md](catalog.md). |
| `adbc.connection.transaction.isolation_level` | `default`, `read_committed`, or `serializable`. |
| Tracing options | As above. |

## Statement options

| Option | Description |
| --- | --- |
| `adbc.ingest.*` | Bulk ingestion; see [type-support.md](type-support.md#bulk-ingestion-arrow--oracle). |
| `adbc.statement.bind_by_name` | `true` binds parameters by field name, `false` by position. When unset, parameters bind by name if every placeholder matches a field name. See [plsql.md](plsql.md#binding-parameters). |
| `oracle.batch_size` | Rows per Arrow batch and per array-DML chunk (default 8192). |
| `oracle.fetch_array_size`, `oracle.prefetch_rows` | Fetch round-trip tuning. |
| `oracle.unconstrained_number`, `oracle.unconstrained_number_scale` | How `NUMBER` columns without a declared precision are read. |
| `oracle.partition.count`, `oracle.partition.column`, `oracle.partition.consistent` | Partitioned reads; see [partitions.md](partitions.md). |

## Not supported

Proxy authentication (`user[session_user]`) is not available: the underlying
`oracledb` 26.0.0-beta.5 driver's logon is rejected by the server
(ORA-01017), although the same credentials work with SQL*Plus
([oracle/rust-oracledb#56](https://github.com/oracle/rust-oracledb/issues/56)). Token, IAM,
Kerberos and external authentication, `cwallet.sso` wallets, and Easy Connect
`?key=value` parameters are not supported by `oracledb` yet.
