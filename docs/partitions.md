# Partitioned Reads

`execute_partitions` splits a query into independent partitions without
fetching any rows. Each partition is an opaque descriptor that
`read_partition` turns into an Arrow stream, on the same connection or on any
other connection to the same database, in another thread, process, or
machine. Engines that read in parallel (for example, a distributed scan) can
spread one query across workers this way.

## Options

| Statement option | Default | Meaning |
| --- | --- | --- |
| `oracle.partition.count` | `1` | Number of partitions (1 to 4096). |
| `oracle.partition.column` | — | Result column to hash on. Required when the count is above 1. |
| `oracle.partition.consistent` | `false` | Read every partition as of one SCN. |

With one partition, the descriptor runs the query unchanged. With `n`
partitions, partition `k` runs:

```sql
SELECT orkidea_p.* FROM (<query>) orkidea_p
 WHERE ORA_HASH(orkidea_p."COLUMN", n - 1) = k
       -- partition 0 also takes the rows where the column is NULL
```

Every row of the result belongs to exactly one partition. Pick a column with
many distinct values (a key or a sequence) so that partitions are evenly
sized. A column with few values, or mostly NULLs, puts most rows into a few
partitions. Each partition runs the full query with a filter, so Oracle
reads the source data once per partition. Partitioning pays off when the
cost is in transferring and converting rows, and when the database has
capacity for concurrent scans.

The column name follows Oracle name resolution: `id` refers to the column
`ID`. The query cannot have bound parameters.

## Consistency

By default, each partition reads the data as it is when that partition is
read. If rows change between reads, a row can be read twice or not at all.

With `oracle.partition.consistent = true`, the driver captures the current
SCN when it creates the partitions, and every partition reads the database
as of that SCN using Oracle Flashback Query. The union of all partitions is
then an exact snapshot, whatever changes in between. This needs:

```sql
GRANT EXECUTE ON DBMS_FLASHBACK TO <user>;
```

Without the grant, `execute_partitions` fails with `UNAUTHORIZED`.

Flashback has two limits:

- The snapshot must still be within the database's undo retention
  (`UNDO_RETENTION`). Reading partitions long after creating them can fail
  with ORA-01555.
- A table created or altered after the snapshot, or within a few seconds
  before it, cannot be read at that SCN (ORA-01466).

Flashback mode is enabled only while each partition's cursor is opened, and
disabled again immediately, so the connection stays usable for DML.

## Descriptors

A descriptor contains the SQL text, the partitioning, the SCN, and the type
and fetch options (`oracle.unconstrained_number`, `oracle.batch_size`,
`oracle.fetch_array_size`, `oracle.prefetch_rows`). Every partition therefore
produces the schema that `execute_partitions` reported, whichever connection
reads it. Descriptors contain no credentials, but they do contain the SQL
text, so handle them like the query itself.

## Example (Python)

```python
from concurrent.futures import ThreadPoolExecutor
import adbc_driver_manager.dbapi as dbapi

kwargs = dict(driver="orkidea", db_kwargs={"uri": uri})

with dbapi.connect(**kwargs) as conn, conn.cursor() as cur:
    cur.adbc_statement.set_options(**{
        "oracle.partition.count": "8",
        "oracle.partition.column": "order_id",
        "oracle.partition.consistent": "true",
    })
    partitions, schema = cur.adbc_execute_partitions("SELECT * FROM orders")

def read(partition):
    with dbapi.connect(**kwargs) as conn, conn.cursor() as cur:
        cur.adbc_read_partition(partition)
        return cur.fetch_arrow_table()

with ThreadPoolExecutor(8) as pool:
    tables = list(pool.map(read, partitions))
```
