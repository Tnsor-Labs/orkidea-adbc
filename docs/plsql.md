# PL/SQL, Procedures, and DML RETURNING

`execute()` returns the outputs of PL/SQL blocks and of `RETURNING` clauses as
Arrow data. `execute_update()` runs the same statements and returns only the
row count.

## Binding parameters

Parameters bind **by name** when every placeholder name matches a parameter
field name (case-insensitively). Field order does not matter:

```sql
BEGIN :doubled := :n * 2; END;   -- fields "n" and "doubled", in any order
```

Otherwise parameters bind **by position**, in order of each placeholder's
first appearance (`:1`, `:2`, ...).

OUT parameters are bound like any other parameter, as NULL values. Give the
NULL a type, either through the Arrow type (a null `Int64` column binds as
`NUMBER`) or with field metadata `oracle.bind_type`. The metadata works on an
Arrow `Null` column, which on its own binds as `VARCHAR2`. Accepted values:
`CURSOR`, `NUMBER`, `BINARY_FLOAT`, `BINARY_DOUBLE`, `VARCHAR2`, `NVARCHAR2`,
`CHAR`, `RAW`, `DATE`, `TIMESTAMP`, `TIMESTAMP WITH TIME ZONE`,
`INTERVAL DAY TO SECOND`, `INTERVAL YEAR TO MONTH`, `BOOLEAN`, `JSON`,
`VECTOR`.

String OUT parameters bound as NULL receive at most 4000 bytes.

A batch with several rows runs the statement once per row.

## Results

| Statement | Result of `execute()` |
| --- | --- |
| PL/SQL with OUT / IN OUT parameters | One row per execution. Columns are named after the OUT placeholders; IN-only parameters are not included. |
| PL/SQL with a single REF CURSOR OUT parameter | The cursor's rows, streamed like a query. With several parameter rows, the cursors are concatenated. |
| PL/SQL without OUT parameters | An empty result; side effects are applied. |
| DML with `RETURNING ... INTO` | The returned rows, one per affected row. |

All output columns are nullable. A block that mixes a REF CURSOR with other
OUT parameters is rejected; return them from separate calls.

Under autocommit, the work done by a PL/SQL block or a DML statement is
committed after execution. On a read-only connection, PL/SQL is refused,
because the driver cannot tell whether a block writes.

## Examples

Python, calling a procedure that returns a REF CURSOR:

```python
import pyarrow as pa

params = pa.record_batch(
    [pa.array([3]), pa.nulls(1)],
    schema=pa.schema([
        pa.field("count", pa.int64()),
        pa.field("rows", pa.null(), metadata={"oracle.bind_type": "CURSOR"}),
    ]),
)
with conn.cursor() as cur:
    cur.execute("BEGIN get_rows(:count, :rows); END;", params)
    table = cur.fetch_arrow_table()
```

DML RETURNING:

```sql
INSERT INTO orders (customer) VALUES (:customer)
RETURNING id, created_at INTO :id, :created_at
```

Bind `customer` as a string column, and `id` and `created_at` as typed NULL
columns. The result has one row per inserted row.

## Vectors

Arrow `FixedSizeList` / `List` columns of `Float32`, `Float64`, `Int8`, or
`UInt8` (packed binary) bind as `VECTOR`:

```sql
SELECT id FROM docs ORDER BY VECTOR_DISTANCE(embedding, :query) FETCH FIRST 10 ROWS ONLY
```

oracledb 26.0.0-beta.5 corrupts any bind that follows a native `VECTOR` bind
in the same statement. The driver works around it:

- Ingest lists vector columns last in its INSERT.
- In your own SQL, a vector parameter followed only by other vector
  parameters binds natively. One followed by other parameters is sent as its
  text form (`[1,2,3]`), which Oracle converts implicitly. That form is
  limited to 4000 bytes; beyond it, place vector placeholders after all
  other placeholders.
