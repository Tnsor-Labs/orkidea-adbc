# Catalog Metadata

## Catalogs and schemas

Oracle has no catalogs in the ADBC sense. Orkidea reports a single catalog,
named after the container the connection is attached to
(`SYS_CONTEXT('USERENV', 'CON_NAME')`, for example `FREEPDB1`). In a non-CDB
database this is the database name.

- `get_option(current_catalog)` returns this name. Setting it to the same name
  is accepted. Any other value is refused, because changing container
  requires connecting to a different service.
- Schemas are database users (`ALL_USERS`).
- Every filter is an ADBC search pattern (`%` and `_`), matched
  case-sensitively against the names Oracle stores. Unquoted Oracle names are
  stored upper-case, so filter with `HR`, not `hr`.

## `get_objects`

| Depth | Contents |
| --- | --- |
| `catalogs` | The catalog; `catalog_db_schemas` is null |
| `db_schemas` | Schemas; `db_schema_tables` is null |
| `tables` | Tables, views, materialized views, temporary tables; columns and constraints are null |
| `all` / `columns` | Everything, including columns and constraints |

Objects come from the `ALL_*` dictionary views, so the result contains what
the connected user can see. Dropped tables (in the recycle bin), nested
tables, IOT overflow segments, and the container tables behind materialized
views are left out.

Table types are `TABLE`, `VIEW`, `MATERIALIZED VIEW`, and `TEMPORARY TABLE`
(global temporary tables). Use them in the `table_type` filter.

### Columns

| Field | Source |
| --- | --- |
| `ordinal_position` | `COLUMN_ID` |
| `remarks` | Column comment (`COMMENT ON COLUMN`) |
| `xdbc_type_name` | Oracle type name, for example `NUMBER`, `TIMESTAMP(3)` |
| `xdbc_data_type`, `xdbc_sql_data_type`, `xdbc_datetime_sub` | ODBC type codes, following Oracle's ODBC driver conventions. `DATE` is a timestamp because it has a time part. `VECTOR` and object types have no ODBC code and report null. |
| `xdbc_column_size` | Precision for numbers (38 when unconstrained), characters for character types, bytes for `RAW` |
| `xdbc_decimal_digits`, `xdbc_num_prec_radix` | Scale and radix for numbers; fractional digits for timestamps |
| `xdbc_char_octet_length` | Byte length for character and binary types |
| `xdbc_nullable`, `xdbc_is_nullable` | `NULLABLE` |
| `xdbc_column_def` | Default expression |
| `xdbc_is_autoincrement` | Identity column |
| `xdbc_is_generatedcolumn` | Virtual column |

Hidden (system-generated) columns are not listed. The `column_name` filter
narrows the columns returned without removing tables.

### Constraints

Primary key, unique, foreign key, and check constraints are listed with their
columns in key order. For foreign keys, `constraint_column_usage` lists the
referenced table and columns in the same order. `NOT NULL` constraints are
reported through `xdbc_nullable`, not as check constraints.

## `get_statistics`

Statistics come from Oracle's optimizer statistics (`ALL_TAB_STATISTICS`,
`ALL_TAB_COL_STATISTICS`):

| Statistic | Level | Source |
| --- | --- | --- |
| `adbc.statistic.row_count` | Table | `NUM_ROWS` |
| `adbc.statistic.byte_width` | Table | `AVG_ROW_LEN` |
| `adbc.statistic.distinct_count` | Column | `NUM_DISTINCT` |
| `adbc.statistic.null_count` | Column | `NUM_NULLS` |
| `adbc.statistic.byte_width` | Column | `AVG_COL_LEN` |

They are estimates as of the last statistics gathering (for example
`DBMS_STATS.GATHER_TABLE_STATS`), so every value is flagged
`statistic_is_approximate = true`, even when exact values are requested.
Tables without gathered statistics are omitted. Minimum and maximum values are
not reported: Oracle stores them in an internal encoding.

## Table schemas from the dictionary

`execute_schema` on a statement with `oracle.table_schemas` set to a schema
name gives the Arrow schema of each of its tables, read from `ALL_TAB_COLS`
with one query, where describing them takes a query of each. The name is an identifier, as in SQL: `hr` and `HR` name `HR`, and
`"Hr"` names `Hr`. Setting it replaces any SQL query on the statement, and
setting a query clears it; such a statement cannot be executed.

The result has one non-nullable struct field per table, named after it,
whose fields are the table's columns: the same fields, with the same
types, nullability, and `oracle.type-name`, as `SELECT *` of the table
gives, under the statement's `oracle.unconstrained_number` options. Left
out, because the dictionary cannot say how a query describes them:

- tables with a column of a type the driver cannot read, or whose shape is
  not in the dictionary (`VECTOR`, `XMLTYPE`, object types);
- tables with a virtual column declared with a precision, such as
  `total NUMBER(10,2) AS (qty * price)`, which a query describes as the type
  of its expression (`NUMBER`);
- views, whose columns are expressions: a view over such a virtual column
  has `NUMBER(10,2)` in the dictionary, too.

Describe those with a query of each. Like `get_objects`, the result holds
the tables the user can see, which includes a table on which the user has
been granted only `INSERT`, `UPDATE`, or `DELETE` and cannot query.

```python
stmt = conn.adbc_statement
stmt.set_options(**{"oracle.table_schemas": "HR"})
schema = stmt.execute_schema()
schema.field("EMPLOYEES").type  # struct<EMPLOYEE_ID: int32, ...>
```

## Performance

Catalog calls are only as fast as Oracle's `ALL_*` dictionary views, which
can take several seconds on large dictionaries or resource-limited instances
such as Oracle Database Free. Filters are pushed into the dictionary queries,
so pass a schema and table pattern whenever possible. An unfiltered
`get_objects` at depth `all` reads every column of every visible table,
including Oracle's own schemas.
