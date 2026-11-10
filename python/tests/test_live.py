"""DB-API tests against a live Oracle database.

They run when ``ORKIDEA_ADBC_URI`` is set, for example
``oracle://user:password@127.0.0.1:1521/FREEPDB1``. The user needs the
CREATE TABLE and CREATE PROCEDURE privileges.
"""

import os
import uuid

import pytest

URI = os.environ.get("ORKIDEA_ADBC_URI")
if not URI:
    pytest.skip("ORKIDEA_ADBC_URI is not set", allow_module_level=True)

import pyarrow as pa  # noqa: E402

import adbc_driver_orkidea  # noqa: E402
from adbc_driver_orkidea import dbapi  # noqa: E402


@pytest.fixture
def conn():
    with dbapi.connect(URI) as conn:
        yield conn


@pytest.fixture
def table(conn):
    name = f"ORKIDEA_PY_{uuid.uuid4().hex[:12].upper()}"
    data = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "name": ["a", "b", None]})
    with conn.cursor() as cur:
        assert cur.adbc_ingest(name, data, mode="create") == 3
    conn.commit()
    yield name
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE {name} PURGE")


def test_query_returns_arrow(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT CAST(42 AS NUMBER(10)) AS n, 'x' AS s FROM dual")
        result = cur.fetch_arrow_table()
    assert result.column_names == ["N", "S"]
    assert result.schema.field("N").type == pa.int64()
    assert result.to_pylist() == [{"N": 42, "S": "x"}]


def test_dict_parameters_bind_by_name(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT :a - :b FROM dual", {"b": 2, "a": 10})
        assert cur.fetchone()[0] == 8


def test_dict_parameters_bind_every_use_of_a_name(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT :a * :b + :a FROM dual", {"a": 10, "b": 2})
        assert cur.fetchone()[0] == 30


def test_two_cursors_run_the_same_query_with_parameters(conn):
    # Each cursor prepares the query. The second prepare used to leave the
    # statement the first had run without binds (ORA-01008).
    for params in ({"n": 21}, {"n": 21}, (21,)):
        with conn.cursor() as cur:
            cur.execute("SELECT CAST(:n * 2 AS NUMBER(9)) FROM dual", params)
            assert cur.fetchone()[0] == 42


def test_sequence_parameters_bind_by_position(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT :a - :b FROM dual", (10, 2))
        assert cur.fetchone()[0] == 8


def test_dict_parameters_must_match_the_placeholders(conn):
    with conn.cursor() as cur:
        with pytest.raises(dbapi.ProgrammingError, match='no bound parameter is named "B"'):
            cur.execute("SELECT :a - :b FROM dual", {"a": 10, "c": 2})


def test_ingest_and_read_back(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, name FROM {table} ORDER BY id")
        assert cur.fetchall() == [(1, "a"), (2, "b"), (3, None)]


def test_executemany_and_transactions(conn, table):
    with conn.cursor() as cur:
        cur.executemany(f"INSERT INTO {table} (id, name) VALUES (:id, :name)", [(4, "d"), (5, "e")])
        conn.rollback()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        assert cur.fetchone()[0] == 3
        cur.executemany(f"INSERT INTO {table} (id, name) VALUES (:id, :name)", [(4, "d"), (5, "e")])
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        assert cur.fetchone()[0] == 5


def test_table_schema_reports_oracle_types(conn, table):
    schema = conn.adbc_get_table_schema(table)
    assert schema.names == ["ID", "NAME"]
    type_name = schema.field("ID").metadata[adbc_driver_orkidea.TYPE_NAME_KEY.encode()]
    assert type_name.startswith(b"NUMBER")


def test_plsql_out_parameters(conn):
    params = pa.record_batch(
        [pa.array([21], pa.int64()), pa.nulls(1, pa.int64())],
        names=["n", "doubled"],
    )
    with conn.cursor() as cur:
        cur.execute("BEGIN :doubled := :n * 2; END;", params)
        assert cur.fetch_arrow_table().to_pylist() == [{"DOUBLED": 42}]


def test_ref_cursor_out_parameter(conn, table):
    params = pa.record_batch(
        [pa.array([2], pa.int64()), pa.nulls(1)],
        schema=pa.schema(
            [
                pa.field("max_id", pa.int64()),
                pa.field("rows", pa.null(), metadata={adbc_driver_orkidea.BIND_TYPE_KEY: "CURSOR"}),
            ]
        ),
    )
    with conn.cursor() as cur:
        cur.execute(
            f"BEGIN OPEN :rows FOR SELECT id FROM {table} WHERE id <= :max_id ORDER BY id; END;",
            params,
        )
        assert cur.fetchall() == [(1,), (2,)]


def test_partitioned_read(conn, table):
    with conn.cursor() as cur:
        options = adbc_driver_orkidea.StatementOptions
        cur.adbc_statement.set_options(
            **{options.PARTITION_COUNT.value: "3", options.PARTITION_COLUMN.value: "id"}
        )
        partitions, schema = cur.adbc_execute_partitions(f"SELECT id FROM {table}")
    assert len(partitions) == 3
    assert schema.names == ["ID"]
    ids = []
    for partition in partitions:
        with conn.cursor() as cur:
            cur.adbc_read_partition(partition)
            ids += cur.fetch_arrow_table().column("ID").to_pylist()
    assert sorted(ids) == [1, 2, 3]


def test_tracing_connection_option():
    module = adbc_driver_orkidea.ConnectionOptions.MODULE.value
    with dbapi.connect(URI, conn_kwargs={module: "orkidea-python"}) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT SYS_CONTEXT('USERENV', 'MODULE') FROM dual")
            assert cur.fetchone()[0] == "orkidea-python"
