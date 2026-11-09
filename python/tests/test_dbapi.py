"""DB-API checks that need pyarrow but no database."""

import pytest

pytest.importorskip("pyarrow")

import adbc_driver_orkidea  # noqa: E402
from adbc_driver_orkidea import dbapi  # noqa: E402


def test_module_globals():
    assert dbapi.apilevel == "2.0"
    assert dbapi.paramstyle == "named"


def test_connection_failure_is_an_operational_error():
    timeout = adbc_driver_orkidea.DatabaseOptions.CONNECT_TIMEOUT_MS.value
    with pytest.raises(dbapi.OperationalError):
        dbapi.connect("oracle://probe:probe@127.0.0.1:9/probe", db_kwargs={timeout: "3000"})
