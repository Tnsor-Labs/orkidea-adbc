"""Checks that need no database and no pyarrow: packaging, loading, errors."""

import importlib.metadata
import pathlib

import adbc_driver_manager
import pytest

import adbc_driver_orkidea


def test_bundles_the_driver_library():
    path = pathlib.Path(adbc_driver_orkidea.driver_path())
    assert path.is_file(), path
    assert path.parent == pathlib.Path(adbc_driver_orkidea.__file__).parent


def test_version_comes_from_the_installed_distribution():
    assert adbc_driver_orkidea.__version__ == importlib.metadata.version("adbc-driver-orkidea")


@pytest.mark.parametrize(
    "options",
    [
        adbc_driver_orkidea.DatabaseOptions,
        adbc_driver_orkidea.ConnectionOptions,
        adbc_driver_orkidea.StatementOptions,
    ],
)
def test_option_keys_are_driver_options(options):
    assert all(option.value.startswith("oracle.") for option in options)


def test_unknown_options_are_rejected():
    with pytest.raises(adbc_driver_manager.ProgrammingError) as info:
        adbc_driver_orkidea.connect(db_kwargs={"oracle.no_such_option": "x"})
    assert info.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT


def test_connection_failure_is_an_operational_error():
    # Port 9 (discard) is closed: the connect must fail cleanly.
    timeout = adbc_driver_orkidea.DatabaseOptions.CONNECT_TIMEOUT_MS.value
    with adbc_driver_orkidea.connect(
        "oracle://probe:probe@127.0.0.1:9/probe", db_kwargs={timeout: "3000"}
    ) as db:
        with pytest.raises(adbc_driver_manager.OperationalError) as info:
            adbc_driver_manager.AdbcConnection(db)
    assert info.value.status_code == adbc_driver_manager.AdbcStatusCode.IO
