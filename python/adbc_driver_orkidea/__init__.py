# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Orkidea, an ADBC driver for Oracle Database.

The wheel bundles the driver library, so no Oracle Client libraries are
needed. Use :mod:`adbc_driver_orkidea.dbapi` for a DB-API 2.0 connection,
or :func:`connect` for the low-level ADBC API.
"""

from __future__ import annotations

import enum
import functools
import importlib.metadata
import importlib.resources
import sys
import typing

import adbc_driver_manager

__all__ = [
    "BIND_TYPE_KEY",
    "ENTRYPOINT",
    "TYPE_NAME_KEY",
    "ConnectionOptions",
    "DatabaseOptions",
    "StatementOptions",
    "__version__",
    "connect",
    "driver_path",
]

try:
    __version__ = importlib.metadata.version("adbc-driver-orkidea")
except importlib.metadata.PackageNotFoundError:  # running from a source tree
    __version__ = "0+unknown"

#: The driver's ADBC entrypoint symbol.
ENTRYPOINT = "OrkideaDriverInit"

#: Field metadata key holding the declared Oracle type of a result column
#: (for example ``NUMBER(10,2)``).
TYPE_NAME_KEY = "oracle.type-name"

#: Field metadata key that gives a NULL parameter an Oracle type, for OUT
#: binds (for example ``CURSOR`` or ``NUMBER``).
BIND_TYPE_KEY = "oracle.bind_type"


class DatabaseOptions(enum.Enum):
    """Database options of the Orkidea driver.

    Pass them in ``db_kwargs``, for example
    ``{DatabaseOptions.CALL_TIMEOUT_MS.value: "5000"}``. The standard
    ``uri``, ``username``, and ``password`` options are also accepted.
    """

    #: Easy Connect string, TNS descriptor, or ``tnsnames.ora`` alias.
    CONNECT_STRING = "oracle.connect_string"
    #: Directory containing ``tnsnames.ora`` (default: ``$TNS_ADMIN``).
    CONFIG_DIR = "oracle.config_dir"
    #: Directory of a PEM wallet (``ewallet.pem``) for TLS and mutual TLS.
    WALLET_LOCATION = "oracle.wallet_location"
    #: Password of the wallet. Write-only.
    WALLET_PASSWORD = "oracle.wallet_password"
    #: Privileged connection: ``sysdba``, ``sysoper``, ``sysasm``,
    #: ``sysbackup``, ``sysdg``, ``syskm``, or ``sysrac``.
    AUTH_MODE = "oracle.auth_mode"
    #: Changes the password while connecting. Write-only.
    NEW_PASSWORD = "oracle.new_password"
    #: Network connect timeout, in milliseconds.
    CONNECT_TIMEOUT_MS = "oracle.connect_timeout_ms"
    #: Timeout for each round trip to the database, in milliseconds.
    CALL_TIMEOUT_MS = "oracle.call_timeout_ms"
    #: Statement cache size (default 20).
    STMT_CACHE_SIZE = "oracle.stmt_cache_size"
    #: DRCP connection class.
    CCLASS = "oracle.cclass"
    #: Session time zone, for example ``UTC`` or ``Europe/Paris``.
    TIME_ZONE = "oracle.time_zone"
    #: Edition for edition-based redefinition.
    EDITION = "oracle.edition"
    #: Program name recorded in ``V$SESSION``.
    PROGRAM = "oracle.program"
    #: Machine name recorded in ``V$SESSION``.
    MACHINE = "oracle.machine"
    #: Terminal name recorded in ``V$SESSION``.
    TERMINAL = "oracle.terminal"
    #: End-to-end tracing: ``V$SESSION.MODULE``.
    MODULE = "oracle.module"
    #: End-to-end tracing: ``V$SESSION.ACTION``.
    ACTION = "oracle.action"
    #: End-to-end tracing: ``V$SESSION.CLIENT_IDENTIFIER``.
    CLIENT_IDENTIFIER = "oracle.client_identifier"
    #: End-to-end tracing: ``V$SESSION.CLIENT_INFO``.
    CLIENT_INFO = "oracle.client_info"
    #: End-to-end tracing: the database operation.
    DBOP = "oracle.dbop"
    #: Default rows per Arrow batch (default 8192).
    BATCH_SIZE = "oracle.batch_size"
    #: Default rows fetched per round trip.
    FETCH_ARRAY_SIZE = "oracle.fetch_array_size"
    #: Default rows prefetched with the execute round trip.
    PREFETCH_ROWS = "oracle.prefetch_rows"
    #: How ``NUMBER`` without a precision is read: ``float64``, ``decimal``,
    #: or ``string``.
    UNCONSTRAINED_NUMBER = "oracle.unconstrained_number"
    #: Scale used when unconstrained ``NUMBER`` is read as ``decimal``.
    UNCONSTRAINED_NUMBER_SCALE = "oracle.unconstrained_number_scale"


class ConnectionOptions(enum.Enum):
    """Connection options of the Orkidea driver.

    The tracing options can be changed at any time; the new value is sent
    with the next round trip.
    """

    MODULE = "oracle.module"
    ACTION = "oracle.action"
    CLIENT_IDENTIFIER = "oracle.client_identifier"
    CLIENT_INFO = "oracle.client_info"
    DBOP = "oracle.dbop"


class StatementOptions(enum.Enum):
    """Statement options of the Orkidea driver."""

    #: Rows per Arrow batch and per array-DML chunk (default 8192).
    BATCH_SIZE = "oracle.batch_size"
    #: Rows fetched per round trip.
    FETCH_ARRAY_SIZE = "oracle.fetch_array_size"
    #: Rows prefetched with the execute round trip.
    PREFETCH_ROWS = "oracle.prefetch_rows"
    #: How ``NUMBER`` without a precision is read.
    UNCONSTRAINED_NUMBER = "oracle.unconstrained_number"
    #: Scale used when unconstrained ``NUMBER`` is read as ``decimal``.
    UNCONSTRAINED_NUMBER_SCALE = "oracle.unconstrained_number_scale"
    #: Number of partitions ``adbc_execute_partitions`` returns.
    PARTITION_COUNT = "oracle.partition.count"
    #: Result column whose hash assigns rows to partitions. Required when
    #: the partition count is above 1.
    PARTITION_COLUMN = "oracle.partition.column"
    #: Read every partition from one consistent snapshot (Flashback Query).
    PARTITION_CONSISTENT = "oracle.partition.consistent"


def connect(
    uri: str | None = None,
    db_kwargs: typing.Mapping[str, str] | None = None,
) -> adbc_driver_manager.AdbcDatabase:
    """Create a low-level ADBC database handle for Oracle.

    Parameters
    ----------
    uri
        ``oracle://user:password@host:port/service``, ``oracle+tcps://...``,
        or a connect string. Prefer passing credentials as the ``username``
        and ``password`` options.
    db_kwargs
        Further database options; see :class:`DatabaseOptions`.
    """
    options = dict(db_kwargs or {})
    if uri is not None:
        options["uri"] = uri
    options["driver"] = driver_path()
    options["entrypoint"] = ENTRYPOINT
    return adbc_driver_manager.AdbcDatabase(**options)


_LIBRARY = {
    "darwin": "libadbc_driver_orkidea.dylib",
    "win32": "adbc_driver_orkidea.dll",
}.get(sys.platform, "libadbc_driver_orkidea.so")


@functools.lru_cache
def driver_path() -> str:
    """The path of the bundled driver library.

    Other ADBC clients can load the driver from this path with entrypoint
    :data:`ENTRYPOINT`. Without a bundled library (a source checkout), this
    returns ``"orkidea"``, so the driver manager looks for a driver manifest
    of that name, as written by the release installers.
    """
    library = importlib.resources.files(__name__).joinpath(_LIBRARY)
    if library.is_file():
        return str(library)
    return "orkidea"
