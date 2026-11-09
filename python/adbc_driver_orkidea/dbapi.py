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

"""DB-API 2.0 (PEP 249) interface to Oracle through the Orkidea driver.

Requires ``pyarrow`` (install ``adbc-driver-orkidea[dbapi]``)::

    from adbc_driver_orkidea import dbapi

    with dbapi.connect("oracle://user@host:1521/service",
                       db_kwargs={"password": "..."}) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM employees WHERE dept = :dept", {"dept": 10})
            table = cur.fetch_arrow_table()

Placeholders use Oracle's ``:name`` syntax. A ``dict`` of parameters binds
by name, and a sequence binds by position, in order of each placeholder's
first appearance.
"""

from __future__ import annotations

import typing

import adbc_driver_manager.dbapi

import adbc_driver_orkidea

__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

apilevel = adbc_driver_manager.dbapi.apilevel
threadsafety = adbc_driver_manager.dbapi.threadsafety
paramstyle = "named"

Warning = adbc_driver_manager.dbapi.Warning
Error = adbc_driver_manager.dbapi.Error
InterfaceError = adbc_driver_manager.dbapi.InterfaceError
DatabaseError = adbc_driver_manager.dbapi.DatabaseError
DataError = adbc_driver_manager.dbapi.DataError
OperationalError = adbc_driver_manager.dbapi.OperationalError
IntegrityError = adbc_driver_manager.dbapi.IntegrityError
InternalError = adbc_driver_manager.dbapi.InternalError
ProgrammingError = adbc_driver_manager.dbapi.ProgrammingError
NotSupportedError = adbc_driver_manager.dbapi.NotSupportedError

Date = adbc_driver_manager.dbapi.Date
Time = adbc_driver_manager.dbapi.Time
Timestamp = adbc_driver_manager.dbapi.Timestamp
DateFromTicks = adbc_driver_manager.dbapi.DateFromTicks
TimeFromTicks = adbc_driver_manager.dbapi.TimeFromTicks
TimestampFromTicks = adbc_driver_manager.dbapi.TimestampFromTicks
STRING = adbc_driver_manager.dbapi.STRING
BINARY = adbc_driver_manager.dbapi.BINARY
NUMBER = adbc_driver_manager.dbapi.NUMBER
DATETIME = adbc_driver_manager.dbapi.DATETIME
ROWID = adbc_driver_manager.dbapi.ROWID

Connection = adbc_driver_manager.dbapi.Connection
Cursor = adbc_driver_manager.dbapi.Cursor


def connect(
    uri: str | None = None,
    db_kwargs: typing.Mapping[str, str] | None = None,
    conn_kwargs: typing.Mapping[str, str] | None = None,
    *,
    autocommit: bool = False,
) -> Connection:
    """Connect to Oracle.

    Parameters
    ----------
    uri
        ``oracle://user:password@host:port/service``, ``oracle+tcps://...``,
        or a connect string. Prefer passing credentials as the ``username``
        and ``password`` options.
    db_kwargs
        Database options; see :class:`adbc_driver_orkidea.DatabaseOptions`.
    conn_kwargs
        Connection options; see :class:`adbc_driver_orkidea.ConnectionOptions`.
    autocommit
        Commit after every statement. PEP 249 requires it to default to
        ``False``; call ``commit()`` to make changes permanent.
    """
    options = dict(db_kwargs or {})
    if uri is not None:
        options["uri"] = uri
    return adbc_driver_manager.dbapi.connect(
        driver=adbc_driver_orkidea.driver_path(),
        entrypoint=adbc_driver_orkidea.ENTRYPOINT,
        db_kwargs=options,
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    )
