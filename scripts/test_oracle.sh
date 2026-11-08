#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_ADBC_URI:?Set ORACLE_ADBC_URI, e.g. oracle://user:password@host:1521/service}"

echo "Running Rust ADBC smoke tests"
cargo test -p orkidea-adbc --test oracle_smoke -- --nocapture

if [[ -n "${ORACLE_ADBC_EXTENSION:-}" ]]; then
  echo "Running DuckDB Orkidea integration tests"
  ./scripts/test_duckdb_orkidea.sh
else
  echo "ORACLE_ADBC_EXTENSION is not set; skipping DuckDB Orkidea test"
fi

if [[ -n "${ORACLE_ODBC_CONN:-}" ]]; then
  : "${DUCKDB_BIN:=duckdb}"
  : "${ORACLE_HOME:=/opt/oracle/product/21c/dbhomeXE}"
  : "${ODBC_LOCAL_LIB:=/tmp/opencode/odbc-local/usr/lib/x86_64-linux-gnu}"
  export ORACLE_HOME
  export LD_LIBRARY_PATH="${ODBC_LOCAL_LIB}:${ORACLE_HOME}/lib:${LD_LIBRARY_PATH:-}"

  echo "Running DuckDB 2.0 ODBC smoke query"
  "${DUCKDB_BIN}" -no-agent -c \
    "LOAD odbc_scanner; SELECT * FROM odbc_query(getenv('ORACLE_ODBC_CONN'), 'SELECT 1 AS value FROM dual');"
else
  echo "ORACLE_ODBC_CONN is not set; skipping DuckDB ODBC test"
fi
