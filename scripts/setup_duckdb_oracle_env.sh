#!/usr/bin/env bash
# Source this file before launching DuckDB's UI:
#   export ORACLE_PASSWORD='...'
#   source scripts/setup_duckdb_oracle_env.sh

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "source this script; do not execute it" >&2
  exit 2
fi

: "${ORACLE_PASSWORD:?Set ORACLE_PASSWORD before sourcing this script}"

export ORACLE_HOME="${ORACLE_HOME:-/opt/oracle/product/21c/dbhomeXE}"
export ORACLE_HOST="${ORACLE_HOST:-127.0.0.1}"
export ORACLE_PORT="${ORACLE_PORT:-1522}"
export ORACLE_SERVICE="${ORACLE_SERVICE:-FREEPDB1}"
export ORACLE_USER="${ORACLE_USER:-arrow}"
export ODBC_LOCAL_LIB="${ODBC_LOCAL_LIB:-/tmp/opencode/odbc-local/usr/lib/x86_64-linux-gnu}"

export LD_LIBRARY_PATH="${ODBC_LOCAL_LIB}:${ORACLE_HOME}/lib:${LD_LIBRARY_PATH:-}"
# Avoid the automatic GTK browser launcher; open DuckDB's printed URL manually.
export BROWSER=true
export ORACLE_ODBC_CONN="DRIVER=${ORACLE_HOME}/lib/libsqora.so.21.1;DBQ=${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE};UID=${ORACLE_USER};PWD=${ORACLE_PASSWORD};"

# This URI is used by the Rust ADBC smoke tests. Passwords containing URI
# punctuation should be percent-encoded before setting ORACLE_ADBC_URI.
if [[ -z "${ORACLE_ADBC_URI:-}" ]]; then
  export ORACLE_ADBC_URI="oracle://${ORACLE_USER}:${ORACLE_PASSWORD}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}"
fi

echo "Oracle/DuckDB environment exported for ${ORACLE_USER}@${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}"
