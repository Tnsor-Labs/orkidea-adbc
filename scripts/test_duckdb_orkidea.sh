#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_ADBC_URI:?Set ORACLE_ADBC_URI before running this test}"

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
extension="${ORACLE_ADBC_EXTENSION:-/tmp/opencode/duckdb-v2.0.0/build/release/extension/orkidea_adbc/orkidea_adbc.duckdb_extension}"
library="${ORACLE_ADBC_LIBRARY:-${root_dir}/target/release/libadbc_driver_orkidea.so}"

output="$(ORACLE_ADBC_LIBRARY="$library" duckdb -unsigned -no-agent -csv -c \
    "LOAD '${extension}'; SELECT customer_id, name, city FROM orkidea_adbc_query('SELECT customer_id, name, city FROM customers ORDER BY customer_id', getenv('ORACLE_ADBC_URI'));" \
    2>&1)"

case "$output" in
    *"1.0,Ada Lovelace,London"*"2.0,Grace Hopper,New York"*"3.0,Linus Torvalds,Helsinki"*)
        printf '%s\n' "DuckDB Orkidea integration test passed."
        ;;
    *)
        printf '%s\n' "$output" >&2
        exit 1
        ;;
esac

types="$(ORACLE_ADBC_LIBRARY="$library" duckdb -unsigned -no-agent -csv -c \
    "LOAD '${extension}'; SELECT * FROM orkidea_adbc_query('SELECT DATE ''2025-01-02'' AS d, CAST(123.45 AS NUMBER(10,2)) AS n, HEXTORAW(''0102'') AS raw_value FROM dual', getenv('ORACLE_ADBC_URI'));" \
    2>&1)"

case "$types" in
    *"2025-01-02 00:00:00,123.45"*"\\x01\\x02"*)
        printf '%s\n' "DuckDB Orkidea type test passed."
        ;;
    *)
        printf '%s\n' "$types" >&2
        exit 1
        ;;
esac
