#!/usr/bin/env fish

# Usage:
#   set -gx ORACLE_PASSWORD '...'
#   set -gx ORKIDEA_ADBC_QUERY 'SELECT owner, table_name FROM all_tables WHERE ROWNUM <= 10'
#   ./scripts/launch_duckdb_orkidea_ui.fish

source (dirname (status filename))/setup_orkidea_env.fish

if not type -q duckdb
    echo "duckdb was not found in PATH" >&2
    exit 1
end

echo "Starting DuckDB UI with the Orkidea ADBC extension"
echo "Open http://localhost:4213/ and query orkidea_adbc_query(sql, getenv('ORKIDEA_ADBC_URI'))"
duckdb -unsigned -cmd "LOAD '$ORKIDEA_ADBC_EXTENSION';" --ui
