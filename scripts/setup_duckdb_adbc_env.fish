# Source this file before launching DuckDB UI for the native ADBC integration.
#
#   set -gx ORACLE_PASSWORD '...'
#   source scripts/setup_duckdb_adbc_env.fish
#   duckdb -unsigned --ui

if not set -q ORACLE_PASSWORD
    echo "Set ORACLE_PASSWORD before sourcing this file" >&2
    return 2
end

if not set -q ORACLE_USER
    set -gx ORACLE_USER arrow
end
if not set -q ORACLE_HOST
    set -gx ORACLE_HOST 127.0.0.1
end
if not set -q ORACLE_PORT
    set -gx ORACLE_PORT 1522
end
if not set -q ORACLE_SERVICE
    set -gx ORACLE_SERVICE FREEPDB1
end
set project_root (realpath (dirname (status filename))/..)
if not set -q ORACLE_ADBC_LIBRARY
    set -gx ORACLE_ADBC_LIBRARY $project_root/target/release/libadbc_driver_orkidea.so
end
if not set -q ORACLE_ADBC_QUERY
    set -gx ORACLE_ADBC_QUERY 'SELECT 1 AS value FROM dual'
end
if not set -q ORACLE_ADBC_EXTENSION
    set -gx ORACLE_ADBC_EXTENSION /tmp/opencode/duckdb-v2.0.0/build/release/extension/orkidea_adbc/orkidea_adbc.duckdb_extension
end

if not set -q ORACLE_ADBC_URI
    set -gx ORACLE_ADBC_URI "oracle://$ORACLE_USER:$ORACLE_PASSWORD@$ORACLE_HOST:$ORACLE_PORT/$ORACLE_SERVICE"
end

# Prevent DuckDB's automatic browser launcher from inheriting Oracle client
# libraries. Open the printed UI URL manually.
set -gx BROWSER true

echo "Native DuckDB ADBC environment exported"
echo "Query: $ORACLE_ADBC_QUERY"
echo "Extension: $ORACLE_ADBC_EXTENSION"
