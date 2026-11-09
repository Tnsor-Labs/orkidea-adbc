# Source this file from Fish:
#   set -gx ORACLE_PASSWORD '...'
#   source scripts/setup_duckdb_oracle_env.fish

if not set -q ORACLE_PASSWORD
    echo "Set ORACLE_PASSWORD before sourcing this file" >&2
    return 2
end

if not set -q ORACLE_HOME
    set -gx ORACLE_HOME /opt/oracle/product/21c/dbhomeXE
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
if not set -q ORACLE_USER
    set -gx ORACLE_USER arrow
end
if not set -q ODBC_LOCAL_LIB
    set -gx ODBC_LOCAL_LIB /tmp/opencode/odbc-local/usr/lib/x86_64-linux-gnu
end

set -gx LD_LIBRARY_PATH $ODBC_LOCAL_LIB $ORACLE_HOME/lib $LD_LIBRARY_PATH
# Oracle's client libraries can make the UI's automatic GTK browser launcher
# crash. DuckDB still starts the UI; open the printed URL manually.
set -gx BROWSER true
set -gx ORACLE_ODBC_CONN "DRIVER=$ORACLE_HOME/lib/libsqora.so.21.1;DBQ=$ORACLE_HOST:$ORACLE_PORT/$ORACLE_SERVICE;UID=$ORACLE_USER;PWD=$ORACLE_PASSWORD;"

if not set -q ORKIDEA_ADBC_URI
    set -gx ORKIDEA_ADBC_URI "oracle://$ORACLE_USER:$ORACLE_PASSWORD@$ORACLE_HOST:$ORACLE_PORT/$ORACLE_SERVICE"
end

echo "Oracle/DuckDB environment exported for $ORACLE_USER@$ORACLE_HOST:$ORACLE_PORT/$ORACLE_SERVICE"
