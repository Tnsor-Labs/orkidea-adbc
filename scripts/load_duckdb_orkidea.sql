-- Replace this path with the path printed by build_duckdb_orkidea.sh.
LOAD '/tmp/opencode/duckdb-v2.0.0/build/release/extension/orkidea_adbc/orkidea_adbc.duckdb_extension';
SELECT * FROM orkidea_adbc_query(
    'SELECT 1 AS value FROM dual',
    getenv('ORKIDEA_ADBC_URI')
);
