-- Replace this path with the path printed by build_duckdb_oracle_adbc.sh.
LOAD '/tmp/opencode/duckdb-v2.0.0/build/release/extension/oracle_adbc/oracle_adbc.duckdb_extension';
SELECT * FROM oracle_adbc_result;
