#!/usr/bin/env bash
set -euo pipefail

container="${ORACLE_CONTAINER:-oracle-arrow-free}"
service="${ORACLE_SERVICE:-FREEPDB1}"
demo_user="${ORACLE_DEMO_USER:-adbc_demo}"
demo_password="${ORACLE_DEMO_PASSWORD:-$(openssl rand -hex 16)}"
project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -n "${ORACLE_SYSTEM_PASSWORD:-}" ]]; then
    system_password="$ORACLE_SYSTEM_PASSWORD"
else
    container_password=""
    while IFS= read -r env_line; do
        case "$env_line" in
            ORACLE_PWD=*) container_password="${env_line#ORACLE_PWD=}";;
        esac
    done < <(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$container")
    if [[ -z "$container_password" ]]; then
        echo "Could not read ORACLE_PWD from container $container; set ORACLE_SYSTEM_PASSWORD" >&2
        exit 1
    fi
    system_password="$container_password"
fi

docker exec -i "$container" sqlplus -s "sys/${system_password}@${service} as sysdba" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
DECLARE
    user_exists NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = UPPER('${demo_user}');
    IF user_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER ${demo_user} IDENTIFIED BY "${demo_password}"';
    ELSE
        EXECUTE IMMEDIATE 'ALTER USER ${demo_user} IDENTIFIED BY "${demo_password}" ACCOUNT UNLOCK';
    END IF;
    EXECUTE IMMEDIATE 'ALTER USER ${demo_user} DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS';
    EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW TO ${demo_user}';
END;
/
EXIT
SQL

docker exec -i "$container" sqlplus -s "${demo_user}/${demo_password}@${service}" < "$project_root/scripts/orkidea_demo.sql"

echo "Seeded Oracle user ${demo_user} on ${service}."
echo "DuckDB URI: oracle://${demo_user}:<password>@127.0.0.1:1522/${service}"
