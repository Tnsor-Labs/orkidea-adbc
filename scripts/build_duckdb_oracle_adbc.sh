#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_SOURCE="${DUCKDB_SOURCE:-/tmp/opencode/duckdb-v2.0.0}"
DUCKDB_BUILD="${DUCKDB_BUILD:-${DUCKDB_SOURCE}/build/release}"

if [[ ! -d "${DUCKDB_SOURCE}/.git" ]]; then
  mkdir -p "$(dirname "${DUCKDB_SOURCE}")"
  git clone --depth 1 --branch v2.0.0 https://github.com/duckdb/duckdb.git "${DUCKDB_SOURCE}"
fi

rm -rf "${DUCKDB_SOURCE}/extension/orkidea_adbc"
cp -R "${ROOT_DIR}/integrations/duckdb-oracle-adbc" "${DUCKDB_SOURCE}/extension/orkidea_adbc"
printf '%s\n' 'duckdb_extension_load(orkidea_adbc)' > "${DUCKDB_SOURCE}/extension/extension_config_local.cmake"

cmake -S "${DUCKDB_SOURCE}" -B "${DUCKDB_BUILD}" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DLINK_CORE_EXTENSIONS=OFF \
  -DSTATICALLY_LINK_EXTENSIONS="" \
  -DDUCKDB_CAPABILITIES="" \
  -DLOCAL_EXTENSION_REPO=""
cmake --build "${DUCKDB_BUILD}" --target orkidea_adbc_loadable_extension

printf 'Built extension: %s\n' \
  "${DUCKDB_BUILD}/extension/orkidea_adbc/orkidea_adbc.duckdb_extension"
