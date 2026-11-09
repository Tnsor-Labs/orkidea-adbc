cmake_minimum_required(VERSION 2.8.12...3.29)

project(OrkideaAdbcExtension)

set(EXTENSION_NAME orkidea_adbc)
set(EXTENSION_FILES oracle_adbc_table_function.cpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${DUCKDB_SOURCE_DIR}/src/include)

set(CAPI_MAJOR 1)
set(CAPI_MINOR 5)
set(CAPI_PATCH 6)

build_loadable_extension_capi(${EXTENSION_NAME} ${CAPI_MAJOR} ${CAPI_MINOR}
                               ${CAPI_PATCH} ${EXTENSION_FILES})
