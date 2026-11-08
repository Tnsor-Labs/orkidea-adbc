#include "duckdb_extension.h"
#include "duckdb/common/adbc/adbc.h"

#include <dlfcn.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

DUCKDB_EXTENSION_EXTERN

struct OracleAdbcQueryState {
    void *library = nullptr;
    AdbcDriver driver{};
    AdbcDatabase database{};
    AdbcConnection connection{};
    AdbcStatement statement{};
    ArrowArrayStream stream{};
    ArrowSchema schema{};
    ArrowArray current{};
    int64_t current_offset = 0;
    bool current_valid = false;
};

static std::string adbc_error(const AdbcError &error, const char *operation) {
    std::string message = operation;
    message += ": ";
    message += error.message ? error.message : "unknown ADBC error";
    return message;
}

static bool check_status(AdbcStatusCode status, const AdbcError &error, duckdb_bind_info info,
                         const char *operation) {
    if (status == ADBC_STATUS_OK) {
        return true;
    }
    duckdb_bind_set_error(info, adbc_error(error, operation).c_str());
    return false;
}

static void release_query_state(void *opaque) {
    auto *state = static_cast<OracleAdbcQueryState *>(opaque);
    if (state->current_valid && state->current.release) {
        state->current.release(&state->current);
    }
    if (state->schema.release) {
        state->schema.release(&state->schema);
    }
    if (state->stream.release) {
        state->stream.release(&state->stream);
    }
    AdbcError error{};
    if (state->driver.StatementRelease && state->statement.private_data) {
        state->driver.StatementRelease(&state->statement, &error);
    }
    if (state->driver.ConnectionRelease && state->connection.private_data) {
        state->driver.ConnectionRelease(&state->connection, &error);
    }
    if (state->driver.DatabaseRelease && state->database.private_data) {
        state->driver.DatabaseRelease(&state->database, &error);
    }
    if (state->driver.release) {
        state->driver.release(&state->driver, &error);
    }
    if (state->library) {
        dlclose(state->library);
    }
    delete state;
}

static duckdb_logical_type logical_type_for_format(const char *format) {
    if (!format || !format[0]) {
        return nullptr;
    }
    switch (format[0]) {
    case 'c':
        return duckdb_create_logical_type(DUCKDB_TYPE_TINYINT);
    case 's':
        return duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
    case 'i':
        return duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    case 'l':
        return duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    case 'f':
        return duckdb_create_logical_type(DUCKDB_TYPE_FLOAT);
    case 'g':
        return duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
    case 'b':
        return duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    case 'u':
        return duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    default:
        return nullptr;
    }
}

static bool arrow_valid(const ArrowArray &array, int64_t index) {
    if (!array.buffers || !array.buffers[0]) {
        return true;
    }
    const auto *validity = static_cast<const uint8_t *>(array.buffers[0]);
    return (validity[index / 8] & (uint8_t(1) << (index % 8))) != 0;
}

static void set_invalid(duckdb_vector vector, idx_t row) {
    duckdb_vector_ensure_validity_writable(vector);
    duckdb_validity_set_row_invalid(duckdb_vector_get_validity(vector), row);
}

static bool copy_arrow_column(duckdb_vector vector, const ArrowSchema &schema, const ArrowArray &array,
                              int64_t source_offset, idx_t count, duckdb_function_info info) {
    const char format = schema.format ? schema.format[0] : '\0';
    // The Oracle ADBC stream reserves buffer 0 for validity and leaves it
    // null when all values are valid, while n_buffers still excludes it.
    const int data_buffer = 1;
    for (idx_t row = 0; row < count; row++) {
        const int64_t source = source_offset + static_cast<int64_t>(row);
        if (!arrow_valid(array, source)) {
            set_invalid(vector, row);
            continue;
        }
        switch (format) {
        case 'c':
            static_cast<int8_t *>(duckdb_vector_get_data(vector))[row] =
                static_cast<const int8_t *>(array.buffers[data_buffer])[source];
            break;
        case 's':
            static_cast<int16_t *>(duckdb_vector_get_data(vector))[row] =
                static_cast<const int16_t *>(array.buffers[data_buffer])[source];
            break;
        case 'i':
            static_cast<int32_t *>(duckdb_vector_get_data(vector))[row] =
                static_cast<const int32_t *>(array.buffers[data_buffer])[source];
            break;
        case 'l':
            static_cast<int64_t *>(duckdb_vector_get_data(vector))[row] =
                static_cast<const int64_t *>(array.buffers[data_buffer])[source];
            break;
        case 'f':
            static_cast<float *>(duckdb_vector_get_data(vector))[row] =
                static_cast<const float *>(array.buffers[data_buffer])[source];
            break;
        case 'g':
            static_cast<double *>(duckdb_vector_get_data(vector))[row] =
                static_cast<const double *>(array.buffers[data_buffer])[source];
            break;
        case 'b': {
            const auto *bits = static_cast<const uint8_t *>(array.buffers[data_buffer]);
            static_cast<bool *>(duckdb_vector_get_data(vector))[row] =
                (bits[source / 8] & (uint8_t(1) << (source % 8))) != 0;
            break;
        }
        case 'u': {
            const auto *offsets = static_cast<const int32_t *>(array.buffers[data_buffer]);
            const auto *data = static_cast<const char *>(array.buffers[data_buffer + 1]);
            const int32_t begin = offsets[source];
            const int32_t end = offsets[source + 1];
            duckdb_vector_assign_string_element_len(vector, row, data + begin,
                                                     static_cast<idx_t>(end - begin));
            break;
        }
        default:
            duckdb_function_set_error(info, "Oracle ADBC returned an unsupported Arrow format");
            return false;
        }
    }
    return true;
}

static void query_bind(duckdb_bind_info info) {
    if (duckdb_bind_get_parameter_count(info) != 2) {
        duckdb_bind_set_error(info, "orkidea_adbc_query requires (sql, uri)");
        return;
    }
    duckdb_value sql_value = duckdb_bind_get_parameter(info, 0);
    duckdb_value uri_value = duckdb_bind_get_parameter(info, 1);
    char *sql = duckdb_get_varchar(sql_value);
    char *uri = duckdb_get_varchar(uri_value);
    duckdb_destroy_value(&sql_value);
    duckdb_destroy_value(&uri_value);
    if (!sql || !uri) {
        duckdb_free(sql);
        duckdb_free(uri);
        duckdb_bind_set_error(info, "orkidea_adbc_query arguments must be non-null strings");
        return;
    }

    auto *state = new OracleAdbcQueryState();
    const char *library_path = std::getenv("ORACLE_ADBC_LIBRARY");
    if (!library_path) {
        duckdb_free(sql);
        duckdb_free(uri);
        delete state;
        duckdb_bind_set_error(info, "ORACLE_ADBC_LIBRARY must point to libadbc_driver_orkidea.so");
        return;
    }
    state->library = dlopen(library_path, RTLD_NOW | RTLD_LOCAL);
    if (!state->library) {
        std::string message = "failed to load ORACLE_ADBC_LIBRARY: ";
        message += dlerror();
        duckdb_free(sql);
        duckdb_free(uri);
        delete state;
        duckdb_bind_set_error(info, message.c_str());
        return;
    }
    auto init = reinterpret_cast<AdbcDriverInitFunc>(dlsym(state->library, "OrkideaDriverInit"));
    if (!init) {
        duckdb_free(sql);
        duckdb_free(uri);
        release_query_state(state);
        duckdb_bind_set_error(info, "ORACLE_ADBC_LIBRARY lacks OrkideaDriverInit");
        return;
    }

    AdbcError error{};
    bool ok = check_status(init(ADBC_VERSION_1_1_0, &state->driver, &error), error, info, "driver init") &&
              check_status(state->driver.DatabaseNew(&state->database, &error), error, info, "database new") &&
              check_status(state->driver.DatabaseSetOption(&state->database, "uri", uri, &error), error, info,
                           "database set uri") &&
              check_status(state->driver.DatabaseInit(&state->database, &error), error, info, "database init") &&
              check_status(state->driver.ConnectionNew(&state->connection, &error), error, info, "connection new") &&
              check_status(state->driver.ConnectionInit(&state->connection, &state->database, &error), error, info,
                           "connection init") &&
              check_status(state->driver.StatementNew(&state->connection, &state->statement, &error), error, info,
                           "statement new") &&
              check_status(state->driver.StatementSetSqlQuery(&state->statement, sql, &error), error, info,
                           "statement set query") &&
              check_status(state->driver.StatementExecuteQuery(&state->statement, &state->stream, nullptr, &error),
                           error, info, "statement execute query");
    duckdb_free(sql);
    duckdb_free(uri);
    if (!ok || state->stream.get_schema(&state->stream, &state->schema)) {
        if (ok) {
            duckdb_bind_set_error(info, "Oracle ADBC returned no readable Arrow schema");
        }
        release_query_state(state);
        return;
    }
    if (state->schema.n_children <= 0) {
        release_query_state(state);
        duckdb_bind_set_error(info, "Oracle ADBC returned an empty schema");
        return;
    }
    for (int64_t index = 0; index < state->schema.n_children; index++) {
        auto *column = state->schema.children[index];
        auto type = logical_type_for_format(column->format);
        if (!type) {
            release_query_state(state);
            duckdb_bind_set_error(info, "Oracle ADBC query contains an unsupported Arrow type");
            return;
        }
        duckdb_bind_add_result_column(info, column->name ? column->name : "column", type);
        duckdb_destroy_logical_type(&type);
    }
    duckdb_bind_set_bind_data(info, state, release_query_state);
    duckdb_bind_set_cardinality(info, 0, false);
}

static void query_init(duckdb_init_info info) {
    duckdb_init_set_init_data(info, duckdb_init_get_bind_data(info), nullptr);
    duckdb_init_set_max_threads(info, 1);
}

static void query_function(duckdb_function_info info, duckdb_data_chunk output) {
    auto *state = static_cast<OracleAdbcQueryState *>(duckdb_function_get_init_data(info));
    if (!state) {
        duckdb_function_set_error(info, "Oracle ADBC query state is missing");
        return;
    }
    if (!state->current_valid) {
        if (state->stream.get_next(&state->stream, &state->current)) {
            duckdb_function_set_error(info, "Oracle ADBC Arrow stream returned an error");
            return;
        }
        state->current_valid = state->current.release != nullptr && state->current.length > 0;
        state->current_offset = 0;
        if (!state->current_valid) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
    }
    const idx_t count = static_cast<idx_t>(std::min<int64_t>(state->current.length - state->current_offset, 1024));
    for (int64_t index = 0; index < state->schema.n_children; index++) {
        auto *column = state->schema.children[index];
        auto *array = state->current.children[index];
        if (!array) {
            duckdb_function_set_error(info, "Oracle ADBC returned a record batch with a missing column array");
            return;
        }
        auto vector = duckdb_data_chunk_get_vector(output, static_cast<idx_t>(index));
        if (!copy_arrow_column(vector, *column, *array, state->current_offset, count, info)) {
            return;
        }
    }
    state->current_offset += static_cast<int64_t>(count);
    if (state->current_offset >= state->current.length) {
        state->current.release(&state->current);
        state->current = {};
        state->current_valid = false;
    }
    duckdb_data_chunk_set_size(output, count);
}

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, duckdb_extension_access *access) {
    auto function = duckdb_create_table_function();
    duckdb_table_function_set_name(function, "orkidea_adbc_query");
    auto varchar = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(function, varchar);
    duckdb_table_function_add_parameter(function, varchar);
    duckdb_destroy_logical_type(&varchar);
    duckdb_table_function_set_bind(function, query_bind);
    duckdb_table_function_set_init(function, query_init);
    duckdb_table_function_set_function(function, query_function);
    if (duckdb_register_table_function(connection, function) != DuckDBSuccess) {
        access->set_error(info, "failed to register orkidea_adbc_query");
        duckdb_destroy_table_function(&function);
        return false;
    }
    duckdb_destroy_table_function(&function);
    return true;
}
