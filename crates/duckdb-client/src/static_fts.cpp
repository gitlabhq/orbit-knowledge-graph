#include "duckdb/main/capi/capi_internal.hpp"
#include "fts_extension.hpp"

#include <cstring>
#include <exception>
#include <memory>

static_assert(sizeof(duckdb::DatabaseWrapper) == sizeof(std::shared_ptr<duckdb::DuckDB>),
              "DuckDB's C API database wrapper layout changed");

static void SetError(char **out_error, const char *message) noexcept {
    if (!out_error) {
        return;
    }
    const auto length = std::strlen(message);
    auto buffer = static_cast<char *>(duckdb_malloc(length + 1));
    if (!buffer) {
        return;
    }
    std::memcpy(buffer, message, length + 1);
    *out_error = buffer;
}

extern "C" int orbit_duckdb_load_fts(duckdb_database database, char **out_error) {
    if (out_error) {
        *out_error = nullptr;
    }
    if (!database) {
        SetError(out_error, "DuckDB database handle is null");
        return 1;
    }
    try {
        // DuckDB's C API uses this internal representation too, so the shim and
        // out-of-tree extension must stay aligned with the pinned DuckDB release.
        auto wrapper = reinterpret_cast<duckdb::DatabaseWrapper *>(database);
        wrapper->database->LoadStaticExtension<duckdb::FtsExtension>();
        return 0;
    } catch (const std::exception &error) {
        SetError(out_error, error.what());
        return 1;
    } catch (...) {
        SetError(out_error, "unknown C++ exception");
        return 1;
    }
}
