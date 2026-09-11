#include "duckdb/main/capi/capi_internal.hpp"
#include "fts_extension.hpp"

#include <memory>

static_assert(sizeof(duckdb::DatabaseWrapper) == sizeof(std::shared_ptr<duckdb::DuckDB>),
              "DuckDB's C API database wrapper layout changed");

extern "C" int orbit_duckdb_load_fts(duckdb_database database) {
    if (!database) {
        return 1;
    }
    try {
        // DuckDB's C API uses this internal representation too, so the shim and
        // out-of-tree extension must stay aligned with the pinned DuckDB release.
        auto wrapper = reinterpret_cast<duckdb::DatabaseWrapper *>(database);
        wrapper->database->LoadStaticExtension<duckdb::FtsExtension>();
        return 0;
    } catch (...) {
        return 1;
    }
}
