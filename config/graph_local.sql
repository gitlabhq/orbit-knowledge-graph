-- DuckDB schema for local code graph tables.
--
-- Mirrors the code-indexing subset of graph.sql (ClickHouse).
-- Differences:
--   - No ENGINE, CODEC, PROJECTION, INDEX, or SETTINGS clauses
--   - No traversal_path — local mode has no multi-tenant namespace scoping
--   - _version is BIGINT (not DateTime64) — local mode uses a simple counter
--   - No _deleted column — local mode does full delete-and-reinsert

CREATE TABLE IF NOT EXISTS gl_directory (
    id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_file (
    id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    extension VARCHAR NOT NULL DEFAULT '',
    language VARCHAR NOT NULL DEFAULT '',
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_definition (
    id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    fqn VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    definition_type VARCHAR NOT NULL,
    start_line BIGINT NOT NULL,
    end_line BIGINT NOT NULL,
    start_byte BIGINT NOT NULL,
    end_byte BIGINT NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_imported_symbol (
    id BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    import_type VARCHAR NOT NULL,
    import_path VARCHAR NOT NULL,
    identifier_name VARCHAR,
    identifier_alias VARCHAR,
    start_line BIGINT NOT NULL,
    end_line BIGINT NOT NULL,
    start_byte BIGINT NOT NULL,
    end_byte BIGINT NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_edge (
    source_id BIGINT NOT NULL,
    source_kind VARCHAR NOT NULL,
    relationship_kind VARCHAR NOT NULL,
    target_id BIGINT NOT NULL,
    target_kind VARCHAR NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);
