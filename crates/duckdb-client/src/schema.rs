pub const SCHEMA_DDL: &str = "
CREATE TABLE IF NOT EXISTS gl_directory (
    id BIGINT NOT NULL,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_file (
    id BIGINT NOT NULL,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    extension VARCHAR,
    language VARCHAR,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_definition (
    id BIGINT NOT NULL,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    fqn VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    definition_type VARCHAR NOT NULL,
    start_line BIGINT,
    end_line BIGINT,
    start_byte BIGINT,
    end_byte BIGINT,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_imported_symbol (
    id BIGINT NOT NULL,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    import_type VARCHAR,
    import_path VARCHAR,
    identifier_name VARCHAR,
    identifier_alias VARCHAR,
    start_line BIGINT,
    end_line BIGINT,
    start_byte BIGINT,
    end_byte BIGINT,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_edge (
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    source_id BIGINT NOT NULL,
    source_kind VARCHAR NOT NULL,
    relationship_kind VARCHAR NOT NULL,
    target_id BIGINT NOT NULL,
    target_kind VARCHAR NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);
";

pub const CODE_GRAPH_TABLES: &[&str] = &[
    "gl_directory",
    "gl_file",
    "gl_definition",
    "gl_imported_symbol",
    "gl_edge",
];
