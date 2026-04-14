/// Manifest table DDL for DuckDB. This table is outside the ontology:
/// it tracks indexed repos and maps repo paths to project IDs.
pub const MANIFEST_DDL: &str = "\
CREATE TYPE IF NOT EXISTS repo_status AS ENUM ('pending', 'indexing', 'indexed', 'error');

CREATE TABLE IF NOT EXISTS _orbit_manifest (
    repo_path VARCHAR PRIMARY KEY,
    project_id BIGINT NOT NULL,
    parent_repo_path VARCHAR,
    branch VARCHAR,
    commit_sha VARCHAR,
    status repo_status NOT NULL DEFAULT 'pending',
    last_indexed_at TIMESTAMP,
    error_message VARCHAR
);";
