CREATE TYPE IF NOT EXISTS repo_status AS ENUM ('pending', 'indexing', 'indexed', 'error');

CREATE TABLE IF NOT EXISTS _orbit_manifest (
    repo_path VARCHAR NOT NULL,
    project_id BIGINT NOT NULL,
    parent_repo_path VARCHAR,
    branch VARCHAR,
    commit_sha VARCHAR NOT NULL DEFAULT '',
    database_path VARCHAR,
    status repo_status NOT NULL DEFAULT 'pending',
    last_indexed_at TIMESTAMP,
    error_message VARCHAR,
    PRIMARY KEY (repo_path, commit_sha)
);

CREATE TABLE IF NOT EXISTS _orbit_incremental_state (
    project_id BIGINT PRIMARY KEY,
    state JSON NOT NULL
);
