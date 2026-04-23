-- Eval harness schema. Executed once via ensure_schema().

CREATE TABLE IF NOT EXISTS servers (
    arm VARCHAR PRIMARY KEY,
    status VARCHAR NOT NULL,
    port INTEGER NOT NULL,
    pid INTEGER,
    work_dir VARCHAR,
    log_path VARCHAR,
    started_at TIMESTAMP,
    stopped_at TIMESTAMP,
    error VARCHAR
);

CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    arms VARCHAR[],
    task_count INTEGER,
    status VARCHAR NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS task_results (
    run_id VARCHAR NOT NULL,
    task_id VARCHAR NOT NULL,
    arm VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    timestamp VARCHAR NOT NULL,
    structured_output JSON,
    error VARCHAR,
    error_type VARCHAR,
    session_id VARCHAR,
    steps INTEGER DEFAULT 0,
    tool_calls INTEGER DEFAULT 0,
    tokens_input INTEGER DEFAULT 0,
    tokens_output INTEGER DEFAULT 0,
    tokens_cache_read INTEGER DEFAULT 0,
    cost DOUBLE DEFAULT 0.0,
    duration_ms INTEGER DEFAULT 0,
    PRIMARY KEY (run_id, arm, task_id)
);

CREATE TABLE IF NOT EXISTS snapshots (
    run_id VARCHAR NOT NULL,
    arm VARCHAR NOT NULL,
    task_id VARCHAR NOT NULL,
    data JSON NOT NULL,
    PRIMARY KEY (run_id, arm, task_id)
);

CREATE TABLE IF NOT EXISTS scores (
    run_id VARCHAR NOT NULL,
    arm VARCHAR NOT NULL,
    task_id VARCHAR NOT NULL,
    evaluator VARCHAR NOT NULL,
    score JSON NOT NULL,
    PRIMARY KEY (run_id, arm, task_id, evaluator)
);

CREATE TABLE IF NOT EXISTS run_configs (
    run_id VARCHAR NOT NULL,
    config_name VARCHAR NOT NULL,
    config_version VARCHAR NOT NULL,
    config_hash VARCHAR NOT NULL,
    config JSON NOT NULL,
    files JSON NOT NULL,
    PRIMARY KEY (run_id)
);
