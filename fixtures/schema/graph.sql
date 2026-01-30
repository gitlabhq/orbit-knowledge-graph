-- Watermark tables

CREATE TABLE IF NOT EXISTS global_indexing_watermark (
    watermark DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(_version) ORDER BY tuple();

CREATE TABLE IF NOT EXISTS namespace_indexing_watermark (
    namespace Int64,
    watermark DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(_version) ORDER BY (namespace) PRIMARY KEY(namespace);

-- Graph node tables

CREATE TABLE IF NOT EXISTS gl_users (
    id Int64,
    username String DEFAULT '',
    email String DEFAULT '',
    name String DEFAULT '',
    first_name String DEFAULT '',
    last_name String DEFAULT '',
    state String DEFAULT '',
    public_email Nullable(String),
    preferred_language Nullable(String),
    last_activity_on Nullable(Date32),
    private_profile Bool DEFAULT false,
    is_admin Bool DEFAULT false,
    is_auditor Bool DEFAULT false,
    is_external Bool DEFAULT false,
    user_type String DEFAULT '',
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (id) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS gl_groups (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level Nullable(String),
    full_path Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String default '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_projects (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level Nullable(String),
    full_path Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    archived Nullable(Bool),
    star_count Nullable(Int64),
    last_activity_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String default '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_notes (
    id Int64,
    note Nullable(String),
    noteable_type String DEFAULT '',
    noteable_id Nullable(Int64),
    system Bool DEFAULT false,
    line_code Nullable(String),
    commit_id Nullable(String),
    discussion_id Nullable(String),
    resolved_at Nullable(DateTime64(6, 'UTC')),
    internal Bool DEFAULT false,
    confidential Nullable(Bool),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_merge_requests (
    id Int64,
    iid Nullable(Int64),
    title String DEFAULT '',
    description String DEFAULT '',
    source_branch String DEFAULT '',
    target_branch String DEFAULT '',
    state String DEFAULT '',
    merge_status String DEFAULT 'unchecked',
    draft Bool DEFAULT false,
    squash Bool DEFAULT false,
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    merge_commit_sha Nullable(String),
    discussion_locked Nullable(Bool),
    prepared_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_merge_request_diffs (
    id Int64,
    merge_request_id Int64,
    state Nullable(String),
    base_commit_sha Nullable(String),
    head_commit_sha Nullable(String),
    start_commit_sha Nullable(String),
    commits_count Nullable(Int64),
    files_count Nullable(Int64),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_merge_request_diff_files (
    id Int64,
    merge_request_diff_id Int64,
    too_large Bool DEFAULT false,
    new_path Nullable(String),
    old_path String DEFAULT '',
    new_file Bool DEFAULT false,
    renamed_file Bool DEFAULT false,
    deleted_file Bool DEFAULT false,
    binary Nullable(Bool),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_milestones (
    id Int64,
    iid Nullable(Int64),
    title String DEFAULT '',
    description Nullable(String),
    state Nullable(String),
    due_date Nullable(Date32),
    start_date Nullable(Date32),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_labels (
    id Int64,
    title Nullable(String),
    description Nullable(String),
    color Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_work_items (
    id Int64,
    iid Nullable(Int64),
    title String DEFAULT '',
    description Nullable(String),
    state String DEFAULT '',
    work_item_type String DEFAULT '',
    confidential Bool DEFAULT false,
    due_date Nullable(Date32),
    start_date Nullable(Date32),
    weight Nullable(Int64),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    closed_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id);

CREATE TABLE IF NOT EXISTS gl_edges (
    source_id Int64,
    source_kind String,
    relationship_kind String,
    target_id Int64,
    target_kind String,
    _version DateTime64(6, 'UTC') DEFAULT now(),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (source_id, target_id) PRIMARY KEY (source_id, target_id);
