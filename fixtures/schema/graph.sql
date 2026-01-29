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
