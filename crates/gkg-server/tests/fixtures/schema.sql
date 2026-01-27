-- User tables
CREATE TABLE IF NOT EXISTS test.siphon_users (
    id Int64,
    email String DEFAULT '',
    name String DEFAULT '',
    username String DEFAULT '',
    first_name String DEFAULT '',
    last_name String DEFAULT '',
    state String DEFAULT '',
    public_email Nullable(String),
    preferred_language Nullable(String),
    last_activity_on Nullable(Date32),
    private_profile Bool DEFAULT false,
    admin Bool DEFAULT false,
    auditor Bool DEFAULT false,
    external Bool DEFAULT false,
    user_type Int8 DEFAULT 0,
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    _siphon_replicated_at DateTime64(6, 'UTC') DEFAULT now(),
    _siphon_deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id;

CREATE TABLE IF NOT EXISTS test.users (
    id Int64,
    username Nullable(String),
    email Nullable(String),
    name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    state Nullable(String),
    public_email Nullable(String),
    preferred_language Nullable(String),
    last_activity_on Nullable(String),
    private_profile Nullable(Bool),
    is_admin Nullable(Bool),
    is_auditor Nullable(Bool),
    is_external Nullable(Bool),
    user_type Nullable(String),
    created_at Nullable(String),
    updated_at Nullable(String)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS test.user_indexing_watermark (
    watermark DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(_version) ORDER BY tuple();

-- Namespace/Group source tables
CREATE TABLE IF NOT EXISTS test.siphon_namespaces (
    id Int64,
    name String,
    path String,
    owner_id Nullable(Int64),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    type LowCardinality(String) DEFAULT 'User',
    description String DEFAULT '',
    visibility_level Int64 DEFAULT 20,
    parent_id Nullable(Int64),
    traversal_ids Array(Int64) DEFAULT [],
    organization_id Int64 DEFAULT 0,
    _siphon_replicated_at DateTime64(6, 'UTC') DEFAULT now(),
    _siphon_deleted Bool DEFAULT false,
    state Int8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id;

CREATE TABLE IF NOT EXISTS test.siphon_namespace_details (
    namespace_id Int64,
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    cached_markdown_version Nullable(Int64),
    description Nullable(String),
    description_html Nullable(String),
    creator_id Nullable(Int64),
    deleted_at Nullable(DateTime64(6, 'UTC')),
    _siphon_replicated_at DateTime64(6, 'UTC') DEFAULT now(),
    _siphon_deleted Bool DEFAULT false,
    state_metadata String DEFAULT '{}'
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY namespace_id
ORDER BY namespace_id;

CREATE TABLE IF NOT EXISTS test.namespace_traversal_paths (
    id Int64 DEFAULT 0,
    traversal_path String DEFAULT '0/',
    version DateTime64(6, 'UTC') DEFAULT now(),
    deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512;

-- Group destination tables
CREATE TABLE IF NOT EXISTS test.groups (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level Nullable(String),
    path Nullable(String),
    parent_id Nullable(Int64),
    owner_id Nullable(Int64),
    created_at Nullable(String),
    updated_at Nullable(String),
    traversal_path Nullable(String),
    deleted Nullable(Bool)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS test.edges (
    source_id Int64,
    source_kind String,
    relationship_kind String,
    target_id Int64,
    target_kind String
) ENGINE = MergeTree() ORDER BY (source_id, target_id);

-- Project source tables
CREATE TABLE IF NOT EXISTS test.siphon_projects (
    id Int64,
    name Nullable(String),
    path Nullable(String),
    description Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    creator_id Nullable(Int64),
    namespace_id Int64,
    last_activity_at Nullable(DateTime64(6, 'UTC')),
    visibility_level Int64 DEFAULT 0,
    archived Bool DEFAULT false,
    star_count Int64 DEFAULT 0,
    project_namespace_id Nullable(Int64),
    organization_id Nullable(Int64),
    _siphon_replicated_at DateTime64(6, 'UTC') DEFAULT now(),
    _siphon_deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id;

CREATE TABLE IF NOT EXISTS test.project_namespace_traversal_paths (
    id Int64 DEFAULT 0,
    traversal_path String DEFAULT '0/',
    version DateTime64(6, 'UTC') DEFAULT now(),
    deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512;

-- Project destination tables
CREATE TABLE IF NOT EXISTS test.projects (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level Nullable(String),
    path Nullable(String),
    namespace_id Nullable(Int64),
    creator_id Nullable(Int64),
    created_at Nullable(String),
    updated_at Nullable(String),
    archived Nullable(Bool),
    star_count Nullable(Int64),
    last_activity_at Nullable(String),
    traversal_path Nullable(String),
    deleted Nullable(Bool)
) ENGINE = MergeTree() ORDER BY id;

-- Namespace watermark table
CREATE TABLE IF NOT EXISTS test.namespace_indexing_watermark (
    namespace Int64,
    watermark DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(_version) ORDER BY namespace
