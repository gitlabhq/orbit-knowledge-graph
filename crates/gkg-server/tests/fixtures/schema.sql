CREATE TABLE IF NOT EXISTS test.siphon_users
(
    `id` Int64,
    `email` String DEFAULT '',
    `sign_in_count` Int64 DEFAULT 0,
    `current_sign_in_at` Nullable(DateTime64(6, 'UTC')),
    `last_sign_in_at` Nullable(DateTime64(6, 'UTC')),
    `current_sign_in_ip` Nullable(String),
    `last_sign_in_ip` Nullable(String),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `name` String DEFAULT '',
    `admin` Bool DEFAULT false,
    `projects_limit` Int64,
    `failed_attempts` Int64 DEFAULT 0,
    `locked_at` Nullable(DateTime64(6, 'UTC')),
    `username` String DEFAULT '',
    `can_create_group` Bool DEFAULT true,
    `can_create_team` Bool DEFAULT true,
    `state` String DEFAULT '',
    `color_scheme_id` Int64 DEFAULT 1,
    `created_by_id` Nullable(Int64),
    `last_credential_check_at` Nullable(DateTime64(6, 'UTC')),
    `avatar` Nullable(String),
    `unconfirmed_email` String DEFAULT '',
    `hide_no_ssh_key` Bool DEFAULT false,
    `admin_email_unsubscribed_at` Nullable(DateTime64(6, 'UTC')),
    `notification_email` Nullable(String),
    `hide_no_password` Bool DEFAULT false,
    `password_automatically_set` Bool DEFAULT false,
    `public_email` Nullable(String),
    `dashboard` Int64 DEFAULT 0,
    `project_view` Int64 DEFAULT 2,
    `consumed_timestep` Nullable(Int64),
    `layout` Int64 DEFAULT 0,
    `hide_project_limit` Bool DEFAULT false,
    `note` Nullable(String),
    `otp_grace_period_started_at` Nullable(DateTime64(6, 'UTC')),
    `external` Bool DEFAULT false,
    `auditor` Bool DEFAULT false,
    `require_two_factor_authentication_from_group` Bool DEFAULT false,
    `two_factor_grace_period` Int64 DEFAULT 48,
    `last_activity_on` Nullable(Date32),
    `notified_of_own_activity` Nullable(Bool) DEFAULT false,
    `preferred_language` Nullable(String),
    `theme_id` Nullable(Int8),
    `accepted_term_id` Nullable(Int64),
    `private_profile` Bool DEFAULT false,
    `roadmap_layout` Nullable(Int8),
    `include_private_contributions` Nullable(Bool),
    `commit_email` Nullable(String),
    `group_view` Nullable(Int64),
    `managing_group_id` Nullable(Int64),
    `first_name` String DEFAULT '',
    `last_name` String DEFAULT '',
    `user_type` Int8 DEFAULT 0,
    `onboarding_in_progress` Bool DEFAULT false,
    `color_mode_id` Int8 DEFAULT 1,
    `composite_identity_enforced` Bool DEFAULT false,
    `organization_id` Int64,
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS test.users (
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
    updated_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS test.user_indexing_watermark (
    watermark DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(_version) ORDER BY tuple();

-- Namespace/Group source tables
CREATE TABLE IF NOT EXISTS test.siphon_namespaces
(
    `id` Int64,
    `name` String,
    `path` String,
    `owner_id` Nullable(Int64),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `type` LowCardinality(String) DEFAULT 'User',
    `description` String DEFAULT '',
    `visibility_level` Int64 DEFAULT 20,
    `parent_id` Nullable(Int64),
    `traversal_ids` Array(Int64) DEFAULT [],
    `organization_id` Int64 DEFAULT 0,
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    `state` Int8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id;

CREATE TABLE IF NOT EXISTS test.siphon_namespace_details
(
    `namespace_id` Int64,
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `cached_markdown_version` Nullable(Int64),
    `description` Nullable(String),
    `description_html` Nullable(String),
    `creator_id` Nullable(Int64),
    `deleted_at` Nullable(DateTime64(6, 'UTC')),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    `state_metadata` String DEFAULT '{}'
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY namespace_id
ORDER BY namespace_id;

CREATE TABLE IF NOT EXISTS test.namespace_traversal_paths
(
    `id` Int64 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `version` DateTime64(6, 'UTC') DEFAULT now(),
    `deleted` Bool DEFAULT false
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512;

-- Group destination table
CREATE TABLE IF NOT EXISTS test.groups (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level Nullable(String),
    path Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path Nullable(String),
    deleted Nullable(Bool)
) ENGINE = MergeTree() ORDER BY id;

-- Edges table for relationships
CREATE TABLE IF NOT EXISTS test.edges (
    source_id Int64,
    source_kind String,
    relationship_kind String,
    target_id Int64,
    target_kind String
) ENGINE = MergeTree() ORDER BY (source_id, target_id);

-- Project source tables
CREATE TABLE IF NOT EXISTS test.siphon_projects
(
    `id` Int64,
    `name` Nullable(String),
    `path` Nullable(String),
    `description` Nullable(String),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `creator_id` Nullable(Int64),
    `namespace_id` Int64,
    `last_activity_at` Nullable(DateTime64(6, 'UTC')),
    `visibility_level` Int64 DEFAULT 0,
    `archived` Bool DEFAULT false,
    `star_count` Int64 DEFAULT 0,
    `project_namespace_id` Nullable(Int64),
    `organization_id` Nullable(Int64),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id;

CREATE TABLE IF NOT EXISTS test.project_namespace_traversal_paths
(
    `id` Int64 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `version` DateTime64(6, 'UTC') DEFAULT now(),
    `deleted` Bool DEFAULT false
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512;

-- Project destination table
CREATE TABLE IF NOT EXISTS test.projects (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level Nullable(String),
    path Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    archived Nullable(Bool),
    star_count Nullable(Int64),
    last_activity_at Nullable(DateTime64(6, 'UTC')),
    traversal_path Nullable(String),
    deleted Nullable(Bool)
) ENGINE = MergeTree() ORDER BY id;

-- Namespace watermark table
CREATE TABLE IF NOT EXISTS test.namespace_indexing_watermark (
    namespace Int64,
    watermark DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(_version) ORDER BY namespace
