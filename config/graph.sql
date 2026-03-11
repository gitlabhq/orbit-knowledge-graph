-- Checkpoint table

CREATE TABLE IF NOT EXISTS checkpoint (
    key String,
    watermark DateTime64(6, 'UTC'),
    cursor_values String DEFAULT '',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (key)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

-- Namespace deletion schedule

CREATE TABLE IF NOT EXISTS namespace_deletion_schedule (
    namespace_id Int64,
    traversal_path String,
    scheduled_deletion_date DateTime64(6, 'UTC'),
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (namespace_id, traversal_path)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

-- Graph node tables

CREATE TABLE IF NOT EXISTS gl_user (
    id Int64,
    username String DEFAULT '',
    email String DEFAULT '',
    name String DEFAULT '',
    first_name String DEFAULT '',
    last_name String DEFAULT '',
    state String DEFAULT '',
    avatar_url Nullable(String),
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
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (id) PRIMARY KEY (id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_group (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level Nullable(String),
    full_path Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String default '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_project (
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
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_note (
    id Int64,
    note Nullable(String),
    noteable_type String DEFAULT '',
    noteable_id Nullable(Int64),
    line_code Nullable(String),
    commit_id Nullable(String),
    discussion_id Nullable(String),
    resolved_at Nullable(DateTime64(6, 'UTC')),
    internal Bool DEFAULT false,
    confidential Nullable(Bool),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_merge_request (
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
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_merge_request_diff (
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
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_merge_request_diff_file (
    id Int64,
    merge_request_id Int64,
    merge_request_diff_id Int64,
    too_large Bool DEFAULT false,
    new_path Nullable(String),
    old_path String DEFAULT '',
    new_file Bool DEFAULT false,
    renamed_file Bool DEFAULT false,
    deleted_file Bool DEFAULT false,
    binary Nullable(Bool),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_milestone (
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
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_label (
    id Int64,
    title Nullable(String),
    description Nullable(String),
    color Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_work_item (
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
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_edge (
    traversal_path String DEFAULT '0/',
    source_id Int64,
    source_kind String,
    relationship_kind String,
    target_id Int64,
    target_kind String,
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind)
PRIMARY KEY (traversal_path, source_id, source_kind, relationship_kind)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

-- CI graph tables

CREATE TABLE IF NOT EXISTS gl_pipeline (
    id Int64,
    iid Nullable(Int64),
    sha Nullable(String),
    ref Nullable(String),
    status String DEFAULT '',
    source String DEFAULT '',
    tag Bool DEFAULT false,
    duration Nullable(Int64),
    failure_reason Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    started_at Nullable(DateTime64(6, 'UTC')),
    finished_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_stage (
    id Int64,
    name Nullable(String),
    status String DEFAULT '',
    position Nullable(Int64),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_job (
    id Int64,
    name Nullable(String),
    status String DEFAULT '',
    ref Nullable(String),
    tag Nullable(Bool),
    allow_failure Bool DEFAULT false,
    coverage Nullable(String),
    environment Nullable(String),
    `when` Nullable(String),
    retried Nullable(Bool),
    failure_reason Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    started_at Nullable(DateTime64(6, 'UTC')),
    finished_at Nullable(DateTime64(6, 'UTC')),
    queued_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

-- Security graph tables

CREATE TABLE IF NOT EXISTS gl_vulnerability (
    id Int64,
    title String DEFAULT '',
    description Nullable(String),
    state String DEFAULT '',
    severity String DEFAULT '',
    report_type String DEFAULT '',
    resolved_on_default_branch Bool DEFAULT false,
    present_on_default_branch Bool DEFAULT true,
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    detected_at Nullable(DateTime64(6, 'UTC')),
    resolved_at Nullable(DateTime64(6, 'UTC')),
    confirmed_at Nullable(DateTime64(6, 'UTC')),
    dismissed_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_vulnerability_scanner (
    id Int64,
    external_id String DEFAULT '',
    name String DEFAULT '',
    vendor String DEFAULT 'GitLab',
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_vulnerability_identifier (
    id Int64,
    external_type String DEFAULT '',
    external_id String DEFAULT '',
    name String DEFAULT '',
    url Nullable(String),
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_finding (
    id Int64,
    uuid String DEFAULT '',
    name Nullable(String),
    description Nullable(String),
    solution Nullable(String),
    severity String DEFAULT '',
    deduplicated Bool DEFAULT false,
    overridden_uuid Nullable(String),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_security_scan (
    id Int64,
    scan_type String DEFAULT '',
    status String DEFAULT '',
    latest Bool DEFAULT true,
    created_at DateTime64(6, 'UTC') DEFAULT now(),
    updated_at DateTime64(6, 'UTC') DEFAULT now(),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_vulnerability_occurrence (
    id Int64,
    uuid String DEFAULT '',
    name String DEFAULT '',
    description Nullable(String),
    solution Nullable(String),
    cve Nullable(String),
    location Nullable(String),
    location_fingerprint String DEFAULT '',
    severity String DEFAULT '',
    report_type String DEFAULT '',
    detection_method String DEFAULT '',
    created_at DateTime64(6, 'UTC') DEFAULT now(),
    updated_at DateTime64(6, 'UTC') DEFAULT now(),
    detected_at Nullable(DateTime64(6, 'UTC')),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_branch (
    id Int64,
    project_id Int64,
    name String DEFAULT '',
    protected Nullable(Bool),
    is_default Nullable(Bool),
    traversal_path String DEFAULT '0/',
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

-- Code indexing tables

CREATE TABLE IF NOT EXISTS gl_directory (
    id Int64,
    traversal_path String,
    project_id Int64,
    branch String,
    path String,
    name String,
    _version  DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false,
    PROJECTION id_lookup (SELECT * ORDER BY id)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (traversal_path, project_id, branch, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_file (
    id Int64,
    traversal_path String,
    project_id Int64,
    branch String,
    path String,
    name String,
    extension String,
    language String,
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false,
    PROJECTION id_lookup (SELECT * ORDER BY id)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (traversal_path, project_id, branch, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_definition (
    id Int64,
    traversal_path String,
    project_id Int64,
    branch String,
    file_path String,
    fqn String,
    name String,
    definition_type String,
    start_line Int64,
    end_line Int64,
    start_byte Int64,
    end_byte Int64,
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false,
    PROJECTION id_lookup (SELECT * ORDER BY id)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (traversal_path, project_id, branch, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_imported_symbol (
    id Int64,
    traversal_path String,
    project_id Int64,
    branch String,
    file_path String,
    import_type String,
    import_path String,
    identifier_name Nullable(String),
    identifier_alias Nullable(String),
    start_line Int64,
    end_line Int64,
    start_byte Int64,
    end_byte Int64,
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false,
    PROJECTION id_lookup (SELECT * ORDER BY id)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (traversal_path, project_id, branch, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS code_indexing_checkpoint (
    traversal_path String,
    project_id Int64,
    branch String,
    last_event_id Int64,
    last_commit String,
    indexed_at DateTime64(6, 'UTC'),
    _version UInt64,
    _deleted Bool DEFAULT false,
    PROJECTION project_lookup (SELECT * ORDER BY project_id)
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, project_id, branch)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', allow_experimental_replacing_merge_with_cleanup = 1;

