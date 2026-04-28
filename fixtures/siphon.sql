-- Siphon source tables for user data
CREATE TABLE IF NOT EXISTS siphon_users
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

-- Siphon source tables for namespace/group data
CREATE TABLE IF NOT EXISTS siphon_namespaces
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
    `state` Int8
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS siphon_namespace_details
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

CREATE TABLE IF NOT EXISTS namespace_traversal_paths
(
    `id` Int64 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `version` DateTime64(6, 'UTC') DEFAULT now(),
    `deleted` Bool DEFAULT false,
    PROJECTION by_traversal_path
    (
        SELECT *
        ORDER BY traversal_path
    )
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512, deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for project data
CREATE TABLE IF NOT EXISTS siphon_projects
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

CREATE TABLE IF NOT EXISTS project_namespace_traversal_paths
(
    `id` Int64 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `version` DateTime64(6, 'UTC') DEFAULT now(),
    `deleted` Bool DEFAULT false,
    PROJECTION by_traversal_path
    (
        SELECT *
        ORDER BY traversal_path
    )
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512, deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for routes (authoritative full_path)
CREATE TABLE IF NOT EXISTS siphon_routes
(
    `id` Int64 CODEC(DoubleDelta, ZSTD),
    `source_id` Int64 CODEC(ZSTD(1)),
    `source_type` LowCardinality(String) CODEC(LZ4),
    `path` String CODEC(ZSTD(3)),
    `created_at` DateTime64(6, 'UTC') CODEC(Delta, ZSTD(1)),
    `updated_at` DateTime64(6, 'UTC') CODEC(Delta, ZSTD(1)),
    `name` String,
    `namespace_id` Int64,
    `traversal_path` String DEFAULT '0/' CODEC(ZSTD(3)),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC') CODEC(ZSTD(1)),
    `_siphon_deleted` Bool DEFAULT FALSE CODEC(ZSTD(1)),
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, source_type, source_id, id)
ORDER BY (traversal_path, source_type, source_id, id)
SETTINGS index_granularity = 2048, deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for notes
CREATE TABLE IF NOT EXISTS siphon_notes
(
    `note` String,
    `noteable_type` LowCardinality(String),
    `author_id` Nullable(Int64),
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `project_id` Nullable(Int64),
    `line_code` Nullable(String),
    `commit_id` Nullable(String),
    `noteable_id` Int64,
    `system` Bool DEFAULT false,
    `st_diff` Nullable(String),
    `updated_by_id` Nullable(Int64),
    `type` LowCardinality(String),
    `position` Nullable(String),
    `original_position` Nullable(String),
    `resolved_at` Nullable(DateTime64(6, 'UTC')),
    `resolved_by_id` Nullable(Int64),
    `discussion_id` String,
    `change_position` Nullable(String),
    `resolved_by_push` Nullable(Bool),
    `review_id` Nullable(Int64),
    `confidential` Bool,
    `last_edited_at` Nullable(DateTime64(6, 'UTC')),
    `internal` Bool DEFAULT false,
    `id` Int64,
    `namespace_id` Nullable(Int64),
    `imported_from` Int8 DEFAULT 0,
    `organization_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, noteable_type, noteable_id, id)
ORDER BY (traversal_path, noteable_type, noteable_id, id);

-- Siphon source tables for merge requests
CREATE TABLE IF NOT EXISTS merge_requests
(
    `id` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `target_branch` String,
    `source_branch` String,
    `source_project_id` Nullable(Int64),
    `author_id` Nullable(Int64),
    `assignee_id` Nullable(Int64),
    `title` String CODEC(ZSTD(1)),
    `created_at` DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `updated_at` DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `milestone_id` Nullable(Int64),
    `merge_status` LowCardinality(String) DEFAULT 'unchecked',
    `target_project_id` Int64,
    `iid` Int64,
    `description` String CODEC(ZSTD(3)),
    `updated_by_id` Nullable(Int64),
    `merge_error` Nullable(String),
    `merge_params` Nullable(String),
    `merge_when_pipeline_succeeds` Bool DEFAULT false CODEC(ZSTD(1)),
    `merge_user_id` Nullable(Int64),
    `merge_commit_sha` Nullable(String),
    `approvals_before_merge` Nullable(Int64),
    `rebase_commit_sha` Nullable(String),
    `in_progress_merge_commit_sha` Nullable(String),
    `time_estimate` Nullable(Int64) DEFAULT 0,
    `squash` Bool DEFAULT false CODEC(ZSTD(1)),
    `cached_markdown_version` Nullable(Int64),
    `last_edited_at` Nullable(DateTime64(6, 'UTC')),
    `last_edited_by_id` Nullable(Int64),
    `merge_jid` String,
    `discussion_locked` Nullable(Bool) CODEC(ZSTD(1)),
    `latest_merge_request_diff_id` Nullable(Int64),
    `allow_maintainer_to_push` Nullable(Bool) DEFAULT true CODEC(ZSTD(1)),
    `state_id` Int16 DEFAULT 1,
    `rebase_jid` Nullable(String),
    `squash_commit_sha` Nullable(String),
    `merge_ref_sha` Nullable(String),
    `draft` Bool DEFAULT false CODEC(ZSTD(1)),
    `prepared_at` Nullable(DateTime64(6, 'UTC')),
    `merged_commit_sha` Nullable(String),
    `override_requested_changes` Bool DEFAULT false CODEC(ZSTD(1)),
    `head_pipeline_id` Nullable(Int64),
    `imported_from` Int16 DEFAULT 0,
    `retargeted` Bool DEFAULT false CODEC(ZSTD(1)),
    `traversal_path` String DEFAULT '0/' CODEC(ZSTD(3)),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now() CODEC(ZSTD(1)),
    `_siphon_deleted` Bool DEFAULT false CODEC(ZSTD(1)),
    `metric_latest_build_started_at` Nullable(DateTime64(6, 'UTC')),
    `metric_latest_build_finished_at` Nullable(DateTime64(6, 'UTC')),
    `metric_first_deployed_to_production_at` Nullable(DateTime64(6, 'UTC')),
    `metric_merged_at` Nullable(DateTime64(6, 'UTC')),
    `metric_merged_by_id` Nullable(Int64),
    `metric_latest_closed_by_id` Nullable(Int64),
    `metric_latest_closed_at` Nullable(DateTime64(6, 'UTC')),
    `metric_first_comment_at` Nullable(DateTime64(6, 'UTC')),
    `metric_first_commit_at` Nullable(DateTime64(6, 'UTC')),
    `metric_last_commit_at` Nullable(DateTime64(6, 'UTC')),
    `metric_diff_size` Nullable(Int64),
    `metric_modified_paths_size` Nullable(Int64),
    `metric_commits_count` Nullable(Int64),
    `metric_first_approved_at` Nullable(DateTime64(6, 'UTC')),
    `metric_first_reassigned_at` Nullable(DateTime64(6, 'UTC')),
    `metric_added_lines` Nullable(Int64),
    `metric_removed_lines` Nullable(Int64),
    `metric_first_contribution` Bool DEFAULT false,
    `metric_pipeline_id` Nullable(Int64),
    `metric_reviewer_first_assigned_at` Nullable(DateTime64(6, 'UTC')),
    `reviewers` Array(Tuple(
        user_id UInt64,
        state Int16,
        created_at DateTime64(6, 'UTC'))),
    `assignees` Array(Tuple(
        user_id UInt64,
        created_at DateTime64(6, 'UTC'))),
    `approvals` Array(Tuple(
        user_id UInt64,
        created_at DateTime64(6, 'UTC'))),
    `label_ids` Array(Tuple(
        label_id UInt64,
        created_at DateTime64(6, 'UTC'))),
    `award_emojis` Array(Tuple(
        name String,
        user_id UInt64,
        created_at DateTime64(6, 'UTC'))),
    PROJECTION pg_pkey_ordered
    (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS index_granularity = 2048, deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for merge request diffs
CREATE TABLE IF NOT EXISTS siphon_merge_request_diffs
(
    `id` Int64,
    `state` LowCardinality(Nullable(String)),
    `merge_request_id` Int64,
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `base_commit_sha` Nullable(String),
    `real_size` Nullable(String),
    `head_commit_sha` Nullable(String),
    `start_commit_sha` Nullable(String),
    `commits_count` Nullable(Int64),
    `external_diff` Nullable(String),
    `external_diff_store` Nullable(Int64) DEFAULT 1,
    `stored_externally` Bool DEFAULT false,
    `files_count` Nullable(Int16),
    `sorted` Bool DEFAULT false,
    `diff_type` Int8 DEFAULT 1,
    `patch_id_sha` Nullable(String),
    `project_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, merge_request_id, id)
ORDER BY (traversal_path, merge_request_id, id);

-- Siphon source tables for merge request diff files
CREATE TABLE IF NOT EXISTS siphon_merge_request_diff_files
(
    `merge_request_diff_id` Int64,
    `relative_order` Int64,
    `new_file` Bool,
    `renamed_file` Bool,
    `deleted_file` Bool,
    `too_large` Bool,
    `a_mode` String,
    `b_mode` String,
    `new_path` Nullable(String),
    `old_path` String,
    `diff` Nullable(String),
    `binary` Nullable(Bool),
    `external_diff_offset` Nullable(Int64),
    `external_diff_size` Nullable(Int64),
    `generated` Nullable(Bool),
    `encoded_file_path` Bool DEFAULT false,
    `project_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, merge_request_diff_id, relative_order)
ORDER BY (traversal_path, merge_request_diff_id, relative_order);

-- Siphon source tables for milestones
CREATE TABLE IF NOT EXISTS siphon_milestones
(
    `id` Int64,
    `title` String,
    `project_id` Nullable(Int64),
    `description` String,
    `due_date` Nullable(Date32),
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `state` LowCardinality(String),
    `iid` Int64,
    `start_date` Nullable(Date32),
    `group_id` Nullable(Int64),
    `lock_version` Int64 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

-- Siphon source tables for labels
CREATE TABLE IF NOT EXISTS siphon_labels
(
    `id` Int64,
    `title` String,
    `color` String,
    `project_id` Nullable(Int64),
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `template` Nullable(Bool) DEFAULT false,
    `description` String,
    `type` LowCardinality(String),
    `group_id` Nullable(Int64),
    `lock_on_merge` Bool DEFAULT false,
    `archived` Bool DEFAULT false,
    `organization_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);


-- Work items table
CREATE TABLE work_items
(
    `id` Int64 CODEC(Delta(8), ZSTD(1)),
    `title` String CODEC(ZSTD(3)),
    `author_id` Nullable(Int64),
    `project_id` Nullable(Int64),
    `created_at` DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `updated_at` DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `description` String CODEC(ZSTD(3)),
    `milestone_id` Nullable(Int64),
    `iid` Int64,
    `updated_by_id` Nullable(Int64),
    `weight` Nullable(Int64),
    `confidential` Bool DEFAULT false CODEC(ZSTD(1)),
    `due_date` Nullable(Date32),
    `moved_to_id` Nullable(Int64),
    `time_estimate` Nullable(Int64) DEFAULT 0,
    `relative_position` Nullable(Int64),
    `service_desk_reply_to` Nullable(String),
    `cached_markdown_version` Nullable(Int64),
    `last_edited_at` Nullable(DateTime64(6, 'UTC')),
    `last_edited_by_id` Nullable(Int64),
    `discussion_locked` Nullable(Bool) CODEC(ZSTD(1)),
    `closed_at` Nullable(DateTime64(6, 'UTC')),
    `closed_by_id` Nullable(Int64),
    `state_id` Int16 DEFAULT 1,
    `duplicated_to_id` Nullable(Int64),
    `promoted_to_epic_id` Nullable(Int64),
    `health_status` Nullable(Int16),
    `sprint_id` Nullable(Int64),
    `blocking_issues_count` Int64 DEFAULT 0,
    `upvotes_count` Int64 DEFAULT 0,
    `work_item_type_id` Int64,
    `namespace_id` Int64,
    `start_date` Nullable(Date32),
    `imported_from` Int16 DEFAULT 0,
    `namespace_traversal_ids` Array(Int64) DEFAULT [],
    `traversal_path` String DEFAULT '0/' CODEC(ZSTD(3)),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(ZSTD(1)),
    `_siphon_deleted` Bool DEFAULT false CODEC(ZSTD(1)),
    `metric_first_mentioned_in_commit_at` Nullable(DateTime64(6, 'UTC')),
    `metric_first_associated_with_milestone_at` Nullable(DateTime64(6, 'UTC')),
    `metric_first_added_to_board_at` Nullable(DateTime64(6, 'UTC')),
    `assignees` Array(UInt64),
    `label_ids` Array(Tuple(
        label_id UInt64,
        created_at DateTime64(6, 'UTC'))),
    `award_emojis` Array(Tuple(
        name String,
        user_id UInt64,
        created_at DateTime64(6, 'UTC'))),
    `system_defined_status_id` Nullable(Int64),
    `custom_status_id` Nullable(Int64),
    PROJECTION pg_pkey_ordered
    (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS index_granularity = 2048, deduplicate_merge_projection_mode = 'rebuild';

-- Knowledge graph enabled namespaces.
-- `traversal_path` mirrors the production column added in
-- gitlab-org/gitlab!232941. In production it has a dictionary-backed
-- default. Tests use a literal '0/' default and rely on seeders to
-- write the real value.
CREATE TABLE IF NOT EXISTS test.siphon_knowledge_graph_enabled_namespaces
(
    `id` Int64,
    `root_namespace_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (root_namespace_id, id)
ORDER BY (root_namespace_id, id)
SETTINGS index_granularity = 8192;

-- Siphon source tables for security vulnerabilities
CREATE TABLE IF NOT EXISTS siphon_vulnerabilities
(
    `id` Int64,
    `project_id` Int64,
    `author_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `title` String,
    `description` Nullable(String),
    `state` Int16 DEFAULT 1,
    `severity` Int16,
    `severity_overridden` Nullable(Bool) DEFAULT false,
    `resolved_by_id` Nullable(Int64),
    `resolved_at` Nullable(DateTime64(6, 'UTC')),
    `report_type` Int16,
    `confirmed_by_id` Nullable(Int64),
    `confirmed_at` Nullable(DateTime64(6, 'UTC')),
    `dismissed_at` Nullable(DateTime64(6, 'UTC')),
    `dismissed_by_id` Nullable(Int64),
    `resolved_on_default_branch` Bool DEFAULT false,
    `present_on_default_branch` Bool DEFAULT true,
    `detected_at` Nullable(DateTime64(6, 'UTC')) DEFAULT now(),
    `finding_id` Int64,
    `cvss` Nullable(String) DEFAULT '[]',
    `auto_resolved` Bool DEFAULT false,
    `uuid` Nullable(UUID),
    `solution` Nullable(String),
    `partition_id` Nullable(Int64) DEFAULT 1,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

-- Siphon source tables for vulnerability scanners
CREATE TABLE IF NOT EXISTS siphon_vulnerability_scanners
(
    `id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `project_id` Int64,
    `external_id` String,
    `name` LowCardinality(String),
    `vendor` LowCardinality(String) DEFAULT 'GitLab',
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

-- Siphon source tables for vulnerability identifiers
CREATE TABLE IF NOT EXISTS siphon_vulnerability_identifiers
(
    `id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `project_id` Int64,
    `fingerprint` String,
    `external_type` LowCardinality(String),
    `external_id` String,
    `name` String,
    `url` Nullable(String),
    `partition_id` Nullable(Int64) DEFAULT 1,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

-- Siphon source tables for security findings
CREATE TABLE IF NOT EXISTS siphon_security_findings
(
    `id` Int64,
    `scan_id` Int64,
    `scanner_id` Int64,
    `severity` Int16,
    `deduplicated` Bool DEFAULT false,
    `uuid` UUID,
    `overridden_uuid` Nullable(UUID),
    `partition_number` Int64 DEFAULT 1,
    `finding_data` String DEFAULT '{}',
    `project_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id, partition_number)
ORDER BY (traversal_path, id, partition_number);

-- Siphon source tables for security scans
CREATE TABLE IF NOT EXISTS siphon_security_scans
(
    `id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `build_id` Int64,
    `scan_type` Int16,
    `info` String DEFAULT '{}',
    `project_id` Int64,
    `pipeline_id` Nullable(Int64),
    `latest` Bool DEFAULT true,
    `status` Int16 DEFAULT 0,
    `findings_partition_number` Int64 DEFAULT 1,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

CREATE TABLE IF NOT EXISTS siphon_vulnerability_occurrences
(
    `id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `severity` Int16,
    `report_type` Int16,
    `project_id` Int64,
    `scanner_id` Int64,
    `primary_identifier_id` Int64,
    `location_fingerprint` String,
    `name` String,
    `metadata_version` String,
    `raw_metadata` Nullable(String),
    `vulnerability_id` Nullable(Int64),
    `details` String DEFAULT '{}',
    `description` String DEFAULT '',
    `solution` String DEFAULT '',
    `cve` Nullable(String),
    `location` Nullable(String),
    `detection_method` Int16 DEFAULT 0,
    `uuid` UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    `initial_pipeline_id` Nullable(Int64),
    `latest_pipeline_id` Nullable(Int64),
    `security_project_tracked_context_id` Nullable(Int64),
    `detected_at` DateTime64(6, 'UTC') DEFAULT now(),
    `new_uuid` Nullable(UUID),
    `partition_id` Nullable(Int64) DEFAULT 1,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

-- Siphon source tables for CI pipelines
CREATE TABLE IF NOT EXISTS siphon_p_ci_pipelines
(
    `ref` Nullable(String),
    `sha` Nullable(String),
    `before_sha` Nullable(String),
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `tag` Nullable(Bool) DEFAULT false,
    `yaml_errors` Nullable(String),
    `committed_at` Nullable(DateTime64(6, 'UTC')),
    `project_id` Int64,
    `status` LowCardinality(String) DEFAULT '',
    `started_at` Nullable(DateTime64(6, 'UTC')),
    `finished_at` Nullable(DateTime64(6, 'UTC')),
    `duration` Nullable(Int64),
    `user_id` Nullable(Int64),
    `lock_version` Int64 DEFAULT 0,
    `pipeline_schedule_id` Nullable(Int64),
    `source` Nullable(Int64),
    `config_source` Nullable(Int64),
    `protected` Nullable(Bool),
    `failure_reason` Nullable(Int64),
    `iid` Nullable(Int64),
    `merge_request_id` Nullable(Int64),
    `source_sha` Nullable(String),
    `target_sha` Nullable(String),
    `external_pull_request_id` Nullable(Int64),
    `ci_ref_id` Nullable(Int64),
    `locked` Int16 DEFAULT 1,
    `partition_id` Int64,
    `id` Int64,
    `auto_canceled_by_id` Nullable(Int64),
    `auto_canceled_by_partition_id` Nullable(Int64),
    `trigger_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id, partition_id)
ORDER BY (traversal_path, id, partition_id);

-- Siphon source tables for CI stages
CREATE TABLE IF NOT EXISTS siphon_p_ci_stages
(
    `project_id` Int64,
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `name` Nullable(String),
    `status` Nullable(Int64),
    `lock_version` Int64 DEFAULT 0,
    `position` Nullable(Int64),
    `id` Int64,
    `partition_id` Int64,
    `pipeline_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id, partition_id)
ORDER BY (traversal_path, id, partition_id);

-- Siphon source tables for CI builds (jobs)
CREATE TABLE IF NOT EXISTS siphon_p_ci_builds
(
    `status` LowCardinality(String) DEFAULT '',
    `finished_at` Nullable(DateTime64(6, 'UTC')),
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `started_at` Nullable(DateTime64(6, 'UTC')),
    `coverage` Nullable(Float64),
    `name` Nullable(String),
    `options` Nullable(String),
    `allow_failure` Bool DEFAULT false,
    `stage_idx` Nullable(Int64),
    `tag` Nullable(Bool),
    `ref` Nullable(String),
    `type` LowCardinality(String) DEFAULT '',
    `target_url` Nullable(String),
    `description` Nullable(String),
    `erased_at` Nullable(DateTime64(6, 'UTC')),
    `artifacts_expire_at` Nullable(DateTime64(6, 'UTC')),
    `environment` LowCardinality(String) DEFAULT '',
    `when` LowCardinality(String) DEFAULT '',
    `yaml_variables` Nullable(String),
    `queued_at` Nullable(DateTime64(6, 'UTC')),
    `lock_version` Int64 DEFAULT 0,
    `coverage_regex` Nullable(String),
    `retried` Nullable(Bool),
    `protected` Nullable(Bool),
    `failure_reason` Nullable(Int64),
    `scheduled_at` Nullable(DateTime64(6, 'UTC')),
    `token_encrypted` Nullable(String),
    `resource_group_id` Nullable(Int64),
    `waiting_for_resource_at` Nullable(DateTime64(6, 'UTC')),
    `processed` Nullable(Bool),
    `scheduling_type` Nullable(Int16),
    `id` Int64,
    `stage_id` Nullable(Int64),
    `partition_id` Int64,
    `auto_canceled_by_partition_id` Nullable(Int64),
    `auto_canceled_by_id` Nullable(Int64),
    `commit_id` Nullable(Int64),
    `erased_by_id` Nullable(Int64),
    `project_id` Int64,
    `runner_id` Nullable(Int64),
    `upstream_pipeline_id` Nullable(Int64),
    `user_id` Nullable(Int64),
    `execution_config_id` Nullable(Int64),
    `upstream_pipeline_partition_id` Nullable(Int64),
    `scoped_user_id` Nullable(Int64),
    `timeout` Nullable(Int64),
    `timeout_source` Nullable(Int16),
    `exit_code` Nullable(Int16),
    `debug_trace_enabled` Nullable(Bool),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id, partition_id)
ORDER BY (traversal_path, id, partition_id);

-- Siphon source tables for vulnerability merge request links (join table)
CREATE TABLE IF NOT EXISTS siphon_vulnerability_merge_request_links
(
    `id` Int64,
    `vulnerability_id` Int64,
    `merge_request_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `project_id` Int64,
    `vulnerability_occurrence_id` Nullable(Int64),
    `readiness_score` Nullable(Float64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for merge request reviewers (standalone edge)
CREATE TABLE IF NOT EXISTS siphon_merge_request_reviewers
(
    `id` Int64,
    `user_id` Int64,
    `merge_request_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `state` Int16 DEFAULT 0,
    `project_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, merge_request_id, id)
ORDER BY (traversal_path, merge_request_id, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for approvals (standalone edge)
CREATE TABLE IF NOT EXISTS siphon_approvals
(
    `id` Int64,
    `merge_request_id` Int64,
    `user_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `patch_id_sha` Nullable(String),
    `project_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, merge_request_id, id)
ORDER BY (traversal_path, merge_request_id, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for merge request assignees (standalone edge)
CREATE TABLE IF NOT EXISTS siphon_merge_request_assignees
(
    `id` Int64,
    `user_id` Int64,
    `merge_request_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `project_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, merge_request_id, id)
ORDER BY (traversal_path, merge_request_id, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for merge requests closing issues (join table)
CREATE TABLE IF NOT EXISTS siphon_merge_requests_closing_issues
(
    `id` Int64,
    `merge_request_id` Int64,
    `issue_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `from_mr_description` Bool DEFAULT true,
    `project_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, issue_id, id)
ORDER BY (traversal_path, issue_id, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for work item parent links (join table)
CREATE TABLE IF NOT EXISTS siphon_work_item_parent_links
(
    `id` Int64,
    `work_item_id` Int64,
    `work_item_parent_id` Int64,
    `relative_position` Nullable(Int64),
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `namespace_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, work_item_parent_id, id)
ORDER BY (traversal_path, work_item_parent_id, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for issue links (join table)
CREATE TABLE IF NOT EXISTS siphon_issue_links
(
    `id` Int64,
    `source_id` Int64,
    `target_id` Int64,
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `link_type` Int8 DEFAULT 0,
    `namespace_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, source_id, id)
ORDER BY (traversal_path, source_id, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for vulnerability occurrence identifiers (join table)
CREATE TABLE IF NOT EXISTS siphon_vulnerability_occurrence_identifiers
(
    `id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `occurrence_id` Int64,
    `identifier_id` Int64,
    `project_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source tables for deployments
CREATE TABLE IF NOT EXISTS siphon_deployments
(
    `id` Int64,
    `iid` Int64,
    `project_id` Int64,
    `environment_id` Int64,
    `ref` String,
    `tag` Bool DEFAULT false,
    `sha` String,
    `user_id` Nullable(Int64),
    `deployable_type` String DEFAULT '',
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `on_stop` Nullable(String),
    `status` Int8 DEFAULT 0,
    `finished_at` Nullable(DateTime64(6, 'UTC')),
    `deployable_id` Nullable(Int64),
    `archived` Bool DEFAULT false,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', index_granularity = 2048;

-- Siphon source tables for environments
CREATE TABLE IF NOT EXISTS siphon_environments
(
    `id` Int64,
    `project_id` Int64,
    `name` String,
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `external_url` Nullable(String),
    `environment_type` Nullable(String),
    `state` String DEFAULT 'available',
    `slug` String,
    `auto_stop_at` Nullable(DateTime64(6, 'UTC')),
    `auto_delete_at` Nullable(DateTime64(6, 'UTC')),
    `tier` Nullable(Int8),
    `merge_request_id` Nullable(Int64),
    `cluster_agent_id` Nullable(Int64),
    `kubernetes_namespace` Nullable(String),
    `flux_resource_path` Nullable(String),
    `description` Nullable(String),
    `auto_stop_setting` Int8 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', index_granularity = 2048;

-- Siphon source tables for deployment -> merge request links (join table)
CREATE TABLE IF NOT EXISTS siphon_deployment_merge_requests
(
    `deployment_id` Int64,
    `merge_request_id` Int64,
    `environment_id` Nullable(Int64),
    `project_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, deployment_id, merge_request_id)
ORDER BY (traversal_path, deployment_id, merge_request_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', index_granularity = 2048;

-- Siphon source tables for members (join table for user membership)
CREATE TABLE IF NOT EXISTS siphon_members
(
    `id` Int64,
    `access_level` Int64,
    `source_id` Int64,
    `source_type` String,
    `user_id` Nullable(Int64),
    `notification_level` Int64,
    `type` String,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `created_by_id` Nullable(Int64),
    `invite_email` Nullable(String),
    `invite_token` Nullable(String),
    `invite_accepted_at` Nullable(DateTime64(6, 'UTC')),
    `requested_at` Nullable(DateTime64(6, 'UTC')),
    `expires_at` Nullable(Date32),
    `ldap` Bool DEFAULT false,
    `override` Bool DEFAULT false,
    `state` Int8 DEFAULT 0,
    `invite_email_success` Bool DEFAULT true,
    `member_namespace_id` Nullable(Int64),
    `member_role_id` Nullable(Int64),
    `expiry_notified_at` Nullable(DateTime64(6, 'UTC')),
    `request_accepted_at` Nullable(DateTime64(6, 'UTC')),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild', index_granularity = 8192;

-- Siphon source table for CI runners (mirrors db/click_house/migrate/main/20260216174855_create_siphon_ci_runners.rb
-- plus 20260227143636_add_token_rotation_deadline_to_siphon_ci_runners.rb)
CREATE TABLE IF NOT EXISTS siphon_ci_runners
(
    `id` Int64,
    `creator_id` Nullable(Int64),
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `contacted_at` Nullable(DateTime64(6, 'UTC')),
    `token_expires_at` Nullable(DateTime64(6, 'UTC')),
    `public_projects_minutes_cost_factor` Float64 DEFAULT 1.0,
    `private_projects_minutes_cost_factor` Float64 DEFAULT 1.0,
    `access_level` Int64 DEFAULT 0,
    `maximum_timeout` Nullable(Int64),
    `runner_type` Int16,
    `registration_type` Int16 DEFAULT 0,
    `creation_state` Int16 DEFAULT 0,
    `active` Bool DEFAULT true,
    `run_untagged` Bool DEFAULT true,
    `locked` Bool DEFAULT false,
    `name` Nullable(String),
    `token_encrypted` String DEFAULT '',
    `description` String DEFAULT '',
    `maintainer_note` String DEFAULT '',
    `allowed_plans` Array(String) DEFAULT [],
    `allowed_plan_ids` Array(Int64) DEFAULT [],
    `organization_id` Nullable(Int64),
    `allowed_plan_name_uids` Array(Int16) DEFAULT [],
    `token_rotation_deadline` DateTime64(6, 'UTC') DEFAULT toDateTime64('9999-12-31 23:59:59.999999', 6, 'UTC'),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (id, runner_type)
SETTINGS index_granularity = 2048;

CREATE TABLE IF NOT EXISTS siphon_ci_runner_namespaces
(
    `id` Int64,
    `runner_id` Int64,
    `namespace_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
SETTINGS index_granularity = 2048, deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE IF NOT EXISTS siphon_ci_runner_projects
(
    `id` Int64,
    `runner_id` Int64,
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `project_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
SETTINGS index_granularity = 2048, deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source table for per-build metadata (interruptible, timeout policy, exit_code)
-- mirrors db/click_house/migrate/main/<ts>_create_siphon_p_ci_builds_metadata.rb
CREATE TABLE IF NOT EXISTS siphon_p_ci_builds_metadata
(
    `id` Int64,
    `build_id` Int64,
    `project_id` Int64,
    `partition_id` Int64,
    `timeout` Nullable(Int64),
    `timeout_source` Int64 DEFAULT 1,
    `interruptible` Nullable(Bool),
    `has_exposed_artifacts` Nullable(Bool),
    `environment_auto_stop_in` Nullable(String),
    `expanded_environment_name` Nullable(String),
    `debug_trace_enabled` Bool DEFAULT false,
    `exit_code` Nullable(Int16),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, build_id, partition_id)
ORDER BY (traversal_path, build_id, partition_id)
SETTINGS index_granularity = 2048;

-- Siphon source table for CI pipeline parent/child + bridge linkage
-- (mirrors monolith migration db/click_house/migrate/main/<ts>_create_siphon_ci_sources_pipelines.rb)
CREATE TABLE IF NOT EXISTS siphon_ci_sources_pipelines
(
    `id` Int64,
    `project_id` Nullable(Int64),
    `source_project_id` Nullable(Int64),
    `source_job_id` Nullable(Int64),
    `partition_id` Int64,
    `source_partition_id` Int64,
    `pipeline_id` Nullable(Int64),
    `source_pipeline_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id, partition_id)
ORDER BY (traversal_path, id, partition_id)
SETTINGS index_granularity = 2048;

-- Siphon source table for issue assignees (join table)
CREATE TABLE IF NOT EXISTS siphon_issue_assignees
(
    `user_id` Int64,
    `issue_id` Int64,
    `namespace_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, issue_id, user_id)
ORDER BY (traversal_path, issue_id, user_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Siphon source table for system note metadata (action discriminator for system notes)
-- Mirrors monolith: db/structure.sql `system_note_metadata` table.
-- Not yet replicated in production Siphon config — requires Analytics team coordination
-- (see https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499).
-- Added here so GKG can reference it once replication is enabled.
CREATE TABLE IF NOT EXISTS siphon_system_note_metadata
(
    `id` Int64,
    `note_id` Int64,
    `action` LowCardinality(String),
    `commit_count` Nullable(Int32),
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `namespace_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false,
    INDEX idx_action (action) TYPE set(16) GRANULARITY 2
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, note_id)
ORDER BY (traversal_path, note_id, id)
SETTINGS index_granularity = 2048;

-- Denormalized views joining system note metadata with its parent note,
-- one per lifecycle action.  Each view exposes:
-- who (author_id) performed the action on which entity
-- (noteable_id / noteable_type) within a namespace (traversal_path).
-- These are the source tables for the MERGED, CLOSED, and REOPENED lifecycle
-- edge ETL plans.  The per-action split is necessary because the standalone
-- edge ETL does not support WHERE-clause filtering, so each view pre-filters
-- to a single action value and the edge plan can reference it directly.
CREATE VIEW IF NOT EXISTS siphon_system_note_merged AS
SELECT
    snm.id                        AS id,
    snm.note_id                   AS note_id,
    sn.author_id                  AS author_id,
    sn.noteable_id                AS noteable_id,
    sn.noteable_type              AS noteable_type,
    snm.traversal_path            AS traversal_path,
    snm._siphon_replicated_at     AS _siphon_replicated_at,
    snm._siphon_deleted           AS _siphon_deleted
FROM siphon_system_note_metadata snm
INNER JOIN siphon_notes sn USING (note_id)
WHERE snm.action = 'merged';

CREATE VIEW IF NOT EXISTS siphon_system_note_closed AS
SELECT
    snm.id                        AS id,
    snm.note_id                   AS note_id,
    sn.author_id                  AS author_id,
    sn.noteable_id                AS noteable_id,
    sn.noteable_type              AS noteable_type,
    snm.traversal_path            AS traversal_path,
    snm._siphon_replicated_at     AS _siphon_replicated_at,
    snm._siphon_deleted           AS _siphon_deleted
FROM siphon_system_note_metadata snm
INNER JOIN siphon_notes sn USING (note_id)
WHERE snm.action = 'closed';

CREATE VIEW IF NOT EXISTS siphon_system_note_reopened AS
SELECT
    snm.id                        AS id,
    snm.note_id                   AS note_id,
    sn.author_id                  AS author_id,
    sn.noteable_id                AS noteable_id,
    sn.noteable_type              AS noteable_type,
    snm.traversal_path            AS traversal_path,
    snm._siphon_replicated_at     AS _siphon_replicated_at,
    snm._siphon_deleted           AS _siphon_deleted
FROM siphon_system_note_metadata snm
INNER JOIN siphon_notes sn USING (note_id)
WHERE snm.action = 'reopened';

-- Siphon source table for label links (polymorphic join table)
CREATE TABLE IF NOT EXISTS siphon_label_links
(
    `id` Int64,
    `label_id` Int64,
    `target_id` Int64,
    `target_type` LowCardinality(String),
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `namespace_id` Int64,
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT FALSE,
    PROJECTION pg_pkey_ordered (
        SELECT *
        ORDER BY id
    )
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, target_id, id)
ORDER BY (traversal_path, target_id, id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';
