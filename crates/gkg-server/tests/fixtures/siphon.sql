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
    `state` Int8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY id
ORDER BY id;

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
    `deleted` Bool DEFAULT false
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512;

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
    `deleted` Bool DEFAULT false
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512;

-- Siphon source tables for notes
CREATE TABLE IF NOT EXISTS siphon_notes
(
    `id` Int64,
    `note` Nullable(String),
    `noteable_type` Nullable(String),
    `noteable_id` Nullable(Int64),
    `author_id` Nullable(Int64),
    `system` Bool DEFAULT false,
    `line_code` Nullable(String),
    `commit_id` Nullable(String),
    `discussion_id` Nullable(String),
    `resolved_at` Nullable(DateTime64(6, 'UTC')),
    `resolved_by_id` Nullable(Int64),
    `internal` Bool DEFAULT false,
    `confidential` Nullable(Bool),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

-- Siphon source tables for merge requests
CREATE TABLE IF NOT EXISTS hierarchy_merge_requests
(
    `traversal_path` String,
    `id` Int64,
    `target_branch` String,
    `source_branch` String,
    `source_project_id` Nullable(Int64),
    `author_id` Nullable(Int64),
    `assignee_id` Nullable(Int64),
    `title` String DEFAULT '',
    `created_at` DateTime64(6, 'UTC') DEFAULT now(),
    `updated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `milestone_id` Nullable(Int64),
    `merge_status` LowCardinality(String) DEFAULT 'unchecked',
    `target_project_id` Int64,
    `iid` Nullable(Int64),
    `description` String DEFAULT '',
    `updated_by_id` Nullable(Int64),
    `merge_error` Nullable(String),
    `merge_params` Nullable(String),
    `merge_when_pipeline_succeeds` Bool DEFAULT false,
    `merge_user_id` Nullable(Int64),
    `merge_commit_sha` Nullable(String),
    `approvals_before_merge` Nullable(Int64),
    `rebase_commit_sha` Nullable(String),
    `in_progress_merge_commit_sha` Nullable(String),
    `lock_version` Int64 DEFAULT 0,
    `time_estimate` Nullable(Int64) DEFAULT 0,
    `squash` Bool DEFAULT false,
    `cached_markdown_version` Nullable(Int64),
    `last_edited_at` Nullable(DateTime64(6, 'UTC')),
    `last_edited_by_id` Nullable(Int64),
    `merge_jid` Nullable(String),
    `discussion_locked` Nullable(Bool),
    `latest_merge_request_diff_id` Nullable(Int64),
    `allow_maintainer_to_push` Nullable(Bool) DEFAULT true,
    `state_id` Int8 DEFAULT 1,
    `rebase_jid` Nullable(String),
    `squash_commit_sha` Nullable(String),
    `sprint_id` Nullable(Int64),
    `merge_ref_sha` Nullable(String),
    `draft` Bool DEFAULT false,
    `prepared_at` Nullable(DateTime64(6, 'UTC')),
    `merged_commit_sha` Nullable(String),
    `override_requested_changes` Bool DEFAULT false,
    `head_pipeline_id` Nullable(Int64),
    `imported_from` Int8 DEFAULT 0,
    `retargeted` Bool DEFAULT false,
    `label_ids` String DEFAULT '',
    `assignee_ids` String DEFAULT '',
    `approver_ids` String DEFAULT '',
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
    `version` DateTime64(6, 'UTC') DEFAULT now(),
    `deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id)
SETTINGS index_granularity = 8192;

-- Siphon source tables for merge request diffs
CREATE TABLE IF NOT EXISTS siphon_merge_request_diffs
(
    `id` Int64,
    `state` Nullable(String),
    `merge_request_id` Int64,
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `base_commit_sha` Nullable(String),
    `real_size` Nullable(String),
    `head_commit_sha` Nullable(String),
    `start_commit_sha` Nullable(String),
    `commits_count` Nullable(Int64),
    `external_diff` Nullable(String),
    `external_diff_store` Nullable(Int64) DEFAULT 1,
    `stored_externally` Nullable(Bool),
    `files_count` Nullable(Int8),
    `sorted` Bool DEFAULT false,
    `diff_type` Int8 DEFAULT 1,
    `patch_id_sha` Nullable(String),
    `project_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

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
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (merge_request_diff_id, relative_order)
ORDER BY (merge_request_diff_id, relative_order);

-- Siphon source tables for milestones
CREATE TABLE IF NOT EXISTS siphon_milestones
(
    `id` Int64,
    `title` String,
    `project_id` Nullable(Int64),
    `description` Nullable(String),
    `due_date` Nullable(Date32),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `state` Nullable(String),
    `iid` Nullable(Int64),
    `title_html` Nullable(String),
    `description_html` Nullable(String),
    `start_date` Nullable(Date32),
    `cached_markdown_version` Nullable(Int64),
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
    `title` Nullable(String),
    `color` Nullable(String),
    `project_id` Nullable(Int64),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `template` Nullable(Bool) DEFAULT false,
    `description` Nullable(String),
    `description_html` Nullable(String),
    `type` Nullable(String),
    `group_id` Nullable(Int64),
    `cached_markdown_version` Nullable(Int64),
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

-- Siphon source tables for label links (join table)
CREATE TABLE IF NOT EXISTS siphon_label_links
(
    `id` Int64,
    `label_id` Nullable(Int64),
    `target_id` Nullable(Int64),
    `target_type` Nullable(String),
    `created_at` Nullable(DateTime64(6, 'UTC')),
    `updated_at` Nullable(DateTime64(6, 'UTC')),
    `namespace_id` Nullable(Int64),
    `traversal_path` String DEFAULT '0/',
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, id)
ORDER BY (traversal_path, id);

-- Knowledge graph enabled namespaces
CREATE TABLE IF NOT EXISTS test.siphon_knowledge_graph_enabled_namespaces
(
    `id` Int64,
    `root_namespace_id` Int64,
    `created_at` DateTime64(6, 'UTC'),
    `updated_at` DateTime64(6, 'UTC'),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now(),
    `_siphon_deleted` Bool DEFAULT false
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (root_namespace_id, id)
ORDER BY (root_namespace_id, id)
SETTINGS index_granularity = 8192;
