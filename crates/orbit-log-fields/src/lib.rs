//! Log and span field names for Orbit.
//!
//! [`safe`] fields are written on every log line; [`safe::ALL`] is the allowlist for labkit's
//! `ExpandedLogsPolicy`. [`expanded`] fields are written only on expanded log lines.
//!
//! A safe value can never hold customer data. The allowlist also passes dependency log
//! events, so a safe name must not be a generic name that a dependency uses for free text.
//!
//! The macros take the `tracing` arguments, except `parent:` and `name:` on events:
//!
//! ```
//! use orbit_log_fields::info;
//!
//! let project_id: u64 = 42;
//! let path = "app/models/user.rb";
//! info!(safe(GL_PROJECT_ID) = project_id, expanded(FILE_PATH) = path, "parsed file");
//! ```
//!
//! `#[instrument]` and `Span::record` use the constants: `fields({ safe::GL_PROJECT_ID } = id)`.
//!
//! Record safe fields as plain values, never with `%` or `?`: the allowlist checks only the name.

mod macros;

#[doc(hidden)]
// ast-grep-ignore: no-raw-tracing-macro -- the macros of this crate call tracing through this path
pub use tracing as __tracing;

macro_rules! fields {
    ($($(#[$doc:meta])* $name:ident = $value:expr;)*) => {
        $($(#[$doc])* pub const $name: &str = $value;)*

        /// Every field name declared in this module.
        pub const ALL: &[&str] = &[$($name),*];
    };
}

/// Fields that are written on every log line and kept in exported traces.
pub mod safe {
    use labkit_fields as labkit;

    fields! {
        CORRELATION_ID = labkit::CORRELATION_ID;
        GL_ORGANIZATION_ID = labkit::GL_ORGANIZATION_ID;
        /// The expanded logs predicate reads it.
        GL_ROOT_NAMESPACE_ID = labkit::GL_ROOT_NAMESPACE_ID;
        GL_NAMESPACE_ID = labkit::GL_NAMESPACE_ID;
        GL_PROJECT_ID = labkit::GL_PROJECT_ID;
        GL_USER_ID = labkit::GL_USER_ID;
        /// Billing namespace from the JWT `root_namespace_id` claim, not the namespace that a
        /// request reads. The expanded logs predicate does not read it.
        GL_GOVERNING_NAMESPACE_ID = "gl_governing_namespace_id";
        /// Numeric IDs, `<org_id>/<root_namespace_id>/.../`. The expanded logs predicate reads it.
        TRAVERSAL_PATH = "traversal_path";
        /// Comma separated; the expanded logs predicate does not read it.
        TRAVERSAL_PATHS = "traversal_paths";
        INSTANCE_ID = "instance_id";
        UNIQUE_INSTANCE_ID = "unique_instance_id";
        EVENT_ID = "event_id";
        /// Comma separated. Orbit generates the IDs.
        EVENT_IDS = "event_ids";
        IID = "iid";
        NOTEABLE_ID = "noteable_id";
        DIFF_ID = "diff_id";
        TASK_ID = "task_id";
        DISPATCH_ID = "dispatch_id";
        /// Orbit generates it (`migration-v<N>`).
        CAMPAIGN_ID = "campaign_id";
        /// Orbit builds it as `<stream>.<stream_seq>.<consumer_seq>`.
        MESSAGE_ID = "message_id";
        /// Orbit generates it for one redaction exchange.
        RESULT_ID = "result_id";
        COMMIT_SHA = "commit_sha";
        FROM_COMMIT_SHA = "from_commit_sha";
        TO_COMMIT_SHA = "to_commit_sha";

        /// Whether a trusted caller asked for expanded logs. The expanded logs predicate reads it.
        EXPANDED_LOGS_REQUESTED = "expanded_logs_requested";
        /// Only the values Orbit normalizes to (`SaaS` or `SM`).
        REALM = "realm";
        DEPLOYMENT_TYPE = "deployment_type";
        SOURCE_TYPE = "source_type";
        /// Never the checksum.
        LICENSE_CHECKSUM_PRESENT = "license_checksum_present";
        FEATURE_QUALIFIED_NAME = "feature_qualified_name";
        QUERY_TYPE = "query_type";
        QUERY_FRONTEND = "query_frontend";
        RESPONSE_FORMAT = "response_format";
        METADATA_ONLY = "metadata_only";
        /// Only names that Orbit defines, such as scheduled or named queries.
        QUERY_NAME = "query_name";

        INSTANCE_VERSION = "instance_version";
        SCHEMA_VERSION = "schema_version";
        ACTIVE_SCHEMA_VERSION = "active_schema_version";
        MIGRATING_SCHEMA_VERSION = "migrating_schema_version";
        MIGRATION_VERSION = "migration_version";
        /// Comma separated.
        RETIRED_SCHEMA_VERSIONS = "retired_schema_versions";
        CLICKHOUSE_VERSION = "clickhouse_version";
        /// Sanitized Orbit build version of versioned NATS streams and subjects.
        RELEASE = "release";
        AWS_LC_VERSION = "aws_lc_version";
        SYSROOT_VERSION = "sysroot_version";

        /// From the infrastructure config.
        GITALY_STORAGE = "gitaly_storage";
        TABLE = "table";
        /// Comma separated.
        TABLES = "tables";
        /// Table a statement reads from, when `table` is the target.
        SOURCE_TABLE = "source_table";
        TABLE_PREFIX = "table_prefix";
        CLICKHOUSE_TYPE = "clickhouse_type";
        ENTITY = "entity";
        /// Comma separated.
        ENTITIES = "entities";
        /// Comma separated.
        REQUESTED_ENTITIES = "requested_entities";
        /// Entity or relationship kind that an SDLC indexing plan writes.
        PLAN_TARGET = "plan_target";
        /// Comma separated.
        RELATIONSHIP_KINDS = "relationship_kinds";

        NATS_STREAM = "nats_stream";
        NATS_CONSUMER = "nats_consumer";
        /// Subject pattern that Orbit defines.
        NATS_SUBSCRIPTION = "nats_subscription";
        /// Subject patterns that Orbit defines, comma separated.
        NATS_SUBSCRIPTIONS = "nats_subscriptions";
        NATS_STREAM_SEQUENCE = "nats_stream_sequence";
        NATS_DELIVERY_COUNT = "nats_delivery_count";
        /// Bucket name that Orbit defines.
        NATS_KV_BUCKET = "nats_kv_bucket";

        /// Starts at 1.
        ATTEMPT = "attempt";
        MAX_ATTEMPTS = "max_attempts";
        /// Starts at 1.
        ITERATION = "iteration";
        /// Starts at 1.
        PAGE_NUMBER = "page_number";
        /// Zero-based.
        BATCH_INDEX = "batch_index";
        BYTE_OFFSET = "byte_offset";

        /// Use it when the message names what is counted.
        COUNT = "count";
        ROW_COUNT = "row_count";
        /// Number of values in a saved pagination cursor.
        CURSOR_LENGTH = "cursor_length";
        /// Number of columns in the sort key of an indexing plan.
        SORT_KEY_LENGTH = "sort_key_length";
        /// Rows that a ClickHouse query read and returned.
        READ_ROWS = "read_rows";
        /// Rows that ClickHouse scanned to answer a query.
        SCANNED_ROWS = "scanned_rows";
        WRITTEN_ROWS = "written_rows";
        BATCH_COUNT = "batch_count";
        FILE_COUNT = "file_count";
        ERROR_COUNT = "error_count";
        SKIPPED_COUNT = "skipped_count";
        ENTITY_COUNT = "entity_count";
        PROJECT_COUNT = "project_count";
        INDEXED_PROJECT_COUNT = "indexed_project_count";
        PARTITION_COUNT = "partition_count";
        /// Number of cut points that a partition probe returned.
        CUT_COUNT = "cut_count";
        HANDLER_COUNT = "handler_count";
        GLOBAL_HANDLER_COUNT = "global_handler_count";
        NAMESPACED_HANDLER_COUNT = "namespaced_handler_count";
        PARTITIONED_ENTITY_COUNT = "partitioned_entity_count";
        SUBSCRIPTION_COUNT = "subscription_count";
        PENDING_TASK_COUNT = "pending_task_count";
        DISPATCHED_COUNT = "dispatched_count";
        NAMESPACE_COUNT = "namespace_count";
        ENABLED_NAMESPACE_COUNT = "enabled_namespace_count";
        SDLC_INDEXED_NAMESPACE_COUNT = "sdlc_indexed_namespace_count";
        CODE_ELIGIBLE_PROJECT_COUNT = "code_eligible_project_count";
        CODE_INDEXED_PROJECT_COUNT = "code_indexed_project_count";
        /// From 0 to 1.
        CODE_COVERAGE_RATIO = "code_coverage_ratio";
        ITERATION_COUNT = "iteration_count";
        THREAD_COUNT = "thread_count";
        CPU_COUNT = "cpu_count";
        DEFINITION_COUNT = "definition_count";
        IMPORT_COUNT = "import_count";
        EDGE_COUNT = "edge_count";
        NODE_COUNT = "node_count";
        PHI_COUNT = "phi_count";
        STRING_COUNT = "string_count";
        CLONED_TABLE_COUNT = "cloned_table_count";
        REBUILT_TABLE_COUNT = "rebuilt_table_count";
        SEEDED_TABLE_COUNT = "seeded_table_count";

        DURATION_S = labkit::DURATION_S;
        PARSE_DURATION_S = "parse_duration_s";
        RESOLVE_DURATION_S = "resolve_duration_s";
        EXTRACT_DURATION_S = "extract_duration_s";
        TRANSFORM_DURATION_S = "transform_duration_s";
        WRITE_DURATION_S = "write_duration_s";
        TIMEOUT_S = "timeout_s";
        RETRY_DELAY_S = "retry_delay_s";
        /// Unix time in seconds.
        EXPIRES_AT = "expires_at";
        /// Unix time in seconds.
        REFRESH_AT = "refresh_at";
        /// Timestamp that Orbit formats.
        WATERMARK = "watermark";

        SIZE_BYTES = "size_bytes";
        MEMORY_LIMIT_BYTES = "memory_limit_bytes";
        /// Bytes that a ClickHouse query read and returned.
        READ_BYTES = "read_bytes";
        /// Bytes that ClickHouse scanned to answer a query.
        SCANNED_BYTES = "scanned_bytes";
        WRITTEN_BYTES = labkit::WRITTEN_BYTES;
        LIMIT = "limit";
        LIMIT_BYTES = "limit_bytes";
        BATCH_SIZE = "batch_size";
        CONCURRENCY = "concurrency";

        /// Address that the server listens on, `host:port`.
        TCP_ADDRESS = labkit::TCP_ADDRESS;
        LISTENER_GROUP = "listener_group";
        SNOWPLOW_APP_ID = "snowplow_app_id";
        DEPLOYMENT_ENVIRONMENT = "deployment_environment";
        AUTH_MODE = "auth_mode";
        SERVER_MODE = "server_mode";
        INDEXING_MODE = "indexing_mode";
        /// Comma separated.
        INDEXER_MODULES = "indexer_modules";
        HANDLER = "handler";
        TASK = "task";
        TRIGGER = "trigger";
        WORKER_GROUP = "worker_group";
        /// Comma separated `group=limit` pairs from the config.
        WORKER_GROUPS = "worker_groups";
        COMPONENT = "component";
        K8S_NAMESPACE = "k8s_namespace";
        K8S_KIND = "k8s_kind";
        /// Orbit detects it from the file.
        LANGUAGE = "language";
        LANGUAGE_FAMILY = "language_family";
        /// From a closed set that Orbit defines.
        REASON = "reason_code";
        OUTCOME = "outcome";
        PHASE = "phase";
        /// From a closed set that Orbit defines.
        KIND = "kind";
        FROM_STATE = "from_state";
        MIGRATION_SCOPE = "migration_scope";
        /// Scope before Orbit widens it.
        REQUESTED_MIGRATION_SCOPE = "requested_migration_scope";

        TLS_ENABLED = "tls_enabled";
        CACHE_HIT = "cache_hit";
        SERVING_CACHED_TOKEN = "serving_cached_token";
        CACHED_TOKEN_EXPIRED = "cached_token_expired";
        ENABLED = "enabled";
        FULL_SWEEP = "full_sweep";
        /// Whether an error message is safe to return to the client.
        CLIENT_SAFE = "client_safe";

        /// Fixed error kind. Never the error text.
        ERROR_TYPE = labkit::ERROR_TYPE;
        HTTP_STATUS_CODE = "http_status_code";
        /// `file:line:column` in Orbit or dependency code.
        PANIC_LOCATION = "panic_location";
    }
}

/// Fields that are written only on expanded log lines.
pub mod expanded {
    use labkit_fields as labkit;

    fields! {
        GLOBAL_USER_ID = "global_user_id";

        /// Realm from a token, before Orbit validates it.
        RAW_REALM = "raw_realm";
        /// Free text that CustomersDot returns.
        BLOCK_REASON = "block_reason";
        /// Name that a client sends for a skill, a command or a tool.
        REQUESTED_NAME = "requested_name";
        /// Rails copies it from a client header.
        CLIENT_SESSION_ID = "client_session_id";
        CLIENT_REQUEST_ID = "client_request_id";
        /// A client sends it back.
        RECEIVED_RESULT_ID = "received_result_id";
        CLIENT_ERROR_CODE = "client_error_code";
        /// From the client user agent.
        CODING_AGENT = "coding_agent";
        /// Without query string or fragment.
        URL = labkit::HTTP_URL;
        HOST = labkit::HTTP_HOST;
        /// Host name that Orbit resolves in place of [`HOST`].
        RESOLVE_HOST = "resolve_host";
        REMOTE_ADDR = labkit::REMOTE_ADDR;

        SQL = "sql";
        QUERY_PARAMETER = "query_parameter";

        /// Can hold an encoded branch name.
        NATS_SUBJECT = "nats_subject";
        KV_KEY = "kv_key";

        /// Path inside a repository.
        FILE_PATH = "file_path";
        /// Path on the local disk of the Orbit server.
        LOCAL_PATH = "local_path";
        MANIFEST_PATH = "manifest_path";
        SYMLINK_TARGET = "symlink_target";
        BRANCH = "branch";
        PACKAGE_NAME = "package_name";

        /// Log a fixed [`ERROR_TYPE`](super::safe::ERROR_TYPE) next to it.
        ERROR_MESSAGE = labkit::ERROR_MESSAGE;
        ADDITIONAL_DETAILS = labkit::ADDITIONAL_DETAILS;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{expanded, safe};

    const LABKIT_RESERVED: &[&str] = &[
        "timestamp",
        "level",
        "target",
        "span",
        "spans",
        "log_expanded",
        "version",
        "commit",
        "build-date",
        "message",
    ];

    fn all_fields() -> Vec<&'static str> {
        safe::ALL.iter().chain(expanded::ALL).copied().collect()
    }

    #[test]
    fn names_are_unique_across_modules() {
        let fields = all_fields();
        let unique: HashSet<_> = fields.iter().collect();
        assert_eq!(unique.len(), fields.len());
    }

    #[test]
    fn names_are_snake_case() {
        for field in all_fields() {
            let is_snake_case = field
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            assert!(is_snake_case, "{field} must be lowercase snake_case");
        }
    }

    #[test]
    fn names_do_not_shadow_labkit_keys() {
        for field in all_fields() {
            assert!(
                !LABKIT_RESERVED.contains(&field),
                "labkit drops event fields named {field}"
            );
        }
    }
}
