//! Log and span field names for Orbit.
//!
//! Every structured field that Orbit code writes to a log line or a span is
//! named by a constant from this crate. The constants are split in two
//! modules:
//!
//! - [`safe`] fields are written on every log line. [`safe::ALL`] is the
//!   allowlist that the server gives to labkit's `ExpandedLogsPolicy`.
//! - [`expanded`] fields are written only on expanded log lines: lines inside
//!   the span of an internal tenant, or every line when the global override is
//!   on.
//!
//! A field is safe only when its value can never hold customer data. Safe
//! values are numeric IDs, counts, sizes, durations, flags, and names that
//! Orbit itself defines (ontology entities, tables, NATS streams, metric-style
//! labels). Anything a customer or a client can choose is expanded: file paths,
//! branch names, query text, error messages, URLs, usernames and free text.
//! The allowlist also passes the fields of dependency log events, so a safe name
//! must not be a generic name that a dependency uses for free text.
//! Before you add a generic safe name, check that no dependency logs free text under it.
//!
//! Log with the macros of this crate. They take the `tracing` arguments except
//! `parent:` and `name:` on events, and name each field with `safe(NAME)` or `expanded(NAME)`:
//!
//! ```
//! use orbit_log_fields::info;
//!
//! let project_id: u64 = 42;
//! let path = "app/models/user.rb";
//! info!(safe(GL_PROJECT_ID) = project_id, expanded(FILE_PATH) = path, "parsed file");
//! ```
//!
//! `#[instrument(fields(...))]` and `Span::record` cannot use these macros, so
//! they name fields with the constants: `fields({ safe::GL_PROJECT_ID } = id)`
//! and `span.record(safe::GL_PROJECT_ID, id)`.
//!
//! Record a safe field as a plain value (integer, float, bool, `&str`), never
//! with `%` or `?`. The allowlist checks the name, not the value, so a `Debug`
//! dump of a struct under a safe name would leak every field of the struct.

mod macros;

#[doc(hidden)]
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
        /// Correlation ID of the request or the unit of work.
        CORRELATION_ID = labkit::CORRELATION_ID;
        /// GitLab organization ID.
        GL_ORGANIZATION_ID = labkit::GL_ORGANIZATION_ID;
        /// GitLab top-level namespace ID. The expanded logs predicate reads it.
        GL_ROOT_NAMESPACE_ID = labkit::GL_ROOT_NAMESPACE_ID;
        /// GitLab namespace (group) ID.
        GL_NAMESPACE_ID = labkit::GL_NAMESPACE_ID;
        /// GitLab project ID.
        GL_PROJECT_ID = labkit::GL_PROJECT_ID;
        /// GitLab user ID.
        GL_USER_ID = labkit::GL_USER_ID;
        /// Namespace that a user picks for billing (the JWT `root_namespace_id` claim).
        /// It does not say which namespace a request reads, so the expanded logs
        /// predicate does not read it.
        GL_GOVERNING_NAMESPACE_ID = "gl_governing_namespace_id";
        /// Namespace traversal path of numeric IDs, `<org_id>/<root_namespace_id>/.../`.
        /// The expanded logs predicate reads the root namespace ID from it.
        TRAVERSAL_PATH = "traversal_path";
        /// GitLab instance ID from the Cloud Connector claims.
        INSTANCE_ID = "instance_id";
        /// Unique GitLab instance ID from the Cloud Connector claims.
        UNIQUE_INSTANCE_ID = "unique_instance_id";
        /// ID of a billing or analytics event.
        EVENT_ID = "event_id";
        /// Comma-separated IDs of billing or analytics events that Orbit generates.
        EVENT_IDS = "event_ids";
        /// Internal ID of an issue or merge request inside its project.
        IID = "iid";
        /// ID of the issue or merge request that owns a note.
        NOTEABLE_ID = "noteable_id";
        /// Merge request diff ID.
        DIFF_ID = "diff_id";
        /// Code indexing task ID.
        TASK_ID = "task_id";
        /// ID of one dispatcher run.
        DISPATCH_ID = "dispatch_id";
        /// Indexing campaign ID that Orbit generates (`migration-v<N>`).
        CAMPAIGN_ID = "campaign_id";
        /// NATS message ID that Orbit builds as `<stream>.<stream_seq>.<consumer_seq>`.
        MESSAGE_ID = "message_id";
        /// ID that Orbit generates for one redaction exchange.
        RESULT_ID = "result_id";
        /// Git commit SHA.
        COMMIT_SHA = "commit_sha";
        /// Git commit SHA at the start of a diff.
        FROM_COMMIT_SHA = "from_commit_sha";
        /// Git commit SHA at the end of a diff.
        TO_COMMIT_SHA = "to_commit_sha";

        /// Whether a trusted caller asked for expanded logs on this request.
        /// The expanded logs predicate reads it.
        EXPANDED_LOGS_REQUESTED = "expanded_logs_requested";
        /// Cloud Connector realm after Orbit normalizes it (`SaaS` or `SM`).
        REALM = "realm";
        /// Deployment type of the calling instance.
        DEPLOYMENT_TYPE = "deployment_type";
        /// Source of the request (`frontend`, `dws`, `mcp`, `core`, `rest`, `code_intelligence`).
        SOURCE_TYPE = "source_type";
        /// Whether the JWT carries a license checksum. Never the checksum.
        LICENSE_CHECKSUM_PRESENT = "license_checksum_present";
        /// Billing feature qualified name.
        FEATURE_QUALIFIED_NAME = "feature_qualified_name";
        /// Query type (`traversal`, `aggregation`, `path_finding`, `neighbors`, ...).
        QUERY_TYPE = "query_type";
        /// Query frontend (`json`, `gql`).
        QUERY_FRONTEND = "query_frontend";
        /// Proto enum value of the response format that a client asks for.
        RESPONSE_FORMAT = "response_format";
        /// Whether a client asks for metadata only.
        METADATA_ONLY = "metadata_only";
        /// Name of a query that Orbit defines, such as a scheduled or named query.
        QUERY_NAME = "query_name";

        /// GitLab version of the calling instance.
        INSTANCE_VERSION = "instance_version";
        /// Graph schema version.
        SCHEMA_VERSION = "schema_version";
        /// Graph schema version that serves queries.
        ACTIVE_SCHEMA_VERSION = "active_schema_version";
        /// Graph schema version that a migration builds.
        MIGRATING_SCHEMA_VERSION = "migrating_schema_version";
        /// Schema migration ledger version.
        MIGRATION_VERSION = "migration_version";
        /// Graph schema versions that a promotion retires, comma separated.
        RETIRED_SCHEMA_VERSIONS = "retired_schema_versions";
        /// Version string that a ClickHouse server reports.
        CLICKHOUSE_VERSION = "clickhouse_version";
        /// Orbit release tag (sanitized build version) of versioned NATS streams and subjects.
        RELEASE = "release";
        /// Version of the AWS-LC FIPS module that the server links.
        AWS_LC_VERSION = "aws_lc_version";
        /// Version of the Rust sysroot that the server embeds.
        SYSROOT_VERSION = "sysroot_version";

        /// Gitaly storage name from the infrastructure config.
        GITALY_STORAGE = "gitaly_storage";
        /// ClickHouse table, view or dictionary name.
        TABLE = "table";
        /// ClickHouse table names, comma separated.
        TABLES = "tables";
        /// ClickHouse table that a statement reads from, when `table` is the target.
        SOURCE_TABLE = "source_table";
        /// ClickHouse table name prefix of a schema version.
        TABLE_PREFIX = "table_prefix";
        /// ClickHouse data type name (`Int64`, `String`, ...).
        CLICKHOUSE_TYPE = "clickhouse_type";
        /// Ontology entity (node type) name.
        ENTITY = "entity";
        /// Ontology entity names, comma separated.
        ENTITIES = "entities";
        /// Ontology entity or relationship kind that an SDLC indexing plan writes.
        PLAN_TARGET = "plan_target";
        /// Ontology relationship kinds, comma separated.
        RELATIONSHIP_KINDS = "relationship_kinds";

        /// NATS JetStream stream name.
        NATS_STREAM = "nats_stream";
        /// NATS JetStream consumer name.
        NATS_CONSUMER = "nats_consumer";
        /// NATS subscription subject pattern that Orbit defines.
        NATS_SUBSCRIPTION = "nats_subscription";
        /// NATS subject patterns that Orbit defines, comma separated.
        NATS_SUBSCRIPTIONS = "nats_subscriptions";
        /// NATS JetStream stream sequence number.
        NATS_STREAM_SEQUENCE = "nats_stream_sequence";
        /// Number of times NATS delivered a message.
        NATS_DELIVERY_COUNT = "nats_delivery_count";
        /// NATS KV bucket name that Orbit defines.
        NATS_KV_BUCKET = "nats_kv_bucket";

        /// Attempt number of a retried operation, starting at 1.
        ATTEMPT = "attempt";
        /// Maximum number of attempts of a retried operation.
        MAX_ATTEMPTS = "max_attempts";
        /// Iteration number of a repeated run, starting at 1.
        ITERATION = "iteration";
        /// Page number of a paged read, starting at 1.
        PAGE_NUMBER = "page_number";
        /// Zero-based index of a batch in a sequence of batches.
        BATCH_INDEX = "batch_index";
        /// Byte offset inside a file.
        BYTE_OFFSET = "byte_offset";

        /// Number of items. Use it when the message names what is counted.
        COUNT = "count";
        /// Number of rows.
        ROW_COUNT = "row_count";
        /// Number of values in a saved pagination cursor.
        CURSOR_LENGTH = "cursor_length";
        /// Number of columns in the sort key of an indexing plan.
        SORT_KEY_LENGTH = "sort_key_length";
        /// Rows that a ClickHouse query read and returned.
        READ_ROWS = "read_rows";
        /// Rows that ClickHouse scanned to answer a query.
        SCANNED_ROWS = "scanned_rows";
        /// Rows written.
        WRITTEN_ROWS = "written_rows";
        /// Number of batches.
        BATCH_COUNT = "batch_count";
        /// Number of files.
        FILE_COUNT = "file_count";
        /// Number of errors.
        ERROR_COUNT = "error_count";
        /// Number of items that were skipped.
        SKIPPED_COUNT = "skipped_count";
        /// Number of ontology entities.
        ENTITY_COUNT = "entity_count";
        /// Number of projects.
        PROJECT_COUNT = "project_count";
        /// Number of projects that are indexed.
        INDEXED_PROJECT_COUNT = "indexed_project_count";
        /// Number of partitions of a partitioned load.
        PARTITION_COUNT = "partition_count";
        /// Number of cut points that a partition probe returned.
        CUT_COUNT = "cut_count";
        /// Number of message handlers.
        HANDLER_COUNT = "handler_count";
        /// Number of registered handlers for global SDLC entities.
        GLOBAL_HANDLER_COUNT = "global_handler_count";
        /// Number of registered handlers for namespaced SDLC entities.
        NAMESPACED_HANDLER_COUNT = "namespaced_handler_count";
        /// Number of SDLC entities that have a partition strategy.
        PARTITIONED_ENTITY_COUNT = "partitioned_entity_count";
        /// Number of NATS subscriptions.
        SUBSCRIPTION_COUNT = "subscription_count";
        /// Number of tasks that are still running.
        PENDING_TASK_COUNT = "pending_task_count";
        /// Number of requests that a dispatcher published.
        DISPATCHED_COUNT = "dispatched_count";
        /// Number of namespaces.
        NAMESPACE_COUNT = "namespace_count";
        /// Number of namespaces that have the knowledge graph enabled.
        ENABLED_NAMESPACE_COUNT = "enabled_namespace_count";
        /// Number of enabled namespaces whose SDLC indexing is complete for a schema version.
        SDLC_INDEXED_NAMESPACE_COUNT = "sdlc_indexed_namespace_count";
        /// Number of projects that code indexing must index.
        CODE_ELIGIBLE_PROJECT_COUNT = "code_eligible_project_count";
        /// Number of eligible projects that code indexing indexed for a schema version.
        CODE_INDEXED_PROJECT_COUNT = "code_indexed_project_count";
        /// Ratio of indexed to eligible code projects, from 0 to 1.
        CODE_COVERAGE_RATIO = "code_coverage_ratio";
        /// Number of iterations of a repeated run.
        ITERATION_COUNT = "iteration_count";
        /// Number of threads in a worker pool.
        THREAD_COUNT = "thread_count";
        /// Number of CPUs that the process can use.
        CPU_COUNT = "cpu_count";
        /// Number of code definitions.
        DEFINITION_COUNT = "definition_count";
        /// Number of code imports.
        IMPORT_COUNT = "import_count";
        /// Number of graph edges.
        EDGE_COUNT = "edge_count";
        /// Number of graph nodes.
        NODE_COUNT = "node_count";
        /// Number of SSA phi nodes.
        PHI_COUNT = "phi_count";
        /// Number of interned strings.
        STRING_COUNT = "string_count";
        /// Number of tables that a schema migration cloned from the active version.
        CLONED_TABLE_COUNT = "cloned_table_count";
        /// Number of tables that a schema migration rebuilt empty.
        REBUILT_TABLE_COUNT = "rebuilt_table_count";
        /// Number of tables that a schema migration seeded from the active version.
        SEEDED_TABLE_COUNT = "seeded_table_count";

        /// Duration of an operation in seconds.
        DURATION_S = labkit::DURATION_S;
        /// Duration of the parse phase of a file, in seconds.
        PARSE_DURATION_S = "parse_duration_s";
        /// Duration of the resolve phase of a file, in seconds.
        RESOLVE_DURATION_S = "resolve_duration_s";
        /// Duration of the extract phase of an indexing run, in seconds.
        EXTRACT_DURATION_S = "extract_duration_s";
        /// Duration of the transform phase of an indexing run, in seconds.
        TRANSFORM_DURATION_S = "transform_duration_s";
        /// Duration of the write phase of an indexing run, in seconds.
        WRITE_DURATION_S = "write_duration_s";
        /// Timeout of an operation in seconds.
        TIMEOUT_S = "timeout_s";
        /// Delay before a retry or a redelivery, in seconds.
        RETRY_DELAY_S = "retry_delay_s";
        /// Unix time in seconds when a token or a lease expires.
        EXPIRES_AT = "expires_at";
        /// Unix time in seconds of the next refresh of a cached value.
        REFRESH_AT = "refresh_at";
        /// Lower time bound of an incremental scan, as a timestamp that Orbit formats.
        WATERMARK = "watermark";

        /// Size in bytes.
        SIZE_BYTES = "size_bytes";
        /// Memory limit of the container in bytes.
        MEMORY_LIMIT_BYTES = "memory_limit_bytes";
        /// Bytes that a ClickHouse query read and returned.
        READ_BYTES = "read_bytes";
        /// Bytes that ClickHouse scanned to answer a query.
        SCANNED_BYTES = "scanned_bytes";
        /// Bytes that an indexing run wrote to ClickHouse.
        WRITTEN_BYTES = labkit::WRITTEN_BYTES;
        /// A configured upper bound on a number of items.
        LIMIT = "limit";
        /// A configured upper bound in bytes.
        LIMIT_BYTES = "limit_bytes";
        /// Number of items that one batch or one fetch holds at most.
        BATCH_SIZE = "batch_size";
        /// Maximum number of operations that run at the same time.
        CONCURRENCY = "concurrency";

        /// TCP address that a server listens on, `host:port`.
        TCP_ADDRESS = labkit::TCP_ADDRESS;
        /// Name of a group of server listeners that share TLS settings.
        LISTENER_GROUP = "listener_group";
        /// Snowplow app ID of an event tracker.
        SNOWPLOW_APP_ID = "snowplow_app_id";
        /// Deployment environment that the server reports in analytics.
        DEPLOYMENT_ENVIRONMENT = "deployment_environment";
        /// Authentication mode of an outbound client.
        AUTH_MODE = "auth_mode";
        /// `gkg-server` mode (`webserver`, `indexer`, ...).
        SERVER_MODE = "server_mode";
        /// Indexing mode of a code indexing run.
        INDEXING_MODE = "indexing_mode";
        /// Indexer modules that a process runs, comma separated.
        INDEXER_MODULES = "indexer_modules";
        /// Name of a message handler.
        HANDLER = "handler";
        /// Name of a scheduled task.
        TASK = "task";
        /// Name of an orchestrator trigger.
        TRIGGER = "trigger";
        /// Name of a worker concurrency group.
        WORKER_GROUP = "worker_group";
        /// Worker concurrency groups from the config, comma separated `group=limit` pairs.
        WORKER_GROUPS = "worker_groups";
        /// Name of a server component or a dependency that Orbit deploys, probes or guards.
        COMPONENT = "component";
        /// Kubernetes namespace that Orbit is deployed to.
        K8S_NAMESPACE = "k8s_namespace";
        /// Kubernetes resource kind (`deployment`, `statefulset`).
        K8S_KIND = "k8s_kind";
        /// Programming language that Orbit detects for a file.
        LANGUAGE = "language";
        /// Language family that the code graph indexes together (`c_family`, `jvm`, or a language).
        LANGUAGE_FAMILY = "language_family";
        /// Fixed reason label of a decision, from a closed set that Orbit defines.
        REASON = "reason";
        /// Fixed outcome label (`success`, `error`, `skipped`, ...).
        OUTCOME = "outcome";
        /// Phase or stage of a multi-step operation.
        PHASE = "phase";
        /// Kind label of an item, from a closed set that Orbit defines.
        KIND = "kind";
        /// Previous state label of a state machine transition.
        FROM_STATE = "from_state";
        /// Schema migration scope (`full`, `code`, `sdlc`, `none`).
        MIGRATION_SCOPE = "migration_scope";
        /// Schema migration scope that was requested, before Orbit widens it.
        REQUESTED_MIGRATION_SCOPE = "requested_migration_scope";

        /// Whether TLS is on for a connection or listener.
        TLS_ENABLED = "tls_enabled";
        /// Whether a cache lookup found the item.
        CACHE_HIT = "cache_hit";
        /// Whether a failed token refresh falls back to the cached token.
        SERVING_CACHED_TOKEN = "serving_cached_token";
        /// Whether the cached token is past its expiry.
        CACHED_TOKEN_EXPIRED = "cached_token_expired";
        /// Whether a feature or component is on.
        ENABLED = "enabled";
        /// Whether a dispatcher run sweeps every enabled namespace instead of changed ones.
        FULL_SWEEP = "full_sweep";
        /// Whether an error message is safe to return to the client.
        CLIENT_SAFE = "client_safe";

        /// Fixed error kind, from an error enum variant or a string literal.
        /// Never the error text.
        ERROR_TYPE = labkit::ERROR_TYPE;
        /// HTTP response status code.
        HTTP_STATUS_CODE = "http_status_code";
        /// Source location of a panic, `file:line:column` in Orbit or dependency code.
        PANIC_LOCATION = "panic_location";
    }
}

/// Fields that are written only on expanded log lines.
pub mod expanded {
    use labkit_fields as labkit;

    fields! {
        /// Cloud Connector global user ID.
        GLOBAL_USER_ID = "global_user_id";

        /// Cloud Connector realm value from a token, before Orbit validates it.
        RAW_REALM = "raw_realm";
        /// Block reason text that CustomersDot returns for a quota check.
        BLOCK_REASON = "block_reason";
        /// Name that a client sends for a skill, a command or a tool.
        REQUESTED_NAME = "requested_name";
        /// Session ID from the JWT `session_id` claim. Rails copies it from a client header.
        CLIENT_SESSION_ID = "client_session_id";
        /// Request ID from the JWT `request_id` claim.
        CLIENT_REQUEST_ID = "client_request_id";
        /// Redaction result ID that a client sends back.
        RECEIVED_RESULT_ID = "received_result_id";
        /// Error code that a client sends.
        CLIENT_ERROR_CODE = "client_error_code";
        /// Coding agent name from the client user agent, such as `claude-code`.
        CODING_AGENT = "coding_agent";
        /// URL, without query string or fragment.
        URL = labkit::HTTP_URL;
        /// Host name of the GitLab instance that Orbit calls.
        HOST = labkit::HTTP_HOST;
        /// Host name that Orbit resolves in place of [`HOST`].
        RESOLVE_HOST = "resolve_host";
        /// Socket address of a remote server, `ip:port`.
        REMOTE_ADDR = labkit::REMOTE_ADDR;

        /// SQL text.
        SQL = "sql";
        /// Name of a parameter that is bound to a query.
        QUERY_PARAMETER = "query_parameter";

        /// NATS subject of one message. It can hold an encoded branch name.
        NATS_SUBJECT = "nats_subject";
        /// Key of a NATS KV entry or a checkpoint row.
        KV_KEY = "kv_key";

        /// Path of a file or directory inside a repository.
        FILE_PATH = "file_path";
        /// Path on the local disk of the Orbit server.
        LOCAL_PATH = "local_path";
        /// Path of a package manifest (`Cargo.toml`, `package.json`, ...) on the local disk.
        MANIFEST_PATH = "manifest_path";
        /// Target of a symbolic link, as the link stores it.
        SYMLINK_TARGET = "symlink_target";
        /// Git branch or ref name.
        BRANCH = "branch";
        /// Name of a package or dependency that a manifest declares.
        PACKAGE_NAME = "package_name";

        /// Error text. Log a fixed [`ERROR_TYPE`](super::safe::ERROR_TYPE) next to it.
        ERROR_MESSAGE = labkit::ERROR_MESSAGE;
        /// Free-form details that have no dedicated field.
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
