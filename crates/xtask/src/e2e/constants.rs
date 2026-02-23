//! Structural constants for the E2E harness.
//!
//! File paths, table lists, filenames, and env var names. These are not
//! configurable — they define the shape of the harness itself.
//!
//! All configurable defaults (namespaces, timeouts, image tags, etc.)
//! live in `e2e/config.yaml` and are loaded by `config.rs`.

// -- Config file path ---------------------------------------------------------

pub const CONFIG_YAML: &str = "e2e/config.yaml";

// -- Table lists (iteration targets, not config) ------------------------------

/// Datalake tables polled during step 21 to confirm siphon data is flowing
/// before dispatch-indexing.
///
/// `hierarchy_merge_requests` — canary for the full MV chain (namespace
///   traversal paths → project traversal paths → hierarchy MV).
/// `siphon_knowledge_graph_enabled_namespaces` — the dispatcher reads this
///   to discover which namespaces to index.
/// `siphon_namespace_details` — the Group entity's ETL query does a 3-way
///   INNER JOIN (`siphon_namespaces ⋈ siphon_namespace_details ⋈
///   namespace_traversal_paths`). This table is replicated independently
///   from the others and can lag behind, causing `gl_group: 0` if the
///   indexer runs before it arrives. Polling it here prevents that race.
///
/// We intentionally do NOT poll `hierarchy_work_items` here — it may lag
/// behind `hierarchy_merge_requests` and is not required pre-dispatch. The
/// post-indexer poll (step 22) waits for every graph table including
/// `gl_work_item`, which is the real gate.
pub const SIPHON_POLL_TABLES: &[&str] = &[
    "hierarchy_merge_requests",
    "siphon_knowledge_graph_enabled_namespaces",
    "siphon_namespace_details",
];

/// ClickHouse graph tables operated on by OPTIMIZE TABLE FINAL and row-count
/// verification.
pub const GL_TABLES: &[&str] = &[
    "gl_user",
    "gl_group",
    "gl_project",
    "gl_merge_request",
    "gl_work_item",
    "gl_note",
    "gl_milestone",
    "gl_label",
    "gl_edge",
];

// -- Directories (relative to GKG repo root) ----------------------------------

pub const CNG_DIR: &str = "e2e/cng";
pub const LOG_DIR: &str = ".dev";
pub const E2E_TESTS_DIR: &str = "e2e/tests";

// -- Paths (relative to GKG repo root) ----------------------------------------

pub const GRAPH_SQL_PATH: &str = "fixtures/schema/graph.sql";
pub const GKG_CHART_PATH: &str = "helm-dev/gkg";
pub const HELM_VALUES_YAML: &str = "e2e/helm-values.yaml";
pub const BUILD_DEV_SCRIPT: &str = "scripts/build-dev.sh";
pub const DISPATCH_JOB_TEMPLATE: &str = "e2e/templates/dispatch-indexing-job.yaml.tmpl";
pub const CLICK_HOUSE_YML_TEMPLATE: &str = "e2e/templates/click-house.yml.tmpl";

// -- Filenames ----------------------------------------------------------------

pub const CLICKHOUSE_YAML: &str = "clickhouse.yaml";
pub const CREATE_TEST_DATA_LOG: &str = "create-test-data.log";
pub const MANIFEST_JSON: &str = "manifest.json";
pub const TRAEFIK_VALUES_YAML: &str = "traefik-values.yaml";
pub const GITLAB_VALUES_YAML: &str = "gitlab-values.yaml";
pub const DOCKERFILE_RAILS: &str = "Dockerfile.rails";
pub const COLIMA_START_LOG: &str = "colima-start.log";
pub const CH_MIGRATE_LOG: &str = "clickhouse-migrate.log";
pub const REDACTION_TEST_RB: &str = "redaction_test.rb";
pub const REDACTION_TEST_LOG: &str = "redaction-test.log";

// -- Log / artifact files cleaned during teardown -----------------------------

pub const TEARDOWN_LOG_FILES: &[&str] = &[
    CREATE_TEST_DATA_LOG,
    MANIFEST_JSON,
    COLIMA_START_LOG,
    CH_MIGRATE_LOG,
    REDACTION_TEST_LOG,
];

/// Subset of log files removed during GKG-only teardown.
/// Omits CNG-phase logs (colima-start, create-test-data, manifest).
pub const GKG_TEARDOWN_LOG_FILES: &[&str] = &[CH_MIGRATE_LOG, REDACTION_TEST_LOG];
