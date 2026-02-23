//! Structural constants for the E2E harness.
//!
//! File paths, table lists, filenames, and env var names. These are not
//! configurable — they define the shape of the harness itself.
//!
//! All configurable defaults (namespaces, timeouts, image tags, etc.)
//! live in `e2e/config.yaml` and are loaded by `config.rs`.

use const_format::concatcp;

// -- Config file path ---------------------------------------------------------

pub const CONFIG_YAML: &str = "e2e/config.yaml";

// -- Tilt env var -------------------------------------------------------------

pub const TILT_CNG_ENV: &str = "GKG_E2E_CNG";

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
pub const TILT_DIR: &str = "e2e/tilt";
pub const LOG_DIR: &str = ".dev";
pub const E2E_TESTS_DIR: &str = "e2e/tests";

// -- Paths (relative to GKG repo root) ----------------------------------------

pub const GRAPH_SQL_PATH: &str = "fixtures/schema/graph.sql";
pub const TILTFILE_PATH: &str = concatcp!(TILT_DIR, "/Tiltfile");
pub const DISPATCH_JOB_TEMPLATE: &str = "e2e/templates/dispatch-indexing-job.yaml.tmpl";
pub const CLICK_HOUSE_YML_TEMPLATE: &str = "e2e/templates/click-house.yml.tmpl";

// -- Filenames ----------------------------------------------------------------

const TILT_CI_STEM: &str = "tilt-ci";
pub const TILT_CI_LOG: &str = concatcp!(TILT_CI_STEM, ".log");
pub const TILT_CI_PID: &str = concatcp!(TILT_CI_STEM, ".pid");

pub const CLICKHOUSE_YAML: &str = "clickhouse.yaml";
pub const CREATE_TEST_DATA_LOG: &str = "create-test-data.log";
pub const MANIFEST_JSON: &str = "manifest.json";
pub const SECRETS_FILE: &str = ".secrets";
pub const TRAEFIK_VALUES_YAML: &str = "traefik-values.yaml";
pub const GITLAB_VALUES_YAML: &str = "gitlab-values.yaml";
pub const DOCKERFILE_RAILS: &str = "Dockerfile.rails";
pub const COLIMA_START_LOG: &str = "colima-start.log";
pub const CH_MIGRATE_LOG: &str = "clickhouse-migrate.log";
pub const REDACTION_TEST_RB: &str = "redaction_test.rb";
pub const REDACTION_TEST_LOG: &str = "redaction-test.log";
pub const TILT_E2E_LOG: &str = "tilt-e2e.log";

// -- Log / artifact files cleaned during teardown -----------------------------

pub const TEARDOWN_LOG_FILES: &[&str] = &[
    CREATE_TEST_DATA_LOG,
    MANIFEST_JSON,
    COLIMA_START_LOG,
    TILT_CI_LOG,
    TILT_CI_PID,
    CH_MIGRATE_LOG,
    REDACTION_TEST_LOG,
    TILT_E2E_LOG,
];

/// Subset of log files removed during GKG-only teardown.
/// Omits CNG-phase logs (colima-start, create-test-data, manifest).
pub const GKG_TEARDOWN_LOG_FILES: &[&str] = &[
    TILT_CI_LOG,
    TILT_CI_PID,
    CH_MIGRATE_LOG,
    REDACTION_TEST_LOG,
    TILT_E2E_LOG,
];
