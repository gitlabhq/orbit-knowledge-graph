//! Structural constants for the E2E harness.
//!
//! File paths, table lists, filenames, env var names, and concurrency limits.
//! These are not configurable — they define the shape of the harness itself.
//!
//! All configurable defaults (namespaces, timeouts, image tags, etc.)
//! live in `config/e2e.yaml` and are loaded by `config.rs`.

// -- Preflight: required CLI tools --------------------------------------------

pub const REQUIRED_TOOLS: &[&str] = &["colima", "docker", "helm"];

// -- Environment variable names -----------------------------------------------

pub const DOCKER_HOST_ENV: &str = "DOCKER_HOST";
pub const GITLAB_SRC_ENV: &str = "GITLAB_SRC";

// -- SSA field manager (kube-rs server-side apply) ----------------------------

pub const SSA_FIELD_MANAGER: &str = "xtask";

// -- Config file path ---------------------------------------------------------

pub const CONFIG_YAML: &str = "config/e2e.yaml";

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
pub const RAILS_CLICKHOUSE_CONFIG_TEMPLATE: &str = "e2e/templates/rails-clickhouse-config.yml.tmpl";

// -- Filenames ----------------------------------------------------------------

pub const CLICKHOUSE_YAML_TEMPLATE: &str = "e2e/cng/clickhouse.yaml.tmpl";
pub const CREATE_TEST_DATA_LOG: &str = "create-test-data.log";
pub const MANIFEST_JSON: &str = "manifest.json";
pub const TRAEFIK_VALUES_YAML: &str = "traefik-values.yaml";
pub const GITLAB_VALUES_YAML: &str = "gitlab-values.yaml";
pub const DOCKERFILE_RAILS: &str = "Dockerfile.rails";
pub const COLIMA_START_LOG: &str = "colima-start.log";
pub const CH_MIGRATE_LOG: &str = "clickhouse-migrate.log";
pub const REDACTION_TEST_RB: &str = "redaction_test.rb";
pub const REDACTION_TEST_LOG: &str = "redaction-test.log";
pub const TEST_RESULTS_JSON: &str = "test-results.json";

// -- Log / artifact files cleaned during teardown -----------------------------

pub const TEARDOWN_LOG_FILES: &[&str] = &[
    CREATE_TEST_DATA_LOG,
    MANIFEST_JSON,
    COLIMA_START_LOG,
    CH_MIGRATE_LOG,
    REDACTION_TEST_LOG,
    TEST_RESULTS_JSON,
];

/// Subset of log files removed during GKG-only teardown.
/// Omits CNG-phase logs (colima-start, create-test-data, manifest).
pub const GKG_TEARDOWN_LOG_FILES: &[&str] =
    &[CH_MIGRATE_LOG, REDACTION_TEST_LOG, TEST_RESULTS_JSON];

// -- GKG Helm chart deployments (rollout restart targets) ---------------------

/// Deployment names created by the GKG Helm chart in helm-dev/gkg/templates/.
pub const GKG_DEPLOYMENTS: &[&str] = &[
    "gkg-indexer",
    "gkg-webserver",
    "gkg-health-check",
    "siphon-producer",
    "siphon-consumer",
];

/// Datalake tables dumped for diagnostics after indexing.
pub const DATALAKE_DIAGNOSTIC_TABLES: &[&str] = &[
    "hierarchy_merge_requests",
    "hierarchy_work_items",
    "siphon_merge_requests",
    "siphon_issues",
    "siphon_namespace_details",
    "siphon_namespaces",
    "project_namespace_traversal_paths",
    "namespace_traversal_paths",
    "siphon_organizations",
];

// -- Concurrency limits -------------------------------------------------------

pub const CH_OPTIMIZE_CONCURRENCY: usize = 3;

// -- Diagnostic defaults ------------------------------------------------------

pub const DIAGNOSTIC_LOG_TAIL_LINES: i64 = 30;

// -- Colima docker socket path pattern ----------------------------------------

pub const COLIMA_SOCKET_TEMPLATE: &str = ".colima/{}/docker.sock";

// -- Rails environment --------------------------------------------------------

pub const RAILS_ENV: &str = "production";

// -- Docker client ------------------------------------------------------------

pub const DOCKER_SOCKET_TIMEOUT: u64 = 120;

// -- CNG image build concurrency ----------------------------------------------

pub const CNG_BUILD_CONCURRENCY: usize = 3;
