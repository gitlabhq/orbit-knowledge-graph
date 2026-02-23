//! Default values for E2E configuration.
//!
//! Every constant here is the fallback used when the corresponding
//! environment variable is not set. Gathered in one place so they are
//! easy to audit and update across releases.
//!
//! User-specific paths (e.g. GITLAB_SRC) are intentionally absent —
//! those are required env vars with no fallback.

use const_format::concatcp;

// -- Colima / k8s -------------------------------------------------------------

pub const COLIMA_PROFILE: &str = "cng";
pub const COLIMA_MEMORY: &str = "12";
pub const COLIMA_CPUS: &str = "4";
pub const COLIMA_DISK: &str = "60";
pub const COLIMA_K8S_VERSION: &str = "v1.31.5+k3s1";

// -- Kubernetes namespaces ----------------------------------------------------

pub const GITLAB_NS: &str = "gitlab";
pub const DEFAULT_NS: &str = "default";
pub const KUBE_SYSTEM_NS: &str = "kube-system";

// -- CNG image settings -------------------------------------------------------

pub const BASE_TAG: &str = "v18.8.1";
pub const CNG_REGISTRY: &str = "registry.gitlab.com/gitlab-org/build/cng";
pub const LOCAL_PREFIX: &str = "gkg-e2e";
pub const LOCAL_TAG: &str = "local";

pub const CNG_COMPONENTS: &[&str] = &[
    "gitlab-webservice-ee",
    "gitlab-sidekiq-ee",
    "gitlab-toolbox-ee",
];

pub const WORKHORSE_COMPONENT: &str = "gitlab-workhorse-ee";

/// Directories staged from the GitLab checkout into the temp build context.
pub const STAGING_DIRS: &[&str] = &["app", "config", "db", "ee", "lib", "locale", "gems"];

// -- PostgreSQL ---------------------------------------------------------------

pub const PG_SECRET_NAME: &str = "gitlab-postgresql-password";
pub const PG_PASSWORD_KEY: &str = "postgresql-password";
pub const PG_SUPERPASS_KEY: &str = "postgresql-postgres-password";
pub const PG_POD: &str = "postgresql-0";
pub const PG_DATABASE: &str = "gitlabhq_production";
pub const PG_USER: &str = "gitlab";
pub const PG_SUPERUSER: &str = "postgres";

/// Secret name for PG credentials bridged to the default namespace (for Siphon).
pub const PG_BRIDGE_SECRET_NAME: &str = "postgres-credentials";
pub const PG_GKG_ENABLED_TABLE: &str = "knowledge_graph_enabled_namespaces";

// -- Paths inside pods --------------------------------------------------------

pub const RAILS_ROOT: &str = "/srv/gitlab";
pub const JWT_SECRET_PATH: &str = "/etc/gitlab/shell/.gitlab_shell_secret";
pub const E2E_POD_DIR: &str = "/tmp/e2e";

// -- Helm releases & repos ----------------------------------------------------

pub const GITLAB_HELM_RELEASE: &str = "gitlab";
pub const GITLAB_HELM_CHART: &str = "gitlab/gitlab";
pub const GITLAB_HELM_REPO_NAME: &str = "gitlab";
pub const GITLAB_HELM_REPO_URL: &str = "https://charts.gitlab.io";
pub const GITLAB_HELM_TIMEOUT: &str = "15m";

pub const TRAEFIK_HELM_RELEASE: &str = "traefik";
pub const TRAEFIK_HELM_CHART: &str = "traefik/traefik";
pub const TRAEFIK_HELM_REPO_NAME: &str = "traefik";
pub const TRAEFIK_HELM_REPO_URL: &str = "https://traefik.github.io/charts";
pub const TRAEFIK_HELM_TIMEOUT: &str = "5m";

pub const HELM_UNINSTALL_TIMEOUT: &str = "5m";

// -- Label selectors ----------------------------------------------------------

pub const TOOLBOX_LABEL: &str = "app=toolbox";

// -- Pod readiness checks -----------------------------------------------------

/// (label selector, timeout) pairs for GitLab pod readiness.
pub const POD_READINESS_CHECKS: &[(&str, &str)] = &[
    ("app.kubernetes.io/name=postgresql", "600s"),
    ("app=webservice", "600s"),
    ("app=sidekiq", "600s"),
    ("app=toolbox", "300s"),
    ("app=gitaly", "300s"),
];

// -- ClickHouse ---------------------------------------------------------------

pub const CH_SERVICE_NAME: &str = "gkg-e2e-clickhouse";
pub const CH_DATALAKE_DB: &str = "gitlab_clickhouse_development";
pub const CH_GRAPH_DB: &str = "gkg-development";
pub const CH_DEFAULT_USER: &str = "default";

// -- Siphon -------------------------------------------------------------------

pub const SIPHON_PUBLICATION: &str = "siphon_publication_main_db";
pub const SIPHON_SLOT: &str = "siphon_slot_main_db";
pub const SIPHON_POLL_TIMEOUT: u64 = 600;

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

/// Poll interval (seconds) for siphon data checks.
pub const SIPHON_POLL_INTERVAL: u64 = 15;

// -- GKG ----------------------------------------------------------------------

pub const GKG_SERVER_IMAGE: &str = "gkg-server";
pub const GKG_DISPATCH_JOB: &str = "gkg-dispatch-indexing";
pub const GKG_INDEXER_CONFIGMAP: &str = "gkg-indexer-config";
pub const GKG_GRPC_ENDPOINT: &str = "gkg-webserver.default.svc.cluster.local:50051";

/// Image tag used by the dispatch-indexing k8s Job.
pub const GKG_DEV_TAG: &str = "dev";

/// k8s secret providing the ClickHouse password to dispatch-indexing.
pub const CH_CREDENTIALS_SECRET: &str = "clickhouse-credentials";
pub const CH_CREDENTIALS_KEY: &str = "password";

/// Helm release name for the GKG chart (NATS + siphon + GKG).
pub const GKG_HELM_RELEASE: &str = "gkg-e2e";

/// ClickHouse init ConfigMap created by clickhouse.yaml.
pub const CH_INIT_CONFIGMAP: &str = "gkg-e2e-clickhouse-init";

/// GKG server credentials secret (JWT secret, created by Tilt).
pub const GKG_SERVER_CREDENTIALS_SECRET: &str = "gkg-server-credentials";

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

// -- Timeouts -----------------------------------------------------------------

pub const CH_POD_TIMEOUT: &str = "300s";
pub const TILT_CI_TIMEOUT: &str = "20m";
pub const DISPATCH_JOB_TIMEOUT: &str = "120s";
pub const INDEXER_POLL_TIMEOUT: u64 = 300;
pub const INDEXER_POLL_INTERVAL: u64 = 10;
pub const INDEXER_SETTLE_SECS: u64 = 30;

// -- Tilt ---------------------------------------------------------------------

pub const TILT_CNG_ENV: &str = "GKG_E2E_CNG";

// -- Directories (relative to GKG repo root) ----------------------------------

pub const CNG_DIR: &str = "e2e/cng";
pub const TILT_DIR: &str = "e2e/tilt";
pub const LOG_DIR: &str = ".dev";
pub const E2E_TESTS_DIR: &str = "e2e/tests";

// -- Paths (relative to GKG repo root) ----------------------------------------

pub const GRAPH_SQL_PATH: &str = "fixtures/schema/graph.sql";
pub const TILTFILE_PATH: &str = concatcp!(TILT_DIR, "/Tiltfile");
pub const DISPATCH_JOB_TEMPLATE: &str = "e2e/templates/dispatch-indexing-job.yaml.tmpl";

// -- Filenames ----------------------------------------------------------------

/// Stem shared by Tilt CI log and PID files.
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
