//! Default values for E2E configuration.
//!
//! Every constant here is the fallback used when the corresponding
//! environment variable is not set. Gathered in one place so they are
//! easy to audit and update across releases.
//!
//! User-specific paths (e.g. GITLAB_SRC) are intentionally absent —
//! those are required env vars with no fallback.

// -- Colima / k8s -------------------------------------------------------------

pub const COLIMA_PROFILE: &str = "cng";
pub const COLIMA_MEMORY: &str = "12";
pub const COLIMA_CPUS: &str = "4";
pub const COLIMA_DISK: &str = "60";
pub const COLIMA_K8S_VERSION: &str = "v1.31.5+k3s1";

// -- Kubernetes namespaces ----------------------------------------------------

pub const GITLAB_NS: &str = "gitlab";
pub const DEFAULT_NS: &str = "default";

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

/// Directories staged from the GitLab checkout into the temp build context.
pub const STAGING_DIRS: &[&str] = &["app", "config", "db", "ee", "lib", "locale", "gems"];

// -- PostgreSQL ---------------------------------------------------------------

pub const PG_SECRET_NAME: &str = "gitlab-postgresql-password";
pub const PG_PASSWORD_KEY: &str = "postgresql-password";
pub const PG_SUPERPASS_KEY: &str = "postgresql-postgres-password";
pub const PG_POD: &str = "postgresql-0";
pub const PG_DATABASE: &str = "gitlabhq_production";
pub const PG_USER: &str = "gitlab";

// -- Paths inside pods --------------------------------------------------------

pub const RAILS_ROOT: &str = "/srv/gitlab";
pub const JWT_SECRET_PATH: &str = "/etc/gitlab/shell/.gitlab_shell_secret";
pub const E2E_POD_DIR: &str = "/tmp/e2e";

// -- Pod readiness checks -----------------------------------------------------

/// (label selector, timeout) pairs for GitLab pod readiness.
pub const POD_READINESS_CHECKS: &[(&str, &str)] = &[
    ("app.kubernetes.io/name=postgresql", "600s"),
    ("app=webservice", "600s"),
    ("app=sidekiq", "600s"),
    ("app=toolbox", "300s"),
    ("app=gitaly", "300s"),
];

// -- Log / artifact files cleaned during teardown -----------------------------

pub const TEARDOWN_LOG_FILES: &[&str] = &[
    "create-test-data.log",
    "manifest.json",
    "colima-start.log",
    "tilt-ci.log",
    "tilt-ci.pid",
    "clickhouse-migrate.log",
    "redaction-test.log",
    "tilt-e2e.log",
];
