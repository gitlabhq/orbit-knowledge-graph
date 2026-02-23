//! E2E environment configuration.
//!
//! All configurable values for the E2E harness. Infrastructure defaults
//! (namespaces, PG pod names, in-pod paths, etc.) are stable and have
//! sensible fallbacks. User-specific paths like `GITLAB_SRC` are **required**
//! — the binary panics at startup if they are not set, rather than silently
//! using a path that only works on one developer's machine.

use std::env;
use std::path::PathBuf;

use super::constants as c;
use super::env as e;

/// All configuration for the E2E environment.
pub struct Config {
    // -- Paths ----------------------------------------------------------------
    /// Root of the GKG repository (auto-detected from workspace).
    pub gkg_root: PathBuf,
    /// Path to the e2e/cng directory (contains Dockerfile, values files).
    pub cng_dir: PathBuf,
    /// Path to the Tilt directory (e2e/tilt).
    pub tilt_dir: PathBuf,
    /// Path to the local GitLab Rails checkout.
    pub gitlab_src: PathBuf,
    /// Log / artifact output directory (.dev/).
    pub log_dir: PathBuf,

    // -- Colima / k8s ---------------------------------------------------------
    pub colima_profile: String,
    pub colima_memory: String,
    pub colima_cpus: String,
    pub colima_disk: String,
    pub colima_k8s_version: String,

    // -- Kubernetes namespaces ------------------------------------------------
    pub gitlab_ns: String,
    pub default_ns: String,

    // -- CNG image settings ---------------------------------------------------
    pub base_tag: String,
    pub cng_registry: String,
    pub local_prefix: String,
    pub local_tag: String,
    pub workhorse_image: String,
    pub cng_components: Vec<String>,

    // -- PostgreSQL ------------------------------------------------------------
    pub pg_secret_name: String,
    pub pg_password_key: String,
    pub pg_superpass_key: String,
    pub pg_pod: String,
    pub pg_database: String,
    pub pg_user: String,

    // -- Paths inside pods ----------------------------------------------------
    pub rails_root: String,
    pub jwt_secret_path: String,
    pub e2e_pod_dir: String,
    pub manifest_pod_path: String,

    // -- ClickHouse -----------------------------------------------------------
    pub ch_service_name: String,
    pub ch_url: String,
    pub ch_datalake_db: String,
    pub ch_graph_db: String,

    // -- Siphon ---------------------------------------------------------------
    pub siphon_publication: String,
    pub siphon_slot: String,
    pub siphon_poll_timeout: u64,

    // -- GKG ------------------------------------------------------------------
    pub gkg_server_image: String,
    pub gkg_dispatch_job: String,
    pub gkg_indexer_configmap: String,
    pub gkg_grpc_endpoint: String,
}

impl Config {
    /// Build config from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        let gkg_root = e::workspace_root();
        let cng_dir = gkg_root.join(c::CNG_DIR);
        let tilt_dir = gkg_root.join(c::TILT_DIR);
        let log_dir = gkg_root.join(c::LOG_DIR);

        let base_tag = e::env_or("BASE_TAG", c::BASE_TAG);
        let cng_registry = e::env_or("CNG_REGISTRY", c::CNG_REGISTRY);
        let local_prefix = e::env_or("LOCAL_PREFIX", c::LOCAL_PREFIX);
        let local_tag = e::env_or("LOCAL_TAG", c::LOCAL_TAG);
        let workhorse_image = format!("{cng_registry}/{}:{base_tag}", c::WORKHORSE_COMPONENT);

        let e2e_pod_dir = e::env_or("E2E_POD_DIR", c::E2E_POD_DIR);
        let manifest_pod_path = format!("{e2e_pod_dir}/{}", c::MANIFEST_JSON);

        let ch_service_name = e::env_or("CH_SERVICE_NAME", c::CH_SERVICE_NAME);
        let default_ns_val = e::env_or("DEFAULT_NS", c::DEFAULT_NS);
        let ch_url = e::env_or(
            "CH_URL",
            &format!("http://{ch_service_name}.{default_ns_val}.svc.cluster.local:8123"),
        );
        Self {
            gitlab_src: e::expand_home(&e::require("GITLAB_SRC")),
            cng_dir,
            tilt_dir,
            log_dir,
            gkg_root,

            colima_profile: e::env_or("COLIMA_PROFILE", c::COLIMA_PROFILE),
            colima_memory: e::env_or("COLIMA_MEMORY", c::COLIMA_MEMORY),
            colima_cpus: e::env_or("COLIMA_CPUS", c::COLIMA_CPUS),
            colima_disk: e::env_or("COLIMA_DISK", c::COLIMA_DISK),
            colima_k8s_version: e::env_or("COLIMA_K8S_VERSION", c::COLIMA_K8S_VERSION),

            gitlab_ns: e::env_or("GITLAB_NS", c::GITLAB_NS),
            default_ns: default_ns_val,

            cng_components: c::CNG_COMPONENTS.iter().map(|s| (*s).into()).collect(),
            base_tag,
            cng_registry,
            local_prefix,
            local_tag,
            workhorse_image,

            pg_secret_name: e::env_or("PG_SECRET_NAME", c::PG_SECRET_NAME),
            pg_password_key: e::env_or("PG_PASSWORD_KEY", c::PG_PASSWORD_KEY),
            pg_superpass_key: e::env_or("PG_SUPERPASS_KEY", c::PG_SUPERPASS_KEY),
            pg_pod: e::env_or("PG_POD", c::PG_POD),
            pg_database: e::env_or("PG_DATABASE", c::PG_DATABASE),
            pg_user: e::env_or("PG_USER", c::PG_USER),

            rails_root: e::env_or("RAILS_ROOT", c::RAILS_ROOT),
            jwt_secret_path: e::env_or("JWT_SECRET_PATH", c::JWT_SECRET_PATH),
            e2e_pod_dir,
            manifest_pod_path,

            ch_service_name,
            ch_url,
            ch_datalake_db: e::env_or("CH_DATALAKE_DB", c::CH_DATALAKE_DB),
            ch_graph_db: e::env_or("CH_GRAPH_DB", c::CH_GRAPH_DB),

            siphon_publication: e::env_or("SIPHON_PUBLICATION", c::SIPHON_PUBLICATION),
            siphon_slot: e::env_or("SIPHON_SLOT", c::SIPHON_SLOT),
            siphon_poll_timeout: env::var("SIPHON_POLL_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(c::SIPHON_POLL_TIMEOUT),

            gkg_server_image: e::env_or("GKG_SERVER_IMAGE", c::GKG_SERVER_IMAGE),
            gkg_dispatch_job: e::env_or("GKG_DISPATCH_JOB", c::GKG_DISPATCH_JOB),
            gkg_indexer_configmap: e::env_or("GKG_INDEXER_CONFIGMAP", c::GKG_INDEXER_CONFIGMAP),
            gkg_grpc_endpoint: e::env_or("GKG_GRPC_ENDPOINT", c::GKG_GRPC_ENDPOINT),
        }
    }

    /// Docker socket path for the colima profile.
    pub fn docker_host(&self) -> String {
        let home = env::var("HOME").unwrap_or_default();
        format!("unix://{home}/.colima/{}/docker.sock", self.colima_profile)
    }
}
