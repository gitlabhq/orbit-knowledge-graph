//! E2E environment configuration.
//!
//! Deserializes `e2e/config.yaml` directly into [`Config`].  The only env var
//! is `GITLAB_SRC` — a required, user-specific path with no YAML default.

use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use super::constants as c;
use super::env as e;

// =============================================================================
// Sub-structs — mirror the YAML sections, deserialized directly.
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct Colima {
    pub profile: String,
    pub memory: String,
    pub cpus: String,
    pub disk: String,
    pub k8s_version: String,
}

#[derive(Debug, Deserialize)]
pub struct Namespaces {
    pub gitlab: String,
    pub default: String,
    pub kube_system: String,
}

#[derive(Debug, Deserialize)]
pub struct Cng {
    pub base_tag: String,
    pub registry: String,
    pub local_prefix: String,
    pub local_tag: String,
    pub workhorse_component: String,
    pub components: Vec<String>,
    pub staging_dirs: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Helm {
    pub gitlab: HelmRelease,
    pub traefik: HelmRelease,
    pub gkg: HelmGkg,
    pub uninstall_timeout: String,
}

#[derive(Debug, Deserialize)]
pub struct HelmRelease {
    pub release: String,
    pub chart: String,
    pub repo_name: String,
    pub repo_url: String,
    pub timeout: String,
}

#[derive(Debug, Deserialize)]
pub struct HelmGkg {
    pub release: String,
}

#[derive(Debug, Deserialize)]
pub struct Postgres {
    pub secret_name: String,
    pub password_key: String,
    pub superpass_key: String,
    pub pod: String,
    pub database: String,
    pub user: String,
    pub superuser: String,
    pub bridge_secret_name: String,
    pub kg_enabled_table: String,
}

#[derive(Debug, Deserialize)]
pub struct PodPaths {
    pub rails_root: String,
    pub jwt_secret_path: String,
    pub e2e_pod_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct ClickHouse {
    pub service_name: String,
    pub datalake_db: String,
    pub graph_db: String,
    pub default_user: String,
    pub init_configmap: String,
    pub credentials_secret: String,
    pub credentials_key: String,
}

#[derive(Debug, Deserialize)]
pub struct Siphon {
    pub publication: String,
    pub slot: String,
    pub poll_timeout: u64,
    pub poll_interval: u64,
}

#[derive(Debug, Deserialize)]
pub struct Gkg {
    pub server_image: String,
    pub dev_tag: String,
    pub dispatch_job: String,
    pub indexer_configmap: String,
    pub grpc_endpoint: String,
    pub server_credentials_secret: String,
}

#[derive(Debug, Deserialize)]
pub struct Labels {
    pub toolbox: String,
}

#[derive(Debug, Deserialize)]
pub struct PodReadiness {
    pub label: String,
    pub timeout: String,
}

#[derive(Debug, Deserialize)]
pub struct Timeouts {
    pub ch_pod: String,
    pub gkg_chart: String,
    pub dispatch_job: String,
    pub indexer_poll: u64,
    pub indexer_poll_interval: u64,
    pub indexer_settle: u64,
}

// =============================================================================
// Top-level Config
// =============================================================================

/// All configuration for the E2E environment.
///
/// YAML-sourced sections are deserialized directly.  Runtime-derived paths
/// (`gkg_root`, `cng_dir`, etc.) are populated in [`Config::load`].
#[derive(Debug, Deserialize)]
pub struct Config {
    // -- YAML sections (deserialized) -----------------------------------------
    pub colima: Colima,
    pub namespaces: Namespaces,
    pub cng: Cng,
    pub helm: Helm,
    pub postgres: Postgres,
    pub pod_paths: PodPaths,
    pub clickhouse: ClickHouse,
    pub siphon: Siphon,
    pub gkg: Gkg,
    pub labels: Labels,
    pub pod_readiness: Vec<PodReadiness>,
    pub timeouts: Timeouts,

    // -- Runtime-derived paths (not in YAML) ----------------------------------
    #[serde(skip)]
    pub gkg_root: PathBuf,
    #[serde(skip)]
    pub cng_dir: PathBuf,
    #[serde(skip)]
    pub gitlab_src: PathBuf,
    #[serde(skip)]
    pub log_dir: PathBuf,
}

impl Config {
    /// Load `e2e/config.yaml`, populate runtime paths, return Config.
    pub fn load() -> Result<Self> {
        let gkg_root = e::workspace_root();
        let yaml_path = gkg_root.join(c::CONFIG_YAML);

        if !yaml_path.exists() {
            bail!("missing {}: this file is required", yaml_path.display());
        }
        let contents = std::fs::read_to_string(&yaml_path)
            .with_context(|| format!("reading {}", yaml_path.display()))?;
        let mut cfg: Self = serde_yaml::from_str(&contents)
            .with_context(|| format!("parsing {}", yaml_path.display()))?;

        cfg.cng_dir = gkg_root.join(c::CNG_DIR);
        cfg.log_dir = gkg_root.join(c::LOG_DIR);
        cfg.gitlab_src = e::expand_home(&e::require("GITLAB_SRC"));
        cfg.gkg_root = gkg_root;

        Ok(cfg)
    }

    /// Docker socket path for the colima profile.
    pub fn docker_host(&self) -> String {
        let home = env::var("HOME").unwrap_or_default();
        format!("unix://{home}/.colima/{}/docker.sock", self.colima.profile)
    }

    /// Workhorse container image (computed from CNG registry + tag).
    pub fn workhorse_image(&self) -> String {
        format!(
            "{}/{}:{}",
            self.cng.registry, self.cng.workhorse_component, self.cng.base_tag
        )
    }

    /// ClickHouse HTTP URL (computed from service name + default namespace).
    pub fn ch_url(&self) -> String {
        format!(
            "http://{}.{}.svc.cluster.local:8123",
            self.clickhouse.service_name, self.namespaces.default,
        )
    }

    /// Path to manifest.json inside the pod.
    pub fn manifest_pod_path(&self) -> String {
        format!("{}/{}", self.pod_paths.e2e_pod_dir, c::MANIFEST_JSON)
    }

    /// Kubernetes label selector for ClickHouse pods.
    pub fn ch_label(&self) -> String {
        format!("app={}", self.clickhouse.service_name)
    }
}
