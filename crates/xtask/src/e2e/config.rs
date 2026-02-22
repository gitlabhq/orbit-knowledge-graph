//! E2E environment configuration.
//!
//! All configurable values for the E2E harness. Each field can be overridden
//! via the corresponding environment variable.

use std::env;
use std::path::PathBuf;

/// Resolve `~` at the start of a path to `$HOME`.
fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = env::var("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(path)
}

/// Read an env var or return the default.
fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

/// All configuration for the E2E environment.
pub struct Config {
    // -- Paths ----------------------------------------------------------------
    /// Root of the GKG repository (auto-detected from workspace).
    pub gkg_root: PathBuf,
    /// Path to the e2e/cng directory (contains Dockerfile, values files).
    pub cng_dir: PathBuf,
    /// Path to the local GitLab Rails checkout.
    pub gitlab_src: PathBuf,

    // -- Colima / k8s ---------------------------------------------------------
    pub colima_profile: String,
    pub colima_memory: String,
    pub colima_cpus: String,
    pub colima_disk: String,
    pub colima_k8s_version: String,

    // -- Kubernetes namespaces ------------------------------------------------
    pub gitlab_ns: String,

    // -- CNG image settings ---------------------------------------------------
    pub base_tag: String,
    pub cng_registry: String,
    pub local_prefix: String,
    pub local_tag: String,
    pub workhorse_image: String,
    pub cng_components: Vec<String>,
}

impl Config {
    /// Build config from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        let gkg_root = workspace_root();
        let cng_dir = gkg_root.join("e2e/cng");

        let base_tag = env_or("BASE_TAG", "v18.8.1");
        let cng_registry = env_or("CNG_REGISTRY", "registry.gitlab.com/gitlab-org/build/cng");
        let local_prefix = env_or("LOCAL_PREFIX", "gkg-e2e");
        let local_tag = env_or("LOCAL_TAG", "local");
        let workhorse_image = format!("{cng_registry}/gitlab-workhorse-ee:{base_tag}");

        Self {
            gitlab_src: expand_home(&env_or("GITLAB_SRC", "~/Desktop/Code/gdk/gitlab")),
            cng_dir,
            gkg_root,

            colima_profile: env_or("COLIMA_PROFILE", "cng"),
            colima_memory: env_or("COLIMA_MEMORY", "12"),
            colima_cpus: env_or("COLIMA_CPUS", "4"),
            colima_disk: env_or("COLIMA_DISK", "60"),
            colima_k8s_version: env_or("COLIMA_K8S_VERSION", "v1.31.5+k3s1"),

            gitlab_ns: env_or("GITLAB_NS", "gitlab"),

            cng_components: vec![
                "gitlab-webservice-ee".into(),
                "gitlab-sidekiq-ee".into(),
                "gitlab-toolbox-ee".into(),
            ],
            base_tag,
            cng_registry,
            local_prefix,
            local_tag,
            workhorse_image,
        }
    }

    /// Docker socket path for the colima profile.
    pub fn docker_host(&self) -> String {
        let home = env::var("HOME").unwrap_or_default();
        format!("unix://{home}/.colima/{}/docker.sock", self.colima_profile)
    }
}

/// Find the workspace root by walking up from the xtask binary location.
/// Falls back to CARGO_MANIFEST_DIR at compile time.
fn workspace_root() -> PathBuf {
    // At runtime: the binary is at <root>/target/debug/xtask.
    // Walk up from current exe to find Cargo.toml with [workspace].
    if let Ok(exe) = env::current_exe() {
        let mut dir = exe.as_path();
        while let Some(parent) = dir.parent() {
            if parent.join("Cargo.toml").exists() && parent.join("crates").exists() {
                return parent.to_path_buf();
            }
            dir = parent;
        }
    }

    // Compile-time fallback: xtask's Cargo.toml is at crates/xtask/
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .expect("could not determine workspace root")
}
