//! Consumer-owned Snowplow context types for Orbit analytics.
//!
//! These replace the deprecated `labkit_events::orbit::*` types.
//! Each struct wraps a `serde_json::Value` payload and implements
//! `SnowplowContext` with the corresponding Iglu schema URI.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use labkit_events::SnowplowContext;
use serde::Serialize;

/// Root of the orbit Iglu schemas in the vendored subtree.
fn iglu_dir() -> PathBuf {
    // IGLU_DIR is set in .cargo/config.toml (relative to workspace root).
    // At runtime, the working directory is the workspace root.
    PathBuf::from(env!("IGLU_DIR"))
}

pub static ORBIT_COMMON_SCHEMA: LazyLock<String> =
    LazyLock::new(|| load_latest_schema_uri(&iglu_dir(), "orbit_common"));

pub static ORBIT_QUERY_SCHEMA: LazyLock<String> =
    LazyLock::new(|| load_latest_schema_uri(&iglu_dir(), "orbit_query"));

/// Scan `{iglu_dir}/{name}/jsonschema/` for the latest version and build
/// the Iglu schema URI from the directory structure.
fn load_latest_schema_uri(iglu_dir: &Path, name: &str) -> String {
    let version = latest_version(iglu_dir, name);
    format!("iglu:com.gitlab/{name}/jsonschema/{version}")
}

/// Find the latest version directory name under `{iglu_dir}/{name}/jsonschema/`.
fn latest_version(iglu_dir: &Path, name: &str) -> String {
    let jsonschema_dir = iglu_dir.join(name).join("jsonschema");
    let mut versions: Vec<(u32, u32, u32, String)> = std::fs::read_dir(&jsonschema_dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", jsonschema_dir.display()))
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().to_str()?.to_string();
            let parts: Vec<u32> = name.split('-').filter_map(|p| p.parse().ok()).collect();
            (parts.len() == 3).then(|| (parts[0], parts[1], parts[2], name))
        })
        .collect();
    versions.sort();
    versions
        .last()
        .unwrap_or_else(|| panic!("no versions in {}", jsonschema_dir.display()))
        .3
        .clone()
}

/// Load the raw schema JSON for a given schema name (latest version).
pub fn load_latest_schema_json(name: &str) -> serde_json::Value {
    let dir = iglu_dir();
    let version = latest_version(&dir, name);
    let path = dir.join(name).join("jsonschema").join(&version);
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&content).expect("vendored Iglu schema is valid JSON")
}

// ─────────────────────────────────────────────────────────────────────────────
// orbit_common
// ─────────────────────────────────────────────────────────────────────────────

pub struct OrbitCommonContext {
    data: serde_json::Value,
}

impl SnowplowContext for OrbitCommonContext {
    fn schema(&self) -> &str {
        &ORBIT_COMMON_SCHEMA
    }

    fn data(&self) -> serde_json::Value {
        self.data.clone()
    }
}

#[derive(Serialize)]
pub struct OrbitCommonData<'a> {
    pub deployment_type: &'a str,
    pub environment: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_instance_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_namespace_ids: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<&'a str>,
}

impl OrbitCommonContext {
    pub fn new(data: OrbitCommonData<'_>) -> Self {
        Self {
            data: serde_json::to_value(data).expect("OrbitCommonData is always serializable"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// orbit_query
// ─────────────────────────────────────────────────────────────────────────────

pub struct OrbitQueryContext {
    data: serde_json::Value,
}

impl SnowplowContext for OrbitQueryContext {
    fn schema(&self) -> &str {
        &ORBIT_QUERY_SCHEMA
    }

    fn data(&self) -> serde_json::Value {
        self.data.clone()
    }
}

#[derive(Serialize)]
pub struct OrbitQueryData<'a> {
    pub source_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coding_agent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queried_namespace_ids: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_namespace_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_user_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<&'a str>,
}

impl OrbitQueryContext {
    pub fn new(data: OrbitQueryData<'_>) -> Self {
        Self {
            data: serde_json::to_value(data).expect("OrbitQueryData is always serializable"),
        }
    }
}
