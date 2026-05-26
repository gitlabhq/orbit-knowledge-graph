//! Consumer-owned Snowplow context types for Orbit analytics.
//!
//! These replace the deprecated `labkit_events::orbit::*` types.
//! Each struct wraps a `serde_json::Value` payload and implements
//! `SnowplowContext` with the corresponding Iglu schema URI.

use std::path::PathBuf;
use std::sync::LazyLock;

use labkit_events::SnowplowContext;
use serde::Serialize;

/// Read the pinned version from a `.iglu-version` file.
fn pinned_version(name: &str) -> String {
    let path = PathBuf::from(env!("SCHEMA_DIR")).join("iglu").join(format!("{name}.iglu-version"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
        .trim()
        .to_string()
}

/// Resolve the schema JSON path in the vendored subtree for a pinned version.
fn schema_path(name: &str, version: &str) -> PathBuf {
    PathBuf::from(env!("IGLU_DIR"))
        .join(name)
        .join("jsonschema")
        .join(version)
}

pub static ORBIT_COMMON_SCHEMA: LazyLock<String> = LazyLock::new(|| {
    let version = pinned_version("orbit_common");
    format!("iglu:com.gitlab/orbit_common/jsonschema/{version}")
});

pub static ORBIT_QUERY_SCHEMA: LazyLock<String> = LazyLock::new(|| {
    let version = pinned_version("orbit_query");
    format!("iglu:com.gitlab/orbit_query/jsonschema/{version}")
});

/// Load the schema JSON for a given schema name at its pinned version.
pub fn load_schema_json(name: &str) -> serde_json::Value {
    let version = pinned_version(name);
    let path = schema_path(name, &version);
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_gitlab_team_member: Option<bool>,
}

impl OrbitQueryContext {
    pub fn new(data: OrbitQueryData<'_>) -> Self {
        Self {
            data: serde_json::to_value(data).expect("OrbitQueryData is always serializable"),
        }
    }
}
