//! Consumer-owned Snowplow context types for Orbit analytics.
//!
//! These replace the deprecated `labkit_events::orbit::*` types.
//! Each struct wraps a `serde_json::Value` payload and implements
//! `SnowplowContext` with the corresponding Iglu schema URI.

use labkit_events::SnowplowContext;
use serde::Serialize;

// Pinned versions, schema URIs, and full schema JSON are inlined at compile time
// by `build.rs` from `config/schemas/iglu/*.version` and the vendored Iglu subtree.
// The runtime binary never reads these files.
include!(concat!(env!("OUT_DIR"), "/iglu_schemas.rs"));

/// Return the inlined schema JSON for a given schema name at its pinned version.
pub fn load_schema_json(name: &str) -> serde_json::Value {
    let raw = match name {
        "orbit_common" => ORBIT_COMMON_SCHEMA_JSON,
        "orbit_query" => ORBIT_QUERY_SCHEMA_JSON,
        other => panic!("unknown iglu schema {other:?}"),
    };
    serde_json::from_str(raw).expect("vendored Iglu schema is valid JSON")
}

// ─────────────────────────────────────────────────────────────────────────────
// orbit_common
// ─────────────────────────────────────────────────────────────────────────────

pub struct OrbitCommonContext {
    data: serde_json::Value,
}

impl SnowplowContext for OrbitCommonContext {
    fn schema(&self) -> &str {
        ORBIT_COMMON_SCHEMA
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
        ORBIT_QUERY_SCHEMA
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
