//! Consumer-owned Snowplow context types for Orbit analytics.
//!
//! These replace the deprecated `labkit_events::orbit::*` types.
//! Each struct wraps a `serde_json::Value` payload and implements
//! `SnowplowContext` with the corresponding Iglu schema URI.

use labkit_events::SnowplowContext;
use serde::Serialize;

pub const ORBIT_COMMON_SCHEMA: &str = "iglu:com.gitlab/orbit_common/jsonschema/1-0-0";
pub const ORBIT_QUERY_SCHEMA: &str = "iglu:com.gitlab/orbit_query/jsonschema/2-0-1";

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
}

impl OrbitQueryContext {
    pub fn new(data: OrbitQueryData<'_>) -> Self {
        Self {
            data: serde_json::to_value(data).expect("OrbitQueryData is always serializable"),
        }
    }
}
