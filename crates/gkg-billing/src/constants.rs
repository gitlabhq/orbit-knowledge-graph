pub const CDOT_QUOTA_PATH: &str = "/api/v1/consumers/resolve";
pub const CATEGORY: &str = "orbit";
pub const EVENT_TYPE: &str = "orbit_workflow_completion";
pub const METERED_SOURCE_TYPES: &[&str] = &["mcp", "rest"];
pub const QUOTA_DEFAULT_TTL_SECS: u64 = 3600;
pub const QUOTA_MAX_CACHE_ENTRIES: u64 = 10_000;
pub const UNIT_OF_MEASURE: &str = "request";
pub const APP_ID: &str = "gkg-server";
pub const REALM_SAAS: &str = "SaaS";
pub const REALM_SM: &str = "SM";

pub fn feature_qualified_name(source_type: &str) -> String {
    format!("orbit-{source_type}")
}

pub fn normalize_realm(realm: &str) -> Option<&'static str> {
    match realm {
        "saas" | "SaaS" => Some(REALM_SAAS),
        "SM" | "self-managed" => Some(REALM_SM),
        _ => None,
    }
}
