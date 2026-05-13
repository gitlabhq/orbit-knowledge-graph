pub const CATEGORY: &str = "orbit";
pub const EVENT_TYPE: &str = "orbit_workflow_completion";
pub const UNIT_OF_MEASURE: &str = "request";
pub const APP_ID: &str = "gkg-server";

/// GKG-owned identifier for the Orbit feature being consumed. The same value
/// is sent to CustomersDot as the `feature_qualified_name` cache-key field on
/// quota checks and embedded in the Snowplow billing event metadata, so the
/// two observability surfaces always agree per request.
pub fn feature_qualified_name(source_type: &str) -> String {
    format!("orbit-{source_type}")
}

pub fn normalize_realm(realm: &str) -> Option<&'static str> {
    match realm {
        "saas" | "SaaS" => Some("SaaS"),
        "SM" | "self-managed" => Some("SM"),
        _ => None,
    }
}
