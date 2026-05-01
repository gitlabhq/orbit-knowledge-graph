pub const CATEGORY: &str = "orbit";
pub const EVENT_TYPE: &str = "orbit_workflow_completion";
pub const UNIT_OF_MEASURE: &str = "request";
pub const APP_ID: &str = "gkg-server";

pub fn normalize_realm(realm: &str) -> Option<&'static str> {
    match realm {
        "saas" | "SaaS" => Some("SaaS"),
        "SM" | "self-managed" => Some("SM"),
        _ => None,
    }
}
