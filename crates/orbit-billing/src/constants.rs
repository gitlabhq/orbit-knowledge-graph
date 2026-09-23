pub const CDOT_QUOTA_PATH: &str = "/api/v1/consumers/resolve";
pub const CATEGORY: &str = "orbit";
pub const EVENT_TYPE: &str = "orbit_workflow_completion";
pub const QUOTA_MAX_CACHE_ENTRIES: u64 = 10_000;
pub const UNIT_OF_MEASURE: &str = "request";
pub const APP_ID: &str = "gkg-server";
pub const REALM_SAAS: &str = "SaaS";
pub const REALM_SM: &str = "SM";

// CDot compares the `realm` param to the license's realm after downcasing and mapping
// `-` to `_`; any other value is a 402 `realm_mismatch`, so "SM" is not accepted.
const CDOT_REALM_SELF_MANAGED: &str = "self_managed";

pub fn is_cdot_self_managed_realm(realm: &str) -> bool {
    realm.to_ascii_lowercase().replace('-', "_") == CDOT_REALM_SELF_MANAGED
}

pub fn feature_qualified_name(source_type: &str) -> String {
    format!("orbit_{source_type}")
}

pub fn normalize_realm(realm: &str) -> Option<&'static str> {
    match realm {
        "saas" | "SaaS" => Some(REALM_SAAS),
        "SM" | "self-managed" => Some(REALM_SM),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdot_self_managed_realm_follows_cdot_normalization() {
        for realm in [
            "self-managed",
            "self_managed",
            "Self-Managed",
            "SELF_MANAGED",
        ] {
            assert!(is_cdot_self_managed_realm(realm), "{realm}");
        }
        for realm in ["SM", "sm", "SaaS", ""] {
            assert!(!is_cdot_self_managed_realm(realm), "{realm}");
        }
    }
}
