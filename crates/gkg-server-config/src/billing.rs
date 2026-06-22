use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct BillingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub collector_url: String,
    #[serde(default)]
    pub quota: QuotaConfig,
}

fn default_quota_timeout_ms() -> u64 {
    1000
}

fn default_fallback_ttl_secs() -> u64 {
    3600
}

fn default_entitlement_fail_closed() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QuotaConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub customers_dot_url: String,
    #[serde(default)]
    pub api_user: Option<String>,
    #[serde(default)]
    pub api_token: Option<String>,
    #[serde(default = "default_quota_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_fallback_ttl_secs")]
    pub fallback_cache_ttl_secs: u64,
    /// When true, a CustomersDot 403 (not entitled) or 422 (invalid claim) on a SaaS
    /// request denies (`RESOURCE_EXHAUSTED`) instead of failing open. Kill-switch: set
    /// false to revert to fail-open. Self-managed realms are unaffected — CustomersDot
    /// owns their fail-close policy (`:fail_close_policy`, Dedicated-excluded).
    #[serde(default = "default_entitlement_fail_closed")]
    pub entitlement_fail_closed: bool,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            customers_dot_url: String::new(),
            api_user: None,
            api_token: None,
            request_timeout_ms: default_quota_timeout_ms(),
            fallback_cache_ttl_secs: default_fallback_ttl_secs(),
            entitlement_fail_closed: default_entitlement_fail_closed(),
        }
    }
}
