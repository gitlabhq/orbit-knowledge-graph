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
    2000
}

fn default_quota_ttl_secs() -> u64 {
    3600
}

fn default_quota_max_entries() -> u64 {
    10_000
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QuotaConfig {
    /// Master switch for quota checks. Independent of `billing.enabled` —
    /// billing event emission and quota enforcement can be toggled separately.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub customers_dot_url: String,
    /// CustomersDot admin email, sent as the `X-Admin-Email` header. Mirrors
    /// AIGW's `CUSTOMER_PORTAL_USAGE_QUOTA_API_USER`. When this or
    /// `api_token` is empty the gate runs disabled (no upstream calls).
    #[serde(default)]
    pub api_user: String,
    /// CustomersDot admin token, sent as the `X-Admin-Token` header. Sourced
    /// from `/etc/secrets/billing__quota__api_token` in K8s or from the
    /// `GKG_BILLING__QUOTA__API_TOKEN` env var locally.
    #[serde(default)]
    pub api_token: String,
    #[serde(default = "default_quota_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_quota_ttl_secs")]
    pub default_ttl_secs: u64,
    #[serde(default = "default_quota_max_entries")]
    pub max_cache_entries: u64,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            customers_dot_url: String::new(),
            api_user: String::new(),
            api_token: String::new(),
            request_timeout_ms: default_quota_timeout_ms(),
            default_ttl_secs: default_quota_ttl_secs(),
            max_cache_entries: default_quota_max_entries(),
        }
    }
}
