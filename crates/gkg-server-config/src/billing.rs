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
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub customers_dot_url: String,
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
            request_timeout_ms: default_quota_timeout_ms(),
            default_ttl_secs: default_quota_ttl_secs(),
            max_cache_entries: default_quota_max_entries(),
        }
    }
}
