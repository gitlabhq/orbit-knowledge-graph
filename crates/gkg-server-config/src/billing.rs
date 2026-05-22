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

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QuotaConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub customers_dot_url: String,
    #[serde(default, skip_serializing)]
    #[schemars(skip)]
    pub api_user: String,
    #[serde(default, skip_serializing)]
    #[schemars(skip)]
    pub api_token: String,
    #[serde(default = "default_quota_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_fallback_ttl_secs")]
    pub fallback_cache_ttl_secs: u64,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            customers_dot_url: String::new(),
            api_user: String::new(),
            api_token: String::new(),
            request_timeout_ms: default_quota_timeout_ms(),
            fallback_cache_ttl_secs: default_fallback_ttl_secs(),
        }
    }
}
