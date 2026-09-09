use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct BillingConfig {
    pub enabled: bool,
    pub collector_url: String,
    pub quota: QuotaConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QuotaConfig {
    pub enabled: bool,
    pub customers_dot_url: String,
    pub api_user: Option<String>,
    pub api_token: Option<String>,
    pub request_timeout_ms: u64,
    pub fallback_cache_ttl_secs: u64,
}
