use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BillingAuthMode {
    /// GCP workload-identity OIDC. Only works on GitLab.com (SaaS).
    #[default]
    Oidc,
    /// Cloud Connector instance token, pulled from Rails and cached in memory.
    /// Used on Self-Managed / Dedicated.
    CloudConnector,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct BillingConfig {
    pub enabled: bool,
    pub collector_url: String,
    pub auth_mode: BillingAuthMode,
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
