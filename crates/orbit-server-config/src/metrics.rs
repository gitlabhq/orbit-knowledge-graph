//! Observability / metrics configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct MetricsConfig {
    pub log_level: Option<String>,
    pub expanded_logs: ExpandedLogsConfig,
    pub otel: OtelConfig,
    pub prometheus: PrometheusConfig,
}

/// Which log lines carry expanded fields; other lines carry only safe fields.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ExpandedLogsConfig {
    /// Expand every log line.
    pub global_override: bool,
    /// Root namespace IDs whose logs are always expanded.
    pub root_namespace_ids: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct OtelConfig {
    pub enabled: bool,
    pub endpoint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PrometheusConfig {
    pub enabled: bool,
    pub port: Option<u16>,
}
