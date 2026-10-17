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

/// Which log lines carry expanded fields (file paths, branch names, query text,
/// error text). Every other line carries only the safe fields of the expanded
/// logs policy.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ExpandedLogsConfig {
    /// Expand every log line. On by default, so a deployment keeps its full
    /// logs until it turns the override off.
    pub global_override: bool,
    /// Root namespace IDs whose requests and indexing work are always logged
    /// expanded, for example GitLab's own top-level groups.
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
