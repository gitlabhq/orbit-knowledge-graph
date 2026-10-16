//! Observability / metrics configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct MetricsConfig {
    pub log_level: Option<String>,
    #[serde(default)]
    pub expanded_logs: ExpandedLogsConfig,
    pub otel: OtelConfig,
    pub prometheus: PrometheusConfig,
}

/// Which log lines carry expanded fields (file paths, branch names, query text,
/// error text). Every other line carries only the safe fields of
/// `orbit_log_fields::safe`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct ExpandedLogsConfig {
    /// Expand every log line. On by default, so a deployment keeps its full
    /// logs until it turns the override off.
    pub global_override: bool,
    /// Root namespace IDs whose requests and indexing work are always logged
    /// expanded, for example GitLab's own top-level groups.
    pub root_namespace_ids: Vec<u64>,
}

impl Default for ExpandedLogsConfig {
    fn default() -> Self {
        Self {
            global_override: true,
            root_namespace_ids: Vec::new(),
        }
    }
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
