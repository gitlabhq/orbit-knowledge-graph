//! Observability / metrics configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default)]
    pub log_level: Option<String>,
    #[serde(default)]
    pub otel: OtelConfig,
    #[serde(default)]
    pub prometheus: PrometheusConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct OtelConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: String,
}

fn default_prometheus_port() -> u16 {
    9394
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PrometheusConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_prometheus_port")]
    pub port: u16,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_prometheus_port(),
        }
    }
}
