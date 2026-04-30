use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_failure_threshold() -> u32 {
    5
}

fn default_window_secs() -> u64 {
    30
}

fn default_cooldown_secs() -> u64 {
    60
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ServiceCircuitBreakerConfig {
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,

    #[serde(default = "default_window_secs")]
    pub window_secs: u64,

    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
}

impl Default for ServiceCircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            window_secs: default_window_secs(),
            cooldown_secs: default_cooldown_secs(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    #[serde(default)]
    pub clickhouse_datalake: ServiceCircuitBreakerConfig,

    #[serde(default)]
    pub clickhouse_graph: ServiceCircuitBreakerConfig,

    #[serde(default)]
    pub nats: ServiceCircuitBreakerConfig,

    #[serde(default)]
    pub rails: ServiceCircuitBreakerConfig,
}
