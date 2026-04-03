//! Health check configuration.

use std::net::SocketAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_bind_address() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 4201))
}

fn default_namespace() -> String {
    "default".to_string()
}

fn default_services() -> Vec<String> {
    vec![
        "siphon-consumer".to_string(),
        "siphon-producer".to_string(),
        "gkg-indexer".to_string(),
        "nats".to_string(),
    ]
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HealthCheckConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default = "default_services")]
    pub services: Vec<String>,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            namespace: default_namespace(),
            services: default_services(),
        }
    }
}
