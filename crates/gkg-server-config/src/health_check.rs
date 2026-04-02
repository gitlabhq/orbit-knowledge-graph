//! Health check configuration.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::env::env_var_or;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default = "default_services")]
    pub services: Vec<String>,
}

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
    ]
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([0, 0, 0, 0], 4201)),
            namespace: "default".to_string(),
            services: vec![
                "siphon-consumer".to_string(),
                "siphon-producer".to_string(),
                "gkg-indexer".to_string(),
                "nats".to_string(),
            ],
        }
    }
}

impl HealthCheckConfig {
    pub fn from_env() -> Self {
        let bind_address = env_var_or(
            "GKG_HEALTH_CHECK_BIND_ADDRESS",
            SocketAddr::from(([0, 0, 0, 0], 4201)),
        );

        let namespace =
            std::env::var("GKG_HEALTH_CHECK_NAMESPACE").unwrap_or_else(|_| "default".to_string());

        let services = std::env::var("GKG_HEALTH_CHECK_SERVICES")
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_else(|_| {
                vec![
                    "siphon-consumer".to_string(),
                    "siphon-producer".to_string(),
                    "gkg-indexer".to_string(),
                    "nats".to_string(),
                ]
            });

        Self {
            bind_address,
            namespace,
            services,
        }
    }
}
