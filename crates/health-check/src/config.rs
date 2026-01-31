use std::net::SocketAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    pub bind_address: SocketAddr,
    pub namespace: String,
    pub services: Vec<String>,
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

fn env_var_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
