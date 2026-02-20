use std::net::SocketAddr;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(
        default = "default_services",
        deserialize_with = "deserialize_services"
    )]
    pub services: Vec<String>,
}

/// Accepts either a sequence or a comma-separated string.
/// The `config` crate passes env vars as strings, not sequences, when
/// `try_parsing` is disabled.
fn deserialize_services<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{SeqAccess, Visitor};
    use std::fmt;

    struct ServicesVisitor;

    impl<'de> Visitor<'de> for ServicesVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a sequence or comma-separated string")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Vec<String>, E> {
            Ok(value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect())
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<String>, A::Error> {
            let mut services = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                services.push(s);
            }
            Ok(services)
        }
    }

    deserializer.deserialize_any(ServicesVisitor)
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

fn env_var_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
