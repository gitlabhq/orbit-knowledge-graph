use std::net::SocketAddr;
use std::sync::Arc;

use health_check::HealthCheckConfig;
use indexer::ModulesConfig;
use indexer::clickhouse::ClickHouseConfiguration;
use indexer::configuration::EngineConfiguration;
use indexer::modules::code::GitalyConfiguration;
use indexer::nats::NatsConfiguration;
use labkit_rs::metrics::MetricsConfig;
use serde::{Deserialize, Serialize};

fn default_bind_address() -> SocketAddr {
    "127.0.0.1:4200".parse().unwrap()
}

fn default_grpc_bind_address() -> SocketAddr {
    "127.0.0.1:50051".parse().unwrap()
}

fn default_jwt_clock_skew_secs() -> u64 {
    60
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    #[serde(default = "default_grpc_bind_address")]
    pub grpc_bind_address: SocketAddr,
    #[serde(default)]
    pub jwt_secret: Option<String>,
    #[serde(default = "default_jwt_clock_skew_secs")]
    pub jwt_clock_skew_secs: u64,
    #[serde(default)]
    pub health_check_url: Option<String>,
    #[serde(default)]
    pub nats: NatsConfiguration,
    #[serde(default)]
    pub datalake: ClickHouseConfiguration,
    #[serde(default)]
    pub graph: ClickHouseConfiguration,
    #[serde(default)]
    pub engine: EngineConfiguration,
    #[serde(default)]
    pub gitaly: Option<GitalyConfiguration>,
    #[serde(default)]
    pub modules: ModulesConfig,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(
                config::Environment::with_prefix("GKG")
                    .prefix_separator("_")
                    .separator("__")
                    .list_separator(",")
                    .with_list_parse_key("health_check.services")
                    .try_parsing(true),
            )
            .build()
            .map_err(ConfigError::Config)?;

        config.try_deserialize().map_err(ConfigError::Config)
    }

    pub fn jwt_secret(&self) -> Result<&str, ConfigError> {
        self.jwt_secret
            .as_deref()
            .ok_or(ConfigError::MissingJwtSecret)
    }

    pub fn into_shared(self) -> SharedAppConfig {
        Arc::new(self)
    }
}

pub type SharedAppConfig = Arc<AppConfig>;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("GKG_JWT_SECRET is required")]
    MissingJwtSecret,
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;
    use serial_test::serial;

    /// Reproduces the crash seen when deploying with GKG_ prefixed env vars.
    /// `try_parsing(true)` + `list_separator(",")` without `with_list_parse_key`
    /// wraps every value in a sequence, causing "invalid type: sequence, expected
    /// a string" errors for plain string fields.
    #[test]
    #[serial]
    fn env_string_fields_not_parsed_as_lists() {
        let vars = [
            ("GKG_NATS__URL", "gkg-nats:4222"),
            ("GKG_GRAPH__URL", "http://clickhouse:8123"),
            ("GKG_GRAPH__DATABASE", "gkg"),
            ("GKG_GRAPH__USERNAME", "default"),
            ("GKG_GRAPH__PASSWORD", "supersecret"),
            ("GKG_METRICS__OTLP_ENDPOINT", "http://gkg-obs-alloy:4317"),
        ];

        // SAFETY: tests run serially via #[serial], no concurrent env access
        unsafe {
            for (k, v) in &vars {
                std::env::set_var(k, v);
            }
        }

        let result = AppConfig::load();

        unsafe {
            for (k, _) in &vars {
                std::env::remove_var(k);
            }
        }

        let config = result.expect("AppConfig::load should not fail for plain string env vars");
        assert_eq!(config.nats.url, "gkg-nats:4222");
        assert_eq!(config.graph.password.as_deref(), Some("supersecret"));
        assert_eq!(config.metrics.otlp_endpoint, "http://gkg-obs-alloy:4317");
    }

    #[test]
    #[serial]
    fn health_check_services_parsed_as_list() {
        let vars = [(
            "GKG_HEALTH_CHECK__SERVICES",
            "siphon-consumer,nats,gkg-indexer",
        )];

        unsafe {
            for (k, v) in &vars {
                std::env::set_var(k, v);
            }
        }

        let result = AppConfig::load();

        unsafe {
            for (k, _) in &vars {
                std::env::remove_var(k);
            }
        }

        let config = result.expect("AppConfig::load should parse health_check.services as a list");
        assert_eq!(
            config.health_check.services,
            vec!["siphon-consumer", "nats", "gkg-indexer"],
        );
    }
}
