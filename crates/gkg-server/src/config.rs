use std::net::SocketAddr;
use std::sync::Arc;

use health_check::HealthCheckConfig;
use indexer::clickhouse::ClickHouseConfiguration;
use indexer::configuration::EngineConfiguration;
use indexer::modules::code::GitalyConfiguration;
use indexer::modules::code::config::CodeIndexingConfig;
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
    pub code_indexing: CodeIndexingConfig,
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
                    .separator("__")
                    .list_separator(","),
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
