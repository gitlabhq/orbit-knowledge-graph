use std::net::SocketAddr;

use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::configuration::EngineConfiguration;
use etl_engine::nats::NatsConfiguration;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub bind_address: SocketAddr,
    pub jwt_secret: Option<String>,
    pub jwt_clock_skew_secs: u64,
    #[serde(default)]
    pub nats: NatsConfiguration,
    #[serde(default)]
    pub datalake: ClickHouseConfiguration,
    #[serde(default)]
    pub graph: ClickHouseConfiguration,
    #[serde(default)]
    pub engine: EngineConfiguration,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_address = std::env::var("GKG_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8080".into())
            .parse()
            .map_err(|_| ConfigError::InvalidBindAddress)?;

        let jwt_secret = std::env::var("GKG_JWT_SECRET").ok();

        let jwt_clock_skew_secs = std::env::var("GKG_JWT_CLOCK_SKEW_SECS")
            .unwrap_or_else(|_| "60".into())
            .parse()
            .unwrap_or(60);

        Ok(Self {
            bind_address,
            jwt_secret,
            jwt_clock_skew_secs,
            nats: NatsConfiguration::from_env(),
            datalake: ClickHouseConfiguration::from_env_with_prefix("DATALAKE"),
            graph: ClickHouseConfiguration::from_env_with_prefix("GRAPH"),
            engine: EngineConfiguration::default(),
        })
    }

    pub fn jwt_secret(&self) -> Result<&str, ConfigError> {
        self.jwt_secret
            .as_deref()
            .ok_or(ConfigError::MissingJwtSecret)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid bind address")]
    InvalidBindAddress,
    #[error("GKG_JWT_SECRET environment variable is required")]
    MissingJwtSecret,
}
