use std::net::SocketAddr;

use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::configuration::EngineConfiguration;
use etl_engine::nats::NatsConfiguration;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub bind_address: SocketAddr,
    pub grpc_bind_address: SocketAddr,
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
            .unwrap_or_else(|_| "127.0.0.1:4200".into())
            .parse()
            .map_err(|_| ConfigError::InvalidBindAddress)?;

        let grpc_bind_address = std::env::var("GKG_GRPC_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:50051".into())
            .parse()
            .map_err(|_| ConfigError::InvalidGrpcBindAddress)?;

        let jwt_secret = std::env::var("GKG_JWT_SECRET").ok();

        let jwt_clock_skew_secs = std::env::var("GKG_JWT_CLOCK_SKEW_SECS")
            .unwrap_or_else(|_| "60".into())
            .parse()
            .unwrap_or(60);

        Ok(Self {
            bind_address,
            grpc_bind_address,
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
    #[error("invalid HTTP bind address")]
    InvalidBindAddress,
    #[error("invalid gRPC bind address")]
    InvalidGrpcBindAddress,
    #[error("GKG_JWT_SECRET environment variable is required")]
    MissingJwtSecret,
}
