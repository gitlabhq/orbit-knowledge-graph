use std::net::SocketAddr;

use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::configuration::EngineConfiguration;
use etl_engine::nats::NatsConfiguration;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    /// HTTP bind address for health checks and metrics
    pub bind_address: SocketAddr,
    /// gRPC bind address for Knowledge Graph service
    pub grpc_bind_address: SocketAddr,
    pub jwt_secret: String,
    pub jwt_clock_skew_secs: u64,
    #[serde(default)]
    pub nats: NatsConfiguration,
    #[serde(default)]
    pub clickhouse: ClickHouseConfiguration,
    #[serde(default)]
    pub engine: EngineConfiguration,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind_address = std::env::var("GKG_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8080".into())
            .parse()
            .map_err(|_| ConfigError::InvalidBindAddress)?;

        let grpc_bind_address = std::env::var("GKG_GRPC_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:50051".into())
            .parse()
            .map_err(|_| ConfigError::InvalidGrpcBindAddress)?;

        let jwt_secret =
            std::env::var("GKG_JWT_SECRET").map_err(|_| ConfigError::MissingJwtSecret)?;

        if jwt_secret.len() < 32 {
            return Err(ConfigError::JwtSecretTooShort);
        }

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
            clickhouse: ClickHouseConfiguration::from_env(),
            engine: EngineConfiguration::default(),
        })
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
    #[error("JWT secret must be at least 32 bytes")]
    JwtSecretTooShort,
}
