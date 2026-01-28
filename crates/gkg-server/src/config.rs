use std::net::SocketAddr;
use std::path::PathBuf;

use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::configuration::EngineConfiguration;
use etl_engine::nats::NatsConfiguration;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub bind_address: SocketAddr,
    pub jwt_secret: String,
    pub jwt_clock_skew_secs: u64,
    pub ontology_path: PathBuf,
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

        let jwt_secret =
            std::env::var("GKG_JWT_SECRET").map_err(|_| ConfigError::MissingJwtSecret)?;

        if jwt_secret.len() < 32 {
            return Err(ConfigError::JwtSecretTooShort);
        }

        let jwt_clock_skew_secs = std::env::var("GKG_JWT_CLOCK_SKEW_SECS")
            .unwrap_or_else(|_| "60".into())
            .parse()
            .unwrap_or(60);

        let ontology_path = std::env::var("GKG_ONTOLOGY_PATH")
            .map(PathBuf::from)
            .map_err(|_| ConfigError::MissingOntologyPath)?;

        Ok(Self {
            bind_address,
            jwt_secret,
            jwt_clock_skew_secs,
            ontology_path,
            nats: NatsConfiguration::from_env(),
            clickhouse: ClickHouseConfiguration::from_env(),
            engine: EngineConfiguration::default(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid bind address")]
    InvalidBindAddress,
    #[error("GKG_JWT_SECRET environment variable is required")]
    MissingJwtSecret,
    #[error("JWT secret must be at least 32 bytes")]
    JwtSecretTooShort,
    #[error("GKG_ONTOLOGY_PATH environment variable is required")]
    MissingOntologyPath,
}
