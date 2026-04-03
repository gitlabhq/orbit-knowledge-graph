//! Top-level application configuration.

use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::clickhouse::ClickHouseConfiguration;
use crate::engine::{EngineConfiguration, ScheduleConfig};
use crate::gitlab::{GitlabClientConfiguration, GitlabConfig};
use crate::grpc::GrpcConfig;
use crate::health_check::HealthCheckConfig;
use crate::metrics::MetricsConfig;
use crate::nats::NatsConfiguration;
use crate::query::QuerySettings;
use crate::secret_file_source::SecretFileSource;
use crate::tls::TlsConfig;

pub const SECRET_FILE_DIR: &str = "/etc/secrets";

fn default_bind_address() -> SocketAddr {
    "127.0.0.1:4200".parse().unwrap()
}

fn default_grpc_bind_address() -> SocketAddr {
    "127.0.0.1:50054".parse().unwrap()
}

fn default_indexer_health_bind_address() -> SocketAddr {
    "0.0.0.0:4202".parse().unwrap()
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
    pub gitlab: GitlabConfig,
    #[serde(default)]
    pub schedule: ScheduleConfig,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    #[serde(default = "default_indexer_health_bind_address")]
    pub indexer_health_bind_address: SocketAddr,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub query: QuerySettings,
    #[serde(default)]
    pub grpc: GrpcConfig,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_with_secret_dir(SECRET_FILE_DIR)
    }

    fn load_with_secret_dir(secret_dir: &str) -> Result<Self, ConfigError> {
        let config = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(SecretFileSource::new(secret_dir))
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
        self.gitlab
            .jwt
            .verifying_key
            .as_deref()
            .ok_or(ConfigError::MissingJwtSecret)
    }

    pub fn gitlab_client_config(&self) -> Option<GitlabClientConfiguration> {
        self.gitlab.client_config()
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
    #[error("GKG_GITLAB__JWT__VERIFYING_KEY is required")]
    MissingJwtSecret,
}
