use std::net::SocketAddr;
use std::sync::Arc;

use gitlab_client::GitlabClientConfiguration;
use health_check::HealthCheckConfig;
use indexer::clickhouse::ClickHouseConfiguration;
use indexer::configuration::EngineConfiguration;
use indexer::dispatcher::DispatchConfig;
use indexer::nats::NatsConfiguration;
use labkit_rs::metrics::MetricsConfig;
use serde::{Deserialize, Serialize};

use crate::constants::SECRET_FILE_DIR;
use crate::secret_file_source::SecretFileSource;

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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JwtConfig {
    #[serde(default)]
    pub signing_key: Option<String>,
    #[serde(default)]
    pub verifying_key: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GitlabConfig {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub jwt: JwtConfig,
}

impl GitlabConfig {
    pub fn client_config(&self) -> Option<GitlabClientConfiguration> {
        let base_url = self.base_url.clone()?;
        let signing_key = self.jwt.signing_key.clone()?;
        Some(GitlabClientConfiguration {
            base_url,
            signing_key,
        })
    }
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
    pub dispatch: DispatchConfig,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    #[serde(default = "default_indexer_health_bind_address")]
    pub indexer_health_bind_address: SocketAddr,
    #[serde(default)]
    pub metrics: MetricsConfig,
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

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;

    /// Reproduces the crash seen when deploying with GKG_ prefixed env vars.
    /// `try_parsing(true)` + `list_separator(",")` without `with_list_parse_key`
    /// wraps every value in a sequence, causing "invalid type: sequence, expected
    /// a string" errors for plain string fields.
    #[test]
    fn env_string_fields_not_parsed_as_lists() {
        let vars = [
            ("GKG_NATS__URL", "gkg-nats:4222"),
            ("GKG_GRAPH__URL", "http://clickhouse:8123"),
            ("GKG_GRAPH__DATABASE", "gkg"),
            ("GKG_GRAPH__USERNAME", "default"),
            ("GKG_GRAPH__PASSWORD", "supersecret"),
            ("GKG_METRICS__OTLP_ENDPOINT", "http://gkg-obs-alloy:4317"),
        ];

        // SAFETY: nextest runs each test in its own process, so env mutations are isolated
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
