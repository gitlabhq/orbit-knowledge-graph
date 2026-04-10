//! Top-level application configuration.

use std::net::SocketAddr;
use std::sync::Arc;

use schemars::JsonSchema;
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

fn default_dispatcher_health_bind_address() -> SocketAddr {
    "0.0.0.0:4203".parse().unwrap()
}

fn default_jwt_clock_skew_secs() -> u64 {
    60
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
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
    #[serde(default = "default_dispatcher_health_bind_address")]
    pub dispatcher_health_bind_address: SocketAddr,
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
                    .separator("__")
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
    #[error(
        "gitlab.jwt.verifying_key is required (set GKG_GITLAB__JWT__VERIFYING_KEY, add to config/default.yaml, or mount at /etc/secrets/gitlab/jwt/verifying_key)"
    )]
    MissingJwtSecret,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfiguration;

    /// Verifies the kebab-case handler config keys in YAML actually
    /// deserialize into the correct Rust struct fields.
    #[test]
    fn handler_configs_deserialize_from_kebab_case_yaml() {
        let yaml = r#"
max_concurrent_workers: 16
concurrency_groups:
  sdlc: 12
  code: 4
handlers:
  global-handler:
    concurrency_group: sdlc
    max_attempts: 1
    retry_interval_secs: 60
  namespace-handler:
    concurrency_group: sdlc
    max_attempts: 1
    retry_interval_secs: 60
  code-indexing-task:
    concurrency_group: code
    max_attempts: 5
    retry_interval_secs: 60
  namespace-deletion:
    concurrency_group: code
    max_attempts: 1
"#;

        let engine: EngineConfiguration =
            serde_yaml::from_str(yaml).expect("engine config should deserialize");

        assert_eq!(
            engine
                .handlers
                .global_handler
                .engine
                .concurrency_group
                .as_deref(),
            Some("sdlc"),
        );
        assert_eq!(
            engine
                .handlers
                .namespace_handler
                .engine
                .concurrency_group
                .as_deref(),
            Some("sdlc"),
        );
        assert_eq!(
            engine
                .handlers
                .code_indexing_task
                .engine
                .concurrency_group
                .as_deref(),
            Some("code"),
        );
        assert_eq!(
            engine.handlers.code_indexing_task.engine.max_attempts,
            Some(5)
        );
        assert_eq!(
            engine
                .handlers
                .namespace_deletion
                .engine
                .concurrency_group
                .as_deref(),
            Some("code"),
        );
        assert_eq!(
            engine.handlers.namespace_deletion.engine.max_attempts,
            Some(1)
        );
    }

    /// Environment source with `GKG_` prefix and `__` separator maps env
    /// vars to nested config keys:
    ///   GKG_NATS__URL -> nats.url
    ///   GKG_GRAPH__DATABASE -> graph.database
    #[test]
    fn environment_source_overrides_file_values() {
        // Build a config that simulates what env vars would produce by
        // testing the Environment source directly against a known set of
        // overrides. We use Config::builder with manual set() calls to
        // mirror the env var effect without mutating process state.
        let dir = tempfile::TempDir::new().unwrap();

        let config = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(SecretFileSource::new(dir.path()))
            // Provide required base config (normally from config/default.yaml)
            .set_default("nats.url", "localhost:4222")
            .unwrap()
            .set_default("datalake.url", "http://127.0.0.1:8123")
            .unwrap()
            .set_default("datalake.database", "default")
            .unwrap()
            .set_default("datalake.username", "default")
            .unwrap()
            .set_default("graph.url", "http://127.0.0.1:8123")
            .unwrap()
            .set_default("graph.database", "default")
            .unwrap()
            .set_default("graph.username", "default")
            .unwrap()
            // Simulate what GKG_NATS__URL, GKG_GRAPH__DATABASE, etc. would
            // produce via config::Environment
            .set_override("nats.url", "nats://custom:4222")
            .unwrap()
            .set_override("graph.database", "test-graph-db")
            .unwrap()
            .set_override("datalake.database", "test-datalake-db")
            .unwrap()
            .set_override(
                "gitlab.jwt.verifying_key",
                "env-secret-at-least-32-bytes-long",
            )
            .unwrap()
            .build()
            .unwrap();

        let config: AppConfig = config.try_deserialize().expect("config should deserialize");

        assert_eq!(config.nats.url, "nats://custom:4222");
        assert_eq!(config.graph.database, "test-graph-db");
        assert_eq!(config.datalake.database, "test-datalake-db");
        assert_eq!(
            config.gitlab.jwt.verifying_key.as_deref(),
            Some("env-secret-at-least-32-bytes-long")
        );
    }
}
