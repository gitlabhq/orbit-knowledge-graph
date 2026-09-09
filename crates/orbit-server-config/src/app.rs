//! Top-level application configuration.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::analytics::AnalyticsConfig;
use crate::billing::BillingConfig;
use crate::clickhouse::ClickHouseConfiguration;
use crate::engine::{EngineConfiguration, ScheduleConfig};
use crate::features::FeaturesConfig;
use crate::gitlab::{GitlabClientConfiguration, GitlabConfig};
use crate::grpc::GrpcConfig;
use crate::health_check::HealthCheckConfig;
use crate::metrics::MetricsConfig;
use crate::nats::NatsConfiguration;
use crate::query::{PathResolverConfig, QuerySettings};
use crate::schema::SchemaConfig;
use crate::secret_file_source::SecretFileSource;
use crate::tls::TlsConfig;

pub const SECRET_FILE_DIR: &str = "/etc/secrets";
pub const DEFAULT_CONFIG_FILE: &str = "config/default";
pub const OVERLAY_CONFIG_FILE: &str = "config/config";

/// `config/default.yaml`, compiled in as the lowest configuration layer.
pub const EMBEDDED_DEFAULTS: &str = include_str!(concat!(env!("CONFIG_DIR"), "/default.yaml"));

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AppConfig {
    pub bind_address: SocketAddr,
    pub grpc_bind_address: SocketAddr,
    pub jwt_clock_skew_secs: u64,
    pub health_check_url: Option<String>,
    pub nats: NatsConfiguration,
    pub datalake: ClickHouseConfiguration,
    pub graph: ClickHouseConfiguration,
    pub engine: EngineConfiguration,
    pub gitlab: GitlabConfig,
    pub schedule: ScheduleConfig,
    pub health_check: HealthCheckConfig,
    pub indexer_health_bind_address: SocketAddr,
    pub dispatcher_health_bind_address: SocketAddr,
    pub metrics: MetricsConfig,
    pub tls: TlsConfig,
    pub query: QuerySettings,
    pub path_resolver: PathResolverConfig,
    pub grpc: GrpcConfig,
    pub schema: SchemaConfig,
    pub analytics: AnalyticsConfig,
    pub billing: BillingConfig,
    pub features: FeaturesConfig,
}

impl AppConfig {
    /// Layers, lowest to highest priority: the embedded `config/default.yaml`,
    /// an on-disk `config/default.yaml` when present (the Helm chart's ConfigMap
    /// key), the overlays (each `--config <path>` in order, else
    /// `config/config.yaml` when present), secret files.
    pub fn load(overlays: &[PathBuf]) -> Result<Self, ConfigError> {
        Self::load_from(overlays, Path::new(SECRET_FILE_DIR))
    }

    /// The embedded `config/default.yaml` alone: no overlay or secrets. The
    /// fixture every test that needs a config starts from.
    pub fn embedded_defaults() -> Self {
        config::Config::builder()
            .add_source(embedded_defaults_source())
            .build()
            .and_then(config::Config::try_deserialize)
            .expect("embedded config/default.yaml must deserialize into AppConfig")
    }

    fn load_from(overlays: &[PathBuf], secret_dir: &Path) -> Result<Self, ConfigError> {
        let mut builder = config::Config::builder()
            .add_source(embedded_defaults_source())
            .add_source(config::File::with_name(DEFAULT_CONFIG_FILE).required(false));
        if overlays.is_empty() {
            builder =
                builder.add_source(config::File::with_name(OVERLAY_CONFIG_FILE).required(false));
        }
        for overlay in overlays {
            builder = builder.add_source(config::File::from(overlay.clone()).required(true));
        }
        let config = builder
            .add_source(SecretFileSource::new(secret_dir))
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

fn embedded_defaults_source() -> config::File<config::FileSourceString, config::FileFormat> {
    config::File::from_str(EMBEDDED_DEFAULTS, config::FileFormat::Yaml)
}

pub type SharedAppConfig = Arc<AppConfig>;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error(
        "gitlab.jwt.verifying_key is required (set it in a config overlay or mount it at /etc/secrets/gitlab/jwt/verifying_key)"
    )]
    MissingJwtSecret,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfiguration;

    const OVERLAY_BASE: &str = r#"
nats:
  url: "nats://overlay:4222"
datalake:
  database: "overlay-datalake"
graph:
  database: "overlay-graph"
  password: "overlay-password"
gitlab:
  jwt:
    verifying_key: "overlay-secret-at-least-32-bytes-long"
"#;

    fn builder_with_defaults() -> config::ConfigBuilder<config::builder::DefaultState> {
        config::Config::builder().add_source(embedded_defaults_source())
    }

    #[test]
    fn embedded_defaults_declare_every_setting() {
        let config = AppConfig::embedded_defaults();

        assert_eq!(config.nats.url, "localhost:4222");
        assert_eq!(config.engine.modules, crate::IndexerModule::all());
        assert!(config.gitlab.jwt.verifying_key.is_none());
        assert!(config.engine.max_concurrent_workers.is_none());
    }

    #[test]
    fn missing_scalar_in_defaults_is_an_error() {
        let yaml = EMBEDDED_DEFAULTS.replace("jwt_clock_skew_secs: 60\n", "");
        assert_ne!(yaml, EMBEDDED_DEFAULTS);

        let err = config::Config::builder()
            .add_source(config::File::from_str(&yaml, config::FileFormat::Yaml))
            .build()
            .and_then(config::Config::try_deserialize::<AppConfig>)
            .unwrap_err();

        assert!(err.to_string().contains("jwt_clock_skew_secs"), "{err}");
    }

    fn write_overlay(dir: &Path, name: &str, yaml: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, yaml).unwrap();
        path
    }

    #[test]
    fn explicit_overlay_file_overrides_defaults() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(dir.path(), "custom.yaml", OVERLAY_BASE);
        let secrets = dir.path().join("secrets");
        std::fs::create_dir(&secrets).unwrap();

        let config = AppConfig::load_from(&[overlay], &secrets).unwrap();

        assert_eq!(config.nats.url, "nats://overlay:4222");
        assert_eq!(config.graph.database, "overlay-graph");
        assert_eq!(config.datalake.database, "overlay-datalake");
        assert_eq!(config.datalake.url, "http://127.0.0.1:8123");
    }

    #[test]
    fn explicit_overlay_file_must_exist() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("missing.yaml");

        let err = AppConfig::load_from(&[missing], dir.path()).unwrap_err();

        assert!(matches!(err, ConfigError::Config(_)), "{err}");
    }

    #[test]
    fn later_overlays_override_earlier_ones() {
        let dir = tempfile::TempDir::new().unwrap();
        let base = write_overlay(dir.path(), "base.yaml", OVERLAY_BASE);
        let mode = write_overlay(
            dir.path(),
            "mode.yaml",
            "nats:\n  url: \"nats://mode:4222\"\n  consumer_name: \"mode-consumer\"\n",
        );
        let secrets = dir.path().join("secrets");
        std::fs::create_dir(&secrets).unwrap();

        let config = AppConfig::load_from(&[base, mode], &secrets).unwrap();

        assert_eq!(config.nats.url, "nats://mode:4222");
        assert_eq!(config.nats.consumer_name.as_deref(), Some("mode-consumer"));
        assert_eq!(config.graph.database, "overlay-graph");
    }

    #[test]
    fn overlay_reaches_topics_and_schedule() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(
            dir.path(),
            "tuning.yaml",
            r#"
engine:
  topics:
    code-indexing-task:
      max_attempts: 2
schedule:
  tasks:
    global:
      cron: "0 */2 * * * *"
"#,
        );
        let secrets = dir.path().join("secrets");
        std::fs::create_dir(&secrets).unwrap();

        let config = AppConfig::load_from(&[overlay], &secrets).unwrap();

        assert_eq!(
            config.engine.topics["code-indexing-task"].max_attempts,
            Some(2)
        );
        assert_eq!(
            config.schedule.tasks.global.schedule.cron.expression(),
            "0 */2 * * * *"
        );
    }

    #[test]
    fn secret_files_override_overlay_values() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(dir.path(), "custom.yaml", OVERLAY_BASE);
        let secrets = dir.path().join("secrets");
        std::fs::create_dir_all(secrets.join("graph")).unwrap();
        std::fs::write(secrets.join("graph/password"), "secret-password").unwrap();

        let config = AppConfig::load_from(&[overlay], &secrets).unwrap();

        assert_eq!(config.graph.password.as_deref(), Some("secret-password"));
    }

    #[test]
    fn engine_overlay_merges_into_embedded_defaults() {
        let yaml = r#"
engine:
  max_concurrent_workers: 16
  concurrency_groups:
    sdlc: 12
    code: 4
  topics:
    global-handler:
      concurrency_group: sdlc
      max_attempts: 1
      retry_interval_secs: 60
    code-indexing-task:
      concurrency_group: code
      max_attempts: 5
      retry_interval_secs: 60
      dead_letter_on_exhaustion: true
    namespace-deletion:
      concurrency_group: code
      max_attempts: 1
  handlers:
    code-indexing-task:
      pipeline:
        max_file_size_bytes: 10000000
        max_files: 200000
        worker_threads: 2
        max_concurrent_languages: 3
"#;

        let config: AppConfig = builder_with_defaults()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .expect("engine overlay should deserialize");
        let engine: EngineConfiguration = config.engine;

        assert_eq!(
            engine.topics["global-handler"].concurrency_group.as_deref(),
            Some("sdlc"),
        );
        assert_eq!(engine.topics["code-indexing-task"].max_attempts, Some(5));
        assert_eq!(
            engine.topics["code-indexing-task"].dead_letter_on_exhaustion,
            Some(true)
        );
        assert_eq!(engine.topics["namespace-deletion"].max_attempts, Some(1));
        let pipeline = &engine.handlers.code_indexing_task.pipeline;
        assert_eq!(pipeline.max_file_size_bytes, 10_000_000);
        assert_eq!(pipeline.max_files, 200_000);
        assert_eq!(pipeline.worker_threads, 2);
        assert_eq!(pipeline.max_concurrent_languages, 3);
        assert_eq!(pipeline.per_file_timeout_ms, 2000);
    }
}
