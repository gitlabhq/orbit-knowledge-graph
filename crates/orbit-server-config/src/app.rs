//! Top-level application configuration.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use labkit_config::{Format, Loader};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::analytics::AnalyticsConfig;
use crate::billing::BillingConfig;
use crate::clickhouse::ClickHouseConfiguration;
use crate::clickhouse_setup::ClickHouseSetupConfig;
use crate::engine::{EngineConfiguration, ScheduleConfig};
use crate::features::FeaturesConfig;
use crate::gitlab::{GitlabClientConfiguration, GitlabConfig};
use crate::grpc::GrpcConfig;
use crate::health_check::HealthCheckConfig;
use crate::merge::merge;
use crate::metrics::MetricsConfig;
use crate::nats::NatsConfiguration;
use crate::object_storage::ObjectStorageConfig;
use crate::probe_server::{ProbeServerConfig, bind_address_for_port, default_bind_address};
use crate::proto;
use crate::query::QuerySettings;
use crate::schema::SchemaConfig;
use crate::secrets;
use crate::tls::TlsConfig;
use crate::undeclared::undeclared;

pub const SECRET_FILE_DIR: &str = "/etc/secrets";
pub const OVERLAY_CONFIG_FILE: &str = "config/config.yaml";

/// `config/default.yaml`, compiled in as the lowest configuration layer.
pub const EMBEDDED_DEFAULTS: &str =
    include_str!(concat!(env!("ORBIT_CONFIG_DIR"), "/default.yaml"));

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AppConfig {
    pub grpc_bind_address: SocketAddr,
    pub jwt_clock_skew_secs: u64,
    pub health_check_url: Option<String>,
    pub nats: NatsConfiguration,
    pub datalake: ClickHouseConfiguration,
    pub graph: ClickHouseConfiguration,
    pub clickhouse_setup: ClickHouseSetupConfig,
    pub engine: EngineConfiguration,
    pub gitlab: GitlabConfig,
    pub schedule: ScheduleConfig,
    pub health_check: HealthCheckConfig,
    pub metrics: MetricsConfig,
    pub probe_server: ProbeServerConfig,
    pub tls: TlsConfig,
    pub query: QuerySettings,
    pub grpc: GrpcConfig,
    pub schema: SchemaConfig,
    pub analytics: AnalyticsConfig,
    pub billing: BillingConfig,
    pub features: FeaturesConfig,
    pub object_storage: ObjectStorageConfig,
}

impl AppConfig {
    /// Layers, lowest to highest priority: the embedded `config/default.yaml`,
    /// the overlay (`--config <path>`, else `config/config.yaml` when present),
    /// secret files.
    pub fn load(overlay: Option<&Path>) -> Result<Self, ConfigError> {
        Self::load_from(overlay, Path::new(SECRET_FILE_DIR))
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

    pub fn load_from(overlay: Option<&Path>, secret_dir: &Path) -> Result<Self, ConfigError> {
        let mut document = parse_yaml(
            EMBEDDED_DEFAULTS.as_bytes(),
            Path::new("config/default.yaml"),
        )?;

        if let Some(path) = overlay_path(overlay) {
            let data = std::fs::read(&path).map_err(|source| ConfigError::ReadOverlay {
                path: path.clone(),
                source,
            })?;
            merge(&mut document, parse_yaml(&data, &path)?);
        }
        merge(&mut document, secrets::tree(secret_dir)?);
        Self::decode(document)
    }

    fn decode(document: serde_json::Value) -> Result<Self, ConfigError> {
        let errors = undeclared(&document);
        if !errors.is_empty() {
            return Err(ConfigError::Undeclared(errors));
        }

        let json = serde_json::to_vec(&document).expect("a parsed document re-serialises");
        let loaded = Loader::new()
            .with_descriptors(proto::FILE_DESCRIPTOR_SET)?
            .load_bytes::<proto::Config>(&json, Format::Json)?;

        Ok(Self::try_from(loaded)?)
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

    /// The listener that serves `/-/liveness`, `/-/readiness` and, when
    /// `metrics.prometheus.enabled`, `/-/metrics`.
    pub fn probe_server_bind_address(&self) -> Result<SocketAddr, ConfigError> {
        match (self.probe_server.bind_address, self.metrics.prometheus.port) {
            (Some(address), Some(port)) if address.port() != port => {
                Err(ConfigError::ProbeServerAddressConflict {
                    bind_address: address,
                    port,
                })
            }
            (Some(address), _) => Ok(address),
            (None, Some(port)) => Ok(bind_address_for_port(port)),
            (None, None) => Ok(default_bind_address()),
        }
    }

    pub fn into_shared(self) -> SharedAppConfig {
        Arc::new(self)
    }
}

fn embedded_defaults_source() -> config::File<config::FileSourceString, config::FileFormat> {
    config::File::from_str(EMBEDDED_DEFAULTS, config::FileFormat::Yaml)
}

/// An explicit `--config` path must exist; the conventional overlay is used
/// only when it is there.
fn overlay_path(overlay: Option<&Path>) -> Option<PathBuf> {
    match overlay {
        Some(path) => Some(path.to_path_buf()),
        None => Some(PathBuf::from(OVERLAY_CONFIG_FILE)).filter(|path| path.is_file()),
    }
}

fn parse_yaml(data: &[u8], path: &Path) -> Result<serde_json::Value, ConfigError> {
    let document: serde_json::Value = serde_saphyr::from_slice_with_options(
        data,
        serde_saphyr::options! { strict_booleans: true },
    )
    .map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source: Box::new(source),
    })?;
    // An empty or comment-only file parses to null, which would replace every layer beneath it.
    Ok(match document {
        serde_json::Value::Null => serde_json::Value::Object(serde_json::Map::new()),
        document => document,
    })
}

pub type SharedAppConfig = Arc<AppConfig>;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("failed to read the config overlay {}: {source}", path.display())]
    ReadOverlay {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{} is not valid YAML: {source}", path.display())]
    Parse {
        path: PathBuf,
        #[source]
        source: Box<serde_saphyr::Error>,
    },
    #[error("config: not declared by the schema:\n  - {}", .0.join("\n  - "))]
    Undeclared(Vec<String>),
    #[error(transparent)]
    Secrets(#[from] crate::secrets::SecretError),
    #[error(transparent)]
    Load(#[from] labkit_config::Error),
    #[error(transparent)]
    Convert(#[from] crate::convert::ConvertError),
    #[error(
        "gitlab.jwt.verifying_key is required (set it in a config overlay or mount it at /etc/secrets/gitlab/jwt/verifying_key)"
    )]
    MissingJwtSecret,
    #[error(
        "probe_server.bind_address is {bind_address} but the deprecated metrics.prometheus.port is {port}; set probe_server.bind_address alone"
    )]
    ProbeServerAddressConflict { bind_address: SocketAddr, port: u16 },
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
    fn a_pool_insert_settings_overlay_keeps_the_other_defaults() {
        let yaml = r#"
graph:
  insert_settings:
    wait_for_async_insert: "0"
"#;
        let config: AppConfig = builder_with_defaults()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        let mut expected = AppConfig::embedded_defaults().graph.insert_settings;
        expected.insert("wait_for_async_insert".to_string(), "0".to_string());
        assert_eq!(config.graph.insert_settings, expected);
    }

    fn address(value: &str) -> SocketAddr {
        value.parse().unwrap()
    }

    #[test]
    fn probe_server_address_defaults_when_no_key_is_set() {
        let config = AppConfig::embedded_defaults();

        assert!(config.probe_server.bind_address.is_none());
        assert!(config.metrics.prometheus.port.is_none());
        assert_eq!(
            config.probe_server_bind_address().unwrap(),
            address("0.0.0.0:9394")
        );
    }

    #[test]
    fn probe_server_address_falls_back_to_the_deprecated_port() {
        let mut config = AppConfig::embedded_defaults();
        config.metrics.prometheus.port = Some(9200);

        assert_eq!(
            config.probe_server_bind_address().unwrap(),
            address("0.0.0.0:9200")
        );
    }

    #[test]
    fn probe_server_address_prefers_the_explicit_bind_address() {
        let mut config = AppConfig::embedded_defaults();
        config.probe_server.bind_address = Some(address("127.0.0.1:9100"));

        assert_eq!(
            config.probe_server_bind_address().unwrap(),
            address("127.0.0.1:9100")
        );
    }

    #[test]
    fn probe_server_address_accepts_keys_that_agree() {
        let mut config = AppConfig::embedded_defaults();
        config.probe_server.bind_address = Some(address("0.0.0.0:9200"));
        config.metrics.prometheus.port = Some(9200);

        assert_eq!(
            config.probe_server_bind_address().unwrap(),
            address("0.0.0.0:9200")
        );
    }

    #[test]
    fn probe_server_address_accepts_a_loopback_address_on_the_deprecated_port() {
        let mut config = AppConfig::embedded_defaults();
        config.probe_server.bind_address = Some(address("127.0.0.1:9200"));
        config.metrics.prometheus.port = Some(9200);

        assert_eq!(
            config.probe_server_bind_address().unwrap(),
            address("127.0.0.1:9200")
        );
    }

    #[test]
    fn probe_server_address_rejects_keys_that_disagree() {
        let mut config = AppConfig::embedded_defaults();
        config.probe_server.bind_address = Some(address("0.0.0.0:9100"));
        config.metrics.prometheus.port = Some(9200);

        let err = config.probe_server_bind_address().unwrap_err();

        assert!(
            matches!(err, ConfigError::ProbeServerAddressConflict { .. }),
            "{err}"
        );
    }

    #[test]
    fn probe_server_address_is_read_from_an_overlay() {
        let dir = tempfile::TempDir::new().unwrap();
        let yaml = format!("{OVERLAY_BASE}probe_server:\n  bind_address: \"127.0.0.1:9500\"\n");
        let overlay = write_overlay(dir.path(), "probe.yaml", &yaml);
        let secrets = dir.path().join("secrets");
        std::fs::create_dir(&secrets).unwrap();

        let config = AppConfig::load_from(Some(&overlay), &secrets).unwrap();

        assert_eq!(
            config.probe_server_bind_address().unwrap(),
            address("127.0.0.1:9500")
        );
    }

    #[test]
    fn missing_scalar_in_defaults_is_an_error() {
        let yaml = EMBEDDED_DEFAULTS.replace("jwt_clock_skew_secs: 60\n", "");
        assert_ne!(yaml, EMBEDDED_DEFAULTS);

        let document = parse_yaml(yaml.as_bytes(), Path::new("config/default.yaml")).unwrap();
        let err = AppConfig::decode(document).unwrap_err();

        assert!(
            err.to_string()
                .contains("- jwt_clock_skew_secs: value is required"),
            "{err}"
        );
    }

    fn write_overlay(dir: &Path, name: &str, yaml: &str) -> std::path::PathBuf {
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

        let config = AppConfig::load_from(Some(&overlay), &secrets).unwrap();

        assert_eq!(config.nats.url, "nats://overlay:4222");
        assert_eq!(config.graph.database, "overlay-graph");
        assert_eq!(config.datalake.database, "overlay-datalake");
        assert_eq!(config.datalake.url, "http://127.0.0.1:8123");
    }

    #[test]
    fn the_committed_dev_overlay_loads() {
        let overlay = Path::new(env!("ORBIT_CONFIG_DIR")).join("dev.yaml");
        let secrets = tempfile::TempDir::new().unwrap();

        let config = AppConfig::load_from(Some(&overlay), secrets.path()).unwrap();

        assert_eq!(config.query.overrides.len(), 1);
        assert!(!config.query.default.use_query_cache.unwrap());
    }

    #[test]
    fn a_misspelled_feature_scope_key_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(
            dir.path(),
            "features.yaml",
            "features:\n  code_graph_incremental:\n    enabeld: true\n",
        );

        let error = AppConfig::load_from(Some(&overlay), dir.path())
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("- features.code_graph_incremental.enabeld: unknown field"),
            "{error}"
        );
    }

    #[test]
    fn an_overlay_with_nothing_in_it_changes_nothing() {
        let dir = tempfile::TempDir::new().unwrap();
        let defaults = serde_json::to_value(AppConfig::embedded_defaults()).unwrap();

        for (name, yaml) in [
            ("empty.yaml", ""),
            ("comments.yaml", "# nothing set here\n"),
        ] {
            let overlay = write_overlay(dir.path(), name, yaml);
            let config = AppConfig::load_from(Some(&overlay), dir.path()).unwrap();
            assert_eq!(serde_json::to_value(config).unwrap(), defaults, "{name}");
        }
    }

    #[test]
    fn every_missing_section_is_reported_at_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(
            dir.path(),
            "nulled.yaml",
            "grpc: null\ntls:\n  internal: null\nengine:\n  handlers:\n    entity-handler: null\n",
        );

        let error = AppConfig::load_from(Some(&overlay), dir.path())
            .unwrap_err()
            .to_string();

        assert!(error.contains("- grpc: value is required"), "{error}");
        assert!(
            error.contains("- tls.internal: value is required"),
            "{error}"
        );
        assert!(
            error.contains("- engine.handlers.entity_handler: value is required"),
            "{error}"
        );
    }

    #[test]
    fn a_null_value_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(
            dir.path(),
            "nulled.yaml",
            "jwt_clock_skew_secs: null\nnats:\n  connection_timeout_secs: null\n",
        );

        let error = AppConfig::load_from(Some(&overlay), dir.path())
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("- jwt_clock_skew_secs: value is required"),
            "{error}"
        );
        assert!(
            error.contains("- nats.connection_timeout_secs: value is required"),
            "{error}"
        );
    }

    #[test]
    fn an_unknown_feature_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(
            dir.path(),
            "features.yaml",
            "features:\n  not_a_real_flag:\n    enabled: true\n",
        );

        let error = AppConfig::load_from(Some(&overlay), dir.path())
            .unwrap_err()
            .to_string();

        assert!(error.contains("- features[\"not_a_real_flag\"]"), "{error}");
    }

    #[test]
    fn an_undeclared_enum_name_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let overlay = write_overlay(
            dir.path(),
            "enums.yaml",
            "billing:\n  quota:\n    auth_mode: licence_checksum\nengine:\n  modules: [sdlc, cod]\n",
        );

        let error = AppConfig::load_from(Some(&overlay), dir.path())
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("- billing.quota.auth_mode: unknown value \"licence_checksum\""),
            "{error}"
        );
        assert!(
            error.contains("- engine.modules[1]: unknown value \"cod\""),
            "{error}"
        );
    }

    #[test]
    fn explicit_overlay_file_must_exist() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("missing.yaml");

        let err = AppConfig::load_from(Some(&missing), dir.path()).unwrap_err();

        assert!(matches!(err, ConfigError::ReadOverlay { .. }), "{err}");
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

        let config = AppConfig::load_from(Some(&overlay), &secrets).unwrap();

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

        let config = AppConfig::load_from(Some(&overlay), &secrets).unwrap();

        assert_eq!(config.graph.password.as_deref(), Some("secret-password"));
    }

    #[test]
    fn a_retired_pipeline_key_in_an_overlay_is_ignored() {
        let yaml = r#"
engine:
  handlers:
    code-indexing-task:
      pipeline:
        write_buffer_age_secs: 5
        write_max_flush_age_secs: 5
"#;

        let config: AppConfig = builder_with_defaults()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .expect("a deployed overlay may still carry keys a release removed");

        assert_eq!(
            config
                .engine
                .handlers
                .code_indexing_task
                .pipeline
                .write_max_concurrent,
            AppConfig::embedded_defaults()
                .engine
                .handlers
                .code_indexing_task
                .pipeline
                .write_max_concurrent
        );
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
