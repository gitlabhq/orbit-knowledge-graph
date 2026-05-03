//! Top-level application configuration.

use std::net::SocketAddr;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::analytics::AnalyticsConfig;
use crate::billing::BillingConfig;
use crate::clickhouse::ClickHouseConfiguration;
use crate::engine::{EngineConfiguration, ScheduleConfig};
use crate::gitlab::{GitlabClientConfiguration, GitlabConfig};
use crate::grpc::GrpcConfig;
use crate::health_check::HealthCheckConfig;
use crate::metrics::MetricsConfig;
use crate::nats::NatsConfiguration;
use crate::query::QuerySettings;
use crate::schema::SchemaConfig;
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
    #[serde(default)]
    pub schema: SchemaConfig,
    #[serde(default)]
    pub analytics: AnalyticsConfig,
    #[serde(default)]
    pub billing: BillingConfig,
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
    pipeline:
      max_file_size_bytes: 10000000
      max_files: 200000
      worker_threads: 2
      max_concurrent_languages: 3
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
                .code_indexing_task
                .pipeline
                .max_file_size_bytes,
            10_000_000
        );
        assert_eq!(
            engine.handlers.code_indexing_task.pipeline.max_files,
            200_000
        );
        assert_eq!(
            engine.handlers.code_indexing_task.pipeline.worker_threads,
            2
        );
        assert_eq!(
            engine
                .handlers
                .code_indexing_task
                .pipeline
                .max_concurrent_languages,
            3
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

    /// Verifies `prefix_separator("_")` is required for `GKG_GRAPH__DATABASE`
    /// style env vars to work with real process env vars.
    ///
    /// Without it, the config crate defaults the prefix separator to the
    /// hierarchy separator (`__`), so it looks for `GKG__GRAPH__DATABASE`
    /// (double underscore after prefix) and silently ignores
    /// `GKG_GRAPH__DATABASE` (single underscore).
    ///
    /// Uses a subprocess since env vars must be set before config loading
    /// and `std::env::set_var` is unsafe in multi-threaded test runners.
    #[test]
    fn real_env_vars_override_yaml_defaults() {
        let test_bin = std::env::current_exe().unwrap();
        // The test binary must run from the workspace root so config/default.yaml is found.
        let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let output = std::process::Command::new(&test_bin)
            .current_dir(workspace_root)
            .arg("app::tests::subprocess_env_config_loader")
            .arg("--ignored")
            .arg("--exact")
            .arg("--nocapture")
            .env("GKG_GRAPH__DATABASE", "env_graph_db")
            .env("GKG_DATALAKE__DATABASE", "env_datalake_db")
            .env("GKG_NATS__URL", "nats://env-host:4222")
            .output()
            .expect("failed to spawn subprocess");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        // Parse the JSON line printed by the inner test.
        let json_line = stdout
            .lines()
            .find(|l| l.starts_with('{'))
            .unwrap_or_else(|| {
                panic!(
                    "inner test did not print JSON.\nstdout: {stdout}\nstderr: {stderr}\nexit: {}",
                    output.status
                )
            });
        let values: serde_json::Value =
            serde_json::from_str(json_line).expect("inner test should print valid JSON");

        // If the inner test returned an error, fail with it.
        if let Some(err) = values.get("error") {
            panic!("inner test config load failed: {err}");
        }

        assert_eq!(
            values["graph_database"], "env_graph_db",
            "GKG_GRAPH__DATABASE should override config/default.yaml"
        );
        assert_eq!(
            values["datalake_database"], "env_datalake_db",
            "GKG_DATALAKE__DATABASE should override config/default.yaml"
        );
        assert_eq!(
            values["nats_url"], "nats://env-host:4222",
            "GKG_NATS__URL should override config/default.yaml"
        );
    }

    /// Subprocess helper: loads config with real process env vars and prints
    /// the resolved values as JSON. Only runs when called by the outer test.
    #[test]
    #[ignore]
    fn subprocess_env_config_loader() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = match AppConfig::load_with_secret_dir(dir.path().to_str().unwrap()) {
            Ok(c) => c,
            Err(e) => {
                println!("{}", serde_json::json!({"error": e.to_string()}));
                return;
            }
        };

        println!(
            "{}",
            serde_json::json!({
                "graph_database": config.graph.database,
                "datalake_database": config.datalake.database,
                "nats_url": config.nats.url,
            })
        );
    }
}
