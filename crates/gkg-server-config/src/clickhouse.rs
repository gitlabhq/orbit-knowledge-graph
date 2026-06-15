//! ClickHouse connection configuration.

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default `max_query_size` in bytes (10 MiB). Applied as a session-level
/// setting because it controls the parse buffer and must be set before
/// the SQL text is read.
pub const DEFAULT_MAX_QUERY_SIZE: u64 = 10 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ClickHouseConfiguration {
    pub database: String,
    pub url: String,
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
    /// ClickHouse `max_query_size` in bytes. Controls the parse buffer;
    /// must be a session-level setting (cannot go in the SQL SETTINGS clause).
    /// Default: 10 MiB.
    #[serde(default = "default_max_query_size")]
    pub max_query_size: u64,
    #[serde(default)]
    pub session_settings: HashMap<String, String>,
    /// Settings applied to INSERT operations only (both bulk Arrow IPC and
    /// parameterized `INSERT VALUES`).
    ///
    /// Typical use: enable server-side batching via `async_insert` to reduce
    /// part creation when many small or concurrent writes hit the same tables.
    #[serde(default)]
    pub insert_settings: HashMap<String, String>,
    #[serde(default)]
    pub profiling: ProfilingConfig,
}

fn default_max_query_size() -> u64 {
    DEFAULT_MAX_QUERY_SIZE
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ProfilingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub query_log: bool,
    #[serde(default)]
    pub processors: bool,
    #[serde(default)]
    pub instance_health: bool,
}

impl Default for ClickHouseConfiguration {
    fn default() -> Self {
        Self {
            database: "default".to_string(),
            url: "http://127.0.0.1:8123".to_string(),
            username: "default".to_string(),
            password: None,
            max_query_size: DEFAULT_MAX_QUERY_SIZE,
            session_settings: HashMap::new(),
            insert_settings: HashMap::new(),
            profiling: ProfilingConfig::default(),
        }
    }
}

impl ClickHouseConfiguration {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.database.is_empty() {
            return Err(ConfigurationError::EmptyDatabase);
        }

        if self.url.is_empty() {
            return Err(ConfigurationError::EmptyUrl);
        }

        if self.username.is_empty() {
            return Err(ConfigurationError::EmptyUsername);
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("database cannot be empty")]
    EmptyDatabase,

    #[error("url cannot be empty")]
    EmptyUrl,

    #[error("username cannot be empty")]
    EmptyUsername,
}
