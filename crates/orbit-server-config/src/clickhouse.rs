//! ClickHouse connection configuration.

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 7200;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ClickHouseConfiguration {
    pub database: String,
    pub url: String,
    pub username: String,
    pub password: Option<String>,
    #[serde(default)]
    pub session_settings: HashMap<String, String>,
    pub replicated: bool,
    /// Settings applied to INSERT operations only (both bulk Arrow IPC and
    /// parameterized `INSERT VALUES`).
    ///
    /// Typical use: enable server-side batching via `async_insert` to reduce
    /// part creation when many small or concurrent writes hit the same tables.
    #[serde(default)]
    pub insert_settings: HashMap<String, String>,
    /// Server-side `max_execution_time` for each executed statement (DDL,
    /// `INSERT ... SELECT`, `INSERT VALUES`), with the client waiting that
    /// long plus up to 30 s more as a backstop for a stuck socket. 0 turns
    /// both off. The default is not measured: two hours leaves room for the
    /// `INSERT ... SELECT` copies a migration runs on a large tenant, which
    /// nothing else in the code bounds.
    #[serde(default = "default_query_timeout_secs")]
    pub query_timeout_secs: u64,
    pub profiling: ProfilingConfig,
}

fn default_query_timeout_secs() -> u64 {
    DEFAULT_QUERY_TIMEOUT_SECS
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ProfilingConfig {
    pub enabled: bool,
    pub explain: bool,
    pub query_log: bool,
    pub processors: bool,
    pub instance_health: bool,
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

        if self.replicated
            && self
                .session_settings
                .get("insert_quorum")
                .is_some_and(|value| value == "0")
        {
            return Err(ConfigurationError::ReplicatedWithoutQuorum);
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

    #[error("replicated is enabled but insert_quorum is set to 0")]
    ReplicatedWithoutQuorum,
}
