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
    /// Bound on one executed statement (DDL, `INSERT ... SELECT`, `INSERT
    /// VALUES`) from the request to the server's answer. A hung connection
    /// fails the statement instead of holding its task open. 0 turns the
    /// bound off. Keep it above the longest statement these paths run; the
    /// table cleanup caps its mutations at 7,200 s on the server.
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
