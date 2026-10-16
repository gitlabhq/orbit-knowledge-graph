//! ClickHouse connection configuration.

use std::collections::HashMap;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    pub timeouts: TimeoutConfig,
    pub profiling: ProfilingConfig,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TimeoutConfig {
    pub insert_secs: u64,
    pub statement_secs: u64,
}

impl TimeoutConfig {
    pub fn insert(&self) -> Option<Duration> {
        (self.insert_secs > 0).then(|| Duration::from_secs(self.insert_secs))
    }

    pub fn statement(&self) -> Option<Duration> {
        (self.statement_secs > 0).then(|| Duration::from_secs(self.statement_secs))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_second_bound_turns_that_timeout_off() {
        let timeouts = TimeoutConfig {
            insert_secs: 0,
            statement_secs: 5,
        };
        assert_eq!(timeouts.insert(), None);
        assert_eq!(timeouts.statement(), Some(Duration::from_secs(5)));
    }
}
