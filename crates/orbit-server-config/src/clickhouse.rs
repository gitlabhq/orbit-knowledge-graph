//! ClickHouse connection configuration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClickHouseConfiguration {
    pub database: String,
    pub url: String,
    pub username: String,
    pub password: Option<String>,
    pub compatibility: String,
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
    pub profiling: ProfilingConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilingConfig {
    pub enabled: bool,
    pub explain: bool,
    pub query_log: bool,
    pub processors: bool,
    pub instance_health: bool,
}

impl ClickHouseConfiguration {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
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
    #[error("replicated is enabled but insert_quorum is set to 0")]
    ReplicatedWithoutQuorum,
}

#[cfg(test)]
mod tests {
    use crate::AppConfig;

    #[test]
    fn an_empty_connection_setting_is_rejected_on_both_connections() {
        for connection in ["datalake", "graph"] {
            for key in ["url", "database", "username"] {
                let error = AppConfig::with_overlay(&format!("{connection}:\n  {key}: \"\"\n"))
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains(&format!("- {connection}.{key}: ")),
                    "{error}"
                );
            }
        }
    }
}
