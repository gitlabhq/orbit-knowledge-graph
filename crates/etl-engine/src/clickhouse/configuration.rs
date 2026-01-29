//! ClickHouse connection configuration.

use serde::{Deserialize, Serialize};

use super::arrow_client::ArrowClickHouseClient;
use crate::destination::DestinationError;
use crate::env::env_var_or;

/// ClickHouse connection settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClickHouseConfiguration {
    /// Database name.
    pub database: String,

    /// Server URL, e.g. "http://127.0.0.1:8123" (HTTP protocol).
    pub url: String,

    /// Username for authentication.
    pub username: String,

    /// Optional password for authentication.
    #[serde(default)]
    pub password: Option<String>,
}

impl Default for ClickHouseConfiguration {
    fn default() -> Self {
        Self {
            database: "default".to_string(),
            url: "http://127.0.0.1:8123".to_string(),
            username: "default".to_string(),
            password: None,
        }
    }
}

impl ClickHouseConfiguration {
    /// Creates configuration from environment variables.
    ///
    /// Uses defaults for any unset variables:
    /// - `CLICKHOUSE_URL`: Server address (default: "http://127.0.0.1:8123")
    /// - `CLICKHOUSE_DATABASE`: Database name (default: "default")
    /// - `CLICKHOUSE_USERNAME`: Username (default: "default")
    /// - `CLICKHOUSE_PASSWORD`: Optional password
    pub fn from_env() -> Self {
        Self::from_env_with_prefix("")
    }

    /// Creates configuration from environment variables with a custom prefix.
    ///
    /// For example, with prefix "DATALAKE", reads:
    /// - `DATALAKE_CLICKHOUSE_URL`: Server address (default: "http://127.0.0.1:8123")
    /// - `DATALAKE_CLICKHOUSE_DATABASE`: Database name (default: "default")
    /// - `DATALAKE_CLICKHOUSE_USERNAME`: Username (default: "default")
    /// - `DATALAKE_CLICKHOUSE_PASSWORD`: Optional password
    ///
    /// With an empty prefix, reads the standard `CLICKHOUSE_*` variables.
    pub fn from_env_with_prefix(prefix: &str) -> Self {
        let prefix = if prefix.is_empty() {
            "CLICKHOUSE".to_string()
        } else {
            format!("{prefix}_CLICKHOUSE")
        };

        Self {
            url: env_var_or(&format!("{prefix}_URL"), "http://127.0.0.1:8123".into()),
            database: env_var_or(&format!("{prefix}_DATABASE"), "default".into()),
            username: env_var_or(&format!("{prefix}_USERNAME"), "default".into()),
            password: std::env::var(format!("{prefix}_PASSWORD")).ok(),
        }
    }

    /// Validates the configuration fields.
    pub fn validate(&self) -> Result<(), DestinationError> {
        if self.database.is_empty() {
            return Err(DestinationError::InvalidConfiguration(
                "database cannot be empty".to_string(),
            ));
        }

        if self.url.is_empty() {
            return Err(DestinationError::InvalidConfiguration(
                "url cannot be empty".to_string(),
            ));
        }

        if self.username.is_empty() {
            return Err(DestinationError::InvalidConfiguration(
                "username cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Builds an Arrow ClickHouse client from this configuration.
    pub fn build_client(&self) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &self.url,
            &self.database,
            &self.username,
            self.password.as_deref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    #[test]
    fn test_optional_password() {
        let json = r#"{
            "database": "test",
            "url": "http://127.0.0.1:8123",
            "username": "default",
            "password": "secret"
        }"#;

        let config: ClickHouseConfiguration = serde_json::from_str(json).unwrap();
        assert_eq!(config.password, Some("secret".to_string()));
    }

    #[test]
    fn test_password_defaults_to_none() {
        let json = r#"{
            "database": "test",
            "url": "http://127.0.0.1:8123",
            "username": "default"
        }"#;

        let config: ClickHouseConfiguration = serde_json::from_str(json).unwrap();
        assert_eq!(config.password, None);
    }

    #[test]
    fn test_validate_success() {
        let config = ClickHouseConfiguration {
            database: "test".to_string(),
            url: "http://127.0.0.1:8123".to_string(),
            username: "default".to_string(),
            password: None,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_database() {
        let config = ClickHouseConfiguration {
            database: "".to_string(),
            url: "http://127.0.0.1:8123".to_string(),
            username: "default".to_string(),
            password: None,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("database"));
    }

    #[test]
    fn test_validate_empty_url() {
        let config = ClickHouseConfiguration {
            database: "test".to_string(),
            url: "".to_string(),
            username: "default".to_string(),
            password: None,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("url"));
    }

    #[test]
    fn test_validate_empty_username() {
        let config = ClickHouseConfiguration {
            database: "test".to_string(),
            url: "http://127.0.0.1:8123".to_string(),
            username: "".to_string(),
            password: None,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("username"));
    }

    #[test]
    fn test_default_uses_http() {
        let config = ClickHouseConfiguration::default();
        assert!(config.url.starts_with("http://"));
        assert!(config.url.contains("8123"));
    }

    #[test]
    #[serial]
    fn test_from_env_with_prefix_reads_prefixed_variables() {
        // SAFETY: Test is serialized and cleans up env vars after use.
        unsafe {
            std::env::set_var("DATALAKE_CLICKHOUSE_URL", "http://datalake:8123");
            std::env::set_var("DATALAKE_CLICKHOUSE_DATABASE", "datalake_db");
            std::env::set_var("DATALAKE_CLICKHOUSE_USERNAME", "datalake_user");
            std::env::set_var("DATALAKE_CLICKHOUSE_PASSWORD", "datalake_pass");
        }

        let config = ClickHouseConfiguration::from_env_with_prefix("DATALAKE");

        assert_eq!(config.url, "http://datalake:8123");
        assert_eq!(config.database, "datalake_db");
        assert_eq!(config.username, "datalake_user");
        assert_eq!(config.password, Some("datalake_pass".to_string()));

        // SAFETY: Cleaning up env vars set above.
        unsafe {
            std::env::remove_var("DATALAKE_CLICKHOUSE_URL");
            std::env::remove_var("DATALAKE_CLICKHOUSE_DATABASE");
            std::env::remove_var("DATALAKE_CLICKHOUSE_USERNAME");
            std::env::remove_var("DATALAKE_CLICKHOUSE_PASSWORD");
        }
    }

    #[test]
    #[serial]
    fn test_from_env_with_empty_prefix_reads_standard_variables() {
        // SAFETY: Test is serialized and cleans up env vars after use.
        unsafe {
            std::env::set_var("CLICKHOUSE_URL", "http://standard:8123");
            std::env::set_var("CLICKHOUSE_DATABASE", "standard_db");
        }

        let config = ClickHouseConfiguration::from_env_with_prefix("");

        assert_eq!(config.url, "http://standard:8123");
        assert_eq!(config.database, "standard_db");

        // SAFETY: Cleaning up env vars set above.
        unsafe {
            std::env::remove_var("CLICKHOUSE_URL");
            std::env::remove_var("CLICKHOUSE_DATABASE");
        }
    }
}
