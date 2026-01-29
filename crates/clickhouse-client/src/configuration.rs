use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::arrow_client::ArrowClickHouseClient;
use crate::error::ConfigurationError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClickHouseConfiguration {
    pub database: String,
    pub url: String,
    pub username: String,
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
    pub fn from_env() -> Self {
        Self::from_env_with_prefix("")
    }

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

    pub fn build_client(&self) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &self.url,
            &self.database,
            &self.username,
            self.password.as_deref(),
        )
    }
}

fn env_var_or<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
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
        assert!(matches!(result, Err(ConfigurationError::EmptyDatabase)));
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
        assert!(matches!(result, Err(ConfigurationError::EmptyUrl)));
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
        assert!(matches!(result, Err(ConfigurationError::EmptyUsername)));
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
        unsafe {
            std::env::set_var("CLICKHOUSE_URL", "http://standard:8123");
            std::env::set_var("CLICKHOUSE_DATABASE", "standard_db");
        }

        let config = ClickHouseConfiguration::from_env_with_prefix("");

        assert_eq!(config.url, "http://standard:8123");
        assert_eq!(config.database, "standard_db");

        unsafe {
            std::env::remove_var("CLICKHOUSE_URL");
            std::env::remove_var("CLICKHOUSE_DATABASE");
        }
    }
}
