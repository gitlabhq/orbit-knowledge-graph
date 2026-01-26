//! ClickHouse connection configuration.

use clickhouse_arrow::{ArrowFormat, Client, ClientBuilder};
use serde::{Deserialize, Serialize};

use crate::destination::DestinationError;

/// ClickHouse connection settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClickHouseConfiguration {
    /// Database name.
    pub database: String,

    /// Server URL, e.g. "127.0.0.1:9000" (native protocol port).
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
            url: "127.0.0.1:9000".to_string(),
            username: "default".to_string(),
            password: None,
        }
    }
}

impl ClickHouseConfiguration {
    /// Creates configuration from environment variables.
    ///
    /// Uses defaults for any unset variables:
    /// - `CLICKHOUSE_URL`: Server address (default: "127.0.0.1:9000")
    /// - `CLICKHOUSE_DATABASE`: Database name (default: "default")
    /// - `CLICKHOUSE_USERNAME`: Username (default: "default")
    /// - `CLICKHOUSE_PASSWORD`: Optional password
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "127.0.0.1:9000".into()),
            database: std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into()),
            username: std::env::var("CLICKHOUSE_USERNAME").unwrap_or_else(|_| "default".into()),
            password: std::env::var("CLICKHOUSE_PASSWORD").ok(),
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

    pub async fn build_client(&self) -> Result<Client<ArrowFormat>, clickhouse_arrow::Error> {
        let mut builder = ClientBuilder::new()
            .with_endpoint(&self.url)
            .with_database(&self.database)
            .with_username(&self.username);

        if let Some(password) = &self.password {
            builder = builder.with_password(password);
        }

        builder.build_arrow().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optional_password() {
        let json = r#"{
            "database": "test",
            "url": "127.0.0.1:9000",
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
            "url": "127.0.0.1:9000",
            "username": "default"
        }"#;

        let config: ClickHouseConfiguration = serde_json::from_str(json).unwrap();
        assert_eq!(config.password, None);
    }

    #[test]
    fn test_validate_success() {
        let config = ClickHouseConfiguration {
            database: "test".to_string(),
            url: "127.0.0.1:9000".to_string(),
            username: "default".to_string(),
            password: None,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_database() {
        let config = ClickHouseConfiguration {
            database: "".to_string(),
            url: "127.0.0.1:9000".to_string(),
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
            url: "127.0.0.1:9000".to_string(),
            username: "".to_string(),
            password: None,
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("username"));
    }
}
