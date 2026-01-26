//! ClickHouse connection configuration.

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

impl ClickHouseConfiguration {
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
