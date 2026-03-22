use std::collections::HashMap;

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
    #[serde(default = "default_join_algorithm")]
    pub join_algorithm: String,
    #[serde(default)]
    pub query_settings: HashMap<String, String>,
    #[serde(default)]
    pub profiling: ProfilingConfig,
}

fn default_join_algorithm() -> String {
    "hash".to_string()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
            join_algorithm: default_join_algorithm(),
            query_settings: HashMap::new(),
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

    pub fn build_client(&self) -> ArrowClickHouseClient {
        let mut settings = self.query_settings.clone();
        settings
            .entry("join_algorithm".to_string())
            .or_insert_with(|| self.join_algorithm.clone());

        ArrowClickHouseClient::new(
            &self.url,
            &self.database,
            &self.username,
            self.password.as_deref(),
            &settings,
        )
    }
}

#[cfg(test)]
mod tests {
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
            join_algorithm: default_join_algorithm(),
            query_settings: std::collections::HashMap::new(),
            profiling: Default::default(),
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
            join_algorithm: default_join_algorithm(),
            query_settings: std::collections::HashMap::new(),
            profiling: Default::default(),
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
            join_algorithm: default_join_algorithm(),
            query_settings: std::collections::HashMap::new(),
            profiling: Default::default(),
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
            join_algorithm: default_join_algorithm(),
            query_settings: std::collections::HashMap::new(),
            profiling: Default::default(),
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
    fn test_join_algorithm_defaults_to_hash() {
        let json = r#"{
            "database": "test",
            "url": "http://127.0.0.1:8123",
            "username": "default"
        }"#;

        let config: ClickHouseConfiguration = serde_json::from_str(json).unwrap();
        assert_eq!(config.join_algorithm, "hash");
    }

    #[test]
    fn test_join_algorithm_override() {
        let json = r#"{
            "database": "test",
            "url": "http://127.0.0.1:8123",
            "username": "default",
            "join_algorithm": "parallel_hash"
        }"#;

        let config: ClickHouseConfiguration = serde_json::from_str(json).unwrap();
        assert_eq!(config.join_algorithm, "parallel_hash");
    }

    #[test]
    fn test_query_settings_override_wins_over_join_algorithm() {
        let config = ClickHouseConfiguration {
            join_algorithm: "parallel_hash".to_string(),
            query_settings: HashMap::from([(
                "join_algorithm".to_string(),
                "full_sorting_merge".to_string(),
            )]),
            ..Default::default()
        };

        let mut settings = config.query_settings.clone();
        settings
            .entry("join_algorithm".to_string())
            .or_insert_with(|| config.join_algorithm.clone());
        assert_eq!(settings["join_algorithm"], "full_sorting_merge");
    }

    // Without the rustls-tls-* features on the `clickhouse` crate, any HTTPS
    // URL is rejected immediately with "scheme is not http". This test guards
    // against accidental removal of those features (e.g. by Renovate Bot).
    #[tokio::test]
    async fn test_https_url_does_not_fail_with_missing_tls() {
        let config = ClickHouseConfiguration {
            database: "default".to_string(),
            url: "https://localhost:1".to_string(),
            username: "default".to_string(),
            password: None,
            join_algorithm: default_join_algorithm(),
            query_settings: std::collections::HashMap::new(),
            profiling: Default::default(),
        };

        let client = config.build_client();
        let err = client.execute("SELECT 1").await.unwrap_err();
        let msg = err.to_string();

        assert!(
            !msg.contains("scheme is not http"),
            "TLS features are missing on the clickhouse crate: {msg}"
        );
    }
}
