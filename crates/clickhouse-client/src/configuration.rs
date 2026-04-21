use gkg_server_config::ClickHouseConfiguration;

use crate::arrow_client::ArrowClickHouseClient;

pub trait ClickHouseConfigurationExt {
    fn build_client(&self) -> ArrowClickHouseClient;
}

impl ClickHouseConfigurationExt for ClickHouseConfiguration {
    fn build_client(&self) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &self.url,
            &self.database,
            &self.username,
            self.password.as_deref(),
            &self.query_settings,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gkg_server_config::ConfigurationError;

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
