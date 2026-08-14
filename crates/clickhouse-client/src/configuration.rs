use std::collections::HashMap;

use orbit_server_config::ClickHouseConfiguration;

use crate::arrow_client::ArrowClickHouseClient;

/// Serialized inserts make `select_sequential_consistency` work; `async_insert` (on by default since 26.x) is rejected with a quorum.
const QUORUM_SESSION_SETTINGS: [(&str, &str); 4] = [
    ("insert_quorum", "auto"),
    ("insert_quorum_parallel", "0"),
    ("select_sequential_consistency", "1"),
    ("async_insert", "0"),
];

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
            &build_session_settings_with_quorum_defaults(self),
            &self.insert_settings,
        )
    }
}

fn build_session_settings_with_quorum_defaults(
    config: &ClickHouseConfiguration,
) -> HashMap<String, String> {
    if !config.quorum_writes {
        return config.session_settings.clone();
    }
    let mut settings: HashMap<String, String> = QUORUM_SESSION_SETTINGS
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    settings.extend(
        config
            .session_settings
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    settings
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_server_config::ConfigurationError;

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
            session_settings: std::collections::HashMap::new(),
            quorum_writes: false,
            insert_settings: std::collections::HashMap::new(),
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
            session_settings: std::collections::HashMap::new(),
            quorum_writes: false,
            insert_settings: std::collections::HashMap::new(),
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
            session_settings: std::collections::HashMap::new(),
            quorum_writes: false,
            insert_settings: std::collections::HashMap::new(),
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
            session_settings: std::collections::HashMap::new(),
            quorum_writes: false,
            insert_settings: std::collections::HashMap::new(),
            profiling: Default::default(),
        };

        let result = config.validate();
        assert!(matches!(result, Err(ConfigurationError::EmptyUsername)));
    }

    #[test]
    fn quorum_writes_with_zero_insert_quorum_is_rejected() {
        let config = ClickHouseConfiguration {
            quorum_writes: true,
            session_settings: HashMap::from([("insert_quorum".to_string(), "0".to_string())]),
            ..ClickHouseConfiguration::default()
        };

        let result = config.validate();

        assert!(matches!(
            result,
            Err(ConfigurationError::QuorumWritesWithoutQuorum)
        ));
    }

    #[test]
    fn quorum_writes_expand_to_session_settings() {
        let config = ClickHouseConfiguration {
            quorum_writes: true,
            ..ClickHouseConfiguration::default()
        };

        let settings = build_session_settings_with_quorum_defaults(&config);

        assert_eq!(
            settings.get("insert_quorum").map(String::as_str),
            Some("auto")
        );
        assert_eq!(
            settings.get("insert_quorum_parallel").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            settings
                .get("select_sequential_consistency")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            settings.get("async_insert").map(String::as_str),
            Some("0"),
            "servers since 26.x default async_insert on, and async inserts \
             cannot carry a quorum"
        );
    }

    #[test]
    fn explicit_session_setting_overrides_quorum_default() {
        let config = ClickHouseConfiguration {
            quorum_writes: true,
            session_settings: std::collections::HashMap::from([(
                "insert_quorum".to_string(),
                "3".to_string(),
            )]),
            ..ClickHouseConfiguration::default()
        };

        let settings = build_session_settings_with_quorum_defaults(&config);

        assert_eq!(settings.get("insert_quorum").map(String::as_str), Some("3"));
    }

    #[test]
    fn quorum_writes_unset_leaves_session_settings_alone() {
        let config = ClickHouseConfiguration::default();

        let settings = build_session_settings_with_quorum_defaults(&config);

        assert!(settings.is_empty());
    }

    #[test]
    fn quorum_writes_reach_the_built_client() {
        let config = ClickHouseConfiguration {
            quorum_writes: true,
            ..ClickHouseConfiguration::default()
        };

        assert!(config.build_client().has_quorum_writes());
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
            session_settings: std::collections::HashMap::new(),
            quorum_writes: false,
            insert_settings: std::collections::HashMap::new(),
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
