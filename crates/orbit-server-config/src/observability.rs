use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_host_map_refresh_seconds() -> u64 {
    300
}

fn default_host_map_retry_after_seconds() -> u64 {
    30
}

fn default_request_timeout_seconds() -> u64 {
    10
}

fn default_query_timeout_seconds() -> u64 {
    15
}

fn default_max_result_rows() -> u64 {
    10_000
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ObservabilityConfig {
    pub provisioner_url: Option<String>,
    pub provisioner_api_key: Option<String>,
    #[serde(default = "default_host_map_refresh_seconds")]
    pub host_map_refresh_seconds: u64,
    /// Minimum wait before retrying the provisioner after a failed refresh, so
    /// a provisioner outage does not add the request timeout to every query.
    #[serde(default = "default_host_map_retry_after_seconds")]
    pub host_map_retry_after_seconds: u64,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    /// Server-side bound on a tenant ClickHouse query, sent as max_execution_time.
    #[serde(default = "default_query_timeout_seconds")]
    pub query_timeout_seconds: u64,
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: u64,
}

/// The host map carries every tenant's ClickHouse password, so plain HTTP is
/// only accepted for a loopback provisioner.
fn transport_is_protected(url: &str) -> bool {
    if url.starts_with("https://") {
        return true;
    }

    let Some(authority) = url.strip_prefix("http://") else {
        return false;
    };
    let authority = authority.split(['/', '?', '#']).next().unwrap_or_default();
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _port)| host);

    matches!(host, "127.0.0.1" | "localhost" | "[::1]" | "::1")
}

impl ObservabilityConfig {
    pub fn resolver_settings(&self) -> Option<ObservabilityResolverSettings> {
        let provisioner_url = non_empty(self.provisioner_url.as_deref())?;
        if !transport_is_protected(&provisioner_url) {
            return None;
        }

        Some(ObservabilityResolverSettings {
            provisioner_url,
            provisioner_api_key: non_empty(self.provisioner_api_key.as_deref())?,
            host_map_refresh_seconds: self.host_map_refresh_seconds,
            host_map_retry_after_seconds: self.host_map_retry_after_seconds,
            request_timeout_seconds: self.request_timeout_seconds,
            query_timeout_seconds: self.query_timeout_seconds,
            max_result_rows: self.max_result_rows,
        })
    }
}

fn non_empty(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[derive(Clone, Debug)]
pub struct ObservabilityResolverSettings {
    pub provisioner_url: String,
    pub provisioner_api_key: String,
    pub host_map_refresh_seconds: u64,
    pub host_map_retry_after_seconds: u64,
    pub request_timeout_seconds: u64,
    pub query_timeout_seconds: u64,
    pub max_result_rows: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete() -> ObservabilityConfig {
        ObservabilityConfig {
            provisioner_url: Some("https://provisioner.internal".to_string()),
            provisioner_api_key: Some("key".to_string()),
            host_map_refresh_seconds: 300,
            host_map_retry_after_seconds: 30,
            request_timeout_seconds: 10,
            query_timeout_seconds: 15,
            max_result_rows: 10_000,
        }
    }

    #[test]
    fn complete_configuration_yields_settings() {
        let settings = complete().resolver_settings().expect("settings");

        assert_eq!(settings.provisioner_url, "https://provisioner.internal");
        assert_eq!(settings.query_timeout_seconds, 15);
    }

    #[test]
    fn default_configuration_yields_no_settings() {
        assert!(ObservabilityConfig::default().resolver_settings().is_none());
    }

    #[test]
    fn each_missing_field_disables_resolution() {
        for mutate in [
            (|c: &mut ObservabilityConfig| c.provisioner_url = None)
                as fn(&mut ObservabilityConfig),
            |c: &mut ObservabilityConfig| c.provisioner_api_key = None,
        ] {
            let mut config = complete();
            mutate(&mut config);
            assert!(config.resolver_settings().is_none());
        }
    }

    #[test]
    fn a_plaintext_provisioner_url_disables_resolution() {
        let mut config = complete();
        config.provisioner_url = Some("http://provisioner.internal".to_string());

        assert!(
            config.resolver_settings().is_none(),
            "the host map carries tenant ClickHouse passwords"
        );
    }

    #[test]
    fn a_loopback_provisioner_may_use_plain_http() {
        for url in [
            "http://127.0.0.1:3000",
            "http://localhost:3000",
            "http://[::1]:3000",
            "http://localhost",
            "http://127.0.0.1/api",
        ] {
            let mut config = complete();
            config.provisioner_url = Some(url.to_string());
            assert!(config.resolver_settings().is_some(), "{url}");
        }
    }

    #[test]
    fn a_host_that_merely_starts_with_a_loopback_name_is_rejected() {
        for url in [
            "http://localhost.example.com",
            "http://127.0.0.1.example.com",
            "http://localhost.evil.test:3000",
            "http://[::1].example.com",
        ] {
            let mut config = complete();
            config.provisioner_url = Some(url.to_string());
            assert!(config.resolver_settings().is_none(), "{url}");
        }
    }

    #[test]
    fn blank_values_are_treated_as_absent() {
        let mut config = complete();
        config.provisioner_api_key = Some("   ".to_string());

        assert!(config.resolver_settings().is_none());
    }

    #[test]
    fn values_are_trimmed() {
        let mut config = complete();
        config.provisioner_url = Some("  https://provisioner.internal  ".to_string());

        let settings = config.resolver_settings().expect("settings");

        assert_eq!(settings.provisioner_url, "https://provisioner.internal");
    }
}
