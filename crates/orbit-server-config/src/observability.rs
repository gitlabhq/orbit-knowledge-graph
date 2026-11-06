use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ObservabilityConfig {
    pub provisioner_url: Option<String>,
    pub provisioner_api_key: Option<String>,
    #[schemars(range(min = 1))]
    pub host_map_refresh_seconds: u64,
    /// Minimum wait before retrying the provisioner after a failed refresh, so
    /// a provisioner outage does not add the request timeout to every query.
    #[schemars(range(min = 1))]
    pub host_map_retry_after_seconds: u64,
    #[schemars(range(min = 1))]
    pub request_timeout_seconds: u64,
    /// Server-side bound on a tenant ClickHouse query, sent as max_execution_time.
    #[schemars(range(min = 1))]
    pub query_timeout_seconds: u64,
    #[schemars(range(min = 1))]
    pub max_result_rows: u64,
}

/// The host map carries every tenant's ClickHouse password, so plain HTTP is
/// only accepted for a loopback provisioner.
fn transport_is_protected(raw: &str) -> bool {
    let Ok(url) = url::Url::parse(raw) else {
        return false;
    };

    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }

    match url.scheme() {
        "https" => true,
        "http" => matches!(
            url.host(),
            Some(url::Host::Domain("localhost"))
                | Some(url::Host::Ipv4(std::net::Ipv4Addr::LOCALHOST))
                | Some(url::Host::Ipv6(std::net::Ipv6Addr::LOCALHOST))
        ),
        _ => false,
    }
}

/// Distinguishes an unconfigured block from a misconfigured one, so the
/// resolver can say why it stayed off rather than leaving null columns as the
/// only clue.
#[derive(Debug, PartialEq, Eq)]
pub enum ObservabilityConfigError {
    MissingProvisionerApiKey,
    UnprotectedProvisionerUrl,
    NonPositive(&'static str),
}

impl std::fmt::Display for ObservabilityConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingProvisionerApiKey => {
                write!(f, "observability.provisioner_api_key is not set")
            }
            Self::UnprotectedProvisionerUrl => write!(
                f,
                "observability.provisioner_url must be https, or http on loopback"
            ),
            Self::NonPositive(field) => {
                write!(f, "observability.{field} must be greater than zero")
            }
        }
    }
}

impl ObservabilityConfig {
    /// `Ok(None)` when the block is simply unconfigured.
    pub fn resolver_settings(
        &self,
    ) -> Result<Option<ObservabilityResolverSettings>, ObservabilityConfigError> {
        let url = non_empty(self.provisioner_url.as_deref());
        let key = non_empty(self.provisioner_api_key.as_deref());

        let (Some(provisioner_url), key) = (url, key) else {
            return Ok(None);
        };
        if !transport_is_protected(&provisioner_url) {
            return Err(ObservabilityConfigError::UnprotectedProvisionerUrl);
        }
        let provisioner_api_key = key.ok_or(ObservabilityConfigError::MissingProvisionerApiKey)?;

        Ok(Some(ObservabilityResolverSettings {
            provisioner_url,
            provisioner_api_key,
            host_map_refresh_seconds: positive(
                self.host_map_refresh_seconds,
                "host_map_refresh_seconds",
            )?,
            host_map_retry_after_seconds: positive(
                self.host_map_retry_after_seconds,
                "host_map_retry_after_seconds",
            )?,
            request_timeout_seconds: positive(
                self.request_timeout_seconds,
                "request_timeout_seconds",
            )?,
            query_timeout_seconds: positive(self.query_timeout_seconds, "query_timeout_seconds")?,
            max_result_rows: positive(self.max_result_rows, "max_result_rows")?,
        }))
    }
}

/// Zero means unlimited to ClickHouse, so a zeroed bound is a missing bound.
fn positive(value: u64, field: &'static str) -> Result<u64, ObservabilityConfigError> {
    (value > 0)
        .then_some(value)
        .ok_or(ObservabilityConfigError::NonPositive(field))
}

fn non_empty(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
        let settings = complete().resolver_settings().unwrap().expect("settings");

        assert_eq!(settings.provisioner_url, "https://provisioner.internal");
        assert_eq!(settings.query_timeout_seconds, 15);
    }

    #[test]
    fn an_unconfigured_provisioner_is_not_an_error() {
        let mut config = complete();
        config.provisioner_url = None;
        config.provisioner_api_key = None;

        assert_eq!(config.resolver_settings(), Ok(None));
    }

    #[test]
    fn a_url_without_a_key_names_the_missing_field() {
        let mut config = complete();
        config.provisioner_api_key = None;

        assert_eq!(
            config.resolver_settings(),
            Err(ObservabilityConfigError::MissingProvisionerApiKey)
        );
    }

    #[test]
    fn a_plaintext_provisioner_url_is_an_error_rather_than_silence() {
        let mut config = complete();
        config.provisioner_url = Some("http://provisioner.internal".to_string());

        assert_eq!(
            config.resolver_settings(),
            Err(ObservabilityConfigError::UnprotectedProvisionerUrl)
        );
    }

    #[test]
    fn a_loopback_provisioner_may_use_plain_http() {
        for url in [
            "http://127.0.0.1:3000",
            "http://localhost:3000",
            "http://[::1]:3000",
            "http://[::1]",
            "http://localhost",
            "http://127.0.0.1/api",
        ] {
            let mut config = complete();
            config.provisioner_url = Some(url.to_string());
            assert!(config.resolver_settings().unwrap().is_some(), "{url}");
        }
    }

    #[test]
    fn a_url_carrying_userinfo_is_rejected() {
        for url in [
            "http://localhost:80@evil.com",
            "http://localhost@evil.com",
            "http://user:pass@evil.com",
            "https://localhost@evil.com",
        ] {
            let mut config = complete();
            config.provisioner_url = Some(url.to_string());
            assert_eq!(
                config.resolver_settings(),
                Err(ObservabilityConfigError::UnprotectedProvisionerUrl),
                "{url}"
            );
        }
    }

    #[test]
    fn a_non_http_scheme_is_rejected() {
        for url in ["ftp://provisioner.internal", "file:///etc/passwd"] {
            let mut config = complete();
            config.provisioner_url = Some(url.to_string());
            assert!(config.resolver_settings().is_err(), "{url}");
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
            assert!(config.resolver_settings().is_err(), "{url}");
        }
    }

    #[test]
    fn a_zeroed_bound_names_the_field() {
        for (zero, field) in [
            (
                (|c: &mut ObservabilityConfig| c.host_map_refresh_seconds = 0)
                    as fn(&mut ObservabilityConfig),
                "host_map_refresh_seconds",
            ),
            (
                |c: &mut ObservabilityConfig| c.host_map_retry_after_seconds = 0,
                "host_map_retry_after_seconds",
            ),
            (
                |c: &mut ObservabilityConfig| c.request_timeout_seconds = 0,
                "request_timeout_seconds",
            ),
            (
                |c: &mut ObservabilityConfig| c.query_timeout_seconds = 0,
                "query_timeout_seconds",
            ),
            (
                |c: &mut ObservabilityConfig| c.max_result_rows = 0,
                "max_result_rows",
            ),
        ] {
            let mut config = complete();
            zero(&mut config);
            assert_eq!(
                config.resolver_settings(),
                Err(ObservabilityConfigError::NonPositive(field))
            );
        }
    }

    #[test]
    fn blank_values_are_treated_as_absent() {
        let mut config = complete();
        config.provisioner_api_key = Some("   ".to_string());

        assert_eq!(
            config.resolver_settings(),
            Err(ObservabilityConfigError::MissingProvisionerApiKey)
        );
    }

    #[test]
    fn values_are_trimmed() {
        let mut config = complete();
        config.provisioner_url = Some("  https://provisioner.internal  ".to_string());

        let settings = config.resolver_settings().unwrap().expect("settings");

        assert_eq!(settings.provisioner_url, "https://provisioner.internal");
    }
}
