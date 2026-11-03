//! GitLab Observability connection configuration.
//!
//! Observability data lives in per-tenant ClickHouse databases on shared
//! central servers, outside the graph. Orbit reads it at query time to resolve
//! virtual columns, so it needs both a read credential and a way to learn
//! which server holds a given group's data.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_host_map_refresh_seconds() -> u64 {
    300
}

fn default_request_timeout_seconds() -> u64 {
    10
}

fn default_max_log_lines() -> u32 {
    50
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Base URL of the provisioner that owns the group to ClickHouse host map
    /// (e.g. "https://provisioner.internal"). Resolution is disabled when unset.
    pub provisioner_url: Option<String>,
    /// API key for the provisioner's host lookup endpoint.
    pub provisioner_api_key: Option<String>,
    /// ClickHouse user with SELECT on each tenant's traces and logs databases.
    pub clickhouse_username: Option<String>,
    pub clickhouse_password: Option<String>,
    /// How long a fetched host map stays usable before a refresh is attempted.
    #[serde(default = "default_host_map_refresh_seconds")]
    pub host_map_refresh_seconds: u64,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    /// Upper bound on log lines returned by the job log tail lookup.
    #[serde(default = "default_max_log_lines")]
    pub max_log_lines: u32,
}

impl ObservabilityConfig {
    /// Resolution needs a provisioner to route with and a credential to read
    /// with, so all four values must be present for the resolver to register.
    pub fn resolver_settings(&self) -> Option<ObservabilityResolverSettings> {
        Some(ObservabilityResolverSettings {
            provisioner_url: non_empty(self.provisioner_url.as_deref())?,
            provisioner_api_key: non_empty(self.provisioner_api_key.as_deref())?,
            clickhouse_username: non_empty(self.clickhouse_username.as_deref())?,
            clickhouse_password: non_empty(self.clickhouse_password.as_deref())?,
            host_map_refresh_seconds: self.host_map_refresh_seconds,
            request_timeout_seconds: self.request_timeout_seconds,
            max_log_lines: self.max_log_lines,
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
    pub clickhouse_username: String,
    pub clickhouse_password: String,
    pub host_map_refresh_seconds: u64,
    pub request_timeout_seconds: u64,
    pub max_log_lines: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete() -> ObservabilityConfig {
        ObservabilityConfig {
            provisioner_url: Some("https://provisioner.internal".to_string()),
            provisioner_api_key: Some("key".to_string()),
            clickhouse_username: Some("orbit_reader".to_string()),
            clickhouse_password: Some("secret".to_string()),
            host_map_refresh_seconds: 300,
            request_timeout_seconds: 10,
            max_log_lines: 50,
        }
    }

    #[test]
    fn complete_configuration_yields_settings() {
        let settings = complete().resolver_settings().expect("settings");

        assert_eq!(settings.clickhouse_username, "orbit_reader");
        assert_eq!(settings.max_log_lines, 50);
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
            |c: &mut ObservabilityConfig| c.clickhouse_username = None,
            |c: &mut ObservabilityConfig| c.clickhouse_password = None,
        ] {
            let mut config = complete();
            mutate(&mut config);
            assert!(config.resolver_settings().is_none());
        }
    }

    #[test]
    fn blank_values_are_treated_as_absent() {
        let mut config = complete();
        config.clickhouse_password = Some("   ".to_string());

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
