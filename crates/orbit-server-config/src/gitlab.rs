//! GitLab client and server-side GitLab configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_stream_retry_max_attempts() -> usize {
    3
}

fn default_webserver_channel_cache_capacity() -> u64 {
    32
}

fn default_webserver_max_sessions() -> usize {
    64
}

fn default_webserver_max_inflight_streams() -> usize {
    64
}

fn default_webserver_max_inflight_streams_per_channel() -> usize {
    16
}

fn default_webserver_channel_idle_timeout_secs() -> u64 {
    60
}

fn default_webserver_negative_cache_ttl_secs() -> u64 {
    30
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GitalyTransport {
    #[default]
    RailsHttp,
    WorkhorseWs,
    WorkhorseWsWithFallback,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct GitalyProxyConfig {
    #[serde(default = "default_stream_retry_max_attempts")]
    pub stream_retry_max_attempts: usize,
    #[serde(default = "default_webserver_channel_cache_capacity")]
    pub webserver_channel_cache_capacity: u64,
    #[serde(default = "default_webserver_max_sessions")]
    pub webserver_max_sessions: usize,
    #[serde(default = "default_webserver_max_inflight_streams")]
    pub webserver_max_inflight_streams: usize,
    #[serde(default = "default_webserver_max_inflight_streams_per_channel")]
    pub webserver_max_inflight_streams_per_channel: usize,
    #[serde(default = "default_webserver_channel_idle_timeout_secs")]
    pub webserver_channel_idle_timeout_secs: u64,
    #[serde(default = "default_webserver_negative_cache_ttl_secs")]
    pub webserver_negative_cache_ttl_secs: u64,
}

impl Default for GitalyProxyConfig {
    fn default() -> Self {
        Self {
            stream_retry_max_attempts: default_stream_retry_max_attempts(),
            webserver_channel_cache_capacity: default_webserver_channel_cache_capacity(),
            webserver_max_sessions: default_webserver_max_sessions(),
            webserver_max_inflight_streams: default_webserver_max_inflight_streams(),
            webserver_max_inflight_streams_per_channel:
                default_webserver_max_inflight_streams_per_channel(),
            webserver_channel_idle_timeout_secs: default_webserver_channel_idle_timeout_secs(),
            webserver_negative_cache_ttl_secs: default_webserver_negative_cache_ttl_secs(),
        }
    }
}

/// Configuration for connecting to the GitLab internal API.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct GitlabClientConfiguration {
    /// Base URL for the GitLab API (e.g. "https://staging.gitlab.com:11443").
    pub base_url: String,
    /// Base64-encoded key used to sign outbound JWT tokens.
    pub signing_key: String,
    /// Optional hostname to resolve for DNS override. When set, the host in
    /// `base_url` is resolved to the IP of this hostname instead. This allows
    /// TLS to verify against the `base_url` host while routing traffic through
    /// a different endpoint (e.g. a PSC internal gateway).
    #[serde(default)]
    pub resolve_host: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct JwtConfig {
    #[serde(default)]
    pub signing_key: Option<String>,
    #[serde(default)]
    pub verifying_key: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct GitlabConfig {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub jwt: JwtConfig,
    #[serde(default)]
    pub resolve_host: Option<String>,
    #[serde(default)]
    pub gitaly_transport: GitalyTransport,
    #[serde(default)]
    pub gitaly_proxy: GitalyProxyConfig,
}

impl GitlabConfig {
    pub fn client_config(&self) -> Option<GitlabClientConfiguration> {
        let base_url = self.base_url.clone()?;
        let signing_key = self.jwt.signing_key.clone()?;
        Some(GitlabClientConfiguration {
            base_url,
            signing_key,
            resolve_host: self.resolve_host.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gitaly_transport_defaults_to_rails_http() {
        let config: GitlabConfig = serde_json::from_str("{}").unwrap();

        assert_eq!(config.gitaly_transport, GitalyTransport::RailsHttp);
        assert_eq!(config.gitaly_proxy.stream_retry_max_attempts, 3);
        assert_eq!(config.gitaly_proxy.webserver_channel_cache_capacity, 32);
        assert_eq!(config.gitaly_proxy.webserver_max_sessions, 64);
        assert_eq!(config.gitaly_proxy.webserver_max_inflight_streams, 64);
        assert_eq!(
            config
                .gitaly_proxy
                .webserver_max_inflight_streams_per_channel,
            16
        );
        assert_eq!(config.gitaly_proxy.webserver_channel_idle_timeout_secs, 60);
        assert_eq!(config.gitaly_proxy.webserver_negative_cache_ttl_secs, 30);
    }

    #[test]
    fn parses_all_gitaly_transport_modes() {
        for (value, expected) in [
            ("rails_http", GitalyTransport::RailsHttp),
            ("workhorse_ws", GitalyTransport::WorkhorseWs),
            (
                "workhorse_ws_with_fallback",
                GitalyTransport::WorkhorseWsWithFallback,
            ),
        ] {
            let config: GitlabConfig = serde_json::from_value(serde_json::json!({
                "gitaly_transport": value,
                "gitaly_proxy": { "stream_retry_max_attempts": 7 }
            }))
            .unwrap();

            assert_eq!(config.gitaly_transport, expected);
            assert_eq!(config.gitaly_proxy.stream_retry_max_attempts, 7);
            assert_eq!(config.gitaly_proxy.webserver_channel_cache_capacity, 32);
        }
    }
}
