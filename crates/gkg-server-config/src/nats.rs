//! NATS broker configuration.

use std::path::Path;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// NATS connection settings.
///
/// Matches siphon's QueuingConfig fields.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct NatsConfiguration {
    /// Server address, e.g. "localhost:4222".
    pub url: String,

    /// Optional username for authentication.
    #[serde(default)]
    pub username: Option<String>,

    /// Optional password for authentication.
    /// For production, prefer environment variables over storing in config files.
    #[serde(default)]
    pub password: Option<String>,

    /// Path to CA certificate (PEM) for verifying the NATS server.
    /// Setting this enables TLS (connection uses `tls://` scheme).
    #[serde(default)]
    pub tls_ca_cert_path: Option<String>,

    /// Path to client certificate (PEM) for mTLS authentication.
    /// Must be paired with `tls_key_path`.
    #[serde(default)]
    pub tls_cert_path: Option<String>,

    /// Path to client private key (PEM) for mTLS authentication.
    /// Must be paired with `tls_cert_path`.
    #[serde(default)]
    pub tls_key_path: Option<String>,

    /// Connection timeout in seconds. Defaults to 10.
    #[serde(default = "NatsConfiguration::default_connection_timeout_secs")]
    pub connection_timeout_secs: u64,

    /// Request timeout in seconds. Defaults to 5.
    #[serde(default = "NatsConfiguration::default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Acknowledgment wait time in seconds before message redelivery. Defaults to 300.
    #[serde(default = "NatsConfiguration::default_ack_wait_secs")]
    pub ack_wait_secs: u64,

    /// Maximum redelivery attempts. None means unlimited. Defaults to 5.
    #[serde(default = "NatsConfiguration::default_max_deliver")]
    pub max_deliver: Option<u32>,

    /// How many messages to buffer per subscription. Defaults to 100.
    ///
    /// This controls the capacity of the internal channel between the NATS fetch loop
    /// and your message handler. When `subscribe()` is called, a background task fetches
    /// messages and queues them in this buffer.
    ///
    /// - **Smaller buffer**: Less memory, but the fetch loop may block waiting for the handler
    /// - **Larger buffer**: More messages pre-fetched, smoother throughput, higher memory usage
    ///
    /// For slow handlers or bursty workloads, consider increasing this value.
    #[serde(default = "NatsConfiguration::default_subscription_buffer_size")]
    pub subscription_buffer_size: usize,

    /// Consumer name for durable subscriptions. Defaults to None (ephemeral consumer).
    ///
    /// **Ephemeral consumers** (`None`): Created on subscribe, destroyed on disconnect.
    /// Messages are only delivered while connected. Good for transient workers or testing.
    ///
    /// **Durable consumers** (`Some("name")`): Persist across restarts. NATS tracks the
    /// last acknowledged message, so reconnecting consumers resume where they left off.
    /// Required for reliable message processing.
    ///
    /// For horizontal scaling, give all instances the same `consumer_name`. NATS will
    /// distribute messages across them (each message delivered to exactly one instance).
    #[serde(default)]
    pub consumer_name: Option<String>,

    /// How many messages to fetch per batch. Higher values improve throughput
    /// but increase memory usage. Defaults to 10.
    #[serde(default = "NatsConfiguration::default_batch_size")]
    pub batch_size: usize,

    /// Whether to auto-create streams on startup. Defaults to true.
    #[serde(default = "NatsConfiguration::default_auto_create_streams")]
    pub auto_create_streams: bool,

    /// Number of stream replicas for fault tolerance. Defaults to 1.
    /// Production should use 3 for fault tolerance.
    #[serde(default = "NatsConfiguration::default_stream_replicas")]
    pub stream_replicas: usize,

    /// Maximum age of messages in seconds before deletion. Defaults to 14400 (4 hours).
    #[serde(default = "NatsConfiguration::default_stream_max_age_secs")]
    pub stream_max_age_secs: Option<u64>,

    /// Maximum bytes per stream before oldest messages are deleted. Defaults to None (unlimited).
    #[serde(default)]
    pub stream_max_bytes: Option<i64>,

    /// Maximum messages per stream. Defaults to None (unlimited).
    #[serde(default)]
    pub stream_max_messages: Option<i64>,

    /// Server-side timeout in seconds for `consume_pending` batch fetch.
    /// Must be long enough for the NATS server to scan through gaps between
    /// matching messages in filtered consumers. Defaults to 5.
    #[serde(default = "NatsConfiguration::default_fetch_expires_secs")]
    pub fetch_expires_secs: u64,
}

impl NatsConfiguration {
    fn default_connection_timeout_secs() -> u64 {
        10
    }

    fn default_request_timeout_secs() -> u64 {
        5
    }

    fn default_ack_wait_secs() -> u64 {
        300
    }

    fn default_max_deliver() -> Option<u32> {
        Some(5)
    }

    fn default_subscription_buffer_size() -> usize {
        100
    }

    fn default_batch_size() -> usize {
        10
    }

    fn default_auto_create_streams() -> bool {
        true
    }

    fn default_stream_replicas() -> usize {
        1
    }

    fn default_stream_max_age_secs() -> Option<u64> {
        Some(14400)
    }

    fn default_fetch_expires_secs() -> u64 {
        5
    }

    /// Returns true when TLS is configured -- either via cert paths or a `tls://` url scheme.
    pub fn tls_enabled(&self) -> bool {
        self.url.starts_with("tls://")
            || self.tls_ca_cert_path.is_some()
            || self.tls_cert_path.is_some()
            || self.tls_key_path.is_some()
    }

    /// Returns the full connection URL with the appropriate scheme.
    ///
    /// Accepts `url` in any of these formats:
    /// - `"host:port"` -- scheme derived from TLS config
    /// - `"nats://host:port"` -- plaintext
    /// - `"tls://host:port"` -- TLS required
    pub fn connection_url(&self) -> String {
        if self.url.starts_with("nats://") || self.url.starts_with("tls://") {
            return self.url.clone();
        }

        let scheme = if self.tls_enabled() { "tls" } else { "nats" };
        format!("{scheme}://{}", self.url)
    }

    /// Validates TLS configuration completeness and file existence.
    ///
    /// Returns `Ok(())` when:
    /// - No TLS paths are configured (plaintext), or
    /// - All configured paths point to existing files and cert/key form a complete pair.
    ///
    /// Returns `Err` when:
    /// - `tls_cert_path` is set without `tls_key_path` (or vice versa)
    /// - Any configured path points to a nonexistent file
    pub fn validate_tls_config(&self) -> Result<(), String> {
        if !self.tls_enabled() {
            return Ok(());
        }

        match (&self.tls_cert_path, &self.tls_key_path) {
            (Some(_), None) => {
                return Err("tls_cert_path is set but tls_key_path is missing".into());
            }
            (None, Some(_)) => {
                return Err("tls_key_path is set but tls_cert_path is missing".into());
            }
            _ => {}
        }

        let checks: [(&str, Option<&String>); 3] = [
            ("tls_ca_cert_path", self.tls_ca_cert_path.as_ref()),
            ("tls_cert_path", self.tls_cert_path.as_ref()),
            ("tls_key_path", self.tls_key_path.as_ref()),
        ];
        for (field, path) in checks {
            if let Some(p) = path.filter(|p| !Path::new(p.as_str()).exists()) {
                return Err(format!("{field}: file not found at '{p}'"));
            }
        }

        Ok(())
    }

    pub fn connection_timeout(&self) -> Duration {
        Duration::from_secs(self.connection_timeout_secs)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    pub fn ack_wait(&self) -> Duration {
        Duration::from_secs(self.ack_wait_secs)
    }

    /// Returns buffer size, clamped to at least 1.
    pub fn subscription_buffer_size(&self) -> usize {
        self.subscription_buffer_size.max(1)
    }

    /// Returns batch size, clamped to at least 1.
    pub fn batch_size(&self) -> usize {
        self.batch_size.max(1)
    }

    pub fn fetch_expires(&self) -> Duration {
        Duration::from_secs(self.fetch_expires_secs.max(1))
    }

    pub fn stream_max_age(&self) -> Option<Duration> {
        self.stream_max_age_secs.map(Duration::from_secs)
    }
}

impl Default for NatsConfiguration {
    fn default() -> Self {
        Self {
            url: "localhost:4222".to_string(),
            username: None,
            password: None,
            tls_ca_cert_path: None,
            tls_cert_path: None,
            tls_key_path: None,
            connection_timeout_secs: Self::default_connection_timeout_secs(),
            request_timeout_secs: Self::default_request_timeout_secs(),
            ack_wait_secs: Self::default_ack_wait_secs(),
            max_deliver: Self::default_max_deliver(),
            subscription_buffer_size: Self::default_subscription_buffer_size(),
            consumer_name: None,
            batch_size: Self::default_batch_size(),
            auto_create_streams: Self::default_auto_create_streams(),
            stream_replicas: Self::default_stream_replicas(),
            stream_max_age_secs: Self::default_stream_max_age_secs(),
            stream_max_bytes: None,
            stream_max_messages: None,
            fetch_expires_secs: Self::default_fetch_expires_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn bare_host_defaults_to_nats_scheme() {
        let config = NatsConfiguration::default();
        assert_eq!(config.connection_url(), "nats://localhost:4222");
        assert!(!config.tls_enabled());
    }

    #[test]
    fn bare_host_uses_tls_scheme_when_ca_set() {
        let config = NatsConfiguration {
            tls_ca_cert_path: Some("/tmp/ca.pem".into()),
            ..Default::default()
        };
        assert_eq!(config.connection_url(), "tls://localhost:4222");
        assert!(config.tls_enabled());
    }

    #[test]
    fn bare_host_uses_tls_scheme_when_client_cert_set() {
        let config = NatsConfiguration {
            tls_cert_path: Some("/tmp/cert.pem".into()),
            tls_key_path: Some("/tmp/key.pem".into()),
            ..Default::default()
        };
        assert_eq!(config.connection_url(), "tls://localhost:4222");
    }

    #[test]
    fn nats_scheme_in_url_is_preserved() {
        let config = NatsConfiguration {
            url: "nats://my-nats:4222".into(),
            ..Default::default()
        };
        assert_eq!(config.connection_url(), "nats://my-nats:4222");
        assert!(!config.tls_enabled());
    }

    #[test]
    fn tls_scheme_in_url_enables_tls() {
        let config = NatsConfiguration {
            url: "tls://secure-nats:4222".into(),
            ..Default::default()
        };
        assert_eq!(config.connection_url(), "tls://secure-nats:4222");
        assert!(config.tls_enabled());
    }

    #[test]
    fn tls_scheme_in_url_not_duplicated_with_cert_paths() {
        let config = NatsConfiguration {
            url: "tls://secure-nats:4222".into(),
            tls_ca_cert_path: Some("/tmp/ca.pem".into()),
            ..Default::default()
        };
        assert_eq!(config.connection_url(), "tls://secure-nats:4222");
    }

    #[test]
    fn validate_no_tls_is_valid() {
        let config = NatsConfiguration::default();
        assert!(config.validate_tls_config().is_ok());
    }

    #[test]
    fn validate_ca_only_is_valid() {
        let ca_file = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_ca_cert_path: Some(ca_file.path().to_str().unwrap().into()),
            ..Default::default()
        };
        assert!(config.validate_tls_config().is_ok());
    }

    #[test]
    fn validate_full_mtls_is_valid() {
        let ca = NamedTempFile::new().unwrap();
        let cert = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_ca_cert_path: Some(ca.path().to_str().unwrap().into()),
            tls_cert_path: Some(cert.path().to_str().unwrap().into()),
            tls_key_path: Some(key.path().to_str().unwrap().into()),
            ..Default::default()
        };
        assert!(config.validate_tls_config().is_ok());
    }

    #[test]
    fn validate_cert_without_key_is_invalid() {
        let cert = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_cert_path: Some(cert.path().to_str().unwrap().into()),
            ..Default::default()
        };
        let err = config.validate_tls_config().unwrap_err();
        assert!(err.contains("tls_key_path is missing"), "{err}");
    }

    #[test]
    fn validate_key_without_cert_is_invalid() {
        let key = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_key_path: Some(key.path().to_str().unwrap().into()),
            ..Default::default()
        };
        let err = config.validate_tls_config().unwrap_err();
        assert!(err.contains("tls_cert_path is missing"), "{err}");
    }

    #[test]
    fn validate_missing_file_is_invalid() {
        let config = NatsConfiguration {
            tls_ca_cert_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        };
        let err = config.validate_tls_config().unwrap_err();
        assert!(err.contains("tls_ca_cert_path"), "{err}");
        assert!(err.contains("file not found"), "{err}");
    }

    #[test]
    fn validate_existing_ca_but_missing_cert_file_is_invalid() {
        let ca = NamedTempFile::new().unwrap();
        let key = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_ca_cert_path: Some(ca.path().to_str().unwrap().into()),
            tls_cert_path: Some("/nonexistent/cert.pem".into()),
            tls_key_path: Some(key.path().to_str().unwrap().into()),
            ..Default::default()
        };
        let err = config.validate_tls_config().unwrap_err();
        assert!(err.contains("tls_cert_path"), "{err}");
    }

    #[test]
    fn deserialize_with_tls_fields() {
        let yaml = r#"
            url: "localhost:4222"
            tls_ca_cert_path: "/etc/nats/ca.pem"
            tls_cert_path: "/etc/nats/client.pem"
            tls_key_path: "/etc/nats/client-key.pem"
        "#;
        let config: NatsConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.tls_ca_cert_path.as_deref(), Some("/etc/nats/ca.pem"));
        assert_eq!(
            config.tls_cert_path.as_deref(),
            Some("/etc/nats/client.pem")
        );
        assert_eq!(
            config.tls_key_path.as_deref(),
            Some("/etc/nats/client-key.pem")
        );
        assert!(config.tls_enabled());
    }

    #[test]
    fn deserialize_without_tls_fields_uses_defaults() {
        let yaml = r#"url: "localhost:4222""#;
        let config: NatsConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert!(config.tls_ca_cert_path.is_none());
        assert!(config.tls_cert_path.is_none());
        assert!(config.tls_key_path.is_none());
        assert!(!config.tls_enabled());
    }

    #[test]
    fn fetch_expires_defaults_to_5s() {
        let yaml = r#"url: "localhost:4222""#;
        let config: NatsConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.fetch_expires(), Duration::from_secs(5));
    }

    #[test]
    fn fetch_expires_clamps_zero_to_1s() {
        let yaml = r#"
            url: "localhost:4222"
            fetch_expires_secs: 0
        "#;
        let config: NatsConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.fetch_expires(), Duration::from_secs(1));
    }
}
