//! NATS broker configuration.

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::env::{env_var_opt, env_var_or};

/// NATS connection settings.
///
/// Matches siphon's QueuingConfig fields.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

    /// Maximum age of messages in seconds before deletion. Defaults to None (unlimited).
    #[serde(default)]
    pub stream_max_age_secs: Option<u64>,

    /// Maximum bytes per stream before oldest messages are deleted. Defaults to None (unlimited).
    #[serde(default)]
    pub stream_max_bytes: Option<i64>,

    /// Maximum messages per stream. Defaults to None (unlimited).
    #[serde(default)]
    pub stream_max_messages: Option<i64>,
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

    /// Returns true when any TLS path is configured.
    pub fn tls_enabled(&self) -> bool {
        self.tls_ca_cert_path.is_some()
            || self.tls_cert_path.is_some()
            || self.tls_key_path.is_some()
    }

    /// Returns `"tls"` when TLS is configured, `"nats"` otherwise.
    pub fn scheme(&self) -> &str {
        if self.tls_enabled() { "tls" } else { "nats" }
    }

    /// Checks that all configured TLS paths point to existing files.
    /// Returns a list of `(field_name, path)` for paths that could not be found.
    pub fn validate_tls_paths(&self) -> Vec<(&'static str, &str)> {
        let checks: [(&str, Option<&String>); 3] = [
            ("tls_ca_cert_path", self.tls_ca_cert_path.as_ref()),
            ("tls_cert_path", self.tls_cert_path.as_ref()),
            ("tls_key_path", self.tls_key_path.as_ref()),
        ];

        checks
            .into_iter()
            .filter_map(|(name, path)| {
                path.filter(|p| !Path::new(p.as_str()).exists())
                    .map(|p| (name, p.as_str()))
            })
            .collect()
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

    pub fn stream_max_age(&self) -> Option<Duration> {
        self.stream_max_age_secs.map(Duration::from_secs)
    }

    /// Creates configuration from environment variables.
    ///
    /// Uses defaults for any unset variables:
    /// - `NATS_URL`: Server address (default: "localhost:4222")
    /// - `NATS_USERNAME`: Optional username
    /// - `NATS_PASSWORD`: Optional password
    /// - `NATS_CONSUMER_NAME`: Optional consumer name for durable subscriptions
    /// - `NATS_AUTO_CREATE_STREAMS`: Whether to auto-create streams (default: true)
    /// - `NATS_STREAM_REPLICAS`: Number of stream replicas (default: 1)
    /// - `NATS_STREAM_MAX_AGE_SECS`: Maximum age of messages in seconds
    /// - `NATS_STREAM_MAX_BYTES`: Maximum bytes per stream
    /// - `NATS_TLS_CA_CERT_PATH`: Path to CA certificate (PEM)
    /// - `NATS_TLS_CERT_PATH`: Path to client certificate (PEM)
    /// - `NATS_TLS_KEY_PATH`: Path to client private key (PEM)
    /// - `NATS_STREAM_MAX_MESSAGES`: Maximum messages per stream
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("NATS_URL").unwrap_or_else(|_| "localhost:4222".into()),
            username: std::env::var("NATS_USERNAME").ok(),
            password: std::env::var("NATS_PASSWORD").ok(),
            tls_ca_cert_path: std::env::var("NATS_TLS_CA_CERT_PATH").ok(),
            tls_cert_path: std::env::var("NATS_TLS_CERT_PATH").ok(),
            tls_key_path: std::env::var("NATS_TLS_KEY_PATH").ok(),
            consumer_name: std::env::var("NATS_CONSUMER_NAME").ok(),
            auto_create_streams: env_var_or(
                "NATS_AUTO_CREATE_STREAMS",
                Self::default_auto_create_streams(),
            ),
            stream_replicas: env_var_or("NATS_STREAM_REPLICAS", Self::default_stream_replicas()),
            stream_max_age_secs: env_var_opt("NATS_STREAM_MAX_AGE_SECS"),
            stream_max_bytes: env_var_opt("NATS_STREAM_MAX_BYTES"),
            stream_max_messages: env_var_opt("NATS_STREAM_MAX_MESSAGES"),
            ..Default::default()
        }
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
            stream_max_age_secs: None,
            stream_max_bytes: None,
            stream_max_messages: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn scheme_is_nats_by_default() {
        let config = NatsConfiguration::default();
        assert_eq!(config.scheme(), "nats");
        assert!(!config.tls_enabled());
    }

    #[test]
    fn scheme_is_tls_when_ca_cert_set() {
        let config = NatsConfiguration {
            tls_ca_cert_path: Some("/tmp/ca.pem".into()),
            ..Default::default()
        };
        assert_eq!(config.scheme(), "tls");
        assert!(config.tls_enabled());
    }

    #[test]
    fn scheme_is_tls_when_client_cert_set() {
        let config = NatsConfiguration {
            tls_cert_path: Some("/tmp/cert.pem".into()),
            tls_key_path: Some("/tmp/key.pem".into()),
            ..Default::default()
        };
        assert_eq!(config.scheme(), "tls");
    }

    #[test]
    fn validate_tls_paths_empty_when_no_tls() {
        let config = NatsConfiguration::default();
        assert!(config.validate_tls_paths().is_empty());
    }

    #[test]
    fn validate_tls_paths_reports_missing_files() {
        let config = NatsConfiguration {
            tls_ca_cert_path: Some("/nonexistent/ca.pem".into()),
            tls_cert_path: Some("/nonexistent/cert.pem".into()),
            tls_key_path: Some("/nonexistent/key.pem".into()),
            ..Default::default()
        };
        let missing = config.validate_tls_paths();
        assert_eq!(missing.len(), 3);
        assert_eq!(missing[0].0, "tls_ca_cert_path");
        assert_eq!(missing[1].0, "tls_cert_path");
        assert_eq!(missing[2].0, "tls_key_path");
    }

    #[test]
    fn validate_tls_paths_skips_existing_files() {
        let ca_file = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_ca_cert_path: Some(ca_file.path().to_str().unwrap().into()),
            tls_cert_path: Some("/nonexistent/cert.pem".into()),
            ..Default::default()
        };
        let missing = config.validate_tls_paths();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].0, "tls_cert_path");
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
        assert_eq!(config.scheme(), "tls");
    }

    #[test]
    fn deserialize_without_tls_fields_uses_defaults() {
        let yaml = r#"url: "localhost:4222""#;
        let config: NatsConfiguration = serde_yaml::from_str(yaml).unwrap();
        assert!(config.tls_ca_cert_path.is_none());
        assert!(config.tls_cert_path.is_none());
        assert!(config.tls_key_path.is_none());
        assert_eq!(config.scheme(), "nats");
    }
}
