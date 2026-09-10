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
    pub username: Option<String>,

    /// Optional password for authentication.
    /// For production, mount it as a secret file at `/etc/secrets/nats/password`.
    pub password: Option<String>,

    /// Path to CA certificate (PEM) for verifying the NATS server.
    /// Setting this enables TLS (connection uses `tls://` scheme).
    pub tls_ca_cert_path: Option<String>,

    /// Path to client certificate (PEM) for mTLS authentication.
    /// Must be paired with `tls_key_path`.
    pub tls_cert_path: Option<String>,

    /// Path to client private key (PEM) for mTLS authentication.
    /// Must be paired with `tls_cert_path`.
    pub tls_key_path: Option<String>,

    /// Connection timeout in seconds.
    pub connection_timeout_secs: u64,

    /// Request timeout in seconds.
    pub request_timeout_secs: u64,

    /// Acknowledgment wait time in seconds before message redelivery.
    pub ack_wait_secs: u64,

    /// Maximum redelivery attempts. None means unlimited.
    pub max_deliver: Option<u32>,

    /// How many messages to buffer per subscription.
    ///
    /// This controls the capacity of the internal channel between the NATS fetch loop
    /// and your message handler. When `subscribe()` is called, a background task fetches
    /// messages and queues them in this buffer.
    ///
    /// - **Smaller buffer**: Less memory, but the fetch loop may block waiting for the handler
    /// - **Larger buffer**: More messages pre-fetched, smoother throughput, higher memory usage
    ///
    /// For slow handlers or bursty workloads, consider increasing this value.
    pub subscription_buffer_size: usize,

    /// Consumer name for durable subscriptions.
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
    pub consumer_name: Option<String>,

    /// How many messages to fetch per batch. Higher values improve throughput
    /// but increase memory usage.
    pub batch_size: usize,

    /// Whether to auto-create streams on startup.
    pub auto_create_streams: bool,

    /// Number of stream replicas for fault tolerance.
    /// Production should use 3 for fault tolerance.
    pub stream_replicas: usize,

    /// Maximum age of messages in seconds before deletion.
    pub stream_max_age_secs: Option<u64>,

    /// Maximum bytes per stream before oldest messages are deleted.
    pub stream_max_bytes: Option<i64>,

    /// Maximum messages per stream.
    pub stream_max_messages: Option<i64>,

    /// Server-side timeout in seconds for `consume_pending` batch fetch.
    /// Must be long enough for the NATS server to scan through gaps between
    /// matching messages in filtered consumers.
    pub fetch_expires_secs: u64,

    /// Inactive threshold for versioned durable consumers in seconds.
    /// After this duration with no activity, NATS auto-deletes the consumer.
    /// Applied to Siphon dispatch consumers and to subscribe consumers on the
    /// versioned work streams, so a retired release's consumers reap
    /// themselves and stop vetoing release GC. A connected consumer's fetch
    /// loop keeps it active. Clamped to a minimum of 60 seconds.
    #[schemars(range(min = 60))]
    pub consumer_inactive_threshold_secs: u64,

    /// How long another release's streams must show no activity (creation,
    /// publishes, attached consumers) before a starting dispatcher deletes
    /// them. A live release's dispatcher publishes every minute, so activity
    /// doubles as liveness. Clamped to a minimum of 600 seconds.
    #[schemars(range(min = 600))]
    pub release_gc_idle_threshold_secs: u64,
}

impl NatsConfiguration {
    pub fn consumer_inactive_threshold(&self) -> Duration {
        Duration::from_secs(self.consumer_inactive_threshold_secs.max(60))
    }

    pub fn release_gc_idle_threshold(&self) -> Duration {
        Duration::from_secs(self.release_gc_idle_threshold_secs.max(600))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppConfig;
    use tempfile::NamedTempFile;

    fn defaults() -> NatsConfiguration {
        AppConfig::embedded_defaults().nats
    }

    #[test]
    fn bare_host_defaults_to_nats_scheme() {
        let config = defaults();
        assert_eq!(config.connection_url(), "nats://localhost:4222");
        assert!(!config.tls_enabled());
    }

    #[test]
    fn bare_host_uses_tls_scheme_when_ca_set() {
        let config = NatsConfiguration {
            tls_ca_cert_path: Some("/tmp/ca.pem".into()),
            ..defaults()
        };
        assert_eq!(config.connection_url(), "tls://localhost:4222");
        assert!(config.tls_enabled());
    }

    #[test]
    fn bare_host_uses_tls_scheme_when_client_cert_set() {
        let config = NatsConfiguration {
            tls_cert_path: Some("/tmp/cert.pem".into()),
            tls_key_path: Some("/tmp/key.pem".into()),
            ..defaults()
        };
        assert_eq!(config.connection_url(), "tls://localhost:4222");
    }

    #[test]
    fn nats_scheme_in_url_is_preserved() {
        let config = NatsConfiguration {
            url: "nats://my-nats:4222".into(),
            ..defaults()
        };
        assert_eq!(config.connection_url(), "nats://my-nats:4222");
        assert!(!config.tls_enabled());
    }

    #[test]
    fn tls_scheme_in_url_enables_tls() {
        let config = NatsConfiguration {
            url: "tls://secure-nats:4222".into(),
            ..defaults()
        };
        assert_eq!(config.connection_url(), "tls://secure-nats:4222");
        assert!(config.tls_enabled());
    }

    #[test]
    fn tls_scheme_in_url_not_duplicated_with_cert_paths() {
        let config = NatsConfiguration {
            url: "tls://secure-nats:4222".into(),
            tls_ca_cert_path: Some("/tmp/ca.pem".into()),
            ..defaults()
        };
        assert_eq!(config.connection_url(), "tls://secure-nats:4222");
    }

    #[test]
    fn validate_no_tls_is_valid() {
        let config = defaults();
        assert!(config.validate_tls_config().is_ok());
    }

    #[test]
    fn validate_ca_only_is_valid() {
        let ca_file = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_ca_cert_path: Some(ca_file.path().to_str().unwrap().into()),
            ..defaults()
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
            ..defaults()
        };
        assert!(config.validate_tls_config().is_ok());
    }

    #[test]
    fn validate_cert_without_key_is_invalid() {
        let cert = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_cert_path: Some(cert.path().to_str().unwrap().into()),
            ..defaults()
        };
        let err = config.validate_tls_config().unwrap_err();
        assert!(err.contains("tls_key_path is missing"), "{err}");
    }

    #[test]
    fn validate_key_without_cert_is_invalid() {
        let key = NamedTempFile::new().unwrap();
        let config = NatsConfiguration {
            tls_key_path: Some(key.path().to_str().unwrap().into()),
            ..defaults()
        };
        let err = config.validate_tls_config().unwrap_err();
        assert!(err.contains("tls_cert_path is missing"), "{err}");
    }

    #[test]
    fn validate_missing_file_is_invalid() {
        let config = NatsConfiguration {
            tls_ca_cert_path: Some("/nonexistent/ca.pem".into()),
            ..defaults()
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
            ..defaults()
        };
        let err = config.validate_tls_config().unwrap_err();
        assert!(err.contains("tls_cert_path"), "{err}");
    }

    #[test]
    fn defaults_omit_tls_fields() {
        let config = defaults();
        assert!(config.tls_ca_cert_path.is_none());
        assert!(config.tls_cert_path.is_none());
        assert!(config.tls_key_path.is_none());
        assert!(!config.tls_enabled());
    }

    #[test]
    fn fetch_expires_clamps_zero_to_1s() {
        let config = NatsConfiguration {
            fetch_expires_secs: 0,
            ..defaults()
        };
        assert_eq!(config.fetch_expires(), Duration::from_secs(1));
    }
}
