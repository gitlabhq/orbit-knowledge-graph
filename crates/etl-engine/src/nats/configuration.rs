//! NATS broker configuration.

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

    /// Connection timeout in seconds. Defaults to 10.
    #[serde(default = "NatsConfiguration::default_connection_timeout_secs")]
    pub connection_timeout_secs: u64,

    /// Request timeout in seconds. Defaults to 5.
    #[serde(default = "NatsConfiguration::default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Acknowledgment wait time in seconds before message redelivery. Defaults to 30.
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
        30
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
    /// - `NATS_STREAM_MAX_MESSAGES`: Maximum messages per stream
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("NATS_URL").unwrap_or_else(|_| "host.docker.internal:4222".into()),
            username: std::env::var("NATS_USERNAME").ok(),
            password: std::env::var("NATS_PASSWORD").ok(),
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
            url: "host.docker.internal:4222".to_string(),
            username: None,
            password: None,
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
