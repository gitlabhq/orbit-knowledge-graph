//! NATS-specific error types.

use thiserror::Error;

use crate::types::SerializationError;

#[derive(Debug, Error)]
pub enum NatsError {
    #[error("serialization failed: {0}")]
    Serialization(#[from] SerializationError),

    #[error("failed to publish message: {0}")]
    Publish(String),

    #[error("publish rejected: message already exists for subject")]
    PublishDuplicate,

    #[error("failed to subscribe to topic: {0}")]
    Subscribe(String),

    #[error("failed to acknowledge message: {0}")]
    Ack(String),

    #[error("failed to reject message: {0}")]
    Nack(String),

    #[error("connection error: {0}")]
    Connection(String),

    #[error("stream '{stream}' not found: {source}")]
    StreamNotFound {
        stream: String,
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::context::GetStreamErrorKind>,
    },

    #[error("failed to create stream '{stream}': {source}")]
    StreamCreationFailed {
        stream: String,
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::context::CreateStreamErrorKind>,
    },

    #[error("KV bucket operation failed for '{bucket}': {message}")]
    KvBucket { bucket: String, message: String },

    #[error("KV get failed for '{bucket}/{key}': {message}")]
    KvGet {
        bucket: String,
        key: String,
        message: String,
    },

    #[error("KV put failed for '{bucket}/{key}': {message}")]
    KvPut {
        bucket: String,
        key: String,
        message: String,
    },

    #[error("KV delete failed for '{bucket}/{key}': {message}")]
    KvDelete {
        bucket: String,
        key: String,
        message: String,
    },

    #[error("KV keys listing failed for '{bucket}': {message}")]
    KvKeys { bucket: String, message: String },
}

impl NatsError {
    /// Static variant name, for use as a metric label.
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::Serialization(_) => "serialization",
            Self::Publish(_) => "publish",
            Self::PublishDuplicate => "publish_duplicate",
            Self::Subscribe(_) => "subscribe",
            Self::Ack(_) => "ack",
            Self::Nack(_) => "nack",
            Self::Connection(_) => "connection",
            Self::StreamNotFound { .. } => "stream_not_found",
            Self::StreamCreationFailed { .. } => "stream_creation_failed",
            Self::KvBucket { .. } => "kv_bucket",
            Self::KvGet { .. } => "kv_get",
            Self::KvPut { .. } => "kv_put",
            Self::KvDelete { .. } => "kv_delete",
            Self::KvKeys { .. } => "kv_keys",
        }
    }

    /// Whether this error is likely to resolve on its own given a bit of time.
    ///
    /// Transient classes recognised:
    /// - JetStream stream-offline (NATS error code 10118)
    /// - JetStream meta-layer timeouts ("jetstream request timed out")
    /// - Subscribe-path 503 responses during meta-leader handoff
    ///
    /// Non-transient errors stay fail-fast: config mistakes, auth failures,
    /// serialization bugs, etc.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::StreamCreationFailed { source, .. } => {
                let message = source.to_string();
                contains_transient_marker(&message)
            }
            Self::StreamNotFound { source, .. } => {
                let message = source.to_string();
                contains_transient_marker(&message)
            }
            Self::Subscribe(message) | Self::Connection(message) => {
                contains_transient_marker(message)
            }
            _ => false,
        }
    }
}

fn contains_transient_marker(message: &str) -> bool {
    // JetStream replies `10118` when a stream is offline because the meta
    // leader has not yet caught up; `10053` shows up during consumer-leader
    // handoff. Both resolve once the cluster settles.
    const TRANSIENT_MARKERS: &[&str] = &[
        "10118",
        "10053",
        "stream is offline",
        "no responders",
        "jetstream request timed out",
        "request timeout",
        "timed out waiting for response",
    ];
    let lower = message.to_ascii_lowercase();
    if TRANSIENT_MARKERS.iter().any(|m| lower.contains(m)) {
        return true;
    }
    // Subscribe path surfaces bare "503, None" on meta-leader blips.
    lower.contains("503, none") || lower.contains("503,none")
}

pub(crate) fn map_connect_error(error: async_nats::ConnectError) -> NatsError {
    NatsError::Connection(error.to_string())
}

pub(crate) fn map_subscribe_error<E: std::fmt::Display>(error: E) -> NatsError {
    NatsError::Subscribe(error.to_string())
}

pub(crate) fn map_ack_error(error: async_nats::Error) -> NatsError {
    NatsError::Ack(error.to_string())
}

pub(crate) fn map_nack_error(error: async_nats::Error) -> NatsError {
    NatsError::Nack(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_503_is_transient() {
        let err = NatsError::Subscribe(
            "error while processing messages from the stream: 503, None".to_string(),
        );
        assert!(err.is_transient());
    }

    #[test]
    fn subscribe_generic_is_not_transient() {
        let err = NatsError::Subscribe("permission denied".to_string());
        assert!(!err.is_transient());
    }

    #[test]
    fn publish_is_not_transient_even_when_resembling_timeout() {
        // Publish-path timeouts are handled at a different layer (nack/redeliver).
        let err = NatsError::Publish("jetstream request timed out".to_string());
        assert!(!err.is_transient());
    }

    #[test]
    fn variant_names_are_stable_labels() {
        let err = NatsError::Connection("x".into());
        assert_eq!(err.variant_name(), "connection");
    }
}
