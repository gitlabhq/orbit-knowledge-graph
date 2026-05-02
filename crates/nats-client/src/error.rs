use circuit_breaker::CircuitBreakerError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NatsError {
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

    #[error("circuit open for service {service}")]
    CircuitOpen { service: &'static str },
}

impl NatsError {
    pub fn is_transient(&self) -> bool {
        use async_nats::jetstream::context::{CreateStreamErrorKind, GetStreamErrorKind};

        match self {
            Self::Connection(_)
            | Self::Publish(_)
            | Self::Subscribe(_)
            | Self::Ack(_)
            | Self::Nack(_)
            | Self::KvBucket { .. }
            | Self::KvGet { .. }
            | Self::KvPut { .. }
            | Self::KvDelete { .. }
            | Self::KvKeys { .. } => true,

            Self::StreamNotFound { source, .. } => matches!(
                source.kind(),
                GetStreamErrorKind::Request | GetStreamErrorKind::JetStream(_)
            ),
            Self::StreamCreationFailed { source, .. } => matches!(
                source.kind(),
                CreateStreamErrorKind::TimedOut
                    | CreateStreamErrorKind::JetStreamUnavailable
                    | CreateStreamErrorKind::Response
                    | CreateStreamErrorKind::JetStream(_)
            ),

            Self::PublishDuplicate | Self::CircuitOpen { .. } => false,
        }
    }

    pub(crate) fn from_circuit_breaker(error: CircuitBreakerError<Self>) -> Self {
        match error {
            CircuitBreakerError::Open { service } => Self::CircuitOpen { service },
            CircuitBreakerError::Inner(inner) => inner,
        }
    }
}

pub(crate) fn map_connect_error(error: async_nats::ConnectError) -> NatsError {
    NatsError::Connection(error.to_string())
}
