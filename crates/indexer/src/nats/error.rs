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

impl From<nats_client::NatsError> for NatsError {
    fn from(error: nats_client::NatsError) -> Self {
        match error {
            nats_client::NatsError::Publish(msg) => NatsError::Publish(msg),
            nats_client::NatsError::PublishDuplicate => NatsError::PublishDuplicate,
            nats_client::NatsError::Subscribe(msg) => NatsError::Subscribe(msg),
            nats_client::NatsError::Ack(msg) => NatsError::Ack(msg),
            nats_client::NatsError::Nack(msg) => NatsError::Nack(msg),
            nats_client::NatsError::Connection(msg) => NatsError::Connection(msg),
            nats_client::NatsError::StreamNotFound { stream, source } => {
                NatsError::StreamNotFound { stream, source }
            }
            nats_client::NatsError::StreamCreationFailed { stream, source } => {
                NatsError::StreamCreationFailed { stream, source }
            }
            nats_client::NatsError::KvBucket { bucket, message } => {
                NatsError::KvBucket { bucket, message }
            }
            nats_client::NatsError::KvGet {
                bucket,
                key,
                message,
            } => NatsError::KvGet {
                bucket,
                key,
                message,
            },
            nats_client::NatsError::KvPut {
                bucket,
                key,
                message,
            } => NatsError::KvPut {
                bucket,
                key,
                message,
            },
            nats_client::NatsError::KvDelete {
                bucket,
                key,
                message,
            } => NatsError::KvDelete {
                bucket,
                key,
                message,
            },
            nats_client::NatsError::KvKeys { bucket, message } => {
                NatsError::KvKeys { bucket, message }
            }
        }
    }
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
