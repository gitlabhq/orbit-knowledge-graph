//! NATS-specific error types.

use thiserror::Error;

use crate::types::SerializationError;

/// Errors that can occur during NATS broker operations.
#[derive(Debug, Error)]
pub enum NatsError {
    /// Failed to serialize or deserialize a message.
    #[error("serialization failed: {0}")]
    Serialization(#[from] SerializationError),

    /// Failed to publish a message to the broker.
    #[error("failed to publish message: {0}")]
    Publish(String),

    /// Failed to subscribe to a topic.
    #[error("failed to subscribe to topic: {0}")]
    Subscribe(String),

    /// Failed to acknowledge a message.
    #[error("failed to acknowledge message: {0}")]
    Ack(String),

    /// Failed to negatively acknowledge a message.
    #[error("failed to reject message: {0}")]
    Nack(String),

    /// Failed to connect to the broker.
    #[error("connection error: {0}")]
    Connection(String),

    /// Stream not found.
    #[error("stream '{stream}' not found: {source}")]
    StreamNotFound {
        stream: String,
        #[source]
        source: async_nats::error::Error<async_nats::jetstream::context::GetStreamErrorKind>,
    },
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
