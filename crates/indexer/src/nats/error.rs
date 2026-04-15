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

    #[error("object store operation failed for '{bucket}': {message}")]
    ObjectStore { bucket: String, message: String },

    #[error("object store put failed for '{bucket}/{name}': {message}")]
    ObjectStorePut {
        bucket: String,
        name: String,
        message: String,
    },

    #[error("object store get failed for '{bucket}/{name}': {message}")]
    ObjectStoreGet {
        bucket: String,
        name: String,
        message: String,
    },

    #[error("object store delete failed for '{bucket}/{name}': {message}")]
    ObjectStoreDelete {
        bucket: String,
        name: String,
        message: String,
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
