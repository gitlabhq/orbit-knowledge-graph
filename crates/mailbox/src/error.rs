//! Error types for the mailbox module.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MailboxError {
    #[error("validation failed: {0}")]
    Validation(String),

    #[error("plugin not found: {plugin_id}")]
    PluginNotFound { plugin_id: String },

    #[error("plugin already exists: {plugin_id} in namespace {namespace_id}")]
    PluginAlreadyExists {
        plugin_id: String,
        namespace_id: i64,
    },

    #[error("authentication failed: {0}")]
    Authentication(String),

    #[error("schema generation failed: {0}")]
    SchemaGeneration(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("message processing failed: {0}")]
    Processing(String),

    #[error("node not found: {node_kind} with external_id {external_id}")]
    NodeNotFound {
        node_kind: String,
        external_id: String,
    },

    #[error("{0}")]
    Internal(String),
}

impl MailboxError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage(message.into())
    }

    pub fn processing(message: impl Into<String>) -> Self {
        Self::Processing(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}
