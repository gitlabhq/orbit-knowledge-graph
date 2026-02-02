//! Mailbox module for extending the GitLab Knowledge Graph with custom nodes and edges.
//!
//! This crate provides a plugin system that enables customers to define custom
//! node and edge types, register plugins with schemas, and ingest data via HTTP.

pub mod auth;
pub mod error;
pub mod handler;
pub mod http;
pub mod schema_generator;
pub mod storage;
pub mod types;
pub mod validation;

mod module;

pub use error::MailboxError;
pub use module::MailboxModule;
pub use types::{
    EdgeDefinition, EdgePayload, MailboxMessage, NodeDefinition, NodePayload, NodeReference,
    Plugin, PluginSchema, PropertyDefinition, PropertyType,
};
