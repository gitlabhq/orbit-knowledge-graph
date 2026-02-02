//! Core domain types for the mailbox module.

mod message;
mod plugin;
mod property_type;
mod schema;

pub use message::{EdgePayload, EdgeReference, MailboxMessage, NodePayload, NodeReference};
pub use plugin::{Plugin, PluginInfo};
pub use property_type::PropertyType;
pub use schema::{EdgeDefinition, NodeDefinition, PluginSchema, PropertyDefinition};
