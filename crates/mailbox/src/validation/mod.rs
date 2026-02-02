//! Schema and message validation for mailbox plugins.

mod message_validator;
mod schema_validator;

pub use message_validator::MessageValidator;
pub use schema_validator::SchemaValidator;
