//! NATS message handler for mailbox messages.

mod arrow_converter;
mod deduplication;
mod id_generator;
mod mailbox_handler;

pub use arrow_converter::ArrowConverter;
pub use deduplication::DeduplicationStore;
pub use id_generator::generate_node_id;
pub use mailbox_handler::{MAILBOX_STREAM, MAILBOX_SUBJECT, MailboxHandler};
