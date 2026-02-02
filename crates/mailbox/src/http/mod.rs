//! HTTP endpoints for mailbox plugin management and message ingestion.

mod request_types;
mod response_types;
mod routes;

pub use request_types::{RegisterPluginRequest, SubmitMessageRequest};
pub use response_types::{
    ErrorResponse, MessageAcceptedResponse, PluginInfoResponse, PluginListResponse,
};
pub use routes::{MailboxState, create_mailbox_router};
