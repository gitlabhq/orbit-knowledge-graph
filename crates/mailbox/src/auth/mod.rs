//! Authentication for mailbox plugins.

mod api_key;
mod extractor;

pub use api_key::{hash_api_key, verify_api_key};
pub use extractor::PluginAuth;
