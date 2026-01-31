#![allow(clippy::doc_lazy_continuation)]

#[path = "proto/gitaly.rs"]
mod gitaly_proto;

pub mod proto {
    pub use super::gitaly_proto::*;
}

mod auth;
mod client;
mod config;
mod error;
mod traits;

pub use client::GitalyClient;
pub use config::GitalyRepositoryConfig;
pub use error::GitalyError;
pub use traits::RepositorySource;
