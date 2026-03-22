#![allow(clippy::doc_lazy_continuation)]

#[path = "proto/gitaly.rs"]
mod gitaly_proto;

pub mod proto {
    pub use super::gitaly_proto::*;
}
