#![allow(clippy::doc_lazy_continuation)]

#[path = "proto/siphon.v1.rs"]
mod siphon_proto;

pub mod proto {
    pub use super::siphon_proto::*;
}

pub use proto::{
    LogicalReplicationEventType, LogicalReplicationEvents, ReplicationEvent, Value,
    replication_event, value,
};
