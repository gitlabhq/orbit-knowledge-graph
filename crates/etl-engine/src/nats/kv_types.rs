//! NATS KV store types.

use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct KvPutOptions {
    pub ttl: Option<Duration>,
    pub create_only: bool,
    pub expected_revision: Option<u64>,
}

impl KvPutOptions {
    pub fn create_only() -> Self {
        Self {
            create_only: true,
            ..Default::default()
        }
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl: Some(ttl),
            ..Default::default()
        }
    }

    pub fn create_with_ttl(ttl: Duration) -> Self {
        Self {
            ttl: Some(ttl),
            create_only: true,
            ..Default::default()
        }
    }

    pub fn update_revision(revision: u64) -> Self {
        Self {
            expected_revision: Some(revision),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvPutResult {
    Success(u64),
    AlreadyExists,
    RevisionMismatch,
}

impl KvPutResult {
    pub fn is_success(&self) -> bool {
        matches!(self, KvPutResult::Success(_))
    }

    pub fn revision(&self) -> Option<u64> {
        match self {
            KvPutResult::Success(rev) => Some(*rev),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: bytes::Bytes,
    pub revision: u64,
}
