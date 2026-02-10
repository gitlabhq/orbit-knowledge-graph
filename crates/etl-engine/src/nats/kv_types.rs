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

/// Configuration for a NATS KV bucket.
///
/// Used with [`NatsBroker::ensure_kv_bucket_exists`](super::NatsBroker::ensure_kv_bucket_exists)
/// to create buckets with specific settings at startup.
#[derive(Debug, Clone, Default)]
pub struct KvBucketConfig {
    /// When set, enables per-message TTL on the bucket (requires NATS 2.11+).
    /// Delete markers are cleaned up after this duration.
    /// A zero duration enables per-message TTL without automatic marker cleanup.
    pub limit_markers: Option<Duration>,
}

impl KvBucketConfig {
    /// Creates a bucket config with per-message TTL enabled (requires NATS 2.11+).
    ///
    /// This allows individual `create_with_ttl` operations to set key-level expiry.
    pub fn with_per_message_ttl() -> Self {
        Self {
            limit_markers: Some(Duration::ZERO),
        }
    }
}
