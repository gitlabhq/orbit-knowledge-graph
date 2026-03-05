use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::debug;

use crate::nats::{KvPutOptions, KvPutResult};

pub const INDEXING_LOCKS_BUCKET: &str = "indexing_locks";

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("lock operation failed: {0}")]
    Backend(String),
}

#[async_trait]
pub trait LockService: Send + Sync {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<bool, LockError>;
    async fn release(&self, key: &str) -> Result<(), LockError>;
}

pub struct NatsLockService {
    nats: std::sync::Arc<dyn crate::nats::NatsServices>,
}

impl NatsLockService {
    pub fn new(nats: std::sync::Arc<dyn crate::nats::NatsServices>) -> Self {
        Self { nats }
    }
}

#[async_trait]
impl LockService for NatsLockService {
    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<bool, LockError> {
        let options = KvPutOptions::create_with_ttl(ttl);
        match self
            .nats
            .kv_put(INDEXING_LOCKS_BUCKET, key, Bytes::new(), options)
            .await
        {
            Ok(KvPutResult::Success(_)) => {
                debug!(key, "lock acquired");
                Ok(true)
            }
            Ok(KvPutResult::AlreadyExists | KvPutResult::RevisionMismatch) => {
                debug!(key, "lock contention, already held");
                Ok(false)
            }
            Err(e) => {
                debug!(key, error = %e, "lock acquisition error");
                Err(LockError::Backend(e.to_string()))
            }
        }
    }

    async fn release(&self, key: &str) -> Result<(), LockError> {
        let result = self
            .nats
            .kv_delete(INDEXING_LOCKS_BUCKET, key)
            .await
            .map_err(|e| LockError::Backend(e.to_string()));
        debug!(key, "lock released");
        result
    }
}
