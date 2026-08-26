//! A `NatsServices` that counts publishes instead of sending them.
//!
//! The testkit mock keeps every published envelope, which at a million
//! dispatches would dominate the very measurement being taken.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use indexer::nats::{NatsMessage, NatsServices};
use indexer::types::{Envelope, Subscription};
use nats_client::{KvEntry, KvPutOptions, KvPutResult, NatsError};

pub struct CountingNats {
    published: AtomicU64,
    published_bytes: AtomicU64,
    publish_delay: Option<std::time::Duration>,
}

impl CountingNats {
    pub fn new(publish_delay: Option<std::time::Duration>) -> Self {
        Self {
            published: AtomicU64::new(0),
            published_bytes: AtomicU64::new(0),
            publish_delay,
        }
    }

    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }

    pub fn published_bytes(&self) -> u64 {
        self.published_bytes.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl NatsServices for CountingNats {
    async fn publish(
        &self,
        _subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError> {
        self.published.fetch_add(1, Ordering::Relaxed);
        self.published_bytes
            .fetch_add(envelope.payload.len() as u64, Ordering::Relaxed);
        if let Some(delay) = self.publish_delay {
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    async fn kv_get(&self, _bucket: &str, _key: &str) -> Result<Option<KvEntry>, NatsError> {
        Ok(None)
    }

    async fn kv_put(
        &self,
        _bucket: &str,
        _key: &str,
        _value: Bytes,
        _options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        Ok(KvPutResult::Success(1))
    }

    async fn kv_delete(&self, _bucket: &str, _key: &str) -> Result<(), NatsError> {
        Ok(())
    }

    async fn kv_keys(&self, _bucket: &str) -> Result<Vec<String>, NatsError> {
        Ok(Vec::new())
    }

    async fn consume_pending(
        &self,
        _subscription: &Subscription,
        _batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError> {
        Ok(Vec::new())
    }
}
