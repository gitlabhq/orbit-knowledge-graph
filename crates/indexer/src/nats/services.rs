//! Mockable NATS services for handlers.
//!
//! The [`NatsServices`] trait provides a mockable interface for handlers
//! that need to interact with NATS (e.g., publishing events, KV operations).
//!
//! # Usage
//!
//! Handlers receive `NatsServices` via [`HandlerContext`](crate::handler::HandlerContext):
//!
//! ```ignore
//! async fn handle(&self, ctx: HandlerContext, envelope: Envelope) -> Result<(), HandlerError> {
//!     let derived = DerivedEvent { /* ... */ };
//!     let topic = Topic::new("stream", "subject");
//!     ctx.nats.publish(&topic, &Envelope::new(&derived)?).await?;
//!     Ok(())
//! }
//! ```
//!
//! # KV Operations
//!
//! Handlers can use KV operations for caching and distributed locking:
//!
//! ```ignore
//! async fn handle(&self, ctx: HandlerContext, envelope: Envelope) -> Result<(), HandlerError> {
//!     // Acquire a lock with TTL
//!     let options = KvPutOptions::create_with_ttl(Duration::from_secs(300));
//!     match ctx.nats.kv_put("locks", "my-key", Bytes::new(), options).await? {
//!         KvPutResult::Success(_) => { /* lock acquired */ }
//!         KvPutResult::AlreadyExists => { /* lock held by another */ }
//!         _ => {}
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Testing
//!
//! Use [`MockNatsServices`](crate::testkit::MockNatsServices) for fast unit tests:
//!
//! ```ignore
//! let mock_nats = MockNatsServices::new();
//! let ctx = HandlerContext::new(destination, metrics, Arc::new(mock_nats.clone()));
//!
//! handler.handle(ctx, envelope).await?;
//!
//! assert_eq!(mock_nats.get_published().len(), 1);
//! ```

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Envelope, Topic};

use super::error::NatsError;
use super::kv_types::{KvEntry, KvPutOptions, KvPutResult};

/// Mockable interface for NATS operations used by handlers.
///
/// This trait abstracts NATS operations that handlers might need,
/// allowing for easy mocking in unit tests while the engine uses
/// the real NATS broker directly.
#[async_trait]
pub trait NatsServices: Send + Sync {
    async fn publish(&self, topic: &Topic, envelope: &Envelope) -> Result<(), NatsError>;

    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError>;

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError>;

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError>;

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError>;
}

pub struct NatsServicesImpl {
    broker: std::sync::Arc<super::broker::NatsBroker>,
}

impl NatsServicesImpl {
    pub fn new(broker: std::sync::Arc<super::broker::NatsBroker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl NatsServices for NatsServicesImpl {
    async fn publish(&self, topic: &Topic, envelope: &Envelope) -> Result<(), NatsError> {
        self.broker.publish(topic, envelope).await
    }

    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        self.broker.kv_get(bucket, key).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        self.broker.kv_put(bucket, key, value, options).await
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.broker.kv_delete(bucket, key).await
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.broker.kv_keys(bucket).await
    }
}
