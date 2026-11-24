//! Mockable NATS services for handlers.
//!
//! The [`NatsServices`] trait provides a mockable interface for handlers
//! that need to interact with NATS (e.g., publishing events, KV operations).
//!
//! # Usage
//!
//! Handlers receive `NatsServices` via [`HandlerContext`](crate::handler::HandlerContext):
//!
//! ```no_run
//! # use indexer::handler::{HandlerContext, HandlerError};
//! # use indexer::types::{Envelope, Event, Subscription};
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct DerivedEvent;
//! # impl Event for DerivedEvent {
//! #     fn subscription() -> Subscription { Subscription::new("stream", "subject") }
//! # }
//! # async fn handle(ctx: HandlerContext, _envelope: Envelope) -> Result<(), HandlerError> {
//! let derived = DerivedEvent { /* ... */ };
//! let envelope = Envelope::new(&derived)
//!     .map_err(|e| HandlerError::Processing(format!("serialize failed: {e}")))?;
//! ctx.nats
//!     .publish(&DerivedEvent::subscription(), &envelope)
//!     .await
//!     .map_err(|e| HandlerError::Processing(format!("publish failed: {e}")))?;
//! # Ok(())
//! # }
//! ```
//!
//! # KV Operations
//!
//! Handlers can use KV operations for caching. For locking, prefer
//! [`LockGuard`](crate::locking::LockGuard), which wraps `LockService`. The service encodes
//! expiration in the value and handles re-acquire over a delete marker:
//!
//! ```no_run
//! # use std::time::Duration;
//! # use indexer::handler::{HandlerContext, HandlerError};
//! # use indexer::locking::LockGuard;
//! # async fn handle(ctx: HandlerContext) -> Result<(), HandlerError> {
//! let ttl = Duration::from_secs(300);
//! let acquired = LockGuard::acquire(ctx.lock_service.clone(), "my-key", ttl)
//!     .await
//!     .map_err(|e| HandlerError::Processing(format!("lock acquire failed: {e}")))?;
//! let guard = match acquired {
//!     Ok(guard) => guard,
//!     Err(held) => {
//!         return Err(HandlerError::Deferred {
//!             reason: "lock my-key held by another indexer".into(),
//!             delay: held.time_left.unwrap_or(ttl),
//!         });
//!     }
//! };
//! // ...
//! # let _: &LockGuard = &guard;
//! # Ok(())
//! # }
//! ```
//!
//! # Testing
//!
//! Use [`MockNatsServices`](crate::testkit::MockNatsServices) for fast unit tests
//! (requires the `testkit` feature outside this crate's own tests):
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use indexer::handler::{Handler, HandlerContext, HandlerError};
//! # use indexer::types::Envelope;
//! # use indexer::nats::ProgressNotifier;
//! # use indexer::testkit::{MockLockService, MockNatsServices};
//! # async fn check(handler: &dyn Handler, envelope: Envelope) -> Result<(), HandlerError> {
//! let mock_nats = MockNatsServices::new();
//! let ctx = HandlerContext::new(
//!     Arc::new(mock_nats.clone()),
//!     Arc::new(MockLockService::new()),
//!     ProgressNotifier::noop(),
//! );
//! handler.handle(ctx, envelope).await?;
//! assert_eq!(mock_nats.get_published().len(), 1);
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use nats_client::{KvEntry, KvPutOptions, KvPutResult};

use crate::types::{Envelope, Subscription};
use nats_client::NatsError;

use super::message::NatsMessage;

#[async_trait]
pub trait NatsServices: Send + Sync {
    async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError>;

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

    async fn consume_pending(
        &self,
        subscription: &Subscription,
        batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError>;
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
    async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError> {
        self.broker.publish(subscription, envelope).await
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

    async fn consume_pending(
        &self,
        subscription: &Subscription,
        batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError> {
        self.broker.consume_pending(subscription, batch_size).await
    }
}
