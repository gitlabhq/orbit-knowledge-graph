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
//!     let subscription = Subscription::new("stream", "subject");
//!     ctx.nats.publish(&subscription, &Envelope::new(&derived)?).await?;
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

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use circuit_breaker::CircuitBreaker;
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

pub struct CircuitBreakingNatsServices {
    inner: Arc<dyn NatsServices>,
    breaker: CircuitBreaker,
}

impl CircuitBreakingNatsServices {
    pub fn new(inner: Arc<dyn NatsServices>, breaker: CircuitBreaker) -> Self {
        Self { inner, breaker }
    }
}

fn is_nats_service_error(error: &NatsError) -> bool {
    !matches!(error, NatsError::PublishDuplicate)
}

fn map_open_error(service: &'static str) -> NatsError {
    NatsError::Connection(format!("circuit breaker open for {service}"))
}

#[async_trait]
impl NatsServices for CircuitBreakingNatsServices {
    async fn publish(
        &self,
        subscription: &Subscription,
        envelope: &Envelope,
    ) -> Result<(), NatsError> {
        self.breaker
            .call_with_filter(
                || self.inner.publish(subscription, envelope),
                is_nats_service_error,
            )
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => map_open_error(service),
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }

    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        self.breaker
            .call_with_filter(|| self.inner.kv_get(bucket, key), is_nats_service_error)
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => map_open_error(service),
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        self.breaker
            .call_with_filter(
                || self.inner.kv_put(bucket, key, value, options),
                is_nats_service_error,
            )
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => map_open_error(service),
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.breaker
            .call_with_filter(|| self.inner.kv_delete(bucket, key), is_nats_service_error)
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => map_open_error(service),
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.breaker
            .call_with_filter(|| self.inner.kv_keys(bucket), is_nats_service_error)
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => map_open_error(service),
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }

    async fn consume_pending(
        &self,
        subscription: &Subscription,
        batch_size: usize,
    ) -> Result<Vec<NatsMessage>, NatsError> {
        self.breaker
            .call_with_filter(
                || self.inner.consume_pending(subscription, batch_size),
                is_nats_service_error,
            )
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => map_open_error(service),
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }
}
