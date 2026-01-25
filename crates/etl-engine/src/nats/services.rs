//! Mockable NATS services for handlers.
//!
//! The [`NatsServices`] trait provides a mockable interface for handlers
//! that need to interact with NATS (e.g., publishing events).
//!
//! # Usage
//!
//! Handlers receive `NatsServices` via [`HandlerContext`](crate::module::HandlerContext):
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

use crate::types::{Envelope, Topic};

use super::error::NatsError;

/// Mockable interface for NATS operations used by handlers.
///
/// This trait abstracts NATS operations that handlers might need,
/// allowing for easy mocking in unit tests while the engine uses
/// the real NATS broker directly.
#[async_trait]
pub trait NatsServices: Send + Sync {
    async fn publish(&self, topic: &Topic, envelope: &Envelope) -> Result<(), NatsError>;
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
}
