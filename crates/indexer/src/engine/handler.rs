//! Handlers process messages from specific topics.
//!
//! ```ignore
//! use etl_engine::handler::{Handler, HandlerContext, HandlerError};
//! use etl_engine::types::{Envelope, Subscription};
//! use async_trait::async_trait;
//!
//! struct MyHandler;
//!
//! #[async_trait]
//! impl Handler for MyHandler {
//!     fn name(&self) -> &str { "my-handler" }
//!     fn subscription(&self) -> Subscription { Subscription::new("my-stream", "my-subject") }
//!
//!     async fn handle(&self, ctx: HandlerContext, msg: Envelope) -> Result<(), HandlerError> {
//!         // ctx.destination has your writers
//!         // ctx.nats has NATS services for publishing
//!         Ok(())
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use thiserror::Error;

use super::types::{Envelope, Subscription};
use crate::{
    indexing_status::IndexingStatusStore,
    locking::LockService,
    nats::{NatsServices, ProgressNotifier},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermanentAction {
    DeadLetter,
    Drop,
}

/// Errors that can occur during message handling.
#[derive(Debug, Error)]
pub enum HandlerError {
    /// A general processing error with a descriptive message.
    #[error("processing failed: {0}")]
    Processing(String),

    /// A deterministic failure that will never succeed on retry.
    #[error("permanent failure: {message}")]
    Permanent {
        message: String,
        action: PermanentAction,
    },

    /// Failed to deserialize the message payload.
    #[error("deserialization failed: {0}")]
    Deserialization(#[from] serde_json::Error),
}

impl HandlerError {
    pub fn error_kind(&self) -> &'static str {
        match self {
            HandlerError::Processing(_) => "processing",
            HandlerError::Permanent { .. } => "permanent",
            HandlerError::Deserialization(_) => "deserialization",
        }
    }

    pub fn is_permanent(&self) -> bool {
        matches!(self, Self::Permanent { .. } | Self::Deserialization(_))
    }
}

/// Errors that can occur during handler initialization.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct HandlerInitError(#[from] Box<dyn std::error::Error + Send + Sync>);

impl HandlerInitError {
    /// Creates a new handler initialization error from any error type.
    pub fn new<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self(Box::new(error))
    }
}

/// Context provided to handlers during message processing.
///
/// Contains shared resources that handlers need to process messages
/// and write results.
#[derive(Clone)]
pub struct HandlerContext {
    pub nats: Arc<dyn NatsServices>,
    pub lock_service: Arc<dyn LockService>,
    pub progress: ProgressNotifier,
    pub indexing_status: Arc<IndexingStatusStore>,
}

impl HandlerContext {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        lock_service: Arc<dyn LockService>,
        progress: ProgressNotifier,
        indexing_status: Arc<IndexingStatusStore>,
    ) -> Self {
        HandlerContext {
            nats,
            lock_service,
            progress,
            indexing_status,
        }
    }
}

/// A message handler that processes events from a specific topic.
///
/// Each handler subscribes to one topic and processes incoming messages.
/// Engine behavior (retries, concurrency, DLQ) is configured on the
/// [`Subscription`] returned by [`subscription()`](Handler::subscription),
/// not on the handler itself.
#[async_trait]
pub trait Handler: Send + Sync {
    /// Returns the unique name of this handler.
    ///
    /// Used for metrics labeling, config lookup, and debugging. Should be a stable identifier.
    fn name(&self) -> &str;

    /// Returns the subscription this handler listens on.
    ///
    /// The subscription carries message-level processing policy (retry,
    /// concurrency group, DLQ) applied at registration time.
    fn subscription(&self) -> Subscription;

    /// Processes a message from the subscribed topic.
    ///
    /// # Errors
    ///
    /// Returns a [`HandlerError`] if processing fails. The engine will
    /// retry or ack based on the subscription's config.
    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError>;

    /// Whether the engine should acquire a worker pool permit before calling
    /// [`handle`](Handler::handle). Defaults to `true`.
    ///
    /// Handlers that manage their own concurrency (e.g. pipelined handlers
    /// with separate fetch and process semaphores) return `false` so the
    /// engine does not gate them behind the shared worker pool.
    fn requires_worker_pool(&self) -> bool {
        true
    }
}

/// A registry for managing handlers and their topic subscriptions.
///
/// The registry collects handlers and provides lookup functionality
/// for the engine to dispatch messages.
#[derive(Default)]
pub struct HandlerRegistry {
    handlers_by_subscription: RwLock<HashMap<Subscription, Vec<Arc<dyn Handler>>>>,
}

impl HandlerRegistry {
    /// Registers a handler, adding it to the registry under its subscription.
    pub fn register_handler(&self, handler: Box<dyn Handler>) {
        let mut handlers_by_subscription = self.handlers_by_subscription.write();
        let subscription = handler.subscription();
        handlers_by_subscription
            .entry(subscription)
            .or_default()
            .push(Arc::from(handler));
    }

    /// Returns all handlers registered for a given subscription.
    pub fn handlers_for(&self, subscription: &Subscription) -> Vec<Arc<dyn Handler>> {
        self.handlers_by_subscription
            .read()
            .get(subscription)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns all unique subscriptions that have registered handlers.
    pub fn subscriptions(&self) -> Vec<Subscription> {
        let mut subscriptions: Vec<_> = self
            .handlers_by_subscription
            .read()
            .keys()
            .cloned()
            .collect();
        subscriptions.sort_by(|a, b| (&*a.stream, &*a.subject).cmp(&(&*b.stream, &*b.subject)));
        subscriptions
    }

    /// Finds a handler by name across all subscriptions.
    pub fn find_by_name(&self, name: &str) -> Option<Arc<dyn Handler>> {
        self.handlers_by_subscription
            .read()
            .values()
            .flatten()
            .find(|handler| handler.name() == name)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::MockHandler;

    #[test]
    fn test_registry_operations() {
        let registry = HandlerRegistry::default();

        registry.register_handler(Box::new(MockHandler::new("stream1", "subject1")));
        registry.register_handler(Box::new(MockHandler::new("stream1", "subject1")));

        let subscription = Subscription::new("stream1", "subject1");
        let handlers = registry.handlers_for(&subscription);
        assert_eq!(handlers.len(), 2);

        let unknown = Subscription::new("unknown", "unknown");
        assert!(registry.handlers_for(&unknown).is_empty());

        assert_eq!(registry.subscriptions(), vec![subscription]);
    }

    #[tokio::test]
    async fn test_concurrent_registry_reads() {
        let registry = Arc::new(HandlerRegistry::default());

        registry.register_handler(Box::new(MockHandler::new("stream", "s0")));
        registry.register_handler(Box::new(MockHandler::new("stream", "s1")));
        registry.register_handler(Box::new(MockHandler::new("stream", "s2")));

        let t0 = Subscription::new("stream", "s0");
        let t1 = Subscription::new("stream", "s1");
        let t2 = Subscription::new("stream", "s2");

        let handles: Vec<_> = (0..50)
            .map(|_| {
                let r = registry.clone();
                let t0 = t0.clone();
                let t1 = t1.clone();
                let t2 = t2.clone();
                tokio::spawn(async move {
                    let _ = r.handlers_for(&t0);
                    let _ = r.handlers_for(&t1);
                    let _ = r.handlers_for(&t2);
                })
            })
            .collect();

        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                futures::future::join_all(handles)
            )
            .await
            .is_ok()
        );
    }
}
