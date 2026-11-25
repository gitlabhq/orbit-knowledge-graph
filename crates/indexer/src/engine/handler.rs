use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;

use super::types::{Envelope, Subscription};
use crate::{
    locking::LockService,
    nats::{NatsServices, ProgressNotifier},
};

#[derive(Clone, Default)]
pub struct WorkSlot(Arc<Mutex<Option<OwnedSemaphorePermit>>>);

impl WorkSlot {
    pub fn release(&self) {
        self.0.lock().take();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermanentAction {
    DeadLetter,
    Drop,
}

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("processing failed: {0}")]
    Processing(String),

    #[error("backpressure: {0}")]
    Backpressure(String),

    #[error("deferred for {delay:?}: {reason}")]
    Deferred { reason: String, delay: Duration },

    #[error("permanent failure: {message}")]
    Permanent {
        message: String,
        action: PermanentAction,
    },

    #[error("deserialization failed: {0}")]
    Deserialization(#[from] serde_json::Error),
}

impl From<crate::engine::retry::RetryExhausted> for HandlerError {
    fn from(e: crate::engine::retry::RetryExhausted) -> Self {
        HandlerError::Processing(e.to_string())
    }
}

impl HandlerError {
    pub fn error_kind(&self) -> &'static str {
        match self {
            HandlerError::Processing(_) => "processing",
            HandlerError::Backpressure(_) => "backpressure",
            HandlerError::Deferred { .. } => "deferred",
            HandlerError::Permanent { .. } => "permanent",
            HandlerError::Deserialization(_) => "deserialization",
        }
    }

    pub fn prefix_message_with(self, step: impl std::fmt::Display) -> Self {
        match self {
            HandlerError::Processing(message) => {
                HandlerError::Processing(format!("{step}: {message}"))
            }
            HandlerError::Backpressure(message) => {
                HandlerError::Backpressure(format!("{step}: {message}"))
            }
            other => other,
        }
    }

    pub fn dead_letter(message: impl Into<String>) -> Self {
        HandlerError::Permanent {
            message: message.into(),
            action: PermanentAction::DeadLetter,
        }
    }

    pub fn drop(message: impl Into<String>) -> Self {
        HandlerError::Permanent {
            message: message.into(),
            action: PermanentAction::Drop,
        }
    }
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct HandlerInitError(#[from] Box<dyn std::error::Error + Send + Sync>);

impl HandlerInitError {
    pub fn new<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self(Box::new(error))
    }
}

#[derive(Clone)]
pub struct HandlerContext {
    pub nats: Arc<dyn NatsServices>,
    pub lock_service: Arc<dyn LockService>,
    pub progress: ProgressNotifier,
    pub work: WorkSlot,
    pub redelivered_on_failure: bool,
}

impl HandlerContext {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        lock_service: Arc<dyn LockService>,
        progress: ProgressNotifier,
    ) -> Self {
        HandlerContext {
            nats,
            lock_service,
            progress,
            work: WorkSlot::default(),
            redelivered_on_failure: true,
        }
    }

    pub fn with_work_slot(mut self, permit: Option<OwnedSemaphorePermit>) -> Self {
        self.work = WorkSlot(Arc::new(Mutex::new(permit)));
        self
    }

    pub fn with_redelivery(mut self, redelivered_on_failure: bool) -> Self {
        self.redelivered_on_failure = redelivered_on_failure;
        self
    }
}

#[async_trait]
pub trait Handler: Send + Sync {
    fn name(&self) -> &str;

    fn subscription(&self) -> Subscription;

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError>;

    fn requires_worker_pool(&self) -> bool {
        true
    }
}

#[derive(Default)]
pub struct HandlerRegistry {
    handlers_by_subscription: RwLock<HashMap<Subscription, Vec<Arc<dyn Handler>>>>,
}

impl HandlerRegistry {
    pub fn register_handler(&self, handler: Box<dyn Handler>) {
        let mut handlers_by_subscription = self.handlers_by_subscription.write();
        let subscription = handler.subscription();
        handlers_by_subscription
            .entry(subscription)
            .or_default()
            .push(Arc::from(handler));
    }

    pub fn handlers_for(&self, subscription: &Subscription) -> Vec<Arc<dyn Handler>> {
        self.handlers_by_subscription
            .read()
            .get(subscription)
            .cloned()
            .unwrap_or_default()
    }

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
