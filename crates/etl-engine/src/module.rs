//! Handlers process messages. Modules group handlers together.
//!
//! ```ignore
//! use etl_engine::module::{Module, Handler, HandlerContext, HandlerError};
//! use etl_engine::types::{Envelope, Topic};
//! use async_trait::async_trait;
//!
//! struct MyHandler;
//!
//! #[async_trait]
//! impl Handler for MyHandler {
//!     fn name(&self) -> &str { "my-handler" }
//!     fn topic(&self) -> Topic { Topic::new("my-stream", "my-subject") }
//!
//!     async fn handle(&self, ctx: HandlerContext, msg: Envelope) -> Result<(), HandlerError> {
//!         // ctx.destination has your writers
//!         // ctx.metrics has the metric collector
//!         // ctx.nats has NATS services for publishing
//!         Ok(())
//!     }
//! }
//!
//! struct MyModule;
//!
//! impl Module for MyModule {
//!     fn name(&self) -> &str { "my-module" }
//!     fn handlers(&self) -> Vec<Box<dyn Handler>> { vec![Box::new(MyHandler)] }
//!     fn entities(&self) -> Vec<Entity> { vec![] }
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    destination::Destination,
    entities::Entity,
    metrics::MetricCollector,
    nats::NatsServices,
    types::{Envelope, Topic},
};

/// Errors that can occur during message handling.
#[derive(Debug, Error)]
pub enum HandlerError {
    /// A general processing error with a descriptive message.
    #[error("processing failed: {0}")]
    Processing(String),

    /// Failed to deserialize the message payload.
    #[error("deserialization failed: {0}")]
    Deserialization(#[from] serde_json::Error),
}

/// Context provided to handlers during message processing.
///
/// Contains shared resources that handlers need to process messages
/// and write results.
#[derive(Clone)]
pub struct HandlerContext {
    /// The destination where processed data should be written.
    pub destination: Arc<dyn Destination>,

    /// The metric collector for recording metrics.
    pub metrics: Arc<dyn MetricCollector>,

    /// NATS services for publishing messages and other NATS operations.
    pub nats: Arc<dyn NatsServices>,
}

impl HandlerContext {
    /// Creates a new handler context with the given resources.
    pub fn new(
        destination: Arc<dyn Destination>,
        metrics: Arc<dyn MetricCollector>,
        nats: Arc<dyn NatsServices>,
    ) -> Self {
        HandlerContext {
            destination,
            metrics,
            nats,
        }
    }
}

/// A message handler that processes events from a specific topic.
///
/// Each handler subscribes to one topic and processes incoming messages.
///
/// # Example
///
/// ```ignore
/// use etl_engine::module::{Handler, HandlerContext, HandlerError};
/// use etl_engine::types::{Envelope, Topic};
/// use async_trait::async_trait;
///
/// struct OrderHandler;
///
/// #[async_trait]
/// impl Handler for OrderHandler {
///     fn name(&self) -> &str {
///         "order-handler"
///     }
///
///     fn topic(&self) -> Topic {
///         Topic::new("orders", "orders.placed")
///     }
///
///     async fn handle(&self, ctx: HandlerContext, msg: Envelope) -> Result<(), HandlerError> {
///         let order: Order = msg.to_event()
///             .map_err(|e| HandlerError::Processing(e.to_string()))?;
///
///         // Process order and write to destination
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Handler: Send + Sync {
    /// Returns the unique name of this handler.
    ///
    /// Used for metrics labeling and debugging. Should be a stable identifier.
    fn name(&self) -> &str;

    /// Returns the topic this handler subscribes to.
    fn topic(&self) -> Topic;

    /// Processes a message from the subscribed topic.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared resources for processing (e.g., destination writers, metrics)
    /// * `message` - The message envelope containing the payload
    ///
    /// # Errors
    ///
    /// Returns a [`HandlerError`] if processing fails. The engine will
    /// nack the message, allowing it to be retried.
    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError>;
}

/// A module that groups related handlers and entities.
///
/// Modules package handlers that process messages along with entity
/// definitions that describe the data model.
///
/// # Example
///
/// ```ignore
/// use etl_engine::module::{Module, Handler};
/// use etl_engine::entities::Entity;
///
/// struct AnalyticsModule;
///
/// impl Module for AnalyticsModule {
///     fn name(&self) -> &str {
///         "analytics"
///     }
///
///     fn handlers(&self) -> Vec<Box<dyn Handler>> {
///         vec![
///             Box::new(PageViewHandler),
///             Box::new(ClickHandler),
///         ]
///     }
///
///     fn entities(&self) -> Vec<Entity> {
///         vec![
///             // Define nodes and edges this module produces
///         ]
///     }
/// }
/// ```
pub trait Module: Send + Sync {
    /// Returns the unique name of this module.
    ///
    /// The module name is used for configuration (e.g., per-module concurrency limits)
    /// and logging purposes.
    fn name(&self) -> &str;

    /// Returns the handlers provided by this module.
    fn handlers(&self) -> Vec<Box<dyn Handler>>;

    /// Returns the entity definitions for data this module produces.
    fn entities(&self) -> Vec<Entity>;
}

struct RegisteredHandler {
    handler: Arc<dyn Handler>,
    module_name: Arc<str>,
}

/// A registry for managing modules and their handlers.
///
/// The registry collects handlers from registered modules and provides
/// lookup functionality for the engine to dispatch messages.
///
/// # Example
///
/// ```ignore
/// use etl_engine::module::ModuleRegistry;
/// use std::sync::Arc;
///
/// let registry = Arc::new(ModuleRegistry::default());
///
/// registry.register_module(&MyModule);
/// registry.register_module(&AnotherModule);
///
/// // Pass to engine
/// let engine = Engine::new(broker, registry, destination);
/// ```
#[derive(Default)]
pub struct ModuleRegistry {
    handlers_by_topic: RwLock<HashMap<Topic, Vec<RegisteredHandler>>>,
}

impl ModuleRegistry {
    /// Registers a module, adding its handlers to the registry.
    ///
    /// All handlers from the module will be associated with the module's name
    /// for concurrency control purposes.
    pub fn register_module(&self, module: &dyn Module) {
        let module_name: Arc<str> = module.name().into();

        let mut handlers_by_topic = self.handlers_by_topic.write();
        for handler in module.handlers() {
            let topic = handler.topic();
            let registered = RegisteredHandler {
                handler: Arc::from(handler),
                module_name: module_name.clone(),
            };
            handlers_by_topic.entry(topic).or_default().push(registered);
        }
    }

    /// Returns all handlers registered for a given topic.
    ///
    /// Each handler is returned with its associated module name for
    /// worker pool permit acquisition.
    pub fn handlers_for(&self, topic: &Topic) -> Vec<(Arc<dyn Handler>, Arc<str>)> {
        self.handlers_by_topic
            .read()
            .get(topic)
            .map(|handlers| {
                handlers
                    .iter()
                    .map(|h| (h.handler.clone(), h.module_name.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns all unique topics that have registered handlers.
    pub fn topics(&self) -> Vec<Topic> {
        let mut topics: Vec<_> = self.handlers_by_topic.read().keys().cloned().collect();
        topics.sort_by(|a, b| (&*a.stream, &*a.subject).cmp(&(&*b.stream, &*b.subject)));
        topics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{MockHandler, MockModule};

    #[test]
    fn test_registry_operations() {
        let registry = ModuleRegistry::default();
        let m1 = MockModule::new("m1").with_handler(MockHandler::new("stream1", "subject1"));
        let m2 = MockModule::new("m2").with_handler(MockHandler::new("stream1", "subject1"));

        registry.register_module(&m1);
        registry.register_module(&m2);

        let topic = Topic::new("stream1", "subject1");
        let handlers = registry.handlers_for(&topic);
        assert_eq!(handlers.len(), 2);
        assert_eq!(&*handlers[0].1, "m1");

        let unknown = Topic::new("unknown", "unknown");
        assert!(registry.handlers_for(&unknown).is_empty());

        assert_eq!(registry.topics(), vec![topic]);
    }

    #[tokio::test]
    async fn test_concurrent_registry_reads() {
        let registry = Arc::new(ModuleRegistry::default());

        registry
            .register_module(&MockModule::new("m0").with_handler(MockHandler::new("stream", "s0")));
        registry
            .register_module(&MockModule::new("m1").with_handler(MockHandler::new("stream", "s1")));
        registry
            .register_module(&MockModule::new("m2").with_handler(MockHandler::new("stream", "s2")));

        let t0 = Topic::new("stream", "s0");
        let t1 = Topic::new("stream", "s1");
        let t2 = Topic::new("stream", "s2");

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
