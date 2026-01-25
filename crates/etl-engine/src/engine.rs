//! The engine subscribes to topics, dispatches messages to handlers, and acks/nacks.
//!
//! # Example
//!
//! ```ignore
//! use etl_engine::engine::EngineBuilder;
//! use etl_engine::module::ModuleRegistry;
//! use etl_engine::configuration::EngineConfiguration;
//! use std::sync::Arc;
//!
//! let registry = Arc::new(ModuleRegistry::default());
//! registry.register_module(&my_module);
//!
//! let engine = EngineBuilder::new(
//!     Box::new(my_broker),
//!     registry,
//!     Arc::new(my_destination),
//! ).build();
//!
//! engine.run(&EngineConfiguration::default()).await?;
//!
//! // From another task:
//! engine.stop();
//! ```
//!
//! # Metrics
//!
//! To collect metrics, pass a [`MetricCollector`](crate::metrics::MetricCollector) to the builder:
//!
//! ```ignore
//! let engine = EngineBuilder::new(broker, registry, destination)
//!     .metrics(Arc::new(my_metrics_backend))
//!     .build();
//! ```
//!
//! Handlers receive the collector via [`HandlerContext`](crate::module::HandlerContext).

use std::sync::Arc;

use futures::StreamExt;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::configuration::EngineConfiguration;
use crate::destination::Destination;
use crate::message_broker::{BrokerError, Envelope, MessageBroker};
use crate::metrics::{MetricCollector, NoopMetricCollector};
use crate::module::{Handler, HandlerContext, HandlerError, ModuleRegistry};
use crate::worker_pool::WorkerPool;

/// Errors that can occur during engine operation.
#[derive(Debug, Error)]
pub enum EngineError {
    /// An error from the message broker.
    #[error("broker error: {0}")]
    Broker(#[from] BrokerError),

    /// An error from a message handler.
    #[error("handler error: {0}")]
    Handler(#[from] HandlerError),
}

/// Builder for constructing an [`Engine`].
///
/// Required components are passed to `new()`. Optional components can be set
/// via builder methods before calling `build()`.
///
/// # Example
///
/// ```ignore
/// use etl_engine::engine::EngineBuilder;
/// use std::sync::Arc;
///
/// let engine = EngineBuilder::new(broker, registry, destination)
///     .metrics(Arc::new(my_metrics))  // optional
///     .build();
/// ```
pub struct EngineBuilder {
    broker: Box<dyn MessageBroker>,
    registry: Arc<ModuleRegistry>,
    destination: Arc<dyn Destination>,
    metrics: Arc<dyn MetricCollector>,
}

impl EngineBuilder {
    /// Creates a new engine builder with the required components.
    pub fn new(
        broker: Box<dyn MessageBroker>,
        registry: Arc<ModuleRegistry>,
        destination: Arc<dyn Destination>,
    ) -> Self {
        Self {
            broker,
            registry,
            destination,
            metrics: Arc::new(NoopMetricCollector),
        }
    }

    /// Sets the metric collector.
    ///
    /// If not called, metrics are discarded.
    pub fn metrics(mut self, metrics: Arc<dyn MetricCollector>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Builds the engine.
    pub fn build(self) -> Engine {
        Engine {
            broker: self.broker,
            registry: self.registry,
            destination: self.destination,
            metrics: self.metrics,
            cancel: CancellationToken::new(),
        }
    }
}

/// The ETL engine that processes messages through registered handlers.
///
/// The engine subscribes to topics based on registered handlers, processes
/// incoming messages, and manages acknowledgments. It uses a worker pool
/// to control concurrency.
///
/// # Creating an engine
///
/// Use [`EngineBuilder`]:
///
/// ```ignore
/// let engine = EngineBuilder::new(broker, registry, destination).build();
/// ```
///
/// # Lifecycle
///
/// 1. Create with [`EngineBuilder`]
/// 2. Start with [`Engine::run`]
/// 3. Stop with [`Engine::stop`]
pub struct Engine {
    broker: Box<dyn MessageBroker>,
    registry: Arc<ModuleRegistry>,
    destination: Arc<dyn Destination>,
    metrics: Arc<dyn MetricCollector>,
    cancel: CancellationToken,
}

impl Engine {
    /// Starts the engine and processes messages until stopped.
    ///
    /// Returns when stopped via [`Engine::stop`] or when all subscriptions end.
    pub async fn run(&self, configuration: &EngineConfiguration) -> Result<(), EngineError> {
        let topics = self.registry.topics();
        if topics.is_empty() {
            return Ok(());
        }

        let worker_pool = Arc::new(WorkerPool::new(configuration));
        let tasks: Vec<_> = topics
            .into_iter()
            .map(|topic| self.listen(topic, worker_pool.clone()))
            .collect();
        futures::future::try_join_all(tasks).await?;

        Ok(())
    }

    async fn listen(&self, topic: String, worker_pool: Arc<WorkerPool>) -> Result<(), EngineError> {
        let mut subscription = self.broker.subscribe(&topic).await?;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break Ok(()),
                Some(msg) = subscription.next() => {
                    let msg = msg?;
                    let handlers = self.registry.handlers_for(&topic);
                    let context = HandlerContext::new(
                        self.destination.clone(),
                        self.metrics.clone(),
                    );

                    match dispatch(&handlers, context, msg.envelope.clone(), &worker_pool).await {
                        Ok(_)  => msg.ack().await?,
                        Err(_) => msg.nack().await?,
                    }
                }
            }
        }
    }

    /// Signals the engine to stop processing.
    ///
    /// In-flight messages will complete before shutdown.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

async fn dispatch(
    handlers: &[(Arc<dyn Handler>, Arc<str>)],
    context: HandlerContext,
    envelope: Envelope,
    worker_pool: &WorkerPool,
) -> Result<(), HandlerError> {
    for (handler, module_name) in handlers {
        let _permit = worker_pool
            .acquire(module_name)
            .await
            .expect("worker pool semaphore closed unexpectedly");
        handler.handle(context.clone(), envelope.clone()).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::module::HandlerError;
    use crate::testkit::{
        MockHandler, MockMessageBroker, MockModule, TestEngineBuilder, TestEnvelopeFactory,
    };
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    async fn run_and_stop(
        engine: std::sync::Arc<super::Engine>,
        config: crate::configuration::EngineConfiguration,
        wait_ms: u64,
    ) {
        let engine_clone = engine.clone();
        let run_handle = tokio::spawn(async move { engine_clone.run(&config).await });
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
        engine.stop();
        run_handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_run_with_no_handlers_returns_immediately() {
        let (engine, config) = TestEngineBuilder::new().build();
        assert!(engine.run(&config).await.is_ok());
    }

    #[tokio::test]
    async fn test_message_dispatch_and_ack() {
        let broker = MockMessageBroker::new();
        broker.queue_messages("topic", vec![TestEnvelopeFactory::simple("payload")]);

        let handler = MockHandler::new("topic");
        let handler_invocations = handler.invocations_arc();

        let (engine, config) = TestEngineBuilder::new()
            .with_broker(broker)
            .with_module(&MockModule::new("test-module").with_handler(handler))
            .build();

        run_and_stop(engine, config, 50).await;
        assert_eq!(handler_invocations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_nack_on_handler_failure() {
        let broker = MockMessageBroker::new();
        broker.queue_messages("topic", vec![TestEnvelopeFactory::simple("payload")]);

        let failing_handler =
            MockHandler::new("topic").with_error(HandlerError::Processing("error".into()));
        let handler_invocations = failing_handler.invocations_arc();

        let (engine, config) = TestEngineBuilder::new()
            .with_broker(broker)
            .with_module(&MockModule::new("test-module").with_handler(failing_handler))
            .build();

        run_and_stop(engine, config, 50).await;
        assert_eq!(handler_invocations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_nack_is_called_on_handler_failure() {
        use crate::testkit::QueuedMessage;

        let broker = MockMessageBroker::new();
        let queued_message = QueuedMessage::new(TestEnvelopeFactory::simple("payload"));
        let ack_handle = queued_message.ack_handle().clone();
        broker.queue_message_with_handle("topic", queued_message);

        let failing_handler =
            MockHandler::new("topic").with_error(HandlerError::Processing("error".into()));

        let (engine, config) = TestEngineBuilder::new()
            .with_broker(broker)
            .with_module(&MockModule::new("test-module").with_handler(failing_handler))
            .build();

        run_and_stop(engine, config, 50).await;

        assert!(
            ack_handle.was_nacked(),
            "Expected nack to be called on handler failure"
        );
        assert!(
            !ack_handle.was_acked(),
            "Expected ack to NOT be called on handler failure"
        );
    }

    #[tokio::test]
    async fn test_stop_cancels_processing() {
        let slow_handler = MockHandler::new("topic").with_delay(Duration::from_secs(10));

        let (engine, config) = TestEngineBuilder::new()
            .with_module(&MockModule::new("test-module").with_handler(slow_handler))
            .build();

        let engine_clone = engine.clone();
        let run_handle = tokio::spawn(async move { engine_clone.run(&config).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        engine.stop();

        assert!(
            tokio::time::timeout(Duration::from_millis(500), run_handle)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_multiple_handlers_and_messages() {
        let broker = MockMessageBroker::new();
        broker.queue_messages("topic", TestEnvelopeFactory::batch(3));

        let first_handler = MockHandler::new("topic");
        let second_handler = MockHandler::new("topic");
        let first_handler_invocations = first_handler.invocations_arc();
        let second_handler_invocations = second_handler.invocations_arc();

        let (engine, config) = TestEngineBuilder::new()
            .with_broker(broker)
            .with_module(
                &MockModule::new("test-module")
                    .with_handler(first_handler)
                    .with_handler(second_handler),
            )
            .build();

        run_and_stop(engine, config, 100).await;
        assert_eq!(first_handler_invocations.load(Ordering::SeqCst), 3);
        assert_eq!(second_handler_invocations.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_multiple_topics() {
        let broker = MockMessageBroker::new();
        broker.queue_messages("topic-a", vec![TestEnvelopeFactory::simple("payload-a")]);
        broker.queue_messages("topic-b", vec![TestEnvelopeFactory::simple("payload-b")]);

        let topic_a_handler = MockHandler::new("topic-a");
        let topic_b_handler = MockHandler::new("topic-b");
        let topic_a_invocations = topic_a_handler.invocations_arc();
        let topic_b_invocations = topic_b_handler.invocations_arc();

        let (engine, config) = TestEngineBuilder::new()
            .with_broker(broker)
            .with_module(
                &MockModule::new("test-module")
                    .with_handler(topic_a_handler)
                    .with_handler(topic_b_handler),
            )
            .build();

        run_and_stop(engine, config, 50).await;
        assert_eq!(topic_a_invocations.load(Ordering::SeqCst), 1);
        assert_eq!(topic_b_invocations.load(Ordering::SeqCst), 1);
    }
}
