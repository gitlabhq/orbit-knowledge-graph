//! The engine subscribes to topics, dispatches messages to handlers, and acks/nacks.
//!
//! # Example
//!
//! ```ignore
//! use etl_engine::engine::EngineBuilder;
//! use etl_engine::module::ModuleRegistry;
//! use etl_engine::nats::{NatsBroker, NatsConfiguration, NatsServicesImpl};
//! use etl_engine::configuration::EngineConfiguration;
//! use std::sync::Arc;
//!
//! let config = NatsConfiguration { url: "localhost:4222".into(), ..Default::default() };
//! let broker = Arc::new(NatsBroker::connect(&config).await?);
//! let nats_services = Arc::new(NatsServicesImpl::new(broker.clone()));
//!
//! let registry = Arc::new(ModuleRegistry::default());
//! registry.register_module(&my_module);
//!
//! let engine = EngineBuilder::new(broker, registry, Arc::new(my_destination))
//!     .nats_services(nats_services)
//!     .build();
//!
//! engine.run(&EngineConfiguration::default()).await?;
//!
//! // From another task:
//! engine.stop();
//! ```

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use opentelemetry::KeyValue;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::configuration::EngineConfiguration;
use crate::destination::Destination;
use crate::metrics::EngineMetrics;
use crate::module::{Handler, HandlerContext, HandlerError, ModuleRegistry};
use crate::nats::{NatsBroker, NatsError, NatsMessage, NatsServices, NatsServicesImpl};
use crate::types::Topic;
use crate::worker_pool::WorkerPool;

/// Errors that can occur during engine operation.
#[derive(Debug, Error)]
pub enum EngineError {
    /// An error from the NATS broker.
    #[error("NATS error: {0}")]
    Nats(#[from] NatsError),

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
/// let engine = EngineBuilder::new(broker, registry, destination).build();
/// ```
pub struct EngineBuilder {
    broker: Arc<NatsBroker>,
    registry: Arc<ModuleRegistry>,
    destination: Arc<dyn Destination>,
    nats_services: Option<Arc<dyn NatsServices>>,
}

impl EngineBuilder {
    /// Creates a new engine builder with the required components.
    pub fn new(
        broker: Arc<NatsBroker>,
        registry: Arc<ModuleRegistry>,
        destination: Arc<dyn Destination>,
    ) -> Self {
        Self {
            broker,
            registry,
            destination,
            nats_services: None,
        }
    }

    /// Sets the NATS services for handlers.
    ///
    /// If not called, a default `NatsServicesImpl` wrapping the broker is used.
    pub fn nats_services(mut self, nats_services: Arc<dyn NatsServices>) -> Self {
        self.nats_services = Some(nats_services);
        self
    }

    /// Builds the engine.
    pub fn build(self) -> Engine {
        let nats_services: Arc<dyn NatsServices> = self
            .nats_services
            .unwrap_or_else(|| Arc::new(NatsServicesImpl::new(self.broker.clone())));

        let metrics = Arc::new(EngineMetrics::new());

        Engine {
            broker: self.broker,
            registry: self.registry,
            destination: self.destination,
            metrics,
            nats_services,
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
    broker: Arc<NatsBroker>,
    registry: Arc<ModuleRegistry>,
    destination: Arc<dyn Destination>,
    metrics: Arc<EngineMetrics>,
    nats_services: Arc<dyn NatsServices>,
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

        self.broker.ensure_streams(&topics).await?;

        let worker_pool = Arc::new(WorkerPool::new(configuration, self.metrics.clone()));
        let tasks: Vec<_> = topics
            .into_iter()
            .map(|topic| self.listen(topic, worker_pool.clone()))
            .collect();
        futures::future::try_join_all(tasks).await?;

        Ok(())
    }

    async fn listen(&self, topic: Topic, worker_pool: Arc<WorkerPool>) -> Result<(), EngineError> {
        let mut subscription = self.broker.subscribe(&topic, self.metrics.clone()).await?;
        let mut inflight = tokio::task::JoinSet::new();
        let topic_label = KeyValue::new("topic", format!("{}.{}", topic.stream, topic.subject));

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                Some(message) = subscription.next() => {
                    let message = message?;
                    inflight.spawn(process_message(
                        message,
                        self.registry.handlers_for(&topic),
                        HandlerContext::new(self.destination.clone(), self.nats_services.clone()),
                        worker_pool.clone(),
                        self.metrics.clone(),
                        topic_label.clone(),
                    ));
                }
            }
        }

        while let Some(result) = inflight.join_next().await {
            if let Err(error) = result {
                warn!(%error, "message processing task panicked");
            }
        }

        Ok(())
    }

    /// Signals the engine to stop processing.
    ///
    /// In-flight messages will complete before shutdown.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

async fn process_message(
    message: NatsMessage,
    handlers: Vec<(Arc<dyn Handler>, Arc<str>)>,
    context: HandlerContext,
    worker_pool: Arc<WorkerPool>,
    metrics: Arc<EngineMetrics>,
    topic_label: KeyValue,
) {
    let message_start = Instant::now();

    let result = run_handlers(&handlers, &context, &message, &worker_pool, &metrics).await;

    let outcome = match result {
        Ok(()) => {
            if let Err(error) = message.ack().await {
                warn!(%error, "failed to ack message");
            }
            "ack"
        }
        Err(_) => {
            if let Err(error) = message.nack().await {
                warn!(%error, "failed to nack message");
            }
            "nack"
        }
    };

    metrics
        .messages_processed
        .add(1, &[topic_label.clone(), KeyValue::new("outcome", outcome)]);
    metrics.message_duration.record(
        message_start.elapsed().as_secs_f64(),
        std::slice::from_ref(&topic_label),
    );
}

async fn run_handlers(
    handlers: &[(Arc<dyn Handler>, Arc<str>)],
    context: &HandlerContext,
    msg: &NatsMessage,
    worker_pool: &WorkerPool,
    metrics: &EngineMetrics,
) -> Result<(), HandlerError> {
    for (handler, module_name) in handlers {
        let _permit = worker_pool
            .acquire_handler_slot(module_name)
            .await
            .expect("worker pool semaphore closed unexpectedly");

        let handler_start = Instant::now();
        handler
            .handle(context.clone(), msg.envelope.clone())
            .await?;

        metrics.handler_duration.record(
            handler_start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("handler", handler.name().to_owned()),
                KeyValue::new("module", module_name.to_string()),
            ],
        );
    }
    Ok(())
}
