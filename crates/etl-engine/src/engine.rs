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
use tracing::{error, info};

use crate::configuration::EngineConfiguration;
use crate::destination::Destination;
use crate::metrics::{MetricCollector, NoopMetricCollector};
use crate::module::{Handler, HandlerContext, HandlerError, ModuleRegistry};
use crate::nats::{NatsBroker, NatsError, NatsServices, NatsServicesImpl};
use crate::types::{Envelope, Topic};
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
/// let engine = EngineBuilder::new(broker, registry, destination)
///     .metrics(Arc::new(my_metrics))  // optional
///     .build();
/// ```
pub struct EngineBuilder {
    broker: Arc<NatsBroker>,
    registry: Arc<ModuleRegistry>,
    destination: Arc<dyn Destination>,
    metrics: Arc<dyn MetricCollector>,
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
            metrics: Arc::new(NoopMetricCollector),
            nats_services: None,
        }
    }

    /// Sets the metric collector.
    ///
    /// If not called, metrics are discarded.
    pub fn metrics(mut self, metrics: Arc<dyn MetricCollector>) -> Self {
        self.metrics = metrics;
        self
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
        let nats_services = self
            .nats_services
            .unwrap_or_else(|| Arc::new(NatsServicesImpl::new(self.broker.clone())));

        Engine {
            broker: self.broker,
            registry: self.registry,
            destination: self.destination,
            metrics: self.metrics,
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
    metrics: Arc<dyn MetricCollector>,
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
            info!("no topics registered, exiting");
            return Ok(());
        }

        info!(count = topics.len(), "ensuring streams exist");
        self.broker.ensure_streams(&topics).await?;

        let worker_pool = Arc::new(WorkerPool::new(configuration));
        let tasks: Vec<_> = topics
            .into_iter()
            .map(|topic| self.listen(topic, worker_pool.clone()))
            .collect();
        futures::future::try_join_all(tasks).await?;

        Ok(())
    }

    async fn listen(&self, topic: Topic, worker_pool: Arc<WorkerPool>) -> Result<(), EngineError> {
        info!(stream = %topic.stream, subject = %topic.subject, "subscribing to topic");
        let mut subscription = self.broker.subscribe(&topic).await?;
        info!(stream = %topic.stream, subject = %topic.subject, "subscribed, waiting for messages");

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break Ok(()),
                Some(msg) = subscription.next() => {
                    let msg = msg?;
                    info!(stream = %topic.stream, subject = %topic.subject, "received message");
                    let handlers = self.registry.handlers_for(&topic);
                    let context = HandlerContext::new(
                        self.destination.clone(),
                        self.metrics.clone(),
                        self.nats_services.clone(),
                    );

                    match dispatch(&handlers, context, msg.envelope.clone(), &worker_pool).await {
                        Ok(_)  => {
                            info!(stream = %topic.stream, subject = %topic.subject, "message processed successfully");
                            msg.ack().await?;
                        }
                        Err(e) => {
                            error!(stream = %topic.stream, subject = %topic.subject, error = %e, "message processing failed, nacking");
                            msg.nack().await?;
                        }
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
