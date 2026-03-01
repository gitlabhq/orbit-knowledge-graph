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
use std::time::{Duration, Instant};

use futures::StreamExt;
use opentelemetry::KeyValue;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::configuration::EngineConfiguration;
use crate::destination::Destination;
use crate::locking::{LockService, NatsLockService};
use crate::metrics::EngineMetrics;
use crate::module::{Handler, HandlerContext, HandlerError, ModuleRegistry};
use crate::nats::{DlqResult, NatsBroker, NatsError, NatsMessage, NatsServices, NatsServicesImpl};
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
/// let engine = EngineBuilder::new(broker, registry, destination).build();
/// ```
pub struct EngineBuilder {
    broker: Arc<NatsBroker>,
    registry: Arc<ModuleRegistry>,
    destination: Arc<dyn Destination>,
    metrics: Option<Arc<EngineMetrics>>,
    nats_services: Option<Arc<dyn NatsServices>>,
    lock_service: Option<Arc<dyn LockService>>,
}

impl EngineBuilder {
    pub fn new(
        broker: Arc<NatsBroker>,
        registry: Arc<ModuleRegistry>,
        destination: Arc<dyn Destination>,
    ) -> Self {
        Self {
            broker,
            registry,
            destination,
            metrics: None,
            nats_services: None,
            lock_service: None,
        }
    }

    pub fn nats_services(mut self, nats_services: Arc<dyn NatsServices>) -> Self {
        self.nats_services = Some(nats_services);
        self
    }

    pub fn lock_service(mut self, lock_service: Arc<dyn LockService>) -> Self {
        self.lock_service = Some(lock_service);
        self
    }

    pub fn metrics(mut self, metrics: Arc<EngineMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn build(self) -> Engine {
        let nats_services: Arc<dyn NatsServices> = self
            .nats_services
            .unwrap_or_else(|| Arc::new(NatsServicesImpl::new(self.broker.clone())));

        let lock_service: Arc<dyn LockService> = self
            .lock_service
            .unwrap_or_else(|| Arc::new(NatsLockService::new(nats_services.clone())));

        let metrics = self
            .metrics
            .unwrap_or_else(|| Arc::new(EngineMetrics::new()));

        Engine {
            broker: self.broker,
            registry: self.registry,
            destination: self.destination,
            metrics,
            nats_services,
            lock_service,
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
    lock_service: Arc<dyn LockService>,
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

        let configuration = Arc::new(configuration.clone());
        let runtime = Arc::new(EngineRuntime {
            worker_pool: WorkerPool::new(&configuration, self.metrics.clone()),
            metrics: self.metrics.clone(),
            configuration,
        });
        let tasks: Vec<_> = topics
            .into_iter()
            .map(|topic| self.listen(topic, runtime.clone()))
            .collect();
        futures::future::try_join_all(tasks).await?;

        Ok(())
    }

    async fn listen(&self, topic: Topic, runtime: Arc<EngineRuntime>) -> Result<(), EngineError> {
        let topic_name = format!("{}.{}", topic.stream, topic.subject);
        info!(topic = %topic_name, "topic listener starting");

        let mut subscription = self
            .broker
            .subscribe(&topic, runtime.metrics.clone())
            .await?;
        let mut inflight = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                Some(message) = subscription.next() => {
                    let message = message?;
                    inflight.spawn(process_message(
                        message,
                        self.registry.handlers_for(&topic),
                        HandlerContext::new(self.destination.clone(), self.nats_services.clone(), self.lock_service.clone()),
                        self.broker.clone(),
                        runtime.clone(),
                        topic.clone(),
                        topic_name.clone(),
                    ));
                }
            }
        }

        let drained = inflight.len();
        while let Some(result) = inflight.join_next().await {
            if let Err(error) = result {
                warn!(%error, topic = %topic_name, "message processing task panicked");
            }
        }

        info!(topic = %topic_name, drained, "topic listener stopped");
        Ok(())
    }

    /// Signals the engine to stop processing.
    ///
    /// In-flight messages will complete before shutdown.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

#[derive(Debug)]
enum HandlersOutcome {
    Success,
    Failed {
        retry_delay: Option<Duration>,
    },
    Exhausted {
        error: String,
        module_name: Arc<str>,
    },
}

struct EngineRuntime {
    worker_pool: WorkerPool,
    metrics: Arc<EngineMetrics>,
    configuration: Arc<EngineConfiguration>,
}

async fn process_message(
    message: NatsMessage,
    handlers: Vec<(Arc<dyn Handler>, Arc<str>)>,
    context: HandlerContext,
    broker: Arc<NatsBroker>,
    runtime: Arc<EngineRuntime>,
    topic: Topic,
    topic_name: String,
) {
    let message_id = message.envelope.id.0.clone();
    let attempt = message.envelope.attempt;
    let topic_label = KeyValue::new("topic", topic_name.clone());

    debug!(topic = %topic_name, %message_id, attempt, "message received");

    if attempt > 1 {
        info!(topic = %topic_name, %message_id, attempt, "message retry received");
    }

    let message_start = Instant::now();
    let outcome = run_handlers(&handlers, &context, &message.envelope, &runtime).await;

    let outcome_label = match outcome {
        HandlersOutcome::Success => {
            if let Err(error) = message.ack().await {
                warn!(%error, %message_id, "failed to ack message");
            }
            "ack"
        }
        HandlersOutcome::Failed { retry_delay } => {
            info!(topic = %topic_name, %message_id, "message nacked, handler failure");
            let nack_result = match retry_delay {
                Some(delay) => message.nack_with_delay(delay).await,
                None => message.nack().await,
            };
            if let Err(error) = nack_result {
                warn!(%error, %message_id, "failed to nack message");
            }
            "nack"
        }
        HandlersOutcome::Exhausted { error, module_name } => {
            let dead_letter_enabled = runtime
                .configuration
                .modules
                .get(module_name.as_ref())
                .is_none_or(|c| c.dead_letter_enabled);

            if dead_letter_enabled {
                match message.to_dlq(&broker, &topic, &error).await {
                    DlqResult::Published => "dead_letter",
                    DlqResult::Nacked => "nack",
                }
            } else {
                warn!(%message_id, topic = %topic_name, "exhausted message discarded (dead letter disabled)");
                if let Err(ack_error) = message.ack().await {
                    warn!(%ack_error, %message_id, "failed to ack discarded message");
                }
                "discarded"
            }
        }
    };

    runtime
        .metrics
        .record_message_outcome(&topic_label, outcome_label);
    runtime
        .metrics
        .record_message_duration(&topic_label, message_start.elapsed().as_secs_f64());
}

async fn run_handlers(
    handlers: &[(Arc<dyn Handler>, Arc<str>)],
    context: &HandlerContext,
    envelope: &Envelope,
    runtime: &EngineRuntime,
) -> HandlersOutcome {
    for (handler, module_name) in handlers {
        let Some(_permit) = runtime.worker_pool.acquire_handler_slot(module_name).await else {
            warn!(
                module = %module_name,
                "worker pool semaphore closed, skipping remaining handlers"
            );
            return HandlersOutcome::Failed { retry_delay: None };
        };

        let handler_start = Instant::now();
        let result = handler.handle(context.clone(), envelope.clone()).await;

        runtime
            .metrics
            .record_handler_duration(handler.name(), handler_start.elapsed().as_secs_f64());

        if let Err(error) = result {
            runtime
                .metrics
                .record_handler_error(handler.name(), error.error_kind());

            let module_config = runtime.configuration.modules.get(module_name.as_ref());
            let max_attempts = module_config.and_then(|c| c.max_retry_attempts);

            if let Some(max_attempts) = max_attempts
                && envelope.attempt >= max_attempts
            {
                warn!(
                    module = %module_name,
                    handler = handler.name(),
                    message_id = %envelope.id.0,
                    attempt = envelope.attempt,
                    %max_attempts,
                    %error,
                    "retry attempts exhausted, sending to dead letter queue"
                );
                return HandlersOutcome::Exhausted {
                    error: error.to_string(),
                    module_name: module_name.clone(),
                };
            }

            let retry_delay = module_config.and_then(|c| c.retry_interval());
            return HandlersOutcome::Failed { retry_delay };
        }
    }
    HandlersOutcome::Success
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::configuration::ModuleConfiguration;
    use crate::testkit::mocks::{
        MockDestination, MockHandler, MockLockService, MockNatsServices, TestEnvelopeFactory,
    };

    fn test_context() -> HandlerContext {
        HandlerContext::new(
            Arc::new(MockDestination::new()),
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
        )
    }

    fn test_runtime(configuration: &EngineConfiguration) -> EngineRuntime {
        let metrics = Arc::new(EngineMetrics::new());
        EngineRuntime {
            worker_pool: WorkerPool::new(configuration, metrics.clone()),
            metrics,
            configuration: Arc::new(configuration.clone()),
        }
    }

    #[tokio::test]
    async fn handler_failure_under_retry_limit_returns_failed() {
        let configuration = EngineConfiguration {
            modules: HashMap::from([(
                "test-module".to_string(),
                ModuleConfiguration {
                    max_retry_attempts: Some(3),
                    retry_interval_secs: Some(5),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()));
        let handlers: Vec<(Arc<dyn Handler>, Arc<str>)> =
            vec![(Arc::new(handler), Arc::from("test-module"))];

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&configuration);
        let outcome = run_handlers(&handlers, &test_context(), &envelope, &runtime).await;

        assert!(
            matches!(outcome, HandlersOutcome::Failed { retry_delay } if retry_delay == Some(Duration::from_secs(5)))
        );
    }

    #[tokio::test]
    async fn handler_failure_at_retry_limit_returns_exhausted() {
        let configuration = EngineConfiguration {
            modules: HashMap::from([(
                "test-module".to_string(),
                ModuleConfiguration {
                    max_retry_attempts: Some(3),
                    retry_interval_secs: Some(5),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()));
        let handlers: Vec<(Arc<dyn Handler>, Arc<str>)> =
            vec![(Arc::new(handler), Arc::from("test-module"))];

        let envelope = TestEnvelopeFactory::with_attempt("payload", 3);
        let runtime = test_runtime(&configuration);
        let outcome = run_handlers(&handlers, &test_context(), &envelope, &runtime).await;

        match outcome {
            HandlersOutcome::Exhausted { error, .. } => {
                assert!(error.contains("boom"));
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exhausted_retries_returns_exhausted_and_skips_remaining_handlers() {
        let configuration = EngineConfiguration {
            modules: HashMap::from([(
                "test-module".to_string(),
                ModuleConfiguration {
                    max_retry_attempts: Some(3),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        let failing_handler = Arc::new(
            MockHandler::new("stream", "subject")
                .with_name("failing-handler")
                .with_error(HandlerError::Processing("db connection refused".into())),
        );
        let skipped_handler =
            Arc::new(MockHandler::new("stream", "subject").with_name("skipped-handler"));

        let handlers: Vec<(Arc<dyn Handler>, Arc<str>)> = vec![
            (failing_handler.clone(), Arc::from("test-module")),
            (skipped_handler.clone(), Arc::from("test-module")),
        ];

        let envelope = TestEnvelopeFactory::with_attempt("payload", 3);
        let runtime = test_runtime(&configuration);
        let outcome = run_handlers(&handlers, &test_context(), &envelope, &runtime).await;

        match outcome {
            HandlersOutcome::Exhausted { error, .. } => {
                assert!(error.contains("db connection refused"));
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
        assert_eq!(
            skipped_handler.invocation_count(),
            0,
            "remaining handlers should not run after exhaustion"
        );
    }

    #[tokio::test]
    async fn handler_failure_without_retry_config_returns_failed_no_delay() {
        let configuration = EngineConfiguration::default();

        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()));
        let handlers: Vec<(Arc<dyn Handler>, Arc<str>)> =
            vec![(Arc::new(handler), Arc::from("test-module"))];

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&configuration);
        let outcome = run_handlers(&handlers, &test_context(), &envelope, &runtime).await;

        assert!(
            matches!(outcome, HandlersOutcome::Failed { retry_delay } if retry_delay.is_none())
        );
    }
}
