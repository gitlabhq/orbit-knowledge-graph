//! The engine subscribes to topics, dispatches messages to handlers, and acks/nacks.
//!
//! # Example
//!
//! ```ignore
//! use etl_engine::engine::EngineBuilder;
//! use etl_engine::engine::handler::HandlerRegistry;
//! use etl_engine::nats::{NatsBroker, NatsConfiguration, NatsServicesImpl};
//! use etl_engine::configuration::EngineConfiguration;
//! use std::sync::Arc;
//!
//! let config = NatsConfiguration { url: "localhost:4222".into(), ..Default::default() };
//! let broker = Arc::new(NatsBroker::connect(&config).await?);
//! let nats_services = Arc::new(NatsServicesImpl::new(broker.clone()));
//!
//! let registry = Arc::new(HandlerRegistry::default());
//! registry.register_handler(Box::new(my_handler));
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

pub mod dead_letter;
pub mod destination;
pub mod handler;
pub mod metrics;
pub mod types;
pub mod worker_pool;

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use futures::StreamExt;
use opentelemetry::KeyValue;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::indexing_status::IndexingStatusStore;
use crate::locking::{LockService, NatsLockService};
use crate::nats::{DlqResult, NatsBroker, NatsError, NatsMessage, NatsServices, NatsServicesImpl};
use destination::Destination;
use gkg_server_config::EngineConfiguration;
use handler::{Handler, HandlerContext, HandlerError, HandlerRegistry, PermanentAction};
use metrics::EngineMetrics;
use types::{Envelope, Subscription};
use worker_pool::WorkerPool;

/// Errors that can occur during engine operation.
#[derive(Debug, Error)]
pub enum EngineError {
    /// An error from the NATS broker.
    #[error("NATS error: {0}")]
    Nats(#[from] NatsError),

    /// An error from a message handler.
    #[error("handler error: {0}")]
    Handler(#[from] HandlerError),

    /// Invalid engine configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
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
    registry: Arc<HandlerRegistry>,
    destination: Arc<dyn Destination>,
    indexing_status: Arc<IndexingStatusStore>,
    metrics: Option<Arc<EngineMetrics>>,
    nats_services: Option<Arc<dyn NatsServices>>,
}

impl EngineBuilder {
    pub fn new(
        broker: Arc<NatsBroker>,
        registry: Arc<HandlerRegistry>,
        destination: Arc<dyn Destination>,
        indexing_status: Arc<IndexingStatusStore>,
    ) -> Self {
        Self {
            broker,
            registry,
            destination,
            indexing_status,
            metrics: None,
            nats_services: None,
        }
    }

    pub fn nats_services(mut self, nats_services: Arc<dyn NatsServices>) -> Self {
        self.nats_services = Some(nats_services);
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

        let lock_service: Arc<dyn LockService> =
            Arc::new(NatsLockService::new(nats_services.clone()));

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
            indexing_status: self.indexing_status,
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
    registry: Arc<HandlerRegistry>,
    destination: Arc<dyn Destination>,
    metrics: Arc<EngineMetrics>,
    nats_services: Arc<dyn NatsServices>,
    lock_service: Arc<dyn LockService>,
    indexing_status: Arc<IndexingStatusStore>,
    cancel: CancellationToken,
}

impl Engine {
    /// Starts the engine and processes messages until stopped.
    ///
    /// Returns when stopped via [`Engine::stop`] or when all subscriptions end.
    pub async fn run(&self, configuration: &EngineConfiguration) -> Result<(), EngineError> {
        let subscriptions = self.registry.subscriptions();
        if subscriptions.is_empty() {
            return Ok(());
        }

        self.validate_concurrency_groups(configuration)?;

        self.broker
            .ensure_unmanaged_streams_exist(&subscriptions)
            .await?;

        let runtime = Arc::new(EngineRuntime {
            worker_pool: WorkerPool::new(configuration, self.metrics.clone()),
            metrics: self.metrics.clone(),
        });
        let tasks: Vec<_> = subscriptions
            .into_iter()
            .map(|subscription| self.listen(subscription, runtime.clone()))
            .collect();
        futures::future::try_join_all(tasks).await?;

        Ok(())
    }

    async fn listen(
        &self,
        subscription: Subscription,
        runtime: Arc<EngineRuntime>,
    ) -> Result<(), EngineError> {
        let topic_name = format!("{}.{}", subscription.stream, subscription.subject);
        info!(topic = %topic_name, "topic listener starting");

        let mut messages = self
            .broker
            .subscribe(&subscription, runtime.metrics.clone())
            .await?;
        let mut inflight = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                Some(message) = messages.next() => {
                    let message = message?;
                    let progress = message.progress_notifier();
                    inflight.spawn(process_message(
                        message,
                        self.registry.handlers_for(&subscription),
                        HandlerContext::new(self.destination.clone(), self.nats_services.clone(), self.lock_service.clone(), progress, self.indexing_status.clone()),
                        self.broker.clone(),
                        runtime.clone(),
                        subscription.clone(),
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

    fn validate_concurrency_groups(
        &self,
        configuration: &EngineConfiguration,
    ) -> Result<(), EngineError> {
        for subscription in &self.registry.subscriptions() {
            for handler in self.registry.handlers_for(subscription) {
                if let Some(group) = &handler.engine_config().concurrency_group
                    && !configuration.concurrency_groups.contains_key(group)
                {
                    return Err(EngineError::InvalidConfig(format!(
                        "handler '{}' references unknown concurrency group '{group}'",
                        handler.name(),
                    )));
                }
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

#[derive(Debug)]
enum HandlersOutcome {
    Success,
    Failed { retry_delay: Option<Duration> },
    Exhausted { error: String },
    Dropped { error: String },
}

struct EngineRuntime {
    worker_pool: WorkerPool,
    metrics: Arc<EngineMetrics>,
}

async fn process_message(
    message: NatsMessage,
    handlers: Vec<Arc<dyn Handler>>,
    context: HandlerContext,
    broker: Arc<NatsBroker>,
    runtime: Arc<EngineRuntime>,
    subscription: Subscription,
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
    let caught = AssertUnwindSafe(run_handlers(
        &handlers,
        &context,
        &message.envelope,
        &runtime,
    ))
    .catch_unwind()
    .await;

    let outcome = match caught {
        Ok(outcome) => outcome,
        Err(panic_payload) => {
            let panic_message = extract_panic_message(&panic_payload);
            error!(
                topic = %topic_name,
                %message_id,
                attempt,
                panic = %panic_message,
                "handler panicked"
            );
            HandlersOutcome::Exhausted {
                error: format!("handler panicked: {panic_message}"),
            }
        }
    };

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
        HandlersOutcome::Dropped { error } => {
            warn!(%message_id, %error, "permanent failure, message dropped");
            if let Err(term_error) = message.term_ack().await {
                warn!(%term_error, %message_id, "failed to term-ack dropped message");
            }
            "term"
        }
        HandlersOutcome::Exhausted { error } => {
            if subscription.dead_letter_on_exhaustion {
                match message.to_dlq(&broker, &subscription, &error).await {
                    DlqResult::Published => "dead_letter",
                    DlqResult::Nacked => "nack",
                }
            } else {
                if let Err(term_error) = message.term_ack().await {
                    warn!(%term_error, %message_id, "failed to term-ack exhausted message");
                }
                "term"
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
    handlers: &[Arc<dyn Handler>],
    context: &HandlerContext,
    envelope: &Envelope,
    runtime: &EngineRuntime,
) -> HandlersOutcome {
    for handler in handlers {
        let handler_config = handler.engine_config();
        let concurrency_group = handler_config.concurrency_group.as_deref();

        let Some(_permit) = runtime
            .worker_pool
            .acquire_handler_slot(concurrency_group)
            .await
        else {
            warn!(
                handler = handler.name(),
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

            if error.is_permanent() {
                let action = match &error {
                    HandlerError::Permanent { action, .. } => *action,
                    HandlerError::Deserialization(_) => PermanentAction::DeadLetter,
                    _ => unreachable!("is_permanent() returned true"),
                };
                warn!(
                    handler = handler.name(),
                    subject = %envelope.subject,
                    message_id = %envelope.id.0,
                    attempt = envelope.attempt,
                    %error,
                    "permanent failure, skipping retries"
                );
                let error = error.to_string();
                return match action {
                    PermanentAction::DeadLetter => HandlersOutcome::Exhausted { error },
                    PermanentAction::Drop => HandlersOutcome::Dropped { error },
                };
            }

            let max_attempts = handler_config.max_attempts;

            if let Some(max_attempts) = max_attempts {
                if envelope.attempt >= max_attempts {
                    warn!(
                        handler = handler.name(),
                        message_id = %envelope.id.0,
                        attempt = envelope.attempt,
                        %max_attempts,
                        %error,
                        "retry attempts exhausted"
                    );
                    return HandlersOutcome::Exhausted {
                        error: error.to_string(),
                    };
                }

                let retry_delay = handler_config.retry_interval();
                return HandlersOutcome::Failed { retry_delay };
            }

            warn!(
                handler = handler.name(),
                message_id = %envelope.id.0,
                %error,
                "handler failed with no retry config, acking message"
            );
            continue;
        }
    }
    HandlersOutcome::Success
}

fn extract_panic_message(payload: &Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats::ProgressNotifier;
    use crate::testkit::mocks::{
        MockDestination, MockHandler, MockLockService, MockNatsServices, TestEnvelopeFactory,
    };
    use gkg_server_config::HandlerConfiguration;

    fn test_context() -> HandlerContext {
        let mock = Arc::new(MockNatsServices::new());
        HandlerContext::new(
            Arc::new(MockDestination::new()),
            mock.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(IndexingStatusStore::new(mock)),
        )
    }

    fn test_runtime(configuration: &EngineConfiguration) -> EngineRuntime {
        let metrics = Arc::new(EngineMetrics::new());
        EngineRuntime {
            worker_pool: WorkerPool::new(configuration, metrics.clone()),
            metrics,
        }
    }

    #[tokio::test]
    async fn handler_failure_under_retry_limit_returns_failed() {
        let configuration = EngineConfiguration::default();

        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()))
            .with_engine_config(HandlerConfiguration {
                max_attempts: Some(3),
                retry_interval_secs: Some(5),
                ..Default::default()
            });
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&configuration);
        let outcome = run_handlers(&handlers, &test_context(), &envelope, &runtime).await;

        assert!(
            matches!(outcome, HandlersOutcome::Failed { retry_delay } if retry_delay == Some(Duration::from_secs(5)))
        );
    }

    #[tokio::test]
    async fn handler_failure_at_retry_limit_returns_exhausted() {
        let configuration = EngineConfiguration::default();

        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()))
            .with_engine_config(HandlerConfiguration {
                max_attempts: Some(3),
                retry_interval_secs: Some(5),
                ..Default::default()
            });
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];

        let envelope = TestEnvelopeFactory::with_attempt("payload", 3);
        let runtime = test_runtime(&configuration);
        let outcome = run_handlers(&handlers, &test_context(), &envelope, &runtime).await;

        match outcome {
            HandlersOutcome::Exhausted { error } => {
                assert!(error.contains("boom"));
            }
            other => panic!("expected Exhausted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handler_failure_without_retry_config_acks() {
        let configuration = EngineConfiguration::default();

        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()));
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&configuration);
        let outcome = run_handlers(&handlers, &test_context(), &envelope, &runtime).await;

        assert!(
            matches!(outcome, HandlersOutcome::Success),
            "handler failure without retry config should ack (retries are opt-in)"
        );
    }
}
