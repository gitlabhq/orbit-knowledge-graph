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

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use opentelemetry::KeyValue;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, warn};

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
                    let span = tracing::info_span!(
                        "process_message",
                        correlation_id = %message.envelope.id.0,
                        topic = %topic_name,
                        attempt = message.envelope.attempt,
                    );
                    inflight.spawn(process_message(
                        message,
                        self.registry.handlers_for(&subscription),
                        HandlerContext::new(self.destination.clone(), self.nats_services.clone(), self.lock_service.clone(), progress, self.indexing_status.clone()),
                        self.broker.clone(),
                        runtime.clone(),
                        subscription.clone(),
                        topic_name.clone(),
                    ).instrument(span));
                }
            }
        }

        let pending_at_shutdown = inflight.len();
        while let Some(result) = inflight.join_next().await {
            if let Err(error) = result {
                warn!(%error, topic = %topic_name, "message processing task panicked");
            }
        }

        info!(
            topic = %topic_name,
            pending_at_shutdown,
            "topic listener stopped"
        );
        Ok(())
    }

    fn validate_concurrency_groups(
        &self,
        configuration: &EngineConfiguration,
    ) -> Result<(), EngineError> {
        for subscription in &self.registry.subscriptions() {
            if let Some(group) = &subscription.concurrency_group
                && !configuration
                    .concurrency_groups
                    .contains_key(group.as_ref())
            {
                return Err(EngineError::InvalidConfig(format!(
                    "subscription '{}.{}' references unknown concurrency group '{group}'",
                    subscription.stream, subscription.subject,
                )));
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

enum HandlerTaskOutcome {
    Ok,
    RetryRequested,
    TransientError(String),
    Exhausted(String),
    Dropped(String),
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
    let topic_label = KeyValue::new("topic", topic_name.clone());
    let subject = message.envelope.subject.clone();
    let handler_count = handlers.len();

    info!(
        %subject,
        handlers = handler_count,
        attempt = message.envelope.attempt,
        "message received"
    );

    let message_start = Instant::now();
    let outcome = run_handlers(
        &handlers,
        &context,
        &message.envelope,
        &runtime,
        &subscription,
    )
    .await;

    let outcome_label = match outcome {
        HandlersOutcome::Success => {
            if let Err(error) = message.ack().await {
                warn!(%error, "failed to ack message");
            }
            "ack"
        }
        HandlersOutcome::Failed { retry_delay } => {
            info!("message nacked, handler failure");
            let nack_result = match retry_delay {
                Some(delay) => message.nack_with_delay(delay).await,
                None => message.nack().await,
            };
            if let Err(error) = nack_result {
                warn!(%error, "failed to nack message");
            }
            "nack"
        }
        HandlersOutcome::Dropped { error } => {
            warn!(%error, "permanent failure, message dropped");
            if let Err(term_error) = message.term_ack().await {
                warn!(%term_error, "failed to term-ack dropped message");
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
                    warn!(%term_error, "failed to term-ack exhausted message");
                }
                "term"
            }
        }
    };

    let elapsed = message_start.elapsed();
    runtime
        .metrics
        .record_message_outcome(&topic_label, outcome_label);
    runtime
        .metrics
        .record_message_duration(&topic_label, elapsed.as_secs_f64());

    info!(
        %subject,
        outcome = outcome_label,
        elapsed_ms = elapsed.as_millis() as u64,
        handlers = handler_count,
        "message processed"
    );
}

/// Runs all handlers concurrently and aggregates their outcomes.
///
/// Precedence: Exhausted > Dropped > RetryRequested > TransientError > Success.
/// Retry policy (max_attempts, retry_interval) is read from the subscription,
/// not from individual handlers.
async fn run_handlers(
    handlers: &[Arc<dyn Handler>],
    context: &HandlerContext,
    envelope: &Envelope,
    runtime: &Arc<EngineRuntime>,
    subscription: &Subscription,
) -> HandlersOutcome {
    let concurrency_group = subscription.concurrency_group.clone();
    let mut tasks = tokio::task::JoinSet::new();

    for handler in handlers {
        let handler = handler.clone();
        let context = context.clone();
        let envelope = envelope.clone();
        let runtime = runtime.clone();
        let concurrency_group = concurrency_group.clone();

        tasks.spawn(async move {
            let Some(_permit) = runtime
                .worker_pool
                .acquire_handler_slot(concurrency_group.as_deref())
                .await
            else {
                warn!(
                    handler = handler.name(),
                    "worker pool semaphore closed, skipping handler"
                );
                return HandlerTaskOutcome::RetryRequested;
            };

            let handler_start = Instant::now();
            let result = handler.handle(context, envelope.clone()).await;

            runtime
                .metrics
                .record_handler_duration(handler.name(), handler_start.elapsed().as_secs_f64());

            let Err(error) = result else {
                return HandlerTaskOutcome::Ok;
            };

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
                    %error,
                    "permanent failure, skipping retries"
                );
                let error = error.to_string();
                return match action {
                    PermanentAction::DeadLetter => HandlerTaskOutcome::Exhausted(error),
                    PermanentAction::Drop => HandlerTaskOutcome::Dropped(error),
                };
            }

            error!(handler = handler.name(), %error, "handler failed");
            HandlerTaskOutcome::TransientError(error.to_string())
        });
    }

    let mut exhausted_error: Option<String> = None;
    let mut dropped_error: Option<String> = None;
    let mut has_retry_requested = false;
    let mut transient_error: Option<String> = None;

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(HandlerTaskOutcome::Ok) => {}
            Ok(HandlerTaskOutcome::RetryRequested) => {
                has_retry_requested = true;
            }
            Ok(HandlerTaskOutcome::TransientError(error)) => {
                transient_error.get_or_insert(error);
            }
            Ok(HandlerTaskOutcome::Exhausted(error)) => {
                exhausted_error.get_or_insert(error);
            }
            Ok(HandlerTaskOutcome::Dropped(error)) => {
                dropped_error.get_or_insert(error);
            }
            Err(join_error) => {
                warn!(%join_error, "handler task panicked");
                exhausted_error.get_or_insert_with(|| format!("handler panicked: {join_error}"));
            }
        }
    }

    if let Some(error) = exhausted_error {
        return HandlersOutcome::Exhausted { error };
    }
    if let Some(error) = dropped_error {
        return HandlersOutcome::Dropped { error };
    }
    if has_retry_requested {
        return HandlersOutcome::Failed { retry_delay: None };
    }
    if let Some(error) = transient_error {
        return match subscription.max_attempts {
            None => HandlersOutcome::Success,
            Some(max_attempts) if envelope.attempt >= max_attempts => {
                warn!(
                    attempt = envelope.attempt,
                    max_attempts, "retry attempts exhausted"
                );
                HandlersOutcome::Exhausted { error }
            }
            Some(_) => HandlersOutcome::Failed {
                retry_delay: subscription.retry_interval(),
            },
        };
    }
    HandlersOutcome::Success
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats::ProgressNotifier;
    use crate::testkit::mocks::{
        MockDestination, MockHandler, MockLockService, MockNatsServices, TestEnvelopeFactory,
    };
    use gkg_server_config::SubscriptionConfig;
    use handler::{HandlerError, PermanentAction};
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn test_runtime(configuration: &EngineConfiguration) -> Arc<EngineRuntime> {
        let metrics = Arc::new(EngineMetrics::new());
        Arc::new(EngineRuntime {
            worker_pool: WorkerPool::new(configuration, metrics.clone()),
            metrics,
        })
    }

    #[tokio::test]
    async fn handler_failure_under_retry_limit_requests_retry() {
        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()));
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];
        let subscription =
            Subscription::new("stream", "subject").with_config(&SubscriptionConfig {
                max_attempts: Some(3),
                retry_interval_secs: Some(5),
                ..Default::default()
            });

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert!(
            matches!(outcome, HandlersOutcome::Failed { retry_delay } if retry_delay == Some(Duration::from_secs(5))),
            "expected Failed with 5s delay, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn handler_failure_at_retry_limit_returns_exhausted() {
        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()));
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];
        let subscription =
            Subscription::new("stream", "subject").with_config(&SubscriptionConfig {
                max_attempts: Some(3),
                retry_interval_secs: Some(5),
                ..Default::default()
            });

        let envelope = TestEnvelopeFactory::with_attempt("payload", 3);
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert!(
            matches!(outcome, HandlersOutcome::Exhausted { .. }),
            "expected Exhausted, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn handler_failure_without_retry_config_succeeds() {
        let handler = MockHandler::new("stream", "subject")
            .with_error(HandlerError::Processing("boom".into()));
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];
        let subscription = Subscription::new("stream", "subject");

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert!(
            matches!(outcome, HandlersOutcome::Success),
            "subscription without retry config should succeed (retries are opt-in), got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn handlers_run_concurrently() {
        let handler_a = MockHandler::new("stream", "subject")
            .with_name("slow-a")
            .with_delay(Duration::from_millis(100));
        let handler_b = MockHandler::new("stream", "subject")
            .with_name("slow-b")
            .with_delay(Duration::from_millis(100));
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler_a), Arc::new(handler_b)];
        let subscription = Subscription::new("stream", "subject");

        let envelope = TestEnvelopeFactory::simple("payload");
        let runtime = test_runtime(&EngineConfiguration::default());

        let start = Instant::now();
        run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(250),
            "two 100ms handlers should overlap, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn all_handlers_execute_despite_one_failing() {
        let call_count = Arc::new(AtomicUsize::new(0));

        let failing = MockHandler::new("stream", "subject")
            .with_name("failing")
            .with_error(HandlerError::Processing("boom".into()));

        let counting = {
            let count = call_count.clone();
            MockHandler::new("stream", "subject")
                .with_name("counting")
                .with_on_handle(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                })
        };

        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(failing), Arc::new(counting)];
        let subscription = Subscription::new("stream", "subject");

        let envelope = TestEnvelopeFactory::simple("payload");
        let runtime = test_runtime(&EngineConfiguration::default());
        run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "succeeding handler should still execute when sibling fails"
        );
    }

    #[tokio::test]
    async fn handler_panic_does_not_prevent_other_handlers() {
        let call_count = Arc::new(AtomicUsize::new(0));

        let panicking = MockHandler::new("stream", "subject")
            .with_name("panicking")
            .with_panic("simulated panic");

        let counting = {
            let count = call_count.clone();
            MockHandler::new("stream", "subject")
                .with_name("counting")
                .with_delay(Duration::from_millis(10))
                .with_on_handle(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                })
        };

        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(panicking), Arc::new(counting)];
        let subscription = Subscription::new("stream", "subject");

        let envelope = TestEnvelopeFactory::simple("payload");
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "non-panicking handler should still complete"
        );
        assert!(
            matches!(outcome, HandlersOutcome::Exhausted { .. }),
            "panic should produce Exhausted outcome, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn retry_requested_when_any_handler_wants_it() {
        let retrying = MockHandler::new("stream", "subject")
            .with_name("retrying")
            .with_error(HandlerError::Processing("transient".into()));

        let succeeding = MockHandler::new("stream", "subject").with_name("succeeding");

        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(retrying), Arc::new(succeeding)];
        let subscription =
            Subscription::new("stream", "subject").with_config(&SubscriptionConfig {
                max_attempts: Some(3),
                retry_interval_secs: Some(10),
                ..Default::default()
            });

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert!(
            matches!(outcome, HandlersOutcome::Failed { retry_delay } if retry_delay == Some(Duration::from_secs(10))),
            "should nack when any handler wants a retry, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn permanent_dead_letter_error_returns_exhausted() {
        let handler = MockHandler::new("stream", "subject").with_error(HandlerError::Permanent {
            message: "fatal error".into(),
            action: PermanentAction::DeadLetter,
        });
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];
        let subscription = Subscription::new("stream", "subject");

        let envelope = TestEnvelopeFactory::simple("payload");
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert!(
            matches!(outcome, HandlersOutcome::Exhausted { .. }),
            "permanent DeadLetter should produce Exhausted, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn permanent_drop_error_returns_dropped() {
        let handler = MockHandler::new("stream", "subject").with_error(HandlerError::Permanent {
            message: "poison pill".into(),
            action: PermanentAction::Drop,
        });
        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(handler)];
        let subscription = Subscription::new("stream", "subject");

        let envelope = TestEnvelopeFactory::simple("payload");
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert!(
            matches!(outcome, HandlersOutcome::Dropped { .. }),
            "permanent Drop should produce Dropped, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn exhausted_takes_precedence_over_retry() {
        let exhausting = MockHandler::new("stream", "subject")
            .with_name("exhausting")
            .with_error(HandlerError::Permanent {
                message: "fatal".into(),
                action: PermanentAction::DeadLetter,
            });

        let retrying = MockHandler::new("stream", "subject")
            .with_name("retrying")
            .with_error(HandlerError::Processing("transient".into()));

        let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(exhausting), Arc::new(retrying)];
        let subscription =
            Subscription::new("stream", "subject").with_config(&SubscriptionConfig {
                max_attempts: Some(3),
                retry_interval_secs: Some(10),
                ..Default::default()
            });

        let envelope = TestEnvelopeFactory::with_attempt("payload", 1);
        let runtime = test_runtime(&EngineConfiguration::default());
        let outcome = run_handlers(
            &handlers,
            &test_context(),
            &envelope,
            &runtime,
            &subscription,
        )
        .await;

        assert!(
            matches!(outcome, HandlersOutcome::Exhausted { .. }),
            "Exhausted should take precedence over retry, got {outcome:?}"
        );
    }
}
