mod metrics;
pub mod table_cleanup;

pub use metrics::ScheduledTaskMetrics;
pub use table_cleanup::TableCleanup;

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use croner::Cron;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::locking::{INDEXING_LOCKS_BUCKET, LockService, NatsLockService};
use crate::nats::{KvBucketConfig, NatsBroker, NatsServices, NatsServicesImpl};
use gkg_server_config::NatsConfiguration;
use gkg_server_config::ScheduleConfiguration;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TaskError(String);

impl TaskError {
    pub fn new(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}

#[async_trait]
pub trait ScheduledTask: Send + Sync {
    fn name(&self) -> &str;

    fn schedule(&self) -> &ScheduleConfiguration;

    async fn run(&self) -> Result<(), TaskError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] crate::nats::NatsError),
    #[error("task \"{task}\" has no cron expression")]
    MissingCron { task: String },
    #[error("task \"{task}\" has invalid cron expression \"{expr}\": {reason}")]
    InvalidCron {
        task: String,
        expr: String,
        reason: String,
    },
    #[error("{count} task loop(s) panicked")]
    TaskPanicked { count: usize },
}

pub struct SchedulerServices {
    pub nats: Arc<dyn NatsServices>,
    pub lock_service: Arc<dyn LockService>,
    pub nats_client: async_nats::Client,
}

pub async fn connect(nats_config: &NatsConfiguration) -> Result<SchedulerServices, SchedulerError> {
    let broker = Arc::new(NatsBroker::connect(nats_config).await?);
    broker
        .ensure_kv_bucket_exists(
            INDEXING_LOCKS_BUCKET,
            KvBucketConfig::with_per_message_ttl(),
        )
        .await?;

    let nats_client = broker.nats_client().clone();
    let nats: Arc<dyn NatsServices> = Arc::new(NatsServicesImpl::new(broker));
    let lock_service: Arc<dyn LockService> = Arc::new(NatsLockService::new(Arc::clone(&nats)));

    Ok(SchedulerServices {
        nats,
        lock_service,
        nats_client,
    })
}

/// Runs all tasks in independent loops until `shutdown` is cancelled.
///
/// Validates that every task has a parseable cron expression before spawning.
pub async fn run_loop(
    tasks: Vec<Box<dyn ScheduledTask>>,
    lock_service: Arc<dyn LockService>,
    shutdown: CancellationToken,
) -> Result<(), SchedulerError> {
    for task in &tasks {
        validate_cron(task.as_ref())?;
    }

    let mut handles = JoinSet::new();

    for task in tasks {
        let lock = lock_service.clone();
        let token = shutdown.clone();
        handles.spawn(run_task_loop(task, lock, token));
    }

    let mut panicked = 0usize;
    while let Some(result) = handles.join_next().await {
        if let Err(error) = result {
            warn!(%error, "task loop panicked");
            panicked += 1;
        }
    }

    if panicked > 0 {
        return Err(SchedulerError::TaskPanicked { count: panicked });
    }

    Ok(())
}

fn validate_cron(task: &dyn ScheduledTask) -> Result<(), SchedulerError> {
    let expr = task
        .schedule()
        .cron
        .as_deref()
        .ok_or_else(|| SchedulerError::MissingCron {
            task: task.name().to_owned(),
        })?;
    Cron::from_str(expr).map_err(|e| SchedulerError::InvalidCron {
        task: task.name().to_owned(),
        expr: expr.to_owned(),
        reason: e.to_string(),
    })?;
    Ok(())
}

async fn run_task_loop(
    task: Box<dyn ScheduledTask>,
    lock_service: Arc<dyn LockService>,
    shutdown: CancellationToken,
) {
    let task_name = task.name().to_owned();
    info!(task = task_name, "task loop started");

    loop {
        let delay = task.schedule().next_delay(Utc::now());

        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(delay) => {}
        }

        let cadence_key = format!("cadence.{}", task_name);
        let ttl = task.schedule().interval_hint();
        match lock_service.try_acquire(&cadence_key, ttl).await {
            Ok(true) => {}
            Ok(false) => {
                info!(task = task_name, "skipping, within interval");
                continue;
            }
            Err(error) => {
                warn!(task = task_name, %error, "cadence lock check failed");
                continue;
            }
        }

        match task.run().await {
            Ok(()) => {
                info!(task = task_name, "task completed");
            }
            Err(error) => {
                warn!(task = task_name, %error, "task failed");
            }
        }
    }

    info!(task = task_name, "task loop stopped");
}

/// Runs all tasks once (for integration tests and backward compatibility).
pub async fn run_once(
    tasks: &[Box<dyn ScheduledTask>],
    lock_service: &dyn LockService,
) -> Result<(), SchedulerError> {
    for task in tasks {
        let task_name = task.name();

        let cadence_key = format!("cadence.{}", task_name);
        let ttl = task.schedule().interval_hint();
        match lock_service.try_acquire(&cadence_key, ttl).await {
            Ok(true) => {}
            Ok(false) => {
                info!(task = task_name, "skipping, within interval");
                continue;
            }
            Err(error) => {
                warn!(task = task_name, %error, "cadence lock check failed");
                continue;
            }
        }

        match task.run().await {
            Ok(()) => {
                info!(task = task_name, "task completed");
            }
            Err(error) => {
                warn!(task = task_name, %error, "task failed");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::locking::LockError;
    use crate::testkit::mocks::MockLockService;

    /// Every-second cron: deterministic short delay (≤1s) regardless of wall clock.
    const EVERY_SECOND: &str = "*/1 * * * * *";
    /// Every-5-seconds cron: short delay (≤5s) regardless of wall clock.
    const EVERY_5S: &str = "*/5 * * * * *";
    /// Yearly cron: delay is always months, safe for "should not fire" assertions.
    const YEARLY: &str = "0 0 0 1 1 *";

    struct StubTask {
        name: &'static str,
        config: ScheduleConfiguration,
        run_count: AtomicUsize,
    }

    impl StubTask {
        fn new(name: &'static str, cron: &str) -> Self {
            Self {
                name,
                config: ScheduleConfiguration {
                    cron: Some(cron.to_owned()),
                },
                run_count: AtomicUsize::new(0),
            }
        }

        fn without_cron(name: &'static str) -> Self {
            Self {
                name,
                config: ScheduleConfiguration { cron: None },
                run_count: AtomicUsize::new(0),
            }
        }

        fn run_count(&self) -> usize {
            self.run_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ScheduledTask for Arc<StubTask> {
        fn name(&self) -> &str {
            self.name
        }

        fn schedule(&self) -> &ScheduleConfiguration {
            &self.config
        }

        async fn run(&self) -> Result<(), TaskError> {
            self.run_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Lock service that always grants (simulates TTL expiry between runs).
    struct AlwaysGrantLockService;

    #[async_trait]
    impl LockService for AlwaysGrantLockService {
        async fn try_acquire(&self, _key: &str, _ttl: Duration) -> Result<bool, LockError> {
            Ok(true)
        }
        async fn release(&self, _key: &str) -> Result<(), LockError> {
            Ok(())
        }
    }

    /// Yield enough times for spawned tasks to process through their await points.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    // ── run_once tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn run_once_runs_when_lock_available() {
        let lock_service = AlwaysGrantLockService;
        let task = Arc::new(StubTask::new("t", EVERY_SECOND));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&task))];

        run_once(&tasks, &lock_service).await.unwrap();
        run_once(&tasks, &lock_service).await.unwrap();

        assert_eq!(task.run_count(), 2);
    }

    #[tokio::test]
    async fn run_once_skips_when_cadence_held() {
        let lock_service = MockLockService::new();
        let task = Arc::new(StubTask::new("hourly", "0 0 * * * *"));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&task))];

        run_once(&tasks, &lock_service).await.unwrap();
        assert_eq!(task.run_count(), 1);

        run_once(&tasks, &lock_service).await.unwrap();
        assert_eq!(task.run_count(), 1, "should skip when cadence lock is held");
    }

    #[tokio::test]
    async fn run_once_cadence_is_per_task() {
        let lock_service = MockLockService::new();
        let a = Arc::new(StubTask::new("a", "0 0 * * * *"));
        let b = Arc::new(StubTask::new("b", "0 0 * * * *"));
        let tasks: Vec<Box<dyn ScheduledTask>> =
            vec![Box::new(Arc::clone(&a)), Box::new(Arc::clone(&b))];

        run_once(&tasks, &lock_service).await.unwrap();

        assert_eq!(a.run_count(), 1);
        assert_eq!(b.run_count(), 1, "each task has its own cadence lock");
    }

    // ── run_loop validation tests ──────────────────────────────────

    #[tokio::test]
    async fn run_loop_rejects_missing_cron() {
        let lock_service = Arc::new(AlwaysGrantLockService);
        let task = Arc::new(StubTask::without_cron("bad"));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&task))];

        let err = run_loop(tasks, lock_service, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, SchedulerError::MissingCron { .. }),
            "expected MissingCron, got: {err}"
        );
    }

    #[tokio::test]
    async fn run_loop_rejects_invalid_cron() {
        let lock_service = Arc::new(AlwaysGrantLockService);
        let task = Arc::new(StubTask::new("bad", "not a cron"));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&task))];

        let err = run_loop(tasks, lock_service, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, SchedulerError::InvalidCron { .. }),
            "expected InvalidCron, got: {err}"
        );
    }

    // ── run_loop scheduling tests ──────────────────────────────────

    #[tokio::test]
    async fn run_loop_exits_on_shutdown() {
        let lock_service = Arc::new(AlwaysGrantLockService);
        let task = Arc::new(StubTask::new("looped", EVERY_SECOND));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&task))];

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        let handle =
            tokio::spawn(async move { run_loop(tasks, lock_service, shutdown_clone).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "run_loop should exit after shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn run_loop_dispatches_tasks_on_schedule() {
        let lock_service = Arc::new(AlwaysGrantLockService);
        let task = Arc::new(StubTask::new("periodic", EVERY_5S));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&task))];

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move { run_loop(tasks, lock_service, shutdown_clone).await });

        settle().await;
        assert_eq!(task.run_count(), 0);

        // next_delay for EVERY_5S is ≤5s from real clock; advance 6s to be safe
        tokio::time::advance(Duration::from_secs(6)).await;
        settle().await;
        assert_eq!(task.run_count(), 1, "should run after first interval");

        tokio::time::advance(Duration::from_secs(6)).await;
        settle().await;
        assert_eq!(task.run_count(), 2, "should run after second interval");

        shutdown.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn run_loop_respects_independent_schedules() {
        let lock_service = Arc::new(AlwaysGrantLockService);

        let fast = Arc::new(StubTask::new("fast", EVERY_5S));
        // Yearly cron: next_delay is always months, won't fire in test window.
        let slow = Arc::new(StubTask::new("slow", YEARLY));

        let tasks: Vec<Box<dyn ScheduledTask>> =
            vec![Box::new(Arc::clone(&fast)), Box::new(Arc::clone(&slow))];

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        tokio::spawn(async move { run_loop(tasks, lock_service, shutdown_clone).await });

        // Advance 30s — fast task should fire multiple times, slow task 0
        for _ in 0..30 {
            tokio::time::advance(Duration::from_secs(1)).await;
            settle().await;
        }

        assert!(
            fast.run_count() >= 2,
            "fast task should have run multiple times"
        );
        assert_eq!(slow.run_count(), 0, "yearly task should not have fired");

        shutdown.cancel();
    }
}
