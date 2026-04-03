mod metrics;
pub mod table_cleanup;

pub use metrics::ScheduledTaskMetrics;
pub use table_cleanup::TableCleanup;

use std::sync::Arc;

use async_trait::async_trait;
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
}

pub struct SchedulerServices {
    pub nats: Arc<dyn NatsServices>,
    pub lock_service: Arc<dyn LockService>,
}

pub async fn connect(nats_config: &NatsConfiguration) -> Result<SchedulerServices, SchedulerError> {
    let broker = Arc::new(NatsBroker::connect(nats_config).await?);
    broker
        .ensure_kv_bucket_exists(
            INDEXING_LOCKS_BUCKET,
            KvBucketConfig::with_per_message_ttl(),
        )
        .await?;

    let nats: Arc<dyn NatsServices> = Arc::new(NatsServicesImpl::new(broker));
    let lock_service: Arc<dyn LockService> = Arc::new(NatsLockService::new(Arc::clone(&nats)));

    Ok(SchedulerServices { nats, lock_service })
}

pub async fn run(
    tasks: &[Box<dyn ScheduledTask>],
    lock_service: &dyn LockService,
) -> Result<(), SchedulerError> {
    for task in tasks {
        let task_name = task.name();
        let interval = task.schedule().interval();

        if let Some(interval) = interval {
            let cadence_key = format!("cadence.{}", task_name);
            match lock_service.try_acquire(&cadence_key, interval).await {
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

    use super::*;
    use crate::testkit::mocks::MockLockService;

    struct StubTask {
        name: &'static str,
        config: ScheduleConfiguration,
        run_count: AtomicUsize,
    }

    impl StubTask {
        fn new(name: &'static str, interval_secs: Option<u64>) -> Self {
            Self {
                name,
                config: ScheduleConfiguration { interval_secs },
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

    #[tokio::test]
    async fn tasks_without_interval_always_run() {
        let lock_service = MockLockService::new();
        let task = Arc::new(StubTask::new("always", None));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&task))];

        run(&tasks, &lock_service).await.unwrap();
        run(&tasks, &lock_service).await.unwrap();

        assert_eq!(task.run_count(), 2);
    }

    #[tokio::test]
    async fn interval_task_skips_when_within_cadence() {
        let lock_service = MockLockService::new();
        let hourly = Arc::new(StubTask::new("hourly", Some(3600)));
        let tasks: Vec<Box<dyn ScheduledTask>> = vec![Box::new(Arc::clone(&hourly))];

        run(&tasks, &lock_service).await.unwrap();
        assert_eq!(hourly.run_count(), 1);

        run(&tasks, &lock_service).await.unwrap();
        assert_eq!(
            hourly.run_count(),
            1,
            "should skip when cadence lock is held"
        );
    }

    #[tokio::test]
    async fn interval_does_not_affect_other_tasks() {
        let lock_service = MockLockService::new();
        let hourly = Arc::new(StubTask::new("hourly", Some(3600)));
        let always = Arc::new(StubTask::new("always", None));
        let tasks: Vec<Box<dyn ScheduledTask>> =
            vec![Box::new(Arc::clone(&hourly)), Box::new(Arc::clone(&always))];

        run(&tasks, &lock_service).await.unwrap();
        run(&tasks, &lock_service).await.unwrap();

        assert_eq!(hourly.run_count(), 1, "hourly should run once");
        assert_eq!(always.run_count(), 2, "always should run every time");
    }
}
