pub mod extract;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use crate::locking::{LockService, NatsLockService};
use crate::modules::sdlc::locking::INDEXING_LOCKS_BUCKET;
use crate::nats::{KvBucketConfig, NatsBroker, NatsConfiguration, NatsServices, NatsServicesImpl};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct DispatchConfig {
    /// Per-dispatcher interval overrides, keyed by dispatcher name.
    /// Value is the interval in seconds.
    #[serde(default)]
    pub intervals: HashMap<String, u64>,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct DispatchError(String);

impl DispatchError {
    pub fn new(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}

#[async_trait]
pub trait Dispatcher: Send + Sync {
    fn name(&self) -> &str;

    async fn dispatch(&self) -> Result<(), DispatchError>;

    fn interval(&self) -> Option<Duration> {
        None
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DispatcherError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] crate::nats::NatsError),
}

pub struct DispatcherServices {
    pub nats: Arc<dyn NatsServices>,
    pub lock_service: Arc<dyn LockService>,
}

pub async fn connect(
    nats_config: &NatsConfiguration,
) -> Result<DispatcherServices, DispatcherError> {
    let broker = Arc::new(NatsBroker::connect(nats_config).await?);
    broker
        .ensure_kv_bucket_exists(
            INDEXING_LOCKS_BUCKET,
            KvBucketConfig::with_per_message_ttl(),
        )
        .await?;

    let nats: Arc<dyn NatsServices> = Arc::new(NatsServicesImpl::new(broker));
    let lock_service: Arc<dyn LockService> = Arc::new(NatsLockService::new(Arc::clone(&nats)));

    Ok(DispatcherServices { nats, lock_service })
}

pub async fn run(
    dispatchers: &[Box<dyn Dispatcher>],
    lock_service: &dyn LockService,
    config: &DispatchConfig,
) -> Result<(), DispatcherError> {
    for dispatcher in dispatchers {
        let dispatcher_name = dispatcher.name();

        let interval = config
            .intervals
            .get(dispatcher_name)
            .map(|seconds| Duration::from_secs(*seconds))
            .or_else(|| dispatcher.interval());

        if let Some(interval) = interval {
            let cadence_key = format!("cadence.{}", dispatcher_name);
            match lock_service.try_acquire(&cadence_key, interval).await {
                Ok(true) => {}
                Ok(false) => {
                    info!(dispatcher = dispatcher_name, "skipping, within interval");
                    continue;
                }
                Err(error) => {
                    warn!(dispatcher = dispatcher_name, %error, "cadence lock check failed");
                    continue;
                }
            }
        }

        match dispatcher.dispatch().await {
            Ok(()) => {
                info!(dispatcher = dispatcher_name, "dispatch completed");
            }
            Err(error) => {
                warn!(dispatcher = dispatcher_name, %error, "dispatch failed");
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

    struct StubDispatcher {
        name: &'static str,
        interval: Option<Duration>,
        dispatch_count: AtomicUsize,
    }

    impl StubDispatcher {
        fn new(name: &'static str, interval: Option<Duration>) -> Self {
            Self {
                name,
                interval,
                dispatch_count: AtomicUsize::new(0),
            }
        }

        fn dispatched(&self) -> usize {
            self.dispatch_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Dispatcher for Arc<StubDispatcher> {
        fn name(&self) -> &str {
            self.name
        }

        async fn dispatch(&self) -> Result<(), DispatchError> {
            self.dispatch_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn interval(&self) -> Option<Duration> {
            self.interval
        }
    }

    #[tokio::test]
    async fn dispatchers_without_interval_always_run() {
        let lock_service = MockLockService::new();
        let config = DispatchConfig::default();
        let dispatcher = Arc::new(StubDispatcher::new("always", None));
        let dispatchers: Vec<Box<dyn Dispatcher>> = vec![Box::new(Arc::clone(&dispatcher))];

        run(&dispatchers, &lock_service, &config).await.unwrap();
        run(&dispatchers, &lock_service, &config).await.unwrap();

        assert_eq!(dispatcher.dispatched(), 2);
    }

    #[tokio::test]
    async fn interval_dispatcher_skips_when_within_cadence() {
        let lock_service = MockLockService::new();
        let config = DispatchConfig::default();
        let hourly = Arc::new(StubDispatcher::new(
            "hourly",
            Some(Duration::from_secs(3600)),
        ));
        let dispatchers: Vec<Box<dyn Dispatcher>> = vec![Box::new(Arc::clone(&hourly))];

        run(&dispatchers, &lock_service, &config).await.unwrap();
        assert_eq!(hourly.dispatched(), 1);

        run(&dispatchers, &lock_service, &config).await.unwrap();
        assert_eq!(
            hourly.dispatched(),
            1,
            "should skip when cadence lock is held"
        );
    }

    #[tokio::test]
    async fn interval_does_not_affect_other_dispatchers() {
        let lock_service = MockLockService::new();
        let config = DispatchConfig::default();
        let hourly = Arc::new(StubDispatcher::new(
            "hourly",
            Some(Duration::from_secs(3600)),
        ));
        let always = Arc::new(StubDispatcher::new("always", None));
        let dispatchers: Vec<Box<dyn Dispatcher>> =
            vec![Box::new(Arc::clone(&hourly)), Box::new(Arc::clone(&always))];

        run(&dispatchers, &lock_service, &config).await.unwrap();
        run(&dispatchers, &lock_service, &config).await.unwrap();

        assert_eq!(hourly.dispatched(), 1, "hourly should dispatch once");
        assert_eq!(always.dispatched(), 2, "always should dispatch every time");
    }

    #[tokio::test]
    async fn config_interval_overrides_trait_default() {
        let lock_service = MockLockService::new();
        let config = DispatchConfig {
            intervals: HashMap::from([("slow".into(), 7200)]),
        };
        let dispatcher = Arc::new(StubDispatcher::new("slow", Some(Duration::from_secs(60))));
        let dispatchers: Vec<Box<dyn Dispatcher>> = vec![Box::new(Arc::clone(&dispatcher))];

        run(&dispatchers, &lock_service, &config).await.unwrap();
        assert_eq!(dispatcher.dispatched(), 1);

        run(&dispatchers, &lock_service, &config).await.unwrap();
        assert_eq!(
            dispatcher.dispatched(),
            1,
            "config interval should override trait default"
        );
    }

    #[tokio::test]
    async fn config_interval_applies_to_dispatcher_without_trait_interval() {
        let lock_service = MockLockService::new();
        let config = DispatchConfig {
            intervals: HashMap::from([("no-default".into(), 3600)]),
        };
        let dispatcher = Arc::new(StubDispatcher::new("no-default", None));
        let dispatchers: Vec<Box<dyn Dispatcher>> = vec![Box::new(Arc::clone(&dispatcher))];

        run(&dispatchers, &lock_service, &config).await.unwrap();
        assert_eq!(dispatcher.dispatched(), 1);

        run(&dispatchers, &lock_service, &config).await.unwrap();
        assert_eq!(
            dispatcher.dispatched(),
            1,
            "config interval should apply even without trait interval"
        );
    }
}
