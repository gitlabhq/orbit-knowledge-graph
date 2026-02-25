pub mod extract;

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, warn};

use crate::locking::{LockService, NatsLockService};
use crate::modules::sdlc::locking::INDEXING_LOCKS_BUCKET;
use crate::nats::{KvBucketConfig, NatsBroker, NatsConfiguration, NatsServices, NatsServicesImpl};

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

pub async fn run(dispatchers: &[Box<dyn Dispatcher>]) -> Result<(), DispatcherError> {
    for dispatcher in dispatchers {
        let dispatcher_name = dispatcher.name();

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
