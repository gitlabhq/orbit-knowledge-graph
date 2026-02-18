mod extract;

use std::sync::Arc;

use chrono::Utc;
use indexer::clickhouse::ClickHouseError;
use indexer::locking::{LockError, LockService, NatsLockService};
use indexer::modules::sdlc::locking::{
    INDEXING_LOCKS_BUCKET, LOCK_TTL, global_lock_key, namespace_lock_key,
};
use indexer::nats::{KvBucketConfig, NatsBroker, NatsServicesImpl};
use indexer::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
use indexer::types::{Envelope, Event};
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use extract::FromArrowColumn;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, organization_id
FROM siphon_knowledge_graph_enabled_namespaces
INNER JOIN siphon_namespaces on siphon_knowledge_graph_enabled_namespaces.root_namespace_id = siphon_namespaces.id
WHERE _siphon_deleted = false
"#;

#[derive(Debug, thiserror::Error)]
pub enum DispatcherError {
    #[error("NATS connection failed: {0}")]
    NatsConnection(#[from] indexer::nats::NatsError),

    #[error("ClickHouse query error: {0}")]
    ClickHouseQueryError(ClickHouseError),

    #[error("Invalid column type: expected {expected}")]
    InvalidColumnType { expected: &'static str },

    #[error("Failed to serialize message: {0}")]
    Serialization(#[from] indexer::types::SerializationError),

    #[error("Failed to publish message: {0}")]
    Publish(indexer::nats::NatsError),

    #[error("Failed to acquire lock: {0}")]
    LockAcquisition(LockError),
}

struct Dispatcher {
    broker: Arc<NatsBroker>,
    lock_service: NatsLockService,
}

enum LockResult {
    Acquired,
    AlreadyHeld,
}

impl Dispatcher {
    fn new(broker: Arc<NatsBroker>) -> Self {
        let nats_services = Arc::new(NatsServicesImpl::new(Arc::clone(&broker)));
        let lock_service = NatsLockService::new(nats_services);
        Self {
            broker,
            lock_service,
        }
    }

    async fn try_acquire_lock(&self, key: &str) -> Result<LockResult, DispatcherError> {
        match self
            .lock_service
            .try_acquire(key, LOCK_TTL)
            .await
            .map_err(DispatcherError::LockAcquisition)?
        {
            true => Ok(LockResult::Acquired),
            false => Ok(LockResult::AlreadyHeld),
        }
    }

    async fn publish<E: Event>(&self, event: &E) -> Result<(), DispatcherError> {
        let envelope = Envelope::new(event)?;
        self.broker
            .publish(&E::topic(), &envelope)
            .await
            .map_err(DispatcherError::Publish)
    }
}

pub async fn run(config: &AppConfig) -> Result<(), DispatcherError> {
    let broker = Arc::new(NatsBroker::connect(&config.nats).await?);
    broker
        .ensure_kv_bucket_exists(
            INDEXING_LOCKS_BUCKET,
            KvBucketConfig::with_per_message_ttl(),
        )
        .await?;

    let dispatcher = Dispatcher::new(broker);
    let datalake = config.datalake.build_client();

    let arrow_batches = datalake
        .query(ENABLED_NAMESPACE_QUERY)
        .fetch_arrow()
        .await
        .map_err(DispatcherError::ClickHouseQueryError)?;

    let namespace_ids = i64::extract_column(&arrow_batches, 0)?;
    let organization_ids = i64::extract_column(&arrow_batches, 1)?;
    let watermark = Utc::now();

    debug!(
        enabled_namespaces = namespace_ids.len(),
        "Found enabled namespaces to dispatch indexing requests for"
    );

    match dispatcher.try_acquire_lock(global_lock_key()).await {
        Ok(LockResult::Acquired) => {
            dispatcher
                .publish(&GlobalIndexingRequest { watermark })
                .await?;
            info!("Dispatched global indexing request");
        }
        Ok(LockResult::AlreadyHeld) => {
            info!("Skipping global indexing request, lock already held");
        }
        Err(error) => {
            warn!(%error, "Failed to acquire global lock, skipping global indexing request");
        }
    }

    let mut dispatched_count = 0;
    let mut skipped_count = 0;
    let mut failed_count = 0;
    for (namespace_id, organization_id) in namespace_ids.iter().zip(organization_ids.iter()) {
        let lock_key = namespace_lock_key(*namespace_id);
        match dispatcher.try_acquire_lock(&lock_key).await {
            Ok(LockResult::Acquired) => {
                dispatcher
                    .publish(&NamespaceIndexingRequest {
                        organization: *organization_id,
                        namespace: *namespace_id,
                        watermark,
                    })
                    .await?;
                dispatched_count += 1;
                debug!(
                    namespace_id = *namespace_id,
                    organization_id = *organization_id,
                    "Dispatched namespace indexing request"
                );
            }
            Ok(LockResult::AlreadyHeld) => {
                skipped_count += 1;
                debug!(
                    namespace_id = *namespace_id,
                    organization_id = *organization_id,
                    "Skipped namespace indexing request, lock already held"
                );
            }
            Err(error) => {
                failed_count += 1;
                warn!(
                    namespace_id = *namespace_id,
                    organization_id = *organization_id,
                    %error,
                    "Failed to acquire lock for namespace, skipping"
                );
            }
        }
    }
    info!(
        dispatched = dispatched_count,
        skipped = skipped_count,
        failed = failed_count,
        "Dispatched namespace indexing requests"
    );

    Ok(())
}
