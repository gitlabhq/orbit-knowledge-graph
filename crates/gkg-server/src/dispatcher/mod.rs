mod extract;

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use etl_engine::clickhouse::ClickHouseError;
use etl_engine::nats::{
    KvBucketConfig, KvPutOptions, KvPutResult, NatsBroker, NatsServices, NatsServicesImpl,
};
use etl_engine::types::{Envelope, Event};
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::indexer::modules::sdlc::locking::{
    INDEXING_LOCKS_BUCKET, LOCK_TTL, global_lock_key, namespace_lock_key,
};
use crate::indexer::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
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
    NatsConnection(#[from] etl_engine::nats::NatsError),

    #[error("ClickHouse query error: {0}")]
    ClickHouseQueryError(ClickHouseError),

    #[error("Invalid column type: expected {expected}")]
    InvalidColumnType { expected: &'static str },

    #[error("Failed to serialize message: {0}")]
    Serialization(#[from] etl_engine::types::SerializationError),

    #[error("Failed to publish message: {0}")]
    Publish(etl_engine::nats::NatsError),

    #[error("Failed to acquire lock: {0}")]
    LockAcquisition(etl_engine::nats::NatsError),
}

struct Dispatcher {
    broker: Arc<NatsBroker>,
    nats_services: NatsServicesImpl,
}

enum LockResult {
    Acquired,
    AlreadyHeld,
}

impl Dispatcher {
    fn new(broker: Arc<NatsBroker>) -> Self {
        let nats_services = NatsServicesImpl::new(Arc::clone(&broker));
        Self {
            broker,
            nats_services,
        }
    }

    async fn try_acquire_lock(&self, key: &str) -> Result<LockResult, DispatcherError> {
        let options = KvPutOptions::create_with_ttl(LOCK_TTL);
        let result = self
            .nats_services
            .kv_put(INDEXING_LOCKS_BUCKET, key, Bytes::new(), options)
            .await
            .map_err(DispatcherError::LockAcquisition)?;

        match result {
            KvPutResult::Success(_) => Ok(LockResult::Acquired),
            KvPutResult::AlreadyExists => Ok(LockResult::AlreadyHeld),
            KvPutResult::RevisionMismatch => Ok(LockResult::AlreadyHeld),
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
