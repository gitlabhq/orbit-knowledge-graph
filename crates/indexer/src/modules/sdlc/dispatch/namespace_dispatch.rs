use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, info};

use crate::clickhouse::ArrowClickHouseClient;
use crate::dispatcher::extract::FromArrowColumn;
use crate::dispatcher::{DispatchError, Dispatcher};
use crate::locking::LockService;
use crate::modules::sdlc::locking::{LOCK_TTL, namespace_lock_key};
use crate::nats::NatsServices;
use crate::topic::NamespaceIndexingRequest;
use crate::types::{Envelope, Event};

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, organization_id
FROM siphon_knowledge_graph_enabled_namespaces
INNER JOIN siphon_namespaces on siphon_knowledge_graph_enabled_namespaces.root_namespace_id = siphon_namespaces.id
WHERE _siphon_deleted = false
"#;

pub struct NamespaceDispatcher {
    nats: Arc<dyn NatsServices>,
    lock_service: Arc<dyn LockService>,
    datalake: ArrowClickHouseClient,
}

impl NamespaceDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        lock_service: Arc<dyn LockService>,
        datalake: ArrowClickHouseClient,
    ) -> Self {
        Self {
            nats,
            lock_service,
            datalake,
        }
    }
}

#[async_trait]
impl Dispatcher for NamespaceDispatcher {
    fn name(&self) -> &str {
        "sdlc.namespace"
    }

    async fn dispatch(&self) -> Result<(), DispatchError> {
        let arrow_batches = self
            .datalake
            .query(ENABLED_NAMESPACE_QUERY)
            .fetch_arrow()
            .await
            .map_err(DispatchError::new)?;

        let namespace_ids = i64::extract_column(&arrow_batches, 0).map_err(DispatchError::new)?;
        let organization_ids =
            i64::extract_column(&arrow_batches, 1).map_err(DispatchError::new)?;

        debug!(
            enabled_namespaces = namespace_ids.len(),
            "found enabled namespaces to dispatch indexing requests for"
        );

        let watermark = Utc::now();
        let mut dispatched = 0;
        let mut skipped = 0;

        for (namespace_id, organization_id) in namespace_ids.iter().zip(organization_ids.iter()) {
            let lock_key = namespace_lock_key(*organization_id, *namespace_id);
            let acquired = self
                .lock_service
                .try_acquire(&lock_key, LOCK_TTL)
                .await
                .map_err(DispatchError::new)?;

            if !acquired {
                skipped += 1;
                debug!(
                    namespace_id = *namespace_id,
                    organization_id = *organization_id,
                    "skipped namespace indexing request, lock already held"
                );
                continue;
            }

            let envelope = Envelope::new(&NamespaceIndexingRequest {
                organization: *organization_id,
                namespace: *namespace_id,
                watermark,
            })
            .map_err(DispatchError::new)?;

            self.nats
                .publish(&NamespaceIndexingRequest::topic(), &envelope)
                .await
                .map_err(DispatchError::new)?;

            dispatched += 1;
            debug!(
                namespace_id = *namespace_id,
                organization_id = *organization_id,
                "dispatched namespace indexing request"
            );
        }

        info!(
            dispatched,
            skipped, "dispatched namespace indexing requests"
        );
        Ok(())
    }
}
