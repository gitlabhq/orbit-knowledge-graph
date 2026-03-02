use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::metrics::DispatchMetrics;
use crate::clickhouse::ArrowClickHouseClient;
use crate::configuration::DispatcherConfiguration;
use crate::dispatcher::{DispatchError, Dispatcher};
use crate::locking::LockService;
use crate::modules::sdlc::locking::{LOCK_TTL, namespace_lock_key};
use crate::nats::NatsServices;
use crate::topic::NamespaceIndexingRequest;
use crate::types::{Envelope, Event};
use clickhouse_client::FromArrowColumn;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, organization_id
FROM siphon_knowledge_graph_enabled_namespaces
INNER JOIN siphon_namespaces on siphon_knowledge_graph_enabled_namespaces.root_namespace_id = siphon_namespaces.id
WHERE _siphon_deleted = false
"#;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceDispatcherConfig {
    #[serde(flatten)]
    pub dispatcher: DispatcherConfiguration,
}

pub struct NamespaceDispatcher {
    nats: Arc<dyn NatsServices>,
    lock_service: Arc<dyn LockService>,
    datalake: ArrowClickHouseClient,
    metrics: DispatchMetrics,
    config: NamespaceDispatcherConfig,
}

impl NamespaceDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        lock_service: Arc<dyn LockService>,
        datalake: ArrowClickHouseClient,
        metrics: DispatchMetrics,
        config: NamespaceDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            lock_service,
            datalake,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Dispatcher for NamespaceDispatcher {
    fn name(&self) -> &str {
        "sdlc.namespace"
    }

    fn dispatcher_config(&self) -> &DispatcherConfiguration {
        &self.config.dispatcher
    }

    async fn dispatch(&self) -> Result<(), DispatchError> {
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl NamespaceDispatcher {
    async fn dispatch_inner(&self) -> Result<(), DispatchError> {
        let query_start = Instant::now();
        let arrow_batches = self
            .datalake
            .query(ENABLED_NAMESPACE_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                DispatchError::new(error)
            })?;
        self.metrics
            .record_query_duration(query_start.elapsed().as_secs_f64());

        let namespace_ids = i64::extract_column(&arrow_batches, 0).map_err(DispatchError::new)?;
        let organization_ids =
            i64::extract_column(&arrow_batches, 1).map_err(DispatchError::new)?;

        debug!(
            enabled_namespaces = namespace_ids.len(),
            "found enabled namespaces to dispatch indexing requests for"
        );

        let watermark = Utc::now();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for (namespace_id, organization_id) in namespace_ids.iter().zip(organization_ids.iter()) {
            let lock_key = namespace_lock_key(*organization_id, *namespace_id);
            let acquired = self
                .lock_service
                .try_acquire(&lock_key, LOCK_TTL)
                .await
                .map_err(|error| {
                    self.metrics.record_error(self.name(), "lock");
                    DispatchError::new(error)
                })?;

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
            .map_err(|error| {
                self.metrics.record_error(self.name(), "publish");
                DispatchError::new(error)
            })?;

            self.nats
                .publish(&NamespaceIndexingRequest::topic(), &envelope)
                .await
                .map_err(|error| {
                    self.metrics.record_error(self.name(), "publish");
                    DispatchError::new(error)
                })?;

            dispatched += 1;
            debug!(
                namespace_id = *namespace_id,
                organization_id = *organization_id,
                "dispatched namespace indexing request"
            );
        }

        self.metrics
            .record_requests_published(self.name(), dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatched,
            skipped, "dispatched namespace indexing requests"
        );
        Ok(())
    }
}
