use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use clickhouse_client::FromArrowColumn;
use ontology::EtlScope;
use tracing::{debug, info};

use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::{EntityIndexingRequest, IndexingScope};
use crate::types::Envelope;
use gkg_server_config::{EntityDispatcherConfig, ScheduleConfiguration};

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != ''
  AND traversal_path != '0/'
"#;

pub struct EntityDescriptor {
    pub entity_kind: String,
    pub scope: EtlScope,
}

pub struct EntityDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: EntityDispatcherConfig,
    entities: Vec<EntityDescriptor>,
}

impl EntityDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: EntityDispatcherConfig,
        entities: Vec<EntityDescriptor>,
    ) -> Self {
        Self {
            nats,
            datalake,
            metrics,
            config,
            entities,
        }
    }
}

#[async_trait]
impl ScheduledTask for EntityDispatcher {
    fn name(&self) -> &str {
        "dispatch.sdlc.entity"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

struct EnabledNamespace {
    namespace_id: i64,
    traversal_path: String,
}

enum PublishOutcome {
    Published,
    Skipped,
}

impl EntityDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let namespaces = self.load_enabled_namespaces().await?;

        let watermark = Utc::now();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for entity in &self.entities {
            let scopes: Vec<IndexingScope> = match entity.scope {
                EtlScope::Global => vec![IndexingScope::Global],
                EtlScope::Namespaced => namespaces
                    .iter()
                    .map(|ns| IndexingScope::Namespace {
                        namespace_id: ns.namespace_id,
                        traversal_path: ns.traversal_path.clone(),
                    })
                    .collect(),
            };

            for scope in scopes {
                let request = EntityIndexingRequest {
                    entity_kind: entity.entity_kind.clone(),
                    watermark,
                    scope,
                };
                match self.publish_request(&request).await? {
                    PublishOutcome::Published => dispatched += 1,
                    PublishOutcome::Skipped => skipped += 1,
                }
            }
        }

        self.metrics
            .record_requests_published(self.name(), dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatched,
            skipped,
            entity_count = self.entities.len(),
            "dispatched entity indexing requests"
        );
        Ok(())
    }

    async fn load_enabled_namespaces(&self) -> Result<Vec<EnabledNamespace>, TaskError> {
        let query_start = Instant::now();
        let arrow_batches = self
            .datalake
            .query(ENABLED_NAMESPACE_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;
        self.metrics
            .record_query_duration("enabled_namespaces", query_start.elapsed().as_secs_f64());

        let namespace_ids = i64::extract_column(&arrow_batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&arrow_batches, 1).map_err(TaskError::new)?;

        let namespaces: Vec<EnabledNamespace> = namespace_ids
            .into_iter()
            .zip(traversal_paths)
            .map(|(namespace_id, traversal_path)| EnabledNamespace {
                namespace_id,
                traversal_path,
            })
            .collect();

        debug!(
            enabled_namespaces = namespaces.len(),
            "loaded enabled namespaces"
        );
        Ok(namespaces)
    }

    async fn publish_request(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<PublishOutcome, TaskError> {
        let subscription = request.publish_subscription();
        let envelope = Envelope::new(request).map_err(|error| {
            self.metrics.record_error(self.name(), "publish");
            TaskError::new(error)
        })?;

        match self.nats.publish(&subscription, &envelope).await {
            Ok(()) => {
                debug!(
                    entity_kind = %request.entity_kind,
                    scope = ?request.scope,
                    "dispatched entity indexing request"
                );
                Ok(PublishOutcome::Published)
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                debug!(
                    entity_kind = %request.entity_kind,
                    scope = ?request.scope,
                    "skipped entity indexing request, already in-flight"
                );
                Ok(PublishOutcome::Skipped)
            }
            Err(error) => {
                self.metrics.record_error(self.name(), "publish");
                Err(TaskError::new(error))
            }
        }
    }
}
