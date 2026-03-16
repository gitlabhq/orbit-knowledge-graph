use async_trait::async_trait;
use siphon_proto::replication_event::Operation;
use tracing::{debug, error, info, warn};

use serde::{Deserialize, Serialize};

use super::config::subjects;
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use crate::clickhouse::ArrowClickHouseClient;
use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::CodeBackfillRequest;
use crate::types::{Envelope, Subscription};
use clickhouse_client::FromArrowColumn;

fn default_events_stream_name() -> String {
    "siphon_stream_main_db".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CodeBackfillDispatchHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,
}

impl Default for CodeBackfillDispatchHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            events_stream_name: default_events_stream_name(),
        }
    }
}

const NAMESPACE_TRAVERSAL_PATH_QUERY: &str = r#"
SELECT traversal_path
FROM namespace_traversal_paths
WHERE id = {namespace_id:Int64}
  AND deleted = false
LIMIT 1
"#;

const NAMESPACE_PROJECTS_QUERY: &str = r#"
SELECT id AS project_id, traversal_path
FROM project_namespace_traversal_paths
WHERE deleted = false
  AND startsWith(traversal_path, {traversal_path:String})
"#;

pub struct CodeBackfillDispatchHandler {
    datalake: ArrowClickHouseClient,
    config: CodeBackfillDispatchHandlerConfig,
}

impl CodeBackfillDispatchHandler {
    pub fn new(datalake: ArrowClickHouseClient, config: CodeBackfillDispatchHandlerConfig) -> Self {
        Self { datalake, config }
    }
}

#[async_trait]
impl Handler for CodeBackfillDispatchHandler {
    fn name(&self) -> &str {
        "code_backfill_dispatch"
    }

    fn subscription(&self) -> Subscription {
        Subscription::new(
            self.config.events_stream_name.clone(),
            format!(
                "{}.{}",
                self.config.events_stream_name,
                subjects::KNOWLEDGE_GRAPH_ENABLED_NAMESPACES
            ),
        )
        .manage_stream(false)
        .dead_letter_on_exhaustion(true)
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let replication_events = decode_logical_replication_events(&message.payload)
            .inspect_err(|e| warn!(message_id = %message.id.0, error = %e, "failed to decode namespace enabled event"))
            .map_err(HandlerError::Processing)?;

        let extractor = ColumnExtractor::new(&replication_events);

        for event in &replication_events.events {
            if event.operation != Operation::Insert as i32 {
                debug!(operation = event.operation, "skipping non-insert event");
                continue;
            }

            let Some(root_namespace_id) = extractor.get_i64(event, "root_namespace_id") else {
                warn!("failed to extract root_namespace_id, skipping");
                continue;
            };

            info!(
                root_namespace_id,
                "namespace enabled, dispatching code backfill"
            );

            if let Err(e) = self
                .dispatch_projects_for_namespace(&context, root_namespace_id)
                .await
            {
                error!(root_namespace_id, error = %e, "failed to dispatch code backfill for namespace");
            }
        }

        Ok(())
    }
}

impl CodeBackfillDispatchHandler {
    async fn resolve_namespace_traversal_path(
        &self,
        root_namespace_id: i64,
    ) -> Result<Option<String>, HandlerError> {
        let batches = self
            .datalake
            .query(NAMESPACE_TRAVERSAL_PATH_QUERY)
            .param("namespace_id", root_namespace_id)
            .fetch_arrow()
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to query namespace traversal path: {e}"))
            })?;

        let paths = String::extract_column(&batches, 0)
            .map_err(|e| HandlerError::Processing(e.to_string()))?;

        Ok(paths.into_iter().next())
    }

    async fn dispatch_projects_for_namespace(
        &self,
        context: &HandlerContext,
        root_namespace_id: i64,
    ) -> Result<(), HandlerError> {
        let Some(traversal_path) = self
            .resolve_namespace_traversal_path(root_namespace_id)
            .await?
        else {
            error!(root_namespace_id, "namespace traversal path not found");
            return Ok(());
        };

        let batches = self
            .datalake
            .query(NAMESPACE_PROJECTS_QUERY)
            .param("traversal_path", &traversal_path)
            .fetch_arrow()
            .await
            .map_err(|e| {
                HandlerError::Processing(format!("failed to query namespace projects: {e}"))
            })?;

        let project_ids = i64::extract_column(&batches, 0)
            .map_err(|e| HandlerError::Processing(e.to_string()))?;
        let traversal_paths = String::extract_column(&batches, 1)
            .map_err(|e| HandlerError::Processing(e.to_string()))?;

        if project_ids.is_empty() {
            debug!(root_namespace_id, "no projects found in namespace");
            return Ok(());
        }

        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for (project_id, traversal_path) in project_ids.iter().zip(traversal_paths.iter()) {
            let request = CodeBackfillRequest {
                project_id: *project_id,
                traversal_path: traversal_path.clone(),
            };

            let subscription = request.publish_subscription();
            let envelope = Envelope::new(&request)
                .map_err(|e| HandlerError::Processing(format!("failed to create envelope: {e}")))?;

            match context.nats.publish(&subscription, &envelope).await {
                Ok(()) => {
                    dispatched += 1;
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    skipped += 1;
                }
                Err(e) => {
                    return Err(HandlerError::Processing(format!(
                        "failed to publish code indexing request: {e}"
                    )));
                }
            }
        }

        info!(
            root_namespace_id,
            dispatched, skipped, "dispatched code backfill requests for namespace"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use prost::Message;
    use siphon_proto::replication_event::Column;
    use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};

    use super::*;
    use crate::handler::Handler;
    use crate::testkit::{MockLockService, MockNatsServices, TestEnvelopeFactory};

    fn build_payload(root_namespace_id: i64, operation: i32) -> Bytes {
        let columns = vec![Column {
            column_index: 0,
            value: Some(Value {
                value: Some(value::Value::Int64Value(root_namespace_id)),
            }),
        }];

        let encoded = LogicalReplicationEvents {
            event: 1,
            table: "knowledge_graph_enabled_namespaces".into(),
            schema: "public".into(),
            application_identifier: "test".into(),
            columns: vec!["root_namespace_id".to_string()],
            events: vec![ReplicationEvent { operation, columns }],
            version_hash: 0,
        }
        .encode_to_vec();

        let compressed = zstd::encode_all(encoded.as_slice(), 0).expect("compression failed");
        Bytes::from(compressed)
    }

    fn test_context(nats: Arc<MockNatsServices>) -> HandlerContext {
        use crate::nats::ProgressNotifier;
        use crate::testkit::MockDestination;

        HandlerContext::new(
            Arc::new(MockDestination::new()),
            nats,
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
        )
    }

    #[tokio::test]
    async fn skips_non_insert_events() {
        let datalake = ArrowClickHouseClient::new("http://localhost:0", "default", "default", None);
        let handler = CodeBackfillDispatchHandler::new(
            datalake,
            CodeBackfillDispatchHandlerConfig::default(),
        );

        let nats = Arc::new(MockNatsServices::new());
        let context = test_context(Arc::clone(&nats));
        let envelope = TestEnvelopeFactory::with_bytes(build_payload(200, 4));

        let result = handler.handle(context, envelope).await;
        assert!(result.is_ok(), "dispatch handler failed: {:?}", result);

        let published = nats.get_published();
        assert!(
            published.is_empty(),
            "expected no backfill requests for delete event, got {}",
            published.len()
        );
    }
}
