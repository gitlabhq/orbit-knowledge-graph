use async_trait::async_trait;
use siphon_proto::replication_event::Operation;
use tracing::{debug, info, warn};

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

const NAMESPACE_PROJECTS_QUERY: &str = r#"
SELECT project.id AS project_id, traversal_paths.traversal_path
FROM siphon_projects project
INNER JOIN project_namespace_traversal_paths traversal_paths
  ON project.id = traversal_paths.id
WHERE project._siphon_deleted = false
  AND startsWith(traversal_paths.traversal_path, {namespace_prefix:String})
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
            if event.operation == Operation::Delete as i32 {
                debug!("skipping delete event");
                continue;
            }

            let Some(root_namespace_id) = extractor.get_i64(event, "root_namespace_id") else {
                debug!("failed to extract root_namespace_id, skipping");
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
                warn!(root_namespace_id, error = %e, "failed to dispatch code backfill for namespace");
            }
        }

        Ok(())
    }
}

impl CodeBackfillDispatchHandler {
    async fn dispatch_projects_for_namespace(
        &self,
        context: &HandlerContext,
        root_namespace_id: i64,
    ) -> Result<(), HandlerError> {
        let namespace_prefix = format!("{root_namespace_id}/");

        let batches = self
            .datalake
            .query(NAMESPACE_PROJECTS_QUERY)
            .param("namespace_prefix", &namespace_prefix)
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

    use super::*;
    use crate::handler::Handler;
    use crate::modules::code::test_helpers::{build_replication_events, namespace_enabled_columns};
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};

    struct TestContext {
        handler: CodeBackfillDispatchHandler,
        mock_nats: Arc<MockNatsServices>,
    }

    impl TestContext {
        fn new() -> Self {
            let mock_nats = Arc::new(MockNatsServices::new());
            let handler = CodeBackfillDispatchHandler::new(
                crate::clickhouse::ClickHouseConfiguration::default().build_client(),
                CodeBackfillDispatchHandlerConfig::default(),
            );

            Self { handler, mock_nats }
        }

        fn handler_context(&self) -> HandlerContext {
            HandlerContext::new(
                Arc::new(MockDestination::new()),
                self.mock_nats.clone(),
                Arc::new(MockLockService::new()),
                ProgressNotifier::noop(),
            )
        }
    }

    #[tokio::test]
    async fn skips_delete_events() {
        use siphon_proto::replication_event::Operation;

        let ctx = TestContext::new();

        let payload = build_replication_events(vec![
            namespace_enabled_columns(42)
                .with_operation(Operation::Delete as i32)
                .build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[test]
    fn handler_name() {
        let ctx = TestContext::new();
        assert_eq!(ctx.handler.name(), "code_backfill_dispatch");
    }
}
