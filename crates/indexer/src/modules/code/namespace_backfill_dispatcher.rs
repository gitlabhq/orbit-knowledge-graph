use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use siphon_proto::replication_event::Operation;
use tracing::{debug, info, warn};

use super::config::subjects;
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use crate::clickhouse::ArrowClickHouseClient;
use crate::configuration::{NamespaceCodeBackfillDispatcherConfig, ScheduleConfiguration};
use crate::nats::NatsServices;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Subscription};
use clickhouse_client::FromArrowColumn;

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

pub struct NamespaceCodeBackfillDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: NamespaceCodeBackfillDispatcherConfig,
}

impl NamespaceCodeBackfillDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: NamespaceCodeBackfillDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            datalake,
            metrics,
            config,
        }
    }

    fn siphon_subscription(&self) -> Subscription {
        Subscription::new(
            self.config.events_stream_name.clone(),
            format!(
                "{}.{}",
                self.config.events_stream_name,
                subjects::KNOWLEDGE_GRAPH_ENABLED_NAMESPACES
            ),
        )
        .manage_stream(false)
    }
}

#[async_trait]
impl ScheduledTask for NamespaceCodeBackfillDispatcher {
    fn name(&self) -> &str {
        "dispatch.code.namespace_backfill"
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

struct DispatchOutcome {
    dispatched: u64,
    skipped: u64,
}

struct PendingProject {
    project_id: i64,
    traversal_path: String,
}

impl NamespaceCodeBackfillDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let subscription = self.siphon_subscription();
        let mut total = DispatchOutcome {
            dispatched: 0,
            skipped: 0,
        };

        loop {
            let messages = self
                .nats
                .consume_pending(&subscription, self.config.batch_size)
                .await
                .map_err(|error| {
                    self.metrics.record_error(self.name(), "consume");
                    TaskError::new(error)
                })?;

            if messages.is_empty() {
                break;
            }

            let namespace_ids = self.extract_namespace_ids(&messages)?;

            for namespace_id in &namespace_ids {
                let outcome = self.dispatch_projects_code_indexing(*namespace_id).await?;
                total.dispatched += outcome.dispatched;
                total.skipped += outcome.skipped;
            }

            for message in messages {
                message.ack().await.map_err(|error| {
                    self.metrics.record_error(self.name(), "ack");
                    TaskError::new(error)
                })?;
            }
        }

        if total.dispatched > 0 || total.skipped > 0 {
            self.metrics
                .record_requests_published(self.name(), total.dispatched);
            self.metrics
                .record_requests_skipped(self.name(), total.skipped);

            info!(
                dispatched = total.dispatched,
                skipped = total.skipped,
                "dispatched namespace code backfill requests"
            );
        }

        Ok(())
    }

    fn extract_namespace_ids(
        &self,
        messages: &[crate::nats::NatsMessage],
    ) -> Result<Vec<i64>, TaskError> {
        let mut namespace_ids = Vec::new();

        for message in messages {
            let replication_events = decode_logical_replication_events(&message.envelope.payload)
                .map_err(|error| {
                self.metrics.record_error(self.name(), "decode");
                TaskError::new(error)
            })?;

            let extractor = ColumnExtractor::new(&replication_events);

            for event in &replication_events.events {
                let is_insert = event.operation == Operation::Insert as i32;
                let is_snapshot = event.operation == Operation::InitialSnapshot as i32;

                if !is_insert && !is_snapshot {
                    debug!(
                        operation = event.operation,
                        "skipping non-insert/snapshot event"
                    );
                    continue;
                }

                let Some(root_namespace_id) = extractor.get_i64(event, "root_namespace_id") else {
                    warn!("failed to extract root_namespace_id, skipping");
                    continue;
                };

                namespace_ids.push(root_namespace_id);
            }
        }

        namespace_ids.sort_unstable();
        namespace_ids.dedup();
        Ok(namespace_ids)
    }

    async fn resolve_namespace_traversal_path(
        &self,
        namespace_id: i64,
    ) -> Result<Option<String>, TaskError> {
        let batches = self
            .datalake
            .query(NAMESPACE_TRAVERSAL_PATH_QUERY)
            .param("namespace_id", namespace_id)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;

        let paths = String::extract_column(&batches, 0).map_err(TaskError::new)?;
        Ok(paths.into_iter().next())
    }

    async fn dispatch_projects_code_indexing(
        &self,
        namespace_id: i64,
    ) -> Result<DispatchOutcome, TaskError> {
        let no_work = DispatchOutcome {
            dispatched: 0,
            skipped: 0,
        };

        let Some(traversal_path) = self.resolve_namespace_traversal_path(namespace_id).await?
        else {
            warn!(namespace_id, "namespace traversal path not found, skipping");
            return Ok(no_work);
        };

        let projects = self.fetch_namespace_projects(&traversal_path).await?;

        if projects.is_empty() {
            debug!(namespace_id, "no pending projects in namespace");
            return Ok(no_work);
        }

        info!(
            namespace_id,
            count = projects.len(),
            "namespace enabled, dispatching code backfill"
        );

        let mut outcome = DispatchOutcome {
            dispatched: 0,
            skipped: 0,
        };

        for project in &projects {
            let request = CodeIndexingTaskRequest {
                task_id: 0,
                project_id: project.project_id,
                branch: None,
                commit_sha: None,
                traversal_path: project.traversal_path.clone(),
            };

            let subscription = request.publish_subscription();
            let envelope = Envelope::new(&request).map_err(|error| {
                self.metrics.record_error(self.name(), "publish");
                TaskError::new(error)
            })?;

            match self.nats.publish(&subscription, &envelope).await {
                Ok(()) => {
                    outcome.dispatched += 1;
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    outcome.skipped += 1;
                }
                Err(error) => {
                    self.metrics.record_error(self.name(), "publish");
                    return Err(TaskError::new(error));
                }
            }
        }

        debug!(
            namespace_id,
            outcome.dispatched, outcome.skipped, "dispatched code backfill for namespace"
        );

        Ok(outcome)
    }

    async fn fetch_namespace_projects(
        &self,
        traversal_path: &str,
    ) -> Result<Vec<PendingProject>, TaskError> {
        let query_start = Instant::now();
        let batches = self
            .datalake
            .query(NAMESPACE_PROJECTS_QUERY)
            .param("traversal_path", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;
        self.metrics.record_query_duration(
            "namespace_pending_projects",
            query_start.elapsed().as_secs_f64(),
        );

        let project_ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;

        Ok(project_ids
            .into_iter()
            .zip(traversal_paths)
            .map(|(project_id, traversal_path)| PendingProject {
                project_id,
                traversal_path,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::modules::code::test_helpers::{EventBuilder, build_replication_events_for_table};
    use crate::scheduler::ScheduledTaskMetrics;
    use crate::testkit::{MockNatsServices, TestEnvelopeFactory};
    use siphon_proto::replication_event::Operation;

    fn test_metrics() -> ScheduledTaskMetrics {
        ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter())
    }

    fn create_dispatcher(nats: Arc<MockNatsServices>) -> NamespaceCodeBackfillDispatcher {
        let datalake = ArrowClickHouseClient::new(
            "http://localhost:0",
            "default",
            "default",
            None,
            &std::collections::HashMap::new(),
        );
        NamespaceCodeBackfillDispatcher::new(
            nats,
            datalake,
            test_metrics(),
            NamespaceCodeBackfillDispatcherConfig::default(),
        )
    }

    fn namespace_enabled_columns(root_namespace_id: i64) -> EventBuilder {
        EventBuilder::new().with_i64("root_namespace_id", root_namespace_id)
    }

    fn build_namespace_events(
        events: Vec<(Vec<String>, siphon_proto::ReplicationEvent)>,
    ) -> bytes::Bytes {
        build_replication_events_for_table("knowledge_graph_enabled_namespaces", events)
    }

    #[tokio::test]
    async fn no_messages_produces_no_dispatches() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = create_dispatcher(Arc::clone(&nats));

        dispatcher.run().await.unwrap();

        let published = nats.get_published();
        assert!(published.is_empty());
    }

    #[tokio::test]
    async fn skips_delete_events() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_namespace_events(vec![
            namespace_enabled_columns(100)
                .with_operation(Operation::Delete as i32)
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        let dispatcher = create_dispatcher(Arc::clone(&nats));
        dispatcher.run().await.unwrap();

        let published = nats.get_published();
        assert!(published.is_empty());
    }

    #[tokio::test]
    async fn extracts_namespace_ids_from_insert_events() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_namespace_events(vec![
            namespace_enabled_columns(100).build(),
            namespace_enabled_columns(200).build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        let dispatcher = create_dispatcher(Arc::clone(&nats));
        let messages = nats
            .consume_pending(&dispatcher.siphon_subscription(), 100)
            .await
            .unwrap();
        let ids = dispatcher.extract_namespace_ids(&messages).unwrap();

        assert_eq!(ids, vec![100, 200]);
    }

    #[tokio::test]
    async fn extracts_namespace_ids_from_snapshot_events() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_namespace_events(vec![
            namespace_enabled_columns(300)
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        let dispatcher = create_dispatcher(Arc::clone(&nats));
        let messages = nats
            .consume_pending(&dispatcher.siphon_subscription(), 100)
            .await
            .unwrap();
        let ids = dispatcher.extract_namespace_ids(&messages).unwrap();

        assert_eq!(ids, vec![300]);
    }
}
