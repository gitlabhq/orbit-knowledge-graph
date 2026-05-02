use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use siphon_proto::replication_event::Operation;
use tracing::{debug, info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::modules::code::config::subjects;
use crate::modules::code::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use crate::nats::NatsServices;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Subscription};
use clickhouse_client::FromArrowColumn;
use gkg_server_config::{NamespaceCodeBackfillDispatcherConfig, ScheduleConfiguration};

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

/// Project IDs that already have a checkpoint row under a given namespace's
/// traversal-path scope. Used to filter the dispatch list down to projects
/// that still need indexing for the current schema version.
const CHECKPOINTED_PROJECT_IDS_QUERY: &str = r#"
SELECT DISTINCT project_id
FROM {table:Identifier} FINAL
WHERE _deleted = false
  AND startsWith(traversal_path, {traversal_path:String})
"#;

const NAMESPACE_PROJECTS_QUERY: &str = r#"
SELECT id AS project_id, traversal_path
FROM project_namespace_traversal_paths
WHERE deleted = false
  AND startsWith(traversal_path, {traversal_path:String})
"#;

/// Enabled namespace ID + traversal path pairs from the datalake. Reads
/// `traversal_path` directly from the enabled namespaces table
/// (gitlab-org/gitlab!232941) instead of joining `namespace_traversal_paths`
/// per namespace.
const ENABLED_NAMESPACES_QUERY: &str = r#"
SELECT root_namespace_id, traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != ''
"#;

pub struct NamespaceCodeBackfillDispatcher {
    nats: Arc<dyn NatsServices>,
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: NamespaceCodeBackfillDispatcherConfig,
}

impl NamespaceCodeBackfillDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: NamespaceCodeBackfillDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            graph,
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
        self.dispatch_cdc_events().await?;
        self.dispatch_active_backfill().await?;
        Ok(())
    }

    /// Consume CDC events for newly-enabled namespaces and dispatch backfill.
    async fn dispatch_cdc_events(&self) -> Result<(), TaskError> {
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

            let enabled = self.extract_enabled_namespaces(&messages)?;

            for (namespace_id, traversal_path) in &enabled {
                let outcome = self
                    .dispatch_projects_code_indexing(*namespace_id, traversal_path)
                    .await?;
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

    /// Dispatches code backfill for every enabled namespace, every tick.
    ///
    /// Coverage-driven: for each namespace, the project list is filtered
    /// against the indexer's current-version checkpoint table so the
    /// dispatcher only publishes work that hasn't been done yet. Once a
    /// namespace's projects are fully checkpointed, this loop produces no
    /// publishes for it (the filter empties the project list).
    ///
    /// Replaces the prior "only when a `migrating` row exists" gate, which
    /// silently no-op'd after a migration completed and stranded any
    /// projects that hadn't been indexed during the brief migration window.
    async fn dispatch_active_backfill(&self) -> Result<(), TaskError> {
        let version = *SCHEMA_VERSION;

        // Datalake unreachable is a transient issue, not a task error. Mirror
        // the tolerance the old `read_migrating_version` gate had: log and
        // try again next tick.
        let enabled = match self.fetch_enabled_namespaces().await {
            Ok(rows) => rows,
            Err(e) => {
                self.metrics.record_error(self.name(), "datalake_query");
                warn!(error = %e, "failed to fetch enabled namespace IDs, skipping backfill");
                return Ok(());
            }
        };

        if enabled.is_empty() {
            return Ok(());
        }

        let mut total = DispatchOutcome {
            dispatched: 0,
            skipped: 0,
        };

        for (namespace_id, traversal_path) in &enabled {
            let outcome = self
                .dispatch_projects_code_indexing(*namespace_id, traversal_path)
                .await?;
            total.dispatched += outcome.dispatched;
            total.skipped += outcome.skipped;
        }

        if total.dispatched > 0 || total.skipped > 0 {
            self.metrics
                .record_requests_published(self.name(), total.dispatched);
            self.metrics
                .record_requests_skipped(self.name(), total.skipped);

            info!(
                version,
                dispatched = total.dispatched,
                skipped = total.skipped,
                "dispatched code backfill requests"
            );
        }

        Ok(())
    }

    /// Returns the set of project IDs whose checkpoint row already exists
    /// under `traversal_path` for the indexer's current schema version.
    async fn fetch_checkpointed_project_ids(
        &self,
        traversal_path: &str,
    ) -> Result<HashSet<i64>, TaskError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let batches = self
            .graph
            .query(CHECKPOINTED_PROJECT_IDS_QUERY)
            .param("table", &table)
            .param("traversal_path", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;

        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        Ok(ids.into_iter().collect())
    }

    /// Returns (root_namespace_id, traversal_path) for every currently-enabled
    /// namespace. Reads `traversal_path` from the enabled namespaces table
    /// directly (gitlab-org/gitlab!232941); the prior implementation joined
    /// `namespace_traversal_paths` per namespace.
    async fn fetch_enabled_namespaces(&self) -> Result<Vec<(i64, String)>, TaskError> {
        let batches = self
            .datalake
            .query(ENABLED_NAMESPACES_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;

        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        Ok(ids.into_iter().zip(paths).collect())
    }

    /// Pulls (namespace_id, traversal_path) from inserted CDC events on the
    /// enabled-namespaces table. The replicated row carries `traversal_path`
    /// directly, so no follow-up lookup is needed.
    fn extract_enabled_namespaces(
        &self,
        messages: &[crate::nats::NatsMessage],
    ) -> Result<Vec<(i64, String)>, TaskError> {
        let mut rows: Vec<(i64, String)> = Vec::new();

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

                let Some(traversal_path) = extractor.get_string(event, "traversal_path") else {
                    warn!(
                        root_namespace_id,
                        "CDC event missing traversal_path; skipping (re-tries next tick via active backfill)"
                    );
                    continue;
                };

                if traversal_path.is_empty() {
                    warn!(
                        root_namespace_id,
                        "CDC event has empty traversal_path; skipping to avoid prefix-matching every project"
                    );
                    continue;
                }

                rows.push((root_namespace_id, traversal_path.to_string()));
            }
        }

        // De-dupe within a single batch.
        rows.sort();
        rows.dedup();
        Ok(rows)
    }

    async fn dispatch_projects_code_indexing(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<DispatchOutcome, TaskError> {
        let no_work = DispatchOutcome {
            dispatched: 0,
            skipped: 0,
        };

        let projects = self.fetch_namespace_projects(traversal_path).await?;

        if projects.is_empty() {
            debug!(namespace_id, "no pending projects in namespace");
            return Ok(no_work);
        }

        // Skip projects that already have a checkpoint for the current
        // schema version. Filtering here keeps the publish loop bounded by
        // the un-indexed remainder rather than the full project set, which
        // matters at scale: a namespace with thousands of projects produces
        // O(remaining) NATS publishes per tick, not O(total).
        let checkpointed = self.fetch_checkpointed_project_ids(traversal_path).await?;
        let pending_count_before_filter = projects.len();
        let projects: Vec<PendingProject> = projects
            .into_iter()
            .filter(|p| !checkpointed.contains(&p.project_id))
            .collect();

        if projects.is_empty() {
            debug!(namespace_id, "all projects already checkpointed");
            return Ok(no_work);
        }

        info!(
            namespace_id,
            count = projects.len(),
            already_checkpointed = pending_count_before_filter - projects.len(),
            "dispatching code backfill for pending projects"
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
        let empty = &std::collections::HashMap::new();
        let graph =
            ArrowClickHouseClient::new("http://localhost:0", "default", "default", None, empty);
        let datalake =
            ArrowClickHouseClient::new("http://localhost:0", "default", "default", None, empty);
        NamespaceCodeBackfillDispatcher::new(
            nats,
            graph,
            datalake,
            test_metrics(),
            NamespaceCodeBackfillDispatcherConfig::default(),
        )
    }

    fn namespace_enabled_columns(root_namespace_id: i64) -> EventBuilder {
        let traversal_path = format!("1/{root_namespace_id}/");
        EventBuilder::new()
            .with_i64("root_namespace_id", root_namespace_id)
            .with_string("traversal_path", &traversal_path)
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
        let rows = dispatcher.extract_enabled_namespaces(&messages).unwrap();

        assert_eq!(
            rows,
            vec![(100, "1/100/".to_string()), (200, "1/200/".to_string())]
        );
    }

    #[test]
    fn enabled_namespaces_query_filters_deleted_and_pulls_path() {
        assert!(ENABLED_NAMESPACES_QUERY.contains("_siphon_deleted = false"));
        assert!(ENABLED_NAMESPACES_QUERY.contains("traversal_path"));
        assert!(
            ENABLED_NAMESPACES_QUERY.contains("traversal_path != ''"),
            "must skip rows where the dictionary-backed default hasn't \
             populated yet — empty path would prefix-match every project"
        );
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
        let rows = dispatcher.extract_enabled_namespaces(&messages).unwrap();

        assert_eq!(rows, vec![(300, "1/300/".to_string())]);
    }
}
