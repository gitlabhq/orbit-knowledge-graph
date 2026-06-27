use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use siphon_proto::LogicalReplicationEvents;
use siphon_proto::replication_event::Operation;
use tracing::{debug, warn};

use crate::nats::NatsServices;
use crate::orchestrator::scheduled::{ScheduledTaskMetrics, TaskError};
use crate::orchestrator::siphon::decoder::ColumnExtractor;
use crate::orchestrator::siphon::route::{CdcContext, Route, RouteOutcome};
use crate::orchestrator::siphon::subjects;
use crate::topic::CodeIndexingTaskRequest;
use crate::types::Envelope;

const METRIC_NAME: &str = "dispatch.code.task";

type ProjectBranch = (i64, String);

pub struct CodeIndexingTaskRoute {
    nats: Arc<dyn NatsServices>,
    metrics: ScheduledTaskMetrics,
}

impl CodeIndexingTaskRoute {
    pub fn new(nats: Arc<dyn NatsServices>, metrics: ScheduledTaskMetrics) -> Self {
        Self { nats, metrics }
    }

    fn collect_latest_requests(
        &self,
        events: &[LogicalReplicationEvents],
        ctx: &CdcContext,
    ) -> HashMap<ProjectBranch, CodeIndexingTaskRequest> {
        let mut latest: HashMap<ProjectBranch, CodeIndexingTaskRequest> = HashMap::new();

        for replication_events in events {
            let extractor = ColumnExtractor::new(replication_events);

            for event in &replication_events.events {
                if event.operation == Operation::InitialSnapshot as i32 {
                    debug!("skipping initial snapshot event");
                    continue;
                }

                let Some(task_id) = extractor.get_i64(event, "id") else {
                    warn!("failed to extract task id, skipping");
                    continue;
                };
                let Some(project_id) = extractor.get_i64(event, "project_id") else {
                    warn!("failed to extract project_id, skipping");
                    continue;
                };
                let Some(ref_name) = extractor.get_string(event, "ref") else {
                    warn!(task_id, "failed to extract ref, skipping");
                    continue;
                };
                let Some(commit_sha) = extractor.get_string(event, "commit_sha") else {
                    warn!(task_id, "failed to extract commit_sha, skipping");
                    continue;
                };
                let Some(traversal_path) = extractor.get_string(event, "traversal_path") else {
                    warn!(task_id, "failed to extract traversal_path, skipping");
                    continue;
                };

                let branch = ref_name
                    .strip_prefix("refs/heads/")
                    .unwrap_or(ref_name)
                    .to_string();

                let key = (project_id, branch.clone());

                let request = CodeIndexingTaskRequest {
                    task_id,
                    project_id,
                    branch: Some(branch),
                    commit_sha: Some(commit_sha.to_string()),
                    traversal_path: traversal_path.to_string(),
                    dispatch_id: ctx.dispatch_id,
                    campaign_id: ctx.campaign_id.clone(),
                };
                latest
                    .entry(key)
                    .and_modify(|existing| {
                        if request.task_id > existing.task_id {
                            *existing = request.clone();
                        }
                    })
                    .or_insert(request);
            }
        }

        latest
    }
}

#[async_trait]
impl Route for CodeIndexingTaskRoute {
    fn source_table(&self) -> &str {
        subjects::CODE_INDEXING_TASKS
    }

    async fn dispatch(
        &self,
        ctx: &CdcContext,
        events: &[LogicalReplicationEvents],
    ) -> Result<RouteOutcome, TaskError> {
        let requests = self.collect_latest_requests(events, ctx);
        let mut outcome = RouteOutcome::default();

        for request in requests.into_values() {
            let envelope = Envelope::new(&request).map_err(|error| {
                self.metrics.record_error(METRIC_NAME, "publish");
                TaskError::new(error)
            })?;

            match self
                .nats
                .publish(&request.publish_subscription(), &envelope)
                .await
            {
                Ok(()) => {
                    outcome.dispatched += 1;
                    debug!(
                        task_id = request.task_id,
                        project_id = request.project_id,
                        "dispatched code indexing task request"
                    );
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    outcome.skipped += 1;
                    debug!(
                        task_id = request.task_id,
                        project_id = request.project_id,
                        "skipped code indexing task request, already in-flight"
                    );
                }
                Err(error) => {
                    self.metrics.record_error(METRIC_NAME, "publish");
                    return Err(TaskError::new(error));
                }
            }
        }

        if outcome.dispatched > 0 || outcome.skipped > 0 {
            self.metrics
                .record_requests_published(METRIC_NAME, outcome.dispatched);
            self.metrics
                .record_requests_skipped(METRIC_NAME, outcome.skipped);
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::campaign::CampaignState;
    use crate::modules::code::test_helpers::{
        build_replication_events, code_indexing_task_columns,
    };
    use crate::orchestrator::siphon::Siphon;
    use crate::testkit::{MockNatsServices, TestEnvelopeFactory};
    use gkg_server_config::SiphonRouterConfig;
    use siphon_proto::replication_event::Operation;

    fn test_metrics() -> ScheduledTaskMetrics {
        ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter())
    }

    fn create_siphon(nats: Arc<MockNatsServices>) -> Siphon {
        let route = Arc::new(CodeIndexingTaskRoute::new(
            Arc::clone(&nats) as Arc<dyn NatsServices>,
            test_metrics(),
        ));
        Siphon::new(
            nats,
            test_metrics(),
            SiphonRouterConfig::default(),
            Arc::new(CampaignState::new()),
            vec![route],
        )
    }

    #[tokio::test]
    async fn dispatches_code_indexing_task_from_siphon_event() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(42, 123, "refs/heads/main", "abc123", "/org/project-123")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 1);

        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.task_id, 42);
        assert_eq!(request.project_id, 123);
        assert_eq!(request.branch.as_deref(), Some("main"));
        assert_eq!(request.commit_sha.as_deref(), Some("abc123"));
        assert_eq!(request.traversal_path, "/org/project-123");
    }

    #[tokio::test]
    async fn strips_refs_heads_prefix() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(
                1,
                100,
                "refs/heads/feature/test",
                "def456",
                "/org/project-100",
            )
            .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.branch.as_deref(), Some("feature/test"));
    }

    #[tokio::test]
    async fn skips_initial_snapshot_events() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(1, 100, "main", "abc123", "/org/project-100")
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert!(published.is_empty());
    }

    #[tokio::test]
    async fn no_messages_produces_no_dispatches() {
        let nats = Arc::new(MockNatsServices::new());

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert!(published.is_empty());
    }

    #[tokio::test]
    async fn publishes_to_correct_subject() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(1, 42, "refs/heads/main", "abc123", "/org/project-42")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(
            published[0].0.subject.as_ref(),
            "code.task.indexing.requested.42.bWFpbg"
        );
    }

    #[tokio::test]
    async fn deduplicates_same_project_branch_within_batch() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(1, 42, "refs/heads/main", "old_sha", "/org/project-42")
                .build(),
            code_indexing_task_columns(2, 42, "refs/heads/main", "new_sha", "/org/project-42")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 1);

        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.task_id, 2);
        assert_eq!(request.commit_sha.as_deref(), Some("new_sha"));
    }

    #[tokio::test]
    async fn deduplicates_across_messages_in_same_batch() {
        let nats = Arc::new(MockNatsServices::new());
        let first_message = build_replication_events(vec![
            code_indexing_task_columns(1, 42, "refs/heads/main", "old_sha", "/org/project-42")
                .build(),
        ]);
        let second_message = build_replication_events(vec![
            code_indexing_task_columns(5, 42, "refs/heads/main", "latest_sha", "/org/project-42")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(first_message));
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(second_message));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 1);

        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.task_id, 5);
        assert_eq!(request.commit_sha.as_deref(), Some("latest_sha"));
    }

    #[tokio::test]
    async fn keeps_distinct_project_branch_pairs() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(1, 42, "refs/heads/main", "sha1", "/org/project-42").build(),
            code_indexing_task_columns(2, 42, "refs/heads/develop", "sha2", "/org/project-42")
                .build(),
            code_indexing_task_columns(3, 99, "refs/heads/main", "sha3", "/org/project-99").build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 3);
    }

    #[tokio::test]
    async fn deduplicates_out_of_order_task_ids() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = build_replication_events(vec![
            code_indexing_task_columns(1, 42, "refs/heads/main", "aaa", "/org/project-42").build(),
            code_indexing_task_columns(3, 42, "refs/heads/main", "ccc", "/org/project-42").build(),
            code_indexing_task_columns(2, 42, "refs/heads/main", "bbb", "/org/project-42").build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 1);

        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.task_id, 3);
        assert_eq!(request.commit_sha.as_deref(), Some("ccc"));
    }
}
