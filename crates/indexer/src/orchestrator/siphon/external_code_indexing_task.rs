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

const METRIC_NAME: &str = "dispatch.code.task.external";

const SOURCE_TYPE_EXTERNAL: &str = "external_repository";

type ExternalRepositoryBranch = (i64, String);

pub struct ExternalCodeIndexingTaskRoute {
    nats: Arc<dyn NatsServices>,
    metrics: ScheduledTaskMetrics,
}

impl ExternalCodeIndexingTaskRoute {
    pub fn new(nats: Arc<dyn NatsServices>, metrics: ScheduledTaskMetrics) -> Self {
        Self { nats, metrics }
    }

    fn collect_latest_requests(
        &self,
        events: &[LogicalReplicationEvents],
        ctx: &CdcContext,
    ) -> HashMap<ExternalRepositoryBranch, CodeIndexingTaskRequest> {
        let mut latest: HashMap<ExternalRepositoryBranch, CodeIndexingTaskRequest> = HashMap::new();

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
                let Some(external_repository_id) =
                    extractor.get_i64(event, "external_repository_id")
                else {
                    warn!(
                        task_id,
                        "failed to extract external_repository_id, skipping"
                    );
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

                let key = (external_repository_id, branch.clone());

                let request = CodeIndexingTaskRequest {
                    task_id,
                    project_id: 0,
                    branch: Some(branch),
                    commit_sha: Some(commit_sha.to_string()),
                    traversal_path: traversal_path.to_string(),
                    dispatch_id: ctx.dispatch_id,
                    campaign_id: ctx.campaign_id.clone(),
                    source_type: Some(SOURCE_TYPE_EXTERNAL.to_string()),
                    external_repository_id: Some(external_repository_id),
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
impl Route for ExternalCodeIndexingTaskRoute {
    fn source_table(&self) -> &str {
        subjects::EXTERNAL_CODE_INDEXING_TASKS
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
                        external_repository_id = request.external_repository_id,
                        "dispatched external code indexing task request"
                    );
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    outcome.skipped += 1;
                    debug!(
                        task_id = request.task_id,
                        external_repository_id = request.external_repository_id,
                        "skipped external code indexing task request, already in-flight"
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
        build_replication_events_for_table, external_code_indexing_task_columns,
    };
    use crate::orchestrator::siphon::Siphon;
    use crate::testkit::{MockNatsServices, TestEnvelopeFactory};
    use gkg_server_config::SiphonRouterConfig;
    use siphon_proto::replication_event::Operation;

    fn test_metrics() -> ScheduledTaskMetrics {
        ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter())
    }

    fn external_events(events: Vec<(Vec<String>, siphon_proto::ReplicationEvent)>) -> bytes::Bytes {
        build_replication_events_for_table(subjects::EXTERNAL_CODE_INDEXING_TASKS, events)
    }

    fn create_siphon(nats: Arc<MockNatsServices>) -> Siphon {
        let route = Arc::new(ExternalCodeIndexingTaskRoute::new(
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
    async fn dispatches_external_task_with_source_fields() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = external_events(vec![
            external_code_indexing_task_columns(42, 7, "refs/heads/main", "abc123", "/org/ext-7")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 1);

        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&published[0].1.payload).unwrap();
        assert_eq!(request.task_id, 42);
        assert_eq!(request.project_id, 0);
        assert_eq!(request.external_repository_id, Some(7));
        assert_eq!(request.source_type.as_deref(), Some("external_repository"));
        assert!(request.is_external_repository());
        assert_eq!(request.branch.as_deref(), Some("main"));
        assert_eq!(request.commit_sha.as_deref(), Some("abc123"));
        assert_eq!(request.traversal_path, "/org/ext-7");
    }

    #[tokio::test]
    async fn publishes_to_external_id_keyed_subject() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = external_events(vec![
            external_code_indexing_task_columns(1, 42, "refs/heads/main", "abc123", "/org/ext-42")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(
            published[0].0.subject.as_ref(),
            "code.task.indexing.requested.ext-42.bWFpbg"
        );
    }

    #[tokio::test]
    async fn distinct_external_repos_do_not_collide_on_same_branch() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = external_events(vec![
            external_code_indexing_task_columns(1, 7, "refs/heads/main", "sha7", "/org/ext-7")
                .build(),
            external_code_indexing_task_columns(2, 8, "refs/heads/main", "sha8", "/org/ext-8")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 2);
        let subjects: Vec<&str> = published.iter().map(|p| p.0.subject.as_ref()).collect();
        assert!(subjects.contains(&"code.task.indexing.requested.ext-7.bWFpbg"));
        assert!(subjects.contains(&"code.task.indexing.requested.ext-8.bWFpbg"));
    }

    #[tokio::test]
    async fn deduplicates_same_external_repo_branch_keeping_latest_task() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = external_events(vec![
            external_code_indexing_task_columns(1, 7, "refs/heads/main", "old_sha", "/org/ext-7")
                .build(),
            external_code_indexing_task_columns(2, 7, "refs/heads/main", "new_sha", "/org/ext-7")
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
    async fn skips_initial_snapshot_events() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = external_events(vec![
            external_code_indexing_task_columns(1, 7, "main", "abc123", "/org/ext-7")
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        assert!(nats.get_published().is_empty());
    }

    #[tokio::test]
    async fn skips_events_missing_external_repository_id() {
        let nats = Arc::new(MockNatsServices::new());
        let payload = external_events(vec![
            crate::modules::code::test_helpers::EventBuilder::new()
                .with_i64("id", 1)
                .with_string("ref", "refs/heads/main")
                .with_string("commit_sha", "abc123")
                .with_string("traversal_path", "/org/ext-7")
                .build(),
        ]);
        nats.add_pending_message(TestEnvelopeFactory::with_bytes(payload));

        create_siphon(Arc::clone(&nats)).drain_once().await.unwrap();

        assert!(nats.get_published().is_empty());
    }
}
