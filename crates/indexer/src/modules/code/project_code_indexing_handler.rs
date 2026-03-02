use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use serde::Deserialize;

use super::config::LOCK_TTL;
use super::indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
use super::metrics::{CodeMetrics, RecordStageError};
use super::project_store::ProjectStore;
use super::push_event_store::PushEventStore;
use super::repository_service::RepositoryService;
use super::watermark_store::CodeWatermarkStore;
use crate::configuration::HandlerConfiguration;
use crate::module::{Handler, HandlerContext, HandlerError};
use crate::modules::sdlc::locking::project_lock_key;
use crate::topic::ProjectCodeIndexingRequest;
use crate::types::{Envelope, Event, Topic};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProjectCodeIndexingHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,
}

pub struct ProjectCodeIndexingHandler {
    pipeline: Arc<CodeIndexingPipeline>,
    repository_service: Arc<dyn RepositoryService>,
    watermark_store: Arc<dyn CodeWatermarkStore>,
    project_store: Arc<dyn ProjectStore>,
    push_event_store: Arc<dyn PushEventStore>,
    metrics: CodeMetrics,
    config: ProjectCodeIndexingHandlerConfig,
}

impl ProjectCodeIndexingHandler {
    pub fn new(
        pipeline: Arc<CodeIndexingPipeline>,
        repository_service: Arc<dyn RepositoryService>,
        watermark_store: Arc<dyn CodeWatermarkStore>,
        project_store: Arc<dyn ProjectStore>,
        push_event_store: Arc<dyn PushEventStore>,
        metrics: CodeMetrics,
        config: ProjectCodeIndexingHandlerConfig,
    ) -> Self {
        Self {
            pipeline,
            repository_service,
            watermark_store,
            project_store,
            push_event_store,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Handler for ProjectCodeIndexingHandler {
    fn name(&self) -> &str {
        "code-project-reconciliation"
    }

    fn topic(&self) -> Topic {
        ProjectCodeIndexingRequest::topic()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let request: ProjectCodeIndexingRequest = serde_json::from_slice(&message.payload)
            .map_err(|e| {
                HandlerError::Processing(format!("failed to deserialize indexing request: {e}"))
            })?;

        debug!(
            project_id = request.project_id,
            "received reconciliation request"
        );

        self.process_request(&context, request.project_id).await
    }
}

impl ProjectCodeIndexingHandler {
    async fn process_request(
        &self,
        context: &HandlerContext,
        project_id: i64,
    ) -> Result<(), HandlerError> {
        let started_at = Instant::now();
        let metrics = &self.metrics;

        let project = self
            .project_store
            .get_project(project_id)
            .await
            .map_err(|e| HandlerError::Processing(format!("project lookup failed: {e}")))?;

        let Some(project) = project else {
            debug!(project_id, "project not found in knowledge graph, skipping");
            metrics.record_outcome("skipped_project_not_found");
            return Ok(());
        };

        let repository = self
            .repository_service
            .repository_info(project_id)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to fetch repository info: {e}")))
            .record_error_stage(metrics, "repository_fetch")?;

        let default_branch = &repository.default_branch;

        let Some(push_event) = self
            .push_event_store
            .latest_push_on_branch(project_id, default_branch)
            .await
            .map_err(|e| HandlerError::Processing(format!("push event lookup failed: {e}")))?
        else {
            debug!(project_id, branch = %default_branch, "no push event found on default branch");
            metrics.record_outcome("skipped_no_push");
            return Ok(());
        };

        if let Ok(Some(watermark)) = self
            .watermark_store
            .get_watermark(project_id, default_branch)
            .await
            && watermark.last_event_id >= push_event.event_id
        {
            debug!(project_id, "already indexed, skipping reconciliation");
            metrics.record_outcome("skipped_watermark");
            return Ok(());
        }

        let lock_key = project_lock_key(project_id, default_branch);
        let acquired = context
            .lock_service
            .try_acquire(&lock_key, LOCK_TTL)
            .await
            .map_err(|e| HandlerError::Processing(format!("lock acquire failed: {e}")))?;

        if !acquired {
            debug!(project_id, "lock held by another indexer, skipping");
            metrics.record_outcome("skipped_lock");
            return Ok(());
        }

        info!(
            project_id,
            branch = %default_branch,
            commit_sha = %push_event.commit_sha,
            "starting reconciliation code indexing"
        );

        let result = self
            .pipeline
            .index_project(
                context,
                &IndexingRequest {
                    project_id,
                    branch: default_branch.to_string(),
                    traversal_path: project.traversal_path.clone(),
                    event_id: push_event.event_id,
                    commit_sha: push_event.commit_sha.clone(),
                    repository,
                },
            )
            .await;

        if let Err(e) = context.lock_service.release(&lock_key).await {
            warn!(project_id, error = %e, "failed to release lock");
        }

        if let Err(e) = &result {
            warn!(project_id, error = %e, "failed to index code during reconciliation");
            metrics.record_outcome("error");
            metrics
                .handler_duration
                .record(started_at.elapsed().as_secs_f64(), &[]);
            return result;
        }

        metrics.record_outcome("indexed");
        metrics
            .handler_duration
            .record(started_at.elapsed().as_secs_f64(), &[]);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::code::indexing_pipeline::CodeIndexingPipeline;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::project_store::ProjectInfo;
    use crate::modules::code::project_store::test_utils::MockProjectStore;
    use crate::modules::code::push_event_store::test_utils::MockPushEventStore;
    use crate::modules::code::repository_service::test_utils::MockRepositoryService;
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use crate::modules::code::watermark_store::CodeIndexingWatermark;
    use crate::modules::code::watermark_store::CodeWatermarkStore;
    use crate::modules::code::watermark_store::test_utils::MockCodeWatermarkStore;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices};
    use chrono::Utc;

    fn test_metrics() -> CodeMetrics {
        let provider = opentelemetry::global::meter_provider();
        let meter = provider.meter("test");
        CodeMetrics::with_meter(&meter)
    }

    struct TestContext {
        handler: ProjectCodeIndexingHandler,
        mock_nats: Arc<MockNatsServices>,
        mock_locks: Arc<MockLockService>,
        mock_watermarks: Arc<MockCodeWatermarkStore>,
        project_store: Arc<MockProjectStore>,
        push_event_store: Arc<MockPushEventStore>,
    }

    impl TestContext {
        fn new() -> Self {
            let mock_repo = MockRepositoryService::with_default_branch(123, "main");
            Self::with_repository_service(mock_repo)
        }

        fn with_repository_service(
            repository_service: Arc<dyn super::super::repository_service::RepositoryService>,
        ) -> Self {
            let mock_nats = Arc::new(MockNatsServices::new());
            let mock_locks = Arc::new(MockLockService::new());
            let mock_watermarks = Arc::new(MockCodeWatermarkStore::new());
            let project_store = Arc::new(MockProjectStore::new());
            let push_event_store = Arc::new(MockPushEventStore::new());
            let stale_data_cleaner = Arc::new(MockStaleDataCleaner::default());
            let metrics = test_metrics();

            let watermark_store: Arc<dyn CodeWatermarkStore> = mock_watermarks.clone();

            let pipeline = Arc::new(CodeIndexingPipeline::new(
                Arc::clone(&repository_service),
                Arc::clone(&watermark_store),
                stale_data_cleaner,
                metrics.clone(),
            ));

            let handler = ProjectCodeIndexingHandler::new(
                pipeline,
                repository_service,
                Arc::clone(&watermark_store),
                project_store.clone(),
                push_event_store.clone(),
                metrics,
                ProjectCodeIndexingHandlerConfig::default(),
            );

            Self {
                handler,
                mock_nats,
                mock_locks,
                mock_watermarks,
                project_store,
                push_event_store,
            }
        }

        fn handler_context(&self) -> HandlerContext {
            HandlerContext::new(
                Arc::new(MockDestination::new()),
                self.mock_nats.clone(),
                self.mock_locks.clone(),
            )
        }

        fn add_project(&self, project_id: i64) {
            self.project_store.projects.lock().insert(
                project_id,
                ProjectInfo {
                    project_id,
                    traversal_path: format!("/org/project-{}", project_id),
                    full_path: format!("org/project-{}", project_id),
                },
            );
        }

        fn make_request(project_id: i64) -> Envelope {
            Envelope::new(&ProjectCodeIndexingRequest { project_id }).unwrap()
        }
    }

    #[tokio::test]
    async fn skips_when_project_not_found() {
        let ctx = TestContext::new();

        let envelope = TestContext::make_request(999);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn skips_when_no_push_event_on_default_branch() {
        let ctx = TestContext::new();
        ctx.add_project(123);

        let envelope = TestContext::make_request(123);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn skips_when_already_indexed() {
        let ctx = TestContext::new();
        ctx.add_project(123);
        ctx.push_event_store
            .add_push_event(123, "main", 50, "abc123");

        ctx.mock_watermarks
            .set_watermark(&CodeIndexingWatermark {
                project_id: 123,
                branch: "main".to_string(),
                last_event_id: 100,
                last_commit: "abc".to_string(),
                indexed_at: Utc::now(),
            })
            .await
            .unwrap();

        let envelope = TestContext::make_request(123);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn skips_when_lock_held() {
        let ctx = TestContext::new();
        ctx.add_project(123);
        ctx.push_event_store
            .add_push_event(123, "main", 100, "abc123");
        let lock_key = project_lock_key(123, "main");
        ctx.mock_locks.set_lock(&lock_key);

        let envelope = TestContext::make_request(123);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[test]
    fn handler_name() {
        let ctx = TestContext::new();
        assert_eq!(ctx.handler.name(), "code-project-reconciliation");
    }

    #[test]
    fn handler_topic_matches_request_topic() {
        let ctx = TestContext::new();
        let topic = ctx.handler.topic();
        let expected = ProjectCodeIndexingRequest::topic();
        assert_eq!(topic.stream, expected.stream);
        assert_eq!(topic.subject, expected.subject);
    }
}
