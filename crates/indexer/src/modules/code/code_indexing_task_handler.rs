use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use serde::{Deserialize, Serialize};

use super::checkpoint_store::CodeCheckpointStore;
use super::config::CODE_LOCK_TTL;
use super::indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
use super::locking::project_lock_key;
use super::metrics::CodeMetrics;
use super::repository::RepositoryService;
use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Event, Subscription};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CodeIndexingTaskHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,
}

pub struct CodeIndexingTaskHandler {
    pipeline: Arc<CodeIndexingPipeline>,
    repository_service: Arc<dyn RepositoryService>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    metrics: CodeMetrics,
    config: CodeIndexingTaskHandlerConfig,
}

impl CodeIndexingTaskHandler {
    pub fn new(
        pipeline: Arc<CodeIndexingPipeline>,
        repository_service: Arc<dyn RepositoryService>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        metrics: CodeMetrics,
        config: CodeIndexingTaskHandlerConfig,
    ) -> Self {
        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Handler for CodeIndexingTaskHandler {
    fn name(&self) -> &str {
        "code_indexing_task"
    }

    fn subscription(&self) -> Subscription {
        CodeIndexingTaskRequest::subscription().dead_letter_on_exhaustion(true)
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let request: CodeIndexingTaskRequest =
            serde_json::from_slice(&message.payload).map_err(|e| {
                HandlerError::Processing(format!(
                    "failed to deserialize code indexing task request: {e}"
                ))
            })?;

        debug!(
            task_id = request.task_id,
            project_id = request.project_id,
            branch = ?request.branch,
            "received code indexing task"
        );

        self.process_task(&context, &request).await
    }
}

impl CodeIndexingTaskHandler {
    async fn resolve_branch(
        &self,
        request: &CodeIndexingTaskRequest,
    ) -> Result<String, HandlerError> {
        match &request.branch {
            Some(branch) => Ok(branch.clone()),
            None => {
                let project_info = self
                    .repository_service
                    .project_info(request.project_id)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!("failed to fetch project info: {e}"))
                    })?;
                Ok(project_info.default_branch)
            }
        }
    }

    async fn process_task(
        &self,
        context: &HandlerContext,
        request: &CodeIndexingTaskRequest,
    ) -> Result<(), HandlerError> {
        let started_at = Instant::now();

        let branch = self.resolve_branch(request).await?;

        if self.is_already_indexed(request, &branch).await {
            self.metrics.record_outcome("skipped_checkpoint");
            return Ok(());
        }

        info!(
            task_id = request.task_id,
            project_id = request.project_id,
            branch = %branch,
            "starting code indexing"
        );

        let result = self.index_with_lock(context, request, &branch).await;

        let outcome = if result.is_ok() { "indexed" } else { "error" };
        self.metrics.record_outcome(outcome);
        self.metrics
            .handler_duration
            .record(started_at.elapsed().as_secs_f64(), &[]);

        result
    }

    async fn index_with_lock(
        &self,
        context: &HandlerContext,
        request: &CodeIndexingTaskRequest,
        branch: &str,
    ) -> Result<(), HandlerError> {
        let project_id = request.project_id;

        if !self.try_acquire_lock(context, project_id, branch).await? {
            debug!(
                task_id = request.task_id,
                project_id,
                branch = %branch,
                "lock held by another indexer, skipping"
            );
            self.metrics.record_outcome("skipped_lock");
            return Ok(());
        }

        let result = self
            .pipeline
            .index_project(
                context,
                &IndexingRequest {
                    project_id,
                    branch: branch.to_string(),
                    traversal_path: request.traversal_path.clone(),
                    task_id: request.task_id,
                    commit_sha: request.commit_sha.clone(),
                },
            )
            .await;

        if let Err(e) = self.release_lock(context, project_id, branch).await {
            warn!(project_id, branch = %branch, error = %e, "failed to release lock");
        }

        if let Err(e) = &result {
            warn!(project_id, branch = %branch, error = %e, "failed to index code");
        }

        result
    }
}

impl CodeIndexingTaskHandler {
    async fn is_already_indexed(&self, request: &CodeIndexingTaskRequest, branch: &str) -> bool {
        if let Ok(Some(checkpoint)) = self
            .checkpoint_store
            .get_checkpoint(&request.traversal_path, request.project_id, branch)
            .await
            && checkpoint.last_task_id >= request.task_id
        {
            debug!(task_id = request.task_id, "already indexed, skipping");
            return true;
        }
        false
    }
}

impl CodeIndexingTaskHandler {
    async fn try_acquire_lock(
        &self,
        ctx: &HandlerContext,
        project_id: i64,
        branch: &str,
    ) -> Result<bool, HandlerError> {
        let key = project_lock_key(project_id, branch);
        ctx.lock_service
            .try_acquire(&key, CODE_LOCK_TTL)
            .await
            .map_err(|e| HandlerError::Processing(format!("lock acquire failed: {e}")))
    }

    async fn release_lock(
        &self,
        ctx: &HandlerContext,
        project_id: i64,
        branch: &str,
    ) -> Result<(), HandlerError> {
        let key = project_lock_key(project_id, branch);
        ctx.lock_service
            .release(&key)
            .await
            .map_err(|e| HandlerError::Processing(format!("lock release failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Handler;
    use crate::modules::code::checkpoint_store::CodeCheckpointStore;
    use crate::modules::code::checkpoint_store::CodeIndexingCheckpoint;
    use crate::modules::code::checkpoint_store::test_utils::MockCodeCheckpointStore;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::repository::RepositoryResolver;
    use crate::modules::code::repository::cache::LocalRepositoryCache;
    use crate::modules::code::repository::service::test_utils::MockRepositoryService;
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices};
    use chrono::Utc;

    fn test_metrics() -> CodeMetrics {
        CodeMetrics::with_meter(&crate::testkit::test_meter())
    }

    struct TestContext {
        handler: CodeIndexingTaskHandler,
        mock_nats: Arc<MockNatsServices>,
        mock_locks: Arc<MockLockService>,
        mock_checkpoints: Arc<MockCodeCheckpointStore>,
        _cache_dir: tempfile::TempDir,
    }

    impl TestContext {
        fn new() -> Self {
            let mock_repo: Arc<dyn RepositoryService> =
                MockRepositoryService::with_default_branch(123, "main");
            let mock_nats = Arc::new(MockNatsServices::new());
            let mock_locks = Arc::new(MockLockService::new());
            let mock_checkpoints = Arc::new(MockCodeCheckpointStore::new());
            let stale_data_cleaner = Arc::new(MockStaleDataCleaner::default());
            let metrics = test_metrics();

            let checkpoint_store: Arc<dyn CodeCheckpointStore> = mock_checkpoints.clone();

            let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
            let table_names = Arc::new(
                crate::modules::code::config::CodeTableNames::from_ontology(&ontology)
                    .expect("code tables must resolve"),
            );

            let temp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
            let config = crate::configuration::RepositoryCacheConfiguration::default();
            let cache: Arc<dyn crate::modules::code::repository::RepositoryCache> =
                Arc::new(LocalRepositoryCache::new(
                    temp_dir.path().to_path_buf(),
                    &config,
                    4,
                    metrics.clone(),
                ));
            let resolver = RepositoryResolver::new(Arc::clone(&mock_repo), cache, metrics.clone());

            let pipeline = Arc::new(CodeIndexingPipeline::new(
                resolver,
                Arc::clone(&checkpoint_store),
                stale_data_cleaner,
                metrics.clone(),
                table_names,
            ));

            let handler = CodeIndexingTaskHandler::new(
                pipeline,
                mock_repo,
                Arc::clone(&checkpoint_store),
                metrics,
                CodeIndexingTaskHandlerConfig::default(),
            );

            Self {
                handler,
                mock_nats,
                mock_locks,
                mock_checkpoints,
                _cache_dir: temp_dir,
            }
        }

        fn handler_context(&self) -> HandlerContext {
            HandlerContext::new(
                Arc::new(MockDestination::new()),
                self.mock_nats.clone(),
                self.mock_locks.clone(),
                ProgressNotifier::noop(),
            )
        }

        fn make_request(task_id: i64, project_id: i64, branch: &str) -> Envelope {
            Envelope::new(&CodeIndexingTaskRequest {
                task_id,
                project_id,
                branch: Some(branch.to_string()),
                commit_sha: Some("abc123".to_string()),
                traversal_path: format!("/org/project-{}", project_id),
            })
            .unwrap()
        }

        async fn set_checkpoint(
            &self,
            project_id: i64,
            traversal_path: &str,
            branch: &str,
            last_task_id: i64,
        ) {
            self.mock_checkpoints
                .set_checkpoint(&CodeIndexingCheckpoint {
                    traversal_path: traversal_path.to_string(),
                    project_id,
                    branch: branch.to_string(),
                    last_task_id,
                    last_commit: Some("abc".to_string()),
                    indexed_at: Utc::now(),
                })
                .await
                .unwrap();
        }

        fn set_lock(&self, project_id: i64, branch: &str) {
            let key = project_lock_key(project_id, branch);
            self.mock_locks.set_lock(&key);
        }

        fn lock_exists(&self, project_id: i64, branch: &str) -> bool {
            let key = project_lock_key(project_id, branch);
            self.mock_locks.is_held(&key)
        }
    }

    #[tokio::test]
    async fn skips_already_indexed_tasks() {
        let ctx = TestContext::new();
        ctx.set_checkpoint(123, "/org/project-123", "main", 100)
            .await;

        let envelope = TestContext::make_request(50, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn skips_when_lock_already_held() {
        let ctx = TestContext::new();
        ctx.set_lock(123, "main");

        let envelope = TestContext::make_request(100, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn resolves_default_branch_when_branch_is_none() {
        let ctx = TestContext::new();
        ctx.set_checkpoint(123, "/org/project-123", "main", 100)
            .await;

        let envelope = Envelope::new(&CodeIndexingTaskRequest {
            task_id: 0,
            project_id: 123,
            branch: None,
            commit_sha: None,
            traversal_path: "/org/project-123".to_string(),
        })
        .unwrap();

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;
        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[test]
    fn handler_name() {
        let ctx = TestContext::new();
        assert_eq!(ctx.handler.name(), "code_indexing_task");
    }

    #[test]
    fn handler_subscription_matches_request_subscription() {
        let ctx = TestContext::new();
        let subscription = ctx.handler.subscription();
        let expected = CodeIndexingTaskRequest::subscription();
        assert_eq!(subscription.stream, expected.stream);
        assert_eq!(subscription.subject, expected.subject);
    }

    #[test]
    fn handler_subscription_has_dead_letter_on_exhaustion() {
        let ctx = TestContext::new();
        let subscription = ctx.handler.subscription();
        assert!(subscription.dead_letter_on_exhaustion);
    }
}
