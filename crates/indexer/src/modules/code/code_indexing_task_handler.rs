use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, info, warn};

use super::checkpoint_store::CodeCheckpointStore;
use super::config::CODE_LOCK_TTL;
use super::indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
use super::locking::project_lock_key;
use super::metrics::CodeMetrics;
use super::repository::RepositoryService;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Event, Subscription};
use gkg_server_config::{CodeIndexingTaskHandlerConfig, HandlerConfiguration};

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

        match self.skip_reason(request, &branch).await {
            Some("skipped_checkpoint") => {
                self.metrics.record_outcome("skipped_checkpoint");
                return Ok(());
            }
            Some(reason) => {
                self.metrics.record_outcome(reason);
                self.republish(context, request).await;
                return Ok(());
            }
            None => {}
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
                "lock held by another indexer, re-publishing"
            );
            self.metrics.record_outcome("skipped_lock");
            self.republish(context, request).await;
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
    async fn skip_reason(
        &self,
        request: &CodeIndexingTaskRequest,
        branch: &str,
    ) -> Option<&'static str> {
        let checkpoint = match self
            .checkpoint_store
            .get_checkpoint(&request.traversal_path, request.project_id, branch)
            .await
        {
            Ok(Some(cp)) => cp,
            _ => return None,
        };

        if checkpoint.last_task_id >= request.task_id {
            debug!(task_id = request.task_id, "already indexed, skipping");
            return Some("skipped_checkpoint");
        }

        let debounce = Duration::from_secs(self.config.debounce_secs);
        if debounce > Duration::ZERO {
            let elapsed = Utc::now().signed_duration_since(checkpoint.indexed_at);
            if elapsed
                .to_std()
                .is_ok_and(|elapsed_std| elapsed_std < debounce)
            {
                debug!(
                    task_id = request.task_id,
                    project_id = request.project_id,
                    branch,
                    debounce_secs = self.config.debounce_secs,
                    "within debounce window, skipping"
                );
                return Some("skipped_debounce");
            }
        }

        None
    }
}

impl CodeIndexingTaskHandler {
    async fn republish(&self, ctx: &HandlerContext, request: &CodeIndexingTaskRequest) {
        let envelope = match crate::types::Envelope::new(request) {
            Ok(e) => e,
            Err(e) => {
                warn!(
                    task_id = request.task_id,
                    project_id = request.project_id,
                    error = %e,
                    "failed to serialize debounced task for re-publish"
                );
                return;
            }
        };

        match ctx
            .nats
            .publish(&request.publish_subscription(), &envelope)
            .await
        {
            Ok(()) => {
                debug!(
                    task_id = request.task_id,
                    project_id = request.project_id,
                    "re-published debounced task"
                );
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                debug!(
                    task_id = request.task_id,
                    project_id = request.project_id,
                    "debounced task already in-flight, skipping re-publish"
                );
            }
            Err(e) => {
                warn!(
                    task_id = request.task_id,
                    project_id = request.project_id,
                    error = %e,
                    "failed to re-publish debounced task"
                );
            }
        }
    }

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
            let cache: Arc<dyn crate::modules::code::repository::RepositoryCache> =
                Arc::new(LocalRepositoryCache::new(temp_dir.path().to_path_buf()));
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
            self.set_checkpoint_at(project_id, traversal_path, branch, last_task_id, Utc::now())
                .await;
        }

        async fn set_checkpoint_at(
            &self,
            project_id: i64,
            traversal_path: &str,
            branch: &str,
            last_task_id: i64,
            indexed_at: chrono::DateTime<Utc>,
        ) {
            self.mock_checkpoints
                .set_checkpoint(&CodeIndexingCheckpoint {
                    traversal_path: traversal_path.to_string(),
                    project_id,
                    branch: branch.to_string(),
                    last_task_id,
                    last_commit: Some("abc".to_string()),
                    indexed_at,
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
    async fn lock_contention_re_publishes_task() {
        let ctx = TestContext::new();
        ctx.set_lock(123, "main");

        let envelope = TestContext::make_request(100, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());

        let published = ctx.mock_nats.get_published();
        assert_eq!(
            published.len(),
            1,
            "lock-contended task should be re-published"
        );
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

    #[tokio::test]
    async fn debounce_re_publishes_task_to_nats() {
        let ctx = TestContext::new();
        // Checkpoint was set just now (within the default 30s debounce window)
        // with a lower task_id so checkpoint skip does not trigger
        ctx.set_checkpoint(123, "/org/project-123", "main", 5).await;

        let envelope = TestContext::make_request(10, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));

        let published = ctx.mock_nats.get_published();
        assert_eq!(published.len(), 1, "debounced task should be re-published");
        assert!(
            published[0].0.subject.contains("123"),
            "re-published subject should contain the project_id"
        );
    }

    #[tokio::test]
    async fn processes_task_outside_debounce_window() {
        let ctx = TestContext::new();
        // Checkpoint was set 60 seconds ago -- outside the 30s debounce window
        let old_time = Utc::now() - chrono::Duration::seconds(60);
        ctx.set_checkpoint_at(123, "/org/project-123", "main", 5, old_time)
            .await;

        let envelope = TestContext::make_request(10, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        // The MockRepositoryService returns an empty stream, so archive
        // extraction fails. An error proves the handler attempted the work
        // rather than skipping via debounce (which returns Ok(())).
        assert!(
            result.is_err(),
            "expected error from empty mock archive, got Ok -- task may have been incorrectly skipped by debounce"
        );
    }
}
