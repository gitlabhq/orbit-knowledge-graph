use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gitlab_client::GitlabClientError;
use tracing::{debug, info, warn};

use super::checkpoint_store::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::indexing_pipeline::{CodeIndexingPipeline, IndexOutcome, IndexingRequest};
use super::locking::project_lock_key;
use super::metrics::CodeMetrics;
use super::repository::{EmptyRepositoryReason, RepositoryService, RepositoryServiceError};
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Event, Subscription};
use gkg_server_config::{CodeIndexingTaskHandlerConfig, HandlerConfiguration};

/// Sentinel branch value written to the checkpoint when the project is
/// resolved as deleted from Rails (404) and we cannot determine its default
/// branch. The dispatcher's `fetch_checkpointed_project_ids` filter keys on
/// `(traversal_path, project_id)` and ignores branch, so any non-empty value
/// satisfies the schema and dedupes future dispatch cycles.
const DELETED_PROJECT_BRANCH_SENTINEL: &str = "HEAD";

pub struct CodeIndexingTaskHandler {
    pipeline: Arc<CodeIndexingPipeline>,
    repository_service: Arc<dyn RepositoryService>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    metrics: CodeMetrics,
    config: CodeIndexingTaskHandlerConfig,
    lock_ttl: Duration,
}

impl CodeIndexingTaskHandler {
    pub fn new(
        pipeline: Arc<CodeIndexingPipeline>,
        repository_service: Arc<dyn RepositoryService>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        metrics: CodeMetrics,
        config: CodeIndexingTaskHandlerConfig,
        lock_ttl: Duration,
    ) -> Self {
        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            metrics,
            config,
            lock_ttl,
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
    /// Returns `Ok(Some(branch))` when the branch is known, `Ok(None)` when
    /// the project is gone from Rails (terminal: the dispatcher has a stale
    /// view; acking avoids DLQ churn), and `Err` for transient failures.
    async fn resolve_branch(
        &self,
        request: &CodeIndexingTaskRequest,
    ) -> Result<Option<String>, HandlerError> {
        match &request.branch {
            Some(branch) => Ok(Some(branch.clone())),
            None => match self
                .repository_service
                .project_info(request.project_id)
                .await
            {
                Ok(project_info) => Ok(Some(project_info.default_branch)),
                Err(RepositoryServiceError::GitlabApi(GitlabClientError::NotFound(_))) => Ok(None),
                Err(e) => Err(HandlerError::Processing(format!(
                    "failed to fetch project info: {e}"
                ))),
            },
        }
    }

    async fn process_task(
        &self,
        context: &HandlerContext,
        request: &CodeIndexingTaskRequest,
    ) -> Result<(), HandlerError> {
        let started_at = Utc::now();

        let Some(branch) = self.resolve_branch(request).await? else {
            warn!(
                project_id = request.project_id,
                task_id = request.task_id,
                "project not found resolving default branch; acknowledging as deleted"
            );
            // Mirror the empty-repository path: write a checkpoint so the
            // dispatcher's `fetch_checkpointed_project_ids` filter excludes
            // this project on subsequent backfill cycles instead of
            // republishing the same task forever.
            let sentinel_branch = request
                .branch
                .as_deref()
                .unwrap_or(DELETED_PROJECT_BRANCH_SENTINEL);
            let checkpoint = CodeIndexingCheckpoint {
                traversal_path: request.traversal_path.clone(),
                project_id: request.project_id,
                branch: sentinel_branch.to_string(),
                last_task_id: request.task_id,
                last_commit: None,
                indexed_at: Utc::now(),
            };
            if let Err(e) = self.checkpoint_store.set_checkpoint(&checkpoint).await {
                warn!(
                    project_id = request.project_id,
                    task_id = request.task_id,
                    error = %e,
                    "failed to write deleted-project checkpoint; dispatcher may republish"
                );
            }
            self.metrics
                .record_empty_repository(EmptyRepositoryReason::NotFound.as_metric_label());
            self.metrics.record_outcome("empty_repository");
            self.metrics.record_handler_duration(started_at);
            return Ok(());
        };

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

        let result = self
            .index_with_lock(context, request, &branch, started_at)
            .await;

        let outcome = match &result {
            Ok(Some(IndexOutcome::Indexed)) => "indexed",
            Ok(Some(IndexOutcome::EmptyRepository)) => "empty_repository",
            Ok(None) => "skipped_lock",
            Err(_) => "error",
        };
        self.metrics.record_outcome(outcome);
        if matches!(
            &result,
            Ok(Some(IndexOutcome::Indexed | IndexOutcome::EmptyRepository))
        ) {
            self.metrics.record_repository_indexed(outcome);
        }
        self.metrics.record_handler_duration(started_at);

        result.map(|_| ())
    }

    async fn index_with_lock(
        &self,
        context: &HandlerContext,
        request: &CodeIndexingTaskRequest,
        branch: &str,
        started_at: DateTime<Utc>,
    ) -> Result<Option<IndexOutcome>, HandlerError> {
        let project_id = request.project_id;

        if !self.try_acquire_lock(context, project_id, branch).await? {
            debug!(
                task_id = request.task_id,
                project_id,
                branch = %branch,
                "lock held by another indexer, skipping"
            );
            return Ok(None);
        }

        context
            .indexing_status
            .record_start(&request.traversal_path, started_at)
            .await;

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

        context
            .indexing_status
            .record_completion(
                &request.traversal_path,
                started_at,
                Utc::now(),
                result.as_ref().err().map(ToString::to_string),
            )
            .await;

        if let Err(e) = self.release_lock(context, project_id, branch).await {
            warn!(project_id, branch = %branch, error = %e, "failed to release lock");
        }

        if let Err(e) = &result {
            warn!(project_id, branch = %branch, error = %e, "failed to index code");
        }

        result.map(Some)
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
            .try_acquire(&key, self.lock_ttl)
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
        mock_repo: Arc<MockRepositoryService>,
        _cache_dir: tempfile::TempDir,
    }

    impl TestContext {
        fn new() -> Self {
            let mock_repo = MockRepositoryService::with_default_branch(123, "main");
            let mock_nats = Arc::new(MockNatsServices::new());
            let mock_locks = Arc::new(MockLockService::new());
            let mock_checkpoints = Arc::new(MockCodeCheckpointStore::new());
            let stale_data_cleaner = Arc::new(MockStaleDataCleaner::default());
            let metrics = test_metrics();

            let checkpoint_store: Arc<dyn CodeCheckpointStore> = mock_checkpoints.clone();
            let repo_service: Arc<dyn RepositoryService> = mock_repo.clone();

            let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
            let table_names = Arc::new(
                crate::modules::code::config::CodeTableNames::from_ontology(&ontology)
                    .expect("code tables must resolve"),
            );

            let temp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
            let cache: Arc<dyn crate::modules::code::repository::RepositoryCache> = Arc::new(
                LocalRepositoryCache::new(temp_dir.path().to_path_buf(), u64::MAX),
            );
            let resolver =
                RepositoryResolver::new(Arc::clone(&repo_service), cache, metrics.clone());

            let pipeline = Arc::new(CodeIndexingPipeline::new(
                resolver,
                Arc::clone(&checkpoint_store),
                stale_data_cleaner,
                metrics.clone(),
                table_names,
                Arc::new(ontology),
                CodeIndexingTaskHandlerConfig::default().pipeline,
            ));

            let handler = CodeIndexingTaskHandler::new(
                pipeline,
                repo_service,
                Arc::clone(&checkpoint_store),
                metrics,
                CodeIndexingTaskHandlerConfig::default(),
                Duration::from_secs(60),
            );

            Self {
                handler,
                mock_nats,
                mock_locks,
                mock_checkpoints,
                mock_repo,
                _cache_dir: temp_dir,
            }
        }

        fn handler_context(&self) -> HandlerContext {
            HandlerContext::new(
                Arc::new(MockDestination::new()),
                self.mock_nats.clone(),
                self.mock_locks.clone(),
                ProgressNotifier::noop(),
                Arc::new(crate::indexing_status::IndexingStatusStore::new(
                    self.mock_nats.clone(),
                )),
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

    #[tokio::test]
    async fn project_info_404_acks_and_writes_checkpoint() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_project_info_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::NotFound(123)),
        );

        let envelope = Envelope::new(&CodeIndexingTaskRequest {
            task_id: 99,
            project_id: 123,
            branch: None,
            commit_sha: None,
            traversal_path: "/org/project-123".to_string(),
        })
        .unwrap();

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            result.is_ok(),
            "project_info 404 should ack (deleted project), got {result:?}"
        );
        assert!(
            !ctx.lock_exists(123, "main"),
            "no lock should be acquired when branch cannot be resolved"
        );
        let checkpoint = ctx
            .mock_checkpoints
            .get_checkpoint("/org/project-123", 123, "HEAD")
            .await
            .unwrap()
            .expect("checkpoint should be written for deleted project so the dispatcher dedupes");
        assert_eq!(checkpoint.last_task_id, 99);
        assert!(checkpoint.last_commit.is_none());
    }

    #[tokio::test]
    async fn project_info_non_404_error_is_retried() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_project_info_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::ServerError {
                project_id: 123,
                status: 500,
            }),
        );

        let envelope = Envelope::new(&CodeIndexingTaskRequest {
            task_id: 99,
            project_id: 123,
            branch: None,
            commit_sha: None,
            traversal_path: "/org/project-123".to_string(),
        })
        .unwrap();

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            result.is_err(),
            "project_info 500 should nack (transient), got {result:?}"
        );
    }

    #[tokio::test]
    async fn empty_repository_sets_checkpoint_and_acks() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_download_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::NotFound(123)),
        );

        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok(), "empty repo should ack, got {result:?}");
        let checkpoint = ctx
            .mock_checkpoints
            .get_checkpoint("/org/project-123", 123, "main")
            .await
            .unwrap()
            .expect("checkpoint should be set for empty repo");
        assert_eq!(checkpoint.last_task_id, 42);
        assert!(checkpoint.last_commit.is_none());
    }

    #[tokio::test]
    async fn server_error_sets_checkpoint_and_acks() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_download_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::ServerError {
                project_id: 123,
                status: 500,
            }),
        );

        let envelope = TestContext::make_request(7, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        let checkpoint = ctx
            .mock_checkpoints
            .get_checkpoint("/org/project-123", 123, "main")
            .await
            .unwrap()
            .expect("checkpoint should be set for missing repository");
        assert_eq!(checkpoint.last_task_id, 7);
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
