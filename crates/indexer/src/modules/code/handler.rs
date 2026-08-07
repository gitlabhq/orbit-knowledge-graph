use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use code_graph::v2::CancellationToken;
use gitlab_client::GitlabClientError;
use tracing::{debug, info, warn};

use super::checkpoint::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::metrics::CodeMetrics;
use super::observer::CodeOtelObserver;
use super::pipeline::{CodeIndexer, Fetched, IndexOutcome, IndexingRequest};
use super::repository::cache::CachedRepository;
use super::repository::{EmptyRepositoryReason, RepositoryService, RepositoryServiceError};
use crate::analytics::IndexingAnalytics;

use crate::engine::retry::{Backoff, RetryMode, RetryPolicy};
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::locking::LockGuard;
use crate::observer::{self, IndexingMode, IndexingObserver, PipelineType};
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Subscription};

/// Sentinel branch value written to the checkpoint when the project is
/// resolved as deleted from Rails (404) and we cannot determine its default
/// branch. The dispatcher's `fetch_checkpointed_project_ids` filter keys on
/// `(traversal_path, project_id)` and ignores branch, so any non-empty value
/// satisfies the schema and dedupes future dispatch cycles.
const DELETED_PROJECT_BRANCH_SENTINEL: &str = "HEAD";

/// A timed-out job is likely transiently slow: retry once, then dead-letter (engine reads this policy).
const JOB_TIMEOUT_RETRY: RetryPolicy = RetryPolicy {
    mode: RetryMode::Global,
    backoff: Backoff::Fixed(&[]),
    max_attempts: 2,
    dead_letter: true,
};

/// One budget shared by every timed phase; queueing for a lane deliberately runs outside it.
struct WorkClock {
    total: Option<Duration>,
    remaining: Duration,
}

impl WorkClock {
    fn new(budget: Option<Duration>) -> Self {
        Self {
            total: budget,
            remaining: budget.unwrap_or(Duration::ZERO),
        }
    }

    async fn run<T>(
        &mut self,
        phase: impl Future<Output = Result<T, HandlerError>>,
        cancel: &CancellationToken,
        attempt: u32,
        project_id: i64,
        branch: &str,
    ) -> Result<T, HandlerError> {
        let Some(total) = self.total else {
            return phase.await;
        };
        let started = tokio::time::Instant::now();
        match tokio::time::timeout(self.remaining, phase).await {
            Ok(result) => {
                self.remaining = self.remaining.saturating_sub(started.elapsed());
                result
            }
            Err(_) => {
                cancel.cancel();
                warn!(
                    project_id,
                    branch = %branch,
                    budget_secs = total.as_secs(),
                    "code indexing job exceeded its work budget"
                );
                Err(JOB_TIMEOUT_RETRY.global_failure(
                    attempt,
                    format!(
                        "code indexing job exceeded the {}s work budget",
                        total.as_secs()
                    ),
                ))
            }
        }
    }
}

fn project_lock_key(project_id: i64, branch: &str) -> String {
    use base64::Engine;
    let encoded_branch = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(branch);
    format!("project.{project_id}.{encoded_branch}")
}

pub struct CodeIndexingTaskHandler {
    pipeline: Arc<CodeIndexer>,
    repository_service: Arc<dyn RepositoryService>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    metrics: CodeMetrics,
    lock_ttl: Duration,
    subscription: Subscription,
    analytics: IndexingAnalytics,
}

impl CodeIndexingTaskHandler {
    /// Flush buffered writes and wait until durable. For tests and shutdown.
    pub async fn flush(&self) -> Result<(), HandlerError> {
        self.pipeline.flush().await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "handler constructor wires all collaborators explicitly; grouping into a struct would just move the arity"
    )]
    pub fn new(
        pipeline: Arc<CodeIndexer>,
        repository_service: Arc<dyn RepositoryService>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        metrics: CodeMetrics,
        lock_ttl: Duration,
        subscription: Subscription,
        analytics: IndexingAnalytics,
    ) -> Self {
        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            metrics,
            lock_ttl,
            subscription,
            analytics,
        }
    }
}

#[async_trait]
impl Handler for CodeIndexingTaskHandler {
    fn name(&self) -> &str {
        "code_indexing_task"
    }

    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }

    fn requires_worker_pool(&self) -> bool {
        false
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
            dispatch_id = %request.dispatch_id,
            campaign_id = request.campaign_id.as_deref().unwrap_or("none"),
            "received code indexing task"
        );

        self.process_task(&context, &request, message.attempt).await
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
        attempt: u32,
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

        let existing_checkpoint = self.load_checkpoint(request, &branch).await;
        if existing_checkpoint
            .as_ref()
            .is_some_and(|cp| cp.last_task_id >= request.task_id)
        {
            debug!(task_id = request.task_id, "already indexed, skipping");
            self.metrics.record_outcome("skipped_checkpoint");
            return Ok(());
        }
        let had_prior_checkpoint = existing_checkpoint.is_some();

        info!(
            task_id = request.task_id,
            project_id = request.project_id,
            branch = %branch,
            had_prior_checkpoint,
            dispatch_id = %request.dispatch_id,
            campaign_id = request.campaign_id.as_deref().unwrap_or("none"),
            "starting code indexing"
        );

        let mut observers: Vec<Box<dyn IndexingObserver>> =
            vec![Box::new(CodeOtelObserver::new(self.metrics.clone()))];
        observers.extend(self.analytics.observer());
        let mut observer: observer::MultiObserver = observer::MultiObserver::new(observers);
        observer.set_dispatch_id(request.dispatch_id);
        observer.set_campaign_id(request.campaign_id.clone());
        observer.set_pipeline_type(PipelineType::Code);
        observer.set_project(request.project_id, &branch);
        observer.set_commit_sha(request.commit_sha.clone());
        observer.set_traversal_path(Some(&request.traversal_path));
        observer.set_indexing_mode(if had_prior_checkpoint {
            IndexingMode::Incremental
        } else {
            IndexingMode::Full
        });

        let result = self
            .index_with_lock(
                context,
                request,
                &branch,
                had_prior_checkpoint,
                started_at,
                attempt,
                &mut observer,
            )
            .await;

        let outcome = match &result {
            Ok(Some(label)) => label,
            Ok(None) => "skipped_lock",
            Err(_) => "error",
        };
        self.metrics.record_outcome(outcome);
        if matches!(&result, Ok(Some(_))) {
            self.metrics.record_repository_indexed(outcome);
        }
        self.metrics.record_handler_duration(started_at);

        match &result {
            Ok(_) => observer.finish(),
            Err(e) => {
                observer.record_error(&e.to_string());
                observer.finish();
            }
        }

        result.map(|_| ())
    }

    /// The project lock is taken once and never renewed, so work plus queueing must stay
    /// inside its TTL; a pod still full past that slack is backpressure, not a job failure.
    async fn acquire_lane_within_lock_slack(
        &self,
        repository: &CachedRepository,
    ) -> Result<Option<OwnedSemaphorePermit>, HandlerError> {
        let Some(budget) = self.pipeline.job_timeout() else {
            return self.pipeline.acquire_indexing_lane(repository).await;
        };
        let cap = self.lock_ttl.saturating_sub(budget);
        match tokio::time::timeout(cap, self.pipeline.acquire_indexing_lane(repository)).await {
            Ok(permit) => permit,
            Err(_) => Err(HandlerError::Backpressure(format!(
                "no indexing lane within {}s",
                cap.as_secs()
            ))),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "indexing stage threads its collaborators and per-delivery state explicitly; a params struct would just move the arity"
    )]
    #[tracing::instrument(
        name = "code_indexing_project",
        skip_all,
        fields(
            project_id = request.project_id,
            namespace_id,
            traversal_path = %request.traversal_path,
            branch = %branch,
        )
    )]
    async fn index_with_lock(
        &self,
        context: &HandlerContext,
        request: &CodeIndexingTaskRequest,
        branch: &str,
        had_prior_checkpoint: bool,
        started_at: DateTime<Utc>,
        attempt: u32,
        observer: &mut dyn IndexingObserver,
    ) -> Result<Option<&'static str>, HandlerError> {
        let Some(namespace_id) =
            gkg_utils::traversal_path::top_level_namespace_id(&request.traversal_path)
        else {
            return Err(HandlerError::Processing(format!(
                "traversal_path {:?} has no namespace_id",
                request.traversal_path
            )));
        };
        tracing::Span::current().record("namespace_id", namespace_id);

        let project_id = request.project_id;
        let key = project_lock_key(project_id, branch);

        let _guard = match LockGuard::acquire(context.lock_service.clone(), &key, self.lock_ttl)
            .await
            .map_err(|e| HandlerError::Processing(format!("lock acquire failed: {e}")))?
        {
            Some(guard) => guard,
            None => {
                warn!(
                    task_id = request.task_id,
                    project_id,
                    branch = %branch,
                    lock_key = %key,
                    "code indexing skipped: lock held by another indexer"
                );
                return Ok(None);
            }
        };

        context
            .indexing_status
            .record_start(&request.traversal_path, started_at)
            .await;

        let indexing_request = IndexingRequest {
            project_id,
            branch: branch.to_string(),
            traversal_path: request.traversal_path.clone(),
            task_id: request.task_id,
            commit_sha: request.commit_sha.clone(),
            had_prior_checkpoint,
        };
        // On timeout: cancel so the detached parse bails, and drop the future before its flush so nothing commits.
        let cancel = CancellationToken::new();
        let mut clock = WorkClock::new(self.pipeline.job_timeout());

        let fetched = clock
            .run(
                self.pipeline.fetch_repository(&indexing_request),
                &cancel,
                attempt,
                project_id,
                branch,
            )
            .await;

        let result = match fetched {
            Ok(Fetched::EmptyRepository) => Ok(IndexOutcome::EmptyRepository),
            Ok(Fetched::Repository(repository)) => {
                // Fetch plus the full lane slack can reach ack_wait; refresh the delivery first.
                context.progress.notify_in_progress().await;
                let _lane = self.acquire_lane_within_lock_slack(&repository).await?;
                clock
                    .run(
                        self.pipeline.index_fetched(
                            context,
                            &indexing_request,
                            &repository,
                            observer,
                            cancel.clone(),
                        ),
                        &cancel,
                        attempt,
                        project_id,
                        branch,
                    )
                    .await
            }
            Err(e) => Err(e),
        };

        let result = result.map(|outcome| outcome.metric_label());

        context
            .indexing_status
            .record_completion(
                &request.traversal_path,
                started_at,
                Utc::now(),
                result.as_ref().err().map(ToString::to_string),
            )
            .await;

        if let Err(e) = &result {
            warn!(project_id, branch = %branch, error = %e, "failed to index code");
        }

        result.map(Some)
    }
}

impl CodeIndexingTaskHandler {
    async fn load_checkpoint(
        &self,
        request: &CodeIndexingTaskRequest,
        branch: &str,
    ) -> Option<CodeIndexingCheckpoint> {
        self.checkpoint_store
            .get_checkpoint(&request.traversal_path, request.project_id, branch)
            .await
            .ok()
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Handler;
    use crate::modules::code::checkpoint::CodeCheckpointStore;
    use crate::modules::code::checkpoint::CodeIndexingCheckpoint;
    use crate::modules::code::checkpoint::test_utils::MockCodeCheckpointStore;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::repository::RepositoryResolver;
    use crate::modules::code::repository::cache::LocalRepositoryCache;
    use crate::modules::code::repository::service::test_utils::MockRepositoryService;
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockLockService, MockNatsServices};
    use crate::types::Event;
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
            let cache: Arc<dyn crate::modules::code::repository::RepositoryCache> =
                Arc::new(LocalRepositoryCache::new(
                    temp_dir.path().to_path_buf(),
                    u64::MAX,
                    0,
                    metrics.clone(),
                ));
            let resolver = RepositoryResolver::new(Arc::clone(&repo_service), cache);

            let pipeline = Arc::new(CodeIndexer::new(
                resolver,
                crate::testkit::test_writer(),
                Arc::clone(&checkpoint_store),
                stale_data_cleaner,
                metrics.clone(),
                table_names,
                Arc::new(ontology),
                gkg_server_config::CodeIndexingPipelineConfig::default(),
            ));

            let handler = CodeIndexingTaskHandler::new(
                pipeline,
                repo_service,
                Arc::clone(&checkpoint_store),
                metrics,
                Duration::from_secs(60),
                CodeIndexingTaskRequest::subscription(),
                IndexingAnalytics::disabled(),
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
                self.mock_nats.clone(),
                self.mock_locks.clone(),
                ProgressNotifier::noop(),
                Arc::new(crate::indexing_status::IndexingStatusStore::new(
                    self.mock_nats.clone(),
                )),
            )
        }

        fn make_request(task_id: i64, project_id: i64, branch: &str) -> Envelope {
            Self::make_request_with_sha(task_id, project_id, branch, Some("abc123"))
        }

        fn make_request_with_sha(
            task_id: i64,
            project_id: i64,
            branch: &str,
            commit_sha: Option<&str>,
        ) -> Envelope {
            Envelope::new(&CodeIndexingTaskRequest {
                task_id,
                project_id,
                branch: Some(branch.to_string()),
                commit_sha: commit_sha.map(str::to_string),
                traversal_path: format!("1/{project_id}/"),
                dispatch_id: uuid::Uuid::new_v4(),
                campaign_id: None,
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
        ctx.set_checkpoint(123, "1/123/", "main", 100).await;

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
        ctx.set_checkpoint(123, "1/123/", "main", 100).await;

        let envelope = Envelope::new(&CodeIndexingTaskRequest {
            task_id: 0,
            project_id: 123,
            branch: None,
            commit_sha: None,
            traversal_path: "1/123/".to_string(),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
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
            traversal_path: "1/123/".to_string(),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
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
            .get_checkpoint("1/123/", 123, "HEAD")
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
            traversal_path: "1/123/".to_string(),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
        })
        .unwrap();

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            result.is_err(),
            "project_info 500 should nack (transient), got {result:?}"
        );
    }

    #[tokio::test]
    async fn empty_repository_without_commit_sha_sets_checkpoint_and_acks() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_download_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::NotFound(123)),
        );

        let envelope = TestContext::make_request_with_sha(42, 123, "main", None);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok(), "empty repo should ack, got {result:?}");
        let checkpoint = ctx
            .mock_checkpoints
            .get_checkpoint("1/123/", 123, "main")
            .await
            .unwrap()
            .expect("checkpoint should be set for empty repo");
        assert_eq!(checkpoint.last_task_id, 42);
        assert!(checkpoint.last_commit.is_none());
    }

    #[tokio::test]
    async fn empty_repository_with_commit_sha_nacks_without_checkpoint() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_download_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::NotFound(123)),
        );

        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            result.is_err(),
            "push-dispatched task must retry on empty archive, got {result:?}"
        );
        let checkpoint = ctx
            .mock_checkpoints
            .get_checkpoint("1/123/", 123, "main")
            .await
            .unwrap();
        assert!(checkpoint.is_none(), "no checkpoint on the raced attempt");
    }

    #[tokio::test]
    async fn server_error_without_commit_sha_sets_checkpoint_and_acks() {
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

        let envelope = TestContext::make_request_with_sha(7, 123, "main", None);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        let checkpoint = ctx
            .mock_checkpoints
            .get_checkpoint("1/123/", 123, "main")
            .await
            .unwrap()
            .expect("checkpoint should be set for missing repository");
        assert_eq!(checkpoint.last_task_id, 7);
    }

    #[tokio::test]
    async fn handler_name() {
        let ctx = TestContext::new();
        assert_eq!(ctx.handler.name(), "code_indexing_task");
    }

    #[tokio::test]
    async fn handler_subscription_matches_request_subscription() {
        let ctx = TestContext::new();
        let subscription = ctx.handler.subscription();
        let expected = CodeIndexingTaskRequest::subscription();
        assert_eq!(subscription.stream, expected.stream);
        assert_eq!(subscription.subject, expected.subject);
    }

    async fn work_for(secs: u64) -> Result<(), HandlerError> {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn phases_share_one_work_budget() {
        let cancel = CancellationToken::new();
        let mut clock = WorkClock::new(Some(Duration::from_secs(250)));

        let started = tokio::time::Instant::now();
        clock
            .run(work_for(100), &cancel, 1, 7, "main")
            .await
            .expect("first phase fits");
        let second = clock.run(work_for(200), &cancel, 1, 7, "main").await;

        assert!(second.is_err(), "100s + 200s must overrun a 250s budget");
        assert!(cancel.is_cancelled());
        assert_eq!(
            started.elapsed().as_secs(),
            250,
            "the second phase gets only what the first left"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_disabled_budget_never_times_out() {
        let cancel = CancellationToken::new();
        let mut clock = WorkClock::new(None);
        clock
            .run(work_for(10_000), &cancel, 1, 7, "main")
            .await
            .expect("no budget means no deadline");
        assert!(!cancel.is_cancelled());
    }

    #[test]
    fn project_lock_key_formats_correctly() {
        assert_eq!(
            project_lock_key(42, "refs/heads/main"),
            "project.42.cmVmcy9oZWFkcy9tYWlu"
        );
    }
}
