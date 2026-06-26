use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use code_graph::v2::CancellationToken;
use gitlab_client::GitlabClientError;
use tracing::{debug, info, warn};

use super::checkpoint::{CodeCheckpointStore, CodeIndexingCheckpoint};
use super::metrics::CodeMetrics;
use super::observer::CodeOtelObserver;
use super::pipeline::{CodeIndexingPipeline, IndexOutcome, IndexingRequest, PendingFlush};
use super::repository::{EmptyRepositoryReason, RepositoryService, RepositoryServiceError};
use crate::analytics::IndexingAnalytics;

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

fn project_lock_key(project_id: i64, branch: &str) -> String {
    use base64::Engine;
    let encoded_branch = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(branch);
    format!("project.{project_id}.{encoded_branch}")
}

pub struct CodeIndexingTaskHandler {
    pipeline: Arc<CodeIndexingPipeline>,
    repository_service: Arc<dyn RepositoryService>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    metrics: CodeMetrics,
    lock_ttl: Duration,
    write_buffer_heartbeat: Duration,
    subscription: Subscription,
    analytics: IndexingAnalytics,
}

impl CodeIndexingTaskHandler {
    #[allow(
        clippy::too_many_arguments,
        reason = "handler constructor wires all collaborators explicitly; grouping into a struct would just move the arity"
    )]
    pub fn new(
        pipeline: Arc<CodeIndexingPipeline>,
        repository_service: Arc<dyn RepositoryService>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        metrics: CodeMetrics,
        lock_ttl: Duration,
        write_buffer_heartbeat: Duration,
        subscription: Subscription,
        analytics: IndexingAnalytics,
    ) -> Self {
        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            metrics,
            lock_ttl,
            write_buffer_heartbeat,
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

    async fn index_with_lock(
        &self,
        context: &HandlerContext,
        request: &CodeIndexingTaskRequest,
        branch: &str,
        had_prior_checkpoint: bool,
        started_at: DateTime<Utc>,
        observer: &mut dyn IndexingObserver,
    ) -> Result<Option<&'static str>, HandlerError> {
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
        // On timeout: cancel so the detached parse bails, and drop the future before its flush so nothing commits; the error is transient (retried, then DLQ'd).
        let cancel = CancellationToken::new();
        let work =
            self.pipeline
                .index_project(context, &indexing_request, observer, cancel.clone());
        let result = match self.pipeline.job_timeout() {
            Some(timeout) => match tokio::time::timeout(timeout, work).await {
                Ok(result) => result,
                Err(_) => {
                    cancel.cancel();
                    warn!(
                        project_id,
                        branch = %branch,
                        timeout_secs = timeout.as_secs(),
                        "code indexing job exceeded wall-clock timeout"
                    );
                    Err(HandlerError::Processing(format!(
                        "code indexing job exceeded the {}s timeout",
                        timeout.as_secs()
                    )))
                }
            },
            None => work.await,
        };

        // Rows aren't durable until the shared coalescer flushes. Wait for the flush
        // watermark to reach this project's seq, heartbeating the lease and renewing the lock
        // so neither lapses, then checkpoint. This wait is intentionally outside the job
        // timeout: flush latency depends on other projects, not this job's own work.
        let result: Result<&'static str, HandlerError> = match result {
            Ok(outcome @ IndexOutcome::EmptyRepository) => Ok(outcome.metric_label()),
            Ok(IndexOutcome::Indexed(pending)) => self
                .await_buffered_flush(context, &key, project_id, branch, pending)
                .await
                .map(|()| "indexed"),
            Err(e) => Err(e),
        };

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

    async fn await_buffered_flush(
        &self,
        context: &HandlerContext,
        lock_key: &str,
        project_id: i64,
        branch: &str,
        mut pending: PendingFlush,
    ) -> Result<(), HandlerError> {
        let mut beat = tokio::time::interval(self.write_buffer_heartbeat);
        beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        beat.tick().await;

        while *pending.watermark.borrow() < pending.seq {
            tokio::select! {
                changed = pending.watermark.changed() => {
                    if changed.is_err() {
                        return Err(HandlerError::Processing(
                            "code write sink closed before flushing buffered project".into(),
                        ));
                    }
                }
                _ = beat.tick() => {
                    context.progress.notify_in_progress().await;
                    if let Err(e) = context.lock_service.renew(lock_key, self.lock_ttl).await {
                        warn!(project_id, branch = %branch, error = %e, "failed to renew lock while awaiting buffered flush");
                    }
                }
            }
        }

        self.pipeline.set_checkpoint(&pending.checkpoint).await
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
            Self::new_with_heartbeat(Duration::from_secs(90))
        }

        fn new_with_heartbeat(heartbeat: Duration) -> Self {
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

            let sink = crate::testkit::test_write_sink();
            let pipeline = Arc::new(CodeIndexingPipeline::new(
                resolver,
                sink,
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
                heartbeat,
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
            Envelope::new(&CodeIndexingTaskRequest {
                task_id,
                project_id,
                branch: Some(branch.to_string()),
                commit_sha: Some("abc123".to_string()),
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
            .get_checkpoint("1/123/", 123, "main")
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

    #[test]
    fn project_lock_key_formats_correctly() {
        assert_eq!(
            project_lock_key(42, "refs/heads/main"),
            "project.42.cmVmcy9oZWFkcy9tYWlu"
        );
    }

    #[tokio::test]
    async fn buffered_flush_renews_lock_then_checkpoints_after_watermark() {
        let ctx = TestContext::new_with_heartbeat(Duration::from_millis(10));
        let (wm_tx, watermark) = tokio::sync::watch::channel(0u64);
        let pending = PendingFlush {
            seq: 5,
            watermark,
            checkpoint: CodeIndexingCheckpoint {
                traversal_path: "1/7/".into(),
                project_id: 7,
                branch: "main".into(),
                last_task_id: 42,
                last_commit: Some("deadbeef".into()),
                indexed_at: Utc::now(),
            },
        };

        let context = ctx.handler_context();
        let key = project_lock_key(7, "main");

        // Release the flush only after the wait has heartbeated at least once, so the
        // assertions on "not yet durable" hold deterministically.
        let releaser = {
            let locks = ctx.mock_locks.clone();
            tokio::spawn(async move {
                while locks.renew_count() == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                wm_tx.send(5).unwrap();
            })
        };

        ctx.handler
            .await_buffered_flush(&context, &key, 7, "main", pending)
            .await
            .unwrap();
        releaser.await.unwrap();

        assert!(
            ctx.mock_locks.renew_count() >= 1,
            "lock must be renewed while awaiting the buffered flush",
        );
        let checkpoint = ctx
            .mock_checkpoints
            .get_checkpoint("1/7/", 7, "main")
            .await
            .unwrap()
            .expect("checkpoint written after watermark reached the project seq");
        assert_eq!(checkpoint.last_task_id, 42);
    }
}
