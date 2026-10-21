use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use code_graph::v2::CancellationToken;
use gitlab_client::{GitlabClientError, ProjectInfo};
use orbit_log_fields::{debug, expanded, info, safe, warn};
use tracing::Instrument;

use super::checkpoint::{CodeCheckpoint, CodeCheckpointStore};
use super::metrics::CodeMetrics;
use super::observer::CodeOtelObserver;
use super::pipeline::{CodeIndexer, IndexError, IndexingRequest};
use super::repository::{EmptyRepositoryReason, RepositoryService, RepositoryServiceError};
use crate::analytics::IndexingAnalytics;

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::locking::{LockError, LockGuard};
use crate::nats::ProgressNotifier;
use crate::observer::{self, IndexingObserver, PipelineType};
use crate::retry::GlobalRetry;
use crate::topic::CodeIndexingTaskRequest;
use crate::types::{Envelope, Subscription};

/// Sentinel branch value written to the checkpoint when the project is
/// resolved as deleted from Rails (404) and we cannot determine its default
/// branch. The dispatcher's `fetch_checkpointed_project_ids` filter keys on
/// `(traversal_path, project_id)` and ignores branch, so any non-empty value
/// satisfies the schema and dedupes future dispatch cycles.
const DELETED_PROJECT_BRANCH_SENTINEL: &str = "HEAD";

/// A timed-out job is likely transiently slow: retry once, then dead-letter (engine reads this policy).
const JOB_TIMEOUT_RETRY: GlobalRetry = GlobalRetry {
    max_attempts: 2,
    dead_letter: true,
};

pub fn project_lock_key(project_id: i64, branch: &str) -> String {
    use base64::Engine;
    let encoded_branch = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(branch);
    format!("project.{project_id}.{encoded_branch}")
}

/// Heartbeat lives in this future (not a spawned task), so it can't leak; it ticks whenever `work` is pending, through the blocking parse and the wait for the rows to land.
async fn run_with_heartbeat<T>(
    work: impl std::future::Future<Output = T>,
    progress: &ProgressNotifier,
    guard: &LockGuard,
    lock_ttl: Duration,
    interval: Duration,
    cancel: &CancellationToken,
) -> T {
    tokio::pin!(work);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            outcome = &mut work => return outcome,
            _ = ticker.tick() => {
                if cancel.is_cancelled() {
                    continue;
                }
                progress.notify_in_progress().await;
                match guard.renew(lock_ttl).await {
                    Ok(()) => {}
                    Err(error @ LockError::Lost) => {
                        warn!(
                            expanded(ERROR_MESSAGE) = %error,
                            "project lock lost mid-index; cancelling to avoid a double-write"
                        );
                        cancel.cancel();
                    }
                    Err(error) => {
                        warn!(
                            expanded(ERROR_MESSAGE) = %error,
                            "lock renewal failed transiently; retrying on the next heartbeat tick"
                        );
                    }
                }
            }
        }
    }
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
        if request.traversal_path.is_valid_any_depth() {
            let message_span = tracing::Span::current();
            message_span.record(
                safe::GL_ROOT_NAMESPACE_ID,
                request.traversal_path.top_level_namespace_id(),
            );
            message_span.record(safe::TRAVERSAL_PATH, request.traversal_path.as_str());
        }

        let dispatch_id = request.dispatch_id.to_string();
        let span = orbit_log_fields::info_span!(
            "code_indexing_project",
            safe(GL_PROJECT_ID) = request.project_id,
            safe(TASK_ID) = request.task_id,
            safe(COMMIT_SHA) = request.commit_sha.as_deref(),
            safe(DISPATCH_ID) = dispatch_id.as_str(),
            safe(CAMPAIGN_ID) = request.campaign_id.as_deref(),
            expanded(BRANCH) = request.branch.as_deref(),
        );
        span.in_scope(|| debug!("received code indexing task"));

        self.process_task(&context, &request, message.attempt)
            .instrument(span)
            .await
    }
}

impl CodeIndexingTaskHandler {
    /// Returns `Ok(Some((branch, project_info)))` when the branch is known,
    /// `Ok(None)` when the project is gone from Rails (terminal: the dispatcher
    /// has a stale view; acking avoids DLQ churn), and `Err` for transient
    /// failures. `project_info` is `Some` when the lookup ran, so the caller
    /// reuses it instead of asking again.
    async fn resolve_branch(
        &self,
        request: &CodeIndexingTaskRequest,
    ) -> Result<Option<(String, Option<ProjectInfo>)>, HandlerError> {
        match &request.branch {
            Some(branch) => Ok(Some((branch.clone(), None))),
            None => match self
                .repository_service
                .project_info(request.project_id)
                .await
            {
                Ok(project_info) => Ok(Some((
                    project_info.default_branch.clone(),
                    Some(project_info),
                ))),
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

        let Some((branch, project_info)) = self.resolve_branch(request).await? else {
            warn!("project not found resolving default branch; acknowledging as deleted");
            // Mirror the empty-repository path: write a checkpoint so the
            // dispatcher's `fetch_checkpointed_project_ids` filter excludes
            // this project on subsequent backfill cycles instead of
            // republishing the same task forever.
            let sentinel_branch = request
                .branch
                .as_deref()
                .unwrap_or(DELETED_PROJECT_BRANCH_SENTINEL);
            let mut checkpoint = CodeCheckpoint::new(
                request.traversal_path.clone(),
                request.project_id,
                sentinel_branch,
            );
            checkpoint.complete_empty_repository(request.task_id);
            if let Err(e) = self.checkpoint_store.save(&checkpoint).await {
                warn!(
                    expanded(ERROR_MESSAGE) = %e,
                    "failed to write deleted-project checkpoint; dispatcher may republish"
                );
            }
            self.metrics
                .record_empty_repository(EmptyRepositoryReason::NotFound.as_metric_label());
            self.metrics.record_outcome("empty_repository");
            self.metrics.record_handler_duration(started_at);
            return Ok(());
        };
        tracing::Span::current().record(expanded::BRANCH, branch.as_str());

        let checkpoint = self
            .checkpoint_store
            .load(&request.traversal_path, request.project_id, &branch)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to load checkpoint: {e}")))?
            .unwrap_or_else(|| {
                CodeCheckpoint::new(request.traversal_path.clone(), request.project_id, &branch)
            });
        if checkpoint.is_indexed_through(request.task_id) {
            debug!("already indexed, skipping");
            self.metrics.record_outcome("skipped_checkpoint");
            return Ok(());
        }
        let mode = checkpoint.indexing_mode();

        info!(
            safe(INDEXING_MODE) = mode.as_str(),
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
        observer.set_indexing_mode(mode);

        let result = self
            .index_with_lock(
                context,
                request,
                &branch,
                checkpoint,
                project_info,
                attempt,
                &mut observer,
            )
            .await;

        let outcome = match &result {
            Ok(label) => label,
            Err(HandlerError::Deferred { .. }) => "skipped_lock",
            Err(_) => "error",
        };
        self.metrics.record_outcome(outcome);
        if result.is_ok() {
            self.metrics.record_repository_indexed(outcome);
        }
        self.metrics.record_handler_duration(started_at);

        match &result {
            Ok(_) | Err(HandlerError::Deferred { .. }) => observer.finish(),
            Err(e) => {
                observer.record_error(&e.to_string());
                observer.finish();
            }
        }

        result.map(|_| ())
    }

    async fn repository_storage(&self, project_id: i64) -> Option<String> {
        match self.repository_service.project_info(project_id).await {
            Ok(info) => info.repository_storage,
            Err(error) => {
                warn!(
                    expanded(ERROR_MESSAGE) = %error,
                    "repository storage lookup failed; using the tenant-wide archive budget"
                );
                None
            }
        }
    }

    async fn refund_attempt(&self, checkpoint: &mut CodeCheckpoint) {
        checkpoint.refund_attempt();
        if let Err(e) = self.checkpoint_store.save(checkpoint).await {
            warn!(
                expanded(ERROR_MESSAGE) = %e,
                "failed to refund the attempt"
            );
        }
    }

    async fn acquire_lock(
        &self,
        context: &HandlerContext,
        key: &str,
        heartbeat_interval: Duration,
    ) -> Result<LockGuard, HandlerError> {
        let held = match LockGuard::acquire(context.lock_service.clone(), key, self.lock_ttl)
            .await
            .map_err(|e| HandlerError::Processing(format!("lock acquire failed: {e}")))?
        {
            Ok(guard) => return Ok(guard),
            Err(held) => held,
        };
        let delay = held.time_left.unwrap_or(self.lock_ttl) + heartbeat_interval;
        warn!(
            expanded(KV_KEY) = key,
            safe(RETRY_DELAY_S) = delay.as_secs_f64(),
            "code indexing deferred: lock held by another indexer"
        );
        Err(HandlerError::Deferred {
            reason: format!("lock {key} held by another indexer"),
            delay,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "indexing stage threads its collaborators and per-delivery state explicitly; a params struct would just move the arity"
    )]
    async fn index_with_lock(
        &self,
        context: &HandlerContext,
        request: &CodeIndexingTaskRequest,
        branch: &str,
        mut checkpoint: CodeCheckpoint,
        project_info: Option<ProjectInfo>,
        attempt: u32,
        observer: &mut dyn IndexingObserver,
    ) -> Result<&'static str, HandlerError> {
        if request.traversal_path.top_level_namespace_id().is_none() {
            return Err(HandlerError::Processing(format!(
                "traversal_path {:?} has no namespace_id",
                request.traversal_path.as_str()
            )));
        }

        let project_id = request.project_id;
        let key = project_lock_key(project_id, branch);

        let heartbeat_interval = self.lock_ttl / 3;
        let guard = self.acquire_lock(context, &key, heartbeat_interval).await?;

        checkpoint.start_attempt();
        self.checkpoint_store
            .save(&checkpoint)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to save start: {e}")))?;

        let repository_storage = match request
            .repository_storage
            .as_ref()
            .filter(|storage| !storage.is_empty())
        {
            Some(storage) => Some(storage.clone()),
            None => match project_info {
                Some(info) => info.repository_storage,
                None => self.repository_storage(project_id).await,
            },
        };
        let indexing_request = IndexingRequest {
            project_id,
            branch: branch.to_string(),
            traversal_path: request.traversal_path.clone(),
            task_id: request.task_id,
            commit_sha: request.commit_sha.clone(),
            checkpoint: checkpoint.clone(),
            repository_storage,
        };
        let cancel = CancellationToken::new();
        let indexing = self.pipeline.index_project(
            context,
            &indexing_request,
            observer,
            cancel.clone(),
            &guard,
        );
        let mut overloaded = false;
        let result = match run_with_heartbeat(
            indexing,
            &context.progress,
            &guard,
            self.lock_ttl,
            heartbeat_interval,
            &cancel,
        )
        .await
        {
            Ok(outcome) => Ok(outcome),
            Err(IndexError::BudgetExceeded { budget }) => {
                warn!(
                    safe(TIMEOUT_S) = budget.as_secs_f64(),
                    "code indexing job exceeded its work budget"
                );
                Err(JOB_TIMEOUT_RETRY.global_failure(
                    attempt,
                    format!(
                        "code indexing job exceeded the {}s work budget",
                        budget.as_secs()
                    ),
                ))
            }
            Err(IndexError::NoLane { waited }) => Err(HandlerError::Backpressure(format!(
                "no indexing capacity within {}s",
                waited.as_secs()
            ))),
            Err(IndexError::Overloaded(cause)) => {
                overloaded = true;
                Err(HandlerError::Processing(format!(
                    "archive endpoint overloaded ({cause}); retrying"
                )))
            }
            Err(IndexError::Failed(e)) => Err(e),
        };

        let result = result.map(|outcome| outcome.metric_label());

        if let Err(e) = &result {
            warn!(
                safe(ERROR_TYPE) = e.error_kind(),
                expanded(ERROR_MESSAGE) = %e,
                "failed to index code"
            );
        }
        let redelivered = match &result {
            Err(HandlerError::Backpressure(_)) => true,
            // Redeliveries share one dispatch; counting each would exhaust MAX_CODE_ATTEMPTS.
            Err(HandlerError::Processing(_)) if overloaded => attempt > 1,
            Err(HandlerError::Processing(_)) => context.redelivered_on_failure,
            _ => false,
        };
        if redelivered {
            match guard.renew(self.lock_ttl).await {
                Ok(()) => self.refund_attempt(&mut checkpoint).await,
                Err(error @ LockError::Lost) => warn!(
                    expanded(ERROR_MESSAGE) = %error,
                    "lock not held at the end of the run; the attempt stays counted"
                ),
                Err(error) => {
                    warn!(
                        expanded(ERROR_MESSAGE) = %error,
                        "lock renew failed transiently at the end of the run; refunding the attempt"
                    );
                    self.refund_attempt(&mut checkpoint).await;
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Handler;
    use crate::modules::code::checkpoint::CodeCheckpoint;
    use crate::modules::code::checkpoint::CodeCheckpointStore;
    use crate::modules::code::checkpoint::test_utils::MockCodeCheckpointStore;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::repository::RepositoryResolver;
    use crate::modules::code::repository::cache::LocalRepositoryCache;
    use crate::modules::code::repository::service::test_utils::{
        CountingRepositoryService, MockRepositoryService, build_tar_gz,
    };
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockLockService, MockNatsServices};
    use crate::types::Event;
    use chrono::Utc;
    use orbit_utils::traversal_path::TraversalPath;

    fn test_metrics() -> CodeMetrics {
        CodeMetrics::with_meter(&crate::testkit::test_meter())
    }

    struct TestContext {
        handler: CodeIndexingTaskHandler,
        pipeline: Arc<CodeIndexer>,
        mock_nats: Arc<MockNatsServices>,
        mock_locks: Arc<MockLockService>,
        mock_checkpoints: Arc<MockCodeCheckpointStore>,
        mock_repo: Arc<MockRepositoryService>,
        counting_repo: Arc<CountingRepositoryService>,
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
            let counting_repo = CountingRepositoryService::wrapping(mock_repo.clone());
            let repo_service: Arc<dyn RepositoryService> = counting_repo.clone();

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
                crate::testkit::builders::test_pipeline_configuration(),
            ));

            let handler = CodeIndexingTaskHandler::new(
                Arc::clone(&pipeline),
                repo_service,
                Arc::clone(&checkpoint_store),
                metrics,
                Duration::from_secs(60),
                CodeIndexingTaskRequest::subscription(),
                IndexingAnalytics::disabled(),
            );

            Self {
                handler,
                pipeline,
                mock_nats,
                mock_locks,
                mock_checkpoints,
                mock_repo,
                counting_repo,
                _cache_dir: temp_dir,
            }
        }

        fn handler_context(&self) -> HandlerContext {
            HandlerContext::new(
                self.mock_nats.clone(),
                self.mock_locks.clone(),
                ProgressNotifier::noop(),
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
            Self::make_request_full(task_id, project_id, branch, commit_sha, None)
        }

        fn make_request_with_storage(
            task_id: i64,
            project_id: i64,
            branch: &str,
            repository_storage: Option<&str>,
        ) -> Envelope {
            Self::make_request_full(
                task_id,
                project_id,
                branch,
                Some("abc123"),
                repository_storage,
            )
        }

        fn make_request_full(
            task_id: i64,
            project_id: i64,
            branch: &str,
            commit_sha: Option<&str>,
            repository_storage: Option<&str>,
        ) -> Envelope {
            Envelope::new(&CodeIndexingTaskRequest {
                task_id,
                project_id,
                branch: Some(branch.to_string()),
                commit_sha: commit_sha.map(str::to_string),
                traversal_path: TraversalPath::new_unchecked(format!("1/{project_id}/")),
                dispatch_id: uuid::Uuid::new_v4(),
                campaign_id: None,
                repository_storage: repository_storage.map(str::to_string),
            })
            .unwrap()
        }

        async fn save_indexed_checkpoint(
            &self,
            project_id: i64,
            traversal_path: &TraversalPath,
            branch: &str,
            last_task_id: i64,
        ) {
            let mut checkpoint = CodeCheckpoint::new(traversal_path.clone(), project_id, branch);
            checkpoint.complete(last_task_id, Some("abc".to_string()), Utc::now());
            self.mock_checkpoints.save(&checkpoint).await.unwrap();
        }

        fn lock_exists(&self, project_id: i64, branch: &str) -> bool {
            let key = project_lock_key(project_id, branch);
            self.mock_locks.is_held(&key)
        }
    }

    #[tokio::test]
    async fn skips_already_indexed_tasks() {
        let ctx = TestContext::new();
        ctx.save_indexed_checkpoint(123, &TraversalPath::new_unchecked("1/123/"), "main", 100)
            .await;

        let envelope = TestContext::make_request(50, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn fails_without_saving_an_attempt_when_the_checkpoint_load_fails() {
        let ctx = TestContext::new();
        ctx.mock_checkpoints.set_fail_loads(true);

        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_err(), "got {result:?}");
        ctx.mock_checkpoints.set_fail_loads(false);
        let saved = ctx
            .mock_checkpoints
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap();
        assert!(saved.is_none(), "no attempt row may replace the stored one");
    }

    #[tokio::test]
    async fn resolves_default_branch_when_branch_is_none() {
        let ctx = TestContext::new();
        ctx.save_indexed_checkpoint(123, &TraversalPath::new_unchecked("1/123/"), "main", 100)
            .await;

        let envelope = Envelope::new(&CodeIndexingTaskRequest {
            task_id: 0,
            project_id: 123,
            branch: None,
            commit_sha: None,
            traversal_path: TraversalPath::new_unchecked("1/123/"),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
            repository_storage: None,
        })
        .unwrap();

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;
        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn a_backfill_task_looks_the_project_up_once() {
        let ctx = TestContext::new();
        let envelope = Envelope::new(&CodeIndexingTaskRequest {
            task_id: 1,
            project_id: 123,
            branch: None,
            commit_sha: None,
            traversal_path: TraversalPath::new_unchecked("1/123/"),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
            repository_storage: None,
        })
        .unwrap();

        let _ = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert_eq!(ctx.counting_repo.project_info_call_count(), 1);
    }

    #[tokio::test]
    async fn a_push_task_looks_the_project_up_once() {
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );

        let envelope = TestContext::make_request(1, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok(), "got {result:?}");
        assert_eq!(ctx.counting_repo.project_info_call_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_storage_from_project_info_picks_the_archive_slots() {
        let config = crate::testkit::builders::test_pipeline_configuration();
        for branch in [None, Some("main")] {
            let ctx = TestContext::new();
            ctx.mock_repo.set_repository_storage(123, "gitaly-1");
            ctx.mock_repo.set_archive(
                123,
                build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
            );
            for slot in 0..config.archive_storage_budget {
                ctx.mock_locks
                    .set_lock(&crate::modules::code::repository::slot_key(
                        Some("gitaly-1"),
                        slot,
                    ));
            }
            let envelope = Envelope::new(&CodeIndexingTaskRequest {
                task_id: 1,
                project_id: 123,
                branch: branch.map(str::to_string),
                commit_sha: Some("abc123".to_string()),
                traversal_path: TraversalPath::new_unchecked("1/123/"),
                dispatch_id: uuid::Uuid::new_v4(),
                campaign_id: None,
                repository_storage: None,
            })
            .unwrap();

            let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

            assert!(
                matches!(result, Err(HandlerError::Backpressure(_))),
                "branch {branch:?}: got {result:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_storage_on_a_push_task_skips_the_project_lookup() {
        let config = crate::testkit::builders::test_pipeline_configuration();
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );
        for slot in 0..config.archive_storage_budget {
            ctx.mock_locks
                .set_lock(&crate::modules::code::repository::slot_key(
                    Some("gitaly-2"),
                    slot,
                ));
        }
        let envelope = TestContext::make_request_with_storage(1, 123, "main", Some("gitaly-2"));

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            matches!(result, Err(HandlerError::Backpressure(_))),
            "got {result:?}"
        );
        assert_eq!(ctx.counting_repo.project_info_call_count(), 0);
    }

    #[tokio::test]
    async fn an_empty_storage_on_a_push_task_falls_back_to_the_lookup() {
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );
        let envelope = TestContext::make_request_with_storage(1, 123, "main", Some(""));

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok(), "got {result:?}");
        assert_eq!(ctx.counting_repo.project_info_call_count(), 1);
    }

    #[test]
    fn an_envelope_without_the_storage_still_deserializes() {
        let request: CodeIndexingTaskRequest = serde_json::from_value(serde_json::json!({
            "task_id": 1,
            "project_id": 2,
            "branch": "main",
            "commit_sha": null,
            "traversal_path": "1/2/",
        }))
        .unwrap();

        assert_eq!(request.repository_storage, None);
    }

    #[tokio::test]
    async fn a_failed_storage_lookup_on_a_push_task_still_indexes() {
        use crate::modules::code::repository::RepositoryServiceError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_project_info_error(
            123,
            RepositoryServiceError::Archive("info unavailable".to_string()),
        );
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("abc123/src/main.rs", b"fn main() {}")]),
        );

        let envelope = TestContext::make_request(1, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok(), "got {result:?}");
        assert_eq!(ctx.counting_repo.project_info_call_count(), 1);
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
            traversal_path: TraversalPath::new_unchecked("1/123/"),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
            repository_storage: None,
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
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "HEAD")
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
            traversal_path: TraversalPath::new_unchecked("1/123/"),
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
            repository_storage: None,
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
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("checkpoint should be set for empty repo");
        assert_eq!(checkpoint.last_task_id, 42);
        assert!(checkpoint.last_commit.is_none());
    }

    #[tokio::test]
    async fn empty_repository_with_commit_sha_nacks_and_counts_an_attempt() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_download_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::NotFound(123)),
        );

        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx
            .handler
            .handle(ctx.handler_context().with_redelivery(false), envelope)
            .await;

        assert!(
            result.is_err(),
            "push-dispatched task must retry on empty archive, got {result:?}"
        );
        let checkpoint = ctx
            .mock_checkpoints
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("the attempt is saved");
        assert!(
            checkpoint.indexed_at.is_none(),
            "no index on the raced attempt"
        );
        assert_eq!(checkpoint.attempts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_lane_waits_while_the_heartbeat_renews_the_lock() {
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );
        let lanes = ctx.pipeline.occupy_small_indexing_lanes().await;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(45)).await;
            drop(lanes);
        });

        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok(), "got {result:?}");
        assert!(ctx.mock_locks.renew_count() >= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn losing_the_lock_while_queued_for_a_lane_stops_the_wait() {
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );
        let _lanes = ctx.pipeline.occupy_small_indexing_lanes().await;
        ctx.mock_locks.steal_leases();

        let started = tokio::time::Instant::now();
        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            matches!(result, Err(HandlerError::Backpressure(_))),
            "got {result:?}"
        );
        assert!(started.elapsed() < ctx.handler.lock_ttl);
        let checkpoint = ctx
            .mock_checkpoints
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("the started attempt is saved");
        assert_eq!(
            checkpoint.attempts, 1,
            "a run that lost its lock must not write a refund over a new holder"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn losing_the_lock_while_queued_for_an_archive_slot_stops_the_wait() {
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );
        let config = orbit_server_config::AppConfig::embedded_defaults()
            .engine
            .handlers
            .code_indexing_task
            .pipeline;
        for slot in 0..config.archive_tenant_budget {
            ctx.mock_locks
                .set_lock(&crate::modules::code::repository::slot_key(None, slot));
        }
        ctx.mock_locks.steal_leases();

        let started = tokio::time::Instant::now();
        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            matches!(result, Err(HandlerError::Backpressure(_))),
            "got {result:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(config.archive_lease_max_wait_secs)
        );
        let checkpoint = ctx
            .mock_checkpoints
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("the started attempt is saved");
        assert_eq!(
            checkpoint.attempts, 1,
            "a run that lost its lock must not write a refund over a new holder"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_saturated_archive_budget_backs_off_quickly_without_counting_an_attempt() {
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );
        let config = orbit_server_config::AppConfig::embedded_defaults()
            .engine
            .handlers
            .code_indexing_task
            .pipeline;
        for slot in 0..config.archive_tenant_budget {
            ctx.mock_locks
                .set_lock(&crate::modules::code::repository::slot_key(None, slot));
        }

        let started = tokio::time::Instant::now();
        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            matches!(result, Err(HandlerError::Backpressure(_))),
            "got {result:?}"
        );
        assert!(
            started.elapsed() <= Duration::from_secs(config.archive_lease_max_wait_secs + 1),
            "the wait is bounded by the lease wait, not the project lock"
        );
        let checkpoint = ctx
            .mock_checkpoints
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("the refunded attempt is saved");
        assert_eq!(checkpoint.attempts, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn no_free_indexing_lane_backs_off_without_counting_an_attempt() {
        let ctx = TestContext::new();
        ctx.mock_repo.set_archive(
            123,
            build_tar_gz(&[("project-abc123/src/main.rs", b"fn main() {}")]),
        );
        let _lanes = ctx.pipeline.occupy_small_indexing_lanes().await;

        let envelope = TestContext::make_request(42, 123, "main");
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(
            matches!(result, Err(HandlerError::Backpressure(_))),
            "got {result:?}"
        );
        let checkpoint = ctx
            .mock_checkpoints
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("the refunded attempt is saved");
        assert_eq!(checkpoint.attempts, 0);
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
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("checkpoint should be set for missing repository");
        assert_eq!(checkpoint.last_task_id, 7);
    }

    #[tokio::test]
    async fn archive_overload_nacks_and_never_checkpoints_empty() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let overloads: [fn() -> GitlabClientError; 2] = [
            || GitlabClientError::ServerError {
                project_id: 123,
                status: 502,
            },
            || GitlabClientError::RateLimited { project_id: 123 },
        ];
        for overload in overloads {
            for attempt in [1, 2, 9] {
                let ctx = TestContext::new();
                ctx.mock_repo
                    .set_download_error(123, RepositoryServiceError::GitlabApi(overload()));
                if attempt > 1 {
                    let mut first_delivery =
                        CodeCheckpoint::new(TraversalPath::new_unchecked("1/123/"), 123, "main");
                    first_delivery.start_attempt();
                    ctx.mock_checkpoints.save(&first_delivery).await.unwrap();
                }

                let mut envelope = TestContext::make_request_with_sha(7, 123, "main", None);
                envelope.attempt = attempt;
                let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

                assert!(
                    matches!(&result, Err(HandlerError::Processing(msg)) if msg.contains("overloaded")),
                    "overload must be retried, got {result:?}"
                );
                let checkpoint = ctx
                    .mock_checkpoints
                    .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
                    .await
                    .unwrap()
                    .expect("start-of-attempt checkpoint is saved");
                assert!(checkpoint.indexed_at.is_none(), "must not be indexed-empty");
                assert_eq!(checkpoint.last_task_id, 0);
                assert_eq!(checkpoint.attempts, 1, "delivery {attempt}");
            }
        }
    }

    #[tokio::test]
    async fn archive_overload_on_the_last_delivery_still_nacks_and_never_checkpoints_empty() {
        use crate::modules::code::repository::RepositoryServiceError;
        use gitlab_client::GitlabClientError;

        let ctx = TestContext::new();
        ctx.mock_repo.set_download_error(
            123,
            RepositoryServiceError::GitlabApi(GitlabClientError::ServerError {
                project_id: 123,
                status: 503,
            }),
        );
        let mut first_delivery =
            CodeCheckpoint::new(TraversalPath::new_unchecked("1/123/"), 123, "main");
        first_delivery.start_attempt();
        ctx.mock_checkpoints.save(&first_delivery).await.unwrap();

        let mut envelope = TestContext::make_request_with_sha(7, 123, "main", None);
        envelope.attempt = 5;
        let result = ctx
            .handler
            .handle(ctx.handler_context().with_redelivery(false), envelope)
            .await;

        assert!(
            matches!(&result, Err(HandlerError::Processing(msg)) if msg.contains("overloaded")),
            "the last delivery of an overload must still nack, got {result:?}"
        );
        let checkpoint = ctx
            .mock_checkpoints
            .load(&TraversalPath::new_unchecked("1/123/"), 123, "main")
            .await
            .unwrap()
            .expect("start-of-attempt checkpoint is saved");
        assert!(checkpoint.indexed_at.is_none(), "must not be indexed-empty");
        assert_eq!(checkpoint.attempts, 1);
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

    struct ProgressCountingAcker {
        progress_acks: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl crate::nats::MessageAcker for ProgressCountingAcker {
        async fn ack(&self) -> Result<(), nats_client::NatsError> {
            Ok(())
        }
        async fn ack_term(&self) -> Result<(), nats_client::NatsError> {
            Ok(())
        }
        async fn ack_progress(&self) -> Result<(), nats_client::NatsError> {
            self.progress_acks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
        async fn nack(&self, _delay: Option<Duration>) -> Result<(), nats_client::NatsError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn heartbeat_pings_progress_and_renews_lock_during_blocking_work() {
        let progress_acks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let message = crate::nats::NatsMessage::new(
            crate::testkit::TestEnvelopeFactory::simple("{}"),
            ProgressCountingAcker {
                progress_acks: progress_acks.clone(),
            },
        );
        let progress = message.progress_notifier();

        let locks = Arc::new(MockLockService::new());
        let guard = LockGuard::acquire(locks.clone(), "p1", Duration::from_secs(30))
            .await
            .expect("acquire ok")
            .expect("acquired");

        let cancel = CancellationToken::new();
        let work = async {
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(150)))
                .await
                .expect("blocking task ok");
            "done"
        };
        let outcome = run_with_heartbeat(
            work,
            &progress,
            &guard,
            Duration::from_secs(30),
            Duration::from_millis(25),
            &cancel,
        )
        .await;

        assert_eq!(outcome, "done");
        assert!(
            progress_acks.load(std::sync::atomic::Ordering::Relaxed) >= 3,
            "heartbeat should ping ack_progress during the blocking parse"
        );
        assert!(
            locks.renew_count() >= 1,
            "heartbeat should renew the lock during the blocking parse"
        );
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn heartbeat_cancels_the_job_when_the_lock_is_lost() {
        let locks = Arc::new(MockLockService::new());
        let guard = LockGuard::acquire(locks.clone(), "p1", Duration::from_secs(30))
            .await
            .expect("acquire ok")
            .expect("acquired");
        locks.steal_leases();

        let cancel = CancellationToken::new();
        let work_cancel = cancel.clone();
        let work = async {
            loop {
                if work_cancel.is_cancelled() {
                    break "cancelled";
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let outcome = run_with_heartbeat(
            work,
            &ProgressNotifier::noop(),
            &guard,
            Duration::from_secs(30),
            Duration::from_millis(20),
            &cancel,
        )
        .await;

        assert_eq!(outcome, "cancelled");
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn heartbeat_tolerates_transient_renew_failures_without_cancelling() {
        let locks = Arc::new(MockLockService::new());
        let guard = LockGuard::acquire(locks.clone(), "p1", Duration::from_secs(30))
            .await
            .expect("acquire ok")
            .expect("acquired");
        locks.fail_renews_transiently();

        let cancel = CancellationToken::new();
        let work = async {
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(150)))
                .await
                .expect("blocking task ok");
            "done"
        };
        let outcome = run_with_heartbeat(
            work,
            &ProgressNotifier::noop(),
            &guard,
            Duration::from_secs(30),
            Duration::from_millis(25),
            &cancel,
        )
        .await;

        assert_eq!(
            outcome, "done",
            "a transient renew error must not kill the job"
        );
        assert!(!cancel.is_cancelled());
        assert!(locks.renew_count() >= 1, "renew should have been attempted");
    }
}
