use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use siphon_proto::replication_event::Operation;
use tracing::{debug, info, warn};

use serde::{Deserialize, Serialize};

use super::checkpoint_store::CodeCheckpointStore;
use super::config::CODE_LOCK_TTL;
use super::config::subjects;
use super::indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
use super::locking::project_lock_key;
use super::metrics::{CodeMetrics, RecordStageError};
use super::repository_service::RepositoryService;
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::{CODE_BACKFILL_SUBJECT_PREFIX, INDEXER_STREAM};
use crate::types::{Envelope, Subscription, SubscriptionSource};

fn default_events_stream_name() -> String {
    "siphon_stream_main_db".to_string()
}

fn default_fan_in_stream_name() -> String {
    "GKG_CODE_INDEXING".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CodeIndexingHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,

    #[serde(default = "default_fan_in_stream_name")]
    pub fan_in_stream_name: String,
}

impl Default for CodeIndexingHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            events_stream_name: default_events_stream_name(),
            fan_in_stream_name: default_fan_in_stream_name(),
        }
    }
}

pub struct CodeIndexingHandler {
    pipeline: Arc<CodeIndexingPipeline>,
    repository_service: Arc<dyn RepositoryService>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    metrics: CodeMetrics,
    config: CodeIndexingHandlerConfig,
}

impl CodeIndexingHandler {
    pub fn new(
        pipeline: Arc<CodeIndexingPipeline>,
        repository_service: Arc<dyn RepositoryService>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        metrics: CodeMetrics,
        config: CodeIndexingHandlerConfig,
    ) -> Self {
        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            metrics,
            config,
        }
    }

    fn siphon_subject(&self) -> String {
        format!(
            "{}.{}",
            self.config.events_stream_name,
            subjects::CODE_INDEXING_TASKS
        )
    }
}

#[async_trait]
impl Handler for CodeIndexingHandler {
    fn name(&self) -> &str {
        "code_indexing"
    }

    fn subscription(&self) -> Subscription {
        let siphon_source = SubscriptionSource::new(
            self.config.events_stream_name.clone(),
            self.siphon_subject(),
        )
        .manage_stream(false);

        let backfill_source =
            SubscriptionSource::new(INDEXER_STREAM, format!("{CODE_BACKFILL_SUBJECT_PREFIX}.*"));

        Subscription::sourced(
            self.config.fan_in_stream_name.clone(),
            vec![siphon_source, backfill_source],
        )
        .dead_letter_on_exhaustion(true)
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        if message.subject.starts_with(CODE_BACKFILL_SUBJECT_PREFIX) {
            self.handle_backfill(&context, &message).await
        } else {
            self.handle_siphon_task(&context, &message).await
        }
    }
}

impl CodeIndexingHandler {
    async fn handle_siphon_task(
        &self,
        context: &HandlerContext,
        message: &Envelope,
    ) -> Result<(), HandlerError> {
        debug!(message_id = %message.id.0, "received code indexing task");

        let replication_events = decode_logical_replication_events(&message.payload)
            .inspect_err(|e| warn!(message_id = %message.id.0, error = %e, "failed to decode code indexing task"))
            .map_err(HandlerError::Processing)
            .record_error_stage(&self.metrics, "decode")?;

        let extractor = ColumnExtractor::new(&replication_events);

        for event in &replication_events.events {
            if event.operation == Operation::InitialSnapshot as i32 {
                debug!("skipping initial snapshot event");
                continue;
            }

            let Some(task) = CdcCodeIndexingTask::extract(&extractor, event) else {
                debug!("failed to extract code indexing task, skipping");
                continue;
            };

            let branch = task.branch_name();
            let request = IndexingRequest {
                project_id: task.project_id,
                branch,
                traversal_path: task.traversal_path,
                event_id: task.id,
                commit_sha: task.commit_sha,
            };

            if let Err(e) = self.process_indexing_request(context, &request).await {
                warn!(
                    project_id = request.project_id,
                    error = %e,
                    "failed to process code indexing task"
                );
            }
        }

        Ok(())
    }

    async fn handle_backfill(
        &self,
        context: &HandlerContext,
        message: &Envelope,
    ) -> Result<(), HandlerError> {
        let backfill: crate::topic::CodeBackfillRequest = serde_json::from_slice(&message.payload)
            .map_err(|e| {
                HandlerError::Processing(format!("failed to deserialize backfill request: {e}"))
            })?;

        debug!(
            project_id = backfill.project_id,
            "received backfill request"
        );

        let project_info = self
            .repository_service
            .project_info(backfill.project_id)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to fetch project info: {e}")))
            .record_error_stage(&self.metrics, "repository_fetch")?;

        let default_branch = project_info.default_branch;

        let request = IndexingRequest {
            project_id: backfill.project_id,
            branch: default_branch.clone(),
            traversal_path: backfill.traversal_path,
            event_id: 0,
            commit_sha: default_branch,
        };

        self.process_indexing_request(context, &request).await
    }
}

impl CodeIndexingHandler {
    async fn process_indexing_request(
        &self,
        context: &HandlerContext,
        request: &IndexingRequest,
    ) -> Result<(), HandlerError> {
        let started_at = Instant::now();
        let project_id = request.project_id;
        let branch = &request.branch;

        if self.is_already_indexed(request).await {
            self.metrics.record_outcome("skipped_checkpoint");
            return Ok(());
        }

        let lock_key = project_lock_key(project_id, branch);
        let acquired = context
            .lock_service
            .try_acquire(&lock_key, CODE_LOCK_TTL)
            .await
            .map_err(|e| HandlerError::Processing(format!("lock acquire failed: {e}")))?;

        if !acquired {
            debug!(project_id, branch = %branch, "lock held by another indexer, skipping");
            self.metrics.record_outcome("skipped_lock");
            return Ok(());
        }

        info!(project_id, branch = %branch, "starting code indexing");

        let result = self.pipeline.index_project(context, request).await;

        if let Err(e) = context.lock_service.release(&lock_key).await {
            warn!(project_id, branch = %branch, error = %e, "failed to release lock");
        }

        let outcome = if result.is_ok() { "indexed" } else { "error" };
        self.metrics.record_outcome(outcome);
        self.metrics
            .handler_duration
            .record(started_at.elapsed().as_secs_f64(), &[]);

        if let Err(e) = &result {
            warn!(project_id, branch = %branch, error = %e, "failed to index code");
        }

        result
    }

    async fn is_already_indexed(&self, request: &IndexingRequest) -> bool {
        if let Ok(Some(checkpoint)) = self
            .checkpoint_store
            .get_checkpoint(&request.traversal_path, request.project_id, &request.branch)
            .await
            && checkpoint.last_event_id >= request.event_id
        {
            debug!(
                project_id = request.project_id,
                event_id = request.event_id,
                "already indexed, skipping"
            );
            return true;
        }
        false
    }
}

#[derive(Debug, Clone)]
struct CdcCodeIndexingTask {
    id: i64,
    project_id: i64,
    ref_name: String,
    commit_sha: String,
    traversal_path: String,
}

impl CdcCodeIndexingTask {
    fn extract(
        extractor: &ColumnExtractor<'_>,
        event: &siphon_proto::ReplicationEvent,
    ) -> Option<Self> {
        Some(Self {
            id: extractor.get_i64(event, "id")?,
            project_id: extractor.get_i64(event, "project_id")?,
            ref_name: extractor.get_string(event, "ref")?.to_string(),
            commit_sha: extractor.get_string(event, "commit_sha")?.to_string(),
            traversal_path: extractor.get_string(event, "traversal_path")?.to_string(),
        })
    }

    fn branch_name(&self) -> String {
        self.ref_name
            .strip_prefix("refs/heads/")
            .unwrap_or(&self.ref_name)
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::code::checkpoint_store::CodeCheckpointStore;
    use crate::modules::code::checkpoint_store::CodeIndexingCheckpoint;
    use crate::modules::code::checkpoint_store::test_utils::MockCodeCheckpointStore;
    use crate::modules::code::indexing_pipeline::CodeIndexingPipeline;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::repository_service::test_utils::MockRepositoryService;
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use crate::modules::code::test_helpers::{
        build_replication_events, code_indexing_task_columns,
    };
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use crate::topic::CodeBackfillRequest;
    use chrono::Utc;

    fn test_metrics() -> CodeMetrics {
        CodeMetrics::with_meter(&crate::testkit::test_meter())
    }

    struct TestContext {
        handler: CodeIndexingHandler,
        mock_nats: Arc<MockNatsServices>,
        mock_locks: Arc<MockLockService>,
        mock_checkpoints: Arc<MockCodeCheckpointStore>,
    }

    impl TestContext {
        fn new() -> Self {
            let mock_repo = MockRepositoryService::with_default_branch(123, "main");
            Self::with_repository_service(mock_repo)
        }

        fn with_repository_service(repository_service: Arc<dyn RepositoryService>) -> Self {
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

            let pipeline = Arc::new(CodeIndexingPipeline::new(
                Arc::clone(&repository_service),
                Arc::clone(&checkpoint_store),
                stale_data_cleaner,
                metrics.clone(),
                table_names,
            ));

            let handler = CodeIndexingHandler::new(
                pipeline,
                repository_service,
                Arc::clone(&checkpoint_store),
                metrics,
                CodeIndexingHandlerConfig::default(),
            );

            Self {
                handler,
                mock_nats,
                mock_locks,
                mock_checkpoints,
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

        fn siphon_envelope(payload: bytes::Bytes) -> Envelope {
            TestEnvelopeFactory::with_subject(
                "siphon_stream_main_db.p_knowledge_graph_code_indexing_tasks",
                payload,
            )
        }

        fn backfill_envelope(project_id: i64) -> Envelope {
            let request = CodeBackfillRequest {
                project_id,
                traversal_path: format!("/org/project-{project_id}"),
            };
            let mut envelope = Envelope::new(&request).unwrap();
            envelope.subject = Arc::from(format!("{CODE_BACKFILL_SUBJECT_PREFIX}.{project_id}"));
            envelope
        }

        async fn set_checkpoint(
            &self,
            project_id: i64,
            traversal_path: &str,
            branch: &str,
            last_event_id: i64,
        ) {
            self.mock_checkpoints
                .set_checkpoint(&CodeIndexingCheckpoint {
                    traversal_path: traversal_path.to_string(),
                    project_id,
                    branch: branch.to_string(),
                    last_event_id,
                    last_commit: "abc".to_string(),
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

    #[test]
    fn handler_name() {
        let ctx = TestContext::new();
        assert_eq!(ctx.handler.name(), "code_indexing");
    }

    #[test]
    fn subscription_is_sourced_from_siphon_and_backfill() {
        let ctx = TestContext::new();
        let subscription = ctx.handler.subscription();

        assert_eq!(subscription.stream.as_ref(), "GKG_CODE_INDEXING");
        assert_eq!(subscription.subject.as_ref(), ">");
        assert!(subscription.dead_letter_on_exhaustion);
        assert_eq!(subscription.sources.len(), 2);

        let siphon = &subscription.sources[0];
        assert_eq!(siphon.stream.as_ref(), "siphon_stream_main_db");
        assert!(!siphon.manage_stream);

        let backfill = &subscription.sources[1];
        assert_eq!(backfill.stream.as_ref(), INDEXER_STREAM);
        assert!(backfill.manage_stream);
    }

    #[tokio::test]
    async fn siphon_skips_already_indexed_commits() {
        let ctx = TestContext::new();
        ctx.set_checkpoint(123, "/org/project-123", "main", 100)
            .await;

        let payload = build_replication_events(vec![
            code_indexing_task_columns(50, 123, "main", "abc123", "/org/project-123").build(),
        ]);
        let envelope = TestContext::siphon_envelope(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn siphon_skips_when_lock_held() {
        let ctx = TestContext::new();
        ctx.set_lock(123, "main");

        let payload = build_replication_events(vec![
            code_indexing_task_columns(100, 123, "main", "abc123", "/org/project-123").build(),
        ]);
        let envelope = TestContext::siphon_envelope(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn siphon_skips_initial_snapshot_events() {
        use siphon_proto::replication_event::Operation;

        let ctx = TestContext::new();

        let payload = build_replication_events(vec![
            code_indexing_task_columns(100, 123, "main", "abc123", "/org/project-123")
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        let envelope = TestContext::siphon_envelope(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn siphon_strips_refs_heads_prefix() {
        let task = CdcCodeIndexingTask {
            id: 1,
            project_id: 123,
            ref_name: "refs/heads/main".to_string(),
            commit_sha: "abc123".to_string(),
            traversal_path: "/org/project-123".to_string(),
        };

        assert_eq!(task.branch_name(), "main");
    }

    #[tokio::test]
    async fn backfill_skips_when_already_indexed() {
        let ctx = TestContext::new();

        ctx.set_checkpoint(123, "/org/project-123", "main", 0).await;

        let envelope = TestContext::backfill_envelope(123);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn backfill_skips_when_lock_held() {
        let ctx = TestContext::new();
        ctx.set_lock(123, "main");

        let envelope = TestContext::backfill_envelope(123);
        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }
}
