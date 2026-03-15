//! Handler for processing code indexing tasks dispatched by Rails.

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
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Subscription};

fn default_events_stream_name() -> String {
    "siphon_stream_main_db".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CodeIndexingTaskHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,
}

impl Default for CodeIndexingTaskHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            events_stream_name: default_events_stream_name(),
        }
    }
}

pub struct CodeIndexingTaskHandler {
    pipeline: Arc<CodeIndexingPipeline>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    metrics: CodeMetrics,
    config: CodeIndexingTaskHandlerConfig,
}

impl CodeIndexingTaskHandler {
    pub fn new(
        pipeline: Arc<CodeIndexingPipeline>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        metrics: CodeMetrics,
        config: CodeIndexingTaskHandlerConfig,
    ) -> Self {
        Self {
            pipeline,
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
        Subscription::new(
            self.config.events_stream_name.clone(),
            format!(
                "{}.{}",
                self.config.events_stream_name,
                subjects::CODE_INDEXING_TASKS
            ),
        )
        .manage_stream(false)
        .dead_letter_on_exhaustion(true)
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
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

            let Some(task) = CodeIndexingTask::extract(&extractor, event) else {
                debug!("failed to extract code indexing task, skipping");
                continue;
            };

            debug!(
                task_id = task.id,
                project_id = task.project_id,
                "processing code indexing task"
            );

            if let Err(e) = self.process_task(&context, &task).await {
                warn!(
                    task_id = task.id,
                    project_id = task.project_id,
                    error = %e,
                    "failed to process code indexing task"
                );
            }
        }

        Ok(())
    }
}

impl CodeIndexingTaskHandler {
    async fn process_task(
        &self,
        context: &HandlerContext,
        task: &CodeIndexingTask,
    ) -> Result<(), HandlerError> {
        let started_at = Instant::now();
        let branch = task.branch_name();

        if self.is_already_indexed(task, &branch).await {
            self.metrics.record_outcome("skipped_checkpoint");
            return Ok(());
        }

        info!(
            task_id = task.id,
            project_id = task.project_id,
            branch = %branch,
            "starting code indexing"
        );

        let result = self.index_with_lock(context, task, &branch).await;

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
        task: &CodeIndexingTask,
        branch: &str,
    ) -> Result<(), HandlerError> {
        let project_id = task.project_id;

        if !self.try_acquire_lock(context, project_id, branch).await? {
            debug!(
                task_id = task.id,
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
                    traversal_path: task.traversal_path.clone(),
                    event_id: task.id,
                    commit_sha: task.commit_sha.clone(),
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
    async fn is_already_indexed(&self, task: &CodeIndexingTask, branch: &str) -> bool {
        if let Ok(Some(checkpoint)) = self
            .checkpoint_store
            .get_checkpoint(&task.traversal_path, task.project_id, branch)
            .await
            && checkpoint.last_event_id >= task.id
        {
            debug!(task_id = task.id, "already indexed, skipping");
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

#[derive(Debug, Clone)]
struct CodeIndexingTask {
    id: i64,
    project_id: i64,
    ref_name: String,
    commit_sha: String,
    traversal_path: String,
}

impl CodeIndexingTask {
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
    use crate::handler::Handler;
    use crate::modules::code::checkpoint_store::CodeCheckpointStore;
    use crate::modules::code::checkpoint_store::CodeIndexingCheckpoint;
    use crate::modules::code::checkpoint_store::test_utils::MockCodeCheckpointStore;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::repository_service::test_utils::MockRepositoryService;
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use crate::modules::code::test_helpers::{
        build_replication_events, code_indexing_task_columns,
    };
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use chrono::Utc;

    fn test_metrics() -> CodeMetrics {
        CodeMetrics::with_meter(&crate::testkit::test_meter())
    }

    struct TestContext {
        handler: CodeIndexingTaskHandler,
        mock_nats: Arc<MockNatsServices>,
        mock_locks: Arc<MockLockService>,
        mock_checkpoints: Arc<MockCodeCheckpointStore>,
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

            let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
            let table_names = Arc::new(
                crate::modules::code::config::CodeTableNames::from_ontology(&ontology)
                    .expect("code tables must resolve"),
            );

            let pipeline = Arc::new(CodeIndexingPipeline::new(
                mock_repo,
                Arc::clone(&checkpoint_store),
                stale_data_cleaner,
                metrics.clone(),
                table_names,
            ));

            let handler = CodeIndexingTaskHandler::new(
                pipeline,
                Arc::clone(&checkpoint_store),
                metrics,
                CodeIndexingTaskHandlerConfig::default(),
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

    #[tokio::test]
    async fn skips_already_indexed_commits() {
        let ctx = TestContext::new();
        ctx.set_checkpoint(123, "/org/project-123", "main", 100)
            .await;

        let payload = build_replication_events(vec![
            code_indexing_task_columns(50, 123, "main", "abc123", "/org/project-123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn skips_when_lock_already_held() {
        let ctx = TestContext::new();
        ctx.set_lock(123, "main");

        let payload = build_replication_events(vec![
            code_indexing_task_columns(100, 123, "main", "abc123", "/org/project-123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn skips_initial_snapshot_events() {
        use siphon_proto::replication_event::Operation;

        let ctx = TestContext::new();

        let payload = build_replication_events(vec![
            code_indexing_task_columns(100, 123, "main", "abc123", "/org/project-123")
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn strips_refs_heads_prefix_from_ref() {
        let task = CodeIndexingTask {
            id: 1,
            project_id: 123,
            ref_name: "refs/heads/main".to_string(),
            commit_sha: "abc123".to_string(),
            traversal_path: "/org/project-123".to_string(),
        };

        assert_eq!(task.branch_name(), "main");
    }

    #[tokio::test]
    async fn handles_ref_without_prefix() {
        let task = CodeIndexingTask {
            id: 1,
            project_id: 123,
            ref_name: "main".to_string(),
            commit_sha: "abc123".to_string(),
            traversal_path: "/org/project-123".to_string(),
        };

        assert_eq!(task.branch_name(), "main");
    }
}
