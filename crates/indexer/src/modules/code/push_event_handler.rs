//! Handler for processing push events and triggering code indexing.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use siphon_proto::replication_event::Operation;
use tracing::{debug, info, warn};

use serde::{Deserialize, Serialize};

use super::checkpoint_store::CodeCheckpointStore;
use super::config::CODE_LOCK_TTL;
use super::config::{siphon_actions, siphon_ref_types, subjects};
use super::indexing_pipeline::{CodeIndexingPipeline, IndexingRequest};
use super::locking::project_lock_key;
use super::metrics::{CodeMetrics, RecordStageError};
use super::project_store::{ProjectInfo, ProjectStore};
use super::repository_service::RepositoryService;
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Topic};

fn default_events_stream_name() -> String {
    "siphon_stream_main_db".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PushEventHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_events_stream_name")]
    pub events_stream_name: String,
}

impl Default for PushEventHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            events_stream_name: default_events_stream_name(),
        }
    }
}

pub struct PushEventHandler {
    pipeline: Arc<CodeIndexingPipeline>,
    repository_service: Arc<dyn RepositoryService>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    project_store: Arc<dyn ProjectStore>,
    metrics: CodeMetrics,
    config: PushEventHandlerConfig,
}

impl PushEventHandler {
    pub fn new(
        pipeline: Arc<CodeIndexingPipeline>,
        repository_service: Arc<dyn RepositoryService>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        project_store: Arc<dyn ProjectStore>,
        metrics: CodeMetrics,
        config: PushEventHandlerConfig,
    ) -> Self {
        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            project_store,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Handler for PushEventHandler {
    fn name(&self) -> &str {
        "code_push_event"
    }

    fn topic(&self) -> Topic {
        Topic::external(
            self.config.events_stream_name.clone(),
            format!(
                "{}.{}",
                self.config.events_stream_name,
                subjects::PUSH_EVENT_PAYLOADS
            ),
        )
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        debug!(message_id = %message.id.0, "received push event payload");

        let replication_events = decode_logical_replication_events(&message.payload)
            .inspect_err(|e| warn!(message_id = %message.id.0, error = %e, "failed to decode push event payload"))
            .map_err(HandlerError::Processing)
            .record_error_stage(&self.metrics, "decode")?;

        let extractor = ColumnExtractor::new(&replication_events);

        for event in &replication_events.events {
            if event.operation == Operation::InitialSnapshot as i32 {
                debug!("skipping initial snapshot event");
                continue;
            }

            let Some(push_event) = PushEventPayload::extract(&extractor, event) else {
                debug!("failed to extract push event payload, skipping");
                continue;
            };

            debug!(
                event_id = push_event.event_id,
                project_id = push_event.project_id,
                ref_name = %push_event.ref_name,
                "processing push event"
            );

            if let Err(e) = self.process_push_event(&context, &push_event).await {
                warn!(
                    event_id = push_event.event_id,
                    project_id = push_event.project_id,
                    ref_name = %push_event.ref_name,
                    error = %e,
                    "failed to process push event"
                );
            }
        }

        Ok(())
    }
}

impl PushEventHandler {
    async fn process_push_event(
        &self,
        context: &HandlerContext,
        event: &PushEventPayload,
    ) -> Result<(), HandlerError> {
        let started_at = Instant::now();
        let Some(branch) = self.validate_push_event(event) else {
            return Ok(());
        };

        let project_id = event.project_id;

        let repository = self
            .repository_service
            .repository_info(project_id)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to fetch repository info: {e}")))
            .record_error_stage(&self.metrics, "repository_fetch")?;

        let default_branch = repository
            .default_branch
            .strip_prefix("refs/heads/")
            .unwrap_or(&repository.default_branch);

        if branch != default_branch {
            debug!(
                event_id = event.event_id,
                project_id,
                branch = %branch,
                "skipping non-default branch"
            );
            self.metrics.record_outcome("skipped_branch");
            return Ok(());
        }

        let Some(project) = self.lookup_project(event.event_id, project_id).await? else {
            self.metrics.record_outcome("skipped_project_not_found");
            return Err(HandlerError::Processing(
                "project not found in knowledge graph".into(),
            ));
        };

        if self
            .is_already_indexed(event, &project.traversal_path, project_id, &branch)
            .await
        {
            self.metrics.record_outcome("skipped_checkpoint");
            return Ok(());
        }

        info!(
            event_id = event.event_id,
            project_id,
            branch = %branch,
            "starting code indexing"
        );

        let result = self
            .index_with_lock(context, event, project_id, &branch, &project, &repository)
            .await;

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
        event: &PushEventPayload,
        project_id: i64,
        branch: &str,
        project: &ProjectInfo,
        repository: &gitlab_client::RepositoryInfo,
    ) -> Result<(), HandlerError> {
        if !self.try_acquire_lock(context, project_id, branch).await? {
            debug!(
                event_id = event.event_id,
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
                    traversal_path: project.traversal_path.clone(),
                    event_id: event.event_id,
                    commit_sha: event.revision_after.clone(),
                    repository: repository.clone(),
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

impl PushEventHandler {
    fn validate_push_event(&self, event: &PushEventPayload) -> Option<String> {
        if event.ref_type != siphon_ref_types::BRANCH {
            debug!(event_id = event.event_id, "skipping non-branch push");
            return None;
        }

        if event.action != siphon_actions::PUSHED {
            debug!(event_id = event.event_id, "skipping non-push action");
            return None;
        }

        Some(Self::extract_branch_name(&event.ref_name))
    }

    fn extract_branch_name(ref_name: &str) -> String {
        ref_name
            .strip_prefix("refs/heads/")
            .unwrap_or(ref_name)
            .to_string()
    }
}

impl PushEventHandler {
    async fn lookup_project(
        &self,
        event_id: i64,
        project_id: i64,
    ) -> Result<Option<ProjectInfo>, HandlerError> {
        match self.project_store.get_project(project_id).await {
            Ok(info) => {
                if info.is_none() {
                    debug!(event_id, project_id, "project not in knowledge graph");
                }
                Ok(info)
            }
            Err(e) => Err(HandlerError::Processing(format!(
                "project lookup failed: {e}"
            ))),
        }
    }

    async fn is_already_indexed(
        &self,
        event: &PushEventPayload,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
    ) -> bool {
        if let Ok(Some(checkpoint)) = self
            .checkpoint_store
            .get_checkpoint(traversal_path, project_id, branch)
            .await
            && checkpoint.last_event_id >= event.event_id
        {
            debug!(event_id = event.event_id, "already indexed, skipping");
            return true;
        }
        false
    }
}

impl PushEventHandler {
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
struct PushEventPayload {
    event_id: i64,
    project_id: i64,
    ref_type: i32,
    action: i32,
    ref_name: String,
    revision_after: String,
}

impl PushEventPayload {
    fn extract(
        extractor: &ColumnExtractor<'_>,
        event: &siphon_proto::ReplicationEvent,
    ) -> Option<Self> {
        Some(Self {
            event_id: extractor.get_i64(event, "event_id")?,
            project_id: extractor.get_i64(event, "project_id")?,
            ref_type: extractor.get_i32(event, "ref_type")?,
            action: extractor.get_i32(event, "action")?,
            ref_name: extractor.get_string(event, "ref")?.to_string(),
            revision_after: Self::parse_commit(extractor.get_string(event, "commit_to")?),
        })
    }

    fn parse_commit(value: &str) -> String {
        value.strip_prefix("\\x").unwrap_or(value).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::PushEventHandlerConfig;
    use super::*;
    use crate::handler::Handler;
    use crate::modules::code::checkpoint_store::CodeCheckpointStore;
    use crate::modules::code::checkpoint_store::CodeIndexingCheckpoint;
    use crate::modules::code::checkpoint_store::test_utils::MockCodeCheckpointStore;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::project_store::ProjectInfo;
    use crate::modules::code::project_store::test_utils::MockProjectStore;
    use crate::modules::code::repository_service::RepositoryService;
    use crate::modules::code::repository_service::test_utils::MockRepositoryService;
    use crate::modules::code::stale_data_cleaner::test_utils::MockStaleDataCleaner;
    use crate::modules::code::test_helpers::{build_replication_events, push_payload_columns};
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use chrono::Utc;

    fn test_metrics() -> CodeMetrics {
        CodeMetrics::with_meter(&crate::testkit::test_meter())
    }

    struct TestContext {
        handler: PushEventHandler,
        mock_nats: Arc<MockNatsServices>,
        mock_locks: Arc<MockLockService>,
        mock_checkpoints: Arc<MockCodeCheckpointStore>,
        project_store: Arc<MockProjectStore>,
    }

    impl TestContext {
        fn with_repository_service(repository_service: Arc<dyn RepositoryService>) -> Self {
            let mock_nats = Arc::new(MockNatsServices::new());
            let mock_locks = Arc::new(MockLockService::new());
            let mock_checkpoints = Arc::new(MockCodeCheckpointStore::new());
            let project_store = Arc::new(MockProjectStore::new());
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

            let handler = PushEventHandler::new(
                pipeline,
                repository_service,
                Arc::clone(&checkpoint_store),
                project_store.clone(),
                metrics,
                PushEventHandlerConfig::default(),
            );

            Self {
                handler,
                mock_nats,
                mock_locks,
                mock_checkpoints,
                project_store,
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

        async fn set_checkpoint(&self, project_id: i64, branch: &str, last_event_id: i64) {
            self.mock_checkpoints
                .set_checkpoint(&CodeIndexingCheckpoint {
                    traversal_path: format!("/org/project-{}", project_id),
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
    async fn ignores_non_default_branch_pushes() {
        let mock_repo = MockRepositoryService::with_default_branch(123, "refs/heads/main");
        let ctx = TestContext::with_repository_service(mock_repo);
        ctx.add_project(123);

        let payload = build_replication_events(vec![
            push_payload_columns(1, 123, "refs/heads/feature/new-thing", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "feature/new-thing"));
    }

    #[tokio::test]
    async fn skips_already_indexed_commits() {
        let mock_repo = MockRepositoryService::with_default_branch(123, "refs/heads/main");
        let ctx = TestContext::with_repository_service(mock_repo);
        ctx.add_project(123);
        ctx.set_checkpoint(123, "main", 100).await;

        let payload = build_replication_events(vec![
            push_payload_columns(50, 123, "refs/heads/main", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn does_not_acquire_lock_when_project_not_in_knowledge_graph() {
        let mock_repo = MockRepositoryService::with_default_branch(999, "refs/heads/main");
        let ctx = TestContext::with_repository_service(mock_repo);

        let payload = build_replication_events(vec![
            push_payload_columns(100, 999, "refs/heads/main", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        ctx.handler
            .handle(ctx.handler_context(), envelope)
            .await
            .unwrap();

        assert!(!ctx.lock_exists(999, "main"));
    }

    #[tokio::test]
    async fn skips_when_lock_already_held() {
        let mock_repo = MockRepositoryService::with_default_branch(123, "refs/heads/main");
        let ctx = TestContext::with_repository_service(mock_repo);
        ctx.add_project(123);
        ctx.set_lock(123, "main");

        let payload = build_replication_events(vec![
            push_payload_columns(100, 123, "refs/heads/main", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn skips_initial_snapshot_events() {
        use siphon_proto::replication_event::Operation;

        let mock_repo = MockRepositoryService::with_default_branch(123, "refs/heads/main");
        let ctx = TestContext::with_repository_service(mock_repo);
        ctx.add_project(123);

        let payload = build_replication_events(vec![
            push_payload_columns(100, 123, "refs/heads/main", "abc123")
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }
}
