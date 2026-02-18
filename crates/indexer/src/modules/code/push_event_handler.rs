//! Handler for processing push events and triggering code indexing.

use std::sync::Arc;

use crate::module::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Topic};
use async_trait::async_trait;
use chrono::Utc;
use code_graph::indexer::{IndexingConfig, RepositoryIndexer};
use code_graph::loading::DirectoryFileSource;
use ontology::EDGE_TABLE;
use tempfile::TempDir;
use tracing::{debug, info, warn};

use super::arrow_converter::ArrowConverter;
use super::config::LOCK_TTL;
use super::config::{
    CodeIndexingConfig, buckets, siphon_actions, siphon_ref_types, subjects, tables,
};
use super::event_cache_handler::CachedEventInfo;
use super::gitaly::RepositoryService;
use super::project_store::{ProjectInfo, ProjectStore};
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};
use super::watermark_store::{CodeIndexingWatermark, CodeWatermarkStore};
use crate::modules::sdlc::locking::project_lock_key;

pub struct PushEventHandler {
    repository_service: Arc<dyn RepositoryService>,
    watermark_store: Arc<dyn CodeWatermarkStore>,
    project_store: Arc<dyn ProjectStore>,
    config: CodeIndexingConfig,
}

impl PushEventHandler {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        watermark_store: Arc<dyn CodeWatermarkStore>,
        project_store: Arc<dyn ProjectStore>,
        config: CodeIndexingConfig,
    ) -> Self {
        Self {
            repository_service,
            watermark_store,
            project_store,
            config,
        }
    }
}

#[async_trait]
impl Handler for PushEventHandler {
    fn name(&self) -> &str {
        "code-push-event"
    }

    fn topic(&self) -> Topic {
        Topic::new(
            self.config.events_stream_name.clone(),
            format!(
                "{}.{}",
                self.config.events_stream_name,
                subjects::PUSH_EVENT_PAYLOADS
            ),
        )
    }

    // TODO: Add metrics around the processed events
    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        debug!(message_id = %message.id.0, "received push event payload");

        let replication_events =
            decode_logical_replication_events(&message.payload).map_err(|e| {
                warn!(message_id = %message.id.0, error = %e, "failed to decode");
                HandlerError::Processing(e)
            })?;

        let extractor = ColumnExtractor::new(&replication_events);

        for event in &replication_events.events {
            let Some(push_event) = PushEventPayload::extract(&extractor, event) else {
                debug!("failed to extract push event payload, skipping");
                continue;
            };

            debug!(
                event_id = push_event.event_id,
                project_id = ?push_event.project_id,
                ref_name = %push_event.ref_name,
                "processing push event"
            );

            if let Err(e) = self.process_push_event(&context, &push_event).await {
                warn!(event_id = push_event.event_id, error = %e, "failed to process");
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
        let Some(branch) = self.validate_push_event(event) else {
            return Ok(());
        };

        let Some(project_id) = self.resolve_project_id(context, event).await? else {
            return Err(HandlerError::Processing(
                "no project_id in payload and not in cache".into(),
            ));
        };

        if !self.is_default_branch(project_id, &branch).await {
            debug!(
                event_id = event.event_id,
                project_id = project_id,
                branch = %branch,
                "skipping non-default branch"
            );
            return Ok(());
        }

        let Some(project) = self.lookup_project(event.event_id, project_id).await? else {
            return Err(HandlerError::Processing(
                "project not found in knowledge graph".into(),
            ));
        };

        if self.is_already_indexed(event, project_id, &branch).await {
            return Ok(());
        }

        info!(
            event_id = event.event_id,
            project_id = project_id,
            branch = %branch,
            "starting code indexing"
        );

        self.index_with_lock(context, event, project_id, &branch, &project)
            .await
    }

    async fn index_with_lock(
        &self,
        context: &HandlerContext,
        event: &PushEventPayload,
        project_id: i64,
        branch: &str,
        project: &ProjectInfo,
    ) -> Result<(), HandlerError> {
        if !self.try_acquire_lock(context, project_id, branch).await? {
            debug!(
                event_id = event.event_id,
                project_id = project_id,
                branch = %branch,
                "lock held by another indexer, skipping"
            );
            return Ok(());
        }

        let result = self
            .run_indexing(
                context,
                project_id,
                branch,
                &event.revision_after,
                &project.traversal_path,
            )
            .await;

        if let Err(e) = self.release_lock(context, project_id, branch).await {
            warn!(project_id, branch = %branch, error = %e, "failed to release lock");
        }

        self.finalize_indexing(event, project_id, branch, result)
            .await
    }

    async fn run_indexing(
        &self,
        context: &HandlerContext,
        project_id: i64,
        branch: &str,
        commit_id: &str,
        traversal_path: &str,
    ) -> Result<(), HandlerError> {
        let temp_dir = TempDir::new()
            .map_err(|e| HandlerError::Processing(format!("failed to create temp dir: {e}")))?;

        self.repository_service
            .extract_repository(project_id, temp_dir.path(), commit_id)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to extract repository: {e}")))?;

        let repo_path = temp_dir.path().to_string_lossy().to_string();
        let indexer = RepositoryIndexer::new(format!("project-{project_id}"), repo_path.clone());
        let file_source = DirectoryFileSource::new(repo_path);

        let result = indexer
            .index_files(file_source, &IndexingConfig::default())
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to index code: {e}")))?;

        if let Some(mut graph_data) = result.graph_data {
            // TODO: This should be done on construction of the GraphData struct.
            graph_data.assign_node_ids(project_id, branch);
            self.write_graph_data(context, project_id, branch, traversal_path, &graph_data)
                .await?;
        }

        Ok(())
    }

    async fn finalize_indexing(
        &self,
        event: &PushEventPayload,
        project_id: i64,
        branch: &str,
        result: Result<(), HandlerError>,
    ) -> Result<(), HandlerError> {
        if let Err(e) = &result {
            warn!(project_id, branch = %branch, error = %e, "failed to index code");
            return result;
        }

        let watermark = CodeIndexingWatermark {
            project_id,
            branch: branch.to_string(),
            last_event_id: event.event_id,
            last_commit: event.revision_after.clone(),
            indexed_at: Utc::now(),
        };

        self.watermark_store
            .set_watermark(&watermark)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to set watermark: {e}")))?;

        info!(
            project_id,
            branch = %branch,
            commit = %event.revision_after,
            "successfully indexed code"
        );

        Ok(())
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

    // TODO: Look into what load this creates on Gitaly, we should probably cache it.
    async fn is_default_branch(&self, project_id: i64, branch: &str) -> bool {
        match self
            .repository_service
            .find_default_branch(project_id)
            .await
        {
            Ok(Some(default_branch)) => {
                let default_branch_name = default_branch
                    .strip_prefix("refs/heads/")
                    .unwrap_or(&default_branch);
                branch == default_branch_name
            }
            Ok(None) => {
                debug!(project_id, "repository has no default branch");
                false
            }
            Err(e) => {
                warn!(project_id, error = %e, "failed to fetch default branch");
                false
            }
        }
    }
}

impl PushEventHandler {
    async fn resolve_project_id(
        &self,
        ctx: &HandlerContext,
        event: &PushEventPayload,
    ) -> Result<Option<i64>, HandlerError> {
        if let Some(id) = event.project_id {
            return Ok(Some(id));
        }

        let key = event.event_id.to_string();
        match ctx.nats.kv_get(buckets::EVENTS_CACHE, &key).await {
            Ok(Some(entry)) => {
                let info: CachedEventInfo = serde_json::from_slice(&entry.value)
                    .map_err(|e| HandlerError::Processing(format!("failed to parse cache: {e}")))?;
                Ok(Some(info.project_id))
            }
            Ok(None) => {
                warn!(
                    event_id = event.event_id,
                    "project_id not in payload or cache"
                );
                Ok(None)
            }
            Err(e) => Err(HandlerError::Processing(format!(
                "cache lookup failed: {e}"
            ))),
        }
    }

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
        project_id: i64,
        branch: &str,
    ) -> bool {
        if let Ok(Some(wm)) = self.watermark_store.get_watermark(project_id, branch).await
            && wm.last_event_id >= event.event_id
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
            .try_acquire(&key, LOCK_TTL)
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

impl PushEventHandler {
    async fn write_graph_data(
        &self,
        ctx: &HandlerContext,
        project_id: i64,
        branch: &str,
        traversal_path: &str,
        graph_data: &code_graph::analysis::types::GraphData,
    ) -> Result<(), HandlerError> {
        let converter =
            ArrowConverter::new(traversal_path.to_string(), project_id, branch.to_string());

        let converted = converter
            .convert_all(graph_data)
            .map_err(|e| HandlerError::Processing(format!("arrow conversion failed: {e}")))?;

        self.write_batch(ctx, tables::GL_DIRECTORY, &converted.directories)
            .await?;
        self.write_batch(ctx, tables::GL_FILE, &converted.files)
            .await?;
        self.write_batch(ctx, tables::GL_DEFINITION, &converted.definitions)
            .await?;
        self.write_batch(ctx, tables::GL_IMPORTED_SYMBOL, &converted.imported_symbols)
            .await?;
        self.write_batch(ctx, EDGE_TABLE, &converted.edges).await?;

        Ok(())
    }

    async fn write_batch(
        &self,
        ctx: &HandlerContext,
        table: &str,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<(), HandlerError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let writer = ctx
            .destination
            .new_batch_writer(table)
            .await
            .map_err(|e| HandlerError::Processing(format!("writer creation failed: {e}")))?;

        writer
            .write_batch(std::slice::from_ref(batch))
            .await
            .map_err(|e| HandlerError::Processing(format!("write to {table} failed: {e}")))
    }
}

#[derive(Debug, Clone)]
struct PushEventPayload {
    event_id: i64,
    project_id: Option<i64>,
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
            project_id: extractor.get_i64(event, "project_id"),
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
    use super::*;
    use crate::module::Handler;
    use crate::modules::code::event_cache_handler::CachedEventInfo;
    use crate::modules::code::gitaly::test_utils::MockRepositoryService;
    use crate::modules::code::project_store::ProjectInfo;
    use crate::modules::code::project_store::test_utils::MockProjectStore;
    use crate::modules::code::test_helpers::{build_replication_events, push_payload_columns};
    use crate::modules::code::watermark_store::test_utils::MockCodeWatermarkStore;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use bytes::Bytes;

    struct TestContext {
        handler: PushEventHandler,
        mock_nats: Arc<MockNatsServices>,
        mock_locks: Arc<MockLockService>,
        watermark_store: Arc<MockCodeWatermarkStore>,
        project_store: Arc<MockProjectStore>,
    }

    impl TestContext {
        fn new() -> Self {
            Self::with_repository_service(MockRepositoryService::create())
        }

        fn with_repository_service(repository_service: Arc<dyn RepositoryService>) -> Self {
            let mock_nats = Arc::new(MockNatsServices::new());
            let mock_locks = Arc::new(MockLockService::new());
            let watermark_store = Arc::new(MockCodeWatermarkStore::new());
            let project_store = Arc::new(MockProjectStore::new());

            let handler = PushEventHandler::new(
                repository_service,
                watermark_store.clone(),
                project_store.clone(),
                CodeIndexingConfig::default(),
            );

            Self {
                handler,
                mock_nats,
                mock_locks,
                watermark_store,
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

        fn cache_event(&self, event_id: i64, project_id: i64) {
            let info = CachedEventInfo {
                project_id,
                author_id: 1,
                created_at: "2024-01-01T00:00:00Z".to_string(),
            };
            let bytes = Bytes::from(serde_json::to_vec(&info).unwrap());
            self.mock_nats
                .set_kv(buckets::EVENTS_CACHE, &event_id.to_string(), bytes);
        }

        async fn set_watermark(&self, project_id: i64, branch: &str, last_event_id: i64) {
            self.watermark_store
                .set_watermark(&CodeIndexingWatermark {
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
            push_payload_columns(1, Some(123), "refs/heads/feature/new-thing", "abc123").build(),
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
        ctx.set_watermark(123, "main", 100).await;

        let payload = build_replication_events(vec![
            push_payload_columns(50, Some(123), "refs/heads/main", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
        assert!(!ctx.lock_exists(123, "main"));
    }

    #[tokio::test]
    async fn does_not_acquire_lock_when_project_id_missing_and_not_cached() {
        let ctx = TestContext::new();

        let payload = build_replication_events(vec![
            push_payload_columns(100, None, "refs/heads/main", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        ctx.handler
            .handle(ctx.handler_context(), envelope)
            .await
            .unwrap();

        assert!(!ctx.lock_exists(100, "main"));
    }

    #[tokio::test]
    async fn resolves_project_id_from_cache_but_skips_when_not_default_branch() {
        let mock_repo = MockRepositoryService::with_default_branch(456, "refs/heads/main");
        let ctx = TestContext::with_repository_service(mock_repo);
        ctx.cache_event(100, 456);

        let payload = build_replication_events(vec![
            push_payload_columns(100, None, "refs/heads/develop", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        ctx.handler
            .handle(ctx.handler_context(), envelope)
            .await
            .unwrap();

        assert!(!ctx.lock_exists(456, "develop"));
    }

    #[tokio::test]
    async fn does_not_acquire_lock_when_project_not_in_knowledge_graph() {
        let mock_repo = MockRepositoryService::with_default_branch(999, "refs/heads/main");
        let ctx = TestContext::with_repository_service(mock_repo);

        let payload = build_replication_events(vec![
            push_payload_columns(100, Some(999), "refs/heads/main", "abc123").build(),
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
            push_payload_columns(100, Some(123), "refs/heads/main", "abc123").build(),
        ]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        let result = ctx.handler.handle(ctx.handler_context(), envelope).await;

        assert!(result.is_ok());
    }
}
