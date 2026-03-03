//! Handler for namespace-scoped entities.
//!
//! Processes entities with `EtlScope::Namespaced` using ontology-driven pipelines.

use std::sync::Arc;
use std::time::Instant;

use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Event, SerializationError, Topic};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use super::cursor_paginator::{CursorValue, serialize_cursor};
use super::locking::namespace_lock_key;
use super::metrics::SdlcMetrics;
use super::pipeline::{OntologyEdgePipeline, OntologyEntityPipeline};
use super::watermark_store::{
    CursorReporter, InProgressCursor, WatermarkError, WatermarkState, WatermarkStore,
};
use crate::clickhouse::TIMESTAMP_FORMAT;
use crate::topic::NamespaceIndexingRequest;

#[derive(Clone, Serialize)]
pub struct NamespaceQueryParams {
    pub traversal_path: String,
    pub last_watermark: String,
    pub watermark: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub __starting_cursor: Option<String>,
}

impl NamespaceQueryParams {
    pub fn new(
        organization: i64,
        namespace: i64,
        last_watermark: &DateTime<Utc>,
        watermark: &DateTime<Utc>,
    ) -> Self {
        Self {
            traversal_path: format!("{organization}/{namespace}/"),
            last_watermark: last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            watermark: watermark.format(TIMESTAMP_FORMAT).to_string(),
            __starting_cursor: None,
        }
    }

    pub fn with_starting_cursor(mut self, cursor_values: &[CursorValue]) -> Self {
        if !cursor_values.is_empty() {
            self.__starting_cursor = Some(serialize_cursor(cursor_values));
        }
        self
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("params serialization failed")
    }
}

fn default_datalake_batch_size() -> u64 {
    1_000_000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NamespaceHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_datalake_batch_size")]
    pub datalake_batch_size: u64,
}

impl Default for NamespaceHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            datalake_batch_size: default_datalake_batch_size(),
        }
    }
}

/// Cursor lifecycle context for a single pipeline run.
struct PipelineRun {
    params: NamespaceQueryParams,
    target_watermark: DateTime<Utc>,
    previous_watermark: DateTime<Utc>,
    reporter: NamespaceCursorReporter,
}

/// Handles entities owned by a namespace.
pub struct NamespaceHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    pipelines: Vec<OntologyEntityPipeline>,
    edge_pipelines: Vec<OntologyEdgePipeline>,
    metrics: SdlcMetrics,
    config: NamespaceHandlerConfig,
}

impl NamespaceHandler {
    pub fn new(
        watermark_store: Arc<dyn WatermarkStore>,
        pipelines: Vec<OntologyEntityPipeline>,
        edge_pipelines: Vec<OntologyEdgePipeline>,
        metrics: SdlcMetrics,
        config: NamespaceHandlerConfig,
    ) -> Self {
        Self {
            watermark_store,
            pipelines,
            edge_pipelines,
            metrics,
            config,
        }
    }

    async fn resolve_namespace_state(
        &self,
        namespace_id: i64,
        entity: &str,
    ) -> WatermarkState {
        match self
            .watermark_store
            .get_namespace_state(namespace_id, entity)
            .await
        {
            Ok(state) => {
                debug!(
                    namespace_id,
                    entity,
                    watermark = %state.watermark.format(TIMESTAMP_FORMAT),
                    has_cursor = state.in_progress.is_some(),
                    "retrieved namespace entity state"
                );
                state
            }
            Err(WatermarkError::NoData) => {
                info!(namespace_id, entity, "no namespace entity watermark found, starting from epoch");
                WatermarkState { watermark: DateTime::<Utc>::UNIX_EPOCH, in_progress: None }
            }
            Err(error) => {
                error!(namespace_id, entity, %error, "watermark state fetch failed, reprocessing from epoch");
                WatermarkState { watermark: DateTime::<Utc>::UNIX_EPOCH, in_progress: None }
            }
        }
    }

    /// Prepares cursor lifecycle for a pipeline: resolves state, saves initial cursor, builds params.
    async fn prepare_pipeline_run(
        &self,
        entity: &str,
        namespace_id: i64,
        organization: i64,
        new_watermark: &DateTime<Utc>,
    ) -> PipelineRun {
        let state = self.resolve_namespace_state(namespace_id, entity).await;

        let (params, target_watermark) = match &state.in_progress {
            Some(cursor) => (
                NamespaceQueryParams::new(organization, namespace_id, &state.watermark, &cursor.upper_watermark)
                    .with_starting_cursor(&cursor.cursor_values),
                cursor.upper_watermark,
            ),
            None => (
                NamespaceQueryParams::new(organization, namespace_id, &state.watermark, new_watermark),
                *new_watermark,
            ),
        };

        if state.in_progress.is_none() {
            let initial_cursor = InProgressCursor { cursor_values: vec![], upper_watermark: target_watermark };
            if let Err(error) = self.watermark_store.save_namespace_cursor(namespace_id, entity, &initial_cursor).await {
                error!(namespace_id, entity, %error, "failed to save initial cursor");
            }
        }

        let reporter = NamespaceCursorReporter {
            watermark_store: Arc::clone(&self.watermark_store),
            namespace_id,
            entity: entity.to_string(),
            upper_watermark: target_watermark,
        };

        PipelineRun { params, target_watermark, previous_watermark: state.watermark, reporter }
    }

    /// Finalizes cursor lifecycle after pipeline execution: advances or clears watermark.
    async fn finalize_pipeline_run(
        &self,
        entity: &str,
        namespace_id: i64,
        run: &PipelineRun,
        rows_indexed: u64,
    ) -> Result<(), HandlerError> {
        if rows_indexed > 0 {
            self.watermark_store
                .complete_namespace_watermark(namespace_id, entity, &run.target_watermark)
                .await
                .map_err(|error| {
                    error!(namespace_id, entity, %error, "failed to complete namespace watermark");
                    HandlerError::Processing(format!(
                        "failed to complete namespace watermark for {entity}: {error}"
                    ))
                })?;
            self.metrics.record_watermark_lag(entity, &run.target_watermark);
        } else if let Err(error) = self
            .watermark_store
            .complete_namespace_watermark(namespace_id, entity, &run.previous_watermark)
            .await
        {
            error!(namespace_id, entity, %error, "failed to clear cursor");
        }
        Ok(())
    }
}

#[async_trait]
impl Handler for NamespaceHandler {
    fn name(&self) -> &str {
        "namespace_handler"
    }

    fn topic(&self) -> Topic {
        NamespaceIndexingRequest::topic()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: NamespaceIndexingRequest =
            message.to_event().map_err(|error| match error {
                SerializationError::Json(e) => HandlerError::Deserialization(e),
            })?;

        let started_at = Instant::now();

        info!(
            namespace_id = payload.namespace,
            organization_id = payload.organization,
            entity_pipeline_count = self.pipelines.len(),
            edge_pipeline_count = self.edge_pipelines.len(),
            "starting namespace indexing"
        );

        let mut errors = Vec::new();
        let mut successful_entity_pipelines = 0;
        let mut successful_edge_pipelines = 0;

        for pipeline in &self.pipelines {
            let entity = pipeline.entity_name();
            let run = self.prepare_pipeline_run(entity, payload.namespace, payload.organization, &payload.watermark).await;

            match pipeline.process(run.params.to_json(), context.destination.as_ref(), &run.reporter).await {
                Ok(rows) => {
                    self.finalize_pipeline_run(entity, payload.namespace, &run, rows).await?;
                    successful_entity_pipelines += 1;
                }
                Err(error) => {
                    error!(namespace_id = payload.namespace, entity, %error, "entity pipeline failed");
                    self.metrics.record_pipeline_error(entity, error.error_kind());
                    errors.push((entity.to_string(), error));
                }
            }
        }

        for edge_pipeline in &self.edge_pipelines {
            let entity = edge_pipeline.relationship_kind();
            let run = self.prepare_pipeline_run(entity, payload.namespace, payload.organization, &payload.watermark).await;

            match edge_pipeline.process(run.params.to_json(), context.destination.as_ref(), &run.reporter).await {
                Ok(rows) => {
                    self.finalize_pipeline_run(entity, payload.namespace, &run, rows).await?;
                    successful_edge_pipelines += 1;
                }
                Err(error) => {
                    error!(namespace_id = payload.namespace, edge = entity, %error, "edge pipeline failed");
                    self.metrics.record_pipeline_error(entity, error.error_kind());
                    errors.push((entity.to_string(), error));
                }
            }
        }

        let elapsed = started_at.elapsed();

        if errors.is_empty() {
            let lock_key = namespace_lock_key(payload.organization, payload.namespace);
            if let Err(error) = context.lock_service.release(&lock_key).await {
                error!(namespace_id = payload.namespace, %error, "failed to release namespace lock, will expire via TTL");
            }
            info!(
                namespace_id = payload.namespace,
                organization_id = payload.organization,
                successful_entity_pipelines,
                successful_edge_pipelines,
                elapsed_ms = elapsed.as_millis() as u64,
                "namespace indexing completed"
            );
        }

        self.metrics.record_handler_duration("namespace_handler", elapsed.as_secs_f64());

        if !errors.is_empty() {
            let failed_count = errors.len();
            error!(
                namespace_id = payload.namespace,
                failed_count,
                successful_entity_pipelines,
                successful_edge_pipelines,
                elapsed_ms = elapsed.as_millis() as u64,
                "namespace indexing finished with failures"
            );
            let error_details: Vec<_> = errors.iter().map(|(name, err)| format!("{name}: {err}")).collect();
            return Err(HandlerError::Processing(format!("entity pipelines failed: {}", error_details.join("; "))));
        }

        Ok(())
    }
}

struct NamespaceCursorReporter {
    watermark_store: Arc<dyn WatermarkStore>,
    namespace_id: i64,
    entity: String,
    upper_watermark: DateTime<Utc>,
}

#[async_trait]
impl CursorReporter for NamespaceCursorReporter {
    async fn on_page_complete(&self, cursor_values: &[CursorValue]) -> Result<(), HandlerError> {
        let cursor = InProgressCursor {
            cursor_values: cursor_values.to_vec(),
            upper_watermark: self.upper_watermark,
        };
        self.watermark_store
            .save_namespace_cursor(self.namespace_id, &self.entity, &cursor)
            .await
            .map_err(|error| {
                HandlerError::Processing(format!("failed to save cursor for {}: {error}", self.entity))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::datalake::DatalakeQuery;
    use crate::modules::sdlc::test_fixtures::{EmptyDatalake, FailingDatalake, NonEmptyDatalake};
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use ontology::{DataType, EtlConfig, EtlScope, Field, NodeEntity, Ontology};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    fn test_metrics() -> SdlcMetrics {
        SdlcMetrics::with_meter(&crate::testkit::test_meter())
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct WatermarkKey { namespace_id: i64, entity: String }

    struct RecordingWatermarkStore {
        watermarks: Mutex<HashMap<WatermarkKey, DateTime<Utc>>>,
        get_behavior: Mutex<HashMap<WatermarkKey, Result<DateTime<Utc>, WatermarkError>>>,
        set_behavior: Mutex<HashMap<WatermarkKey, Result<(), WatermarkError>>>,
        states: Mutex<HashMap<WatermarkKey, WatermarkState>>,
        saved_cursors: Mutex<HashMap<WatermarkKey, InProgressCursor>>,
    }

    impl RecordingWatermarkStore {
        fn new() -> Self {
            Self {
                watermarks: Mutex::new(HashMap::new()),
                get_behavior: Mutex::new(HashMap::new()),
                set_behavior: Mutex::new(HashMap::new()),
                states: Mutex::new(HashMap::new()),
                saved_cursors: Mutex::new(HashMap::new()),
            }
        }

        fn with_watermark(self, namespace_id: i64, entity: &str, watermark: DateTime<Utc>) -> Self {
            let key = WatermarkKey { namespace_id, entity: entity.to_string() };
            self.get_behavior.lock().unwrap().insert(key.clone(), Ok(watermark));
            self.states.lock().unwrap().insert(key, WatermarkState { watermark, in_progress: None });
            self
        }

        fn with_set_failure(self, namespace_id: i64, entity: &str) -> Self {
            let key = WatermarkKey { namespace_id, entity: entity.to_string() };
            self.set_behavior.lock().unwrap().insert(key, Err(WatermarkError::Query("write failed".to_string())));
            self
        }

        fn stored_watermarks(&self) -> HashMap<WatermarkKey, DateTime<Utc>> {
            self.watermarks.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WatermarkStore for RecordingWatermarkStore {
        async fn get_namespace_state(&self, namespace_id: i64, entity: &str) -> Result<WatermarkState, WatermarkError> {
            let key = WatermarkKey { namespace_id, entity: entity.to_string() };
            self.states.lock().unwrap().get(&key).cloned().ok_or(WatermarkError::NoData)
        }
        async fn save_namespace_cursor(&self, namespace_id: i64, entity: &str, cursor: &InProgressCursor) -> Result<(), WatermarkError> {
            let key = WatermarkKey { namespace_id, entity: entity.to_string() };
            self.saved_cursors.lock().unwrap().insert(key, cursor.clone());
            Ok(())
        }
        async fn complete_namespace_watermark(&self, namespace_id: i64, entity: &str, watermark: &DateTime<Utc>) -> Result<(), WatermarkError> {
            let key = WatermarkKey { namespace_id, entity: entity.to_string() };
            if let Some(Err(e)) = self.set_behavior.lock().unwrap().get(&key) {
                return Err(WatermarkError::Query(e.to_string()));
            }
            self.watermarks.lock().unwrap().insert(key.clone(), *watermark);
            self.states.lock().unwrap().insert(key.clone(), WatermarkState { watermark: *watermark, in_progress: None });
            self.saved_cursors.lock().unwrap().remove(&key);
            Ok(())
        }
        async fn get_global_state(&self) -> Result<WatermarkState, WatermarkError> {
            Ok(WatermarkState { watermark: DateTime::<Utc>::UNIX_EPOCH, in_progress: None })
        }
        async fn save_global_cursor(&self, _: &InProgressCursor) -> Result<(), WatermarkError> { Ok(()) }
        async fn complete_global_watermark(&self, _: &DateTime<Utc>) -> Result<(), WatermarkError> { Ok(()) }
    }

    fn create_test_node(name: &str, source_table: &str) -> NodeEntity {
        NodeEntity {
            name: name.to_string(),
            fields: vec![Field {
                name: "id".to_string(), source: "id".to_string(),
                data_type: DataType::Int, nullable: false,
                enum_values: None, enum_type: ontology::EnumType::default(),
            }],
            destination_table: format!("{}{}", ontology::constants::GL_TABLE_PREFIX, name.to_lowercase()),
            etl: Some(EtlConfig::Query {
                scope: EtlScope::Namespaced,
                query: format!("SELECT id, _deleted, _version FROM {source_table} WHERE traversal_path LIKE {{traversal_path:String}}"),
                edges: BTreeMap::new(),
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn handle_processes_pipelines() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let pipelines = vec![
            OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, datalake.clone(), test_metrics()).unwrap(),
            OntologyEntityPipeline::from_node(&create_test_node("Issue", "issues"), &ontology, datalake, test_metrics()).unwrap(),
        ];
        let handler = NamespaceHandler::new(Arc::new(RecordingWatermarkStore::new()), pipelines, vec![], test_metrics(), NamespaceHandlerConfig::default());
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 2, "watermark": "2024-01-21T00:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), Arc::new(MockLockService::new()));
        assert!(handler.handle(context, envelope).await.is_ok());
    }

    #[tokio::test]
    async fn handler_releases_lock_on_success() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let pipelines = vec![OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, datalake, test_metrics()).unwrap()];
        let handler = NamespaceHandler::new(Arc::new(RecordingWatermarkStore::new()), pipelines, vec![], test_metrics(), NamespaceHandlerConfig::default());
        let mock_locks = Arc::new(MockLockService::new());
        let lock_key = namespace_lock_key(1, 42);
        mock_locks.set_lock(&lock_key);
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 42, "watermark": "2024-01-21T00:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), mock_locks.clone());
        assert!(handler.handle(context, envelope).await.is_ok());
        assert!(!mock_locks.is_held(&lock_key));
    }

    #[tokio::test]
    async fn watermark_not_updated_when_no_rows_indexed() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let pipelines = vec![
            OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, datalake.clone(), test_metrics()).unwrap(),
            OntologyEntityPipeline::from_node(&create_test_node("Issue", "issues"), &ontology, datalake, test_metrics()).unwrap(),
        ];
        let store = Arc::new(RecordingWatermarkStore::new());
        let handler = NamespaceHandler::new(store.clone(), pipelines, vec![], test_metrics(), NamespaceHandlerConfig::default());
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 100, "watermark": "2024-06-15T12:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), Arc::new(MockLockService::new()));
        handler.handle(context, envelope).await.unwrap();
        for wm in store.stored_watermarks().values() {
            assert_eq!(*wm, DateTime::<Utc>::UNIX_EPOCH);
        }
    }

    #[tokio::test]
    async fn watermark_updated_per_entity_on_success() {
        let datalake = Arc::new(NonEmptyDatalake);
        let ontology = Ontology::new();
        let pipelines = vec![
            OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, datalake.clone(), test_metrics()).unwrap(),
            OntologyEntityPipeline::from_node(&create_test_node("Issue", "issues"), &ontology, datalake, test_metrics()).unwrap(),
        ];
        let store = Arc::new(RecordingWatermarkStore::new());
        let handler = NamespaceHandler::new(store.clone(), pipelines, vec![], test_metrics(), NamespaceHandlerConfig::default());
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 100, "watermark": "2024-06-15T12:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), Arc::new(MockLockService::new()));
        handler.handle(context, envelope).await.unwrap();
        let stored = store.stored_watermarks();
        let expected = "2024-06-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(stored.get(&WatermarkKey { namespace_id: 100, entity: "Group".into() }), Some(&expected));
        assert_eq!(stored.get(&WatermarkKey { namespace_id: 100, entity: "Issue".into() }), Some(&expected));
    }

    #[tokio::test]
    async fn failed_pipeline_does_not_update_its_watermark() {
        let ontology = Ontology::new();
        let group_pipeline = OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, Arc::new(EmptyDatalake), test_metrics()).unwrap();
        let issue_pipeline = OntologyEntityPipeline::from_node(&create_test_node("Issue", "issues"), &ontology, Arc::new(FailingDatalake) as Arc<dyn DatalakeQuery>, test_metrics()).unwrap();
        let store = Arc::new(RecordingWatermarkStore::new());
        let handler = NamespaceHandler::new(store.clone(), vec![group_pipeline, issue_pipeline], vec![], test_metrics(), NamespaceHandlerConfig::default());
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 200, "watermark": "2024-06-15T12:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), Arc::new(MockLockService::new()));
        assert!(handler.handle(context, envelope).await.is_err());
        assert!(!store.stored_watermarks().contains_key(&WatermarkKey { namespace_id: 200, entity: "Issue".into() }));
    }

    #[tokio::test]
    async fn processing_continues_after_earlier_pipeline_fails() {
        let ontology = Ontology::new();
        let group_pipeline = OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, Arc::new(FailingDatalake) as Arc<dyn DatalakeQuery>, test_metrics()).unwrap();
        let issue_pipeline = OntologyEntityPipeline::from_node(&create_test_node("Issue", "issues"), &ontology, Arc::new(EmptyDatalake), test_metrics()).unwrap();
        let store = Arc::new(RecordingWatermarkStore::new());
        let handler = NamespaceHandler::new(store.clone(), vec![group_pipeline, issue_pipeline], vec![], test_metrics(), NamespaceHandlerConfig::default());
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 300, "watermark": "2024-06-15T12:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), Arc::new(MockLockService::new()));
        assert!(handler.handle(context, envelope).await.is_err());
        assert!(!store.stored_watermarks().contains_key(&WatermarkKey { namespace_id: 300, entity: "Group".into() }));
    }

    #[tokio::test]
    async fn each_entity_resolves_its_own_watermark() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let group_watermark = "2024-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let issue_watermark = "2024-05-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let pipelines = vec![
            OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, datalake.clone(), test_metrics()).unwrap(),
            OntologyEntityPipeline::from_node(&create_test_node("Issue", "issues"), &ontology, datalake, test_metrics()).unwrap(),
        ];
        let store = Arc::new(RecordingWatermarkStore::new().with_watermark(100, "Group", group_watermark).with_watermark(100, "Issue", issue_watermark));
        let handler = NamespaceHandler::new(store.clone(), pipelines, vec![], test_metrics(), NamespaceHandlerConfig::default());
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 100, "watermark": "2024-06-15T12:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), Arc::new(MockLockService::new()));
        handler.handle(context, envelope).await.unwrap();
        let stored = store.stored_watermarks();
        assert_eq!(stored.get(&WatermarkKey { namespace_id: 100, entity: "Group".into() }), Some(&group_watermark));
        assert_eq!(stored.get(&WatermarkKey { namespace_id: 100, entity: "Issue".into() }), Some(&issue_watermark));
    }

    #[tokio::test]
    async fn zero_rows_skips_watermark_set_even_if_store_would_fail() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let pipelines = vec![OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, datalake, test_metrics()).unwrap()];
        let store = Arc::new(RecordingWatermarkStore::new().with_set_failure(100, "Group"));
        let handler = NamespaceHandler::new(store, pipelines, vec![], test_metrics(), NamespaceHandlerConfig::default());
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 100, "watermark": "2024-06-15T12:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), Arc::new(MockLockService::new()));
        assert!(handler.handle(context, envelope).await.is_ok(), "should succeed because cursor clear is best-effort");
    }

    #[tokio::test]
    async fn lock_not_released_when_pipeline_fails() {
        let ontology = Ontology::new();
        let pipelines = vec![OntologyEntityPipeline::from_node(&create_test_node("Group", "groups"), &ontology, Arc::new(FailingDatalake) as Arc<dyn DatalakeQuery>, test_metrics()).unwrap()];
        let handler = NamespaceHandler::new(Arc::new(RecordingWatermarkStore::new()), pipelines, vec![], test_metrics(), NamespaceHandlerConfig::default());
        let mock_locks = Arc::new(MockLockService::new());
        let lock_key = namespace_lock_key(1, 42);
        mock_locks.set_lock(&lock_key);
        let envelope = TestEnvelopeFactory::simple(&serde_json::json!({"organization": 1, "namespace": 42, "watermark": "2024-01-21T00:00:00Z"}).to_string());
        let context = HandlerContext::new(Arc::new(MockDestination::new()), Arc::new(MockNatsServices::new()), mock_locks.clone());
        assert!(handler.handle(context, envelope).await.is_err());
        assert!(mock_locks.is_held(&lock_key));
    }
}
