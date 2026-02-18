//! Handler for namespace-scoped entities.
//!
//! Processes entities with `EtlScope::Namespaced` using ontology-driven pipelines.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Event, SerializationError, Topic};
use serde::Serialize;
use tracing::{debug, info, warn};

use super::locking::{INDEXING_LOCKS_BUCKET, namespace_lock_key};
use super::pipeline::{OntologyEdgePipeline, OntologyEntityPipeline};
use super::watermark_store::{TIMESTAMP_FORMAT, WatermarkError, WatermarkStore};
use crate::indexer::topic::NamespaceIndexingRequest;

#[derive(Clone, Serialize)]
pub struct NamespaceQueryParams {
    pub traversal_path: String,
    pub last_watermark: String,
    pub watermark: String,
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
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("params serialization failed")
    }
}

/// Handles entities owned by a namespace.
///
/// These are records like Project, Issue, or Pipeline that live under a namespace
/// path (e.g., "123/456/"). Queries filter by traversal_path prefix and watermark.
pub struct NamespaceHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    pipelines: Vec<OntologyEntityPipeline>,
    edge_pipelines: Vec<OntologyEdgePipeline>,
}

impl NamespaceHandler {
    pub fn new(
        watermark_store: Arc<dyn WatermarkStore>,
        pipelines: Vec<OntologyEntityPipeline>,
        edge_pipelines: Vec<OntologyEdgePipeline>,
    ) -> Self {
        Self {
            watermark_store,
            pipelines,
            edge_pipelines,
        }
    }

    async fn resolve_namespace_watermark(&self, namespace_id: i64, entity: &str) -> DateTime<Utc> {
        match self
            .watermark_store
            .get_namespace_watermark(namespace_id, entity)
            .await
        {
            Ok(watermark) => {
                debug!(
                    namespace_id,
                    entity,
                    watermark = %watermark.format(TIMESTAMP_FORMAT),
                    "retrieved namespace entity watermark"
                );
                watermark
            }
            Err(WatermarkError::NoData) => {
                debug!(
                    namespace_id,
                    entity, "no namespace entity watermark found, starting from epoch"
                );
                DateTime::<Utc>::UNIX_EPOCH
            }
            Err(error) => {
                warn!(
                    namespace_id,
                    entity,
                    %error,
                    "failed to fetch namespace entity watermark, using epoch"
                );
                DateTime::<Utc>::UNIX_EPOCH
            }
        }
    }
}

#[async_trait]
impl Handler for NamespaceHandler {
    fn name(&self) -> &str {
        "namespace-handler"
    }

    fn topic(&self) -> Topic {
        NamespaceIndexingRequest::topic()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: NamespaceIndexingRequest =
            message.to_event().map_err(|error| match error {
                SerializationError::Json(e) => HandlerError::Deserialization(e),
            })?;

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
            let last_watermark = self
                .resolve_namespace_watermark(payload.namespace, entity)
                .await;

            let params = NamespaceQueryParams::new(
                payload.organization,
                payload.namespace,
                &last_watermark,
                &payload.watermark,
            );

            debug!(
                namespace_id = payload.namespace,
                entity,
                from_watermark = %params.last_watermark,
                to_watermark = %params.watermark,
                "processing namespace entity pipeline"
            );
            if let Err(error) = pipeline
                .process(params.to_json(), context.destination.as_ref())
                .await
            {
                warn!(
                    namespace_id = payload.namespace,
                    entity,
                    %error,
                    "pipeline processing failed"
                );
                errors.push((entity.to_string(), error));
            } else {
                self.watermark_store
                    .set_namespace_watermark(payload.namespace, entity, &payload.watermark)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!(
                            "failed to update namespace watermark for {entity}: {e}"
                        ))
                    })?;

                debug!(
                    namespace_id = payload.namespace,
                    entity,
                    watermark = %payload.watermark.format(TIMESTAMP_FORMAT),
                    "namespace entity pipeline completed, watermark updated"
                );
                successful_entity_pipelines += 1;
            }
        }

        for edge_pipeline in &self.edge_pipelines {
            let entity = edge_pipeline.relationship_kind();
            let last_watermark = self
                .resolve_namespace_watermark(payload.namespace, entity)
                .await;

            let params = NamespaceQueryParams::new(
                payload.organization,
                payload.namespace,
                &last_watermark,
                &payload.watermark,
            );

            debug!(
                namespace_id = payload.namespace,
                edge = entity,
                from_watermark = %params.last_watermark,
                to_watermark = %params.watermark,
                "processing namespace edge pipeline"
            );
            if let Err(error) = edge_pipeline
                .process(params.to_json(), context.destination.as_ref())
                .await
            {
                warn!(
                    namespace_id = payload.namespace,
                    edge = entity,
                    %error,
                    "edge pipeline processing failed"
                );
                errors.push((entity.to_string(), error));
            } else {
                self.watermark_store
                    .set_namespace_watermark(payload.namespace, entity, &payload.watermark)
                    .await
                    .map_err(|e| {
                        HandlerError::Processing(format!(
                            "failed to update namespace watermark for {entity}: {e}"
                        ))
                    })?;

                debug!(
                    namespace_id = payload.namespace,
                    edge = entity,
                    watermark = %payload.watermark.format(TIMESTAMP_FORMAT),
                    "namespace edge pipeline completed, watermark updated"
                );
                successful_edge_pipelines += 1;
            }
        }

        if errors.is_empty() {
            let lock_key = namespace_lock_key(payload.namespace);
            if let Err(error) = context
                .nats
                .kv_delete(INDEXING_LOCKS_BUCKET, &lock_key)
                .await
            {
                warn!(
                    namespace_id = payload.namespace,
                    %error,
                    "failed to release namespace lock, will expire via TTL"
                );
            } else {
                debug!(
                    namespace_id = payload.namespace,
                    "namespace indexing lock released"
                );
            }

            info!(
                namespace_id = payload.namespace,
                organization_id = payload.organization,
                successful_entity_pipelines,
                successful_edge_pipelines,
                "namespace indexing completed"
            );
        }

        if !errors.is_empty() {
            let error_details: Vec<_> = errors
                .iter()
                .map(|(name, err)| format!("{name}: {err}"))
                .collect();
            return Err(HandlerError::Processing(format!(
                "entity pipelines failed: {}",
                error_details.join("; ")
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::modules::sdlc::datalake::{
        DatalakeError, DatalakeQuery, RecordBatchStream,
    };
    use etl_engine::testkit::{MockDestination, MockNatsServices, TestEnvelopeFactory};
    use futures::stream;
    use ontology::{DataType, EtlConfig, EtlScope, Field, NodeEntity, Ontology};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct WatermarkKey {
        namespace_id: i64,
        entity: String,
    }

    struct RecordingWatermarkStore {
        watermarks: Mutex<HashMap<WatermarkKey, DateTime<Utc>>>,
        get_behavior: Mutex<HashMap<WatermarkKey, Result<DateTime<Utc>, WatermarkError>>>,
        set_behavior: Mutex<HashMap<WatermarkKey, Result<(), WatermarkError>>>,
    }

    impl RecordingWatermarkStore {
        fn new() -> Self {
            Self {
                watermarks: Mutex::new(HashMap::new()),
                get_behavior: Mutex::new(HashMap::new()),
                set_behavior: Mutex::new(HashMap::new()),
            }
        }

        fn with_watermark(self, namespace_id: i64, entity: &str, watermark: DateTime<Utc>) -> Self {
            let key = WatermarkKey {
                namespace_id,
                entity: entity.to_string(),
            };
            self.get_behavior.lock().unwrap().insert(key, Ok(watermark));
            self
        }

        fn with_set_failure(self, namespace_id: i64, entity: &str) -> Self {
            let key = WatermarkKey {
                namespace_id,
                entity: entity.to_string(),
            };
            self.set_behavior
                .lock()
                .unwrap()
                .insert(key, Err(WatermarkError::Query("write failed".to_string())));
            self
        }

        fn stored_watermarks(&self) -> HashMap<WatermarkKey, DateTime<Utc>> {
            self.watermarks.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WatermarkStore for RecordingWatermarkStore {
        async fn get_global_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_global_watermark(&self, _: &DateTime<Utc>) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn get_namespace_watermark(
            &self,
            namespace_id: i64,
            entity: &str,
        ) -> Result<DateTime<Utc>, WatermarkError> {
            let key = WatermarkKey {
                namespace_id,
                entity: entity.to_string(),
            };
            match self.get_behavior.lock().unwrap().get(&key) {
                Some(Ok(w)) => Ok(*w),
                Some(Err(_)) => Err(WatermarkError::NoData),
                None => Err(WatermarkError::NoData),
            }
        }

        async fn set_namespace_watermark(
            &self,
            namespace_id: i64,
            entity: &str,
            watermark: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            let key = WatermarkKey {
                namespace_id,
                entity: entity.to_string(),
            };
            if let Some(Err(e)) = self.set_behavior.lock().unwrap().get(&key) {
                return Err(WatermarkError::Query(e.to_string()));
            }
            self.watermarks.lock().unwrap().insert(key, *watermark);
            Ok(())
        }
    }

    struct MockDatalake;

    #[async_trait]
    impl DatalakeQuery for MockDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }
    }

    struct FailingDatalake;

    #[async_trait]
    impl DatalakeQuery for FailingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Err(DatalakeError::Query("simulated failure".to_string()))
        }
    }

    fn create_test_node(name: &str, destination_table: &str, source_table: &str) -> NodeEntity {
        NodeEntity {
            name: name.to_string(),
            domain: String::new(),
            description: String::new(),
            label: String::new(),
            fields: vec![Field {
                name: "id".to_string(),
                source: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                enum_values: None,
                enum_type: ontology::EnumType::default(),
            }],
            primary_keys: vec!["id".to_string()],
            destination_table: destination_table.to_string(),
            etl: Some(EtlConfig::Query {
                scope: EtlScope::Namespaced,
                query: format!(
                    "SELECT id, _deleted, _version FROM {source_table} WHERE traversal_path LIKE {{traversal_path:String}}"
                ),
                edges: BTreeMap::new(),
            }),
            redaction: None,
            style: ontology::NodeStyle::default(),
        }
    }

    #[tokio::test]
    async fn handle_processes_pipelines() {
        let datalake = Arc::new(MockDatalake);
        let ontology = Ontology::new();
        let group_node = create_test_node("Group", "gl_groups", "groups");
        let issue_node = create_test_node("Issue", "gl_issues", "issues");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&group_node, &ontology, datalake.clone()).unwrap(),
            OntologyEntityPipeline::from_node(&issue_node, &ontology, datalake).unwrap(),
        ];

        let handler =
            NamespaceHandler::new(Arc::new(RecordingWatermarkStore::new()), pipelines, vec![]);

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 2,
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(MockNatsServices::new()));

        let result = handler.handle(context, envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handler_releases_lock_on_success() {
        let datalake = Arc::new(MockDatalake);
        let ontology = Ontology::new();
        let group_node = create_test_node("Group", "gl_groups", "groups");

        let pipelines =
            vec![OntologyEntityPipeline::from_node(&group_node, &ontology, datalake).unwrap()];

        let handler =
            NamespaceHandler::new(Arc::new(RecordingWatermarkStore::new()), pipelines, vec![]);

        let namespace_id = 42i64;
        let payload = serde_json::json!({
            "organization": 1,
            "namespace": namespace_id,
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let mock_nats = MockNatsServices::new();
        let lock_key = namespace_lock_key(namespace_id);
        mock_nats.set_kv(INDEXING_LOCKS_BUCKET, &lock_key, bytes::Bytes::new());

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(mock_nats.clone()));

        let result = handler.handle(context, envelope).await;

        assert!(result.is_ok());
        assert!(
            mock_nats.get_kv(INDEXING_LOCKS_BUCKET, &lock_key).is_none(),
            "namespace lock should be released after successful processing"
        );
    }

    #[tokio::test]
    async fn watermark_updated_per_entity_on_success() {
        let datalake = Arc::new(MockDatalake);
        let ontology = Ontology::new();
        let group_node = create_test_node("Group", "gl_groups", "groups");
        let issue_node = create_test_node("Issue", "gl_issues", "issues");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&group_node, &ontology, datalake.clone()).unwrap(),
            OntologyEntityPipeline::from_node(&issue_node, &ontology, datalake).unwrap(),
        ];

        let store = Arc::new(RecordingWatermarkStore::new());
        let handler = NamespaceHandler::new(store.clone(), pipelines, vec![]);

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 100,
            "watermark": "2024-06-15T12:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(MockNatsServices::new()));

        handler.handle(context, envelope).await.unwrap();

        let stored = store.stored_watermarks();
        let expected_watermark = "2024-06-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let group_key = WatermarkKey {
            namespace_id: 100,
            entity: "Group".to_string(),
        };
        let issue_key = WatermarkKey {
            namespace_id: 100,
            entity: "Issue".to_string(),
        };

        assert_eq!(
            stored.get(&group_key),
            Some(&expected_watermark),
            "Group entity watermark should be stored"
        );
        assert_eq!(
            stored.get(&issue_key),
            Some(&expected_watermark),
            "Issue entity watermark should be stored"
        );
    }

    #[tokio::test]
    async fn failed_pipeline_does_not_update_its_watermark() {
        let ok_datalake = Arc::new(MockDatalake);
        let failing_datalake: Arc<dyn DatalakeQuery> = Arc::new(FailingDatalake);
        let ontology = Ontology::new();

        let group_node = create_test_node("Group", "gl_groups", "groups");
        let issue_node = create_test_node("Issue", "gl_issues", "issues");

        let group_pipeline =
            OntologyEntityPipeline::from_node(&group_node, &ontology, ok_datalake).unwrap();

        // Build Issue pipeline with a datalake that always errors
        let issue_pipeline =
            OntologyEntityPipeline::from_node(&issue_node, &ontology, failing_datalake).unwrap();

        let store = Arc::new(RecordingWatermarkStore::new());
        let handler =
            NamespaceHandler::new(store.clone(), vec![group_pipeline, issue_pipeline], vec![]);

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 200,
            "watermark": "2024-06-15T12:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(MockNatsServices::new()));

        let result = handler.handle(context, envelope).await;
        assert!(
            result.is_err(),
            "handler should return error when a pipeline fails"
        );

        let stored = store.stored_watermarks();

        let group_key = WatermarkKey {
            namespace_id: 200,
            entity: "Group".to_string(),
        };
        let issue_key = WatermarkKey {
            namespace_id: 200,
            entity: "Issue".to_string(),
        };

        assert!(
            stored.contains_key(&group_key),
            "Group watermark should be stored since it succeeded"
        );
        assert!(
            !stored.contains_key(&issue_key),
            "Issue watermark should not be stored since it failed"
        );
    }

    #[tokio::test]
    async fn processing_continues_after_earlier_pipeline_fails() {
        let ok_datalake = Arc::new(MockDatalake);
        let failing_datalake: Arc<dyn DatalakeQuery> = Arc::new(FailingDatalake);
        let ontology = Ontology::new();

        let group_node = create_test_node("Group", "gl_groups", "groups");
        let issue_node = create_test_node("Issue", "gl_issues", "issues");

        // Group (first) fails, Issue (second) succeeds
        let group_pipeline =
            OntologyEntityPipeline::from_node(&group_node, &ontology, failing_datalake).unwrap();
        let issue_pipeline =
            OntologyEntityPipeline::from_node(&issue_node, &ontology, ok_datalake).unwrap();

        let store = Arc::new(RecordingWatermarkStore::new());
        let handler =
            NamespaceHandler::new(store.clone(), vec![group_pipeline, issue_pipeline], vec![]);

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 300,
            "watermark": "2024-06-15T12:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(MockNatsServices::new()));

        let result = handler.handle(context, envelope).await;
        assert!(result.is_err());

        let stored = store.stored_watermarks();

        let group_key = WatermarkKey {
            namespace_id: 300,
            entity: "Group".to_string(),
        };
        let issue_key = WatermarkKey {
            namespace_id: 300,
            entity: "Issue".to_string(),
        };

        assert!(
            !stored.contains_key(&group_key),
            "Group watermark should not be stored since it failed"
        );
        assert!(
            stored.contains_key(&issue_key),
            "Issue watermark should be stored even though an earlier pipeline failed"
        );
    }

    #[tokio::test]
    async fn each_entity_resolves_its_own_watermark() {
        let datalake = Arc::new(MockDatalake);
        let ontology = Ontology::new();
        let group_node = create_test_node("Group", "gl_groups", "groups");
        let issue_node = create_test_node("Issue", "gl_issues", "issues");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&group_node, &ontology, datalake.clone()).unwrap(),
            OntologyEntityPipeline::from_node(&issue_node, &ontology, datalake).unwrap(),
        ];

        let group_watermark = "2024-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let issue_watermark = "2024-05-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let store = Arc::new(
            RecordingWatermarkStore::new()
                .with_watermark(100, "Group", group_watermark)
                .with_watermark(100, "Issue", issue_watermark),
        );

        let handler = NamespaceHandler::new(store.clone(), pipelines, vec![]);

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 100,
            "watermark": "2024-06-15T12:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(MockNatsServices::new()));

        // The handler should resolve different watermarks per entity.
        // Both succeed, so both get the new watermark stored.
        handler.handle(context, envelope).await.unwrap();

        let stored = store.stored_watermarks();
        assert_eq!(
            stored.len(),
            2,
            "both entities should have updated watermarks"
        );
    }

    #[tokio::test]
    async fn watermark_set_failure_returns_handler_error() {
        let datalake = Arc::new(MockDatalake);
        let ontology = Ontology::new();
        let group_node = create_test_node("Group", "gl_groups", "groups");

        let pipelines =
            vec![OntologyEntityPipeline::from_node(&group_node, &ontology, datalake).unwrap()];

        let store = Arc::new(RecordingWatermarkStore::new().with_set_failure(100, "Group"));
        let handler = NamespaceHandler::new(store, pipelines, vec![]);

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 100,
            "watermark": "2024-06-15T12:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(MockNatsServices::new()));

        let result = handler.handle(context, envelope).await;
        assert!(
            result.is_err(),
            "should propagate watermark write failure as HandlerError"
        );

        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("failed to update namespace watermark for Group"),
            "error should identify the entity: {error_message}"
        );
    }

    #[tokio::test]
    async fn lock_not_released_when_pipeline_fails() {
        let failing_datalake: Arc<dyn DatalakeQuery> = Arc::new(FailingDatalake);
        let ontology = Ontology::new();
        let group_node = create_test_node("Group", "gl_groups", "groups");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&group_node, &ontology, failing_datalake).unwrap(),
        ];

        let handler =
            NamespaceHandler::new(Arc::new(RecordingWatermarkStore::new()), pipelines, vec![]);

        let namespace_id = 42i64;
        let payload = serde_json::json!({
            "organization": 1,
            "namespace": namespace_id,
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let mock_nats = MockNatsServices::new();
        let lock_key = namespace_lock_key(namespace_id);
        mock_nats.set_kv(INDEXING_LOCKS_BUCKET, &lock_key, bytes::Bytes::new());

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(destination, Arc::new(mock_nats.clone()));

        let result = handler.handle(context, envelope).await;
        assert!(result.is_err());

        assert!(
            mock_nats.get_kv(INDEXING_LOCKS_BUCKET, &lock_key).is_some(),
            "namespace lock should NOT be released when processing fails"
        );
    }
}
