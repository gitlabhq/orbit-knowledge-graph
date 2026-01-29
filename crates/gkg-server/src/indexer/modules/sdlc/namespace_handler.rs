//! Handler for namespace-scoped entities.
//!
//! Processes entities with `EtlScope::Namespaced` using ontology-driven pipelines.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Event, SerializationError, Topic};
use serde::Serialize;
use tracing::warn;

use super::pipeline::OntologyEntityPipeline;
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
}

impl NamespaceHandler {
    pub fn new(
        watermark_store: Arc<dyn WatermarkStore>,
        pipelines: Vec<OntologyEntityPipeline>,
    ) -> Self {
        Self {
            watermark_store,
            pipelines,
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

        let last_watermark = match self
            .watermark_store
            .get_namespace_watermark(payload.namespace)
            .await
        {
            Ok(w) => w,
            Err(WatermarkError::NoData) => DateTime::<Utc>::UNIX_EPOCH,
            Err(error) => {
                warn!(%error, "failed to fetch namespace watermark, using epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
        };

        let params = NamespaceQueryParams::new(
            payload.organization,
            payload.namespace,
            &last_watermark,
            &payload.watermark,
        );

        let mut errors = Vec::new();
        for pipeline in &self.pipelines {
            if let Err(error) = pipeline
                .process(params.to_json(), context.destination.as_ref())
                .await
            {
                warn!(entity = pipeline.entity_name(), %error, "pipeline processing failed");
                errors.push((pipeline.entity_name().to_string(), error));
            }
        }

        if errors.is_empty() {
            self.watermark_store
                .set_namespace_watermark(payload.namespace, &payload.watermark)
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to update namespace watermark: {e}"))
                })?;
        }

        // TODO: We should store per entity watermarks so we can resume for a specific entity and not the whole thing.
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
    use etl_engine::testkit::{
        MockDestination, MockMetricCollector, MockNatsServices, TestEnvelopeFactory,
    };
    use futures::stream;
    use ontology::{DataType, EtlConfig, EtlScope, Field, NodeEntity};
    use std::collections::BTreeMap;

    struct MockWatermarkStore;

    #[async_trait]
    impl WatermarkStore for MockWatermarkStore {
        async fn get_global_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_global_watermark(&self, _: &DateTime<Utc>) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn get_namespace_watermark(&self, _: i64) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_namespace_watermark(
            &self,
            _: i64,
            _: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
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

    fn create_test_node(name: &str, destination_table: &str, source_table: &str) -> NodeEntity {
        NodeEntity {
            name: name.to_string(),
            fields: vec![Field {
                name: "id".to_string(),
                source: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                enum_values: None,
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
        }
    }

    #[tokio::test]
    async fn handle_processes_pipelines() {
        let datalake = Arc::new(MockDatalake);
        let group_node = create_test_node("Group", "gl_groups", "groups");
        let issue_node = create_test_node("Issue", "gl_issues", "issues");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&group_node, datalake.clone()).unwrap(),
            OntologyEntityPipeline::from_node(&issue_node, datalake).unwrap(),
        ];

        let handler = NamespaceHandler::new(Arc::new(MockWatermarkStore), pipelines);

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 2,
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockMetricCollector::new()),
            Arc::new(MockNatsServices::new()),
        );

        let result = handler.handle(context, envelope).await;

        assert!(result.is_ok());
    }
}
