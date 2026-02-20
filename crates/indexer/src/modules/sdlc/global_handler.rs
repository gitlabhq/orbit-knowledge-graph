//! Handler for global-scoped entities.
//!
//! Processes entities with `EtlScope::Global` using ontology-driven pipelines.

use std::sync::Arc;
use std::time::Instant;

use crate::module::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Event, SerializationError, Topic};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use serde::Serialize;
use tracing::{debug, error, info};

use super::locking::global_lock_key;
use super::metrics::SdlcMetrics;
use super::pipeline::OntologyEntityPipeline;
use super::watermark_store::{TIMESTAMP_FORMAT, WatermarkError, WatermarkStore};
use crate::topic::GlobalIndexingRequest;

#[derive(Clone, Serialize)]
pub struct GlobalQueryParams {
    pub last_watermark: String,
    pub watermark: String,
}

impl GlobalQueryParams {
    pub fn new(last_watermark: &DateTime<Utc>, watermark: &DateTime<Utc>) -> Self {
        Self {
            last_watermark: last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            watermark: watermark.format(TIMESTAMP_FORMAT).to_string(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("params serialization failed")
    }
}

/// Handles entities without namespace ownership.
///
/// These are instance-wide records like User or Organization that exist outside
/// the namespace hierarchy. Queries filter by watermark time range only.
pub struct GlobalHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    pipelines: Vec<OntologyEntityPipeline>,
    metrics: SdlcMetrics,
}

impl GlobalHandler {
    pub fn new(
        watermark_store: Arc<dyn WatermarkStore>,
        pipelines: Vec<OntologyEntityPipeline>,
        metrics: SdlcMetrics,
    ) -> Self {
        Self {
            watermark_store,
            pipelines,
            metrics,
        }
    }
}

#[async_trait]
impl Handler for GlobalHandler {
    fn name(&self) -> &str {
        "global-handler"
    }

    fn topic(&self) -> Topic {
        GlobalIndexingRequest::topic()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: GlobalIndexingRequest = message.to_event().map_err(|error| match error {
            SerializationError::Json(e) => HandlerError::Deserialization(e),
        })?;

        let last_watermark = match self.watermark_store.get_global_watermark().await {
            Ok(w) => {
                debug!(
                    watermark = %w.format(TIMESTAMP_FORMAT),
                    "retrieved global watermark"
                );
                w
            }
            Err(WatermarkError::NoData) => {
                info!("no global watermark found, starting from epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
            Err(error) => {
                error!(%error, "global watermark fetch failed, reprocessing from epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
        };

        let started_at = Instant::now();
        let params = GlobalQueryParams::new(&last_watermark, &payload.watermark);

        info!(
            from_watermark = %params.last_watermark,
            to_watermark = %params.watermark,
            pipeline_count = self.pipelines.len(),
            "starting global indexing"
        );

        let mut errors = Vec::new();
        let mut successful_pipelines = 0;
        for pipeline in &self.pipelines {
            if let Err(error) = pipeline
                .process(params.to_json(), context.destination.as_ref(), "global")
                .await
            {
                error!(entity = pipeline.entity_name(), %error, "pipeline processing failed");
                self.metrics.pipeline_errors.add(
                    1,
                    &[
                        KeyValue::new("entity", pipeline.entity_name().to_owned()),
                        KeyValue::new("scope", "global"),
                        KeyValue::new("error_kind", error.error_kind()),
                    ],
                );
                errors.push((pipeline.entity_name().to_string(), error));
            } else {
                successful_pipelines += 1;
            }
        }

        let elapsed = started_at.elapsed();
        let handler_labels = [KeyValue::new("handler", "global-handler")];

        if errors.is_empty() {
            self.watermark_store
                .set_global_watermark(&payload.watermark)
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to update global watermark: {e}"))
                })?;

            let lag = Utc::now()
                .signed_duration_since(payload.watermark)
                .num_milliseconds()
                .max(0) as f64
                / 1000.0;
            self.metrics.watermark_lag.record(
                lag,
                &[
                    KeyValue::new("entity", "global"),
                    KeyValue::new("scope", "global"),
                ],
            );

            info!(
                watermark = %payload.watermark.format(TIMESTAMP_FORMAT),
                "global watermark updated"
            );

            if let Err(error) = context.lock_service.release(global_lock_key()).await {
                error!(%error, "failed to release global lock, will expire via TTL");
            }

            info!(
                successful_pipelines,
                elapsed_ms = elapsed.as_millis() as u64,
                "global indexing completed"
            );
        }

        self.metrics
            .handler_duration
            .record(elapsed.as_secs_f64(), &handler_labels);

        if !errors.is_empty() {
            let failed_count = errors.len();
            error!(
                failed_count,
                successful_pipelines,
                elapsed_ms = elapsed.as_millis() as u64,
                "global indexing finished with failures"
            );

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
    use crate::modules::sdlc::datalake::{DatalakeError, DatalakeQuery, RecordBatchStream};
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use futures::stream;
    use ontology::{DataType, EtlConfig, EtlScope, Field, NodeEntity, Ontology};
    use std::collections::BTreeMap;

    fn test_metrics() -> SdlcMetrics {
        let provider = opentelemetry::global::meter_provider();
        let meter = provider.meter("test");
        SdlcMetrics::with_meter(&meter)
    }

    struct MockWatermarkStore;

    #[async_trait]
    impl WatermarkStore for MockWatermarkStore {
        async fn get_global_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_global_watermark(&self, _: &DateTime<Utc>) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn get_namespace_watermark(
            &self,
            _: i64,
            _: &str,
        ) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_namespace_watermark(
            &self,
            _: i64,
            _: &str,
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

    fn create_test_node(name: &str, destination_table: &str, source: &str) -> NodeEntity {
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
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Global,
                source: source.to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
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
        let user_node = create_test_node("User", "gl_user", "siphon_users");
        let project_node = create_test_node("Project", "gl_project", "siphon_projects");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(
                &user_node,
                &ontology,
                datalake.clone(),
                test_metrics(),
            )
            .unwrap(),
            OntologyEntityPipeline::from_node(&project_node, &ontology, datalake, test_metrics())
                .unwrap(),
        ];

        let handler = GlobalHandler::new(Arc::new(MockWatermarkStore), pipelines, test_metrics());

        let payload = serde_json::json!({
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
        );

        let result = handler.handle(context, envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handler_releases_lock_on_success() {
        let datalake = Arc::new(MockDatalake);
        let ontology = Ontology::new();
        let user_node = create_test_node("User", "gl_user", "siphon_users");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&user_node, &ontology, datalake, test_metrics())
                .unwrap(),
        ];

        let handler = GlobalHandler::new(Arc::new(MockWatermarkStore), pipelines, test_metrics());

        let payload = serde_json::json!({
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let mock_locks = Arc::new(MockLockService::new());
        mock_locks.set_lock(global_lock_key());

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockNatsServices::new()),
            mock_locks.clone(),
        );

        let result = handler.handle(context, envelope).await;

        assert!(result.is_ok());
        assert!(
            !mock_locks.is_held(global_lock_key()),
            "global lock should be released after successful processing"
        );
    }
}
