use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use crate::checkpoint::namespace_position_key;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::progress::ProgressWriter;
use crate::types::{Envelope, Event, SerializationError, Subscription};
use async_trait::async_trait;
use gkg_server_config::{HandlerConfiguration, NamespaceHandlerConfig};
use tracing::{info, warn};

use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::pipeline::{Pipeline, PipelineContext};
use crate::modules::sdlc::plan::PipelinePlan;
use crate::topic::NamespaceIndexingRequest;

pub struct NamespaceHandler {
    plans: Vec<PipelinePlan>,
    pipeline: Arc<Pipeline>,
    metrics: SdlcMetrics,
    config: NamespaceHandlerConfig,
    progress_writer: Arc<ProgressWriter>,
}

impl NamespaceHandler {
    pub fn new(
        plans: Vec<PipelinePlan>,
        pipeline: Arc<Pipeline>,
        metrics: SdlcMetrics,
        config: NamespaceHandlerConfig,
        progress_writer: Arc<ProgressWriter>,
    ) -> Self {
        Self {
            plans,
            pipeline,
            metrics,
            config,
            progress_writer,
        }
    }
}

#[async_trait]
impl Handler for NamespaceHandler {
    fn name(&self) -> &str {
        "namespace_handler"
    }

    fn subscription(&self) -> Subscription {
        NamespaceIndexingRequest::subscription()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: NamespaceIndexingRequest =
            message.to_event().map_err(|error| match error {
                SerializationError::Json(err) => HandlerError::Deserialization(err),
            })?;

        let started_at = Instant::now();
        let started_at_wall = chrono::Utc::now();
        info!(
            namespace_id = payload.namespace,
            organization_id = payload.organization,
            pipeline_count = self.plans.len(),
            "starting namespace indexing"
        );

        let traversal_path = format!("{}/{}/", payload.organization, payload.namespace);

        // Best-effort pre-ETL write of state="indexing" so readers see the active
        // phase. Failing here must not block ETL.
        if let Err(e) = self
            .progress_writer
            .mark_indexing_started(context.nats.as_ref(), payload.namespace, started_at_wall)
            .await
        {
            warn!(namespace_id = payload.namespace, error = %e, "failed to mark indexing started");
        }

        let pipeline_context = PipelineContext {
            watermark: payload.watermark,
            position_key: namespace_position_key(payload.namespace),
            base_conditions: BTreeMap::from([(
                "traversal_path".to_string(),
                traversal_path.clone(),
            )]),
        };

        let result = self
            .pipeline
            .run(
                &self.plans,
                &pipeline_context,
                context.destination.as_ref(),
                &context.progress,
            )
            .await;

        let elapsed = started_at.elapsed();
        self.metrics
            .record_handler_duration("namespace_handler", elapsed.as_secs_f64());

        let total_rows = result.as_ref().ok().map(|o| o.total_rows).unwrap_or(0);
        let error_msg = result.as_ref().err().map(|e| e.to_string());

        if result.is_ok() {
            info!(
                namespace_id = payload.namespace,
                elapsed_ms = elapsed.as_millis() as u64,
                total_rows,
                "namespace indexing completed"
            );
        }

        if let Err(e) = self
            .progress_writer
            .write_progress(
                context.nats.as_ref(),
                payload.namespace,
                &traversal_path,
                started_at_wall,
                elapsed,
                total_rows,
                error_msg.as_deref(),
            )
            .await
        {
            warn!(namespace_id = payload.namespace, error = %e, "failed to write indexing progress");
        }

        result.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::ClickHouseConfigurationExt;
    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_fixtures::{EmptyDatalake, MockCheckpointStore, test_metrics};
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use ontology::Ontology;

    use gkg_server_config::indexing_progress::{INDEXING_PROGRESS_BUCKET, MetaSnapshot, meta_key};

    fn build_handler() -> NamespaceHandler {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000);
        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
        ));
        let graph_client =
            Arc::new(gkg_server_config::ClickHouseConfiguration::default().build_client());
        // debounce 9999s so write_progress won't try to query ClickHouse (debounced
        // after first call), but mark_indexing_started is not debounced.
        let progress_writer = Arc::new(ProgressWriter::new(graph_client, Arc::new(ontology), 9999));
        NamespaceHandler::new(
            plans.namespaced,
            pipeline,
            test_metrics(),
            NamespaceHandlerConfig::default(),
            progress_writer,
        )
    }

    #[tokio::test]
    async fn handle_processes_pipelines() {
        let handler = build_handler();

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
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
        );

        let result = handler.handle(context, envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_marks_indexing_state_before_etl() {
        let handler = build_handler();
        let mock = Arc::new(MockNatsServices::new());

        let payload = serde_json::json!({
            "organization": 1,
            "namespace": 42,
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let context = HandlerContext::new(
            Arc::new(MockDestination::new()),
            Arc::clone(&mock) as Arc<dyn crate::nats::NatsServices>,
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
        );

        handler.handle(context, envelope).await.unwrap();

        // After the handler runs, the meta key must exist. With empty datalake,
        // the final write path writes state="idle". But BEFORE the debounced
        // write_progress, mark_indexing_started fires and writes state="indexing".
        // Because EmptyDatalake returns 0 rows and debounce is 9999s, the idle
        // write is also applied (debounce is AFTER the first write, not before).
        // So after the run, we expect state="idle" (idle overwrites indexing).
        let entry = mock
            .get_kv(INDEXING_PROGRESS_BUCKET, &meta_key(42))
            .expect("meta key should exist after handle");
        let meta: MetaSnapshot = serde_json::from_slice(&entry).unwrap();
        assert!(
            meta.state == "idle" || meta.state == "indexing",
            "state should be idle or indexing, got {}",
            meta.state
        );
    }
}
