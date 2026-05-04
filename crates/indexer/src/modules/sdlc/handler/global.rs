use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Event, SerializationError, Subscription};
use async_trait::async_trait;
use gkg_server_config::{GlobalHandlerConfig, HandlerConfiguration};
use tracing::info;

use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::pipeline::{Pipeline, PipelineContext};
use crate::modules::sdlc::plan::PipelinePlan;
use crate::topic::GlobalIndexingRequest;

pub struct GlobalHandler {
    plans: Vec<PipelinePlan>,
    pipeline: Arc<Pipeline>,
    metrics: SdlcMetrics,
    config: GlobalHandlerConfig,
}

impl GlobalHandler {
    pub fn new(
        plans: Vec<PipelinePlan>,
        pipeline: Arc<Pipeline>,
        metrics: SdlcMetrics,
        config: GlobalHandlerConfig,
    ) -> Self {
        Self {
            plans,
            pipeline,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Handler for GlobalHandler {
    fn name(&self) -> &str {
        "global_handler"
    }

    fn subscription(&self) -> Subscription {
        GlobalIndexingRequest::subscription()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: GlobalIndexingRequest = message.to_event().map_err(|error| match error {
            SerializationError::Json(err) => HandlerError::Deserialization(err),
        })?;

        let started_at = Instant::now();
        info!(
            pipeline_count = self.plans.len(),
            "starting global indexing"
        );

        let pipeline_context = PipelineContext {
            watermark: payload.watermark,
            position_key: "global".to_string(),
            base_conditions: BTreeMap::new(),
        };

        let result = self
            .pipeline
            .run(
                &self.plans,
                &pipeline_context,
                context.destination.as_ref(),
                &context.progress,
                self.plans.len(),
            )
            .await;

        let elapsed = started_at.elapsed();
        self.metrics
            .record_handler_duration("global_handler", elapsed.as_secs_f64());

        if result.is_ok() {
            info!(
                elapsed_ms = elapsed.as_millis() as u64,
                "global indexing completed"
            );
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_helpers::{EmptyDatalake, MockCheckpointStore, test_metrics};
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use ontology::Ontology;

    #[tokio::test]
    async fn handle_processes_pipelines() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
            Default::default(),
        ));

        let handler = GlobalHandler::new(
            plans.global,
            pipeline,
            test_metrics(),
            GlobalHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let mock_nats = Arc::new(MockNatsServices::new());
        let context = HandlerContext::new(
            destination,
            mock_nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(crate::indexing_status::IndexingStatusStore::new(mock_nats)),
        );

        let result = handler.handle(context, envelope).await;
        assert!(result.is_ok());
    }
}
