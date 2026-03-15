use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use crate::checkpoint::namespace_position_key;
use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Event, SerializationError, Subscription};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::modules::sdlc::handler::default_datalake_batch_size;
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::pipeline::{Pipeline, PipelineContext};
use crate::modules::sdlc::plan::PipelinePlan;
use crate::topic::NamespaceIndexingRequest;

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

pub struct NamespaceHandler {
    plans: Vec<PipelinePlan>,
    pipeline: Arc<Pipeline>,
    metrics: SdlcMetrics,
    config: NamespaceHandlerConfig,
}

impl NamespaceHandler {
    pub fn new(
        plans: Vec<PipelinePlan>,
        pipeline: Arc<Pipeline>,
        metrics: SdlcMetrics,
        config: NamespaceHandlerConfig,
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
        info!(
            namespace_id = payload.namespace,
            organization_id = payload.organization,
            pipeline_count = self.plans.len(),
            "starting namespace indexing"
        );

        let traversal_path = format!("{}/{}/", payload.organization, payload.namespace);

        let pipeline_context = PipelineContext {
            watermark: payload.watermark,
            position_key: namespace_position_key(payload.namespace),
            base_conditions: BTreeMap::from([("traversal_path".to_string(), traversal_path)]),
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

        if result.is_ok() {
            info!(
                namespace_id = payload.namespace,
                elapsed_ms = elapsed.as_millis() as u64,
                "namespace indexing completed"
            );
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_fixtures::{EmptyDatalake, MockCheckpointStore, test_metrics};
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use ontology::Ontology;

    #[tokio::test]
    async fn handle_processes_pipelines() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000);

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
        ));

        let handler = NamespaceHandler::new(
            plans.namespaced,
            pipeline,
            test_metrics(),
            NamespaceHandlerConfig::default(),
        );

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
}
