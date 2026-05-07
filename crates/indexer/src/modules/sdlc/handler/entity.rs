use std::collections::BTreeMap;
use std::sync::Arc;

use crate::checkpoint::namespace_position_key;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Event, SerializationError, Subscription};
use async_trait::async_trait;
use chrono::Utc;
use gkg_server_config::{EntityIndexingHandlerConfig, HandlerConfiguration};
use tracing::{info, warn};

use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::pipeline::{Pipeline, PipelineContext};
use crate::modules::sdlc::plan::PipelinePlan;
use crate::topic::EntityIndexingRequest;

pub struct EntityIndexingHandler {
    plans: Vec<PipelinePlan>,
    pipeline: Arc<Pipeline>,
    metrics: SdlcMetrics,
    config: EntityIndexingHandlerConfig,
}

impl EntityIndexingHandler {
    pub fn new(
        plans: Vec<PipelinePlan>,
        pipeline: Arc<Pipeline>,
        metrics: SdlcMetrics,
        config: EntityIndexingHandlerConfig,
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
impl Handler for EntityIndexingHandler {
    fn name(&self) -> &str {
        "entity_indexing_handler"
    }

    fn subscription(&self) -> Subscription {
        EntityIndexingRequest::subscription()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: EntityIndexingRequest = message.to_event().map_err(|error| match error {
            SerializationError::Json(err) => HandlerError::Deserialization(err),
        })?;

        let plan = self.plans.iter().find(|p| p.name == payload.entity);
        let plan = match plan {
            Some(p) => p,
            None => {
                warn!(entity = %payload.entity, "no plan found for entity, skipping");
                return Ok(());
            }
        };

        let mut plan = plan.clone();
        plan.extract_query = plan.extract_query.with_range(payload.range);

        let is_namespaced = payload.traversal_path.is_some();

        let started_at = Utc::now();
        let position_key = match (&payload.traversal_path, payload.namespace) {
            (Some(_), Some(ns)) => namespace_position_key(ns),
            _ => "global".to_string(),
        };

        if let Some(traversal_path) = &payload.traversal_path {
            info!(
                entity = %payload.entity,
                traversal_path = %traversal_path,
                "starting entity indexing"
            );
            context
                .indexing_status
                .record_start(traversal_path, started_at)
                .await;
        } else {
            info!(entity = %payload.entity, "starting global entity indexing");
        }

        let mut base_conditions = BTreeMap::new();
        if let Some(tp) = &payload.traversal_path {
            base_conditions.insert("traversal_path".to_string(), tp.clone());
        }

        let pipeline_context = PipelineContext {
            watermark: payload.watermark,
            position_key,
            base_conditions,
        };

        let result = self
            .pipeline
            .run(
                std::slice::from_ref(&plan),
                &pipeline_context,
                context.destination.as_ref(),
                &context.progress,
                1,
            )
            .await;

        let completed_at = Utc::now();
        let elapsed = completed_at
            .signed_duration_since(started_at)
            .to_std()
            .unwrap_or_default();
        self.metrics
            .record_handler_duration("entity_indexing_handler", elapsed.as_secs_f64());

        if is_namespaced && let Some(traversal_path) = &payload.traversal_path {
            context
                .indexing_status
                .record_completion(
                    traversal_path,
                    started_at,
                    completed_at,
                    result.as_ref().err().map(ToString::to_string),
                )
                .await;
        }

        if result.is_ok() {
            info!(
                entity = %payload.entity,
                elapsed_ms = elapsed.as_millis() as u64,
                "entity indexing completed"
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
    async fn handle_processes_global_entity() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
            Default::default(),
        ));

        let handler = EntityIndexingHandler::new(
            plans.all(),
            pipeline,
            test_metrics(),
            EntityIndexingHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "entity": "User",
            "namespace": null,
            "traversal_path": null,
            "range": null,
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

    #[tokio::test]
    async fn handle_processes_namespaced_entity() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
            Default::default(),
        ));

        let handler = EntityIndexingHandler::new(
            plans.all(),
            pipeline,
            test_metrics(),
            EntityIndexingHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "entity": "Project",
            "namespace": 42,
            "traversal_path": "1/42/",
            "range": null,
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

    #[tokio::test]
    async fn handle_skips_unknown_entity() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
            Default::default(),
        ));

        let handler = EntityIndexingHandler::new(
            plans.all(),
            pipeline,
            test_metrics(),
            EntityIndexingHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "entity": "NonExistentEntity",
            "namespace": null,
            "traversal_path": null,
            "range": null,
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
