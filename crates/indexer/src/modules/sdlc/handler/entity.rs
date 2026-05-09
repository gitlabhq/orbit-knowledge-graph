use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use gkg_server_config::HandlerConfiguration;
use tracing::{info, warn};

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::modules::sdlc::entity_pipeline::EntityPipeline;
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::topic::EntityIndexingRequest;
use crate::types::{Envelope, Event, SerializationError, Subscription};

pub struct EntityIndexingHandler {
    subscription: Subscription,
    pipelines: HashMap<String, Arc<dyn EntityPipeline>>,
    metrics: SdlcMetrics,
    config: HandlerConfiguration,
}

impl EntityIndexingHandler {
    pub fn new(
        pipelines: HashMap<String, Arc<dyn EntityPipeline>>,
        metrics: SdlcMetrics,
        config: HandlerConfiguration,
    ) -> Self {
        let subscription = EntityIndexingRequest::subscription();
        Self {
            subscription,
            pipelines,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Handler for EntityIndexingHandler {
    fn name(&self) -> &str {
        "entity_handler"
    }

    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let request: EntityIndexingRequest = message.to_event().map_err(|error| match error {
            SerializationError::Json(err) => HandlerError::Deserialization(err),
        })?;

        let pipeline = match self.pipelines.get(&request.entity_kind) {
            Some(p) => p,
            None => {
                warn!(
                    entity_kind = %request.entity_kind,
                    "no pipeline registered for entity kind, skipping"
                );
                return Ok(());
            }
        };

        let started_at = Utc::now();
        info!(
            entity_kind = %request.entity_kind,
            scope = ?request.scope,
            partition = ?request.partition,
            "starting entity indexing"
        );

        let result = pipeline
            .execute(&request, context.destination.as_ref(), &context.progress)
            .await;

        let elapsed = Utc::now()
            .signed_duration_since(started_at)
            .to_std()
            .unwrap_or_default();
        self.metrics
            .record_handler_duration(&request.entity_kind, elapsed.as_secs_f64());

        if result.is_ok() {
            info!(
                entity_kind = %request.entity_kind,
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
    use crate::destination::Destination;
    use crate::modules::sdlc::entity_pipeline::BasePipeline;
    use crate::modules::sdlc::pipeline::Pipeline;
    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_helpers::{EmptyDatalake, MockCheckpointStore, test_metrics};
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use ontology::Ontology;

    fn test_handler_context() -> HandlerContext {
        let destination = Arc::new(MockDestination::new());
        let mock_nats = Arc::new(MockNatsServices::new());
        HandlerContext::new(
            destination,
            mock_nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(crate::indexing_status::IndexingStatusStore::new(mock_nats)),
        )
    }

    fn test_handler(pipelines: HashMap<String, Arc<dyn EntityPipeline>>) -> EntityIndexingHandler {
        EntityIndexingHandler::new(pipelines, test_metrics(), HandlerConfiguration::default())
    }

    #[tokio::test]
    async fn handle_processes_global_entity() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let user_plan = plans
            .global
            .into_iter()
            .find(|p| p.name == "User")
            .expect("User plan should exist");

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
            Default::default(),
        ));

        let handler = test_handler(HashMap::from([(
            "User".to_string(),
            Arc::new(BasePipeline::new(
                user_plan,
                Some("id".to_string()),
                pipeline,
            )) as Arc<dyn EntityPipeline>,
        )]));

        let payload = serde_json::json!({
            "entity_kind": "User",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": "Global",
            "partition": null
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_processes_namespaced_entity() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let mr_plan = plans
            .namespaced
            .into_iter()
            .find(|p| p.name == "MergeRequest")
            .expect("MergeRequest plan should exist");

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
            test_metrics(),
            Default::default(),
        ));

        let handler = test_handler(HashMap::from([(
            "MergeRequest".to_string(),
            Arc::new(BasePipeline::new(mr_plan, Some("id".to_string()), pipeline))
                as Arc<dyn EntityPipeline>,
        )]));

        let payload = serde_json::json!({
            "entity_kind": "MergeRequest",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": { "Namespace": { "namespace_id": 100, "traversal_path": "42/100/" } },
            "partition": null
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_unknown_entity_kind_returns_ok() {
        let handler = test_handler(HashMap::new());

        let payload = serde_json::json!({
            "entity_kind": "Unknown",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": "Global",
            "partition": null
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[test]
    fn subscription_covers_all_entities() {
        let handler = test_handler(HashMap::new());

        assert_eq!(
            handler.subscription().subject.as_ref(),
            "sdlc.entity.indexing.requested.>"
        );
    }

    struct NoopPipeline;

    #[async_trait]
    impl EntityPipeline for NoopPipeline {
        async fn execute(
            &self,
            _request: &EntityIndexingRequest,
            _destination: &dyn Destination,
            _progress: &ProgressNotifier,
        ) -> Result<(), HandlerError> {
            Ok(())
        }
    }
}
