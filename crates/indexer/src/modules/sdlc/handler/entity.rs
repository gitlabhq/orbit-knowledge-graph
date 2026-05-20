use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use gkg_server_config::HandlerConfiguration;
use tracing::{info, warn};

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::indexing_status::entity_status_key;
use crate::modules::sdlc::entity_pipeline::EntityPipeline;
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::topic::{EntityIndexingRequest, IndexingScope};
use crate::types::{Envelope, Event, SerializationError, Subscription};

pub struct EntityIndexingHandler {
    subscription: Subscription,
    config: HandlerConfiguration,
    pipelines: HashMap<String, Arc<dyn EntityPipeline>>,
    metrics: SdlcMetrics,
}

impl EntityIndexingHandler {
    pub fn new(
        config: HandlerConfiguration,
        pipelines: HashMap<String, Arc<dyn EntityPipeline>>,
        metrics: SdlcMetrics,
    ) -> Self {
        let subscription = EntityIndexingRequest::subscription();
        Self {
            subscription,
            config,
            pipelines,
            metrics,
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

        let Some(pipeline) = self.pipelines.get(&request.entity_kind) else {
            warn!(
                entity_kind = %request.entity_kind,
                "no pipeline registered for entity kind, skipping"
            );
            return Ok(());
        };

        let started_at = Utc::now();
        info!(
            entity_kind = %request.entity_kind,
            scope = ?request.scope,
            partition = ?request.partition,
            "starting entity indexing"
        );

        let status_key = match &request.scope {
            IndexingScope::Namespace { traversal_path, .. } => {
                Some(entity_status_key(traversal_path, &request.entity_kind))
            }
            IndexingScope::Global => None,
        };

        if let Some(key) = &status_key {
            context.indexing_status.record_start(key, started_at).await;
        }

        let result = pipeline
            .run(&request, context.destination.as_ref(), &context.progress)
            .await;

        let completed_at = Utc::now();
        let elapsed = completed_at
            .signed_duration_since(started_at)
            .to_std()
            .unwrap_or_default();
        self.metrics
            .record_handler_duration("entity_handler", elapsed.as_secs_f64());

        if let Some(key) = &status_key {
            context
                .indexing_status
                .record_completion(
                    key,
                    started_at,
                    completed_at,
                    result.as_ref().err().map(ToString::to_string),
                )
                .await;
        }

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
    use crate::checkpoint::CheckpointStore;
    use crate::modules::sdlc::entity_pipeline::SimpleEntityPipeline;
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

    fn build_test_handler() -> EntityIndexingHandler {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());
        let metrics = test_metrics();

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore::new()),
            metrics.clone(),
            Default::default(),
        ));

        let mut pipelines: HashMap<String, Arc<dyn EntityPipeline>> = HashMap::new();
        for plan in plans.global.into_iter().chain(plans.namespaced) {
            let name = plan.name.clone();
            pipelines.insert(
                name,
                Arc::new(SimpleEntityPipeline::new(plan, Arc::clone(&pipeline))),
            );
        }

        EntityIndexingHandler::new(HandlerConfiguration::default(), pipelines, metrics)
    }

    #[tokio::test]
    async fn handle_routes_global_entity() {
        let handler = build_test_handler();

        let payload = serde_json::json!({
            "dispatch_id": "20240121T000000",
            "entity_kind": "User",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": "Global"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_routes_namespaced_entity() {
        let handler = build_test_handler();

        let payload = serde_json::json!({
            "dispatch_id": "20240121T000000",
            "entity_kind": "MergeRequest",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": { "Namespace": { "namespace_id": 100, "traversal_path": "42/100/" } }
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_unknown_entity_returns_ok() {
        let handler = build_test_handler();

        let payload = serde_json::json!({
            "dispatch_id": "20240121T000000",
            "entity_kind": "NonExistent",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": "Global"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    fn build_handler_with_checkpoint_store(
        checkpoint_store: Arc<MockCheckpointStore>,
    ) -> EntityIndexingHandler {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());
        let metrics = test_metrics();

        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            checkpoint_store,
            metrics.clone(),
            Default::default(),
        ));

        let mut pipelines: HashMap<String, Arc<dyn EntityPipeline>> = HashMap::new();
        for plan in plans.global.into_iter().chain(plans.namespaced) {
            let name = plan.name.clone();
            pipelines.insert(
                name,
                Arc::new(SimpleEntityPipeline::new(plan, Arc::clone(&pipeline))),
            );
        }

        EntityIndexingHandler::new(HandlerConfiguration::default(), pipelines, metrics)
    }

    #[tokio::test]
    async fn handle_all_global_entities_writes_correct_checkpoints() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());
        let entity_names: Vec<String> = plans.global.iter().map(|p| p.name.clone()).collect();

        let checkpoint_store = Arc::new(MockCheckpointStore::new());
        let handler = build_handler_with_checkpoint_store(checkpoint_store.clone());

        for entity_kind in &entity_names {
            let payload = serde_json::json!({
                "dispatch_id": "20240121T000000",
                "entity_kind": entity_kind,
                "watermark": "2024-01-21T00:00:00Z",
                "scope": "Global"
            })
            .to_string();
            let envelope = TestEnvelopeFactory::simple(&payload);

            let result = handler.handle(test_handler_context(), envelope).await;
            assert!(result.is_ok(), "failed for global entity {entity_kind}");
        }

        for entity_kind in &entity_names {
            let key = format!("global.{entity_kind}");
            let checkpoint = checkpoint_store.load(&key).await.unwrap();
            assert!(
                checkpoint.is_some(),
                "missing checkpoint for {key} — pipeline did not run"
            );
            assert!(
                checkpoint.unwrap().cursor_values.is_none(),
                "checkpoint for {key} should be completed (no cursor)"
            );
        }
    }

    #[tokio::test]
    async fn handle_all_namespaced_entities_writes_correct_checkpoints() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());
        let entity_names: Vec<String> = plans.namespaced.iter().map(|p| p.name.clone()).collect();

        let checkpoint_store = Arc::new(MockCheckpointStore::new());
        let handler = build_handler_with_checkpoint_store(checkpoint_store.clone());

        for entity_kind in &entity_names {
            let payload = serde_json::json!({
                "dispatch_id": "20240121T000000",
                "entity_kind": entity_kind,
                "watermark": "2024-01-21T00:00:00Z",
                "scope": { "Namespace": { "namespace_id": 100, "traversal_path": "42/100/" } }
            })
            .to_string();
            let envelope = TestEnvelopeFactory::simple(&payload);

            let result = handler.handle(test_handler_context(), envelope).await;
            assert!(result.is_ok(), "failed for namespaced entity {entity_kind}");
        }

        for entity_kind in &entity_names {
            let key = format!("ns.100.{entity_kind}");
            let checkpoint = checkpoint_store.load(&key).await.unwrap();
            assert!(
                checkpoint.is_some(),
                "missing checkpoint for {key} — pipeline did not run"
            );
            let checkpoint = checkpoint.unwrap();
            assert!(
                checkpoint.cursor_values.is_none(),
                "checkpoint for {key} should be completed (no cursor)"
            );
            assert_eq!(
                checkpoint.watermark.to_rfc3339(),
                "2024-01-21T00:00:00+00:00",
                "checkpoint watermark for {key} should match request"
            );
        }
    }

    #[tokio::test]
    async fn handle_with_partition_assignment() {
        let handler = build_test_handler();

        let payload = serde_json::json!({
            "dispatch_id": "20240121T000000",
            "entity_kind": "MergeRequest",
            "watermark": "2024-01-21T00:00:00Z",
            "scope": { "Namespace": { "namespace_id": 100, "traversal_path": "42/100/" } },
            "partition": {
                "index": 0,
                "total": 4,
                "column": "id",
                "bounds": { "type": "Range", "lower_bound": "1", "upper_bound": "1000" }
            }
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let result = handler.handle(test_handler_context(), envelope).await;
        assert!(result.is_ok());
    }
}
