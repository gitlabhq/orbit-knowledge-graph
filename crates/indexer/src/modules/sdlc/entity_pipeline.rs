use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::checkpoint::EntityCheckpointKey;
use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::topic::{EntityIndexingRequest, IndexingScope};

use super::pipeline::{Pipeline, PipelineContext};
use super::plan::PipelinePlan;

#[async_trait]
pub(in crate::modules::sdlc) trait EntityPipeline: Send + Sync {
    async fn execute(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError>;
}

// Additive: SimpleEntityPipeline will absorb Pipeline (pipeline.rs)
// once entity-level indexing fully replaces the namespace-level path.
pub(in crate::modules::sdlc) struct SimpleEntityPipeline {
    plan: PipelinePlan,
    pipeline: Arc<Pipeline>,
}

impl SimpleEntityPipeline {
    pub fn new(plan: PipelinePlan, pipeline: Arc<Pipeline>) -> Self {
        Self { plan, pipeline }
    }
}

#[async_trait]
impl EntityPipeline for SimpleEntityPipeline {
    async fn execute(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        let checkpoint_key = EntityCheckpointKey::new(&request.scope);
        let mut plan = self.plan.clone();

        let position_key = match &request.partition {
            Some(assignment) => {
                plan.extract_query = plan
                    .extract_query
                    .with_partition_filter(assignment.column.clone(), assignment.bounds.clone());
                checkpoint_key
                    .with_partition(assignment.index, assignment.total)
                    .full_key(&self.plan.name)
            }
            None => checkpoint_key.full_key(&self.plan.name),
        };

        let context = PipelineContext {
            watermark: request.watermark,
            position_key: position_key.clone(),
            base_conditions: scope_conditions(&request.scope),
        };

        self.pipeline
            .run_plan(&plan, &position_key, &context, destination, progress)
            .await
    }
}

fn scope_conditions(scope: &IndexingScope) -> BTreeMap<String, String> {
    match scope {
        IndexingScope::Global => BTreeMap::new(),
        IndexingScope::Namespace { traversal_path, .. } => {
            BTreeMap::from([("traversal_path".to_string(), traversal_path.clone())])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::pipeline::Pipeline;
    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_helpers::{EmptyDatalake, MockCheckpointStore, test_metrics};
    use crate::nats::ProgressNotifier;
    use crate::testkit::MockDestination;
    use ontology::Ontology;

    fn make_pipeline() -> Arc<Pipeline> {
        let checkpoint_store: Arc<dyn crate::checkpoint::CheckpointStore> =
            Arc::new(MockCheckpointStore);
        Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            checkpoint_store,
            test_metrics(),
            Default::default(),
        ))
    }

    #[tokio::test]
    async fn single_run_completes_for_global_entity() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let user_plan = plans
            .global
            .into_iter()
            .find(|p| p.name == "User")
            .expect("User plan should exist");

        let pipeline = make_pipeline();
        let entity_pipeline = SimpleEntityPipeline::new(user_plan, pipeline);
        let destination = MockDestination::new();
        let request = EntityIndexingRequest {
            entity_kind: "User".to_string(),
            watermark: "2024-01-21T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Global,
            partition: None,
        };

        let result = entity_pipeline
            .execute(&request, &destination, &ProgressNotifier::noop())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn single_run_completes_for_namespaced_entity() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let mr_plan = plans
            .namespaced
            .into_iter()
            .find(|p| p.name == "MergeRequest")
            .expect("MergeRequest plan should exist");

        let pipeline = make_pipeline();
        let entity_pipeline = SimpleEntityPipeline::new(mr_plan, pipeline);
        let destination = MockDestination::new();
        let request = EntityIndexingRequest {
            entity_kind: "MergeRequest".to_string(),
            watermark: "2024-01-21T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Namespace {
                namespace_id: 100,
                traversal_path: "42/100/".to_string(),
            },
            partition: None,
        };

        let result = entity_pipeline
            .execute(&request, &destination, &ProgressNotifier::noop())
            .await;
        assert!(result.is_ok());
    }
}
