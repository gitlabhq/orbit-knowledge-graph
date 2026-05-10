use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::info;

use std::collections::HashMap;

use crate::checkpoint::{Checkpoint, CheckpointStore, EntityCheckpointKey};
use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionSpec};

use super::partition_strategy::PartitionStrategy;
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

pub(in crate::modules::sdlc) struct SimpleEntityPipeline {
    plan: PipelinePlan,
    partition_strategy: Option<Arc<dyn PartitionStrategy>>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    pipeline: Arc<Pipeline>,
}

// Additive: SimpleEntityPipeline will absorb Pipeline (pipeline.rs)
// once entity-level indexing fully replaces the namespace-level path.
impl SimpleEntityPipeline {
    pub fn new(
        plan: PipelinePlan,
        partition_strategy: Option<Arc<dyn PartitionStrategy>>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        pipeline: Arc<Pipeline>,
    ) -> Self {
        Self {
            plan,
            partition_strategy,
            checkpoint_store,
            pipeline,
        }
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
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(checkpoint_key.prefix())
            .await
            .map_err(|err| {
                HandlerError::Processing(format!("failed to load checkpoints: {err}"))
            })?;

        let unified_key = checkpoint_key.full_key(&self.plan.name);
        let completed = checkpoints
            .get(&unified_key)
            .is_some_and(|cp| cp.is_completed());

        let runs = if completed {
            vec![self.plan_single_run(request)]
        } else {
            self.plan_pending_runs(request, &checkpoints).await?
        };

        if !runs.is_empty() {
            self.run_plans(&runs, destination, progress).await?;
        }

        if !completed && self.partition_strategy.is_some() {
            self.consolidate_checkpoint(&checkpoints, &unified_key, &request.watermark)
                .await?;
        }

        Ok(())
    }
}

impl SimpleEntityPipeline {
    async fn plan_pending_runs(
        &self,
        request: &EntityIndexingRequest,
        checkpoints: &HashMap<String, Checkpoint>,
    ) -> Result<Vec<(PipelinePlan, PipelineContext)>, HandlerError> {
        let Some(strategy) = &self.partition_strategy else {
            return Ok(vec![self.plan_single_run(request)]);
        };

        match strategy.compute_partitions(request).await? {
            Some(specs) => {
                let pending =
                    filter_pending_partitions(request, &self.plan.name, specs, checkpoints);
                info!(
                    entity_kind = %request.entity_kind,
                    pending = pending.len(),
                    "running partitioned pipeline"
                );
                Ok(pending
                    .iter()
                    .map(|spec| self.plan_partition_run(request, spec))
                    .collect())
            }
            None => {
                info!(
                    partition_count = strategy.partition_count(),
                    "insufficient quantiles, falling back to single pipeline"
                );
                Ok(vec![self.plan_single_run(request)])
            }
        }
    }

    fn plan_single_run(&self, request: &EntityIndexingRequest) -> (PipelinePlan, PipelineContext) {
        let checkpoint_key = EntityCheckpointKey::new(&request.scope);
        let context = PipelineContext {
            watermark: request.watermark,
            position_key: checkpoint_key.full_key(&self.plan.name),
            base_conditions: scope_conditions(&request.scope),
        };
        (self.plan.clone(), context)
    }

    fn plan_partition_run(
        &self,
        request: &EntityIndexingRequest,
        spec: &PartitionSpec,
    ) -> (PipelinePlan, PipelineContext) {
        let mut plan = self.plan.clone();
        if let Some(strategy) = &self.partition_strategy {
            plan.extract_query = plan.extract_query.with_partition_filter(
                strategy.partition_column().to_string(),
                spec.bounds.clone(),
            );
        }

        let checkpoint_key = EntityCheckpointKey::new(&request.scope)
            .with_partition(spec.partition_index, spec.total_partitions);
        let context = PipelineContext {
            watermark: request.watermark,
            position_key: checkpoint_key.full_key(&self.plan.name),
            base_conditions: scope_conditions(&request.scope),
        };
        (plan, context)
    }

    async fn run_plans(
        &self,
        runs: &[(PipelinePlan, PipelineContext)],
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        let futures: Vec<_> = runs
            .iter()
            .map(|(plan, context)| {
                self.pipeline
                    .run_plan(plan, &context.position_key, context, destination, progress)
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let errors: Vec<String> = results
            .into_iter()
            .filter_map(|r| r.err().map(|e| e.to_string()))
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(HandlerError::Processing(format!(
                "pipeline run failed: {}",
                errors.join("; ")
            )))
        }
    }

    async fn consolidate_checkpoint(
        &self,
        checkpoints: &HashMap<String, Checkpoint>,
        unified_key: &str,
        watermark: &DateTime<Utc>,
    ) -> Result<(), HandlerError> {
        let min_watermark = checkpoints
            .iter()
            .filter(|(key, cp)| {
                key.as_str() != unified_key && key.starts_with(unified_key) && cp.is_completed()
            })
            .map(|(_, cp)| cp.watermark)
            .fold(*watermark, Ord::min);

        self.checkpoint_store
            .save_completed(unified_key, &min_watermark)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to save consolidated checkpoint for {unified_key}: {err}"
                ))
            })?;

        info!(
            consolidated_watermark = %min_watermark,
            "consolidated partition checkpoints"
        );

        Ok(())
    }
}

fn filter_pending_partitions(
    request: &EntityIndexingRequest,
    plan_name: &str,
    specs: Vec<PartitionSpec>,
    checkpoints: &HashMap<String, Checkpoint>,
) -> Vec<PartitionSpec> {
    let total = specs.len();
    let pending: Vec<PartitionSpec> = specs
        .into_iter()
        .filter(|spec| {
            let key = EntityCheckpointKey::new(&request.scope)
                .with_partition(spec.partition_index, spec.total_partitions)
                .full_key(plan_name);
            !matches!(checkpoints.get(&key), Some(cp) if cp.is_completed())
        })
        .collect();

    if pending.len() < total {
        info!(
            pending = pending.len(),
            total, "resuming partitioned pipeline, some partitions already completed"
        );
    }

    pending
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

    fn make_pipeline() -> (Arc<dyn crate::checkpoint::CheckpointStore>, Arc<Pipeline>) {
        let checkpoint_store: Arc<dyn crate::checkpoint::CheckpointStore> =
            Arc::new(MockCheckpointStore);
        let pipeline = Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::clone(&checkpoint_store),
            test_metrics(),
            Default::default(),
        ));
        (checkpoint_store, pipeline)
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

        let (checkpoint_store, pipeline) = make_pipeline();
        let entity_pipeline =
            SimpleEntityPipeline::new(user_plan, None, checkpoint_store, pipeline);
        let destination = MockDestination::new();
        let request = EntityIndexingRequest {
            entity_kind: "User".to_string(),
            watermark: "2024-01-21T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Global,
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

        let (checkpoint_store, pipeline) = make_pipeline();
        let entity_pipeline = SimpleEntityPipeline::new(mr_plan, None, checkpoint_store, pipeline);
        let destination = MockDestination::new();
        let request = EntityIndexingRequest {
            entity_kind: "MergeRequest".to_string(),
            watermark: "2024-01-21T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Namespace {
                namespace_id: 100,
                traversal_path: "42/100/".to_string(),
            },
        };

        let result = entity_pipeline
            .execute(&request, &destination, &ProgressNotifier::noop())
            .await;
        assert!(result.is_ok());
    }
}
