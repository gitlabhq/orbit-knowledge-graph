use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

use crate::checkpoint::{CheckpointStore, Partition, entity_checkpoint_key};
use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionBounds, PartitionSpec};

use super::partition_strategy::PartitionStrategy;
use super::pipeline::{Pipeline, PipelineContext};
use super::plan::PipelinePlan;

// ---------------------------------------------------------------------------
// EntityPipeline — the trait each entity kind implements
// ---------------------------------------------------------------------------

#[async_trait]
pub(in crate::modules::sdlc) trait EntityPipeline: Send + Sync {
    async fn execute(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError>;
}

// ---------------------------------------------------------------------------
// SimpleEntityPipeline — the common entry point for entity-level indexing
// ---------------------------------------------------------------------------

pub(in crate::modules::sdlc) struct SimpleEntityPipeline {
    plan: PipelinePlan,
    partition_strategy: Option<Arc<dyn PartitionStrategy>>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    pipeline: Arc<Pipeline>,
}

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
        if self.has_consolidated_checkpoint(request).await? {
            let runs = vec![self.plan_single_run(request)];
            self.run_plans(&runs, destination, progress).await?;
            return Ok(());
        }

        let Some(strategy) = &self.partition_strategy else {
            let runs = vec![self.plan_single_run(request)];
            self.run_plans(&runs, destination, progress).await?;
            return Ok(());
        };

        let runs = match strategy.compute_partitions(request).await? {
            None => {
                info!(
                    partition_count = strategy.partition_count(),
                    "insufficient quantiles, falling back to single pipeline"
                );
                let runs = vec![self.plan_single_run(request)];
                self.run_plans(&runs, destination, progress).await?;
                return Ok(());
            }
            Some(specs) => {
                let runs: Vec<_> = specs
                    .iter()
                    .map(|spec| self.plan_partition_run(request, spec))
                    .collect();
                info!(
                    entity_kind = %request.entity_kind,
                    partitions = runs.len(),
                    "running partitioned pipeline"
                );
                runs
            }
        };

        self.run_plans(&runs, destination, progress).await?;
        self.consolidate_checkpoint(&request.scope, &request.entity_kind)
            .await?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private methods
// ---------------------------------------------------------------------------

impl SimpleEntityPipeline {
    async fn has_consolidated_checkpoint(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<bool, HandlerError> {
        let unified_key = format!(
            "{}.{}",
            entity_checkpoint_key(&request.scope, &request.entity_kind, None),
            self.plan.name
        );
        let checkpoint = self
            .checkpoint_store
            .load(&unified_key)
            .await
            .map_err(|err| HandlerError::Processing(format!("failed to load checkpoint: {err}")))?;

        Ok(checkpoint.is_some_and(|cp| cp.watermark.timestamp_micros() > 0))
    }

    fn plan_single_run(&self, request: &EntityIndexingRequest) -> (PipelinePlan, PipelineContext) {
        let position_key = entity_checkpoint_key(&request.scope, &request.entity_kind, None);
        let context = PipelineContext {
            watermark: request.watermark,
            position_key,
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
            let column = strategy.partition_column();
            plan.extract_query = plan
                .extract_query
                .with_partition_filter(partition_filter_sql(column, spec));
        }

        let partition = Partition::Range {
            index: spec.partition_index,
            total: spec.total_partitions,
        };
        let position_key =
            entity_checkpoint_key(&request.scope, &request.entity_kind, Some(&partition));
        let context = PipelineContext {
            watermark: request.watermark,
            position_key,
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
                self.pipeline.run(
                    std::slice::from_ref(plan),
                    context,
                    destination,
                    progress,
                    1,
                )
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
        scope: &IndexingScope,
        entity_kind: &str,
    ) -> Result<(), HandlerError> {
        let base_prefix = entity_checkpoint_key(scope, entity_kind, None);
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(&base_prefix)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!("failed to load checkpoints: {err}"))
            })?;

        let unified_key = format!("{base_prefix}.{}", self.plan.name);
        let min_watermark = checkpoints
            .iter()
            .filter(|(key, _)| *key != &unified_key)
            .map(|(_, cp)| cp.watermark)
            .min();

        let watermark = min_watermark.unwrap_or_else(Utc::now);
        self.checkpoint_store
            .save_completed(&unified_key, &watermark)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to save consolidated checkpoint for {unified_key}: {err}"
                ))
            })?;

        info!(
            consolidated_watermark = %watermark,
            "consolidated partition checkpoints"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

fn scope_conditions(scope: &IndexingScope) -> BTreeMap<String, String> {
    match scope {
        IndexingScope::Global => BTreeMap::new(),
        IndexingScope::Namespace { traversal_path, .. } => {
            BTreeMap::from([("traversal_path".to_string(), traversal_path.clone())])
        }
    }
}

fn partition_filter_sql(column: &str, spec: &PartitionSpec) -> String {
    match &spec.bounds {
        PartitionBounds::Range {
            lower_bound,
            upper_bound,
        } => format!("{column} >= '{lower_bound}' AND {column} < '{upper_bound}'"),
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
