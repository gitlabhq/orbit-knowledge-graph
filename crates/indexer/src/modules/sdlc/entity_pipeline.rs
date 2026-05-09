use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::info;

use crate::checkpoint::entity_checkpoint_key;
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
    pipeline: Arc<Pipeline>,
}

impl SimpleEntityPipeline {
    pub fn new(
        plan: PipelinePlan,
        partition_strategy: Option<Arc<dyn PartitionStrategy>>,
        pipeline: Arc<Pipeline>,
    ) -> Self {
        Self {
            plan,
            partition_strategy,
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
        let state = self.resolve_checkpoint_state(request).await?;

        let execution = match &state {
            CheckpointState::Completed => ExecutionPlan {
                runs: vec![self.plan_single_run(request)],
                consolidate: false,
            },
            CheckpointState::NoCheckpoint => self.plan_initial_run(request).await?,
            CheckpointState::Incomplete { pending } if pending.is_empty() => ExecutionPlan {
                runs: vec![],
                consolidate: true,
            },
            CheckpointState::Incomplete { pending } => {
                self.plan_resumed_run(request, pending).await?
            }
        };

        if !execution.runs.is_empty() {
            self.run_plans(&execution.runs, destination, progress)
                .await?;
        }

        if execution.consolidate {
            self.consolidate_checkpoint(&request.scope, &request.entity_kind)
                .await?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private types
// ---------------------------------------------------------------------------

enum CheckpointState {
    NoCheckpoint,
    Completed,
    Incomplete { pending: Vec<u32> },
}

struct ExecutionPlan {
    runs: Vec<(PipelinePlan, PipelineContext)>,
    consolidate: bool,
}

// ---------------------------------------------------------------------------
// Private methods — checkpoint resolution, run planning, execution
// ---------------------------------------------------------------------------

impl SimpleEntityPipeline {
    async fn resolve_checkpoint_state(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<CheckpointState, HandlerError> {
        let unified_key = self.plan_checkpoint_key(&request.scope, &request.entity_kind, None);
        if let Some(cp) = self.pipeline.load_checkpoint_option(&unified_key).await?
            && cp.watermark.timestamp_micros() > 0
        {
            return Ok(CheckpointState::Completed);
        }

        let strategy = match &self.partition_strategy {
            Some(s) => s,
            None => return Ok(CheckpointState::Completed),
        };

        let pending = self
            .find_pending_partitions(
                &request.scope,
                &request.entity_kind,
                strategy.partition_count(),
            )
            .await?;

        if pending.len() == strategy.partition_count() as usize {
            Ok(CheckpointState::NoCheckpoint)
        } else {
            Ok(CheckpointState::Incomplete { pending })
        }
    }

    async fn plan_initial_run(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<ExecutionPlan, HandlerError> {
        let strategy = self
            .partition_strategy
            .as_ref()
            .expect("plan_initial_run requires a partition strategy");

        match strategy.compute_partitions(request).await? {
            None => {
                info!(
                    partition_count = strategy.partition_count(),
                    "insufficient boundaries, falling back to single pipeline"
                );
                Ok(ExecutionPlan {
                    runs: vec![self.plan_single_run(request)],
                    consolidate: false,
                })
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
                Ok(ExecutionPlan {
                    runs,
                    consolidate: true,
                })
            }
        }
    }

    async fn plan_resumed_run(
        &self,
        request: &EntityIndexingRequest,
        pending: &[u32],
    ) -> Result<ExecutionPlan, HandlerError> {
        let strategy = self
            .partition_strategy
            .as_ref()
            .expect("plan_resumed_run requires a partition strategy");

        match strategy.compute_partitions(request).await? {
            None => {
                info!(
                    partition_count = strategy.partition_count(),
                    "insufficient boundaries on resume, falling back to single pipeline"
                );
                Ok(ExecutionPlan {
                    runs: vec![self.plan_single_run(request)],
                    consolidate: false,
                })
            }
            Some(specs) => {
                let runs: Vec<_> = pending
                    .iter()
                    .map(|&i| self.plan_partition_run(request, &specs[i as usize]))
                    .collect();
                info!(
                    entity_kind = %request.entity_kind,
                    partitions = runs.len(),
                    total = strategy.partition_count(),
                    "resuming partitioned pipeline"
                );
                Ok(ExecutionPlan {
                    runs,
                    consolidate: true,
                })
            }
        }
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
                .with_additional_filter(&partition_filter_sql(column, spec));
        }

        let position_key = entity_checkpoint_key(&request.scope, &request.entity_kind, Some(spec));
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

    async fn find_pending_partitions(
        &self,
        scope: &IndexingScope,
        entity_kind: &str,
        partition_count: u32,
    ) -> Result<Vec<u32>, HandlerError> {
        let mut pending = Vec::new();

        for i in 0..partition_count {
            let spec = stub_partition_spec(i, partition_count);
            let key = self.plan_checkpoint_key(scope, entity_kind, Some(&spec));
            match self.pipeline.load_checkpoint_option(&key).await? {
                Some(cp) if cp.cursor_values.is_none() => {}
                _ => pending.push(i),
            }
        }

        Ok(pending)
    }

    async fn consolidate_checkpoint(
        &self,
        scope: &IndexingScope,
        entity_kind: &str,
    ) -> Result<(), HandlerError> {
        let partition_count = self
            .partition_strategy
            .as_ref()
            .map(|s| s.partition_count())
            .unwrap_or(1);

        let mut min_watermark: Option<DateTime<Utc>> = None;

        for i in 0..partition_count {
            let spec = stub_partition_spec(i, partition_count);
            let key = self.plan_checkpoint_key(scope, entity_kind, Some(&spec));
            if let Some(cp) = self.pipeline.load_checkpoint_option(&key).await? {
                let wm = cp.watermark;
                min_watermark = Some(match min_watermark {
                    Some(current) if wm < current => wm,
                    Some(current) => current,
                    None => wm,
                });
            }
        }

        let watermark = min_watermark.unwrap_or_else(Utc::now);
        let unified_key = self.plan_checkpoint_key(scope, entity_kind, None);
        self.pipeline
            .save_completed(&unified_key, &watermark)
            .await?;

        info!(
            partitions = partition_count,
            consolidated_watermark = %watermark,
            "consolidated partition checkpoints"
        );

        Ok(())
    }

    fn plan_checkpoint_key(
        &self,
        scope: &IndexingScope,
        entity_kind: &str,
        partition: Option<&PartitionSpec>,
    ) -> String {
        let base = entity_checkpoint_key(scope, entity_kind, partition);
        format!("{}.{}", base, self.plan.name)
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

fn stub_partition_spec(partition_index: u32, total_partitions: u32) -> PartitionSpec {
    PartitionSpec {
        partition_index,
        total_partitions,
        bounds: PartitionBounds::Range {
            lower_bound: String::new(),
            upper_bound: String::new(),
        },
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

    fn make_pipeline() -> Arc<Pipeline> {
        Arc::new(Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(MockCheckpointStore),
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

        let pipeline = SimpleEntityPipeline::new(user_plan, None, make_pipeline());
        let destination = MockDestination::new();
        let request = EntityIndexingRequest {
            entity_kind: "User".to_string(),
            watermark: "2024-01-21T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Global,
        };

        let result = pipeline
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

        let pipeline = SimpleEntityPipeline::new(mr_plan, None, make_pipeline());
        let destination = MockDestination::new();
        let request = EntityIndexingRequest {
            entity_kind: "MergeRequest".to_string(),
            watermark: "2024-01-21T00:00:00Z".parse().unwrap(),
            scope: IndexingScope::Namespace {
                namespace_id: 100,
                traversal_path: "42/100/".to_string(),
            },
        };

        let result = pipeline
            .execute(&request, &destination, &ProgressNotifier::noop())
            .await;
        assert!(result.is_ok());
    }
}
