use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::info;

use crate::checkpoint::entity_checkpoint_key;
use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionSpec, PartitionStrategy};

use super::pipeline::{Pipeline, PipelineContext};
use super::plan::PipelinePlan;

const MAX_PARTITION_UPPER_BOUND: &str = "99999999999999999999";

#[async_trait]
pub(in crate::modules::sdlc) trait EntityPipeline: Send + Sync {
    async fn execute(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError>;
}

pub(in crate::modules::sdlc) struct BasePipeline {
    plan: PipelinePlan,
    partition_column: Option<String>,
    source_table: Option<String>,
    partition_count: u32,
    pipeline: Arc<Pipeline>,
}

struct ExecutionPlan {
    runs: Vec<(PipelinePlan, PipelineContext)>,
    consolidate: bool,
}

impl BasePipeline {
    pub fn new(
        plan: PipelinePlan,
        partition_column: Option<String>,
        source_table: Option<String>,
        partition_count: u32,
        pipeline: Arc<Pipeline>,
    ) -> Self {
        Self {
            plan,
            partition_column,
            source_table,
            partition_count,
            pipeline,
        }
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

    fn single_run(&self, request: &EntityIndexingRequest) -> (PipelinePlan, PipelineContext) {
        let position_key = entity_checkpoint_key(&request.scope, &request.entity_kind, None);
        let context = PipelineContext {
            watermark: request.watermark,
            position_key,
            base_conditions: scope_conditions(&request.scope),
        };
        (self.plan.clone(), context)
    }

    async fn build_runs(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<ExecutionPlan, HandlerError> {
        if self.partition_count <= 1 || self.is_bootstrapped(request).await? {
            return Ok(ExecutionPlan {
                runs: vec![self.single_run(request)],
                consolidate: false,
            });
        }

        let incomplete = self
            .incomplete_partitions(&request.scope, &request.entity_kind)
            .await?;

        if incomplete.is_empty() {
            return Ok(ExecutionPlan {
                runs: vec![],
                consolidate: true,
            });
        }

        let boundaries = match (&self.source_table, &self.partition_column) {
            (Some(table), Some(col)) => {
                self.pipeline
                    .compute_boundaries(
                        table,
                        col,
                        &request.scope,
                        self.partition_count,
                        &request.watermark,
                    )
                    .await?
            }
            _ => vec![],
        };

        if boundaries.len() < (self.partition_count - 1) as usize {
            info!(
                partition_count = self.partition_count,
                boundaries = boundaries.len(),
                "insufficient boundaries, falling back to single pipeline"
            );
            return Ok(ExecutionPlan {
                runs: vec![self.single_run(request)],
                consolidate: false,
            });
        }

        let base_conditions = scope_conditions(&request.scope);

        let runs: Vec<(PipelinePlan, PipelineContext)> = incomplete
            .iter()
            .map(|&i| {
                let spec = build_partition_spec(i, self.partition_count, &boundaries);
                let mut plan = self.plan.clone();
                if let Some(column) = &self.partition_column {
                    plan.extract_query = plan
                        .extract_query
                        .with_additional_filter(&partition_filter_sql(column, &spec));
                }
                let position_key =
                    entity_checkpoint_key(&request.scope, &request.entity_kind, Some(&spec));
                let context = PipelineContext {
                    watermark: request.watermark,
                    position_key,
                    base_conditions: base_conditions.clone(),
                };
                (plan, context)
            })
            .collect();

        info!(
            entity_kind = %request.entity_kind,
            partitions = runs.len(),
            total = self.partition_count,
            "running partitioned pipeline"
        );

        Ok(ExecutionPlan {
            runs,
            consolidate: true,
        })
    }

    async fn is_bootstrapped(&self, request: &EntityIndexingRequest) -> Result<bool, HandlerError> {
        let unified_key = self.plan_checkpoint_key(&request.scope, &request.entity_kind, None);
        match self.pipeline.load_checkpoint_option(&unified_key).await? {
            Some(cp) if cp.watermark.timestamp_micros() > 0 => Ok(true),
            _ => Ok(false),
        }
    }

    async fn incomplete_partitions(
        &self,
        scope: &IndexingScope,
        entity_kind: &str,
    ) -> Result<Vec<u32>, HandlerError> {
        let mut incomplete = Vec::new();

        for i in 0..self.partition_count {
            let spec = stub_partition_spec(i, self.partition_count);
            let key = self.plan_checkpoint_key(scope, entity_kind, Some(&spec));
            match self.pipeline.load_checkpoint_option(&key).await? {
                Some(cp) if cp.cursor_values.is_none() => {}
                _ => incomplete.push(i),
            }
        }

        Ok(incomplete)
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
        let mut min_watermark: Option<DateTime<Utc>> = None;

        for i in 0..self.partition_count {
            let spec = stub_partition_spec(i, self.partition_count);
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
            partitions = self.partition_count,
            consolidated_watermark = %watermark,
            "consolidated partition checkpoints"
        );

        Ok(())
    }
}

#[async_trait]
impl EntityPipeline for BasePipeline {
    async fn execute(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        let execution = self.build_runs(request).await?;

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
        strategy: PartitionStrategy::Range {
            lower_bound: String::new(),
            upper_bound: String::new(),
        },
    }
}

fn build_partition_spec(
    partition_index: u32,
    total_partitions: u32,
    boundaries: &[String],
) -> PartitionSpec {
    let lower_bound = if partition_index == 0 {
        String::new()
    } else {
        boundaries[(partition_index - 1) as usize].clone()
    };

    let upper_bound = if partition_index == total_partitions - 1 {
        MAX_PARTITION_UPPER_BOUND.to_string()
    } else {
        boundaries[partition_index as usize].clone()
    };

    PartitionSpec {
        partition_index,
        total_partitions,
        strategy: PartitionStrategy::Range {
            lower_bound,
            upper_bound,
        },
    }
}

fn partition_filter_sql(column: &str, spec: &PartitionSpec) -> String {
    match &spec.strategy {
        PartitionStrategy::Range {
            lower_bound,
            upper_bound,
        } => format!("{column} >= '{lower_bound}' AND {column} < '{upper_bound}'"),
    }
}

pub(crate) fn partition_column(order_by: &[String], scope: ontology::EtlScope) -> Option<&str> {
    let skip = match scope {
        ontology::EtlScope::Namespaced => 1,
        ontology::EtlScope::Global => 0,
    };
    order_by.get(skip).map(String::as_str)
}
