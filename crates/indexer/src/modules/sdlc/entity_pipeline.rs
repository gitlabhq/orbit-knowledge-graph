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

    async fn run_single(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        let position_key = entity_checkpoint_key(&request.scope, &request.entity_kind, None);
        let context = PipelineContext {
            watermark: request.watermark,
            position_key,
            base_conditions: scope_conditions(&request.scope),
        };
        self.pipeline
            .run(
                std::slice::from_ref(&self.plan),
                &context,
                destination,
                progress,
                1,
            )
            .await
    }

    async fn scan_partition_checkpoints(
        &self,
        scope: &IndexingScope,
        entity_kind: &str,
    ) -> Result<PartitionState, HandlerError> {
        let mut incomplete_indices = Vec::new();
        let mut any_exist = false;
        let mut all_completed = true;

        for i in 0..self.partition_count {
            let spec = stub_partition_spec(i, self.partition_count);
            let key = self.plan_checkpoint_key(scope, entity_kind, Some(&spec));
            let checkpoint = self.pipeline.load_checkpoint_option(&key).await?;

            match checkpoint {
                Some(cp) => {
                    any_exist = true;
                    if cp.cursor_values.is_some() {
                        all_completed = false;
                        incomplete_indices.push(i);
                    }
                }
                None => {
                    all_completed = false;
                    incomplete_indices.push(i);
                }
            }
        }

        if !any_exist {
            return Ok(PartitionState::NoneExist);
        }

        if all_completed {
            return Ok(PartitionState::AllCompleted);
        }

        Ok(PartitionState::SomeIncomplete { incomplete_indices })
    }

    async fn run_partitioned(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
        partition_indices: &[u32],
    ) -> Result<(), HandlerError> {
        let boundaries = match (&self.source_table, &self.partition_column) {
            (Some(table), Some(col)) => {
                self.pipeline
                    .compute_boundaries(table, col, &request.scope, self.partition_count)
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
            return self.run_single(request, destination, progress).await;
        }

        let base_conditions = scope_conditions(&request.scope);

        let partition_runs: Vec<(PipelinePlan, PipelineContext)> = partition_indices
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
            partitions = partition_indices.len(),
            total = self.partition_count,
            "running partitioned pipeline"
        );

        let futures: Vec<_> = partition_runs
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
                "partitioned run failed: {}",
                errors.join("; ")
            )))
        }
    }

    async fn consolidate_checkpoint(
        &self,
        scope: &IndexingScope,
        entity_kind: &str,
    ) -> Result<DateTime<Utc>, HandlerError> {
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
            entity_kind,
            partitions = self.partition_count,
            consolidated_watermark = %watermark,
            "consolidated partition checkpoints"
        );

        Ok(watermark)
    }
}

enum PartitionState {
    NoneExist,
    AllCompleted,
    SomeIncomplete { incomplete_indices: Vec<u32> },
}

#[async_trait]
impl EntityPipeline for BasePipeline {
    async fn execute(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        if self.partition_count <= 1 {
            return self.run_single(request, destination, progress).await;
        }

        let unified_key = self.plan_checkpoint_key(&request.scope, &request.entity_kind, None);
        if let Some(cp) = self.pipeline.load_checkpoint_option(&unified_key).await?
            && cp.watermark.timestamp_micros() > 0
        {
            return self.run_single(request, destination, progress).await;
        }

        let state = self
            .scan_partition_checkpoints(&request.scope, &request.entity_kind)
            .await?;

        match state {
            PartitionState::NoneExist => {
                let all_indices: Vec<u32> = (0..self.partition_count).collect();
                self.run_partitioned(request, destination, progress, &all_indices)
                    .await?;
                self.consolidate_checkpoint(&request.scope, &request.entity_kind)
                    .await?;
            }
            PartitionState::AllCompleted => {
                self.consolidate_checkpoint(&request.scope, &request.entity_kind)
                    .await?;
                return self.run_single(request, destination, progress).await;
            }
            PartitionState::SomeIncomplete { incomplete_indices } => {
                self.run_partitioned(request, destination, progress, &incomplete_indices)
                    .await?;
                self.consolidate_checkpoint(&request.scope, &request.entity_kind)
                    .await?;
            }
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
