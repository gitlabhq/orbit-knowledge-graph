use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionSpec, PartitionStrategy};

use super::pipeline::{Pipeline, PipelineContext};
use super::plan::PipelinePlan;
use crate::checkpoint::entity_checkpoint_key;

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
    pipeline: Arc<Pipeline>,
}

impl BasePipeline {
    pub fn new(
        plan: PipelinePlan,
        partition_column: Option<String>,
        pipeline: Arc<Pipeline>,
    ) -> Self {
        Self {
            plan,
            partition_column,
            pipeline,
        }
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
        let mut plan = self.plan.clone();

        if let (Some(spec), Some(column)) = (&request.partition, &self.partition_column) {
            plan.extract_query = plan
                .extract_query
                .with_additional_filter(&partition_filter_sql(column, spec));
        }

        let position_key = entity_checkpoint_key(
            &request.scope,
            &request.entity_kind,
            request.partition.as_ref(),
        );

        let base_conditions = match &request.scope {
            IndexingScope::Global => BTreeMap::new(),
            IndexingScope::Namespace { traversal_path, .. } => {
                BTreeMap::from([("traversal_path".to_string(), traversal_path.clone())])
            }
        };

        let context = PipelineContext {
            watermark: request.watermark,
            position_key,
            base_conditions,
        };

        self.pipeline
            .run(&[plan], &context, destination, progress, 1)
            .await
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

pub(in crate::modules::sdlc) fn partition_column(
    order_by: &[String],
    scope: ontology::EtlScope,
) -> Option<&str> {
    let skip = match scope {
        ontology::EtlScope::Namespaced => 1,
        ontology::EtlScope::Global => 0,
    };
    order_by.get(skip).map(String::as_str)
}
