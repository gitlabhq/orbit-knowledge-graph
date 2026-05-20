use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::checkpoint::namespace_position_key;
use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::topic::{EntityIndexingRequest, IndexingScope};

use super::pipeline::{Pipeline, PipelineContext};
use super::plan::PipelinePlan;

#[async_trait]
pub(in crate::modules::sdlc) trait EntityPipeline: Send + Sync {
    async fn run(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError>;
}

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
    async fn run(
        &self,
        request: &EntityIndexingRequest,
        destination: &dyn Destination,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        let (position_key, base_conditions) = match &request.scope {
            IndexingScope::Global => ("global".to_string(), BTreeMap::new()),
            IndexingScope::Namespace {
                namespace_id,
                traversal_path,
            } => (
                namespace_position_key(*namespace_id),
                BTreeMap::from([("traversal_path".to_string(), traversal_path.clone())]),
            ),
        };

        let plan = match &request.partition {
            Some(partition) => self.plan.clone().with_partition(partition),
            None => self.plan.clone(),
        };

        let context = PipelineContext {
            watermark: request.watermark,
            position_key,
            base_conditions,
        };

        self.pipeline
            .run_plan(&plan, &context, destination, progress)
            .await
    }
}
