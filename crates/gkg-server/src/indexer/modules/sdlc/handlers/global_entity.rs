use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl_engine::module::{HandlerContext, HandlerError};
use ontology::NodeEntity;
use serde::Serialize;

use super::HandlerCreationError;
use super::ontology_entity_pipeline::OntologyEntityPipeline;
use crate::indexer::modules::sdlc::datalake::DatalakeQuery;
use crate::indexer::modules::sdlc::watermark_store::TIMESTAMP_FORMAT;

pub struct GlobalEntityContext {
    pub handler_context: HandlerContext,
    pub last_watermark: DateTime<Utc>,
    pub watermark: DateTime<Utc>,
}

#[async_trait]
pub trait GlobalEntityHandler: Send + Sync {
    fn name(&self) -> &str;
    async fn handle(&self, context: &GlobalEntityContext) -> Result<(), HandlerError>;
}

#[derive(Clone, Serialize)]
struct GlobalQueryParams {
    last_watermark: String,
    watermark: String,
}

pub struct GlobalEntityHandlerImpl {
    pipeline: OntologyEntityPipeline,
}

impl GlobalEntityHandlerImpl {
    pub fn from_node(
        node: &NodeEntity,
        datalake: Arc<dyn DatalakeQuery>,
    ) -> Result<Self, HandlerCreationError> {
        let pipeline = OntologyEntityPipeline::from_node(node, datalake)?;
        Ok(Self { pipeline })
    }
}

#[async_trait]
impl GlobalEntityHandler for GlobalEntityHandlerImpl {
    fn name(&self) -> &str {
        &self.pipeline.entity_name
    }

    async fn handle(&self, context: &GlobalEntityContext) -> Result<(), HandlerError> {
        let params = GlobalQueryParams {
            last_watermark: context.last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            watermark: context.watermark.format(TIMESTAMP_FORMAT).to_string(),
        };

        self.pipeline
            .run(params, context.handler_context.destination.clone())
            .await
    }
}
