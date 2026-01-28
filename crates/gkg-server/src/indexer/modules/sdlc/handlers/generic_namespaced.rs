use std::sync::Arc;

use async_trait::async_trait;
use etl_engine::module::HandlerError;
use ontology::NodeEntity;
use serde::Serialize;

use super::HandlerCreationError;
use super::ontology_entity_pipeline::OntologyEntityPipeline;
use crate::indexer::modules::sdlc::datalake::DatalakeQuery;
use crate::indexer::modules::sdlc::namespace_handler::{
    NamespacedEntityContext, NamespacedEntityHandler,
};
use crate::indexer::modules::sdlc::watermark_store::TIMESTAMP_FORMAT;

#[derive(Clone, Serialize)]
struct NamespacedQueryParams {
    traversal_path: String,
    last_watermark: String,
    watermark: String,
}

pub struct NamespacedEntityHandlerImpl {
    pipeline: OntologyEntityPipeline,
}

impl NamespacedEntityHandlerImpl {
    pub fn from_node(
        node: &NodeEntity,
        datalake: Arc<dyn DatalakeQuery>,
    ) -> Result<Self, HandlerCreationError> {
        let pipeline = OntologyEntityPipeline::from_node(node, datalake)?;
        Ok(Self { pipeline })
    }
}

#[async_trait]
impl NamespacedEntityHandler for NamespacedEntityHandlerImpl {
    fn name(&self) -> &str {
        &self.pipeline.entity_name
    }

    async fn handle(&self, context: &NamespacedEntityContext) -> Result<(), HandlerError> {
        let params = NamespacedQueryParams {
            traversal_path: format!(
                "{}/{}/",
                context.payload.organization, context.payload.namespace
            ),
            last_watermark: context.last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            watermark: context
                .payload
                .watermark
                .format(TIMESTAMP_FORMAT)
                .to_string(),
        };

        self.pipeline
            .run(params, context.handler_context.destination.clone())
            .await
    }
}
