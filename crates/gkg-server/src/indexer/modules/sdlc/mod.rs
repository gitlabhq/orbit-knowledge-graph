mod datalake;
mod global_handler;
mod namespace_handler;
mod pipeline;
mod transform;
mod watermark_store;

use std::path::Path;
use std::sync::Arc;

use datalake::{Datalake, DatalakeQuery};
use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::module::{Handler, Module, ModuleInitError};
use global_handler::GlobalHandler;
use namespace_handler::NamespaceHandler;
use ontology::{EtlScope, Ontology};
use pipeline::OntologyEntityPipeline;
use tracing::warn;
use watermark_store::{ClickHouseWatermarkStore, WatermarkStore};

pub struct SdlcModule {
    datalake: Arc<dyn DatalakeQuery>,
    watermark_store: Arc<dyn WatermarkStore>,
    ontology: Arc<Ontology>,
}

impl SdlcModule {
    pub async fn new(
        configuration: &ClickHouseConfiguration,
        ontology_path: &Path,
    ) -> Result<Self, ModuleInitError> {
        let client = Arc::new(configuration.build_client());
        let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(Arc::clone(&client)));
        let watermark_store: Arc<dyn WatermarkStore> =
            Arc::new(ClickHouseWatermarkStore::new(client));

        let ontology = Ontology::load_from_dir(ontology_path).map_err(ModuleInitError::new)?;

        Ok(Self {
            datalake,
            watermark_store,
            ontology: Arc::new(ontology),
        })
    }
}

impl Module for SdlcModule {
    fn name(&self) -> &str {
        "sdlc"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        let mut global_pipelines = Vec::new();
        let mut namespaced_pipelines = Vec::new();

        for node in self.ontology.nodes() {
            let Some(etl) = &node.etl else { continue };

            match OntologyEntityPipeline::from_node(node, Arc::clone(&self.datalake)) {
                Ok(pipeline) => match etl.scope() {
                    EtlScope::Global => global_pipelines.push(pipeline),
                    EtlScope::Namespaced => namespaced_pipelines.push(pipeline),
                },
                Err(error) => warn!(node = %node.name, %error, "skipping node"),
            }
        }

        let mut handlers: Vec<Box<dyn Handler>> = Vec::new();

        if !global_pipelines.is_empty() {
            handlers.push(Box::new(GlobalHandler::new(
                global_pipelines,
                Arc::clone(&self.watermark_store),
            )));
        }

        if !namespaced_pipelines.is_empty() {
            handlers.push(Box::new(NamespaceHandler::new(
                namespaced_pipelines,
                Arc::clone(&self.watermark_store),
            )));
        }

        handlers
    }

    fn entities(&self) -> Vec<etl_engine::entities::Entity> {
        Vec::new()
    }
}
