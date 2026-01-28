mod datalake;
pub mod handlers;
mod namespace_handler;
mod transform;
mod watermark_store;

use std::path::Path;
use std::sync::Arc;

use datalake::{Datalake, DatalakeQuery};
use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::module::{Handler, Module, ModuleInitError};
use handlers::{
    GenericGlobalEntityHandler, GenericNamespacedHandler, GlobalEntityHandler, GlobalHandler,
};
use namespace_handler::{NamespaceHandler, NamespacedEntityHandler};
use ontology::{EtlScope, Ontology};
use tracing::warn;
use watermark_store::{ClickHouseWatermarkStore, WatermarkStore};

pub struct SdlcModule {
    datalake: Arc<dyn DatalakeQuery>,
    watermark_store: Arc<dyn WatermarkStore>,
    ontology: Arc<Ontology>,
}

impl SdlcModule {
    /// Create the SDLC module with ontology-driven handlers.
    ///
    /// Dynamically generates handlers from the ontology YAML configuration.
    /// Nodes with ETL config are automatically registered as handlers based on their scope:
    /// - Global scope: handled by GlobalHandler orchestrator
    /// - Namespaced scope: handled by NamespaceHandler orchestrator
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
        let mut global_entity_handlers: Vec<Box<dyn GlobalEntityHandler>> = Vec::new();
        let mut namespaced_entity_handlers: Vec<Box<dyn NamespacedEntityHandler>> = Vec::new();

        for node in self.ontology.nodes() {
            let Some(etl) = &node.etl else { continue };

            match etl.scope() {
                EtlScope::Global => {
                    match GenericGlobalEntityHandler::new(
                        node.clone(),
                        Arc::clone(&self.datalake),
                    ) {
                        Ok(handler) => global_entity_handlers.push(Box::new(handler)),
                        Err(error) => warn!(node = %node.name, %error, "skipping node"),
                    }
                }
                EtlScope::Namespaced => {
                    match GenericNamespacedHandler::new(
                        node.clone(),
                        Arc::clone(&self.datalake),
                    ) {
                        Ok(handler) => namespaced_entity_handlers.push(Box::new(handler)),
                        Err(error) => warn!(node = %node.name, %error, "skipping node"),
                    }
                }
            }
        }

        let mut handlers: Vec<Box<dyn Handler>> = Vec::new();

        if !global_entity_handlers.is_empty() {
            handlers.push(Box::new(GlobalHandler::new(
                global_entity_handlers,
                Arc::clone(&self.watermark_store),
            )));
        }

        if !namespaced_entity_handlers.is_empty() {
            handlers.push(Box::new(NamespaceHandler::new(
                namespaced_entity_handlers,
                Arc::clone(&self.watermark_store),
            )));
        }

        handlers
    }

    fn entities(&self) -> Vec<etl_engine::entities::Entity> {
        Vec::new()
    }
}
