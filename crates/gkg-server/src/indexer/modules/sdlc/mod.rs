mod datalake;
mod global_handler;
pub mod locking;
mod namespace_handler;
mod pipeline;
mod prepare;
mod transform;
mod watermark_store;

use std::sync::Arc;

use datalake::{Datalake, DatalakeQuery};
use etl_engine::clickhouse::ClickHouseConfiguration;
use etl_engine::module::{Handler, Module, ModuleInitError};
use global_handler::GlobalHandler;
use namespace_handler::NamespaceHandler;
use ontology::{EtlScope, NodeEntity, Ontology};
use pipeline::{OntologyEdgePipeline, OntologyEntityPipeline};
use tracing::warn;
use watermark_store::{ClickHouseWatermarkStore, WatermarkStore};

pub struct SdlcModule {
    datalake: Arc<dyn DatalakeQuery>,
    watermark_store: Arc<dyn WatermarkStore>,
    ontology: Arc<Ontology>,
}

impl SdlcModule {
    pub async fn new(
        datalake_config: &ClickHouseConfiguration,
        graph_config: &ClickHouseConfiguration,
    ) -> Result<Self, ModuleInitError> {
        let datalake_client = Arc::new(datalake_config.build_client());
        let graph_client = Arc::new(graph_config.build_client());
        let ontology = Ontology::load_embedded().map_err(ModuleInitError::new)?;

        Ok(Self {
            datalake: Arc::new(Datalake::new(datalake_client)),
            watermark_store: Arc::new(ClickHouseWatermarkStore::new(graph_client)),
            ontology: Arc::new(ontology),
        })
    }

    #[cfg(test)]
    fn with_ontology(
        datalake: Arc<dyn DatalakeQuery>,
        watermark_store: Arc<dyn WatermarkStore>,
        ontology: Ontology,
    ) -> Self {
        Self {
            datalake,
            watermark_store,
            ontology: Arc::new(ontology),
        }
    }

    fn create_global_pipelines(&self) -> Vec<OntologyEntityPipeline> {
        self.ontology
            .nodes()
            .filter(|node| {
                node.etl
                    .as_ref()
                    .is_some_and(|etl| etl.scope() == EtlScope::Global)
            })
            .filter_map(|node| self.try_create_pipeline(node))
            .collect()
    }

    fn create_namespace_pipelines(&self) -> Vec<OntologyEntityPipeline> {
        self.ontology
            .nodes()
            .filter(|node| {
                node.etl
                    .as_ref()
                    .is_some_and(|etl| etl.scope() == EtlScope::Namespaced)
            })
            .filter_map(|node| self.try_create_pipeline(node))
            .collect()
    }

    fn try_create_pipeline(&self, node: &NodeEntity) -> Option<OntologyEntityPipeline> {
        let pipeline =
            OntologyEntityPipeline::from_node(node, &self.ontology, Arc::clone(&self.datalake));
        if pipeline.is_none() {
            warn!(
                entity = node.name,
                "failed to create pipeline for entity, skipping"
            );
        }
        pipeline
    }

    fn create_namespace_edge_pipelines(&self) -> Vec<OntologyEdgePipeline> {
        self.ontology
            .edge_etl_configs()
            .filter(|(_, config)| config.scope == EtlScope::Namespaced)
            .map(|(relationship_kind, config)| {
                OntologyEdgePipeline::from_config(
                    relationship_kind,
                    config,
                    &self.ontology,
                    Arc::clone(&self.datalake),
                )
            })
            .collect()
    }
}

impl Module for SdlcModule {
    fn name(&self) -> &str {
        "sdlc"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        let global_pipelines = self.create_global_pipelines();
        let namespace_pipelines = self.create_namespace_pipelines();
        let namespace_edge_pipelines = self.create_namespace_edge_pipelines();

        let mut handlers: Vec<Box<dyn Handler>> = Vec::new();

        if !global_pipelines.is_empty() {
            handlers.push(Box::new(GlobalHandler::new(
                Arc::clone(&self.watermark_store),
                global_pipelines,
            )));
        }

        if !namespace_pipelines.is_empty() || !namespace_edge_pipelines.is_empty() {
            handlers.push(Box::new(NamespaceHandler::new(
                Arc::clone(&self.watermark_store),
                namespace_pipelines,
                namespace_edge_pipelines,
            )));
        }

        handlers
    }

    fn entities(&self) -> Vec<etl_engine::entities::Entity> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_global_pipelines_returns_global_entities() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let module = SdlcModule::with_ontology(
            Arc::new(MockDatalake),
            Arc::new(MockWatermarkStore),
            ontology,
        );

        let pipelines = module.create_global_pipelines();

        let entity_names: Vec<_> = pipelines.iter().map(|p| p.entity_name()).collect();
        assert!(entity_names.contains(&"User"), "should include User entity");
    }

    #[test]
    fn create_namespace_pipelines_returns_namespaced_entities() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let module = SdlcModule::with_ontology(
            Arc::new(MockDatalake),
            Arc::new(MockWatermarkStore),
            ontology,
        );

        let pipelines = module.create_namespace_pipelines();

        let entity_names: Vec<_> = pipelines.iter().map(|p| p.entity_name()).collect();
        assert!(
            entity_names.contains(&"Group"),
            "should include Group entity"
        );
        assert!(
            entity_names.contains(&"Project"),
            "should include Project entity"
        );
    }

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use datalake::{DatalakeError, RecordBatchStream};
    use futures::stream;
    use watermark_store::WatermarkError;

    struct MockDatalake;

    #[async_trait]
    impl DatalakeQuery for MockDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }
    }

    struct MockWatermarkStore;

    #[async_trait]
    impl WatermarkStore for MockWatermarkStore {
        async fn get_global_watermark(&self) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_global_watermark(&self, _: &DateTime<Utc>) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn get_namespace_watermark(&self, _: i64) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_namespace_watermark(
            &self,
            _: i64,
            _: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            Ok(())
        }
    }
}
