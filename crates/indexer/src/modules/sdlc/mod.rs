pub mod config;
mod datalake;
pub mod dispatch;
mod global_handler;
pub mod locking;
mod metrics;
mod namespace_handler;
mod pipeline;
mod prepare;
mod transform;
mod watermark_store;

use std::sync::Arc;

use crate::clickhouse::ClickHouseConfiguration;
use crate::module::{Handler, Module, ModuleInitError};
use datalake::{Datalake, DatalakeQuery};
use global_handler::GlobalHandler;
use metrics::SdlcMetrics;
use namespace_handler::NamespaceHandler;
use ontology::{EtlScope, NodeEntity, Ontology};
use pipeline::{OntologyEdgePipeline, OntologyEntityPipeline};
use tracing::{debug, error, info};
use watermark_store::{ClickHouseWatermarkStore, WatermarkStore};

pub struct SdlcModule {
    datalake: Arc<dyn DatalakeQuery>,
    watermark_store: Arc<dyn WatermarkStore>,
    ontology: Arc<Ontology>,
    metrics: SdlcMetrics,
}

impl SdlcModule {
    pub async fn new(
        datalake_config: &ClickHouseConfiguration,
        graph_config: &ClickHouseConfiguration,
        config: &config::SdlcIndexingConfig,
    ) -> Result<Self, ModuleInitError> {
        let datalake_client = Arc::new(datalake_config.build_client());
        let graph_client = Arc::new(graph_config.build_client());
        let ontology = Ontology::load_embedded().map_err(ModuleInitError::new)?;

        Ok(Self {
            datalake: Arc::new(Datalake::new(datalake_client, config.datalake_batch_size)),
            watermark_store: Arc::new(ClickHouseWatermarkStore::new(graph_client)),
            ontology: Arc::new(ontology),
            metrics: SdlcMetrics::new(),
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
            metrics: SdlcMetrics::new(),
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
        let pipeline = OntologyEntityPipeline::from_node(
            node,
            &self.ontology,
            Arc::clone(&self.datalake),
            self.metrics.clone(),
        );
        if pipeline.is_none() {
            error!(
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
                    self.metrics.clone(),
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

        let global_pipeline_count = global_pipelines.len();
        let namespace_pipeline_count = namespace_pipelines.len();
        let namespace_edge_pipeline_count = namespace_edge_pipelines.len();

        debug!(
            global_entities = ?global_pipelines.iter().map(|p| p.entity_name()).collect::<Vec<_>>(),
            namespace_entities = ?namespace_pipelines.iter().map(|p| p.entity_name()).collect::<Vec<_>>(),
            namespace_edges = ?namespace_edge_pipelines.iter().map(|p| p.relationship_kind()).collect::<Vec<_>>(),
            "sdlc pipeline details"
        );

        let mut handlers: Vec<Box<dyn Handler>> = Vec::new();

        if !global_pipelines.is_empty() {
            handlers.push(Box::new(GlobalHandler::new(
                Arc::clone(&self.watermark_store),
                global_pipelines,
                self.metrics.clone(),
            )));
        }

        if !namespace_pipelines.is_empty() || !namespace_edge_pipelines.is_empty() {
            handlers.push(Box::new(NamespaceHandler::new(
                Arc::clone(&self.watermark_store),
                namespace_pipelines,
                namespace_edge_pipelines,
                self.metrics.clone(),
            )));
        }

        info!(
            global_entity_pipelines = global_pipeline_count,
            namespace_entity_pipelines = namespace_pipeline_count,
            namespace_edge_pipelines = namespace_edge_pipeline_count,
            "sdlc module initialized"
        );

        handlers
    }

    fn entities(&self) -> Vec<crate::entities::Entity> {
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

        async fn get_namespace_watermark(
            &self,
            _: i64,
            _: &str,
        ) -> Result<DateTime<Utc>, WatermarkError> {
            Ok(DateTime::<Utc>::UNIX_EPOCH)
        }

        async fn set_namespace_watermark(
            &self,
            _: i64,
            _: &str,
            _: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            Ok(())
        }
    }
}
