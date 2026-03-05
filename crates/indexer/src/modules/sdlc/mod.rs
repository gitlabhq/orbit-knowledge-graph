#[allow(dead_code)]
mod checkpoint;
mod datalake;
pub mod dispatch;
mod global_handler;
mod metrics;
mod namespace_handler;
mod pipeline;
#[allow(dead_code)]
mod plan;
mod prepare;
mod transform;
mod watermark_store;

use std::sync::Arc;

use crate::IndexerConfig;
use crate::handler::{HandlerInitError, HandlerRegistry};
use datalake::{Datalake, DatalakeQuery};
use global_handler::GlobalHandler;
pub use global_handler::GlobalHandlerConfig;
use metrics::SdlcMetrics;
use namespace_handler::NamespaceHandler;
pub use namespace_handler::NamespaceHandlerConfig;
use ontology::{EtlScope, NodeEntity, Ontology};
use pipeline::{OntologyEdgePipeline, OntologyEntityPipeline};
use tracing::{error, info};
use watermark_store::{ClickHouseWatermarkStore, WatermarkStore};

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &Ontology,
) -> Result<(), HandlerInitError> {
    let global_handler_config = config.engine.handlers.global_handler.clone();
    let namespace_handler_config = config.engine.handlers.namespace_handler.clone();

    let datalake_batch_size = global_handler_config.datalake_batch_size;

    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let datalake: Arc<dyn DatalakeQuery> =
        Arc::new(Datalake::new(datalake_client, datalake_batch_size));
    let watermark_store: Arc<dyn WatermarkStore> =
        Arc::new(ClickHouseWatermarkStore::new(graph_client));
    let ontology = Arc::new(ontology.clone());
    let metrics = SdlcMetrics::new();

    register_global_handler(
        registry,
        &datalake,
        &watermark_store,
        &ontology,
        &metrics,
        global_handler_config,
    );
    register_namespace_handler(
        registry,
        &datalake,
        &watermark_store,
        &ontology,
        &metrics,
        namespace_handler_config,
    );

    Ok(())
}

fn register_global_handler(
    registry: &HandlerRegistry,
    datalake: &Arc<dyn DatalakeQuery>,
    watermark_store: &Arc<dyn WatermarkStore>,
    ontology: &Arc<Ontology>,
    metrics: &SdlcMetrics,
    config: GlobalHandlerConfig,
) {
    let pipelines = create_global_pipelines(ontology, datalake, metrics);

    info!(
        entity_pipelines = pipelines.len(),
        entities = ?pipelines.iter().map(|p| p.entity_name()).collect::<Vec<_>>(),
        "global handler initialized"
    );

    if !pipelines.is_empty() {
        registry.register_handler(Box::new(GlobalHandler::new(
            Arc::clone(watermark_store),
            pipelines,
            metrics.clone(),
            config,
        )));
    }
}

fn register_namespace_handler(
    registry: &HandlerRegistry,
    datalake: &Arc<dyn DatalakeQuery>,
    watermark_store: &Arc<dyn WatermarkStore>,
    ontology: &Arc<Ontology>,
    metrics: &SdlcMetrics,
    config: NamespaceHandlerConfig,
) {
    let entity_pipelines = create_namespace_pipelines(ontology, datalake, metrics);
    let edge_pipelines = create_namespace_edge_pipelines(ontology, datalake, metrics);

    info!(
        entity_pipelines = entity_pipelines.len(),
        edge_pipelines = edge_pipelines.len(),
        entities = ?entity_pipelines.iter().map(|p| p.entity_name()).collect::<Vec<_>>(),
        edges = ?edge_pipelines.iter().map(|p| p.relationship_kind()).collect::<Vec<_>>(),
        "namespace handler initialized"
    );

    if !entity_pipelines.is_empty() || !edge_pipelines.is_empty() {
        registry.register_handler(Box::new(NamespaceHandler::new(
            Arc::clone(watermark_store),
            entity_pipelines,
            edge_pipelines,
            metrics.clone(),
            config,
        )));
    }
}

fn create_global_pipelines(
    ontology: &Arc<Ontology>,
    datalake: &Arc<dyn DatalakeQuery>,
    metrics: &SdlcMetrics,
) -> Vec<OntologyEntityPipeline> {
    ontology
        .nodes()
        .filter(|node| {
            node.etl
                .as_ref()
                .is_some_and(|etl| etl.scope() == EtlScope::Global)
        })
        .filter_map(|node| try_create_pipeline(node, ontology, datalake, metrics))
        .collect()
}

fn create_namespace_pipelines(
    ontology: &Arc<Ontology>,
    datalake: &Arc<dyn DatalakeQuery>,
    metrics: &SdlcMetrics,
) -> Vec<OntologyEntityPipeline> {
    ontology
        .nodes()
        .filter(|node| {
            node.etl
                .as_ref()
                .is_some_and(|etl| etl.scope() == EtlScope::Namespaced)
        })
        .filter_map(|node| try_create_pipeline(node, ontology, datalake, metrics))
        .collect()
}

fn try_create_pipeline(
    node: &NodeEntity,
    ontology: &Ontology,
    datalake: &Arc<dyn DatalakeQuery>,
    metrics: &SdlcMetrics,
) -> Option<OntologyEntityPipeline> {
    let pipeline =
        OntologyEntityPipeline::from_node(node, ontology, Arc::clone(datalake), metrics.clone());
    if pipeline.is_none() {
        error!(
            entity = node.name,
            "failed to create pipeline for entity, skipping"
        );
    }
    pipeline
}

fn create_namespace_edge_pipelines(
    ontology: &Arc<Ontology>,
    datalake: &Arc<dyn DatalakeQuery>,
    metrics: &SdlcMetrics,
) -> Vec<OntologyEdgePipeline> {
    ontology
        .edge_etl_configs()
        .filter(|(_, config)| config.scope == EtlScope::Namespaced)
        .map(|(relationship_kind, config)| {
            OntologyEdgePipeline::from_config(
                relationship_kind,
                config,
                ontology,
                Arc::clone(datalake),
                metrics.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Int64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use futures::stream;

    use super::datalake::{DatalakeError, DatalakeQuery, RecordBatchStream};
    use super::watermark_store::{WatermarkError, WatermarkStore};

    pub(crate) struct EmptyDatalake;

    #[async_trait]
    impl DatalakeQuery for EmptyDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }
    }

    pub(crate) struct NonEmptyDatalake;

    #[async_trait]
    impl DatalakeQuery for NonEmptyDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            let schema = Arc::new(Schema::new(vec![
                ArrowField::new("id", ArrowDataType::Int64, false),
                ArrowField::new("_version", ArrowDataType::Int64, false),
                ArrowField::new("_deleted", ArrowDataType::Boolean, false),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(BooleanArray::from(vec![false])),
                ],
            )
            .unwrap();

            Ok(Box::pin(stream::once(async { Ok(batch) })))
        }
    }

    pub(crate) struct FailingDatalake;

    #[async_trait]
    impl DatalakeQuery for FailingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Err(DatalakeError::Query("simulated failure".to_string()))
        }
    }

    pub(crate) struct MockWatermarkStore;

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

#[cfg(test)]
mod tests {
    use super::*;
    use test_fixtures::EmptyDatalake;

    #[test]
    fn create_global_pipelines_returns_global_entities() {
        let ontology = Arc::new(Ontology::load_embedded().expect("should load ontology"));
        let datalake: Arc<dyn DatalakeQuery> = Arc::new(EmptyDatalake);
        let metrics = SdlcMetrics::new();

        let pipelines = create_global_pipelines(&ontology, &datalake, &metrics);

        let entity_names: Vec<_> = pipelines.iter().map(|p| p.entity_name()).collect();
        assert!(entity_names.contains(&"User"), "should include User entity");
    }

    #[test]
    fn create_namespace_pipelines_returns_namespaced_entities() {
        let ontology = Arc::new(Ontology::load_embedded().expect("should load ontology"));
        let datalake: Arc<dyn DatalakeQuery> = Arc::new(EmptyDatalake);
        let metrics = SdlcMetrics::new();

        let pipelines = create_namespace_pipelines(&ontology, &datalake, &metrics);

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
}
