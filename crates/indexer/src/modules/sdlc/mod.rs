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

use std::collections::HashMap;
use std::sync::Arc;

use crate::clickhouse::ClickHouseConfiguration;
use crate::handler::{Handler, HandlerInitError, deserialize_handler_config};
use datalake::{Datalake, DatalakeQuery};
use global_handler::GlobalHandler;
pub use global_handler::GlobalHandlerConfig;
use metrics::SdlcMetrics;
use namespace_handler::NamespaceHandler;
pub use namespace_handler::NamespaceHandlerConfig;
use ontology::{EtlScope, NodeEntity, Ontology};
use pipeline::{OntologyEdgePipeline, OntologyEntityPipeline};
use tracing::{debug, error, info};
use watermark_store::{ClickHouseWatermarkStore, WatermarkStore};

pub async fn create_sdlc_handlers(
    datalake_config: &ClickHouseConfiguration,
    graph_config: &ClickHouseConfiguration,
    handler_configs: &HashMap<String, serde_json::Value>,
) -> Result<Vec<Box<dyn Handler>>, HandlerInitError> {
    let global_handler_config: GlobalHandlerConfig =
        deserialize_handler_config(handler_configs, "global-handler")?;

    let namespace_handler_config: NamespaceHandlerConfig =
        deserialize_handler_config(handler_configs, "namespace-handler")?;

    let datalake_batch_size = global_handler_config.datalake_batch_size;

    let datalake_client = Arc::new(datalake_config.build_client());
    let graph_client = Arc::new(graph_config.build_client());
    let ontology = Ontology::load_embedded().map_err(HandlerInitError::new)?;

    let datalake: Arc<dyn DatalakeQuery> =
        Arc::new(Datalake::new(datalake_client, datalake_batch_size));
    let watermark_store: Arc<dyn WatermarkStore> =
        Arc::new(ClickHouseWatermarkStore::new(graph_client));
    let ontology = Arc::new(ontology);
    let metrics = SdlcMetrics::new();

    create_handlers(
        &datalake,
        &watermark_store,
        &ontology,
        &metrics,
        global_handler_config,
        namespace_handler_config,
    )
}

fn create_handlers(
    datalake: &Arc<dyn DatalakeQuery>,
    watermark_store: &Arc<dyn WatermarkStore>,
    ontology: &Arc<Ontology>,
    metrics: &SdlcMetrics,
    global_handler_config: GlobalHandlerConfig,
    namespace_handler_config: NamespaceHandlerConfig,
) -> Result<Vec<Box<dyn Handler>>, HandlerInitError> {
    let global_pipelines = create_global_pipelines(ontology, datalake, metrics);
    let namespace_pipelines = create_namespace_pipelines(ontology, datalake, metrics);
    let namespace_edge_pipelines = create_namespace_edge_pipelines(ontology, datalake, metrics);

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
            Arc::clone(watermark_store),
            global_pipelines,
            metrics.clone(),
            global_handler_config,
        )));
    }

    if !namespace_pipelines.is_empty() || !namespace_edge_pipelines.is_empty() {
        handlers.push(Box::new(NamespaceHandler::new(
            Arc::clone(watermark_store),
            namespace_pipelines,
            namespace_edge_pipelines,
            metrics.clone(),
            namespace_handler_config,
        )));
    }

    info!(
        global_entity_pipelines = global_pipeline_count,
        namespace_entity_pipelines = namespace_pipeline_count,
        namespace_edge_pipelines = namespace_edge_pipeline_count,
        "sdlc handlers initialized"
    );

    Ok(handlers)
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
