mod datalake;
mod handler;
mod indexing_position;
#[allow(dead_code)]
mod locking;
#[allow(dead_code)]
mod metrics;
mod pipeline;
#[allow(dead_code)]
pub(crate) mod plan;

use std::sync::Arc;

use crate::IndexerConfig;
use crate::handler::{HandlerInitError, HandlerRegistry};
use datalake::{Datalake, DatalakeQuery};
use handler::global::{GlobalHandler, GlobalHandlerConfig};
use handler::namespace::{NamespaceHandler, NamespaceHandlerConfig};
use indexing_position::{ClickHousePositionStore, IndexingPositionStore};
use metrics::SdlcMetrics;
use ontology::Ontology;
use pipeline::Pipeline;
use plan::from_ontology::build_plans;
use tracing::info;

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &Ontology,
) -> Result<(), HandlerInitError> {
    let batch_size = config.engine.handlers.global_handler.datalake_batch_size;

    let datalake_client = Arc::new(config.datalake.build_client());
    let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(datalake_client, batch_size));

    let graph_client = Arc::new(config.graph.build_client());
    let position_store: Arc<dyn IndexingPositionStore> =
        Arc::new(ClickHousePositionStore::new(graph_client));

    let destination: Arc<dyn crate::destination::Destination> = Arc::new(
        crate::clickhouse::ClickHouseDestination::new(
            config.graph.clone(),
            Arc::new(crate::metrics::EngineMetrics::default()),
        )
        .map_err(HandlerInitError::new)?,
    );

    let metrics = SdlcMetrics::new();
    let plans = build_plans(ontology, batch_size);

    let pipeline = Arc::new(Pipeline::new(
        Arc::clone(&datalake),
        Arc::clone(&position_store),
        Arc::clone(&destination),
        metrics.clone(),
    ));

    if !plans.global.is_empty() {
        info!(
            pipeline_count = plans.global.len(),
            pipelines = ?plans.global.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            "sdlc_v2 global handler initialized"
        );
        registry.register_handler(Box::new(GlobalHandler::new(
            plans.global,
            Arc::clone(&pipeline),
            metrics.clone(),
            GlobalHandlerConfig {
                engine: config.engine.handlers.global_handler.engine.clone(),
                datalake_batch_size: config.engine.handlers.global_handler.datalake_batch_size,
            },
        )));
    }

    if !plans.namespaced.is_empty() {
        info!(
            pipeline_count = plans.namespaced.len(),
            pipelines = ?plans.namespaced.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            "sdlc_v2 namespace handler initialized"
        );
        registry.register_handler(Box::new(NamespaceHandler::new(
            plans.namespaced,
            pipeline,
            metrics,
            NamespaceHandlerConfig {
                engine: config.engine.handlers.namespace_handler.engine.clone(),
                datalake_batch_size: config.engine.handlers.namespace_handler.datalake_batch_size,
            },
        )));
    }

    Ok(())
}
