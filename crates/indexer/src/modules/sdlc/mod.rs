mod datalake;
pub mod dispatch;
mod handler;
mod metrics;
mod pipeline;
mod plan;

use std::sync::Arc;

use crate::IndexerConfig;
use crate::checkpoint::ClickHouseCheckpointStore;
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{HandlerInitError, HandlerRegistry};
use datalake::{Datalake, DatalakeQuery};
use handler::global::GlobalHandler;
use handler::namespace::NamespaceHandler;
use metrics::SdlcMetrics;
use pipeline::Pipeline;
use plan::build_plans;
use tracing::info;

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
) -> Result<(), HandlerInitError> {
    let global_handler_config = config.engine.handlers.global_handler.clone();
    let namespace_handler_config = config.engine.handlers.namespace_handler.clone();

    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(
        datalake_client,
        global_handler_config.datalake_batch_size,
    ));
    let checkpoint_store: Arc<dyn crate::checkpoint::CheckpointStore> =
        Arc::new(ClickHouseCheckpointStore::new(graph_client));
    let metrics = SdlcMetrics::new();

    let mut batch_size_overrides = global_handler_config.batch_size_overrides.clone();
    batch_size_overrides.extend(namespace_handler_config.batch_size_overrides.clone());

    let plans = build_plans(
        ontology,
        global_handler_config.datalake_batch_size,
        namespace_handler_config.datalake_batch_size,
        &batch_size_overrides,
    );

    info!(
        global_plans = plans.global.len(),
        namespaced_plans = plans.namespaced.len(),
        global_entities = ?plans.global.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        namespaced_entities = ?plans.namespaced.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        "SDLC pipelines initialized"
    );

    let pipeline = Arc::new(Pipeline::new(
        datalake,
        checkpoint_store,
        metrics.clone(),
        config.engine.datalake_retry.clone(),
    ));

    if !plans.global.is_empty() {
        registry.register_handler(Box::new(GlobalHandler::new(
            plans.global,
            Arc::clone(&pipeline),
            metrics.clone(),
            global_handler_config,
        )));
    }

    if !plans.namespaced.is_empty() {
        registry.register_handler(Box::new(NamespaceHandler::new(
            plans.namespaced,
            Arc::clone(&pipeline),
            metrics.clone(),
            namespace_handler_config,
        )));
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod test_helpers;

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::Ontology;

    #[test]
    fn build_plans_returns_global_entities() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let entity_names: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        assert!(entity_names.contains(&"User"), "should include User entity");
    }

    #[test]
    fn build_plans_returns_namespaced_entities() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());

        let entity_names: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();
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
