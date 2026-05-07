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
use handler::entity::EntityIndexingHandler;
use metrics::SdlcMetrics;
use pipeline::Pipeline;
use plan::build_plans;
use tracing::info;

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
) -> Result<(), HandlerInitError> {
    let entity_handler_config = config.engine.handlers.entity_indexing.clone();

    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(
        datalake_client,
        entity_handler_config.datalake_batch_size,
    ));
    let checkpoint_store: Arc<dyn crate::checkpoint::CheckpointStore> =
        Arc::new(ClickHouseCheckpointStore::new(graph_client));
    let metrics = SdlcMetrics::new();

    let plans = build_plans(
        ontology,
        entity_handler_config.datalake_batch_size,
        entity_handler_config.datalake_batch_size,
        &entity_handler_config.batch_size_overrides,
    );

    let all_plans = plans.all();
    info!(
        entity_plans = all_plans.len(),
        entities = ?all_plans.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
        "SDLC entity pipelines initialized"
    );

    let pipeline = Arc::new(Pipeline::new(
        datalake,
        checkpoint_store,
        metrics.clone(),
        config.engine.datalake_retry.clone(),
    ));

    registry.register_handler(Box::new(EntityIndexingHandler::new(
        all_plans,
        Arc::clone(&pipeline),
        metrics.clone(),
        entity_handler_config,
    )));

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
