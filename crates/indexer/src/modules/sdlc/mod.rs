mod datalake;
pub mod dispatch;
pub(crate) mod entity_pipeline;
mod handler;
mod metrics;
pub(crate) mod partition_strategy;
mod pipeline;
mod plan;

use std::sync::Arc;

use crate::IndexerConfig;
use crate::checkpoint::ClickHouseCheckpointStore;
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{HandlerInitError, HandlerRegistry};
use datalake::{Datalake, DatalakeQuery};
use entity_pipeline::SimpleEntityPipeline;
use handler::entity::EntityIndexingHandler;
use handler::global::GlobalHandler;
use handler::namespace::NamespaceHandler;
use metrics::SdlcMetrics;
use partition_strategy::{DatalakePartitionStrategy, PartitionStrategy, partition_column};
use pipeline::Pipeline;
use plan::{build_entity_plans, build_plans};
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

pub async fn register_entity_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
) -> Result<(), HandlerInitError> {
    let entity_config = &config.engine.handlers.entity_handler;

    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(
        datalake_client,
        entity_config.datalake_batch_size,
    ));
    let checkpoint_store: Arc<dyn crate::checkpoint::CheckpointStore> =
        Arc::new(ClickHouseCheckpointStore::new(graph_client));
    let metrics = SdlcMetrics::new();

    let pipeline = Arc::new(Pipeline::new(
        Arc::clone(&datalake),
        checkpoint_store,
        metrics.clone(),
        config.engine.datalake_retry.clone(),
    ));

    let entity_plans = build_entity_plans(
        ontology,
        entity_config.datalake_batch_size,
        &entity_config.batch_size_overrides,
    );

    let entity_names: Vec<&str> = entity_plans.iter().map(|(p, _)| p.name.as_str()).collect();
    info!(
        entity_count = entity_plans.len(),
        entities = ?entity_names,
        "registering entity indexing handler"
    );

    let mut pipelines = std::collections::HashMap::with_capacity(entity_plans.len());

    for (plan, scope) in entity_plans {
        let entity_kind = plan.name.clone();

        let partition_strategy =
            build_partition_strategy(&entity_kind, scope, ontology, entity_config, &datalake);

        let entity_pipeline: Arc<dyn entity_pipeline::EntityPipeline> = Arc::new(
            SimpleEntityPipeline::new(plan, partition_strategy, Arc::clone(&pipeline)),
        );

        pipelines.insert(entity_kind, entity_pipeline);
    }

    registry.register_handler(Box::new(EntityIndexingHandler::new(
        pipelines,
        metrics,
        entity_config.engine.clone(),
    )));

    Ok(())
}

fn build_partition_strategy(
    entity_kind: &str,
    scope: ontology::EtlScope,
    ontology: &ontology::Ontology,
    config: &gkg_server_config::EntityHandlerConfig,
    datalake: &Arc<dyn DatalakeQuery>,
) -> Option<Arc<dyn PartitionStrategy>> {
    let partition_count = config
        .partition_overrides
        .get(entity_kind)
        .copied()
        .unwrap_or(1);

    if partition_count <= 1 {
        return None;
    }

    let (order_by, source_table) = if let Some(node) = ontology.get_node(entity_kind) {
        let etl = node.etl.as_ref();
        let order = etl.map(|e| e.order_by().to_vec()).unwrap_or_default();
        let source = etl.and_then(|e| match e {
            ontology::etl::EtlConfig::Table { source, .. } => Some(source.clone()),
            ontology::etl::EtlConfig::Query { .. } => None,
        });
        (order, source)
    } else {
        let edge = ontology
            .edge_etl_configs()
            .find(|(kind, _)| *kind == entity_kind);
        match edge {
            Some((_, etl_config)) => (etl_config.order_by.clone(), Some(etl_config.source.clone())),
            None => (vec![], None),
        }
    };

    let partition_col = partition_column(&order_by, scope)?;
    let source_table = source_table?;

    Some(Arc::new(DatalakePartitionStrategy::new(
        source_table,
        partition_col.to_string(),
        partition_count,
        Arc::clone(datalake),
    )))
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
