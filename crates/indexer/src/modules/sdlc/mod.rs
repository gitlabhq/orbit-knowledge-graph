mod datalake;
pub mod dispatch;
mod handler;
mod metrics;
pub(crate) mod observer;
mod partitioning;
mod pipeline;
mod plan;

use std::sync::Arc;

use ontology::EtlScope;

use crate::IndexerConfig;
use crate::analytics::IndexingAnalytics;
use crate::checkpoint::ClickHouseCheckpointStore;
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{HandlerInitError, HandlerRegistry};
use crate::topic::{
    GLOBAL_HANDLER_TOPIC, GlobalIndexingRequest, NAMESPACE_HANDLER_TOPIC, NamespaceIndexingRequest,
};
use crate::types::Event;
use datalake::{Datalake, DatalakeQuery};
use handler::entity::EntityHandler;
use metrics::SdlcMetrics;
use pipeline::Pipeline;
use tracing::info;

pub async fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
    analytics: IndexingAnalytics,
) -> Result<(), HandlerInitError> {
    let entity_handler_config = config.engine.handlers.entity_handler.clone();

    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let datalake: Arc<dyn DatalakeQuery> = Arc::new(Datalake::new(
        datalake_client,
        entity_handler_config.datalake_batch_size,
    ));
    let checkpoint_store: Arc<dyn crate::checkpoint::CheckpointStore> =
        Arc::new(ClickHouseCheckpointStore::new(graph_client));
    let metrics = SdlcMetrics::new();

    let inputs = plan::input::from_ontology(ontology);
    let partition_strategies =
        partitioning::build_strategies(&inputs, &entity_handler_config.partition_overrides);
    let plans = plan::build_plans(
        ontology,
        entity_handler_config.datalake_batch_size,
        entity_handler_config.datalake_batch_size,
        &entity_handler_config.batch_size_overrides,
    );

    let pipeline = Arc::new(Pipeline::new(
        Arc::clone(&datalake),
        Arc::clone(&checkpoint_store),
        metrics.clone(),
        config.engine.datalake_retry.clone(),
    ));

    let mut global_subscription = GlobalIndexingRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(GLOBAL_HANDLER_TOPIC) {
        global_subscription = global_subscription.with_config(topic_config);
    }
    let mut namespace_subscription = NamespaceIndexingRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(NAMESPACE_HANDLER_TOPIC) {
        namespace_subscription = namespace_subscription.with_config(topic_config);
    }

    let mut global_count = 0;
    let mut namespaced_count = 0;
    for plan in plans.global {
        let strategy = partition_strategies.get(&plan.name).cloned();
        registry.register_handler(Box::new(EntityHandler::new(
            plan,
            EtlScope::Global,
            Arc::clone(&pipeline),
            Arc::clone(&datalake),
            Arc::clone(&checkpoint_store),
            metrics.clone(),
            global_subscription.clone(),
            strategy,
            analytics.clone(),
        )));
        global_count += 1;
    }
    for plan in plans.namespaced {
        let strategy = partition_strategies.get(&plan.name).cloned();
        registry.register_handler(Box::new(EntityHandler::new(
            plan,
            EtlScope::Namespaced,
            Arc::clone(&pipeline),
            Arc::clone(&datalake),
            Arc::clone(&checkpoint_store),
            metrics.clone(),
            namespace_subscription.clone(),
            strategy,
            analytics.clone(),
        )));
        namespaced_count += 1;
    }

    info!(
        global_handlers = global_count,
        namespaced_handlers = namespaced_count,
        partitioned_entities = partition_strategies.len(),
        "registered SDLC entity handlers"
    );

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
        let plans = plan::build_plans(&ontology, 1000, 1000, &Default::default());
        let names: Vec<_> = plans.global.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"User"));
    }

    #[test]
    fn build_plans_returns_namespaced_entities() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = plan::build_plans(&ontology, 1000, 1000, &Default::default());
        let names: Vec<_> = plans.namespaced.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"Group"));
        assert!(names.contains(&"Project"));
    }
}
