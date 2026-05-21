pub mod dispatch;
mod handler;
mod lower;
mod metrics;
pub(crate) mod store;

pub use dispatch::NamespaceDeletionScheduler;
pub use handler::NamespaceDeletionHandler;
pub use metrics::DeletionMetrics;
pub use store::{ClickHouseNamespaceDeletionStore, NamespaceDeletionStore};

use std::sync::Arc;

use crate::IndexerConfig;
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{HandlerInitError, HandlerRegistry};
use crate::topic::{NAMESPACE_DELETION_TOPIC, NamespaceDeletionRequest};
use crate::types::Event;

pub fn register_handlers(
    registry: &HandlerRegistry,
    config: &IndexerConfig,
    ontology: &ontology::Ontology,
) -> Result<(), HandlerInitError> {
    let graph_client = Arc::new(config.graph.build_client());
    let datalake_client = Arc::new(config.datalake.build_client());

    let store: Arc<dyn NamespaceDeletionStore> = Arc::new(ClickHouseNamespaceDeletionStore::new(
        datalake_client,
        graph_client,
        ontology,
    ));

    let mut subscription = NamespaceDeletionRequest::subscription();
    if let Some(topic_config) = config.engine.topics.get(NAMESPACE_DELETION_TOPIC) {
        subscription = subscription.with_config(topic_config);
    }

    let handler =
        NamespaceDeletionHandler::new(store, metrics::DeletionMetrics::new(), subscription);

    registry.register_handler(Box::new(handler));
    Ok(())
}
