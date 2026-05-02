pub mod dispatch;
mod handler;
mod lower;
mod metrics;
pub(crate) mod store;

pub use dispatch::NamespaceDeletionScheduler;
pub use handler::NamespaceDeletionHandler;
pub use store::{ClickHouseNamespaceDeletionStore, NamespaceDeletionStore};

use std::sync::Arc;

use crate::IndexerConfig;
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{HandlerInitError, HandlerRegistry};

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

    let handler =
        NamespaceDeletionHandler::new(store, config.engine.handlers.namespace_deletion.clone());

    registry.register_handler(Box::new(handler));
    Ok(())
}
