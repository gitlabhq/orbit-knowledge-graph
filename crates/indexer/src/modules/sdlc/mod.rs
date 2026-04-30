pub(crate) mod datalake;
pub mod dispatch;
mod handler;
mod metrics;
mod pipeline;
mod plan;

use std::sync::Arc;

use circuit_breaker::CircuitBreaker;

use crate::IndexerConfig;
use crate::checkpoint::ClickHouseCheckpointStore;
use crate::clickhouse::ClickHouseConfigurationExt;
use crate::handler::{HandlerInitError, HandlerRegistry};
use datalake::CircuitBreakingDatalake;
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
    datalake_breaker: CircuitBreaker,
) -> Result<(), HandlerInitError> {
    let global_handler_config = config.engine.handlers.global_handler.clone();
    let namespace_handler_config = config.engine.handlers.namespace_handler.clone();

    let datalake_client = Arc::new(config.datalake.build_client());
    let graph_client = Arc::new(config.graph.build_client());

    let datalake: Arc<dyn DatalakeQuery> = Arc::new(CircuitBreakingDatalake::new(
        Datalake::new(datalake_client, global_handler_config.datalake_batch_size),
        datalake_breaker,
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
pub(crate) mod test_fixtures {
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use futures::stream;

    use super::datalake::{DatalakeError, DatalakeQuery, RecordBatchStream};
    use super::metrics::SdlcMetrics;
    use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};

    pub(crate) fn test_metrics() -> SdlcMetrics {
        SdlcMetrics::with_meter(&crate::testkit::test_meter())
    }

    pub(crate) struct EmptyDatalake;

    #[async_trait]
    impl DatalakeQuery for EmptyDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(stream::empty()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: serde_json::Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            Ok(vec![])
        }
    }

    pub(crate) struct FailingDatalake;

    #[async_trait]
    impl DatalakeQuery for FailingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: serde_json::Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Err(DatalakeError::Query("simulated failure".to_string()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: serde_json::Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            Err(DatalakeError::Query("simulated failure".to_string()))
        }
    }

    pub(crate) struct MockCheckpointStore;

    #[async_trait]
    impl CheckpointStore for MockCheckpointStore {
        async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(None)
        }

        async fn save_progress(
            &self,
            _key: &str,
            _checkpoint: &Checkpoint,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn save_completed(
            &self,
            _key: &str,
            _watermark: &chrono::DateTime<chrono::Utc>,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }
    }
}

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
