use std::sync::Arc;

use crate::IndexerConfig;
use crate::engine::{Engine, EngineBuilder};
use crate::handler::{Handler, HandlerRegistry};
use crate::indexing_status::IndexingStatusStore;
use crate::nats::{NatsBroker, NatsServices, NatsServicesImpl};
use orbit_server_config::{
    AppConfig, ClickHouseConfiguration, CodeIndexingPipelineConfig, EngineConfiguration,
};

/// Worker cap for test engines. Production derives this from the container;
/// tests pin it so concurrency assertions do not depend on the host.
pub const TEST_MAX_CONCURRENT_WORKERS: usize = 16;
pub const TEST_SMALL_INDEXING_SLOTS: usize = 6;
pub const TEST_BIG_INDEXING_SLOTS: usize = 2;

/// `config/default.yaml` engine section with the runtime-derived fields pinned.
pub fn test_engine_configuration() -> EngineConfiguration {
    let mut engine = AppConfig::embedded_defaults().engine;
    engine.max_concurrent_workers = Some(TEST_MAX_CONCURRENT_WORKERS);
    engine.handlers.entity_handler.datalake_batch_size = Some(1);
    engine
}

/// `config/default.yaml` code-indexing pipeline with the runtime-derived slots pinned.
pub fn test_pipeline_configuration() -> CodeIndexingPipelineConfig {
    let mut pipeline = AppConfig::embedded_defaults()
        .engine
        .handlers
        .code_indexing_task
        .pipeline;
    pipeline.small_indexing_slots = Some(TEST_SMALL_INDEXING_SLOTS);
    pipeline.big_indexing_slots = Some(TEST_BIG_INDEXING_SLOTS);
    pipeline
}

pub fn create_test_indexer_config(clickhouse_config: &ClickHouseConfiguration) -> IndexerConfig {
    let mut config = IndexerConfig::from(&AppConfig::embedded_defaults());
    config.graph = clickhouse_config.clone();
    config.datalake = clickhouse_config.clone();
    config.engine = test_engine_configuration();
    config
}

pub struct TestEngineBuilder {
    broker: Arc<NatsBroker>,
    nats_services: Option<Arc<dyn NatsServices>>,
    registry: Arc<HandlerRegistry>,
    configuration: EngineConfiguration,
}

impl TestEngineBuilder {
    pub fn new(broker: Arc<NatsBroker>) -> Self {
        Self {
            broker,
            nats_services: None,
            registry: Arc::new(HandlerRegistry::default()),
            configuration: test_engine_configuration(),
        }
    }

    pub fn with_handler(self, handler: Box<dyn Handler>) -> Self {
        self.registry.register_handler(handler);
        self
    }

    pub fn with_nats_services(mut self, nats_services: Arc<dyn NatsServices>) -> Self {
        self.nats_services = Some(nats_services);
        self
    }

    pub fn with_max_workers(mut self, max: usize) -> Self {
        self.configuration.max_concurrent_workers = Some(max);
        self
    }

    pub fn with_concurrency_group(mut self, group: &str, limit: usize) -> Self {
        self.configuration
            .concurrency_groups
            .insert(group.to_string(), limit);
        self
    }

    pub fn build(self) -> (Arc<Engine>, EngineConfiguration) {
        let nats_services: Arc<dyn NatsServices> = self
            .nats_services
            .unwrap_or_else(|| Arc::new(NatsServicesImpl::new(self.broker.clone())));

        let indexing_status = Arc::new(IndexingStatusStore::new(Arc::new(
            nats_client::KvServicesImpl::new(self.broker.client().clone()),
        )));

        let engine = Arc::new(
            EngineBuilder::new(self.broker, self.registry, indexing_status)
                .nats_services(nats_services)
                .build(),
        );
        (engine, self.configuration)
    }
}
