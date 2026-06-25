//! Test builders for constructing test scenarios.

use std::sync::Arc;

use crate::IndexerConfig;
use crate::engine::{Engine, EngineBuilder};
use crate::handler::{Handler, HandlerRegistry};
use crate::indexing_status::IndexingStatusStore;
use crate::nats::{NatsBroker, NatsServices, NatsServicesImpl};
use gkg_server_config::{
    ClickHouseConfiguration, EngineConfiguration, EntityHandlerConfig, HandlersConfiguration,
};

pub fn create_test_indexer_config(clickhouse_config: &ClickHouseConfiguration) -> IndexerConfig {
    IndexerConfig {
        graph: clickhouse_config.clone(),
        datalake: clickhouse_config.clone(),
        engine: EngineConfiguration {
            handlers: HandlersConfiguration {
                entity_handler: EntityHandlerConfig {
                    datalake_batch_size: 1,
                    ..EntityHandlerConfig::default()
                },
                ..HandlersConfiguration::default()
            },
            ..EngineConfiguration::default()
        },
        ..IndexerConfig::default()
    }
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
            configuration: EngineConfiguration::default(),
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
        self.configuration.max_concurrent_workers = max;
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
