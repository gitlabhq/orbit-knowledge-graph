//! Test builders for constructing test scenarios.

use std::sync::Arc;

use crate::IndexerConfig;
use crate::destination::Destination;
use crate::engine::{Engine, EngineBuilder};
use crate::handler::{Handler, HandlerRegistry};
use crate::indexing_status::IndexingStatusStore;
use crate::nats::{NatsBroker, NatsServices, NatsServicesImpl};
use gkg_server_config::{
    ClickHouseConfiguration, EngineConfiguration, GlobalHandlerConfig, HandlersConfiguration,
    NamespaceHandlerConfig,
};

use super::mocks::MockDestination;

/// Creates an `IndexerConfig` suitable for integration tests.
///
/// Sets `datalake_batch_size` to 1 for both SDLC handlers so tests process
/// one row at a time. Uses the provided ClickHouse config for both graph and datalake.
pub fn create_test_indexer_config(clickhouse_config: &ClickHouseConfiguration) -> IndexerConfig {
    IndexerConfig {
        graph: clickhouse_config.clone(),
        datalake: clickhouse_config.clone(),
        engine: EngineConfiguration {
            handlers: HandlersConfiguration {
                global_handler: GlobalHandlerConfig {
                    datalake_batch_size: 1,
                    ..GlobalHandlerConfig::default()
                },
                namespace_handler: NamespaceHandlerConfig {
                    datalake_batch_size: 1,
                    ..NamespaceHandlerConfig::default()
                },
                ..HandlersConfiguration::default()
            },
            ..EngineConfiguration::default()
        },
        ..IndexerConfig::default()
    }
}

/// Fluent builder for test engine setup.
///
/// The engine requires a real NATS broker. For integration tests,
/// use testcontainers to start a NATS container.
pub struct TestEngineBuilder {
    broker: Arc<NatsBroker>,
    destination: Option<Arc<dyn Destination>>,
    nats_services: Option<Arc<dyn NatsServices>>,
    registry: Arc<HandlerRegistry>,
    configuration: EngineConfiguration,
}

impl TestEngineBuilder {
    pub fn new(broker: Arc<NatsBroker>) -> Self {
        Self {
            broker,
            destination: None,
            nats_services: None,
            registry: Arc::new(HandlerRegistry::default()),
            configuration: EngineConfiguration::default(),
        }
    }

    pub fn with_handler(self, handler: Box<dyn Handler>) -> Self {
        self.registry.register_handler(handler);
        self
    }

    pub fn with_destination(mut self, destination: Arc<dyn Destination>) -> Self {
        self.destination = Some(destination);
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
        let destination = self
            .destination
            .unwrap_or_else(|| Arc::new(MockDestination::new()));

        let nats_services: Arc<dyn NatsServices> = self
            .nats_services
            .unwrap_or_else(|| Arc::new(NatsServicesImpl::new(self.broker.clone())));

        let indexing_status = Arc::new(IndexingStatusStore::new(nats_services.clone()));

        let engine_builder =
            EngineBuilder::new(self.broker, self.registry, destination, indexing_status)
                .nats_services(nats_services);

        let engine = Arc::new(engine_builder.build());
        (engine, self.configuration)
    }
}
