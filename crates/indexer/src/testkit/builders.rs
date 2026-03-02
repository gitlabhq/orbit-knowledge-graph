//! Test builders for constructing test scenarios.

use std::sync::Arc;

use crate::configuration::EngineConfiguration;
use crate::destination::Destination;
use crate::engine::{Engine, EngineBuilder};
use crate::module::{Module, ModuleRegistry};
use crate::nats::{NatsBroker, NatsServices};

use super::mocks::{MockDestination, MockNatsServices};

/// Fluent builder for test engine setup.
///
/// The engine requires a real NATS broker. For integration tests,
/// use testcontainers to start a NATS container.
pub struct TestEngineBuilder {
    broker: Arc<NatsBroker>,
    destination: Option<Arc<dyn Destination>>,
    nats_services: Option<Arc<dyn NatsServices>>,
    registry: Arc<ModuleRegistry>,
    configuration: EngineConfiguration,
}

impl TestEngineBuilder {
    pub fn new(broker: Arc<NatsBroker>) -> Self {
        Self {
            broker,
            destination: None,
            nats_services: None,
            registry: Arc::new(ModuleRegistry::default()),
            configuration: EngineConfiguration::default(),
        }
    }

    pub fn with_module(self, module: &dyn Module) -> Self {
        self.registry.register_module(module);
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

        let nats_services = self
            .nats_services
            .unwrap_or_else(|| Arc::new(MockNatsServices::new()));

        let engine_builder = EngineBuilder::new(self.broker, self.registry, destination)
            .nats_services(nats_services);

        let engine = Arc::new(engine_builder.build());
        (engine, self.configuration)
    }
}
