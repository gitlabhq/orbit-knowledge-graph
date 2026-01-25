//! Test builders for constructing test scenarios.

use std::sync::Arc;

use crate::configuration::{EngineConfiguration, ModuleConfiguration};
use crate::destination::Destination;
use crate::engine::{Engine, EngineBuilder};
use crate::metrics::MetricCollector;
use crate::module::{Module, ModuleRegistry};
use crate::nats::{NatsBroker, NatsServices};

use super::mocks::{MockDestination, MockNatsServices};

/// Fluent builder for test engine setup.
///
/// The engine requires a real NATS broker. For integration tests,
/// use testcontainers to start a NATS container.
///
/// # Example
///
/// ```ignore
/// let config = NatsConfiguration { url: "localhost:4222".into(), ..Default::default() };
/// let broker = Arc::new(NatsBroker::connect(&config).await?);
///
/// let (engine, config) = TestEngineBuilder::new(broker)
///     .with_module(&MyModule)
///     .build();
/// ```
pub struct TestEngineBuilder {
    broker: Arc<NatsBroker>,
    destination: Option<Arc<dyn Destination>>,
    metrics: Option<Arc<dyn MetricCollector>>,
    nats_services: Option<Arc<dyn NatsServices>>,
    registry: Arc<ModuleRegistry>,
    configuration: EngineConfiguration,
}

impl TestEngineBuilder {
    pub fn new(broker: Arc<NatsBroker>) -> Self {
        Self {
            broker,
            destination: None,
            metrics: None,
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

    pub fn with_metrics<M: MetricCollector + 'static>(mut self, metrics: M) -> Self {
        self.metrics = Some(Arc::new(metrics));
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

    pub fn with_module_concurrency(mut self, module: &str, max: usize) -> Self {
        self.configuration.modules.insert(
            module.to_string(),
            ModuleConfiguration {
                max_concurrency: Some(max),
            },
        );
        self
    }

    pub fn build(self) -> (Arc<Engine>, EngineConfiguration) {
        let destination = self
            .destination
            .unwrap_or_else(|| Arc::new(MockDestination::new()));

        let nats_services = self
            .nats_services
            .unwrap_or_else(|| Arc::new(MockNatsServices::new()));

        let mut engine_builder = EngineBuilder::new(self.broker, self.registry, destination)
            .nats_services(nats_services);

        if let Some(metrics) = self.metrics {
            engine_builder = engine_builder.metrics(metrics);
        }

        let engine = Arc::new(engine_builder.build());
        (engine, self.configuration)
    }
}
