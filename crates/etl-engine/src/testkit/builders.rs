//! Test builders for constructing test scenarios.

use std::sync::Arc;

use crate::configuration::{EngineConfiguration, ModuleConfiguration};
use crate::destination::Destination;
use crate::engine::{Engine, EngineBuilder};
use crate::message_broker::MessageBroker;
use crate::metrics::MetricCollector;
use crate::module::{Module, ModuleRegistry};

use super::mocks::{MockDestination, MockMessageBroker};

/// Fluent builder for test engine setup.
pub struct TestEngineBuilder {
    broker: Option<Box<dyn MessageBroker>>,
    destination: Option<Arc<dyn Destination>>,
    metrics: Option<Arc<dyn MetricCollector>>,
    registry: Arc<ModuleRegistry>,
    configuration: EngineConfiguration,
}

impl TestEngineBuilder {
    pub fn new() -> Self {
        Self {
            broker: None,
            destination: None,
            metrics: None,
            registry: Arc::new(ModuleRegistry::default()),
            configuration: EngineConfiguration::default(),
        }
    }

    pub fn with_broker<B: MessageBroker + 'static>(mut self, broker: B) -> Self {
        self.broker = Some(Box::new(broker));
        self
    }

    pub fn with_module(self, module: &dyn Module) -> Self {
        self.registry.register_module(module);
        self
    }

    pub fn with_metrics<M: MetricCollector + 'static>(mut self, metrics: M) -> Self {
        self.metrics = Some(Arc::new(metrics));
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
        let broker = self
            .broker
            .unwrap_or_else(|| Box::new(MockMessageBroker::new()));
        let destination = self
            .destination
            .unwrap_or_else(|| Arc::new(MockDestination::new()));

        let mut engine_builder = EngineBuilder::new(broker, self.registry, destination);

        if let Some(metrics) = self.metrics {
            engine_builder = engine_builder.metrics(metrics);
        }

        let engine = Arc::new(engine_builder.build());
        (engine, self.configuration)
    }
}

impl Default for TestEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}
