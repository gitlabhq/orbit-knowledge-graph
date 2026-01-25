//! Test builders for constructing test scenarios.

use std::sync::Arc;

use crate::configuration::{EngineConfiguration, ModuleConfiguration};
use crate::destination::Destination;
use crate::engine::Engine;
use crate::message_broker::MessageBroker;
use crate::module::{Module, ModuleRegistry};

use super::mocks::{MockDestination, MockMessageBroker};

/// Fluent builder for test engine setup.
pub struct TestEngineBuilder {
    broker: Option<Box<dyn MessageBroker>>,
    destination: Option<Arc<dyn Destination>>,
    registry: Arc<ModuleRegistry>,
    configuration: EngineConfiguration,
}

impl TestEngineBuilder {
    pub fn new() -> Self {
        Self {
            broker: None,
            destination: None,
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

        let engine = Arc::new(Engine::new(broker, self.registry, destination));
        (engine, self.configuration)
    }
}

impl Default for TestEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}
