use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use circuit_breaker::{CircuitBreakerObserver, CircuitBreakerRegistry, CircuitConfig, ServiceName};
use gkg_server_config::{CircuitBreakerConfig, ServiceCircuitBreakerConfig};

#[derive(Debug, Clone, Copy)]
pub enum IndexerService {
    ClickHouseDatalake,
    ClickHouseGraph,
    Nats,
    Rails,
}

impl ServiceName for IndexerService {
    fn as_str(&self) -> &'static str {
        match self {
            Self::ClickHouseDatalake => "clickhouse_datalake",
            Self::ClickHouseGraph => "clickhouse_graph",
            Self::Nats => "nats",
            Self::Rails => "rails",
        }
    }
}

fn to_circuit_config(config: &ServiceCircuitBreakerConfig) -> CircuitConfig {
    CircuitConfig {
        failure_threshold: config.failure_threshold,
        window: Duration::from_secs(config.window_secs),
        cooldown: Duration::from_secs(config.cooldown_secs),
    }
}

pub fn build_registry(
    settings: &CircuitBreakerConfig,
    observer: Arc<dyn CircuitBreakerObserver>,
) -> CircuitBreakerRegistry {
    let configs: HashMap<&'static str, CircuitConfig> = HashMap::from([
        (
            IndexerService::ClickHouseDatalake.as_str(),
            to_circuit_config(&settings.clickhouse_datalake),
        ),
        (
            IndexerService::ClickHouseGraph.as_str(),
            to_circuit_config(&settings.clickhouse_graph),
        ),
        (
            IndexerService::Nats.as_str(),
            to_circuit_config(&settings.nats),
        ),
        (
            IndexerService::Rails.as_str(),
            to_circuit_config(&settings.rails),
        ),
    ]);

    CircuitBreakerRegistry::new(configs, observer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_registry_creates_all_services() {
        let settings = CircuitBreakerConfig::default();
        let registry = build_registry(&settings, Arc::new(circuit_breaker::NoopObserver));

        assert!(registry.is_available(IndexerService::ClickHouseDatalake));
        assert!(registry.is_available(IndexerService::ClickHouseGraph));
        assert!(registry.is_available(IndexerService::Nats));
        assert!(registry.is_available(IndexerService::Rails));
        assert!(registry.unavailable_services().is_empty());
    }

    #[test]
    fn config_conversion_preserves_values() {
        let config = ServiceCircuitBreakerConfig {
            failure_threshold: 10,
            window_secs: 45,
            cooldown_secs: 120,
        };
        let circuit_config = to_circuit_config(&config);

        assert_eq!(circuit_config.failure_threshold, 10);
        assert_eq!(circuit_config.window, Duration::from_secs(45));
        assert_eq!(circuit_config.cooldown, Duration::from_secs(120));
    }
}
