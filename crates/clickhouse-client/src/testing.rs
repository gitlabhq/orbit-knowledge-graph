use std::sync::Arc;
use std::time::Duration;

use circuit_breaker::{CircuitBreaker, CircuitConfig, NoopObserver};

pub(crate) const UNREACHABLE_URL: &str = "http://127.0.0.1:1";

pub(crate) fn circuit_breaker(failure_threshold: u32) -> CircuitBreaker {
    let config = CircuitConfig {
        failure_threshold,
        window: Duration::from_secs(10),
        cooldown: Duration::from_secs(60),
    };
    CircuitBreaker::new("clickhouse-test", config, Arc::new(NoopObserver))
}
