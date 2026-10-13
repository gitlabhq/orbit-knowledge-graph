use std::collections::HashMap;
use std::time::Duration;

use circuit_breaker::{CircuitBreaker, CircuitBreakerRegistry, CircuitConfig, ServiceName};

pub(crate) const UNREACHABLE_URL: &str = "http://127.0.0.1:1";

struct TestService;

impl ServiceName for TestService {
    fn as_str(&self) -> &'static str {
        "clickhouse-test"
    }
}

pub(crate) fn circuit_breaker(failure_threshold: u32) -> CircuitBreaker {
    let configs = HashMap::from([(
        TestService.as_str(),
        CircuitConfig {
            failure_threshold,
            window: Duration::from_secs(10),
            cooldown: Duration::from_secs(60),
        },
    )]);
    CircuitBreakerRegistry::without_observer(configs).circuit_breaker(TestService)
}
