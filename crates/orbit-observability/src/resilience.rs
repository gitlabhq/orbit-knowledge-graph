use std::collections::HashMap;
use std::sync::Arc;

use circuit_breaker::{
    CircuitBreaker, CircuitBreakerObserver, CircuitBreakerRegistry, ServiceName, StateLabel,
    Transition,
};
use opentelemetry::metrics::Counter;
use opentelemetry::{Key, KeyValue};

use crate::MetricSpec;

pub mod labels {
    pub const SERVICE: &str = "service";
    pub const FROM: &str = "from";
    pub const TO: &str = "to";
    pub const OUTCOME: &str = "outcome";
}

const DOMAIN: &str = "resilience.circuit_breaker";

pub const STATE_TRANSITIONS: MetricSpec = MetricSpec::counter(
    "gkg.circuit_breaker.state.transitions",
    "Circuit-breaker state transitions, labelled by service and direction.",
    None,
    &[labels::SERVICE, labels::FROM, labels::TO],
    DOMAIN,
);

pub const CALLS_REJECTED: MetricSpec = MetricSpec::counter(
    "gkg.circuit_breaker.calls.rejected",
    "Calls rejected by an open circuit breaker.",
    None,
    &[labels::SERVICE],
    DOMAIN,
);

pub const CALLS: MetricSpec = MetricSpec::counter(
    "gkg.circuit_breaker.calls",
    "Classified call outcomes through the circuit breaker.",
    None,
    &[labels::SERVICE, labels::OUTCOME],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&STATE_TRANSITIONS, &CALLS_REJECTED, &CALLS];

const SERVICE_KEY: Key = Key::from_static_str(labels::SERVICE);
const FROM_KEY: Key = Key::from_static_str(labels::FROM);
const TO_KEY: Key = Key::from_static_str(labels::TO);
const OUTCOME_KEY: Key = Key::from_static_str(labels::OUTCOME);

#[derive(Clone, Copy)]
enum ExternalService {
    Graph,
    Datalake,
    Gitlab,
}

impl ServiceName for ExternalService {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Datalake => "datalake",
            Self::Gitlab => "gitlab",
        }
    }
}

#[derive(Clone)]
pub struct CircuitBreakers {
    pub graph: CircuitBreaker,
    pub datalake: CircuitBreaker,
    pub gitlab: CircuitBreaker,
}

impl CircuitBreakers {
    pub fn new() -> Self {
        let registry =
            CircuitBreakerRegistry::new(HashMap::new(), Arc::new(MetricsObserver::new()));
        Self {
            graph: registry.circuit_breaker(ExternalService::Graph),
            datalake: registry.circuit_breaker(ExternalService::Datalake),
            gitlab: registry.circuit_breaker(ExternalService::Gitlab),
        }
    }
}

impl Default for CircuitBreakers {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MetricsObserver {
    state_transitions: Counter<u64>,
    calls_rejected: Counter<u64>,
    calls: Counter<u64>,
}

impl MetricsObserver {
    pub fn new() -> Self {
        let meter = crate::meter();
        Self {
            state_transitions: STATE_TRANSITIONS.build_counter_u64(&meter),
            calls_rejected: CALLS_REJECTED.build_counter_u64(&meter),
            calls: CALLS.build_counter_u64(&meter),
        }
    }
}

impl Default for MetricsObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl CircuitBreakerObserver for MetricsObserver {
    fn on_state_change(&self, service: &str, transition: Transition) {
        self.state_transitions.add(
            1,
            &[
                KeyValue::new(SERVICE_KEY.clone(), service.to_owned()),
                KeyValue::new(FROM_KEY.clone(), transition.from.as_str()),
                KeyValue::new(TO_KEY.clone(), transition.to.as_str()),
            ],
        );

        match transition.to {
            StateLabel::Open => {
                tracing::warn!(
                    service,
                    from = transition.from.as_str(),
                    "circuit breaker opened"
                );
            }
            StateLabel::Closed => {
                tracing::info!(
                    service,
                    from = transition.from.as_str(),
                    "circuit breaker closed"
                );
            }
            StateLabel::HalfOpen => {
                tracing::info!(
                    service,
                    from = transition.from.as_str(),
                    "circuit breaker half-open"
                );
            }
        }
    }

    fn on_call_rejected(&self, service: &str) {
        self.calls_rejected
            .add(1, &[KeyValue::new(SERVICE_KEY.clone(), service.to_owned())]);
    }

    fn on_call_success(&self, service: &str) {
        self.calls.add(
            1,
            &[
                KeyValue::new(SERVICE_KEY.clone(), service.to_owned()),
                KeyValue::new(OUTCOME_KEY.clone(), "success"),
            ],
        );
    }

    fn on_call_failure(&self, service: &str) {
        self.calls.add(
            1,
            &[
                KeyValue::new(SERVICE_KEY.clone(), service.to_owned()),
                KeyValue::new(OUTCOME_KEY.clone(), "failure"),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fail_until_open(breaker: &CircuitBreaker) {
        while breaker.is_available() {
            let _ = breaker.call(|| async { Err::<(), _>("down") }).await;
        }
    }

    #[tokio::test]
    async fn each_service_has_its_own_circuit_shared_by_every_clone() {
        let breakers = CircuitBreakers::new();
        let clone = breakers.clone();

        fail_until_open(&breakers.graph).await;

        assert!(!clone.graph.is_available());
        assert!(clone.datalake.is_available());
        assert!(clone.gitlab.is_available());
    }
}
