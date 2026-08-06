use circuit_breaker::{CircuitBreakerObserver, StateLabel, Transition};
use opentelemetry::metrics::{Counter, Gauge};
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

pub const STATE: MetricSpec = MetricSpec::gauge(
    "gkg.circuit_breaker.state",
    "Current circuit-breaker state per service: 0 closed, 1 half-open, 2 open. \
     Pre-registered at closed on startup so alerts can match on the value \
     rather than on transition rates.",
    None,
    &[labels::SERVICE],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&STATE_TRANSITIONS, &CALLS_REJECTED, &CALLS, &STATE];

fn state_value(state: StateLabel) -> f64 {
    match state {
        StateLabel::Closed => 0.0,
        StateLabel::HalfOpen => 1.0,
        StateLabel::Open => 2.0,
    }
}

const SERVICE_KEY: Key = Key::from_static_str(labels::SERVICE);
const FROM_KEY: Key = Key::from_static_str(labels::FROM);
const TO_KEY: Key = Key::from_static_str(labels::TO);
const OUTCOME_KEY: Key = Key::from_static_str(labels::OUTCOME);

pub struct MetricsObserver {
    state_transitions: Counter<u64>,
    calls_rejected: Counter<u64>,
    calls: Counter<u64>,
    state: Gauge<f64>,
}

impl MetricsObserver {
    pub fn new() -> Self {
        let meter = crate::meter();
        Self {
            state_transitions: STATE_TRANSITIONS.build_counter_u64(&meter),
            calls_rejected: CALLS_REJECTED.build_counter_u64(&meter),
            calls: CALLS.build_counter_u64(&meter),
            state: STATE.build_gauge_f64(&meter),
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

    fn on_state_snapshot(&self, service: &str, state: StateLabel) {
        self.state.record(
            state_value(state),
            &[KeyValue::new(SERVICE_KEY.clone(), service.to_owned())],
        );
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

    #[test]
    fn state_values_are_ordered_by_badness() {
        assert_eq!(state_value(StateLabel::Closed), 0.0);
        assert_eq!(state_value(StateLabel::HalfOpen), 1.0);
        assert_eq!(state_value(StateLabel::Open), 2.0);
    }
}
