use circuit_breaker::{CircuitBreakerObserver, StateLabel, Transition};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge};

use crate::MetricSpec;

pub mod labels {
    pub const SERVICE: &str = "service";
    pub const STATE: &str = "state";
}

const DOMAIN: &str = "circuit_breaker";

pub const STATE_TRANSITIONS: MetricSpec = MetricSpec::counter(
    "gkg.circuit_breaker.state.transitions",
    "Circuit breaker state transitions, labelled by service and target state.",
    None,
    &[labels::SERVICE, labels::STATE],
    DOMAIN,
);

pub const CALLS_REJECTED: MetricSpec = MetricSpec::counter(
    "gkg.circuit_breaker.calls.rejected",
    "Calls rejected because the circuit was open.",
    None,
    &[labels::SERVICE],
    DOMAIN,
);

pub const CALLS_SUCCESS: MetricSpec = MetricSpec::counter(
    "gkg.circuit_breaker.calls.success",
    "Successful calls through the circuit breaker.",
    None,
    &[labels::SERVICE],
    DOMAIN,
);

pub const CALLS_FAILURE: MetricSpec = MetricSpec::counter(
    "gkg.circuit_breaker.calls.failure",
    "Failed calls that counted toward the circuit breaker threshold.",
    None,
    &[labels::SERVICE],
    DOMAIN,
);

pub const CURRENT_STATE: MetricSpec = MetricSpec::gauge(
    "gkg.circuit_breaker.state.current",
    "Current circuit breaker state per service (0=closed, 1=half_open, 2=open).",
    None,
    &[labels::SERVICE],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[
    &STATE_TRANSITIONS,
    &CALLS_REJECTED,
    &CALLS_SUCCESS,
    &CALLS_FAILURE,
    &CURRENT_STATE,
];

pub struct MetricsObserver {
    state_transitions: Counter<u64>,
    calls_rejected: Counter<u64>,
    calls_success: Counter<u64>,
    calls_failure: Counter<u64>,
    current_state: Gauge<f64>,
}

impl Default for MetricsObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsObserver {
    pub fn new() -> Self {
        let meter = crate::meter();
        Self {
            state_transitions: STATE_TRANSITIONS.build_counter_u64(&meter),
            calls_rejected: CALLS_REJECTED.build_counter_u64(&meter),
            calls_success: CALLS_SUCCESS.build_counter_u64(&meter),
            calls_failure: CALLS_FAILURE.build_counter_u64(&meter),
            current_state: CURRENT_STATE.build_gauge_f64(&meter),
        }
    }
}

fn state_to_f64(state: StateLabel) -> f64 {
    match state {
        StateLabel::Closed => 0.0,
        StateLabel::HalfOpen => 1.0,
        StateLabel::Open => 2.0,
    }
}

impl CircuitBreakerObserver for MetricsObserver {
    fn on_state_change(&self, service: &str, transition: Transition) {
        self.state_transitions.add(
            1,
            &[
                KeyValue::new(labels::SERVICE, service.to_owned()),
                KeyValue::new(labels::STATE, transition.to.as_str()),
            ],
        );
    }

    fn on_call_rejected(&self, service: &str) {
        self.calls_rejected
            .add(1, &[KeyValue::new(labels::SERVICE, service.to_owned())]);
    }

    fn on_call_success(&self, service: &str) {
        self.calls_success
            .add(1, &[KeyValue::new(labels::SERVICE, service.to_owned())]);
    }

    fn on_call_failure(&self, service: &str) {
        self.calls_failure
            .add(1, &[KeyValue::new(labels::SERVICE, service.to_owned())]);
    }

    fn on_state_snapshot(&self, service: &str, state: StateLabel) {
        self.current_state.record(
            state_to_f64(state),
            &[KeyValue::new(labels::SERVICE, service.to_owned())],
        );
    }
}
