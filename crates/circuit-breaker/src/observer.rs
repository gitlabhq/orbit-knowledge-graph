use crate::state::{StateLabel, Transition};

/// Metrics/logging hook. All methods default to no-ops.
pub trait CircuitBreakerObserver: Send + Sync {
    fn on_state_change(&self, service: &str, transition: Transition) {
        let _ = (service, transition);
    }

    fn on_call_rejected(&self, service: &str) {
        let _ = service;
    }

    fn on_call_success(&self, service: &str) {
        let _ = service;
    }

    fn on_call_failure(&self, service: &str) {
        let _ = service;
    }

    fn on_state_snapshot(&self, service: &str, state: StateLabel) {
        let _ = (service, state);
    }
}

pub struct NoopObserver;

impl CircuitBreakerObserver for NoopObserver {}
