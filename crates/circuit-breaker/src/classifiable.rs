use crate::error::CircuitBreakerError;

pub trait CircuitBreakableError: Sized {
    fn is_transient(&self) -> bool;

    fn circuit_open(service: &'static str) -> Self;

    fn from_circuit_breaker(error: CircuitBreakerError<Self>) -> Self {
        match error {
            CircuitBreakerError::Open { service } => Self::circuit_open(service),
            CircuitBreakerError::Inner(inner) => inner,
        }
    }
}
