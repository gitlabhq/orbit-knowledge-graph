use std::fmt;

#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    /// Call was rejected without executing.
    Open { service: &'static str },
    /// The wrapped call returned an error.
    Inner(E),
}

impl<E: fmt::Display> fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { service } => write!(f, "circuit open for service '{service}'"),
            Self::Inner(error) => error.fmt(f),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for CircuitBreakerError<E> {}

impl<E> CircuitBreakerError<E> {
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open { .. })
    }

    pub fn into_inner(self) -> Option<E> {
        match self {
            Self::Inner(error) => Some(error),
            _ => None,
        }
    }
}
