use circuit_breaker::CircuitBreakerError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClickHouseError {
    #[error("query error: {0}")]
    Query(#[source] clickhouse::error::Error),

    #[error("insert error: {0}")]
    Insert(#[source] clickhouse::error::Error),

    #[error("arrow decode error: {0}")]
    ArrowDecode(#[source] arrow::error::ArrowError),

    #[error("arrow encode error: {0}")]
    ArrowEncode(#[source] arrow::error::ArrowError),

    #[error("bad response ({status}): {body}")]
    BadResponse { status: u16, body: String },

    #[error("circuit open for service {service}")]
    CircuitOpen { service: &'static str },
}

fn is_clickhouse_error_transient(error: &clickhouse::error::Error) -> bool {
    match error {
        clickhouse::error::Error::Network(_) | clickhouse::error::Error::TimedOut => true,
        clickhouse::error::Error::BadResponse(message) => is_memory_limit_exceeded(message),
        _ => false,
    }
}

// ClickHouse error code 241 (MEMORY_LIMIT_EXCEEDED). Repeated OOMs signal
// server-level memory pressure worth backing off from.
fn is_memory_limit_exceeded(message: &str) -> bool {
    message.contains("MEMORY_LIMIT_EXCEEDED")
}

impl ClickHouseError {
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Query(inner) | Self::Insert(inner) => is_clickhouse_error_transient(inner),
            Self::BadResponse { status, .. } => *status >= 500,
            _ => false,
        }
    }

    pub(crate) fn from_circuit_breaker(error: CircuitBreakerError<Self>) -> Self {
        match error {
            CircuitBreakerError::Open { service } => Self::CircuitOpen { service },
            CircuitBreakerError::Inner(inner) => inner,
        }
    }
}
