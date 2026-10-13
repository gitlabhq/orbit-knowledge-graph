use circuit_breaker::CircuitBreakableError;
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

// A per-query or per-user memory limit is the caller's problem. Only the
// server-wide `(total)` limit means the server itself is overloaded.
const SERVER_OVERLOAD_MARKERS: [&str; 3] = [
    "TOO_MANY_PARTS",
    "TOO_MANY_SIMULTANEOUS_QUERIES",
    "Memory limit (total) exceeded",
];

fn is_server_overload_message(message: &str) -> bool {
    SERVER_OVERLOAD_MARKERS
        .iter()
        .any(|marker| message.contains(marker))
}

fn is_server_unavailable(error: &clickhouse::error::Error) -> bool {
    match error {
        clickhouse::error::Error::Network(_) => true,
        clickhouse::error::Error::BadResponse(message) => is_server_overload_message(message),
        _ => false,
    }
}

impl ClickHouseError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Query(inner) | Self::Insert(inner) => is_clickhouse_error_transient(inner),
            Self::BadResponse { status, .. } => *status >= 500,
            _ => false,
        }
    }
}

impl CircuitBreakableError for ClickHouseError {
    fn is_transient(&self) -> bool {
        match self {
            Self::Query(inner) | Self::Insert(inner) => is_server_unavailable(inner),
            Self::BadResponse { body, .. } => is_server_overload_message(body),
            _ => false,
        }
    }

    fn circuit_open(service: &'static str) -> Self {
        Self::CircuitOpen { service }
    }
}
