use circuit_breaker::CircuitBreakableError;
use thiserror::Error;

#[derive(Debug, Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
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

    #[error("column decode error: {0}")]
    Extract(#[from] crate::extract::ExtractError),

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

fn is_memory_limit_exceeded(message: &str) -> bool {
    message.contains("MEMORY_LIMIT_EXCEEDED")
}

const TOO_MANY_PARTS: &str = "TOO_MANY_PARTS";
const QUERY_SLOT_REJECTED: &str = "TOO_MANY_SIMULTANEOUS_QUERIES";

// Only the server-wide memory tracker adds `current RSS` to MEMORY_LIMIT_EXCEEDED.
// A query or user limit ("Query memory limit exceeded") is the caller's problem.
fn is_server_memory_limit(message: &str) -> bool {
    is_memory_limit_exceeded(message) && message.contains("current RSS")
}

fn is_server_degraded(message: &str) -> bool {
    is_server_memory_limit(message) || message.contains(TOO_MANY_PARTS)
}

const UNKNOWN_TABLE_CODE: &str = "(UNKNOWN_TABLE)";

impl ClickHouseError {
    fn server_message(&self) -> Option<&str> {
        match self {
            Self::Query(clickhouse::error::Error::BadResponse(message))
            | Self::Insert(clickhouse::error::Error::BadResponse(message)) => Some(message),
            Self::BadResponse { body, .. } => Some(body),
            _ => None,
        }
    }

    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Query(inner) | Self::Insert(inner) => is_clickhouse_error_transient(inner),
            Self::BadResponse { status, .. } => *status >= 500,
            _ => false,
        }
    }

    pub fn is_server_overload(&self) -> bool {
        self.server_message().is_some_and(|message| {
            is_server_degraded(message) || message.contains(QUERY_SLOT_REJECTED)
        })
    }

    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::CircuitOpen { .. }) || self.is_server_overload()
    }

    pub fn is_unknown_table(&self) -> bool {
        match self {
            Self::Query(clickhouse::error::Error::BadResponse(message)) => {
                message.contains(UNKNOWN_TABLE_CODE)
            }
            Self::BadResponse { body, .. } => body.contains(UNKNOWN_TABLE_CODE),
            _ => false,
        }
    }
}

impl CircuitBreakableError for ClickHouseError {
    fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Query(clickhouse::error::Error::Network(_))
                | Self::Insert(clickhouse::error::Error::Network(_))
        ) || self.server_message().is_some_and(is_server_degraded)
    }

    fn circuit_open(service: &'static str) -> Self {
        Self::CircuitOpen { service }
    }
}
