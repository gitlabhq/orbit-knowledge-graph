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

/// ClickHouse exception names, as written at the end of a server error, and
/// their `error_type`. A name not in the list is a `server_error`.
const SERVER_ERROR_TYPES: [(&str, &str); 22] = [
    ("(MEMORY_LIMIT_EXCEEDED)", "memory_limit_exceeded"),
    ("(TOO_MANY_PARTS)", "too_many_parts"),
    (
        "(TOO_MANY_SIMULTANEOUS_QUERIES)",
        "too_many_simultaneous_queries",
    ),
    ("(TIMEOUT_EXCEEDED)", "timeout_exceeded"),
    ("(TOO_SLOW)", "too_slow"),
    ("(TOO_MANY_ROWS)", "too_many_rows"),
    ("(TOO_MANY_BYTES)", "too_many_bytes"),
    ("(TOO_MANY_ROWS_OR_BYTES)", "too_many_rows_or_bytes"),
    ("(QUERY_WAS_CANCELLED)", "query_was_cancelled"),
    (
        "(UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE)",
        "unsatisfied_quorum_for_previous_write",
    ),
    ("(TOO_FEW_LIVE_REPLICAS)", "too_few_live_replicas"),
    ("(REPLICA_IS_NOT_IN_QUORUM)", "replica_is_not_in_quorum"),
    ("(TABLE_IS_READ_ONLY)", "table_is_read_only"),
    ("(KEEPER_EXCEPTION)", "keeper_exception"),
    ("(NETWORK_ERROR)", "network_error"),
    ("(SOCKET_TIMEOUT)", "socket_timeout"),
    ("(UNKNOWN_TABLE)", "unknown_table"),
    ("(UNKNOWN_DATABASE)", "unknown_database"),
    ("(UNKNOWN_IDENTIFIER)", "unknown_identifier"),
    ("(SYNTAX_ERROR)", "syntax_error"),
    ("(AUTHENTICATION_FAILED)", "authentication_failed"),
    ("(ACCESS_DENIED)", "access_denied"),
];

fn server_error_type(message: &str) -> &'static str {
    SERVER_ERROR_TYPES
        .iter()
        .find(|(name, _)| message.contains(name))
        .map_or("server_error", |(_, error_type)| error_type)
}

fn client_error_type(error: &clickhouse::error::Error) -> Option<&'static str> {
    match error {
        clickhouse::error::Error::Network(_) => Some("network"),
        clickhouse::error::Error::TimedOut => Some("timed_out"),
        clickhouse::error::Error::DataFormat(_) => Some("data_format"),
        clickhouse::error::Error::BadResponse(message) => Some(server_error_type(message)),
        _ => None,
    }
}

/// The cause of the error for the `error_type` log field. It never holds the error text.
impl From<&ClickHouseError> for &'static str {
    fn from(error: &ClickHouseError) -> Self {
        match error {
            ClickHouseError::Query(inner) => client_error_type(inner).unwrap_or("query"),
            ClickHouseError::Insert(inner) => client_error_type(inner).unwrap_or("insert"),
            ClickHouseError::ArrowDecode(_) => "arrow_decode",
            ClickHouseError::ArrowEncode(_) => "arrow_encode",
            ClickHouseError::BadResponse { body, .. } => server_error_type(body),
            ClickHouseError::CircuitOpen { .. } => "circuit_open",
        }
    }
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

const SERVER_OVERLOAD_CODES: [&str; 2] = ["TOO_MANY_PARTS", "TOO_MANY_SIMULTANEOUS_QUERIES"];

// Only the server-wide memory tracker adds `current RSS` to MEMORY_LIMIT_EXCEEDED.
// A query or user limit ("Query memory limit exceeded") is the caller's problem.
fn is_server_memory_limit(message: &str) -> bool {
    is_memory_limit_exceeded(message) && message.contains("current RSS")
}

fn is_server_overload_message(message: &str) -> bool {
    is_server_memory_limit(message)
        || SERVER_OVERLOAD_CODES
            .iter()
            .any(|code| message.contains(code))
}

fn is_server_unavailable(error: &clickhouse::error::Error) -> bool {
    match error {
        clickhouse::error::Error::Network(_) => true,
        clickhouse::error::Error::BadResponse(message) => is_server_overload_message(message),
        _ => false,
    }
}

const UNKNOWN_TABLE_CODE: &str = "(UNKNOWN_TABLE)";

impl ClickHouseError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Query(inner) | Self::Insert(inner) => is_clickhouse_error_transient(inner),
            Self::BadResponse { status, .. } => *status >= 500,
            _ => false,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn network() -> clickhouse::error::Error {
        clickhouse::error::Error::Network(Box::new(std::io::Error::other("customer text")))
    }

    fn server(message: &str) -> clickhouse::error::Error {
        clickhouse::error::Error::BadResponse(message.to_owned())
    }

    #[test]
    fn error_type_names_the_cause_and_never_the_text() {
        let cases = [
            (ClickHouseError::Query(network()), "network"),
            (
                ClickHouseError::Insert(clickhouse::error::Error::TimedOut),
                "timed_out",
            ),
            (
                ClickHouseError::Query(server(
                    "Code: 241. DB::Exception: Memory limit (total) exceeded: customer text. (MEMORY_LIMIT_EXCEEDED)",
                )),
                "memory_limit_exceeded",
            ),
            (
                ClickHouseError::Insert(server(
                    "Code: 252. DB::Exception: Too many parts (300). (TOO_MANY_PARTS)",
                )),
                "too_many_parts",
            ),
            (
                ClickHouseError::Query(server(
                    "Code: 62. DB::Exception: Syntax error near (SECRET_TEXT). (SYNTAX_ERROR)",
                )),
                "syntax_error",
            ),
            (
                ClickHouseError::Query(server("Code: 9999. DB::Exception: (CUSTOMER_TEXT)")),
                "server_error",
            ),
            (
                ClickHouseError::Query(clickhouse::error::Error::Custom("customer text".into())),
                "query",
            ),
            (
                ClickHouseError::BadResponse {
                    status: 500,
                    body: "Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)".into(),
                },
                "timeout_exceeded",
            ),
            (
                ClickHouseError::CircuitOpen { service: "graph" },
                "circuit_open",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(<&str>::from(&error), expected, "{error}");
        }
    }
}
