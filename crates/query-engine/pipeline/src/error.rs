use compiler::{QueryError, RejectionReason};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Security context error: {0}")]
    Security(String),

    #[error("Query compilation failed: {message}")]
    Compile {
        message: String,
        /// When true the message only describes user-input problems
        /// (parse/schema/reference/pagination/limit errors) and is safe
        /// to return to clients verbatim.
        client_safe: bool,
        reason: RejectionReason,
    },

    #[error("No enabled namespaces for this user")]
    NoEnabledNamespaces,

    #[error("Query execution failed: {0}")]
    Execution(String),

    #[error("Authorization failed: {0}")]
    Authorization(String),

    #[error("Content resolution failed: {0}")]
    ContentResolution(String),

    #[error("Streaming channel not available: {0}")]
    Streaming(String),

    #[error("Query exceeded the configured stream timeout")]
    Timeout,

    #[error("{0}")]
    Custom(Box<dyn std::error::Error + Send + Sync>),
}

impl PipelineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Security(_) | Self::NoEnabledNamespaces => "security_error",
            Self::Compile { .. } => "compile_error",
            Self::Execution(_) => "execution_error",
            Self::Authorization(_) => "authorization_error",
            Self::ContentResolution(_) => "content_resolution_error",
            Self::Streaming(_) => "streaming_error",
            Self::Timeout => "timeout",
            Self::Custom(_) => "custom_error",
        }
    }

    pub fn custom(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Custom(err.into())
    }

    pub fn client_closed() -> Self {
        Self::Streaming("client closed the result stream".into())
    }

    pub fn is_caller_error(&self) -> bool {
        matches!(
            self,
            Self::Compile {
                client_safe: true,
                ..
            } | Self::NoEnabledNamespaces
        )
    }

    pub fn failure_reason(&self) -> FailureReason {
        match self {
            Self::Compile { reason, .. } => FailureReason::Rejected(*reason),
            Self::Execution(message) => ClickHouseLimit::from_message(message)
                .map_or(FailureReason::Execution, FailureReason::ClickHouse),
            Self::Security(_) => FailureReason::SecurityContext,
            Self::NoEnabledNamespaces => FailureReason::NoEnabledNamespaces,
            Self::Authorization(_) => FailureReason::Redaction,
            Self::ContentResolution(_) => FailureReason::ContentResolution,
            Self::Streaming(_) => FailureReason::Streaming,
            Self::Timeout => FailureReason::Timeout,
            Self::Custom(_) => FailureReason::Custom,
        }
    }
}

impl From<QueryError> for PipelineError {
    fn from(error: QueryError) -> Self {
        Self::Compile {
            client_safe: error.is_client_safe(),
            reason: error.rejection_reason(),
            message: error.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureReason {
    Rejected(RejectionReason),
    ClickHouse(ClickHouseLimit),
    NoEnabledNamespaces,
    SecurityContext,
    Execution,
    Redaction,
    ContentResolution,
    Streaming,
    Timeout,
    Custom,
}

impl FailureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rejected(reason) => reason.into(),
            Self::ClickHouse(limit) => limit.into(),
            Self::NoEnabledNamespaces => "no_enabled_namespaces",
            Self::SecurityContext => "security_context",
            Self::Execution => "execution",
            Self::Redaction => "redaction",
            Self::ContentResolution => "content_resolution",
            Self::Streaming => "streaming",
            Self::Timeout => "timeout",
            Self::Custom => "custom",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum ClickHouseLimit {
    MemoryLimit,
    TooSlow,
    TooManyBytes,
    TooManyRows,
    SetSizeLimit,
    TypeMismatch,
}

impl ClickHouseLimit {
    pub fn from_message(message: &str) -> Option<Self> {
        match clickhouse_error_code(message)? {
            241 => Some(Self::MemoryLimit),
            159 | 160 => Some(Self::TooSlow),
            307 => Some(Self::TooManyBytes),
            158 => Some(Self::TooManyRows),
            191 => Some(Self::SetSizeLimit),
            53 => Some(Self::TypeMismatch),
            _ => None,
        }
    }
}

fn clickhouse_error_code(message: &str) -> Option<u32> {
    let after = &message[message.find("Code: ")? + 6..];
    after
        .split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clickhouse_limit_reads_real_error_messages() {
        let prefix = "Query execution failed: query error: bad response: ";
        let cases = [
            ("Code: 159", Some(ClickHouseLimit::TooSlow)),
            (
                "Code: 159. DB::Exception: Timeout exceeded: elapsed 30038.950827 ms, maximum: 30000 ms. (TIMEOUT_EXCEEDED)",
                Some(ClickHouseLimit::TooSlow),
            ),
            (
                "Code: 241. DB::Exception: (total) memory limit exceeded: would use 32.41 GiB",
                Some(ClickHouseLimit::MemoryLimit),
            ),
            (
                "Code: 158. DB::Exception: Limit for rows (controlled by 'max_rows_to_read' setting) exceeded",
                Some(ClickHouseLimit::TooManyRows),
            ),
            (
                "Code: 307. DB::Exception: Limit for rows or bytes to read exceeded, max bytes: 1.00 B",
                Some(ClickHouseLimit::TooManyBytes),
            ),
            (
                "Code: 191. DB::Exception: Limit for IN",
                Some(ClickHouseLimit::SetSizeLimit),
            ),
            (
                "Code: 53. DB::Exception: Cannot convert string 'abc' to type UInt64. (TYPE_MISMATCH)",
                Some(ClickHouseLimit::TypeMismatch),
            ),
            (
                "Code: 47. DB::Exception: Unknown expression identifier `p.id`",
                None,
            ),
            ("connection refused", None),
        ];
        for (message, expected) in cases {
            assert_eq!(
                ClickHouseLimit::from_message(&format!("{prefix}{message}")),
                expected,
                "{message}"
            );
        }
    }
}
