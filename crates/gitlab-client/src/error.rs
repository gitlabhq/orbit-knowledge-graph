use circuit_breaker::CircuitBreakerError;

#[derive(Debug, thiserror::Error)]
pub enum GitlabClientError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("unauthorized (401) — check JWT secret")]
    Unauthorized,

    #[error("project {0} not found (404)")]
    NotFound(i64),

    #[error("server error for project {project_id}: status {status}")]
    ServerError { project_id: i64, status: u16 },

    #[error("force push detected for project {0}")]
    ForcePush(i64),

    #[error("JWT signing failed: {0}")]
    JwtSigning(String),

    #[error("invalid base64 in JWT secret: {0}")]
    InvalidSecret(#[from] base64::DecodeError),

    #[error("unexpected response: {0}")]
    Unexpected(String),

    #[error("circuit open for service {service}")]
    CircuitOpen { service: &'static str },
}

impl GitlabClientError {
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Request(_) | Self::ServerError { .. } => true,
            Self::Unauthorized
            | Self::NotFound(_)
            | Self::ForcePush(_)
            | Self::JwtSigning(_)
            | Self::InvalidSecret(_)
            | Self::Unexpected(_)
            | Self::CircuitOpen { .. } => false,
        }
    }

    pub(crate) fn from_circuit_breaker(error: CircuitBreakerError<Self>) -> Self {
        match error {
            CircuitBreakerError::Open { service } => Self::CircuitOpen { service },
            CircuitBreakerError::Inner(inner) => inner,
        }
    }
}
