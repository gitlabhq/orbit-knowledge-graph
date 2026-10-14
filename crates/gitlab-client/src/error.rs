use circuit_breaker::CircuitBreakableError;

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

    #[error("rate limited (429) for project {project_id}{}", retry_after_suffix(*retry_after_secs))]
    RateLimited {
        project_id: i64,
        retry_after_secs: Option<u64>,
    },

    #[error("force push detected for project {0}")]
    ForcePush(i64),

    #[error("JWT signing failed: {0}")]
    JwtSigning(String),

    #[error("JWT decoding failed: {0}")]
    JwtDecoding(String),

    #[error("invalid base64 in JWT secret: {0}")]
    InvalidSecret(#[from] base64::DecodeError),

    #[error("unexpected response: {0}")]
    Unexpected(String),

    #[error("circuit open for service {service}")]
    CircuitOpen { service: &'static str },
}

fn retry_after_suffix(retry_after_secs: Option<u64>) -> String {
    retry_after_secs
        .map(|secs| format!("; retry after {secs}s"))
        .unwrap_or_default()
}

impl CircuitBreakableError for GitlabClientError {
    fn is_transient(&self) -> bool {
        match self {
            Self::Request(_) => true,
            Self::ServerError { .. }
            | Self::RateLimited { .. }
            | Self::Unauthorized
            | Self::NotFound(_)
            | Self::ForcePush(_)
            | Self::JwtSigning(_)
            | Self::JwtDecoding(_)
            | Self::InvalidSecret(_)
            | Self::Unexpected(_)
            | Self::CircuitOpen { .. } => false,
        }
    }

    fn circuit_open(service: &'static str) -> Self {
        Self::CircuitOpen { service }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limited_message_mentions_retry_after_only_when_present() {
        let with = GitlabClientError::RateLimited {
            project_id: 1,
            retry_after_secs: Some(30),
        };
        let without = GitlabClientError::RateLimited {
            project_id: 1,
            retry_after_secs: None,
        };
        assert_eq!(
            with.to_string(),
            "rate limited (429) for project 1; retry after 30s"
        );
        assert_eq!(without.to_string(), "rate limited (429) for project 1");
    }
}
