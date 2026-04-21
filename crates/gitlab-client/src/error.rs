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
}
