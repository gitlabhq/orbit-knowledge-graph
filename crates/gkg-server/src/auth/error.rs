use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing authorization header")]
    MissingHeader,
    #[error("invalid authorization format")]
    InvalidFormat,
    #[error("invalid token: {0}")]
    InvalidToken(String),
    #[error("token expired")]
    TokenExpired,
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("security context: {0}")]
    SecurityContext(String),
}
