use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing authorization header")]
    MissingHeader,
    #[error("invalid authorization format")]
    InvalidFormat,
    #[error("invalid token")]
    InvalidToken,
    #[error("token expired")]
    TokenExpired,
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}
