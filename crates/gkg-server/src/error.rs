use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("invalid token: {0}")]
    InvalidToken(String),
    #[error("token expired")]
    TokenExpired,
    #[error("missing authentication")]
    MissingAuth,
    #[error("configuration error: {0}")]
    Config(String),
    #[error("server error: {0}")]
    Server(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tool not found: {0}")]
    ToolNotFound(String),
    #[error("tool execution failed: {0}")]
    ToolExecution(String),
}
