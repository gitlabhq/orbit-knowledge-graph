use thiserror::Error;

#[derive(Debug, Error)]
pub enum SessionGraphError {
    #[error("database error: {0}")]
    Database(#[from] duckdb::Error),

    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("query compilation error: {0}")]
    Compilation(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, SessionGraphError>;
