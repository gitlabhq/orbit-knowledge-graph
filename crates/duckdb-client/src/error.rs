use thiserror::Error;

pub type Result<T> = std::result::Result<T, DuckDbError>;

#[derive(Debug, Error)]
pub enum DuckDbError {
    #[error("database error: {0}")]
    Database(#[from] duckdb::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("schema error: {0}")]
    Schema(String),
}
