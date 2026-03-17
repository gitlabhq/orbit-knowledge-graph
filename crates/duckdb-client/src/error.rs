use thiserror::Error;

#[derive(Debug, Error)]
pub enum DuckDbError {
    #[error("DuckDB error: {0}")]
    Database(#[from] duckdb::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Schema initialization failed: {0}")]
    Schema(String),
}

pub type Result<T> = std::result::Result<T, DuckDbError>;
