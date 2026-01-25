use thiserror::Error;

#[derive(Debug, Error)]
pub enum GitalyError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("RPC failed: {0}")]
    Rpc(#[from] tonic::Status),

    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("archive extraction failed: {0}")]
    Archive(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("invalid token: {0}")]
    InvalidToken(String),
}
