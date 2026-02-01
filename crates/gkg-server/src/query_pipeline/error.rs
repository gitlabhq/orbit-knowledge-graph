use thiserror::Error;
use tonic::Status;

use crate::redaction::RedactionExchangeError;

use super::stages::SecurityError;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Security context error: {0}")]
    Security(#[from] SecurityError),

    #[error("Query compilation failed: {0}")]
    Compile(String),

    #[error("Query execution failed: {0}")]
    Execution(String),

    #[error("Authorization exchange failed")]
    Authorization(RedactionExchangeError),
}

impl PipelineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Security(_) => "security_error",
            Self::Compile(_) => "compile_error",
            Self::Execution(_) => "execution_error",
            Self::Authorization(_) => "authorization_error",
        }
    }

    pub fn into_status(self) -> Status {
        match self {
            Self::Security(e) => Status::permission_denied(e.to_string()),
            Self::Compile(msg) => Status::invalid_argument(msg),
            Self::Execution(msg) => Status::internal(msg),
            Self::Authorization(e) => e.into_status(),
        }
    }
}

impl From<RedactionExchangeError> for PipelineError {
    fn from(e: RedactionExchangeError) -> Self {
        Self::Authorization(e)
    }
}
