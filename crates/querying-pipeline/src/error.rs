use std::fmt;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Security context error: {0}")]
    Security(String),

    #[error("Query compilation failed: {0}")]
    Compile(String),

    #[error("Query execution failed: {0}")]
    Execution(String),

    #[error("Authorization failed: {0}")]
    Authorization(String),

    #[error("Streaming channel not available: {0}")]
    Streaming(String),

    #[error("{0}")]
    Custom(Box<dyn std::error::Error + Send + Sync>),
}

impl PipelineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Security(_) => "security_error",
            Self::Compile(_) => "compile_error",
            Self::Execution(_) => "execution_error",
            Self::Authorization(_) => "authorization_error",
            Self::Streaming(_) => "streaming_error",
            Self::Custom(_) => "custom_error",
        }
    }

    pub fn custom(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Custom(err.into())
    }
}

impl fmt::Display for SecurityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug)]
pub struct SecurityError(pub String);

impl std::error::Error for SecurityError {}
