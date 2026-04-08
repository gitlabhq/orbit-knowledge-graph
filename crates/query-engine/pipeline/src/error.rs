use thiserror::Error;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Security context error: {0}")]
    Security(String),

    #[error("Query compilation failed: {message}")]
    Compile {
        message: String,
        /// When true the message only describes user-input problems
        /// (parse/schema/reference/pagination/limit errors) and is safe
        /// to return to clients verbatim.
        client_safe: bool,
    },

    #[error("Query execution failed: {0}")]
    Execution(String),

    #[error("Authorization failed: {0}")]
    Authorization(String),

    #[error("Content resolution failed: {0}")]
    ContentResolution(String),

    #[error("Streaming channel not available: {0}")]
    Streaming(String),

    #[error("{0}")]
    Custom(Box<dyn std::error::Error + Send + Sync>),
}

impl PipelineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Security(_) => "security_error",
            Self::Compile { .. } => "compile_error",
            Self::Execution(_) => "execution_error",
            Self::Authorization(_) => "authorization_error",
            Self::ContentResolution(_) => "content_resolution_error",
            Self::Streaming(_) => "streaming_error",
            Self::Custom(_) => "custom_error",
        }
    }

    pub fn custom(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Custom(err.into())
    }
}
