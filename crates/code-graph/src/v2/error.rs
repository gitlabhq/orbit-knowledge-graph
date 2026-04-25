use std::fmt;

/// Errors from the v2 code-graph pipeline.
///
/// Covers infrastructure failures (thread pools, I/O), per-file
/// processing errors, and graph invariant violations.
#[derive(Debug)]
pub enum CodeGraphError {
    /// A file could not be read from disk.
    FileRead {
        path: String,
        source: std::io::Error,
    },

    /// A file could not be parsed (tree-sitter / OXC / Prism failure).
    ParseFailed { path: String, message: String },

    /// A rayon thread pool could not be created (OS resource limits).
    ThreadPoolCreation {
        language: String,
        source: rayon::ThreadPoolBuildError,
    },

    /// The sentinel watchdog thread could not be spawned.
    SentinelSpawn { source: std::io::Error },

    /// A graph node was expected to be a Definition but wasn't.
    UnexpectedNodeType { expected: &'static str, got: String },

    /// Arrow conversion failed for a graph.
    ArrowConversion { message: String },

    /// A sink write failed.
    SinkWrite { table: String, message: String },

    /// Generic internal error with context.
    Internal { context: String, message: String },
}

impl fmt::Display for CodeGraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileRead { path, source } => {
                write!(f, "failed to read {path}: {source}")
            }
            Self::ParseFailed { path, message } => {
                write!(f, "failed to parse {path}: {message}")
            }
            Self::ThreadPoolCreation { language, source } => {
                write!(f, "failed to create thread pool for {language}: {source}")
            }
            Self::SentinelSpawn { source } => {
                write!(f, "failed to spawn sentinel thread: {source}")
            }
            Self::UnexpectedNodeType { expected, got } => {
                write!(f, "expected {expected} node, got {got}")
            }
            Self::ArrowConversion { message } => {
                write!(f, "arrow conversion failed: {message}")
            }
            Self::SinkWrite { table, message } => {
                write!(f, "sink write to {table} failed: {message}")
            }
            Self::Internal { context, message } => {
                write!(f, "{context}: {message}")
            }
        }
    }
}

impl std::error::Error for CodeGraphError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FileRead { source, .. } => Some(source),
            Self::SentinelSpawn { source, .. } => Some(source),
            Self::ThreadPoolCreation { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl CodeGraphError {
    /// Returns a stable stage label for metrics recording.
    pub fn stage(&self) -> &'static str {
        match self {
            Self::FileRead { .. } => "file_read",
            Self::ParseFailed { .. } => "parse",
            Self::ThreadPoolCreation { .. } => "thread_pool",
            Self::SentinelSpawn { .. } => "sentinel",
            Self::UnexpectedNodeType { .. } => "graph_node",
            Self::ArrowConversion { .. } => "arrow_conversion",
            Self::SinkWrite { .. } => "sink_write",
            Self::Internal { .. } => "internal",
        }
    }
}
