use std::fmt;

/// Task-level failures from the v2 code-graph pipeline. Per-file
/// failures route through [`FileFault`] instead.
#[derive(Debug)]
pub enum CodeGraphError {
    ThreadPoolCreation {
        language: String,
        source: rayon::ThreadPoolBuildError,
    },
    SentinelSpawn {
        source: std::io::Error,
    },
    UnexpectedNodeType {
        expected: &'static str,
        got: String,
    },
    ArrowConversion {
        message: String,
    },
    SinkWrite {
        table: String,
        message: String,
    },
    Internal {
        context: String,
        message: String,
    },
}

impl fmt::Display for CodeGraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::ThreadPoolCreation { .. } => "thread_pool",
            Self::SentinelSpawn { .. } => "sentinel",
            Self::UnexpectedNodeType { .. } => "graph_node",
            Self::ArrowConversion { .. } => "arrow_conversion",
            Self::SinkWrite { .. } => "sink_write",
            Self::Internal { .. } => "internal",
        }
    }

    /// Pseudo-path used as the `file_path` field on `PipelineError` so
    /// task-level errors are distinguishable from per-file ones in logs.
    pub fn scope(&self) -> &'static str {
        match self {
            Self::ThreadPoolCreation { .. } => "<thread-pool>",
            Self::SentinelSpawn { .. } => "<sentinel>",
            Self::UnexpectedNodeType { .. } => "<graph>",
            Self::ArrowConversion { .. } => "<arrow>",
            Self::SinkWrite { .. } => "<sink>",
            Self::Internal { .. } => "<internal>",
        }
    }
}

/// Per-file benign skip. `as_metric_label` returns the wire-stable label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileSkip {
    Oversize,
    OversizeCombined,
    LineTooLong,
    Minified,
    NotUtf8,
    NonRegularFile,
    UnsafePath,
    TimeoutSentinel,
}

impl FileSkip {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::Oversize => "oversize",
            Self::OversizeCombined => "oversize_combined",
            Self::LineTooLong => "line_too_long",
            Self::Minified => "minified",
            Self::NotUtf8 => "not_utf8",
            Self::NonRegularFile => "non_regular_file",
            Self::UnsafePath => "unsafe_path",
            Self::TimeoutSentinel => "timeout_sentinel",
        }
    }
}

impl fmt::Display for FileSkip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_metric_label())
    }
}

/// Per-file genuine failure. The task itself completes; the file is
/// excluded from the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFault {
    FileRead,
    InvalidUtf8,
    SyntaxError,
    OxcPanic,
    OxcSemantic,
    AnalyzerPanic,
    UnknownSourceType,
    EmbeddedScriptParse,
    RustWorkspaceMissing,
}

impl FileFault {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::FileRead => "file_read",
            Self::InvalidUtf8 => "invalid_utf8",
            Self::SyntaxError => "syntax_error",
            Self::OxcPanic => "oxc_panic",
            Self::OxcSemantic => "oxc_semantic",
            Self::AnalyzerPanic => "analyzer_panic",
            Self::UnknownSourceType => "unknown_source_type",
            Self::EmbeddedScriptParse => "embedded_script_parse",
            Self::RustWorkspaceMissing => "rust_workspace_missing",
        }
    }
}

impl fmt::Display for FileFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_metric_label())
    }
}

/// Per-file outcome from a language analyzer. Encodes skip-vs-fault
/// at the type level so callers route by variant, not by string match.
#[derive(Debug)]
pub enum AnalyzerError {
    Skip { kind: FileSkip, detail: String },
    Fault { kind: FileFault, detail: String },
}

impl fmt::Display for AnalyzerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Skip { kind, detail } => write!(f, "skip ({kind}): {detail}"),
            Self::Fault { kind, detail } => write!(f, "fault ({kind}): {detail}"),
        }
    }
}

impl AnalyzerError {
    pub fn skip(kind: FileSkip, detail: impl Into<String>) -> Self {
        Self::Skip {
            kind,
            detail: detail.into(),
        }
    }

    pub fn fault(kind: FileFault, detail: impl Into<String>) -> Self {
        Self::Fault {
            kind,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SkippedFile {
    pub path: String,
    pub kind: FileSkip,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct FaultedFile {
    pub path: String,
    pub kind: FileFault,
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_skip_labels_are_stable() {
        assert_eq!(FileSkip::Oversize.as_metric_label(), "oversize");
        assert_eq!(
            FileSkip::OversizeCombined.as_metric_label(),
            "oversize_combined"
        );
        assert_eq!(FileSkip::LineTooLong.as_metric_label(), "line_too_long");
        assert_eq!(FileSkip::Minified.as_metric_label(), "minified");
        assert_eq!(FileSkip::NotUtf8.as_metric_label(), "not_utf8");
        assert_eq!(
            FileSkip::NonRegularFile.as_metric_label(),
            "non_regular_file"
        );
        assert_eq!(FileSkip::UnsafePath.as_metric_label(), "unsafe_path");
        assert_eq!(
            FileSkip::TimeoutSentinel.as_metric_label(),
            "timeout_sentinel"
        );
    }

    #[test]
    fn file_fault_labels_are_stable() {
        assert_eq!(FileFault::FileRead.as_metric_label(), "file_read");
        assert_eq!(FileFault::InvalidUtf8.as_metric_label(), "invalid_utf8");
        assert_eq!(FileFault::SyntaxError.as_metric_label(), "syntax_error");
        assert_eq!(FileFault::OxcPanic.as_metric_label(), "oxc_panic");
        assert_eq!(FileFault::OxcSemantic.as_metric_label(), "oxc_semantic");
        assert_eq!(FileFault::AnalyzerPanic.as_metric_label(), "analyzer_panic");
        assert_eq!(
            FileFault::UnknownSourceType.as_metric_label(),
            "unknown_source_type"
        );
        assert_eq!(
            FileFault::EmbeddedScriptParse.as_metric_label(),
            "embedded_script_parse"
        );
        assert_eq!(
            FileFault::RustWorkspaceMissing.as_metric_label(),
            "rust_workspace_missing"
        );
    }
}
