use std::fmt;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("missing required argument: {0}")]
    MissingArg(String),

    #[error("unknown command `{0}`")]
    UnknownCommand(String),

    #[error("command ordering: {0}")]
    StateError(String),

    #[error("query compilation failed: {0}")]
    CompileError(String),

    #[error("expected compile error but query succeeded")]
    ExpectedCompileError,

    #[error("{0}")]
    AssertionFailed(String),

    #[error("seed `{name}` not found at {path}")]
    SeedNotFound {
        name: String,
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Parse(String),

    #[error("failed to read {path}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Wraps `RunnerError` with file path and command context for readable test failures.
#[derive(Debug, Error)]
#[error("{}", match &self.command {
    Some(cmd) => format!("{} [{}]: {}", self.path, cmd, self.inner),
    None => format!("{}: {}", self.path, self.inner),
})]
pub struct LocatedError {
    pub path: String,
    pub command: Option<String>,
    #[source]
    pub inner: RunnerError,
}

pub type Result<T = ()> = std::result::Result<T, RunnerError>;

pub fn located(path: &str, command: Option<&str>, inner: RunnerError) -> LocatedError {
    LocatedError {
        path: path.to_string(),
        command: command.map(str::to_string),
        inner,
    }
}

pub fn assert_eq_result<T: PartialEq + fmt::Debug>(actual: T, expected: T, label: &str) -> Result {
    if actual != expected {
        return Err(RunnerError::AssertionFailed(format!(
            "{label}: expected {expected:?}, got {actual:?}"
        )));
    }
    Ok(())
}
