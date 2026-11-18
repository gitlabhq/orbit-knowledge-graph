use std::fmt;
use std::time::Duration;

pub struct TaskError(TaskErrorKind);

pub(crate) enum TaskErrorKind {
    Failed(anyhow::Error),
    Permanent(anyhow::Error),
    Defer {
        after: Duration,
        reason: &'static str,
    },
    Throttle {
        after: Duration,
        reason: &'static str,
    },
    Incompatible {
        reason: &'static str,
    },
}

impl TaskError {
    pub fn permanent(error: impl Into<anyhow::Error>) -> Self {
        Self(TaskErrorKind::Permanent(error.into()))
    }

    pub fn defer(after: Duration, reason: &'static str) -> Self {
        Self(TaskErrorKind::Defer { after, reason })
    }

    pub fn throttle(after: Duration, reason: &'static str) -> Self {
        Self(TaskErrorKind::Throttle { after, reason })
    }

    pub fn incompatible(reason: &'static str) -> Self {
        Self(TaskErrorKind::Incompatible { reason })
    }

    pub(crate) fn describe(&self) -> &'static str {
        match self.0 {
            TaskErrorKind::Failed(_) => "failed",
            TaskErrorKind::Permanent(_) => "permanent error",
            TaskErrorKind::Defer { .. } => "deferred",
            TaskErrorKind::Throttle { .. } => "throttled",
            TaskErrorKind::Incompatible { .. } => "incompatible",
        }
    }

    pub(crate) fn into_kind(self) -> TaskErrorKind {
        self.0
    }
}

impl<E: Into<anyhow::Error>> From<E> for TaskError {
    fn from(error: E) -> Self {
        Self(TaskErrorKind::Failed(error.into()))
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            TaskErrorKind::Failed(error) => write!(f, "{error:#}"),
            TaskErrorKind::Permanent(error) => write!(f, "permanent: {error:#}"),
            TaskErrorKind::Defer { after, reason } => write!(f, "deferred for {after:?}: {reason}"),
            TaskErrorKind::Throttle { after, reason } => {
                write!(f, "throttled for {after:?}: {reason}")
            }
            TaskErrorKind::Incompatible { reason } => write!(f, "incompatible: {reason}"),
        }
    }
}

impl fmt::Debug for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
