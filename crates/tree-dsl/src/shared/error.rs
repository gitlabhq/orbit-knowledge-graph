//! Errors the pipeline returns instead of panicking.

use crate::sentinel::Killed;

/// A rule file, config file, or pattern that does not compile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadError(pub String);

impl LoadError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LoadError {}

impl From<orbit_utils::yaml::Error> for LoadError {
    fn from(e: orbit_utils::yaml::Error) -> Self {
        Self(format!("yaml: {e}"))
    }
}

impl From<regex::Error> for LoadError {
    fn from(e: regex::Error) -> Self {
        Self(format!("regex: {e}"))
    }
}

/// Anything a pipeline entry point can fail with.
#[derive(Debug)]
pub enum Error {
    Load(LoadError),
    Killed(Killed),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Load(e) => write!(f, "load: {e}"),
            Self::Killed(k) => write!(f, "budget: {k}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<LoadError> for Error {
    fn from(e: LoadError) -> Self {
        Self::Load(e)
    }
}

impl From<Killed> for Error {
    fn from(k: Killed) -> Self {
        Self::Killed(k)
    }
}
