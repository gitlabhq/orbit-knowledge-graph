//! What every phase shares: long-lived, built once, read by many runs.

use crate::error::LoadError;
use crate::sentinel::Limits;

pub struct Env {
    pub limits: Limits,
}

impl Env {
    pub fn load() -> Result<Self, LoadError> {
        Ok(Self::with_limits(Limits::load()?))
    }

    pub fn with_limits(limits: Limits) -> Self {
        Self { limits }
    }
}
