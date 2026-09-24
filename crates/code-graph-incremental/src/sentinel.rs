//! Cooperative deadlines: hot loops call `check()`, and past the budget it
//! returns `Killed` and the caller unwinds.

use std::time::{Duration, Instant};

use crate::error::LoadError;

#[derive(Clone, Copy, serde::Deserialize)]
pub struct Limits {
    pub file_rewrite_ms: u64,
    pub file_link_ms: u64,
    pub file_resolve_ms: u64,
    pub total_ms: u64,
}

impl Limits {
    pub fn load() -> Result<Self, LoadError> {
        Ok(orbit_utils::yaml::from_str(include_str!(
            "../config/limits.yaml"
        ))?)
    }

    pub const UNLIMITED: Self = Self {
        file_rewrite_ms: u64::MAX,
        file_link_ms: u64::MAX,
        file_resolve_ms: u64::MAX,
        total_ms: u64::MAX,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Killed {
    pub label: &'static str,
    pub path: String,
    pub elapsed: Duration,
    pub budget: Duration,
}

impl std::fmt::Display for Killed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (e, b) = (self.elapsed.as_millis(), self.budget.as_millis());
        match self.path.is_empty() {
            true => write!(f, "{} ran {e}ms against a {b}ms budget", self.label),
            false => write!(
                f,
                "{} ran {e}ms in {} against a {b}ms budget",
                self.path, self.label
            ),
        }
    }
}

impl std::error::Error for Killed {}

/// `u64::MAX` means no deadline.
#[derive(Clone)]
pub struct Sentinel {
    started: Instant,
    budget: Duration,
    label: &'static str,
    path: String,
}

impl Sentinel {
    pub fn new(label: &'static str, path: &str, budget_ms: u64) -> Self {
        Self {
            started: Instant::now(),
            budget: Duration::from_millis(budget_ms),
            label,
            path: path.to_string(),
        }
    }

    pub fn disabled() -> Self {
        Self::new("", "", u64::MAX)
    }

    #[inline]
    pub fn check(&self) -> Result<(), Killed> {
        let elapsed = self.started.elapsed();
        if elapsed > self.budget {
            return Err(Killed {
                label: self.label,
                path: self.path.clone(),
                elapsed,
                budget: self.budget,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trips_after_its_budget() {
        let s = Sentinel::new("link", "a.py", 1);
        assert!(s.check().is_ok());
        std::thread::sleep(Duration::from_millis(3));
        let k = s.check().unwrap_err();
        assert_eq!((k.label, k.path.as_str()), ("link", "a.py"));
    }

    #[test]
    fn disabled_never_trips() {
        assert!(Sentinel::disabled().check().is_ok());
    }
}
