//! Cooperative time budgets. A sentinel is one deadline. Hot loops call
//! `check()` on the run-wide sentinel and on their own; past a deadline it
//! returns `Killed` and the caller unwinds.

use std::time::{Duration, Instant};

/// Time budgets, loaded from `config/limits.yaml`. Each one seeds a sentinel.
#[derive(Clone, Copy, serde::Deserialize)]
pub struct Limits {
    pub file_rewrite_ms: u64,
    pub file_link_ms: u64,
    pub file_resolve_ms: u64,
    pub total_ms: u64,
}

impl Limits {
    pub fn load() -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(include_str!("../../config/limits.yaml"))
    }

    pub const UNLIMITED: Self = Self {
        file_rewrite_ms: u64::MAX,
        file_link_ms: u64::MAX,
        file_resolve_ms: u64::MAX,
        total_ms: u64::MAX,
    };
}

impl Default for Limits {
    fn default() -> Self {
        Self::load().unwrap_or(Self {
            file_rewrite_ms: 200,
            file_link_ms: 100,
            file_resolve_ms: 100,
            total_ms: 300_000,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Killed {
    pub label: &'static str,
    pub path: String,
}

impl std::fmt::Display for Killed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.path.is_empty() {
            true => write!(f, "{} exceeded its budget", self.label),
            false => write!(f, "{} timed out in {}", self.path, self.label),
        }
    }
}

impl std::error::Error for Killed {}

/// One deadline. `check()` is one clock read and a compare; `u64::MAX` means
/// no deadline.
#[derive(Clone)]
pub struct Sentinel {
    deadline: Option<Instant>,
    label: &'static str,
    path: String,
}

impl Sentinel {
    pub fn new(label: &'static str, path: &str, budget_ms: u64) -> Self {
        Self {
            deadline: Instant::now().checked_add(Duration::from_millis(budget_ms)),
            label,
            path: path.to_string(),
        }
    }

    pub fn disabled() -> Self {
        Self::new("", "", u64::MAX)
    }

    #[inline]
    pub fn check(&self) -> Result<(), Killed> {
        match self.deadline {
            Some(d) if Instant::now() > d => Err(Killed {
                label: self.label,
                path: self.path.clone(),
            }),
            _ => Ok(()),
        }
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
