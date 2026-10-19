//! Cooperative deadlines: hot loops call `check()`, and past the budget it
//! returns `Killed` and the caller unwinds.

use std::time::{Duration, Instant};

use cpu_time::ThreadTime;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_files: usize,
    pub cross_file_resolve_ms: u64,
    pub file_parse_ms: u64,
    pub file_rewrite_ms: u64,
    pub file_link_ms: u64,
    pub file_resolve_ms: u64,
    pub total_ms: u64,
}

impl Limits {
    pub const UNLIMITED: Self = Self {
        max_files: 0,
        cross_file_resolve_ms: u64::MAX,
        file_parse_ms: u64::MAX,
        file_rewrite_ms: u64::MAX,
        file_link_ms: u64::MAX,
        file_resolve_ms: u64::MAX,
        total_ms: u64::MAX,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Killed {
    pub cancelled: bool,
    pub label: &'static str,
    pub path: String,
    pub elapsed: Duration,
    pub budget: Duration,
}

impl std::fmt::Display for Killed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.cancelled {
            return write!(f, "{} cancelled", self.label);
        }
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

#[derive(Clone, Copy)]
enum Clock {
    Wall(Instant),
    Thread(Instant, Duration),
}

impl Clock {
    fn elapsed(&self) -> Duration {
        match self {
            Self::Wall(started) | Self::Thread(started, _) => started.elapsed(),
        }
    }

    fn over(&self, budget: Duration) -> Option<Duration> {
        let wall = self.elapsed();
        let spent = match self {
            _ if wall <= budget => return None,
            Self::Wall(_) => wall,
            Self::Thread(_, cpu) => ThreadTime::now().as_duration().saturating_sub(*cpu),
        };
        (spent > budget).then_some(spent)
    }
}

/// `u64::MAX` means no deadline.
#[derive(Clone)]
pub struct Sentinel {
    started: Clock,
    budget: Duration,
    label: &'static str,
    path: String,
    cancelled: Option<std::sync::Arc<dyn Fn() -> bool + Send + Sync>>,
}

impl Sentinel {
    pub fn new(label: &'static str, path: &str, budget_ms: u64) -> Self {
        Self {
            started: Clock::Wall(Instant::now()),
            budget: Duration::from_millis(budget_ms),
            label,
            path: path.to_string(),
            cancelled: None,
        }
    }

    pub fn disabled() -> Self {
        Self::new("", "", u64::MAX)
    }

    pub fn child(&self, label: &'static str, path: &str, budget_ms: u64) -> Self {
        let mut child = Self::new(label, path, budget_ms);
        child.budget = child
            .budget
            .min(self.budget.saturating_sub(self.started.elapsed()));
        child.cancelled = self.cancelled.clone();
        child
    }

    pub fn on_thread_cpu(mut self) -> Self {
        self.started = Clock::Thread(Instant::now(), ThreadTime::now().as_duration());
        self
    }

    pub fn with_cancellation(
        mut self,
        cancelled: std::sync::Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        self.cancelled = Some(cancelled);
        self
    }

    #[inline]
    pub fn check(&self) -> Result<(), Killed> {
        let over = self.started.over(self.budget);
        let cancelled = self.cancelled.as_ref().is_some_and(|cancelled| cancelled());
        if over.is_some() || cancelled {
            return Err(Killed {
                cancelled,
                label: self.label,
                path: self.path.clone(),
                elapsed: over.unwrap_or_else(|| self.started.elapsed()),
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
