//! Cooperative deadlines: hot loops call `check()`, and past the budget it
//! returns `Killed` and the caller unwinds.

use std::borrow::Cow;
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
    pub max_tree_depth: usize,
    pub max_cross_file_names: usize,
    pub max_manifest_bytes: u64,
    pub max_file_edges: usize,
    pub max_inflight_bytes: u64,
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
        max_tree_depth: 0,
        max_cross_file_names: 0,
        max_manifest_bytes: 0,
        max_file_edges: 0,
        max_inflight_bytes: 0,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    Reached { observed: usize, cap: usize },
    Below { left: usize, reserve: usize },
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Killed {
    pub cancelled: bool,
    pub label: Cow<'static, str>,
    pub path: String,
    pub family: Option<crate::treesitter::SupportLang>,
    pub elapsed: Duration,
    pub budget: Duration,
    pub stop: Option<Stop>,
}

impl Killed {
    pub fn limit(
        label: impl Into<Cow<'static, str>>,
        path: &str,
        observed: usize,
        cap: usize,
    ) -> Self {
        Self::stopped(label, path, Stop::Reached { observed, cap })
    }

    pub fn stopped(label: impl Into<Cow<'static, str>>, path: &str, stop: Stop) -> Self {
        Self {
            cancelled: false,
            label: label.into(),
            path: path.to_string(),
            family: None,
            elapsed: Duration::ZERO,
            budget: Duration::ZERO,
            stop: Some(stop),
        }
    }
}

impl std::fmt::Display for Killed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.cancelled {
            return write!(f, "{} cancelled", self.label);
        }
        let (label, path) = (&self.label, &self.path);
        let (e, b) = (self.elapsed.as_millis(), self.budget.as_millis());
        match (self.stop, path.is_empty()) {
            (Some(Stop::Reached { observed, cap }), true) => {
                write!(f, "{label} reached {observed} against a limit of {cap}")
            }
            (Some(Stop::Reached { observed, cap }), false) => write!(
                f,
                "{path} reached {observed} in {label} against a limit of {cap}"
            ),
            (Some(Stop::Below { left, reserve }), true) => write!(
                f,
                "{label} had {left} bytes left against a reserve of {reserve}"
            ),
            (Some(Stop::Below { left, reserve }), false) => write!(
                f,
                "{path} had {left} bytes of {label} left against a reserve of {reserve}"
            ),
            (Some(Stop::Failed), true) => write!(f, "{label} returned no result"),
            (Some(Stop::Failed), false) => write!(f, "{label} returned no result for {path}"),
            (None, true) => write!(f, "{label} ran {e}ms against a {b}ms budget"),
            (None, false) => write!(f, "{path} ran {e}ms in {label} against a {b}ms budget"),
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
                label: self.label.into(),
                path: self.path.clone(),
                family: None,
                elapsed: over.unwrap_or_else(|| self.started.elapsed()),
                budget: self.budget,
                stop: None,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stop_reports_the_values_it_saw() {
        let texts = [
            Killed::limit("depth", "a.py", 1_100, 1_024),
            Killed::stopped(
                "stack",
                "a.py",
                Stop::Below {
                    left: 1_000,
                    reserve: 2_000,
                },
            ),
            Killed::stopped("tree-sitter", "a.py", Stop::Failed),
        ]
        .map(|killed| killed.to_string());
        assert_eq!(
            texts,
            [
                "a.py reached 1100 in depth against a limit of 1024",
                "a.py had 1000 bytes of stack left against a reserve of 2000",
                "tree-sitter returned no result for a.py",
            ]
        );
    }

    #[test]
    fn trips_after_its_budget() {
        let s = Sentinel::new("link", "a.py", 1);
        assert!(s.check().is_ok());
        std::thread::sleep(Duration::from_millis(3));
        let k = s.check().unwrap_err();
        assert_eq!((k.label.as_ref(), k.path.as_str()), ("link", "a.py"));
    }

    #[test]
    fn disabled_never_trips() {
        assert!(Sentinel::disabled().check().is_ok());
    }
}
