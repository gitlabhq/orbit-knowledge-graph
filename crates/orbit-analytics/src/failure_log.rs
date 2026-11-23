use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

pub(crate) const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FailureAction {
    LogFirst {
        suppressed: u64,
    },
    Suppress,
    LogSummary {
        suppressed: u64,
        failing_for_secs: u64,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Recovery {
    pub failures: u64,
    pub failing_for_secs: u64,
}

#[derive(Default)]
struct State {
    is_failing: bool,
    failing_since: Option<Instant>,
    held_back_recovery_at: Option<Instant>,
    last_logged: Option<Instant>,
    is_failure_log_unanswered: bool,
    suppressed: u64,
    failures: u64,
}

pub(crate) struct FailureLog {
    interval: Duration,
    is_failing: AtomicBool,
    state: Mutex<State>,
}

impl FailureLog {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            is_failing: AtomicBool::new(false),
            state: Mutex::new(State::default()),
        }
    }

    pub(crate) fn record_failure(&self, now: Instant) -> FailureAction {
        let mut state = self.state.lock();
        self.is_failing.store(true, Ordering::Release);
        state.is_failing = true;
        state.failures += 1;

        if let Some(held_back_recovery_at) = state.held_back_recovery_at.take()
            && now.saturating_duration_since(held_back_recovery_at) >= self.interval
        {
            state.failing_since = None;
        }
        let is_outage_start = state.failing_since.is_none();
        let failing_since = *state.failing_since.get_or_insert(now);
        if state
            .last_logged
            .is_some_and(|last_logged| now.saturating_duration_since(last_logged) < self.interval)
        {
            state.suppressed += 1;
            return FailureAction::Suppress;
        }

        state.last_logged = Some(now);
        state.is_failure_log_unanswered = true;
        let suppressed = std::mem::take(&mut state.suppressed);
        if is_outage_start {
            FailureAction::LogFirst { suppressed }
        } else {
            FailureAction::LogSummary {
                suppressed,
                failing_for_secs: now.saturating_duration_since(failing_since).as_secs(),
            }
        }
    }

    pub(crate) fn record_success(&self, now: impl FnOnce() -> Instant) -> Option<Recovery> {
        if !self.is_failing.load(Ordering::Acquire) {
            return None;
        }
        let mut state = self.state.lock();
        if !state.is_failing {
            return None;
        }
        state.is_failing = false;
        self.is_failing.store(false, Ordering::Release);
        let now = now();
        if !state.is_failure_log_unanswered {
            state.held_back_recovery_at = Some(now);
            return None;
        }
        state.is_failure_log_unanswered = false;
        let failing_since = state.failing_since.take()?;
        Some(Recovery {
            failures: std::mem::take(&mut state.failures),
            failing_for_secs: now.saturating_duration_since(failing_since).as_secs(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: Duration = Duration::from_secs(60);

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn millis(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn first_failure_logs_once() {
        let log = FailureLog::new(INTERVAL);
        let start = Instant::now();

        assert_eq!(
            log.record_failure(start),
            FailureAction::LogFirst { suppressed: 0 }
        );
        assert_eq!(log.record_failure(start), FailureAction::Suppress);
    }

    #[test]
    fn failures_inside_the_interval_are_suppressed() {
        let log = FailureLog::new(INTERVAL);
        let start = Instant::now();

        log.record_failure(start);
        for i in 1..=11 {
            assert_eq!(
                log.record_failure(start + secs(i * 5)),
                FailureAction::Suppress
            );
        }
    }

    #[test]
    fn summary_after_the_interval_carries_counts_and_starts_a_new_window() {
        let log = FailureLog::new(INTERVAL);
        let start = Instant::now();

        log.record_failure(start);
        for i in 1..=5 {
            log.record_failure(start + secs(i * 10));
        }

        assert_eq!(
            log.record_failure(start + secs(60)),
            FailureAction::LogSummary {
                suppressed: 5,
                failing_for_secs: 60,
            }
        );
        assert_eq!(
            log.record_failure(start + secs(70)),
            FailureAction::Suppress
        );
        assert_eq!(
            log.record_failure(start + secs(125)),
            FailureAction::LogSummary {
                suppressed: 1,
                failing_for_secs: 125,
            }
        );
    }

    #[test]
    fn recovery_logs_once_with_totals_and_resets_them() {
        let log = FailureLog::new(INTERVAL);
        let start = Instant::now();

        log.record_failure(start);
        log.record_failure(start + secs(30));
        log.record_failure(start + secs(61));

        assert_eq!(
            log.record_success(|| start + secs(90)),
            Some(Recovery {
                failures: 3,
                failing_for_secs: 90,
            })
        );
        assert_eq!(log.record_success(|| start + secs(91)), None);
        assert_eq!(
            log.record_failure(start + secs(121)),
            FailureAction::LogFirst { suppressed: 0 }
        );
        assert_eq!(
            log.record_success(|| start + secs(122)),
            Some(Recovery {
                failures: 1,
                failing_for_secs: 1,
            })
        );
    }

    #[test]
    fn a_flapping_queue_logs_one_failure_per_interval_and_one_recovery_per_failure_log() {
        let log = FailureLog::new(INTERVAL);
        let start = Instant::now();
        let mut logged_failures = 0;
        let mut logged_recoveries = 0;

        for i in 0..300 {
            let at = start + millis(i * 200);
            if log.record_failure(at) != FailureAction::Suppress {
                logged_failures += 1;
            }
            if log.record_success(|| at + millis(100)).is_some() {
                logged_recoveries += 1;
            }
        }

        assert_eq!(logged_failures, 1);
        assert_eq!(logged_recoveries, 1);
        assert_eq!(
            log.record_failure(start + secs(60)),
            FailureAction::LogSummary {
                suppressed: 299,
                failing_for_secs: 59,
            }
        );
        assert_eq!(
            log.record_success(|| start + secs(60) + millis(100)),
            Some(Recovery {
                failures: 300,
                failing_for_secs: 59,
            })
        );
    }

    #[test]
    fn a_recovery_after_a_logged_failure_is_always_logged() {
        let log = FailureLog::new(INTERVAL);
        let start = Instant::now();

        log.record_failure(start);
        assert!(log.record_success(|| start + secs(50)).is_some());
        assert_eq!(
            log.record_failure(start + secs(55)),
            FailureAction::Suppress
        );
        assert_eq!(log.record_success(|| start + secs(56)), None);
        assert_eq!(
            log.record_failure(start + secs(61)),
            FailureAction::LogSummary {
                suppressed: 1,
                failing_for_secs: 6,
            }
        );
        assert_eq!(
            log.record_success(|| start + secs(62)),
            Some(Recovery {
                failures: 2,
                failing_for_secs: 7,
            })
        );
    }

    #[test]
    fn a_failure_after_a_healthy_interval_starts_a_new_outage() {
        let log = FailureLog::new(INTERVAL);
        let start = Instant::now();
        let three_hours = secs(3 * 60 * 60);

        log.record_failure(start);
        assert!(log.record_success(|| start + secs(10)).is_some());
        assert_eq!(
            log.record_failure(start + secs(20)),
            FailureAction::Suppress
        );
        assert_eq!(log.record_success(|| start + secs(21)), None);

        assert_eq!(
            log.record_failure(start + three_hours),
            FailureAction::LogFirst { suppressed: 1 }
        );
        assert_eq!(
            log.record_success(|| start + three_hours + secs(5)),
            Some(Recovery {
                failures: 2,
                failing_for_secs: 5,
            })
        );
    }

    #[test]
    fn success_without_failures_logs_nothing() {
        let log = FailureLog::new(INTERVAL);

        assert_eq!(log.record_success(Instant::now), None);
    }

    #[test]
    fn concurrent_failures_log_first_exactly_once() {
        let log = std::sync::Arc::new(FailureLog::new(INTERVAL));
        let start = Instant::now();

        let firsts: usize = (0..8)
            .map(|_| {
                let log = log.clone();
                std::thread::spawn(move || {
                    (0..100)
                        .filter(|_| {
                            matches!(log.record_failure(start), FailureAction::LogFirst { .. })
                        })
                        .count()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum();

        assert_eq!(firsts, 1);
    }
}
