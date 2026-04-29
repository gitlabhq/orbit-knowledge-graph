use std::collections::VecDeque;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

use crate::config::CircuitConfig;

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;

/// ```text
///              failure_threshold
///   ┌────────┐   exceeded    ┌──────┐
///   │ Closed ├──────────────►│ Open │
///   └───▲────┘               └──┬───┘
///       │                       │
///       │  probe succeeds       │ cooldown elapsed
///       │                       │
///   ┌───┴──────┐◄───────────────┘
///   │ HalfOpen │
///   └───┬──────┘
///       │
///       │ probe fails
///       │
///       └──────────────────►Open
/// ```
///
/// | From     | Event             | To       |
/// |----------|-------------------|----------|
/// | Closed   | failure threshold | Open     |
/// | Open     | cooldown elapsed  | HalfOpen |
/// | HalfOpen | probe succeeds    | Closed   |
/// | HalfOpen | probe fails       | Open     |
#[derive(Debug)]
enum CircuitState {
    Closed { failures: VecDeque<Instant> },
    Open { until: Instant },
    HalfOpen,
}

/// State label without internal data, for observer callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateLabel {
    Closed,
    Open,
    HalfOpen,
}

impl StateLabel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            STATE_CLOSED => Self::Closed,
            STATE_OPEN => Self::Open,
            STATE_HALF_OPEN => Self::HalfOpen,
            _ => Self::Closed,
        }
    }
}

/// A state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct Transition {
    pub from: StateLabel,
    pub to: StateLabel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquireResult {
    Allowed,
    /// First request after cooldown — its outcome decides the next state.
    Probe,
    Rejected,
}

/// Per-service circuit state. Atomic fast-path: the mutex is only
/// taken on failures and state transitions.
pub(crate) struct Circuit {
    state: AtomicU8,
    inner: Mutex<CircuitInner>,
}

struct CircuitInner {
    config: CircuitConfig,
    state: CircuitState,
}

impl Circuit {
    pub fn new(config: CircuitConfig) -> Self {
        Self {
            state: AtomicU8::new(STATE_CLOSED),
            inner: Mutex::new(CircuitInner {
                config,
                state: CircuitState::Closed {
                    failures: VecDeque::new(),
                },
            }),
        }
    }

    pub fn state_label(&self) -> StateLabel {
        StateLabel::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn is_available(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_CLOSED
    }

    /// Gate a request. Closed → atomic load only, no lock.
    pub fn try_acquire(&self, now: Instant) -> AcquireResult {
        if self.state.load(Ordering::Acquire) == STATE_CLOSED {
            return AcquireResult::Allowed;
        }

        let mut inner = self.inner.lock();
        match &inner.state {
            CircuitState::Closed { .. } => AcquireResult::Allowed,
            CircuitState::Open { until } => {
                if now >= *until {
                    inner.state = CircuitState::HalfOpen;
                    self.state.store(STATE_HALF_OPEN, Ordering::Release);
                    AcquireResult::Probe
                } else {
                    AcquireResult::Rejected
                }
            }
            CircuitState::HalfOpen => AcquireResult::Rejected,
        }
    }

    /// Record success. Closed → atomic load only, no lock.
    pub fn record_success(&self) -> Option<Transition> {
        if self.state.load(Ordering::Acquire) == STATE_CLOSED {
            return None;
        }

        let mut inner = self.inner.lock();
        match &inner.state {
            CircuitState::Closed { .. } | CircuitState::Open { .. } => None,
            CircuitState::HalfOpen => {
                inner.state = CircuitState::Closed {
                    failures: VecDeque::new(),
                };
                self.state.store(STATE_CLOSED, Ordering::Release);
                Some(Transition {
                    from: StateLabel::HalfOpen,
                    to: StateLabel::Closed,
                })
            }
        }
    }

    /// Record failure. Always locks — an IO error already happened,
    /// so the mutex cost is noise.
    pub fn record_failure(&self, now: Instant) -> Option<Transition> {
        let mut inner = self.inner.lock();
        let window = inner.config.window;
        let cooldown = inner.config.cooldown;
        let threshold = inner.config.failure_threshold as usize;

        match &mut inner.state {
            CircuitState::Closed { failures } => {
                let window_start = now - window;
                while failures.front().is_some_and(|t| *t < window_start) {
                    failures.pop_front();
                }
                failures.push_back(now);

                if failures.len() >= threshold {
                    inner.state = CircuitState::Open {
                        until: now + cooldown,
                    };
                    self.state.store(STATE_OPEN, Ordering::Release);
                    Some(Transition {
                        from: StateLabel::Closed,
                        to: StateLabel::Open,
                    })
                } else {
                    None
                }
            }
            CircuitState::HalfOpen => {
                inner.state = CircuitState::Open {
                    until: now + cooldown,
                };
                self.state.store(STATE_OPEN, Ordering::Release);
                Some(Transition {
                    from: StateLabel::HalfOpen,
                    to: StateLabel::Open,
                })
            }
            CircuitState::Open { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn config() -> CircuitConfig {
        CircuitConfig {
            failure_threshold: 3,
            window: Duration::from_secs(10),
            cooldown: Duration::from_secs(5),
        }
    }

    fn trip_circuit(circuit: &Circuit, now: Instant) {
        for i in 0..3 {
            circuit.try_acquire(now);
            circuit.record_failure(now + Duration::from_millis(i));
        }
        assert_eq!(circuit.state_label(), StateLabel::Open);
    }

    #[test]
    fn starts_closed() {
        let circuit = Circuit::new(config());
        assert!(circuit.is_available());
        assert_eq!(circuit.state_label(), StateLabel::Closed);
    }

    #[test]
    fn closed_allows_requests() {
        let circuit = Circuit::new(config());
        assert_eq!(circuit.try_acquire(Instant::now()), AcquireResult::Allowed);
    }

    #[test]
    fn closed_stays_closed_on_success() {
        let circuit = Circuit::new(config());
        circuit.try_acquire(Instant::now());
        assert!(circuit.record_success().is_none());
        assert_eq!(circuit.state_label(), StateLabel::Closed);
    }

    #[test]
    fn closed_to_open_after_threshold_failures() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        for i in 0..2 {
            circuit.try_acquire(now);
            assert!(
                circuit
                    .record_failure(now + Duration::from_millis(i))
                    .is_none()
            );
        }

        circuit.try_acquire(now);
        let transition = circuit.record_failure(now + Duration::from_millis(2));

        assert_eq!(
            transition,
            Some(Transition {
                from: StateLabel::Closed,
                to: StateLabel::Open
            })
        );
        assert_eq!(circuit.state_label(), StateLabel::Open);
        assert!(!circuit.is_available());
    }

    #[test]
    fn open_rejects_requests() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        trip_circuit(&circuit, now);

        assert_eq!(
            circuit.try_acquire(now + Duration::from_secs(1)),
            AcquireResult::Rejected
        );
    }

    #[test]
    fn open_to_half_open_after_cooldown() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        trip_circuit(&circuit, now);

        let after_cooldown = now + Duration::from_secs(6);
        assert_eq!(circuit.try_acquire(after_cooldown), AcquireResult::Probe);
        assert_eq!(circuit.state_label(), StateLabel::HalfOpen);
    }

    #[test]
    fn half_open_rejects_concurrent_requests() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        trip_circuit(&circuit, now);

        let after_cooldown = now + Duration::from_secs(6);
        assert_eq!(circuit.try_acquire(after_cooldown), AcquireResult::Probe);
        assert_eq!(circuit.try_acquire(after_cooldown), AcquireResult::Rejected);
    }

    #[test]
    fn half_open_to_closed_on_success() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        trip_circuit(&circuit, now);

        let after_cooldown = now + Duration::from_secs(6);
        assert_eq!(circuit.try_acquire(after_cooldown), AcquireResult::Probe);

        let transition = circuit.record_success();
        assert_eq!(
            transition,
            Some(Transition {
                from: StateLabel::HalfOpen,
                to: StateLabel::Closed
            })
        );
        assert!(circuit.is_available());
    }

    #[test]
    fn half_open_to_open_on_failure() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        trip_circuit(&circuit, now);

        let after_cooldown = now + Duration::from_secs(6);
        assert_eq!(circuit.try_acquire(after_cooldown), AcquireResult::Probe);

        let transition = circuit.record_failure(after_cooldown);
        assert_eq!(
            transition,
            Some(Transition {
                from: StateLabel::HalfOpen,
                to: StateLabel::Open
            })
        );
        assert_eq!(circuit.state_label(), StateLabel::Open);
    }

    #[test]
    fn failures_outside_window_are_pruned() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        circuit.try_acquire(now);
        circuit.record_failure(now);

        circuit.try_acquire(now);
        circuit.record_failure(now + Duration::from_millis(1));

        let well_after_window = now + Duration::from_secs(15);

        circuit.try_acquire(well_after_window);
        assert!(circuit.record_failure(well_after_window).is_none());

        assert_eq!(circuit.state_label(), StateLabel::Closed);
    }

    #[test]
    fn recovery_resets_failure_window() {
        let circuit = Circuit::new(config());
        let now = Instant::now();

        trip_circuit(&circuit, now);

        let after_cooldown = now + Duration::from_secs(6);
        assert_eq!(circuit.try_acquire(after_cooldown), AcquireResult::Probe);
        circuit.record_success();

        circuit.try_acquire(after_cooldown);
        assert!(circuit.record_failure(after_cooldown).is_none());

        assert_eq!(circuit.state_label(), StateLabel::Closed);
    }
}
