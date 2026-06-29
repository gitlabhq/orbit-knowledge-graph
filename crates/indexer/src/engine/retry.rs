//! One retry vocabulary for the indexer.
//!
//! A failure is handled in one of two modes:
//!
//! - [`RetryMode::InSitu`]: retry on the spot with bounded backoff, in-process. Driven by
//!   [`drive`].
//! - [`RetryMode::Global`]: hand the message back to NATS for redelivery, bounded by delivery
//!   attempts, then dead-letter. Executed by the engine's `run_handlers` path, which reads the
//!   same [`RetryPolicy`].
//!
//! Both modes are described by one [`RetryPolicy`]. The in-situ executor ([`drive`]) reads the
//! backoff ladder and attempt cap from the policy; the callback only classifies each attempt's
//! outcome via [`Step`], so the backoff lives in exactly one place.

use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryMode {
    /// Retry in-process with backoff; never leaves the worker.
    InSitu,
    /// Retry via NATS redelivery; the engine nacks and the broker re-delivers.
    Global,
}

/// How the delay before each retry is computed from the 0-based failed-attempt index.
#[derive(Debug, Clone, Copy)]
pub enum Backoff {
    /// A fixed ladder; attempt `i` waits `ladder[min(i, len-1)]`. Empty = no wait.
    Fixed(&'static [Duration]),
    /// `base * 2^attempt`, clamped to `cap`.
    Exponential { base: Duration, cap: Duration },
}

impl Backoff {
    pub fn delay(&self, attempt: u32) -> Duration {
        match self {
            Backoff::Fixed(ladder) => {
                if ladder.is_empty() {
                    return Duration::ZERO;
                }
                ladder[(attempt as usize).min(ladder.len() - 1)]
            }
            Backoff::Exponential { base, cap } => base
                .saturating_mul(2u32.saturating_pow(attempt.min(16)))
                .min(*cap),
        }
    }
}

/// The retry parameters for one failure class. `max_attempts` caps the total attempts (1 means
/// no retry); for deadline-bounded callers ([`drive_until`]) it is the safety bound and the
/// deadline is the primary exit. `dead_letter` only applies to [`RetryMode::Global`].
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub mode: RetryMode,
    pub backoff: Backoff,
    pub max_attempts: u32,
    pub dead_letter: bool,
}

impl RetryPolicy {
    /// Delay before the retry that follows a 0-based failed-attempt index.
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        self.backoff.delay(attempt)
    }
}

/// The outcome of one attempt. The callback returns this; the harness decides what to do with it.
/// `Retry` carries the state for the next attempt, so any per-attempt mutation is threaded by
/// value through the harness rather than borrowed across the await point (keeps the future
/// `Send` and sidesteps the lending-closure problem). Stateless callers use `S = ()`.
pub enum Step<T, E, S = ()> {
    /// Finished successfully; return the value.
    Done(T),
    /// Not done, not terminal; wait and try again with `S` as the next attempt's state.
    Retry(S),
    /// Terminal failure; return the error without further retries.
    GiveUp(E),
}

/// Stateful retry: the harness owns the carried state `S`, moves it into each attempt by value,
/// and receives the next state back via [`Step::Retry`]. Runs until [`Step::Done`]/[`Step::GiveUp`]
/// or the policy's attempt cap, sleeping the policy's backoff between tries. `attempt` receives
/// the current state and the 0-based attempt index.
///
/// Reaching the cap while every step asked to `Retry` returns [`RetryExhausted`] (via `E`'s
/// `From`). In practice callers return `GiveUp` with the real error once they have nothing left
/// to try, so the cap is a safety bound, not the primary exit.
pub async fn drive_with<T, E, S, F, Fut>(
    policy: &RetryPolicy,
    init: S,
    mut attempt: F,
) -> Result<T, E>
where
    F: FnMut(S, u32) -> Fut,
    Fut: Future<Output = Step<T, E, S>>,
    E: From<RetryExhausted>,
{
    let cap = policy.max_attempts.max(1);
    let mut state = init;
    for i in 0..cap {
        match attempt(state, i).await {
            Step::Done(value) => return Ok(value),
            Step::GiveUp(error) => return Err(error),
            Step::Retry(next) => {
                state = next;
                if i + 1 < cap {
                    tokio::time::sleep(policy.backoff_for(i)).await;
                }
            }
        }
    }
    Err(RetryExhausted { attempts: cap }.into())
}

/// Stateless retry: [`drive_with`] with no carried state. The callback only sees the attempt
/// index and returns [`Step`] with `S = ()`.
pub async fn drive<T, E, F, Fut>(policy: &RetryPolicy, mut attempt: F) -> Result<T, E>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Step<T, E>>,
    E: From<RetryExhausted>,
{
    drive_with(policy, (), move |(), i| attempt(i)).await
}

/// Deadline-bounded retry: poll `attempt` until it yields `Done`/`GiveUp`, sleeping the policy's
/// backoff (clamped to the time remaining) between tries, until `deadline` passes. Returns
/// `None` when the deadline is reached while still retrying, so the caller can build a timeout
/// error with its own context. `max_attempts` is the safety bound; the deadline is the primary
/// exit. State is threaded by value via [`Step::Retry`], like [`drive_with`].
pub async fn drive_until<T, E, S, F, Fut>(
    policy: &RetryPolicy,
    deadline: tokio::time::Instant,
    init: S,
    mut attempt: F,
) -> Option<Result<T, E>>
where
    F: FnMut(S, u32) -> Fut,
    Fut: Future<Output = Step<T, E, S>>,
{
    let cap = policy.max_attempts.max(1);
    let mut state = init;
    for i in 0..cap {
        match attempt(state, i).await {
            Step::Done(value) => return Some(Ok(value)),
            Step::GiveUp(error) => return Some(Err(error)),
            Step::Retry(next) => {
                state = next;
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return None;
                }
                tokio::time::sleep(policy.backoff_for(i).min(remaining)).await;
            }
        }
    }
    None
}

/// Returned by [`drive`] when every attempt asked to `Retry` and the cap was reached. Callers
/// that always end in `GiveUp` never see this; it is the safety bound for a callback that only
/// ever returns `Retry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryExhausted {
    pub attempts: u32,
}

impl std::fmt::Display for RetryExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "retry exhausted after {} attempts", self.attempts)
    }
}

impl std::error::Error for RetryExhausted {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        GaveUp,
        Exhausted,
    }

    impl From<RetryExhausted> for TestError {
        fn from(_: RetryExhausted) -> Self {
            TestError::Exhausted
        }
    }

    const POLICY: RetryPolicy = RetryPolicy {
        mode: RetryMode::InSitu,
        backoff: Backoff::Fixed(&[Duration::from_secs(1), Duration::from_secs(2)]),
        max_attempts: 3,
        dead_letter: false,
    };

    #[tokio::test(start_paused = true)]
    async fn done_on_first_attempt_returns_immediately() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, TestError> = drive(&POLICY, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Step::Done(7))
        })
        .await;
        assert_eq!(result, Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_then_succeeds() {
        // State carried in the callback's own captured environment, not through the harness.
        let mut seen = 0u32;
        let result: Result<u32, TestError> = drive(&POLICY, |attempt| {
            seen = attempt;
            if attempt < 2 {
                std::future::ready(Step::Retry(()))
            } else {
                std::future::ready(Step::Done(attempt))
            }
        })
        .await;
        assert_eq!(result, Ok(2));
        assert_eq!(
            seen, 2,
            "callback mutates its own captured state across attempts"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn give_up_is_terminal_without_consuming_the_cap() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, TestError> = drive(&POLICY, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Step::GiveUp(TestError::GaveUp))
        })
        .await;
        assert_eq!(result, Err(TestError::GaveUp));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "GiveUp stops on the first attempt"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn exhausting_the_cap_yields_retry_exhausted() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, TestError> = drive(&POLICY, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Step::Retry(()))
        })
        .await;
        assert_eq!(result, Err(TestError::Exhausted));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "a Retry-only callback runs exactly max_attempts times"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stateful_threads_state_through_retry() {
        // Each Retry carries a shrinking value; the next attempt receives it by value.
        let result: Result<u64, TestError> = drive_with(&POLICY, 100u64, |size, attempt| {
            std::future::ready(if attempt < 2 {
                Step::Retry(size / 2)
            } else {
                Step::Done(size)
            })
        })
        .await;
        assert_eq!(result, Ok(25), "100 -> 50 -> 25 across two retries");
    }

    #[test]
    fn backoff_clamps_to_the_last_ladder_entry() {
        assert_eq!(POLICY.backoff_for(0), Duration::from_secs(1));
        assert_eq!(POLICY.backoff_for(1), Duration::from_secs(2));
        assert_eq!(POLICY.backoff_for(99), Duration::from_secs(2));
    }

    #[test]
    fn empty_backoff_waits_zero() {
        let policy = RetryPolicy {
            backoff: Backoff::Fixed(&[]),
            ..POLICY
        };
        assert_eq!(policy.backoff_for(0), Duration::ZERO);
    }

    #[test]
    fn exponential_backoff_doubles_and_caps() {
        let backoff = Backoff::Exponential {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(10),
        };
        assert_eq!(backoff.delay(0), Duration::from_secs(1));
        assert_eq!(backoff.delay(1), Duration::from_secs(2));
        assert_eq!(backoff.delay(2), Duration::from_secs(4));
        assert_eq!(backoff.delay(99), Duration::from_secs(10), "clamped to cap");
    }
}
