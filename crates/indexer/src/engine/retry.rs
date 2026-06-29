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

/// The retry parameters for one failure class. `backoff` is the per-attempt delay ladder; the
/// attempt at index `i` waits `backoff[min(i, len-1)]` before retrying. `max_attempts` caps the
/// total attempts (1 means no retry). `dead_letter` only applies to [`RetryMode::Global`].
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub mode: RetryMode,
    pub backoff: &'static [Duration],
    pub max_attempts: u32,
    pub dead_letter: bool,
}

impl RetryPolicy {
    /// Backoff before the retry that follows a 0-based failed-attempt index. Clamps to the last
    /// ladder entry so a short ladder still bounds a longer attempt budget. Empty ladder = no wait.
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        if self.backoff.is_empty() {
            return Duration::ZERO;
        }
        let idx = (attempt as usize).min(self.backoff.len() - 1);
        self.backoff[idx]
    }
}

/// The outcome of one attempt. The callback returns this; [`drive`] decides what to do with it.
pub enum Step<T, E> {
    /// Finished successfully; return the value.
    Done(T),
    /// Not done, not terminal; wait and try again (subject to the policy's attempt cap).
    Retry,
    /// Terminal failure; return the error without further retries.
    GiveUp(E),
}

/// Run `attempt` until it yields [`Step::Done`] or [`Step::GiveUp`], or the policy's attempt cap
/// is hit, sleeping the policy's backoff between tries. The callback owns all variation (what is
/// "done", what state carries forward); the harness only runs, matches, sleeps, repeats.
///
/// On exhausting `max_attempts` while the last step was [`Step::Retry`], the most recent
/// `GiveUp` error is returned; if no `GiveUp` was ever produced, the callback must encode the
/// terminal error itself by returning `GiveUp` on the final attempt. In practice callers return
/// `GiveUp` once they have nothing left to try, so the cap is a safety bound, not the primary
/// exit.
pub async fn drive<T, E, S, F, Fut>(
    policy: &RetryPolicy,
    state: &mut S,
    mut attempt: F,
) -> Result<T, E>
where
    F: FnMut(&mut S, u32) -> Fut,
    Fut: Future<Output = Step<T, E>>,
    E: From<RetryExhausted>,
{
    let cap = policy.max_attempts.max(1);
    let mut last_retry = false;
    for i in 0..cap {
        match attempt(state, i).await {
            Step::Done(value) => return Ok(value),
            Step::GiveUp(error) => return Err(error),
            Step::Retry => {
                last_retry = true;
                if i + 1 < cap {
                    tokio::time::sleep(policy.backoff_for(i)).await;
                }
            }
        }
    }
    debug_assert!(last_retry);
    Err(RetryExhausted { attempts: cap }.into())
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
        backoff: &[Duration::from_secs(1), Duration::from_secs(2)],
        max_attempts: 3,
        dead_letter: false,
    };

    #[tokio::test(start_paused = true)]
    async fn done_on_first_attempt_returns_immediately() {
        let calls = AtomicU32::new(0);
        let mut state = ();
        let result: Result<u32, TestError> = drive(&POLICY, &mut state, |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Step::Done(7))
        })
        .await;
        assert_eq!(result, Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_then_succeeds() {
        let mut state = 0u32;
        let result: Result<u32, TestError> = drive(&POLICY, &mut state, |s, attempt| {
            *s = attempt;
            if attempt < 2 {
                std::future::ready(Step::Retry)
            } else {
                std::future::ready(Step::Done(attempt))
            }
        })
        .await;
        assert_eq!(result, Ok(2));
        assert_eq!(state, 2, "callback observes the attempt index via state");
    }

    #[tokio::test(start_paused = true)]
    async fn give_up_is_terminal_without_consuming_the_cap() {
        let calls = AtomicU32::new(0);
        let mut state = ();
        let result: Result<u32, TestError> = drive(&POLICY, &mut state, |_, _| {
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
        let mut state = ();
        let result: Result<u32, TestError> = drive(&POLICY, &mut state, |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Step::Retry)
        })
        .await;
        assert_eq!(result, Err(TestError::Exhausted));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "a Retry-only callback runs exactly max_attempts times"
        );
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
            backoff: &[],
            ..POLICY
        };
        assert_eq!(policy.backoff_for(0), Duration::ZERO);
    }
}
