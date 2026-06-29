//! One retry vocabulary for the indexer: a [`RetryPolicy`] describes a failure class, [`drive`]
//! and friends run it locally (in-process backoff), and `run_handlers` runs the [`RetryMode::Global`]
//! ones via NATS redelivery.

use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryMode {
    /// Retry in-process with backoff.
    Local,
    /// Retry via NATS redelivery, then dead-letter.
    Global,
}

#[derive(Debug, Clone, Copy)]
pub enum Backoff {
    /// Attempt `i` waits `ladder[min(i, len-1)]`; empty = no wait.
    Fixed(&'static [Duration]),
    /// `base * 2^attempt`, clamped to `cap`.
    Exponential { base: Duration, cap: Duration },
}

impl Backoff {
    pub fn delay(&self, attempt: u32) -> Duration {
        match self {
            Backoff::Fixed(ladder) => ladder
                .get((attempt as usize).min(ladder.len().saturating_sub(1)))
                .copied()
                .unwrap_or(Duration::ZERO),
            Backoff::Exponential { base, cap } => base
                .saturating_mul(2u32.saturating_pow(attempt.min(16)))
                .min(*cap),
        }
    }
}

/// `max_attempts` is the attempt cap (1 = no retry); under [`drive_until`] the deadline is the
/// primary exit and the cap is the safety bound. `dead_letter` applies only to [`RetryMode::Global`].
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub mode: RetryMode,
    pub backoff: Backoff,
    pub max_attempts: u32,
    pub dead_letter: bool,
}

impl RetryPolicy {
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        self.backoff.delay(attempt)
    }

    /// The common transient-error branch: [`Step::Retry`] with `next` while attempts remain, else
    /// [`Step::GiveUp`] surfacing the real error on the final attempt. Centralizes the cap check.
    pub fn retry_or_give_up<T, E, S>(&self, attempt: u32, next: S, give_up: E) -> Step<T, E, S> {
        if attempt + 1 < self.max_attempts.max(1) {
            Step::Retry(next)
        } else {
            Step::GiveUp(give_up)
        }
    }
}

/// The outcome of one attempt. `Retry` carries the next attempt's state by value, so per-attempt
/// mutation never borrows across the await (keeps the future `Send`). Stateless callers use `S = ()`.
pub enum Step<T, E, S = ()> {
    Done(T),
    Retry(S),
    GiveUp(E),
}

/// Bounded retry that threads state by value through [`Step::Retry`].
pub async fn drive_with<T, E, S, F, Fut>(policy: &RetryPolicy, init: S, attempt: F) -> Result<T, E>
where
    F: FnMut(S, u32) -> Fut,
    Fut: Future<Output = Step<T, E, S>>,
    E: From<RetryExhausted>,
{
    let cap = policy.max_attempts.max(1);
    drive_bounded(policy, None, init, attempt, |_| {
        RetryExhausted { attempts: cap }.into()
    })
    .await
}

/// Bounded retry with no carried state.
pub async fn drive<T, E, F, Fut>(policy: &RetryPolicy, mut attempt: F) -> Result<T, E>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Step<T, E>>,
    E: From<RetryExhausted>,
{
    drive_with(policy, (), move |(), i| attempt(i)).await
}

/// Bounded by `deadline` as well as the attempt cap; on deadline, `on_deadline` builds the
/// terminal error from the last state so the result stays a plain `Result`.
pub async fn drive_until<T, E, S, F, Fut, D>(
    policy: &RetryPolicy,
    deadline: tokio::time::Instant,
    init: S,
    attempt: F,
    on_deadline: D,
) -> Result<T, E>
where
    F: FnMut(S, u32) -> Fut,
    Fut: Future<Output = Step<T, E, S>>,
    D: FnOnce(&S) -> E,
{
    drive_bounded(policy, Some(deadline), init, attempt, on_deadline).await
}

async fn drive_bounded<T, E, S, F, Fut, D>(
    policy: &RetryPolicy,
    deadline: Option<tokio::time::Instant>,
    init: S,
    mut attempt: F,
    on_bound: D,
) -> Result<T, E>
where
    F: FnMut(S, u32) -> Fut,
    Fut: Future<Output = Step<T, E, S>>,
    D: FnOnce(&S) -> E,
{
    let cap = policy.max_attempts.max(1);
    let mut state = init;
    for i in 0..cap {
        match attempt(state, i).await {
            Step::Done(value) => return Ok(value),
            Step::GiveUp(error) => return Err(error),
            Step::Retry(next) => {
                state = next;
                let mut delay = policy.backoff_for(i);
                if let Some(deadline) = deadline {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(on_bound(&state));
                    }
                    delay = delay.min(remaining);
                }
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(on_bound(&state))
}

/// One iteration of an unbounded supervisor loop.
pub enum Loop {
    /// Succeeded; reset the failure counter and run again immediately.
    Continue,
    /// Failed; back off then run again (no cap).
    Backoff,
    /// Terminal; leave the loop.
    Stop,
}

/// Unbounded supervisor loop with no result or cap: runs until `step` returns [`Loop::Stop`].
/// `step` gets the consecutive-failure count so [`Backoff::Exponential`] escalates and resets on success.
pub async fn drive_forever<F, Fut>(policy: &RetryPolicy, mut step: F)
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Loop>,
{
    let mut consecutive_failures = 0u32;
    loop {
        match step(consecutive_failures).await {
            Loop::Continue => consecutive_failures = 0,
            Loop::Stop => return,
            Loop::Backoff => {
                tokio::time::sleep(policy.backoff_for(consecutive_failures)).await;
                consecutive_failures = consecutive_failures.saturating_add(1);
            }
        }
    }
}

/// The safety-bound error when a callback only ever returns `Retry` until the cap.
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
        mode: RetryMode::Local,
        backoff: Backoff::Fixed(&[Duration::from_secs(1), Duration::from_secs(2)]),
        max_attempts: 3,
        dead_letter: false,
    };

    /// Run `drive` with a callback that returns `Done(0)` once `attempt` reaches `done_at`, else
    /// `Retry`/`GiveUp` per `terminal`, counting calls. `done_at = None` means never finish.
    async fn run(
        done_at: Option<u32>,
        terminal: fn() -> Step<u32, TestError>,
    ) -> (Result<u32, TestError>, u32) {
        let calls = AtomicU32::new(0);
        let result = drive(&POLICY, |attempt| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(match done_at {
                Some(n) if attempt >= n => Step::Done(attempt),
                _ => terminal(),
            })
        })
        .await;
        (result, calls.load(Ordering::SeqCst))
    }

    #[tokio::test(start_paused = true)]
    async fn done_returns_on_the_first_success() {
        assert_eq!(run(Some(0), || Step::Retry(())).await, (Ok(0), 1));
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_success() {
        assert_eq!(run(Some(2), || Step::Retry(())).await, (Ok(2), 3));
    }

    #[tokio::test(start_paused = true)]
    async fn give_up_is_terminal_on_the_first_attempt() {
        assert_eq!(
            run(None, || Step::GiveUp(TestError::GaveUp)).await,
            (Err(TestError::GaveUp), 1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_only_runs_exactly_max_attempts_then_exhausts() {
        assert_eq!(
            run(None, || Step::Retry(())).await,
            (Err(TestError::Exhausted), 3)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stateful_threads_state_through_retry() {
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

    #[tokio::test(start_paused = true)]
    async fn drive_forever_backs_off_on_failure_and_stops_on_request() {
        let mut script = vec![Loop::Backoff, Loop::Backoff, Loop::Continue, Loop::Stop].into_iter();
        let mut failure_counts = Vec::new();
        drive_forever(&POLICY, |failures| {
            failure_counts.push(failures);
            std::future::ready(script.next().unwrap())
        })
        .await;
        assert_eq!(failure_counts, vec![0, 1, 2, 0]);
    }

    #[test]
    fn retry_or_give_up_respects_the_cap() {
        // POLICY.max_attempts == 3: attempts 0 and 1 retry, attempt 2 gives up.
        assert!(matches!(
            POLICY.retry_or_give_up::<(), _, ()>(0, (), TestError::GaveUp),
            Step::Retry(())
        ));
        assert!(matches!(
            POLICY.retry_or_give_up::<(), _, ()>(2, (), TestError::GaveUp),
            Step::GiveUp(TestError::GaveUp)
        ));
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
