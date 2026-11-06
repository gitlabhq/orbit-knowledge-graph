//! Retry policy for remote requests: exponential backoff with full jitter, and only for failures
//! where re-sending cannot run or bill a query twice.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use rand::RngExt;

pub(crate) const DEFAULT_RETRIES: u32 = 3;
const BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(30);
/// Total time a request may spend waiting between attempts.
const WAIT_BUDGET: Duration = Duration::from_secs(300);

static CONFIGURED_RETRIES: OnceLock<u32> = OnceLock::new();

/// Sets the retry count for the process. The first call wins.
pub(crate) fn configure(retries: u32) {
    let _ = CONFIGURED_RETRIES.set(retries);
}

pub(crate) fn max_retries() -> u32 {
    CONFIGURED_RETRIES.get().copied().unwrap_or(DEFAULT_RETRIES)
}

/// The rate limit is checked before a query runs, so a 429 is always safe to retry. A 503 is
/// retried only where the endpoint allows it, because a 503 on a query can follow a billed run.
pub(crate) fn is_retryable_status(status: u16, retry_unavailable: bool) -> bool {
    status == 429 || (status == 503 && retry_unavailable)
}

/// How long to wait before retry number `retry` (1-based), or `None` when the wait would not fit
/// in what is left of the budget. `Retry-After` is a lower bound and is never capped.
pub(crate) fn next_delay(
    retry: u32,
    retry_after: Option<Duration>,
    waited: Duration,
) -> Option<Duration> {
    let ceiling = BASE_DELAY
        .saturating_mul(2_u32.saturating_pow(retry - 1))
        .min(MAX_DELAY);
    let jittered = Duration::from_millis(
        rand::rng().random_range(0..=u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX)),
    );
    let delay = jittered.max(retry_after.unwrap_or_default());
    (waited + delay <= WAIT_BUDGET).then_some(delay)
}

/// Parses a `Retry-After` value: delay-seconds or an HTTP-date.
pub(crate) fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    Some(date.duration_since(now).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        let now = httpdate::parse_http_date("Wed, 21 Oct 2015 07:28:00 GMT").unwrap();
        assert_eq!(parse_retry_after("60", now), Some(Duration::from_secs(60)));
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:28:30 GMT", now),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:27:00 GMT", now),
            Some(Duration::ZERO)
        );
        assert_eq!(parse_retry_after("soon", now), None);
    }

    #[test]
    fn delay_is_jittered_within_the_exponential_ceiling() {
        for _ in 0..200 {
            assert!(next_delay(1, None, Duration::ZERO).unwrap() <= Duration::from_secs(1));
            assert!(next_delay(3, None, Duration::ZERO).unwrap() <= Duration::from_secs(4));
            assert!(next_delay(20, None, Duration::ZERO).unwrap() <= MAX_DELAY);
        }
    }

    #[test]
    fn delay_never_undercuts_retry_after_even_past_the_cap() {
        let retry_after = Duration::from_secs(60);
        assert_eq!(
            next_delay(1, Some(retry_after), Duration::ZERO),
            Some(retry_after)
        );
    }

    #[test]
    fn a_wait_beyond_the_budget_is_refused() {
        assert_eq!(
            next_delay(1, Some(Duration::from_secs(3600)), Duration::ZERO),
            None
        );
        assert_eq!(
            next_delay(1, Some(Duration::from_secs(60)), Duration::from_secs(250)),
            None
        );
    }

    #[test]
    fn only_429_and_opted_in_503_are_retryable() {
        assert!(is_retryable_status(429, false));
        assert!(is_retryable_status(503, true));
        assert!(!is_retryable_status(503, false));
        for status in [500, 502, 504] {
            assert!(!is_retryable_status(status, true));
        }
    }
}
