use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use rand::RngExt;

pub(crate) const DEFAULT_RETRIES: u32 = 3;
const BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(30);
const DEFAULT_WAIT_BUDGET: Duration = Duration::from_secs(300);

#[derive(Clone, Copy)]
pub(crate) struct Policy {
    pub(crate) retry_unavailable: bool,
    pub(crate) wait_budget: Duration,
}

impl Policy {
    pub(crate) const QUERY: Self = Self {
        retry_unavailable: false,
        wait_budget: DEFAULT_WAIT_BUDGET,
    };
    pub(crate) const READ_ONLY: Self = Self {
        retry_unavailable: true,
        wait_budget: DEFAULT_WAIT_BUDGET,
    };
    pub(crate) const BEST_EFFORT: Self = Self {
        retry_unavailable: true,
        wait_budget: Duration::from_secs(10),
    };
}

static CONFIGURED_RETRIES: OnceLock<u32> = OnceLock::new();

pub(crate) fn configure(retries: u32) {
    let _ = CONFIGURED_RETRIES.set(retries);
}

pub(crate) fn max_retries() -> u32 {
    CONFIGURED_RETRIES.get().copied().unwrap_or(DEFAULT_RETRIES)
}

pub(crate) fn is_retryable_status(status: u16, retry_unavailable: bool) -> bool {
    status == 429 || (status == 503 && retry_unavailable)
}

pub(crate) fn next_delay(
    retry: u32,
    retry_after: Option<Duration>,
    waited: Duration,
    wait_budget: Duration,
) -> Option<Duration> {
    let ceiling = BASE_DELAY
        .saturating_mul(2_u32.saturating_pow(retry - 1))
        .min(MAX_DELAY);
    let jittered = Duration::from_millis(
        rand::rng().random_range(0..=u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX)),
    );
    let delay = jittered.max(retry_after.unwrap_or_default());
    (waited.saturating_add(delay) <= wait_budget).then_some(delay)
}

pub(crate) fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    Some(date.duration_since(now).unwrap_or_default())
}

pub(crate) fn rate_limit_note(retries: u32, retry_after: Option<Duration>) -> String {
    let mut note = String::new();
    if retries > 0 {
        note.push_str(&format!("\nRetries used: {retries}."));
    }
    if let Some(wait) = retry_after {
        note.push_str(&format!(
            "\nThe server asked to wait {} seconds before trying again.",
            wait.as_secs()
        ));
    }
    note
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
            assert!(
                next_delay(1, None, Duration::ZERO, DEFAULT_WAIT_BUDGET).unwrap()
                    <= Duration::from_secs(1)
            );
            assert!(
                next_delay(3, None, Duration::ZERO, DEFAULT_WAIT_BUDGET).unwrap()
                    <= Duration::from_secs(4)
            );
            assert!(
                next_delay(20, None, Duration::ZERO, DEFAULT_WAIT_BUDGET).unwrap() <= MAX_DELAY
            );
        }
    }

    #[test]
    fn delay_never_undercuts_retry_after_even_past_the_cap() {
        let retry_after = Duration::from_secs(60);
        assert_eq!(
            next_delay(1, Some(retry_after), Duration::ZERO, DEFAULT_WAIT_BUDGET),
            Some(retry_after)
        );
    }

    #[test]
    fn a_wait_beyond_the_budget_is_refused() {
        assert_eq!(
            next_delay(
                1,
                Some(Duration::from_secs(3600)),
                Duration::ZERO,
                DEFAULT_WAIT_BUDGET
            ),
            None
        );
        assert_eq!(
            next_delay(
                1,
                Some(Duration::from_secs(60)),
                Duration::from_secs(250),
                DEFAULT_WAIT_BUDGET
            ),
            None
        );
    }

    #[test]
    fn an_enormous_retry_after_after_a_wait_is_refused_without_overflow() {
        let huge = Duration::from_secs(u64::MAX);
        assert_eq!(
            next_delay(2, Some(huge), Duration::from_secs(1), DEFAULT_WAIT_BUDGET),
            None
        );
    }

    #[test]
    fn best_effort_calls_refuse_a_long_retry_after() {
        let wait = Some(Duration::from_secs(60));
        assert_eq!(
            next_delay(1, wait, Duration::ZERO, Policy::BEST_EFFORT.wait_budget),
            None
        );
    }

    #[test]
    fn the_rate_limit_note_reports_retries_and_the_requested_wait() {
        assert_eq!(rate_limit_note(0, None), "");
        assert_eq!(
            rate_limit_note(0, Some(Duration::from_secs(3600))),
            "\nThe server asked to wait 3600 seconds before trying again."
        );
        assert_eq!(rate_limit_note(3, None), "\nRetries used: 3.");
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
