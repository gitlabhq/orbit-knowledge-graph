//! Server-wide rate limiting: global concurrency semaphore + per-user
//! sliding window.
//!
//! Injected into `KnowledgeGraphServiceImpl` and checked at the top of
//! `execute_query` before any pipeline work begins.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use gkg_server_config::RateLimitConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::metrics;

/// Shared, cheaply cloneable rate limiter.
#[derive(Clone)]
pub struct QueryRateLimiter {
    inner: Arc<Inner>,
}

struct Inner {
    config: RateLimitConfig,
    global_semaphore: Option<Arc<Semaphore>>,
    user_windows: Mutex<UserWindowMap>,
}

/// Per-user sliding window tracker.
struct UserWindowMap {
    /// Circular buffer of (user_id, timestamps).
    entries: HashMap<u64, Vec<Instant>>,
    max_entries: usize,
}

impl UserWindowMap {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
        }
    }

    /// Returns `true` if the user is within their rate limit and records
    /// the request. Returns `false` if they've exceeded it.
    fn check_and_record(
        &mut self,
        user_id: u64,
        max_requests: u32,
        window: std::time::Duration,
    ) -> bool {
        let now = Instant::now();
        let cutoff = now - window;

        let timestamps = self.entries.entry(user_id).or_default();

        // Evict expired entries for this user.
        timestamps.retain(|t| *t > cutoff);

        if timestamps.len() >= max_requests as usize {
            return false;
        }

        timestamps.push(now);

        // Evict oldest users if we've exceeded the max tracked entries.
        if self.entries.len() > self.max_entries {
            // Find the user with the oldest most-recent request and remove them.
            let oldest_user = self
                .entries
                .iter()
                .filter(|(id, _)| **id != user_id)
                .min_by_key(|(_, ts)| ts.last().copied())
                .map(|(id, _)| *id);
            if let Some(id) = oldest_user {
                self.entries.remove(&id);
            }
        }

        true
    }
}

/// Why a request was rejected by the rate limiter.
#[derive(Debug)]
pub enum RateLimitRejection {
    /// Server-wide concurrency limit reached.
    GlobalConcurrency,
    /// Per-user request rate exceeded.
    UserRateExceeded,
}

impl std::fmt::Display for RateLimitRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GlobalConcurrency => write!(f, "server concurrency limit reached"),
            Self::UserRateExceeded => write!(f, "per-user rate limit exceeded"),
        }
    }
}

impl QueryRateLimiter {
    pub fn new(config: &RateLimitConfig) -> Self {
        let global_semaphore = if config.max_concurrent_queries > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_queries)))
        } else {
            None
        };

        Self {
            inner: Arc::new(Inner {
                config: config.clone(),
                global_semaphore,
                user_windows: Mutex::new(UserWindowMap::new(config.per_user_max_entries)),
            }),
        }
    }

    /// Try to acquire a query slot. Returns a guard that must be held for
    /// the duration of the query, or a rejection reason.
    ///
    /// Checks run in this order so that a global concurrency rejection does
    /// not consume the caller's per-user budget:
    /// 1. Global concurrency semaphore (non-blocking try_acquire).
    /// 2. Per-user sliding window rate limit (fail-fast, no blocking).
    pub fn try_acquire(&self, user_id: u64) -> Result<QueryPermit, RateLimitRejection> {
        let cfg = &self.inner.config;

        // Global concurrency first -- acquiring the semaphore does not
        // consume per-user budget, so users aren't penalised for server-wide load.
        let permit = if let Some(sem) = &self.inner.global_semaphore {
            match Arc::clone(sem).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    metrics::record_rate_limit_rejected("global_concurrency");
                    return Err(RateLimitRejection::GlobalConcurrency);
                }
            }
        } else {
            None
        };

        // Per-user check -- only record the timestamp after the global slot
        // is secured.
        if cfg.per_user_max_requests > 0 {
            let window = std::time::Duration::from_secs(cfg.per_user_window_secs);
            let mut windows = self
                .inner
                .user_windows
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !windows.check_and_record(user_id, cfg.per_user_max_requests, window) {
                metrics::record_rate_limit_rejected("per_user");
                return Err(RateLimitRejection::UserRateExceeded);
            }
        }

        Ok(QueryPermit { _permit: permit })
    }
}

/// RAII guard that holds a global concurrency slot for the duration of a query.
/// Drop releases the slot back to the semaphore.
#[derive(Debug)]
pub struct QueryPermit {
    _permit: Option<OwnedSemaphorePermit>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(concurrent: usize, per_user: u32, window_secs: u64) -> RateLimitConfig {
        RateLimitConfig {
            max_concurrent_queries: concurrent,
            per_user_max_requests: per_user,
            per_user_window_secs: window_secs,
            per_user_max_entries: 100,
        }
    }

    #[test]
    fn global_concurrency_limits_total_queries() {
        let limiter = QueryRateLimiter::new(&test_config(2, 0, 60));

        let p1 = limiter.try_acquire(1);
        assert!(p1.is_ok());
        let p2 = limiter.try_acquire(2);
        assert!(p2.is_ok());

        // Third should fail — two slots taken.
        let p3 = limiter.try_acquire(3);
        assert!(matches!(
            p3.unwrap_err(),
            RateLimitRejection::GlobalConcurrency
        ));

        // Drop one permit, now it should work.
        drop(p1);
        let p4 = limiter.try_acquire(3);
        assert!(p4.is_ok());
    }

    #[test]
    fn per_user_rate_limits_individual_users() {
        let limiter = QueryRateLimiter::new(&test_config(0, 3, 60));

        // User 1 gets 3 requests.
        assert!(limiter.try_acquire(1).is_ok());
        assert!(limiter.try_acquire(1).is_ok());
        assert!(limiter.try_acquire(1).is_ok());

        // Fourth should fail.
        assert!(matches!(
            limiter.try_acquire(1).unwrap_err(),
            RateLimitRejection::UserRateExceeded
        ));

        // User 2 is unaffected.
        assert!(limiter.try_acquire(2).is_ok());
    }

    #[test]
    fn both_limits_applied_together() {
        let limiter = QueryRateLimiter::new(&test_config(2, 5, 60));

        let p1 = limiter.try_acquire(1);
        assert!(p1.is_ok());
        let p2 = limiter.try_acquire(1);
        assert!(p2.is_ok());

        // User 1 has budget left (5) but global is full (2).
        assert!(matches!(
            limiter.try_acquire(1).unwrap_err(),
            RateLimitRejection::GlobalConcurrency
        ));
    }

    #[test]
    fn disabled_when_zeros() {
        let limiter = QueryRateLimiter::new(&test_config(0, 0, 60));

        // Should always succeed.
        for _ in 0..1000 {
            assert!(limiter.try_acquire(1).is_ok());
        }
    }

    #[test]
    fn default_config_is_sane() {
        let config = RateLimitConfig::default();
        let limiter = QueryRateLimiter::new(&config);
        assert!(limiter.try_acquire(42).is_ok());
    }

    #[test]
    fn global_rejection_does_not_consume_per_user_budget() {
        // 1 global slot, 2 per-user budget.
        let limiter = QueryRateLimiter::new(&test_config(1, 2, 60));

        // Take the only global slot.
        let _p1 = limiter.try_acquire(1).unwrap();

        // User 2 is rejected by the global limit.
        assert!(matches!(
            limiter.try_acquire(2).unwrap_err(),
            RateLimitRejection::GlobalConcurrency
        ));

        // Free the global slot.
        drop(_p1);

        // User 2 should still have full per-user budget (both requests).
        assert!(limiter.try_acquire(2).is_ok());
        assert!(limiter.try_acquire(2).is_ok());
    }
}
