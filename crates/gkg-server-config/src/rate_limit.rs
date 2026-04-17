//! Rate limiting configuration for the gRPC query server.
//!
//! Two independent controls:
//!
//! - **Global concurrency**: a server-wide semaphore limiting how many queries
//!   execute simultaneously across all connections. Prevents ClickHouse overload
//!   regardless of how many clients connect.
//!
//! - **Per-user rate**: a sliding-window rate limit keyed by `user_id` from the
//!   JWT claims. Prevents a single user from monopolizing query capacity.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Maximum number of queries executing concurrently across all connections.
    /// 0 disables the global concurrency limit.
    pub max_concurrent_queries: usize,

    /// Maximum queries a single user can execute per `window_secs` interval.
    /// 0 disables per-user rate limiting.
    pub per_user_max_requests: u32,

    /// Sliding window duration in seconds for per-user rate limiting.
    pub per_user_window_secs: u64,

    /// Maximum number of distinct user buckets to track. Oldest entries are
    /// evicted when this limit is reached. Bounds memory usage.
    pub per_user_max_entries: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: 64,
            per_user_max_requests: 100,
            per_user_window_secs: 60,
            per_user_max_entries: 10_000,
        }
    }
}
