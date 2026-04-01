//! Global compile-time constants shared across all GKG crates.

use std::time::Duration;

/// Default query timeout in seconds.
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default ClickHouse query cache TTL in seconds (for cursor pagination).
pub const DEFAULT_QUERY_CACHE_TTL: u32 = 60;

/// Default query timeout as a Duration.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
