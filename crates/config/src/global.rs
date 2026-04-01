//! Global compile-time constants shared across all GKG crates.

use std::time::Duration;

use crate::query::QueryConfig;

/// Default query timeout in seconds.
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default ClickHouse query cache TTL in seconds (for cursor pagination).
pub const DEFAULT_QUERY_CACHE_TTL: u32 = 60;

/// Default query timeout in seconds.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(DEFAULT_TIMEOUT_SECS);

/// Compile-time default config. Used by the compiler and anywhere a
/// `QueryConfig` is needed without server-side deserialization.
pub const DEFAULT_QUERY_CONFIG: QueryConfig = QueryConfig {
    timeout_secs: Some(DEFAULT_TIMEOUT_SECS),
    use_query_cache: Some(false),
    query_cache_ttl: Some(DEFAULT_QUERY_CACHE_TTL),
};
