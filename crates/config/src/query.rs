//! Query execution configuration shared between server and compiler.
//!
//! `QueryConfig` is the single type for all ClickHouse query-level settings.
//! It is built by the compiler's settings phase, stored on `ParameterizedQuery`,
//! and read by both codegen (SQL SETTINGS clause) and the execution stage
//! (HTTP-level options).

use serde::{Deserialize, Serialize};

use crate::global::{DEFAULT_QUERY_CACHE_TTL, DEFAULT_TIMEOUT_SECS};

/// Compile-time default config.
pub static DEFAULT: QueryConfig = QueryConfig {
    timeout_secs: DEFAULT_TIMEOUT_SECS,
    use_query_cache: false,
    query_cache_ttl: DEFAULT_QUERY_CACHE_TTL,
};

/// Query execution settings. All fields map to ClickHouse query-level
/// settings. The closed set of fields prevents arbitrary user input from
/// reaching the SETTINGS clause (CWE-89).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryConfig {
    /// ClickHouse `max_execution_time`.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,

    /// ClickHouse `use_query_cache`. Enabled for cursor pagination.
    #[serde(default)]
    pub use_query_cache: bool,

    /// ClickHouse `query_cache_ttl` in seconds.
    #[serde(default = "default_cache_ttl")]
    pub query_cache_ttl: u32,
}

impl Default for QueryConfig {
    fn default() -> Self {
        DEFAULT.clone()
    }
}

impl QueryConfig {
    /// Returns ClickHouse SETTINGS as key-value pairs.
    pub fn to_settings(&self) -> Vec<(&'static str, String)> {
        let mut settings = vec![("max_execution_time", self.timeout_secs.to_string())];
        if self.use_query_cache {
            settings.push(("use_query_cache", "1".to_string()));
            settings.push(("query_cache_ttl", self.query_cache_ttl.to_string()));
        }
        settings
    }
}

fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT_SECS
}

fn default_cache_ttl() -> u32 {
    DEFAULT_QUERY_CACHE_TTL
}
