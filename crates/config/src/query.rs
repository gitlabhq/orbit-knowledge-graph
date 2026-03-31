//! Query execution configuration shared between server and compiler.

use serde::{Deserialize, Serialize};

/// Default query timeout in seconds.
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Query execution settings. Deserialized from `AppConfig.query` on the
/// server side, and passed to both the compiler (for per-query SETTINGS
/// injection) and the execution stage (for HTTP-level `max_execution_time`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum seconds a query is allowed to run. Applied as both a
    /// `tokio::time::timeout` on the Rust pipeline and as ClickHouse
    /// `max_execution_time` per query. Defaults to 30s.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            timeout_secs: DEFAULT_TIMEOUT_SECS,
        }
    }
}

fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT_SECS
}

/// Allowed ClickHouse query-level settings. Closed enum prevents
/// arbitrary user input from reaching the SETTINGS clause (CWE-89).
#[derive(Debug, Clone, PartialEq)]
pub enum QuerySetting {
    UseQueryCache(bool),
    QueryCacheTtl(u32),
    MaxExecutionTime(u64),
}
