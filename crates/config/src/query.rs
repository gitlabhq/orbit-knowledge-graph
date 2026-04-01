//! Query execution configuration shared between server and compiler.
//!
//! `QueryConfig` is the single type for all ClickHouse query-level settings.
//! It is deserialized from `AppConfig.query` on the server side, stored in
//! the pipeline context, and read by both the execution stage (HTTP-level
//! options) and codegen (SQL SETTINGS clause).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Query execution settings. All fields map to ClickHouse query-level
/// settings. The closed set of fields prevents arbitrary user input from
/// reaching the SETTINGS clause (CWE-89).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct QueryConfig {
    /// ClickHouse `max_execution_time` in seconds.
    #[serde(rename = "max_execution_time")]
    pub timeout_secs: Option<u64>,

    /// ClickHouse `use_query_cache`. Enabled for cursor pagination.
    pub use_query_cache: Option<bool>,

    /// ClickHouse `query_cache_ttl` in seconds.
    pub query_cache_ttl: Option<u32>,
}

impl QueryConfig {
    /// Returns ClickHouse SETTINGS as key-value pairs, skipping unset (`None`) fields.
    pub fn to_clickhouse_settings(&self) -> Vec<(String, String)> {
        let Value::Object(map) =
            serde_json::to_value(self).expect("QueryConfig is always serializable")
        else {
            unreachable!()
        };
        map.into_iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, v)| {
                let s = match v {
                    Value::Bool(b) => if b { "1" } else { "0" }.to_string(),
                    other => other.to_string(),
                };
                (k, s)
            })
            .collect()
    }
}
