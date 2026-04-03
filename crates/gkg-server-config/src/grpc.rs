//! gRPC server HTTP/2 tuning configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct GrpcConfig {
    pub keepalive_interval_secs: u64,
    pub keepalive_timeout_secs: u64,
    pub tcp_keepalive_secs: u64,
    pub connection_window_size: u32,
    pub stream_window_size: u32,
    pub concurrency_limit: usize,
    pub max_connection_age_secs: u64,
    pub stream_timeout_secs: u64,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            keepalive_interval_secs: 20,
            keepalive_timeout_secs: 20,
            tcp_keepalive_secs: 60,
            connection_window_size: 2 * 1024 * 1024,
            stream_window_size: 1024 * 1024,
            concurrency_limit: 256,
            max_connection_age_secs: 300,
            stream_timeout_secs: 60,
        }
    }
}
