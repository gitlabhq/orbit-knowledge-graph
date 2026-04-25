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
    // Must stay paired with max_connection_age_secs: without a grace value,
    // tonic 0.14.5 panics with "async fn resumed after completion" when the
    // connection-age timer fires (hyperium/tonic#2522).
    pub max_connection_age_grace_secs: u64,
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
            max_connection_age_grace_secs: 30,
            stream_timeout_secs: 60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression guard for hyperium/tonic#2522: a zero grace with a non-zero
    // max age reproduces the "async fn resumed after completion" panic.
    #[test]
    fn default_pairs_max_connection_age_with_nonzero_grace() {
        let cfg = GrpcConfig::default();
        assert!(cfg.max_connection_age_secs > 0);
        assert!(cfg.max_connection_age_grace_secs > 0);
    }

    #[test]
    fn deserializes_without_grace_field_using_default() {
        let yaml = r#"
            keepalive_interval_secs: 20
            keepalive_timeout_secs: 20
            tcp_keepalive_secs: 60
            connection_window_size: 2097152
            stream_window_size: 1048576
            concurrency_limit: 256
            max_connection_age_secs: 300
            stream_timeout_secs: 60
        "#;
        let cfg: GrpcConfig = serde_yaml::from_str(yaml).expect("valid config");
        assert_eq!(cfg.max_connection_age_grace_secs, 30);
    }
}
