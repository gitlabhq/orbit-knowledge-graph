//! gRPC server HTTP/2 tuning configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
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
    pub max_header_list_size_bytes: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppConfig;

    // Regression guard for hyperium/tonic#2522: a zero grace with a non-zero
    // max age reproduces the "async fn resumed after completion" panic.
    #[test]
    fn default_pairs_max_connection_age_with_nonzero_grace() {
        let cfg = AppConfig::embedded_defaults().grpc;
        assert!(cfg.max_connection_age_secs > 0);
        assert!(cfg.max_connection_age_grace_secs > 0);
    }

    #[test]
    fn missing_grace_field_is_rejected() {
        let yaml = r#"
            keepalive_interval_secs: 20
            keepalive_timeout_secs: 20
            tcp_keepalive_secs: 60
            connection_window_size: 2097152
            stream_window_size: 1048576
            concurrency_limit: 256
            max_connection_age_secs: 300
            stream_timeout_secs: 60
            max_header_list_size_bytes: 65536
        "#;
        let err = orbit_utils::yaml::from_str::<GrpcConfig>(yaml).unwrap_err();
        assert!(
            err.to_string().contains("max_connection_age_grace_secs"),
            "{err}"
        );
    }

    #[test]
    fn default_max_header_list_size_exceeds_hyper_default() {
        let cfg = AppConfig::embedded_defaults().grpc;
        assert!(cfg.max_header_list_size_bytes > 16 * 1024);
    }
}
