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
    #[schemars(range(min = 65536, max = 4194304))]
    pub result_chunk_bytes: usize,
    #[schemars(range(min = 1048576))]
    pub max_result_bytes: usize,
    /// How long the server waits on shutdown for in-flight requests to
    /// finish before it stops waiting and exits; requests still running then
    /// end with the process. Keep it above the longest query time and below
    /// the Kubernetes `terminationGracePeriodSeconds`.
    #[schemars(range(min = 1))]
    pub shutdown_drain_timeout_secs: u64,
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

    fn rejection(key: &str, value: u64) -> String {
        AppConfig::with_overlay(&format!("grpc:\n  {key}: {value}\n"))
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn result_chunk_bytes_must_stay_in_range() {
        for bytes in [0, 1, 65535, 4 * 1024 * 1024 + 1, 8 * 1024 * 1024] {
            let error = rejection("result_chunk_bytes", bytes);
            assert!(
                error.contains("- grpc.result_chunk_bytes: "),
                "{bytes}: {error}"
            );
        }
        for bytes in [64 * 1024, 4 * 1024 * 1024] {
            AppConfig::with_overlay(&format!("grpc:\n  result_chunk_bytes: {bytes}\n")).unwrap();
        }
    }

    #[test]
    fn max_result_bytes_has_a_floor() {
        let error = rejection("max_result_bytes", 1024 * 1024 - 1);
        assert!(error.contains("- grpc.max_result_bytes: "), "{error}");
        AppConfig::with_overlay("grpc:\n  max_result_bytes: 1048576\n").unwrap();
    }

    #[test]
    fn shutdown_drain_timeout_cannot_be_zero() {
        let error = rejection("shutdown_drain_timeout_secs", 0);
        assert!(
            error.contains("- grpc.shutdown_drain_timeout_secs: "),
            "{error}"
        );
        AppConfig::with_overlay("grpc:\n  shutdown_drain_timeout_secs: 1\n").unwrap();
    }

    #[test]
    fn default_shutdown_drain_outlasts_the_longest_query() {
        let defaults = AppConfig::embedded_defaults();
        let longest_query_secs = std::iter::once(&defaults.query.default)
            .chain(defaults.query.overrides.values())
            .filter_map(|query| query.max_execution_time)
            .chain([defaults.grpc.stream_timeout_secs])
            .max()
            .unwrap();
        assert!(defaults.grpc.shutdown_drain_timeout_secs > longest_query_secs);
    }

    #[test]
    fn default_max_header_list_size_exceeds_hyper_default() {
        let cfg = AppConfig::embedded_defaults().grpc;
        assert!(cfg.max_header_list_size_bytes > 16 * 1024);
    }
}
