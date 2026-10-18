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
}

const RESULT_CHUNK_BYTES: std::ops::RangeInclusive<usize> = 64 * 1024..=4 * 1024 * 1024;
const MAX_RESULT_BYTES_FLOOR: usize = 1024 * 1024;

impl GrpcConfig {
    pub fn validate(&self) -> Result<(), GrpcConfigError> {
        if !RESULT_CHUNK_BYTES.contains(&self.result_chunk_bytes) {
            return Err(GrpcConfigError::ResultChunkBytesOutOfRange(
                self.result_chunk_bytes,
            ));
        }
        if self.max_result_bytes < MAX_RESULT_BYTES_FLOOR {
            return Err(GrpcConfigError::MaxResultBytesTooLow(self.max_result_bytes));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GrpcConfigError {
    #[error(
        "grpc.result_chunk_bytes must be between 65536 and 4194304 (got {0}); \
         GitLab Rails rejects gRPC messages over 8 MiB"
    )]
    ResultChunkBytesOutOfRange(usize),

    #[error("grpc.max_result_bytes must be at least 1048576 (got {0})")]
    MaxResultBytesTooLow(usize),
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
    fn result_chunk_bytes_must_stay_in_range() {
        let mut cfg = AppConfig::embedded_defaults().grpc;
        assert!(cfg.validate().is_ok());
        for bytes in [0, 1, 8 * 1024 * 1024] {
            cfg.result_chunk_bytes = bytes;
            assert!(cfg.validate().is_err(), "{bytes} must be rejected");
        }
    }

    #[test]
    fn max_result_bytes_has_a_floor() {
        let mut cfg = AppConfig::embedded_defaults().grpc;
        cfg.max_result_bytes = 1024;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn default_max_header_list_size_exceeds_hyper_default() {
        let cfg = AppConfig::embedded_defaults().grpc;
        assert!(cfg.max_header_list_size_bytes > 16 * 1024);
    }
}
