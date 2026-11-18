//! gRPC server HTTP/2 tuning configuration.

use std::time::Duration;

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
    /// Connection age at which the server closes a connection so clients
    /// reconnect and spread across pods behind an L4 load balancer. Unset or
    /// `null` turns the maximum age off: behind the Orbit Gateway, which balances
    /// each request, the closes rebalance nothing and can end running queries.
    pub max_connection_age_secs: Option<u64>,
    // Must stay paired with max_connection_age_secs: without a grace value,
    // tonic 0.14.5 panics with "async fn resumed after completion" when the
    // connection-age timer fires (hyperium/tonic#2522). Ignored when
    // max_connection_age_secs is unset, since no timer fires then.
    pub max_connection_age_grace_secs: u64,
    pub stream_timeout_secs: u64,
    pub max_header_list_size_bytes: u32,
    #[schemars(range(min = 65536, max = 4194304))]
    pub result_chunk_bytes: usize,
    #[schemars(range(min = 1048576))]
    pub max_result_bytes: usize,
}

/// The maximum connection age and the drain time after it, as the server
/// builder takes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxConnectionAge {
    pub age: Duration,
    pub grace: Duration,
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
        match self.max_connection_age_secs {
            Some(0) => return Err(GrpcConfigError::MaxConnectionAgeZero),
            Some(_) if self.max_connection_age_grace_secs == 0 => {
                return Err(GrpcConfigError::MaxConnectionAgeGraceZero);
            }
            _ => {}
        }
        Ok(())
    }

    /// The maximum connection age to apply, or `None` when it is turned off.
    /// With no age the grace is ignored.
    pub fn max_connection_age(&self) -> Option<MaxConnectionAge> {
        self.max_connection_age_secs.map(|age| MaxConnectionAge {
            age: Duration::from_secs(age),
            grace: Duration::from_secs(self.max_connection_age_grace_secs),
        })
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

    #[error(
        "grpc.max_connection_age_secs must not be 0; set it to null to turn the maximum \
         connection age off"
    )]
    MaxConnectionAgeZero,

    #[error(
        "grpc.max_connection_age_grace_secs must not be 0 while grpc.max_connection_age_secs \
         is set (hyperium/tonic#2522)"
    )]
    MaxConnectionAgeGraceZero,
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
        assert_eq!(cfg.max_connection_age_secs, Some(300));
        assert!(cfg.max_connection_age_grace_secs > 0);
        assert_eq!(
            cfg.max_connection_age(),
            Some(MaxConnectionAge {
                age: Duration::from_secs(300),
                grace: Duration::from_secs(30),
            })
        );
    }

    #[test]
    fn a_null_max_connection_age_turns_it_off() {
        let cfg = AppConfig::with_overlay("grpc:\n  max_connection_age_secs: null\n")
            .unwrap()
            .grpc;
        assert_eq!(cfg.max_connection_age_secs, None);
        assert_eq!(cfg.max_connection_age(), None);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn grace_is_ignored_without_a_max_connection_age() {
        let mut cfg = AppConfig::embedded_defaults().grpc;
        cfg.max_connection_age_secs = None;
        cfg.max_connection_age_grace_secs = 0;
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_connection_age(), None);
    }

    #[test]
    fn zero_grace_with_a_max_connection_age_is_rejected() {
        let mut cfg = AppConfig::embedded_defaults().grpc;
        cfg.max_connection_age_grace_secs = 0;
        assert!(matches!(
            cfg.validate(),
            Err(GrpcConfigError::MaxConnectionAgeGraceZero)
        ));
    }

    #[test]
    fn zero_max_connection_age_is_rejected() {
        let mut cfg = AppConfig::embedded_defaults().grpc;
        cfg.max_connection_age_secs = Some(0);
        assert!(matches!(
            cfg.validate(),
            Err(GrpcConfigError::MaxConnectionAgeZero)
        ));
    }

    #[test]
    fn a_null_grace_is_rejected_by_the_schema() {
        let err =
            AppConfig::with_overlay("grpc:\n  max_connection_age_grace_secs: null\n").unwrap_err();
        assert!(
            err.to_string().contains("max_connection_age_grace_secs"),
            "{err}"
        );
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
