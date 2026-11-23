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
    /// reconnect and spread across pods behind an L4 load balancer. Defaults to
    /// 300. Set it to `null` to turn the maximum age off: behind the Orbit
    /// Gateway, which balances each request, the closes rebalance nothing and
    /// can end running queries.
    #[schemars(range(min = 1))]
    pub max_connection_age_secs: Option<u64>,
    // Must stay paired with max_connection_age_secs: without a grace value,
    // tonic 0.14.5 panics with "async fn resumed after completion" when the
    // connection-age timer fires (hyperium/tonic#2522). The server ignores it
    // when max_connection_age_secs is unset, but it must still be at least 1.
    #[schemars(range(min = 1))]
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

impl GrpcConfig {
    /// The maximum connection age to apply, or `None` when it is turned off.
    /// With no age the grace is ignored.
    pub fn max_connection_age(&self) -> Option<MaxConnectionAge> {
        self.max_connection_age_secs.map(|age| MaxConnectionAge {
            age: Duration::from_secs(age),
            grace: Duration::from_secs(self.max_connection_age_grace_secs),
        })
    }
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
    }

    #[test]
    fn zero_grace_is_rejected() {
        for overlay in [
            "grpc:\n  max_connection_age_grace_secs: 0\n",
            "grpc:\n  max_connection_age_secs: null\n  max_connection_age_grace_secs: 0\n",
        ] {
            let error = AppConfig::with_overlay(overlay).unwrap_err().to_string();
            assert!(
                error.contains("- grpc.max_connection_age_grace_secs: "),
                "{overlay}: {error}"
            );
        }
    }

    #[test]
    fn zero_max_connection_age_is_rejected() {
        let error = rejection("max_connection_age_secs", 0);
        assert!(
            error.contains("- grpc.max_connection_age_secs: "),
            "{error}"
        );
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
    fn default_max_header_list_size_exceeds_hyper_default() {
        let cfg = AppConfig::embedded_defaults().grpc;
        assert!(cfg.max_header_list_size_bytes > 16 * 1024);
    }
}
