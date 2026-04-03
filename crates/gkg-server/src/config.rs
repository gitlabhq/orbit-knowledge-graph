use tonic::transport::Identity;
use tonic::transport::server::ServerTlsConfig;

// Re-export everything from gkg-server-config for backward compatibility.
pub use gkg_server_config::{
    AppConfig, ConfigError, GitlabConfig, GrpcConfig, JwtConfig, MetricsConfig, OtelConfig,
    PrometheusConfig, SECRET_FILE_DIR, SharedAppConfig, TlsConfig,
};

/// Load TLS identity from the paths in [`TlsConfig`].
///
/// This lives in `gkg-server` (not `gkg-server-config`) because it depends on
/// `tonic`, which is a heavy runtime dependency.
pub async fn load_tls_config(tls: &TlsConfig) -> anyhow::Result<Option<ServerTlsConfig>> {
    match (&tls.cert_path, &tls.key_path) {
        (Some(cert_path), Some(key_path)) => {
            let cert = tokio::fs::read(cert_path).await?;
            let key = tokio::fs::read(key_path).await?;
            let identity = Identity::from_pem(cert, key);
            Ok(Some(ServerTlsConfig::new().identity(identity)))
        }
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!("both `tls.cert_path` and `tls.key_path` must be set to enable TLS")
        }
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;

    /// Reproduces the crash seen when deploying with GKG_ prefixed env vars.
    /// `try_parsing(true)` + `list_separator(",")` without `with_list_parse_key`
    /// wraps every value in a sequence, causing "invalid type: sequence, expected
    /// a string" errors for plain string fields.
    #[test]
    fn env_string_fields_not_parsed_as_lists() {
        let vars = [
            ("GKG_NATS__URL", "gkg-nats:4222"),
            ("GKG_GRAPH__URL", "http://clickhouse:8123"),
            ("GKG_GRAPH__DATABASE", "gkg"),
            ("GKG_GRAPH__USERNAME", "default"),
            ("GKG_GRAPH__PASSWORD", "supersecret"),
            ("GKG_METRICS__OTEL__ENDPOINT", "http://gkg-obs-alloy:4317"),
        ];

        // SAFETY: nextest runs each test in its own process, so env mutations are isolated
        unsafe {
            for (k, v) in &vars {
                std::env::set_var(k, v);
            }
        }

        let result = AppConfig::load();

        unsafe {
            for (k, _) in &vars {
                std::env::remove_var(k);
            }
        }

        let config = result.expect("AppConfig::load should not fail for plain string env vars");
        assert_eq!(config.nats.url, "gkg-nats:4222");
        assert_eq!(config.graph.password.as_deref(), Some("supersecret"));
        assert_eq!(config.metrics.otel.endpoint, "http://gkg-obs-alloy:4317");
    }

    #[test]
    fn health_check_services_parsed_as_list() {
        let vars = [(
            "GKG_HEALTH_CHECK__SERVICES",
            "siphon-consumer,nats,gkg-indexer",
        )];

        unsafe {
            for (k, v) in &vars {
                std::env::set_var(k, v);
            }
        }

        let result = AppConfig::load();

        unsafe {
            for (k, _) in &vars {
                std::env::remove_var(k);
            }
        }

        let config = result.expect("AppConfig::load should parse health_check.services as a list");
        assert_eq!(
            config.health_check.services,
            vec!["siphon-consumer", "nats", "gkg-indexer"],
        );
    }
}
