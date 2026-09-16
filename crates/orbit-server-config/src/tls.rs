//! TLS configuration.
//!
//! The struct definition lives here; the async `load_tls_config()` method
//! that depends on `tonic` stays in `orbit-server`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// PEM bundle of extra root certificates trusted by every outbound TLS
    /// client (GitLab API, ClickHouse, NATS, object storage), in addition to
    /// the platform trust store. Must exist and hold at least one certificate.
    pub ca_bundle_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::AppConfig;

    #[test]
    fn ca_bundle_path_is_unset_by_default() {
        assert!(AppConfig::embedded_defaults().tls.ca_bundle_path.is_none());
    }

    #[test]
    fn ca_bundle_path_is_read_from_an_overlay() {
        let overlay = "tls:\n  ca_bundle_path: /etc/ssl/private-ca.pem\n";
        let config: AppConfig = config::Config::builder()
            .add_source(config::File::from_str(
                crate::EMBEDDED_DEFAULTS,
                config::FileFormat::Yaml,
            ))
            .add_source(config::File::from_str(overlay, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert_eq!(
            config.tls.ca_bundle_path.as_deref(),
            Some("/etc/ssl/private-ca.pem")
        );
    }
}
