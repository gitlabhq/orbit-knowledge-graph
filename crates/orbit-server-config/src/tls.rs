//! TLS configuration.
//!
//! The struct definitions live here; the async `load_tls_config()` method that
//! depends on `tonic` stays in `orbit-server`.
//!
//! `cert_path` and `key_path` are the shared identity. The gRPC server uses
//! them directly. Each other group of listeners is off by default and may
//! either inherit that identity or name its own, so an external-facing
//! certificate and an internal one can be rotated on different cycles.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// HTTP listeners that answer probes: the webserver, the indexer and
    /// dispatcher health ports, and the health-check service.
    #[serde(default)]
    #[schemars(default)]
    pub probes: ListenerTlsConfig,
    /// The listener labkit serves: `/-/metrics`, and its own `/-/liveness` and
    /// `/-/readiness` on the same port.
    #[serde(default)]
    #[schemars(default)]
    pub metrics: ListenerTlsConfig,
}

/// TLS for one group of listeners.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ListenerTlsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Overrides `tls.cert_path` for this group. Must be set with `key_path`.
    pub cert_path: Option<String>,
    /// Overrides `tls.key_path` for this group. Must be set with `cert_path`.
    pub key_path: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error(
        "TLS is enabled for the {group} listeners but no certificate and key resolve: set \
         tls.{group}.cert_path and tls.{group}.key_path, or tls.cert_path and tls.key_path"
    )]
    Missing { group: &'static str },
    #[error(
        "tls.{group} sets only one of cert_path and key_path: set both to override the shared \
         identity, or neither to inherit it"
    )]
    HalfConfigured { group: &'static str },
}

impl TlsConfig {
    /// Certificate and key for the probe listeners, or `None` when they stay
    /// plaintext.
    pub fn probe_paths(&self) -> Result<Option<(&str, &str)>, IdentityError> {
        self.resolve(&self.probes, "probes")
    }

    /// Certificate and key for the metrics listener, or `None` when it stays
    /// plaintext.
    pub fn metrics_paths(&self) -> Result<Option<(&str, &str)>, IdentityError> {
        self.resolve(&self.metrics, "metrics")
    }

    fn resolve<'a>(
        &'a self,
        listener: &'a ListenerTlsConfig,
        group: &'static str,
    ) -> Result<Option<(&'a str, &'a str)>, IdentityError> {
        // A group carries an identity as a pair or not at all; falling back
        // per-half would silently mix one group's certificate with another's key.
        let own = match (listener.cert_path.as_deref(), listener.key_path.as_deref()) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => return Err(IdentityError::HalfConfigured { group }),
        };

        if !listener.enabled {
            if own.is_some() {
                tracing::warn!(
                    group,
                    "tls.{group} names a certificate but tls.{group}.enabled is false, so the \
                     listener stays plaintext"
                );
            }
            return Ok(None);
        }

        if let Some(pair) = own {
            return Ok(Some(pair));
        }

        match (self.cert_path.as_deref(), self.key_path.as_deref()) {
            (Some(cert), Some(key)) => Ok(Some((cert, key))),
            _ => Err(IdentityError::Missing { group }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::AppConfig;

    fn config_from(overlay: &str) -> AppConfig {
        config::Config::builder()
            .add_source(config::File::from_str(
                crate::EMBEDDED_DEFAULTS,
                config::FileFormat::Yaml,
            ))
            .add_source(config::File::from_str(overlay, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap()
    }

    #[test]
    fn listener_tls_is_off_by_default() {
        let tls = AppConfig::embedded_defaults().tls;

        assert!(!tls.probes.enabled);
        assert!(!tls.metrics.enabled);
        assert_eq!(tls.probe_paths().unwrap(), None);
        assert_eq!(tls.metrics_paths().unwrap(), None);
    }

    #[test]
    fn an_enabled_group_inherits_the_shared_identity() {
        let tls = config_from(
            "tls:\n  cert_path: /etc/tls/tls.crt\n  key_path: /etc/tls/tls.key\n  probes:\n    enabled: true\n",
        )
        .tls;

        assert_eq!(
            tls.probe_paths().unwrap(),
            Some(("/etc/tls/tls.crt", "/etc/tls/tls.key"))
        );
        assert_eq!(tls.metrics_paths().unwrap(), None);
    }

    #[test]
    fn a_group_can_override_the_shared_identity() {
        let tls = config_from(
            "tls:\n  cert_path: /etc/tls/tls.crt\n  key_path: /etc/tls/tls.key\n  probes:\n    enabled: true\n    cert_path: /etc/tls-internal/tls.crt\n    key_path: /etc/tls-internal/tls.key\n",
        )
        .tls;

        assert_eq!(
            tls.probe_paths().unwrap(),
            Some(("/etc/tls-internal/tls.crt", "/etc/tls-internal/tls.key"))
        );
    }

    #[test]
    fn an_enabled_group_without_an_identity_is_an_error() {
        let tls = config_from("tls:\n  metrics:\n    enabled: true\n").tls;

        let error = tls.metrics_paths().unwrap_err();

        assert!(error.to_string().contains("tls.metrics.cert_path"));
    }

    #[test]
    fn a_group_naming_only_one_half_of_an_identity_is_an_error() {
        let tls = config_from(
            "tls:\n  cert_path: /etc/tls/tls.crt\n  key_path: /etc/tls/tls.key\n  metrics:\n    enabled: true\n    cert_path: /etc/tls-internal/tls.crt\n",
        )
        .tls;

        let error = tls.metrics_paths().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("only one of cert_path and key_path"),
            "got: {error}"
        );
    }

    #[test]
    fn the_orbit_prd_overlay_still_parses_without_the_new_groups() {
        let tls =
            config_from("tls:\n  cert_path: /etc/tls/tls.crt\n  key_path: /etc/tls/tls.key\n").tls;

        assert!(!tls.probes.enabled);
        assert_eq!(tls.probe_paths().unwrap(), None);
    }

    #[test]
    fn a_misspelled_key_is_rejected_rather_than_ignored() {
        // Both halves matter: a typo in the group name and a typo in a key
        // inside it would each leave the listener plaintext while the config
        // looks right.
        for overlay in [
            "tls:\n  probes:\n    enable: true\n",
            "tls:\n  probe:\n    enabled: true\n",
        ] {
            let result: Result<AppConfig, _> = config::Config::builder()
                .add_source(config::File::from_str(
                    crate::EMBEDDED_DEFAULTS,
                    config::FileFormat::Yaml,
                ))
                .add_source(config::File::from_str(overlay, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize();

            assert!(
                result.is_err(),
                "a typo must not leave the listener silently plaintext: {overlay}"
            );
        }
    }
}
