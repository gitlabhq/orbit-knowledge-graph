//! Configuration for the extensions platform: the downstream providers Orbit
//! fetches declarative packages from.
//!
//! The feature is off by default. With no `extensions.providers`, Orbit loads no
//! package and behaves as before. Each provider entry names a gRPC endpoint, the
//! file holding the shared HS256 key Orbit signs its per-call JWT with, and how
//! often Orbit re-checks the provider's package digests.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ExtensionsConfig {
    #[serde(default)]
    pub providers: Vec<ExtensionProvider>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ExtensionProvider {
    pub name: String,
    pub url: String,
    pub jwt_key_file: String,
    pub refresh_secs: u64,
}

impl ExtensionsConfig {
    /// Whether any provider is configured. The caller skips every extensions
    /// code path when this is false, so the default build is unchanged.
    pub fn is_enabled(&self) -> bool {
        !self.providers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_providers_means_disabled() {
        let config: ExtensionsConfig = orbit_utils::yaml::from_str("{}").unwrap();
        assert!(!config.is_enabled());
    }

    #[test]
    fn a_configured_provider_enables_the_feature() {
        let config: ExtensionsConfig = orbit_utils::yaml::from_str(
            "providers:\n  - {name: p, url: 'http://p:8081', jwt_key_file: /etc/secrets/p/key, refresh_secs: 60}",
        )
        .unwrap();
        assert!(config.is_enabled());
        assert_eq!(config.providers[0].name, "p");
    }

    #[test]
    fn an_unknown_provider_field_is_rejected() {
        let result = orbit_utils::yaml::from_str::<ExtensionsConfig>(
            "providers:\n  - {name: p, url: u, jwt_key_file: k, refresh_secs: 60, extra: 1}",
        );
        assert!(result.is_err());
    }
}
