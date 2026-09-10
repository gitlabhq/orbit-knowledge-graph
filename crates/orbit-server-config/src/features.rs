//! Process-wide feature flags.
//!
//! Mirrors the [`crate::query`] pattern: a typed config struct stored in a
//! global [`OnceLock`], installed once via [`init`] at startup and read from
//! anywhere via [`enabled`]. Every flag defaults to off, including before
//! [`init`] runs (so unit tests that never load config see all flags off).

use std::collections::HashMap;
use std::sync::OnceLock;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Feature {}

/// A feature flag scoped to root namespaces: `enabled` toggles it, an empty
/// `namespaces` list means every namespace, otherwise only those ids.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeatureScope {
    pub enabled: bool,
    #[serde(default)]
    pub namespaces: Vec<i64>,
}

impl FeatureScope {
    fn enabled_for(&self, namespace_id: Option<i64>) -> bool {
        self.enabled
            && (self.namespaces.is_empty()
                || namespace_id.is_some_and(|id| self.namespaces.contains(&id)))
    }
}

/// An unrecognized key fails deserialization as an unknown [`Feature`] variant,
/// rather than being silently ignored.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct FeaturesConfig(HashMap<Feature, FeatureScope>);

impl FeaturesConfig {
    pub(crate) fn is_enabled(&self, feature: Feature) -> bool {
        self.0.get(&feature).is_some_and(|scope| scope.enabled)
    }

    pub(crate) fn is_enabled_for(&self, feature: Feature, namespace_id: Option<i64>) -> bool {
        self.0
            .get(&feature)
            .is_some_and(|scope| scope.enabled_for(namespace_id))
    }
}

impl FromIterator<(Feature, FeatureScope)> for FeaturesConfig {
    fn from_iter<I: IntoIterator<Item = (Feature, FeatureScope)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

static FEATURES: OnceLock<FeaturesConfig> = OnceLock::new();

/// Call once at startup, after loading `AppConfig`.
pub fn init(features: FeaturesConfig) {
    FEATURES
        .set(features)
        .expect("orbit_server_config::features::init called twice");
}

/// Off until [`init`] has run. Reads only the `enabled` flag, ignoring any
/// `namespaces` allowlist; gate a specific namespace with [`enabled_for`].
pub fn enabled(feature: Feature) -> bool {
    FEATURES
        .get()
        .is_some_and(|features| features.is_enabled(feature))
}

/// Off until [`init`] has run. Gates a namespace-scoped feature against a root
/// namespace id. `None` is global-scope work, which passes only when the
/// feature is unrestricted (no `namespaces` allowlist).
pub fn enabled_for(feature: Feature, namespace_id: Option<i64>) -> bool {
    FEATURES
        .get()
        .is_some_and(|features| features.is_enabled_for(feature, namespace_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_deserializes() {
        let features: FeaturesConfig = orbit_utils::yaml::from_str("{}").unwrap();
        assert!(features.0.is_empty());
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let result = orbit_utils::yaml::from_str::<FeaturesConfig>("not_a_real_flag: true");
        assert!(result.is_err());
    }
}
