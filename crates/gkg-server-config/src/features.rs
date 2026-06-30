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
pub enum Feature {
    /// Stop background merges on retired schema versions before GC drop.
    StopMergesOnRetire,
}

/// A feature flag scoped to root namespaces: `enabled` toggles it, an empty
/// `namespaces` list means every namespace, otherwise only those ids.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FeatureScope {
    pub enabled: bool,
    #[serde(deserialize_with = "deserialize_namespaces")]
    pub namespaces: Vec<i64>,
}

/// A list (YAML/JSON), a single id, or the comma-separated string a `GKG_*`
/// env var delivers (e.g. `9970,1234`) — so scoped flags are settable via env
/// without per-key config-source wiring.
fn deserialize_namespaces<'de, D>(deserializer: D) -> Result<Vec<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Namespaces {
        List(Vec<i64>),
        One(i64),
        CommaSeparated(String),
    }

    Ok(match Namespaces::deserialize(deserializer)? {
        Namespaces::List(ids) => ids,
        Namespaces::One(id) => vec![id],
        Namespaces::CommaSeparated(csv) => csv
            .split(',')
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(|id| id.parse().map_err(serde::de::Error::custom))
            .collect::<Result<_, _>>()?,
    })
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
        .expect("gkg_server_config::features::init called twice");
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

    fn scoped(enabled: bool, namespaces: Vec<i64>) -> FeaturesConfig {
        FeaturesConfig::from_iter([(
            Feature::StopMergesOnRetire,
            FeatureScope {
                enabled,
                namespaces,
            },
        )])
    }

    fn namespaces_of(features: &FeaturesConfig, feature: Feature) -> &[i64] {
        &features.0.get(&feature).expect("flag present").namespaces
    }

    #[test]
    fn flags_default_to_off() {
        assert!(!FeaturesConfig::default().is_enabled(Feature::StopMergesOnRetire));
    }

    #[test]
    fn reads_an_enabled_flag() {
        assert!(scoped(true, vec![]).is_enabled(Feature::StopMergesOnRetire));
    }

    #[test]
    fn unrestricted_flag_enabled_for_every_namespace() {
        let features = scoped(true, vec![]);
        assert!(features.is_enabled_for(Feature::StopMergesOnRetire, Some(9970)));
        assert!(features.is_enabled_for(Feature::StopMergesOnRetire, None));
    }

    #[test]
    fn allowlisted_flag_enabled_only_for_listed_namespaces() {
        let features = scoped(true, vec![9970]);
        assert!(features.is_enabled_for(Feature::StopMergesOnRetire, Some(9970)));
        assert!(!features.is_enabled_for(Feature::StopMergesOnRetire, Some(1234)));
        assert!(!features.is_enabled_for(Feature::StopMergesOnRetire, None));
    }

    #[test]
    fn disabled_flag_off_even_with_allowlist() {
        assert!(!scoped(false, vec![9970]).is_enabled(Feature::StopMergesOnRetire));
        assert!(!scoped(false, vec![9970]).is_enabled_for(Feature::StopMergesOnRetire, Some(9970)));
    }

    #[test]
    fn absent_flag_is_off() {
        let features = FeaturesConfig::default();
        assert!(!features.is_enabled(Feature::StopMergesOnRetire));
        assert!(!features.is_enabled_for(Feature::StopMergesOnRetire, Some(9970)));
    }

    #[test]
    fn deserializes_scoped_flag_from_yaml() {
        let features: FeaturesConfig =
            serde_yaml::from_str("stop_merges_on_retire:\n  enabled: true\n  namespaces: [9970]")
                .unwrap();
        assert!(features.is_enabled(Feature::StopMergesOnRetire));
        assert_eq!(
            namespaces_of(&features, Feature::StopMergesOnRetire),
            [9970]
        );
    }

    #[test]
    fn parses_namespaces_from_comma_separated_string() {
        let features: FeaturesConfig =
            serde_yaml::from_str("stop_merges_on_retire:\n  namespaces: \"9970, 1234\"").unwrap();
        assert_eq!(
            namespaces_of(&features, Feature::StopMergesOnRetire),
            [9970, 1234]
        );
    }

    #[test]
    fn parses_single_namespace_scalar() {
        let features: FeaturesConfig =
            serde_yaml::from_str("stop_merges_on_retire:\n  namespaces: 9970").unwrap();
        assert_eq!(
            namespaces_of(&features, Feature::StopMergesOnRetire),
            [9970]
        );
    }

    #[test]
    fn omitted_block_defaults_off() {
        let features: FeaturesConfig = serde_yaml::from_str("{}").unwrap();
        assert!(!features.is_enabled(Feature::StopMergesOnRetire));
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let result = serde_yaml::from_str::<FeaturesConfig>("not_a_real_flag: true");
        assert!(result.is_err());
    }

    #[test]
    fn unknown_scope_key_is_rejected() {
        let result =
            serde_yaml::from_str::<FeaturesConfig>("stop_merges_on_retire:\n  not_a_key: true");
        assert!(result.is_err());
    }
}
