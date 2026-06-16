//! Process-wide feature flags.
//!
//! Mirrors the [`crate::query`] pattern: a typed config struct stored in a
//! global [`OnceLock`], installed once via [`init`] at startup and read from
//! anywhere via [`enabled`]. Every flag defaults to off, including before
//! [`init`] runs (so unit tests that never load config see all flags off).

use std::sync::OnceLock;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Each variant maps to one field of [`FeaturesConfig`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feature {
    /// SystemNotes derived-entity indexing (system-note reference edges).
    SystemNotes,
    /// Stop background merges on retired schema versions before GC drop.
    StopMergesOnRetire,
}

/// A feature flag scoped to root namespaces: `enabled` toggles it, an empty
/// `namespaces` list means every namespace, otherwise only those ids.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FeatureScope {
    pub enabled: bool,
    pub namespaces: Vec<i64>,
}

impl FeatureScope {
    fn enabled_for(&self, namespace_id: Option<i64>) -> bool {
        self.enabled
            && (self.namespaces.is_empty()
                || namespace_id.is_some_and(|id| self.namespaces.contains(&id)))
    }
}

/// Feature flag states, deserialized from the `features:` config section.
/// Every flag defaults to off; an unknown key is a config error.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FeaturesConfig {
    pub system_notes: FeatureScope,
    pub stop_merges_on_retire: bool,
}

impl FeaturesConfig {
    fn is_enabled(&self, feature: Feature) -> bool {
        match feature {
            Feature::SystemNotes => self.system_notes.enabled,
            Feature::StopMergesOnRetire => self.stop_merges_on_retire,
        }
    }

    fn is_enabled_for(&self, feature: Feature, namespace_id: Option<i64>) -> bool {
        match feature {
            Feature::SystemNotes => self.system_notes.enabled_for(namespace_id),
            Feature::StopMergesOnRetire => self.stop_merges_on_retire,
        }
    }
}

static FEATURES: OnceLock<FeaturesConfig> = OnceLock::new();

/// Call once at startup, after loading `AppConfig`.
pub fn init(features: FeaturesConfig) {
    FEATURES
        .set(features)
        .expect("gkg_server_config::features::init called twice");
}

/// Off until [`init`] has run. True when the feature is enabled for at least
/// one namespace; gate a specific namespace with [`enabled_for`].
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
        FeaturesConfig {
            system_notes: FeatureScope {
                enabled,
                namespaces,
            },
            ..Default::default()
        }
    }

    #[test]
    fn flags_default_to_off() {
        assert!(!FeaturesConfig::default().is_enabled(Feature::SystemNotes));
    }

    #[test]
    fn reads_an_enabled_flag() {
        assert!(scoped(true, vec![]).is_enabled(Feature::SystemNotes));
    }

    #[test]
    fn unrestricted_flag_enabled_for_every_namespace() {
        let features = scoped(true, vec![]);
        assert!(features.is_enabled_for(Feature::SystemNotes, Some(9970)));
        assert!(features.is_enabled_for(Feature::SystemNotes, None));
    }

    #[test]
    fn allowlisted_flag_enabled_only_for_listed_namespaces() {
        let features = scoped(true, vec![9970]);
        assert!(features.is_enabled_for(Feature::SystemNotes, Some(9970)));
        assert!(!features.is_enabled_for(Feature::SystemNotes, Some(1234)));
        assert!(!features.is_enabled_for(Feature::SystemNotes, None));
    }

    #[test]
    fn disabled_flag_off_even_with_allowlist() {
        assert!(!scoped(false, vec![9970]).is_enabled(Feature::SystemNotes));
        assert!(!scoped(false, vec![9970]).is_enabled_for(Feature::SystemNotes, Some(9970)));
    }

    #[test]
    fn deserializes_scoped_flag_from_yaml() {
        let features: FeaturesConfig =
            serde_yaml::from_str("system_notes:\n  enabled: true\n  namespaces: [9970]").unwrap();
        assert!(features.system_notes.enabled);
        assert_eq!(features.system_notes.namespaces, vec![9970]);
    }

    #[test]
    fn omitted_flag_defaults_off() {
        let features: FeaturesConfig = serde_yaml::from_str("{}").unwrap();
        assert!(!features.system_notes.enabled);
        assert!(features.system_notes.namespaces.is_empty());
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let result = serde_yaml::from_str::<FeaturesConfig>("not_a_real_flag: true");
        assert!(result.is_err());
    }

    #[test]
    fn unknown_scope_key_is_rejected() {
        let result = serde_yaml::from_str::<FeaturesConfig>("system_notes:\n  not_a_key: true");
        assert!(result.is_err());
    }
}
