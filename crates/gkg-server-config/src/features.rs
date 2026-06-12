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

/// Feature flag states, deserialized from the `features:` config section.
/// Every flag defaults to `false`; an unknown key is a config error.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FeaturesConfig {
    pub system_notes: bool,
    pub stop_merges_on_retire: bool,
}

impl FeaturesConfig {
    fn is_enabled(&self, feature: Feature) -> bool {
        match feature {
            Feature::SystemNotes => self.system_notes,
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

/// Off until [`init`] has run.
pub fn enabled(feature: Feature) -> bool {
    FEATURES
        .get()
        .is_some_and(|features| features.is_enabled(feature))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_default_to_off() {
        assert!(!FeaturesConfig::default().is_enabled(Feature::SystemNotes));
    }

    #[test]
    fn reads_an_enabled_flag() {
        let features = FeaturesConfig {
            system_notes: true,
            ..Default::default()
        };
        assert!(features.is_enabled(Feature::SystemNotes));
    }

    #[test]
    fn deserializes_set_flag_from_yaml() {
        let features: FeaturesConfig = serde_yaml::from_str("system_notes: true").unwrap();
        assert!(features.system_notes);
    }

    #[test]
    fn omitted_flag_defaults_off() {
        let features: FeaturesConfig = serde_yaml::from_str("{}").unwrap();
        assert!(!features.system_notes);
    }

    #[test]
    fn unknown_flag_is_rejected() {
        let result = serde_yaml::from_str::<FeaturesConfig>("not_a_real_flag: true");
        assert!(result.is_err());
    }
}
