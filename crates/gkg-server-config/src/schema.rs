//! Schema version and retention configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_max_retained_versions() -> u32 {
    2
}

fn default_version_poll_interval_secs() -> u64 {
    5
}

/// Schema configuration: version retention and related settings.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SchemaConfig {
    /// Number of schema version table-sets to retain.
    ///
    /// After migrating to version N, the indexer keeps the N active tables plus
    /// the N-1 rollback target. Must be at least 2.
    #[serde(default = "default_max_retained_versions")]
    #[schemars(range(min = 2))]
    pub max_retained_versions: u32,

    /// How often the webserver polls `gkg_schema_version` for the active
    /// version, in seconds. Must be at least 1.
    #[serde(default = "default_version_poll_interval_secs")]
    #[schemars(range(min = 1))]
    pub version_poll_interval_secs: u64,
}

impl SchemaConfig {
    /// Validates the schema config.
    pub fn validate(&self) -> Result<(), SchemaConfigError> {
        if self.max_retained_versions < 2 {
            return Err(SchemaConfigError::MaxRetainedVersionsTooLow(
                self.max_retained_versions,
            ));
        }
        if self.version_poll_interval_secs < 1 {
            return Err(SchemaConfigError::VersionPollIntervalTooLow);
        }
        Ok(())
    }
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            max_retained_versions: default_max_retained_versions(),
            version_poll_interval_secs: default_version_poll_interval_secs(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaConfigError {
    #[error(
        "schema.max_retained_versions must be at least 2 (got {0}); \
         at least one active and one rollback version must be kept"
    )]
    MaxRetainedVersionsTooLow(u32),

    #[error("schema.version_poll_interval_secs must be at least 1")]
    VersionPollIntervalTooLow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_passes_validation() {
        assert!(SchemaConfig::default().validate().is_ok());
    }

    #[test]
    fn max_retained_versions_one_fails() {
        let cfg = SchemaConfig {
            max_retained_versions: 1,
            version_poll_interval_secs: default_version_poll_interval_secs(),
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn max_retained_versions_zero_fails() {
        let cfg = SchemaConfig {
            max_retained_versions: 0,
            version_poll_interval_secs: default_version_poll_interval_secs(),
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn max_retained_versions_two_passes() {
        let cfg = SchemaConfig {
            max_retained_versions: 2,
            version_poll_interval_secs: default_version_poll_interval_secs(),
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn version_poll_interval_zero_fails() {
        let cfg = SchemaConfig {
            max_retained_versions: 2,
            version_poll_interval_secs: 0,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn version_poll_interval_one_passes() {
        let cfg = SchemaConfig {
            max_retained_versions: 2,
            version_poll_interval_secs: 1,
        };
        assert!(cfg.validate().is_ok());
    }
}
