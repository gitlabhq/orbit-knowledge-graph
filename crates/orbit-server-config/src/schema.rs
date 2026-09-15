//! Schema version and retention configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Schema configuration: version retention and related settings.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SchemaConfig {
    /// Number of schema version table-sets to retain.
    ///
    /// After migrating to version N, the indexer keeps the N active tables plus
    /// the N-1 rollback target. Must be at least 2.
    #[schemars(range(min = 2))]
    pub max_retained_versions: u32,

    /// How long the webserver waits before retrying a failed snapshot install
    /// or reopening a lost active-version watch, in seconds. Also the base
    /// backoff of the indexer readiness gate. Must be at least 1.
    #[schemars(range(min = 1))]
    pub version_poll_interval_secs: u64,

    /// How long the indexer waits for the dispatcher to prepare its schema
    /// version before exiting non-zero (relying on the orchestrator to restart
    /// it). The indexer retries with backoff within this budget. Must be at
    /// least 1.
    #[schemars(range(min = 1))]
    pub indexer_schema_wait_timeout_secs: u64,
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
        if self.indexer_schema_wait_timeout_secs < 1 {
            return Err(SchemaConfigError::IndexerSchemaWaitTimeoutTooLow);
        }
        Ok(())
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

    #[error("schema.indexer_schema_wait_timeout_secs must be at least 1")]
    IndexerSchemaWaitTimeoutTooLow,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppConfig;

    fn defaults() -> SchemaConfig {
        AppConfig::embedded_defaults().schema
    }

    #[test]
    fn default_passes_validation() {
        assert!(defaults().validate().is_ok());
    }

    #[test]
    fn max_retained_versions_one_fails() {
        let cfg = SchemaConfig {
            max_retained_versions: 1,
            ..defaults()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn max_retained_versions_zero_fails() {
        let cfg = SchemaConfig {
            max_retained_versions: 0,
            ..defaults()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn max_retained_versions_two_passes() {
        let cfg = SchemaConfig {
            max_retained_versions: 2,
            ..defaults()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn version_poll_interval_zero_fails() {
        let cfg = SchemaConfig {
            version_poll_interval_secs: 0,
            ..defaults()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn version_poll_interval_one_passes() {
        let cfg = SchemaConfig {
            version_poll_interval_secs: 1,
            ..defaults()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn indexer_schema_wait_timeout_zero_fails() {
        let cfg = SchemaConfig {
            indexer_schema_wait_timeout_secs: 0,
            ..defaults()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn indexer_schema_wait_timeout_defaults_to_five_minutes() {
        assert_eq!(defaults().indexer_schema_wait_timeout_secs, 300);
    }
}
