//! Schema version and retention configuration.

use serde::{Deserialize, Serialize};

/// Schema configuration: version retention and related settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaConfig {
    /// Number of schema version table-sets to retain.
    ///
    /// After migrating to version N, the indexer keeps the N active tables plus
    /// the N-1 rollback target. Must be at least 2.
    pub max_retained_versions: u32,

    /// How long the webserver waits before retrying a failed snapshot install
    /// or reopening a lost active-version watch, in seconds. Also the base
    /// backoff of the indexer readiness gate. Must be at least 1.
    pub version_poll_interval_secs: u64,

    /// How long the indexer waits for the dispatcher to prepare its schema
    /// version before exiting non-zero (relying on the orchestrator to restart
    /// it). The indexer retries with backoff within this budget. Must be at
    /// least 1.
    pub indexer_schema_wait_timeout_secs: u64,
}

#[cfg(test)]
mod tests {
    use crate::AppConfig;

    fn rejection(key: &str, value: u64) -> String {
        AppConfig::with_overlay(&format!("schema:\n  {key}: {value}\n"))
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn max_retained_versions_below_two_is_rejected() {
        for value in [0, 1] {
            let error = rejection("max_retained_versions", value);
            assert!(
                error.contains("- schema.max_retained_versions: "),
                "{error}"
            );
        }
    }

    #[test]
    fn version_poll_interval_zero_is_rejected() {
        let error = rejection("version_poll_interval_secs", 0);
        assert!(
            error.contains("- schema.version_poll_interval_secs: "),
            "{error}"
        );
    }

    #[test]
    fn indexer_schema_wait_timeout_zero_is_rejected() {
        let error = rejection("indexer_schema_wait_timeout_secs", 0);
        assert!(
            error.contains("- schema.indexer_schema_wait_timeout_secs: "),
            "{error}"
        );
    }

    #[test]
    fn the_lowest_allowed_values_load() {
        let schema = AppConfig::with_overlay(
            "schema:\n  max_retained_versions: 2\n  version_poll_interval_secs: 1\n  indexer_schema_wait_timeout_secs: 1\n",
        )
        .unwrap()
        .schema;
        assert_eq!(schema.max_retained_versions, 2);
        assert_eq!(schema.version_poll_interval_secs, 1);
        assert_eq!(schema.indexer_schema_wait_timeout_secs, 1);
    }

    #[test]
    fn indexer_schema_wait_timeout_defaults_to_five_minutes() {
        assert_eq!(
            AppConfig::embedded_defaults()
                .schema
                .indexer_schema_wait_timeout_secs,
            300
        );
    }
}
