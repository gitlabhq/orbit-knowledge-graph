//! Query execution configuration shared between server and compiler.
//!
//! [`QuerySettings`] holds a default [`QueryConfig`] plus optional
//! per-query-type overrides, loaded from `AppConfig` at startup and
//! stored in a process-wide global via [`init`] / [`for_query_type`].

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ═════════════════════════════════════════════════════════════════════════════
// QueryConfig
// ═════════════════════════════════════════════════════════════════════════════

/// Query execution settings. All fields map to ClickHouse query-level
/// settings. The closed set of fields prevents arbitrary user input from
/// reaching the SETTINGS clause (CWE-89).
///
/// `None` means "not specified at this layer" — the merge logic in
/// [`QuerySettings::resolve`] fills in from the default.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct QueryConfig {
    /// ClickHouse `max_execution_time` in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_execution_time: Option<u64>,

    /// ClickHouse `use_query_cache`. Enabled for cursor pagination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_query_cache: Option<bool>,

    /// ClickHouse `query_cache_ttl` in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_cache_ttl: Option<u32>,
}

impl QueryConfig {
    /// Merge `overrides` on top of `self`. Fields set in `overrides`
    /// win; `None` fields fall through to `self`.
    pub fn merge(&self, overrides: &QueryConfig) -> QueryConfig {
        QueryConfig {
            max_execution_time: overrides.max_execution_time.or(self.max_execution_time),
            use_query_cache: overrides.use_query_cache.or(self.use_query_cache),
            query_cache_ttl: overrides.query_cache_ttl.or(self.query_cache_ttl),
        }
    }

    /// Returns ClickHouse SETTINGS as key-value pairs, skipping unset fields.
    ///
    /// Uses serde round-trip so that the field names stay in sync with the
    /// struct definition — no manual string mapping needed.
    pub fn to_clickhouse_settings(&self) -> Vec<(String, String)> {
        let Value::Object(map) =
            serde_json::to_value(self).expect("QueryConfig is always serializable")
        else {
            unreachable!()
        };
        map.into_iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, v)| {
                let s = match v {
                    Value::Bool(b) => if b { "1" } else { "0" }.to_string(),
                    other => other.to_string(),
                };
                (k, s)
            })
            .collect()
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// QuerySettings — per-query-type config map
// ═════════════════════════════════════════════════════════════════════════════

/// Top-level query settings loaded from YAML. Contains a `default` config
/// and optional per-query-type overrides keyed by snake_case query type
/// name (e.g. `traversal`, `aggregation`, `search`).
///
/// ```yaml
/// query:
///   default:
///     max_execution_time: 30
///     use_query_cache: false
///     query_cache_ttl: 60
///   aggregation:
///     max_execution_time: 60
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct QuerySettings {
    #[serde(default)]
    pub default: QueryConfig,

    /// Per-query-type overrides. Keys must match `QueryType` variant names
    /// in snake_case. Validated at startup by the server.
    #[serde(flatten)]
    pub overrides: HashMap<String, QueryConfig>,
}

impl QuerySettings {
    /// Resolve the effective config for a query type. Merges the default
    /// with any type-specific override. `query_type` must be the snake_case
    /// representation (e.g. `"traversal"`, `"aggregation"`).
    pub fn resolve(&self, query_type: &str) -> QueryConfig {
        match self.overrides.get(query_type) {
            Some(override_cfg) => self.default.merge(override_cfg),
            None => self.default,
        }
    }

    /// Validate that all override keys correspond to known query types.
    /// `valid_types` should contain the snake_case names of all `QueryType`
    /// variants (e.g. `["traversal", "aggregation", "search", ...]`).
    ///
    /// Returns the list of unrecognized keys, empty if all are valid.
    pub fn validate_keys(&self, valid_types: &[&str]) -> Vec<String> {
        self.overrides
            .keys()
            .filter(|k| !valid_types.contains(&k.as_str()))
            .cloned()
            .collect()
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Process-wide global
// ═════════════════════════════════════════════════════════════════════════════

static QUERY_SETTINGS: OnceLock<QuerySettings> = OnceLock::new();

/// Initialize the global query settings. Called once at startup by the
/// server after loading `AppConfig`. Panics if called twice.
pub fn init(settings: QuerySettings) {
    QUERY_SETTINGS
        .set(settings)
        .expect("gkg_config::query::init called twice");
}

/// Resolve the effective [`QueryConfig`] for a given query type from
/// the global settings. Falls back to a zero-config default if [`init`]
/// was never called (e.g. in unit tests that don't need config).
pub fn for_query_type(query_type: &str) -> QueryConfig {
    match QUERY_SETTINGS.get() {
        Some(settings) => settings.resolve(query_type),
        None => QueryConfig::default(),
    }
}

/// Returns a reference to the global [`QuerySettings`], if initialized.
pub fn global() -> Option<&'static QuerySettings> {
    QUERY_SETTINGS.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_override_wins() {
        let base = QueryConfig {
            max_execution_time: Some(30),
            use_query_cache: Some(false),
            query_cache_ttl: Some(60),
        };
        let over = QueryConfig {
            max_execution_time: Some(120),
            use_query_cache: None,
            query_cache_ttl: None,
        };
        let merged = base.merge(&over);
        assert_eq!(merged.max_execution_time, Some(120));
        assert_eq!(merged.use_query_cache, Some(false));
        assert_eq!(merged.query_cache_ttl, Some(60));
    }

    #[test]
    fn merge_all_none_falls_through() {
        let base = QueryConfig {
            max_execution_time: Some(30),
            use_query_cache: Some(true),
            query_cache_ttl: Some(60),
        };
        let merged = base.merge(&QueryConfig::default());
        assert_eq!(merged, base);
    }

    #[test]
    fn to_clickhouse_settings_skips_none() {
        let cfg = QueryConfig {
            max_execution_time: Some(30),
            use_query_cache: None,
            query_cache_ttl: None,
        };
        let settings = cfg.to_clickhouse_settings();
        assert_eq!(settings.len(), 1);
        assert_eq!(
            settings[0],
            ("max_execution_time".to_string(), "30".to_string())
        );
    }

    #[test]
    fn to_clickhouse_settings_bool_formatting() {
        let cfg = QueryConfig {
            max_execution_time: None,
            use_query_cache: Some(true),
            query_cache_ttl: None,
        };
        let settings = cfg.to_clickhouse_settings();
        assert_eq!(settings.len(), 1);
        assert_eq!(
            settings[0],
            ("use_query_cache".to_string(), "1".to_string())
        );
    }

    #[test]
    fn to_clickhouse_settings_all_set() {
        let cfg = QueryConfig {
            max_execution_time: Some(60),
            use_query_cache: Some(false),
            query_cache_ttl: Some(120),
        };
        let mut settings = cfg.to_clickhouse_settings();
        settings.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(settings.len(), 3);
        assert_eq!(
            settings[0],
            ("max_execution_time".to_string(), "60".to_string())
        );
        assert_eq!(
            settings[1],
            ("query_cache_ttl".to_string(), "120".to_string())
        );
        assert_eq!(
            settings[2],
            ("use_query_cache".to_string(), "0".to_string())
        );
    }

    #[test]
    fn resolve_uses_default_when_no_override() {
        let settings = QuerySettings {
            default: QueryConfig {
                max_execution_time: Some(30),
                use_query_cache: Some(false),
                query_cache_ttl: Some(60),
            },
            overrides: HashMap::new(),
        };
        let resolved = settings.resolve("traversal");
        assert_eq!(resolved, settings.default);
    }

    #[test]
    fn resolve_merges_type_override() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "aggregation".to_string(),
            QueryConfig {
                max_execution_time: Some(120),
                use_query_cache: None,
                query_cache_ttl: None,
            },
        );
        let settings = QuerySettings {
            default: QueryConfig {
                max_execution_time: Some(30),
                use_query_cache: Some(false),
                query_cache_ttl: Some(60),
            },
            overrides,
        };
        let resolved = settings.resolve("aggregation");
        assert_eq!(resolved.max_execution_time, Some(120));
        assert_eq!(resolved.use_query_cache, Some(false));
        assert_eq!(resolved.query_cache_ttl, Some(60));
    }

    #[test]
    fn for_query_type_returns_default_when_uninitialized() {
        // OnceLock not set in this test process — should return Default
        let cfg = for_query_type("anything");
        assert_eq!(cfg, QueryConfig::default());
    }

    #[test]
    fn yaml_deserialization() {
        let yaml = r#"
default:
  max_execution_time: 30
  use_query_cache: false
  query_cache_ttl: 60
aggregation:
  max_execution_time: 120
path_finding:
  max_execution_time: 90
"#;
        let settings: QuerySettings = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(settings.default.max_execution_time, Some(30));
        assert_eq!(settings.default.use_query_cache, Some(false));
        assert_eq!(settings.overrides.len(), 2);

        let agg = settings.resolve("aggregation");
        assert_eq!(agg.max_execution_time, Some(120));
        assert_eq!(agg.query_cache_ttl, Some(60));

        let pf = settings.resolve("path_finding");
        assert_eq!(pf.max_execution_time, Some(90));
        assert_eq!(pf.use_query_cache, Some(false));
    }

    #[test]
    fn yaml_deserialization_empty_overrides() {
        let yaml = r#"
default:
  max_execution_time: 30
"#;
        let settings: QuerySettings = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(settings.default.max_execution_time, Some(30));
        assert!(settings.overrides.is_empty());
    }

    #[test]
    fn validate_keys_accepts_known_types() {
        let valid = &[
            "traversal",
            "aggregation",
            "search",
            "path_finding",
            "neighbors",
        ];
        let mut overrides = HashMap::new();
        overrides.insert("aggregation".to_string(), QueryConfig::default());
        overrides.insert("search".to_string(), QueryConfig::default());
        let settings = QuerySettings {
            default: QueryConfig::default(),
            overrides,
        };
        assert!(settings.validate_keys(valid).is_empty());
    }

    #[test]
    fn validate_keys_rejects_unknown_types() {
        let valid = &["traversal", "aggregation", "search"];
        let mut overrides = HashMap::new();
        overrides.insert("aggregation".to_string(), QueryConfig::default());
        overrides.insert("bogus_type".to_string(), QueryConfig::default());
        overrides.insert("also_fake".to_string(), QueryConfig::default());
        let settings = QuerySettings {
            default: QueryConfig::default(),
            overrides,
        };
        let mut invalid = settings.validate_keys(valid);
        invalid.sort();
        assert_eq!(invalid, vec!["also_fake", "bogus_type"]);
    }
}
