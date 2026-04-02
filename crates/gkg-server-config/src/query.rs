//! Query execution configuration shared between server and compiler.
//!
//! [`QuerySettings`] holds a default [`QueryConfig`] plus optional
//! per-query-type overrides, loaded from `AppConfig` at startup and
//! stored in a process-wide global via [`init`] / [`for_query_type`].

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A typed ClickHouse setting value. Formatting via [`Display`] produces
/// SQL-safe literals: bare integers, `0`/`1` for bools, and single-quoted
/// escaped strings.
#[derive(Clone, Debug, PartialEq)]
pub enum SettingValue {
    UInt64(u64),
    Bool(bool),
    Str(String),
}

impl std::fmt::Display for SettingValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SettingValue::UInt64(n) => write!(f, "{n}"),
            SettingValue::Bool(b) => write!(f, "{}", if *b { "1" } else { "0" }),
            SettingValue::Str(s) => {
                // ClickHouse string settings use single quotes; escape
                // embedded quotes and backslashes.
                let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
                write!(f, "'{escaped}'")
            }
        }
    }
}

/// Query execution settings. All fields map to ClickHouse query-level
/// settings. The closed set of fields prevents arbitrary user input from
/// reaching the SETTINGS clause (CWE-89).
///
/// `None` means "not specified at this layer" -- the merge logic in
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

    /// Returns ClickHouse SETTINGS as typed key-value pairs, skipping unset fields.
    ///
    /// Uses serde round-trip so that the field names stay in sync with the
    /// struct definition -- no manual string mapping needed.
    ///
    /// Returns an error if a field serializes to a type that cannot be
    /// represented as a ClickHouse setting (e.g. arrays, objects, or
    /// non-u64 numbers).
    pub fn to_clickhouse_settings(&self) -> Result<Vec<(String, SettingValue)>, String> {
        let map = match serde_json::to_value(self) {
            Ok(Value::Object(m)) => m,
            _ => return Ok(Vec::new()),
        };
        map.into_iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, v)| {
                let setting = match &v {
                    Value::Bool(b) => SettingValue::Bool(*b),
                    Value::Number(n) => {
                        let n = n
                            .as_u64()
                            .ok_or_else(|| format!("setting `{k}` has non-u64 value: {v}"))?;
                        SettingValue::UInt64(n)
                    }
                    Value::String(s) => SettingValue::Str(s.clone()),
                    _ => return Err(format!("setting `{k}` has unsupported type: {v}")),
                };
                Ok((k, setting))
            })
            .collect()
    }
}

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
    /// Resolve the effective config for a query type by merging the
    /// default with any type-specific override.
    pub fn resolve(&self, query_type: &str) -> QueryConfig {
        match self.overrides.get(query_type) {
            Some(override_cfg) => self.default.merge(override_cfg),
            None => self.default,
        }
    }

    /// Returns unrecognized override keys sorted alphabetically
    /// (empty if all valid).
    pub fn validate_keys(&self, valid_types: &[&str]) -> Vec<String> {
        let mut invalid: Vec<String> = self
            .overrides
            .keys()
            .filter(|k| !valid_types.contains(&k.as_str()))
            .cloned()
            .collect();
        invalid.sort();
        invalid
    }
}

static QUERY_SETTINGS: OnceLock<QuerySettings> = OnceLock::new();

/// Initialize the global query settings. Called once at startup by the
/// server after loading `AppConfig`.
pub fn init(settings: QuerySettings) {
    QUERY_SETTINGS
        .set(settings)
        .expect("gkg_server_config::query::init called twice");
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
    fn to_clickhouse_settings_skips_none_and_formats_bools() -> Result<(), String> {
        let cfg = QueryConfig {
            max_execution_time: Some(30),
            use_query_cache: Some(true),
            query_cache_ttl: None,
        };
        let mut settings = cfg.to_clickhouse_settings()?;
        settings.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(settings.len(), 2);
        assert_eq!(
            settings[0],
            ("max_execution_time".into(), SettingValue::UInt64(30))
        );
        assert_eq!(
            settings[1],
            ("use_query_cache".into(), SettingValue::Bool(true))
        );
        Ok(())
    }

    #[test]
    fn setting_value_display() {
        assert_eq!(SettingValue::UInt64(42).to_string(), "42");
        assert_eq!(SettingValue::Bool(true).to_string(), "1");
        assert_eq!(SettingValue::Bool(false).to_string(), "0");
        assert_eq!(SettingValue::Str("hello".into()).to_string(), "'hello'");
        assert_eq!(
            SettingValue::Str("it's a test".into()).to_string(),
            "'it\\'s a test'"
        );
        assert_eq!(
            SettingValue::Str("back\\slash".into()).to_string(),
            "'back\\\\slash'"
        );
    }

    #[test]
    fn resolve_merges_type_override_and_falls_back_to_default() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "aggregation".to_string(),
            QueryConfig {
                max_execution_time: Some(120),
                ..Default::default()
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

        let agg = settings.resolve("aggregation");
        assert_eq!(agg.max_execution_time, Some(120));
        assert_eq!(agg.use_query_cache, Some(false));

        let search = settings.resolve("search");
        assert_eq!(search, settings.default);
    }

    #[test]
    fn yaml_round_trip() {
        let yaml = r#"
default:
  max_execution_time: 30
  use_query_cache: false
  query_cache_ttl: 60
aggregation:
  max_execution_time: 120
"#;
        let settings: QuerySettings = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(settings.default.max_execution_time, Some(30));
        assert_eq!(settings.overrides.len(), 1);
        assert_eq!(
            settings.resolve("aggregation").max_execution_time,
            Some(120)
        );
        assert_eq!(settings.resolve("aggregation").query_cache_ttl, Some(60));
    }

    #[test]
    fn validate_keys_rejects_unknown_types() {
        let valid = &["traversal", "aggregation", "search"];
        let mut overrides = HashMap::new();
        overrides.insert("aggregation".to_string(), QueryConfig::default());
        overrides.insert("bogus_type".to_string(), QueryConfig::default());
        let settings = QuerySettings {
            default: QueryConfig::default(),
            overrides,
        };
        let invalid = settings.validate_keys(valid);
        assert_eq!(invalid, vec!["bogus_type"]);
    }
}
