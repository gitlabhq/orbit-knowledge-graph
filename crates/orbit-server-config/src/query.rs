//! ClickHouse settings for end-user and consumer read queries.
//!
//! `config/default.yaml` is the single source of truth: `query.default` and per-query-type
//! overrides. The compiler's settings pass adds only settings that depend on the query shape.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum SettingValue {
    Bool(bool),
    Number(u64),
    Text(String),
}

impl From<bool> for SettingValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<&str> for SettingValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<u64> for SettingValue {
    fn from(value: u64) -> Self {
        Self::Number(value)
    }
}

/// ClickHouse settings by name. Names must be lowercase identifiers, so a value can never
/// reach the SETTINGS clause as SQL.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct QueryConfig(BTreeMap<String, SettingValue>);

impl QueryConfig {
    pub fn merge(&self, overrides: &QueryConfig) -> QueryConfig {
        let mut merged = self.clone();
        merged.0.extend(overrides.0.clone());
        merged
    }

    pub fn set(&mut self, name: &str, value: impl Into<SettingValue>) {
        self.0.insert(name.to_string(), value.into());
    }

    pub fn get_bool(&self, name: &str) -> Option<bool> {
        match self.0.get(name)? {
            SettingValue::Bool(value) => Some(*value),
            SettingValue::Number(0) => Some(false),
            SettingValue::Number(1) => Some(true),
            SettingValue::Text(text) => match text.as_str() {
                "0" | "false" => Some(false),
                "1" | "true" => Some(true),
                _ => None,
            },
            SettingValue::Number(_) => None,
        }
    }

    /// SETTINGS clause literals: strings are single-quoted and escaped.
    pub fn to_clickhouse_settings(&self) -> Result<Vec<(String, String)>, String> {
        self.render(|text| format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'")))
    }

    pub fn to_http_settings(&self) -> Result<Vec<(String, String)>, String> {
        self.render(str::to_owned)
    }

    fn render(&self, text: fn(&str) -> String) -> Result<Vec<(String, String)>, String> {
        self.0
            .iter()
            .map(|(name, value)| {
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                {
                    return Err(format!("setting name `{name}` is not a ClickHouse setting"));
                }
                let rendered = match value {
                    SettingValue::Bool(value) => u8::from(*value).to_string(),
                    SettingValue::Number(value) => value.to_string(),
                    SettingValue::Text(value) => text(value),
                };
                Ok((name.clone(), rendered))
            })
            .collect()
    }
}

/// `query.default` and per-query-type overrides keyed by snake_case query type.
///
/// ```yaml
/// query:
///   default:
///     max_execution_time: 30
///   path_finding:
///     max_execution_time: 15
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct QuerySettings {
    pub default: QueryConfig,

    /// Keys must match `QueryType` variant names. Validated at startup by the server.
    #[serde(flatten)]
    pub overrides: HashMap<String, QueryConfig>,
}

impl QuerySettings {
    pub fn resolve(&self, query_type: &str) -> QueryConfig {
        match self.overrides.get(query_type) {
            Some(override_cfg) => self.default.merge(override_cfg),
            None => self.default.clone(),
        }
    }

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

/// Called once at startup by the server after loading `AppConfig`.
pub fn init(settings: QuerySettings) {
    QUERY_SETTINGS
        .set(settings)
        .expect("orbit_server_config::query::init called twice");
}

/// For tools and tests that run queries without a server: the settings in `config/default.yaml`.
pub fn init_embedded_defaults() {
    QUERY_SETTINGS.get_or_init(|| crate::AppConfig::embedded_defaults().query);
}

/// Before [`init`] every config is empty, so no SETTINGS are emitted.
pub fn for_query_type(query_type: &str) -> QueryConfig {
    QUERY_SETTINGS
        .get()
        .map(|settings| settings.resolve(query_type))
        .unwrap_or_default()
}

/// `query.default`, for callers outside the compiler that issue their own ClickHouse queries.
pub fn default_config() -> QueryConfig {
    QUERY_SETTINGS
        .get()
        .map(|settings| settings.default.clone())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, SettingValue)]) -> QueryConfig {
        let mut config = QueryConfig::default();
        for (name, value) in pairs {
            config.set(name, value.clone());
        }
        config
    }

    #[test]
    fn merge_override_wins() {
        let base = config(&[
            ("max_execution_time", 30u64.into()),
            ("use_query_cache", false.into()),
        ]);
        let merged = base.merge(&config(&[("max_execution_time", 120u64.into())]));
        assert_eq!(
            merged.to_clickhouse_settings(),
            Ok(vec![
                ("max_execution_time".into(), "120".into()),
                ("use_query_cache".into(), "0".into()),
            ])
        );
    }

    #[test]
    fn renders_strings_quoted_for_sql_and_raw_for_http() {
        let cfg = config(&[("join_algorithm", SettingValue::Text("it's".into()))]);
        assert_eq!(
            cfg.to_clickhouse_settings(),
            Ok(vec![("join_algorithm".into(), "'it\\'s'".into())])
        );
        assert_eq!(
            cfg.to_http_settings(),
            Ok(vec![("join_algorithm".into(), "it's".into())])
        );
    }

    #[test]
    fn get_bool_reads_booleans_written_as_numbers_or_strings() {
        for value in [true.into(), 1u64.into(), SettingValue::Text("true".into())] {
            assert_eq!(
                config(&[("use_query_cache", value)]).get_bool("use_query_cache"),
                Some(true)
            );
        }
        for value in [false.into(), 0u64.into(), SettingValue::Text("0".into())] {
            assert_eq!(
                config(&[("use_query_cache", value)]).get_bool("use_query_cache"),
                Some(false)
            );
        }
        assert_eq!(
            config(&[("use_query_cache", 2u64.into())]).get_bool("use_query_cache"),
            None
        );
    }

    #[test]
    fn rejects_setting_names_that_are_not_identifiers() {
        let cfg = config(&[("max_threads = 1, readonly", 1u64.into())]);
        assert!(cfg.to_clickhouse_settings().is_err());
    }

    #[test]
    fn embedded_defaults_resolve_per_query_type() {
        let settings = crate::AppConfig::embedded_defaults().query;
        let traversal = settings
            .resolve("traversal")
            .to_clickhouse_settings()
            .unwrap();
        assert!(traversal.contains(&("max_execution_time".into(), "30".into())));
        assert!(traversal.contains(&("max_memory_usage".into(), "4294967296".into())));
        assert!(traversal.contains(&(
            "query_cache_nondeterministic_function_handling".into(),
            "'ignore'".into()
        )));
        let path_finding = settings
            .resolve("path_finding")
            .to_clickhouse_settings()
            .unwrap();
        assert!(path_finding.contains(&("max_execution_time".into(), "15".into())));
        assert!(settings.validate_keys(&["path_finding"]).is_empty());
    }

    #[test]
    fn yaml_round_trip() {
        let yaml = r#"
default:
  max_execution_time: 30
  use_query_cache: false
aggregation:
  max_execution_time: 120
"#;
        let settings: QuerySettings = orbit_utils::yaml::from_str(yaml).unwrap();
        assert_eq!(settings.overrides.len(), 1);
        assert_eq!(
            settings.resolve("aggregation").to_http_settings(),
            Ok(vec![
                ("max_execution_time".into(), "120".into()),
                ("use_query_cache".into(), "0".into()),
            ])
        );
    }

    #[test]
    fn validate_keys_rejects_unknown_types() {
        let mut overrides = HashMap::new();
        overrides.insert("aggregation".to_string(), QueryConfig::default());
        overrides.insert("bogus_type".to_string(), QueryConfig::default());
        let settings = QuerySettings {
            default: QueryConfig::default(),
            overrides,
        };
        assert_eq!(
            settings.validate_keys(&["traversal", "aggregation"]),
            vec!["bogus_type"]
        );
    }
}
