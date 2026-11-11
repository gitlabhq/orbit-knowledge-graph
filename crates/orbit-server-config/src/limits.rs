//! Query limits: the caps the GQL surface, the logical builder and the compiler enforce.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The `query.limits` section. Every key is required and at least 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryLimits {
    /// Nodes in one MATCH pattern.
    pub nodes: usize,
    /// Hops of one variable-length relationship.
    pub hops: u32,
    /// Hops a path-finding query searches.
    pub path_depth: u32,
    /// Relationship types one relationship pattern names.
    pub relationship_types: usize,
    /// Rows a query returns when it names no LIMIT.
    pub default_rows: u64,
    /// The largest LIMIT.
    pub max_rows: u64,
    /// The largest SKIP.
    pub max_skip: u64,
    /// Values of an IN list on a property other than `id`.
    pub in_values: usize,
    /// Ids an IN list on `id` pins.
    pub pinned_ids: usize,
    /// Columns one node returns.
    pub columns: usize,
    /// Aggregates of one aggregation.
    pub aggregates: usize,
    /// Group keys of one aggregation.
    pub group_keys: usize,
    /// Characters of one string literal.
    pub string_chars: usize,
    /// Bytes of one identifier.
    pub identifier_bytes: usize,
    /// Depth of nested conditions.
    pub nesting: usize,
    /// Bytes of one query text.
    pub query_bytes: usize,
    /// Predicates across the whole query.
    pub predicates: usize,
    /// Filtered properties on one node.
    pub node_filter_properties: usize,
    /// Filtered properties on one relationship.
    pub relationship_filter_properties: usize,
    /// Filters on one property of one node or relationship.
    pub filter_entries_per_property: usize,
    /// The widest `id_range` that makes a node selective.
    pub id_range_span: i64,
    /// The widest `id_range` that anchors a path-finding endpoint.
    pub path_anchor_span: i64,
    /// Rows one path-finding endpoint seeds.
    pub path_anchor_rows: u32,
    /// Rows one dynamic hydration or key lookup reads per entity.
    pub hydration_rows: usize,
    /// Traversal path segments one hydration read binds before it widens to parent paths.
    pub hydration_path_segments: usize,
    /// ClickHouse `use_index_for_in_with_subqueries_max_values` for queries with shared or membership reads.
    pub in_subquery_index_values: u64,
    /// The ceiling on ClickHouse `max_execution_time` for path finding, in seconds.
    pub path_finding_max_execution_time: u64,
    /// The ceiling on ClickHouse `max_memory_usage` for path finding, in bytes.
    pub path_finding_max_memory_usage: u64,
}

impl QueryLimits {
    pub fn validate(&self) -> Result<(), QueryLimitsError> {
        let Ok(Value::Object(fields)) = serde_json::to_value(self) else {
            unreachable!("QueryLimits serializes to an object of integers");
        };
        if let Some((key, value)) = fields
            .into_iter()
            .find(|(_, value)| value.as_i64().is_some_and(|value| value < 1))
        {
            return Err(QueryLimitsError::BelowOne { key, value });
        }
        if self.default_rows > self.max_rows {
            return Err(QueryLimitsError::DefaultRowsAboveMax {
                default_rows: self.default_rows,
                max_rows: self.max_rows,
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryLimitsError {
    #[error("query.limits.{key} must be at least 1 (got {value})")]
    BelowOne { key: String, value: Value },
    #[error(
        "query.limits.default_rows ({default_rows}) must not exceed query.limits.max_rows ({max_rows})"
    )]
    DefaultRowsAboveMax { default_rows: u64, max_rows: u64 },
}

#[cfg(test)]
mod tests {
    use crate::AppConfig;

    #[test]
    fn embedded_defaults_are_valid() {
        AppConfig::embedded_defaults()
            .query
            .limits
            .validate()
            .unwrap();
    }

    #[test]
    fn zero_names_its_key() {
        let mut limits = AppConfig::embedded_defaults().query.limits;
        limits.path_anchor_rows = 0;
        assert_eq!(
            limits.validate().unwrap_err().to_string(),
            "query.limits.path_anchor_rows must be at least 1 (got 0)"
        );
        limits.path_anchor_rows = 1;
        limits.id_range_span = -5;
        assert_eq!(
            limits.validate().unwrap_err().to_string(),
            "query.limits.id_range_span must be at least 1 (got -5)"
        );
    }

    #[test]
    fn default_rows_stay_within_max_rows() {
        let mut limits = AppConfig::embedded_defaults().query.limits;
        limits.default_rows = limits.max_rows + 1;
        assert_eq!(
            limits.validate().unwrap_err().to_string(),
            "query.limits.default_rows (1001) must not exceed query.limits.max_rows (1000)"
        );
    }

    #[test]
    fn missing_key_fails_the_load() {
        let yaml = crate::EMBEDDED_DEFAULTS.replace("    max_skip: 10000\n", "");
        let error = config::Config::builder()
            .add_source(config::File::from_str(&yaml, config::FileFormat::Yaml))
            .build()
            .and_then(config::Config::try_deserialize::<AppConfig>)
            .unwrap_err();
        assert!(error.to_string().contains("max_skip"), "{error}");
    }
}
