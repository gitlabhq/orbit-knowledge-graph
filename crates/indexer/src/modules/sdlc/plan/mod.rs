pub(crate) mod fragments;
pub(crate) mod input;
pub(crate) mod lower;

use std::collections::HashSet;

pub(in crate::modules::sdlc) const SOURCE_DATA_TABLE: &str = "source_data";

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use gkg_utils::arrow::ArrowUtils;
use serde_json::Value;

use super::partitioning::PartitionAssignment;
use crate::checkpoint::Checkpoint;
use crate::clickhouse::TIMESTAMP_FORMAT;
use crate::handler::HandlerError;

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct Cursor {
    values: Vec<String>,
}

impl Cursor {
    pub fn first_page() -> Self {
        Self { values: Vec::new() }
    }

    pub fn from_checkpoint(checkpoint: &Checkpoint) -> Self {
        match &checkpoint.cursor_values {
            Some(values) => Self {
                values: values.clone(),
            },
            None => Self::first_page(),
        }
    }

    pub fn is_first_page(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }

    pub fn advance(&self, batch: &RecordBatch, sort_key: &[String]) -> Result<Self, HandlerError> {
        let last_row = batch.num_rows() - 1;
        let values = sort_key
            .iter()
            .map(|column_name| {
                let column_index = batch.schema().index_of(column_name).map_err(|_| {
                    HandlerError::Processing(format!(
                        "sort key column '{column_name}' not found in batch"
                    ))
                })?;
                let column = batch.column(column_index);
                ArrowUtils::array_value_to_string(column.as_ref(), last_row).ok_or_else(|| {
                    HandlerError::Processing(format!(
                        "unsupported sort key column type for cursor: {}",
                        column.data_type()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { values })
    }

    pub fn to_checkpoint_values(&self) -> Option<Vec<String>> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.values.clone())
        }
    }
}

pub(in crate::modules::sdlc) trait Filter {
    fn condition(&self) -> String;
    fn params(&self) -> Vec<(String, Value)> {
        Vec::new()
    }
}

// `None` is a filter that contributes nothing. Lets call sites stay chainable:
// `prepared.with(maybe_path.map(|p| TraversalPathFilter { path: p }))`.
impl<F: Filter> Filter for Option<F> {
    fn condition(&self) -> String {
        self.as_ref().map(|f| f.condition()).unwrap_or_default()
    }
    fn params(&self) -> Vec<(String, Value)> {
        self.as_ref().map(|f| f.params()).unwrap_or_default()
    }
}

pub(in crate::modules::sdlc) struct WatermarkFilter<'a> {
    pub column: &'a str,
    pub last: DateTime<Utc>,
    pub current: DateTime<Utc>,
}

impl Filter for WatermarkFilter<'_> {
    fn condition(&self) -> String {
        format!(
            "{col} > {{last_watermark:String}} AND {col} <= {{watermark:String}}",
            col = self.column
        )
    }

    fn params(&self) -> Vec<(String, Value)> {
        vec![
            (
                "last_watermark".into(),
                Value::String(self.last.format(TIMESTAMP_FORMAT).to_string()),
            ),
            (
                "watermark".into(),
                Value::String(self.current.format(TIMESTAMP_FORMAT).to_string()),
            ),
        ]
    }
}

pub(in crate::modules::sdlc) struct TraversalPathFilter<'a> {
    pub path: &'a str,
}

impl Filter for TraversalPathFilter<'_> {
    fn condition(&self) -> String {
        "startsWith(traversal_path, {traversal_path:String})".to_string()
    }

    fn params(&self) -> Vec<(String, Value)> {
        vec![(
            "traversal_path".into(),
            Value::String(self.path.to_string()),
        )]
    }
}

/// Half-open partition window `[lower, upper)` over the leading sort-key prefix.
/// A bare `id` range (the 2nd sort key) rescans the namespace on every page;
/// bounding the full prefix prunes by the primary index instead.
pub(in crate::modules::sdlc) struct CompositeRangeFilter<'a> {
    pub columns: &'a [String],
    pub lower: Option<&'a [String]>,
    pub upper: Option<&'a [String]>,
}

impl Filter for CompositeRangeFilter<'_> {
    fn condition(&self) -> String {
        let lower_edge = self
            .lower
            .map(|values| sort_key_prefix_compare(self.columns, values, KeyComparison::AtLeast));
        let upper_edge = self
            .upper
            .map(|values| sort_key_prefix_compare(self.columns, values, KeyComparison::Below));
        [lower_edge, upper_edge]
            .into_iter()
            .flatten()
            .map(|edge| format!("({edge})"))
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

enum KeyComparison {
    Greater,
    AtLeast,
    Below,
}

impl KeyComparison {
    // Lexicographic `>=` is `>` at every depth except the last column.
    fn operator(&self, at_last_column: bool) -> &'static str {
        match self {
            KeyComparison::Greater => ">",
            KeyComparison::AtLeast if at_last_column => ">=",
            KeyComparison::AtLeast => ">",
            KeyComparison::Below => "<",
        }
    }
}

/// Compares the sort-key prefix against a tuple of values as a flat OR-of-ANDs:
/// `(c0 > v0) OR (c0 = v0 AND c1 > v1) OR …`. ClickHouse prunes granules for
/// this shape; a packed `(c0, c1) > (v0, v1)` tuple forces a full scan.
fn sort_key_prefix_compare(
    columns: &[String],
    values: &[String],
    comparison: KeyComparison,
) -> String {
    let disjuncts: Vec<String> = (0..columns.len())
        .map(|pivot| {
            let mut clauses: Vec<String> = columns[..pivot]
                .iter()
                .zip(values)
                .map(|(column, value)| format!("({column} = '{value}')"))
                .collect();
            let operator = comparison.operator(pivot == columns.len() - 1);
            clauses.push(format!(
                "({} {operator} '{}')",
                columns[pivot], values[pivot]
            ));
            format!("({})", clauses.join(" AND "))
        })
        .collect();
    disjuncts.join(" OR ")
}

pub(in crate::modules::sdlc) struct CursorFilter<'a> {
    pub sort_key: &'a [String],
    pub values: &'a [String],
}

impl Filter for CursorFilter<'_> {
    // Empty values → no-op (first page).
    fn condition(&self) -> String {
        if self.values.is_empty() {
            return String::new();
        }
        debug_assert_eq!(self.sort_key.len(), self.values.len());
        sort_key_prefix_compare(self.sort_key, self.values, KeyComparison::Greater)
    }
}

// `extract_template` carries `{{filters}}` (dynamic WHERE conditions) and
// `{{batch_size}}` markers that `PreparedQuery::to_sql` substitutes.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct Plan {
    pub name: String,
    pub extract_template: String,
    pub watermark_column: String,
    pub sort_key: Vec<String>,
    pub batch_size: u64,
    pub transform: TransformSpec,
}

/// How an extracted block becomes graph rows. `DataFusion` carries the
/// declarative SQL projections the built-in transform runs; `Rust` names a
/// Rust-implemented transform resolved from the registry (e.g. `system_notes`),
/// which owns its own outputs and ignores SQL projections entirely.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) enum TransformSpec {
    DataFusion(Vec<Transformation>),
    Rust(String),
}

impl Plan {
    #[cfg(test)]
    pub(in crate::modules::sdlc) fn transformations(&self) -> &[Transformation] {
        match &self.transform {
            TransformSpec::DataFusion(transforms) => transforms,
            TransformSpec::Rust(_) => &[],
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct Transformation {
    pub sql: String,
    pub destination_table: String,
    pub dict_encode_columns: HashSet<String>,
}

#[derive(Clone)]
pub(in crate::modules::sdlc) struct PreparedQuery {
    template: String,
    filters: Vec<String>,
    params: serde_json::Map<String, Value>,
    batch_size: u64,
}

impl Plan {
    pub fn prepare(&self) -> PreparedQuery {
        PreparedQuery {
            template: self.extract_template.clone(),
            filters: Vec::new(),
            params: serde_json::Map::new(),
            batch_size: self.batch_size,
        }
    }
}

impl PreparedQuery {
    pub fn with(mut self, filter: impl Filter) -> Self {
        let condition = filter.condition();
        if condition.is_empty() {
            return self;
        }
        self.filters.push(condition);
        for (key, value) in filter.params() {
            self.params.insert(key, value);
        }
        self
    }

    pub fn to_sql(&self) -> String {
        let filters_sql = if self.filters.is_empty() {
            String::new()
        } else {
            let joined = self
                .filters
                .iter()
                .map(|f| format!("({f})"))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("AND {joined}")
        };
        self.template
            .replace("{{filters}}", &filters_sql)
            .replace("{{batch_size}}", &self.batch_size.to_string())
    }

    pub fn params(&self) -> Value {
        Value::Object(self.params.clone())
    }

    pub fn into_partitions(
        self,
        partitions: Vec<PartitionAssignment>,
    ) -> Vec<(PartitionAssignment, PreparedQuery)> {
        partitions
            .into_iter()
            .map(|p| {
                let query = self.clone().with(CompositeRangeFilter {
                    columns: &p.key_columns,
                    lower: p.lower_bound.as_deref(),
                    upper: p.upper_bound.as_deref(),
                });
                (p, query)
            })
            .collect()
    }
}

pub(in crate::modules::sdlc) struct Plans {
    pub global: Vec<Plan>,
    pub namespaced: Vec<Plan>,
}

pub(in crate::modules::sdlc) fn build_plans(
    ontology: &ontology::Ontology,
    global_batch_size: u64,
    namespaced_batch_size: u64,
    batch_size_overrides: &std::collections::HashMap<String, u64>,
) -> Plans {
    lower::lower(
        input::from_ontology(ontology),
        ontology,
        global_batch_size,
        namespaced_batch_size,
        batch_size_overrides,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_plan(sort_key: Vec<&str>, batch_size: u64) -> Plan {
        let sort_key: Vec<String> = sort_key.iter().map(|s| s.to_string()).collect();
        let sort_key_sql = sort_key.join(", ");
        Plan {
            name: "Test".to_string(),
            extract_template: format!(
                "SELECT id, name, _siphon_watermark AS _version, \
                 _siphon_deleted AS _deleted \
                 FROM source_table \
                 WHERE 1=1 {{{{filters}}}} \
                 ORDER BY {sort_key_sql} \
                 LIMIT {{{{batch_size}}}}"
            ),
            watermark_column: "_siphon_watermark".to_string(),
            sort_key,
            batch_size,
            transform: TransformSpec::DataFusion(vec![]),
        }
    }

    // ── Cursor tests ────────────────────────────────────────────────

    #[test]
    fn first_page_cursor_is_first_page() {
        let cursor = Cursor::first_page();
        assert!(cursor.is_first_page());
    }

    #[test]
    fn cursor_from_completed_checkpoint_is_first_page() {
        let checkpoint = Checkpoint {
            watermark: Utc::now(),
            cursor_values: None,
            resume_floor: None,
        };
        let cursor = Cursor::from_checkpoint(&checkpoint);
        assert!(cursor.is_first_page());
    }

    #[test]
    fn cursor_from_in_progress_checkpoint_has_values() {
        let checkpoint = Checkpoint {
            watermark: Utc::now(),
            cursor_values: Some(vec!["42".to_string()]),
            resume_floor: None,
        };
        let cursor = Cursor::from_checkpoint(&checkpoint);
        assert!(!cursor.is_first_page());
    }

    #[test]
    fn cursor_advance_extracts_last_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("traversal_path", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["1/2/", "1/3/", "1/4/"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();
        let cursor = Cursor::first_page();
        let sort_key = vec!["traversal_path".to_string(), "id".to_string()];
        let advanced = cursor.advance(&batch, &sort_key).unwrap();
        assert_eq!(
            advanced.to_checkpoint_values(),
            Some(vec!["1/4/".to_string(), "30".to_string()])
        );
    }

    // ── CursorFilter tests ──────────────────────────────────────────

    #[test]
    fn cursor_filter_single_column() {
        let sort_key = vec!["id".to_string()];
        let values = vec!["42".to_string()];
        let sql = CursorFilter {
            sort_key: &sort_key,
            values: &values,
        }
        .condition();
        assert_eq!(sql, "((id > '42'))");
    }

    #[test]
    fn cursor_filter_two_columns() {
        let sort_key = vec!["traversal_path".to_string(), "id".to_string()];
        let values = vec!["1/2/".to_string(), "42".to_string()];
        let sql = CursorFilter {
            sort_key: &sort_key,
            values: &values,
        }
        .condition();
        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(
            sql.contains("(traversal_path = '1/2/') AND (id > '42')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn cursor_filter_three_columns() {
        let sort_key = vec![
            "traversal_path".to_string(),
            "project_id".to_string(),
            "id".to_string(),
        ];
        let values = vec!["1/2/".to_string(), "10".to_string(), "99".to_string()];
        let sql = CursorFilter {
            sort_key: &sort_key,
            values: &values,
        }
        .condition();
        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(sql.contains("(project_id > '10')"), "sql: {sql}");
        assert!(
            sql.contains("(project_id = '10')") && sql.contains("(id > '99')"),
            "sql: {sql}"
        );
    }

    // ── CompositeRangeFilter tests ──────────────────────────────────

    #[test]
    fn composite_range_filter_emits_both_edges_as_dnf() {
        let columns = vec!["traversal_path".to_string(), "id".to_string()];
        let lower = vec!["1/9970/".to_string(), "100".to_string()];
        let upper = vec!["1/9970/".to_string(), "500".to_string()];
        let sql = CompositeRangeFilter {
            columns: &columns,
            lower: Some(&lower),
            upper: Some(&upper),
        }
        .condition();
        assert!(
            sql.contains("(traversal_path = '1/9970/') AND (id >= '100')"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("(traversal_path = '1/9970/') AND (id < '500')"),
            "sql: {sql}"
        );
        assert!(sql.contains(") AND ("), "sql: {sql}");
    }

    #[test]
    fn composite_range_filter_open_lower_emits_only_upper() {
        let columns = vec!["id".to_string()];
        let upper = vec!["500".to_string()];
        let sql = CompositeRangeFilter {
            columns: &columns,
            lower: None,
            upper: Some(&upper),
        }
        .condition();
        assert_eq!(sql, "(((id < '500')))");
    }

    // ── PreparedQuery tests ─────────────────────────────────────────

    #[test]
    fn first_page_sql_replaces_template_markers() {
        let plan = test_plan(vec!["traversal_path", "id"], 1000);
        let sql = plan.prepare().to_sql();
        assert!(sql.contains("ORDER BY traversal_path, id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000"), "sql: {sql}");
        assert!(!sql.contains("{{filters}}"), "sql: {sql}");
        assert!(!sql.contains("{{batch_size}}"), "sql: {sql}");
        // No filters → no `AND` added to the bare `WHERE 1=1`.
        assert!(!sql.contains("WHERE 1=1 AND"), "sql: {sql}");
    }

    #[test]
    fn watermark_filter_adds_range_and_params() {
        let plan = test_plan(vec!["id"], 500);
        let prepared = plan.prepare().with(WatermarkFilter {
            column: &plan.watermark_column,
            last: Utc::now(),
            current: Utc::now(),
        });
        let sql = prepared.to_sql();
        assert!(
            sql.contains("_siphon_watermark > {last_watermark:String}"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("_siphon_watermark <= {watermark:String}"),
            "sql: {sql}"
        );
        let params = prepared.params();
        let map = params.as_object().unwrap();
        assert!(map.contains_key("last_watermark"));
        assert!(map.contains_key("watermark"));
    }

    #[test]
    fn traversal_path_filter_adds_starts_with_and_param() {
        let plan = test_plan(vec!["id"], 1000);
        let prepared = plan.prepare().with(TraversalPathFilter { path: "1/2/" });
        let sql = prepared.to_sql();
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
        let params = prepared.params();
        assert_eq!(
            params.as_object().unwrap().get("traversal_path").unwrap(),
            "1/2/"
        );
    }

    #[test]
    fn cursor_filter_appends_pagination_clause() {
        let plan = test_plan(vec!["traversal_path", "id"], 1000);
        let sort_key = plan.sort_key.clone();
        let values = vec!["1/2/".to_string(), "42".to_string()];
        let sql = plan
            .prepare()
            .with(CursorFilter {
                sort_key: &sort_key,
                values: &values,
            })
            .to_sql();
        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(
            sql.contains("(traversal_path = '1/2/') AND (id > '42')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn multiple_filters_are_and_joined() {
        let plan = test_plan(vec!["id"], 1000);
        let sql = plan
            .prepare()
            .with(WatermarkFilter {
                column: &plan.watermark_column,
                last: Utc::now(),
                current: Utc::now(),
            })
            .with(TraversalPathFilter { path: "1/2/" })
            .to_sql();
        // Both filter conditions appear, wrapped in parens and AND-joined.
        assert!(sql.contains(" AND ("), "sql: {sql}");
        assert!(sql.contains("startsWith(traversal_path,"), "sql: {sql}");
        assert!(sql.contains("_siphon_watermark >"), "sql: {sql}");
    }
}
