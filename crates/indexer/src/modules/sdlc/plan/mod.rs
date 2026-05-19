pub(crate) mod input;
pub(crate) mod lower;

use std::collections::HashSet;

pub(in crate::modules::sdlc) const SOURCE_DATA_TABLE: &str = "source_data";

use arrow::record_batch::RecordBatch;
use gkg_utils::arrow::ArrowUtils;

use crate::checkpoint::Checkpoint;
use crate::handler::HandlerError;

/// Paginated ClickHouse extract query. Owns its cursor state and generates
/// SQL on demand. Immutable: `advance` and `resume_from` return new instances.
///
/// The `template` is a complete SQL string with a `{CURSOR}` placeholder
/// that gets replaced with the keyset pagination WHERE clause at emit time.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct ExtractQuery {
    template: String,
    sort_key_columns: Vec<String>,
    cursor_values: Vec<String>,
    batch_size: u64,
}

impl ExtractQuery {
    pub fn new(template: String, sort_key_columns: Vec<String>, batch_size: u64) -> Self {
        Self {
            template,
            sort_key_columns,
            cursor_values: Vec::new(),
            batch_size,
        }
    }

    pub fn to_sql(&self) -> String {
        let cursor_sql = self.build_cursor_clause().unwrap_or_default();
        self.template.replace("{CURSOR}", &cursor_sql)
    }

    pub fn advance(&self, batch: &RecordBatch) -> Result<Self, HandlerError> {
        let cursor_values = self.extract_cursor_values(batch)?;
        let mut next = self.clone();
        next.cursor_values = cursor_values;
        Ok(next)
    }

    pub fn resume_from(mut self, position: &Checkpoint) -> Self {
        if let Some(values) = &position.cursor_values {
            self.cursor_values = values.clone();
        }
        self
    }

    pub fn is_first_page(&self) -> bool {
        self.cursor_values.is_empty()
    }

    pub fn cursor_values(&self) -> &[String] {
        &self.cursor_values
    }

    pub fn batch_size(&self) -> u64 {
        self.batch_size
    }

    /// Builds a DNF (disjunctive normal form) greater-than clause for
    /// composite key cursor pagination. For keys `[c1, c2]` with values `[v1, v2]`:
    /// ` AND ((c1 > 'v1') OR ((c1 = 'v1') AND (c2 > 'v2')))`
    ///
    /// Returns `None` when there are no cursor values (first page).
    fn build_cursor_clause(&self) -> Option<String> {
        if self.cursor_values.is_empty() {
            return None;
        }

        let disjuncts: Vec<String> = (0..self.sort_key_columns.len())
            .map(|depth| {
                let mut conjuncts: Vec<String> = Vec::with_capacity(depth + 1);

                for prefix in 0..depth {
                    conjuncts.push(format!(
                        "({} = '{}')",
                        self.sort_key_columns[prefix], self.cursor_values[prefix]
                    ));
                }
                conjuncts.push(format!(
                    "({} > '{}')",
                    self.sort_key_columns[depth], self.cursor_values[depth]
                ));

                if conjuncts.len() == 1 {
                    conjuncts.into_iter().next().unwrap()
                } else {
                    format!("({})", conjuncts.join(" AND "))
                }
            })
            .collect();

        let expr = if disjuncts.len() == 1 {
            disjuncts.into_iter().next().unwrap()
        } else {
            format!("({})", disjuncts.join(" OR "))
        };

        Some(format!(" AND {expr}"))
    }

    fn extract_cursor_values(&self, batch: &RecordBatch) -> Result<Vec<String>, HandlerError> {
        let last_row = batch.num_rows() - 1;

        self.sort_key_columns
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
            .collect()
    }
}

/// Unified over nodes and edges: a node plan produces node rows + FK edge rows,
/// an edge plan produces only edge rows. The pipeline treats both identically.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct PipelinePlan {
    pub name: String,
    pub extract_query: ExtractQuery,
    pub transforms: Vec<Transformation>,
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct Transformation {
    pub sql: String,
    pub destination_table: String,
    /// Low-cardinality columns to dictionary-encode before Arrow IPC
    /// serialization. Derived from the ontology's `LowCardinality(String)`
    /// storage columns. Empty for node transforms.
    pub dict_encode_columns: HashSet<String>,
}

impl Transformation {
    pub fn to_sql(&self) -> &str {
        &self.sql
    }
}

pub(in crate::modules::sdlc) struct Plans {
    pub global: Vec<PipelinePlan>,
    pub namespaced: Vec<PipelinePlan>,
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
    use chrono::Utc;
    use std::sync::Arc;

    fn position_with_cursor(values: Vec<&str>) -> Checkpoint {
        Checkpoint {
            watermark: Utc::now(),
            cursor_values: Some(values.into_iter().map(String::from).collect()),
        }
    }

    fn base_template(sort_keys: &[&str], batch_size: u64) -> String {
        format!(
            "SELECT id, name, _siphon_replicated_at AS _version, _siphon_deleted AS _deleted \
             FROM source_table \
             WHERE (_siphon_replicated_at > {{last_watermark:String}}) \
             AND (_siphon_replicated_at <= {{watermark:String}}){{CURSOR}} \
             ORDER BY {} LIMIT {batch_size}",
            sort_keys.join(", "),
        )
    }

    fn simple_query(sort_keys: Vec<&str>, batch_size: u64) -> ExtractQuery {
        let sort_key_columns: Vec<String> = sort_keys.iter().map(|s| s.to_string()).collect();
        let template = base_template(&sort_keys, batch_size);
        ExtractQuery::new(template, sort_key_columns, batch_size)
    }

    fn query_with_where(where_clause: &str, sort_keys: Vec<&str>) -> ExtractQuery {
        let sort_key_columns: Vec<String> = sort_keys.iter().map(|s| s.to_string()).collect();
        let template = format!(
            "SELECT id, name, _siphon_replicated_at AS _version, _siphon_deleted AS _deleted \
             FROM source_table \
             WHERE (_siphon_replicated_at > {{last_watermark:String}}) \
             AND (_siphon_replicated_at <= {{watermark:String}}) \
             AND {where_clause}{{CURSOR}} \
             ORDER BY {} LIMIT 1000",
            sort_keys.join(", "),
        );
        ExtractQuery::new(template, sort_key_columns, 1000)
    }

    #[test]
    fn first_page_sql_has_no_cursor_clause() {
        let query = simple_query(vec!["traversal_path", "id"], 1000);

        let sql = query.to_sql();

        assert!(query.is_first_page());
        assert!(sql.contains("ORDER BY traversal_path, id"));
        assert!(sql.contains("LIMIT 1000"));
        assert!(!sql.contains("(traversal_path >"));
    }

    #[test]
    fn first_page_sql_includes_watermark_conditions() {
        let query = simple_query(vec!["id"], 500);

        let sql = query.to_sql();

        assert!(sql.contains("_siphon_replicated_at > {last_watermark:String}"));
        assert!(sql.contains("_siphon_replicated_at <= {watermark:String}"));
        assert!(sql.contains("_siphon_replicated_at AS _version"));
        assert!(sql.contains("_siphon_deleted AS _deleted"));
    }

    #[test]
    fn first_page_sql_includes_where_clause() {
        let query = query_with_where(
            "startsWith(traversal_path, {traversal_path:String})",
            vec!["id"],
        );

        let sql = query.to_sql();

        assert!(sql.contains("startsWith(traversal_path, {traversal_path:String})"));
    }

    #[test]
    fn advanced_page_sql_includes_cursor_clause_single_column() {
        let query = simple_query(vec!["id"], 1000);
        let advanced = query.resume_from(&position_with_cursor(vec!["42"]));

        let sql = advanced.to_sql();

        assert!(!advanced.is_first_page());
        assert!(
            sql.contains("(id > '42')"),
            "expected cursor clause in SQL: {sql}"
        );
        assert!(sql.contains("ORDER BY id LIMIT 1000"));
    }

    #[test]
    fn advanced_page_sql_includes_cursor_clause_two_columns() {
        let query = simple_query(vec!["traversal_path", "id"], 1000);
        let advanced = query.resume_from(&position_with_cursor(vec!["1/2/", "42"]));

        let sql = advanced.to_sql();

        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(
            sql.contains("(traversal_path = '1/2/') AND (id > '42')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn advanced_page_sql_includes_cursor_clause_three_columns() {
        let query = simple_query(vec!["traversal_path", "project_id", "id"], 1000);
        let advanced = query.resume_from(&position_with_cursor(vec!["1/2/", "10", "99"]));

        let sql = advanced.to_sql();

        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(sql.contains("(project_id > '10')"), "sql: {sql}");
        assert!(
            sql.contains("(project_id = '10')") && sql.contains("(id > '99')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn resume_from_applies_cursor_values() {
        let query = simple_query(vec!["id"], 1000);
        let resumed = query.resume_from(&position_with_cursor(vec!["42"]));

        assert!(!resumed.is_first_page());
        assert_eq!(resumed.cursor_values(), &["42"]);
    }

    #[test]
    fn resume_from_completed_position_keeps_first_page() {
        let query = simple_query(vec!["id"], 1000);
        let completed = Checkpoint {
            watermark: Utc::now(),
            cursor_values: None,
        };
        let resumed = query.resume_from(&completed);

        assert!(resumed.is_first_page());
    }

    #[test]
    fn resume_from_produces_correct_sql() {
        let query = simple_query(vec!["id"], 1000);
        let resumed = query.resume_from(&position_with_cursor(vec!["42"]));
        let sql = resumed.to_sql();

        assert!(
            sql.contains("(id > '42')"),
            "resume_from should produce cursor clause: {sql}"
        );
    }

    #[test]
    fn advanced_page_with_where_clause_includes_both_conditions() {
        let query = query_with_where(
            "startsWith(traversal_path, {traversal_path:String})",
            vec!["traversal_path", "id"],
        );
        let advanced = query.resume_from(&position_with_cursor(vec!["1/2/", "42"]));

        let sql = advanced.to_sql();

        assert!(sql.contains("startsWith(traversal_path, {traversal_path:String})"));
        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(
            sql.contains("(traversal_path = '1/2/') AND (id > '42')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn advance_extracts_cursor_from_last_row() {
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

        let query = simple_query(vec!["traversal_path", "id"], 1000);
        let advanced = query.advance(&batch).unwrap();

        assert_eq!(advanced.cursor_values(), &["1/4/", "30"]);
    }

    #[test]
    fn order_by_columns_appear_in_sql() {
        let query = simple_query(vec!["traversal_path", "id"], 1000);
        assert!(query.to_sql().contains("ORDER BY traversal_path, id"));
    }
}
