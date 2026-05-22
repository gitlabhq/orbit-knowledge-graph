pub(crate) mod input;
pub(crate) mod lower;

pub(crate) use crate::llqm_v1::ast;
use crate::llqm_v1::ast::TableRef;
pub(crate) use crate::llqm_v1::codegen;
use std::collections::{BTreeMap, HashSet};

pub(in crate::modules::sdlc) const SOURCE_DATA_TABLE: &str = "source_data";

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use gkg_utils::arrow::ArrowUtils;
use serde_json::Value;

use crate::checkpoint::Checkpoint;
use crate::clickhouse::TIMESTAMP_FORMAT;
use crate::handler::HandlerError;
use ast::{Expr, Op, OrderExpr, Query, SelectExpr};

/// Keyset pagination state. Value type — `advance()` returns a new instance.
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

    /// Builds a DNF (disjunctive normal form) greater-than expression for
    /// composite key cursor pagination. For keys `[c1, c2]` with values `[v1, v2]`:
    /// `(c1 > 'v1') OR (c1 = 'v1' AND c2 > 'v2')`
    pub fn to_expr(&self, sort_key: &[String]) -> Option<Expr> {
        if self.values.is_empty() {
            return None;
        }

        let disjuncts: Vec<Option<Expr>> = (0..sort_key.len())
            .map(|depth| {
                let mut conjuncts: Vec<Option<Expr>> = Vec::with_capacity(depth + 1);
                for (key, val) in sort_key.iter().zip(&self.values).take(depth) {
                    conjuncts.push(Some(Expr::eq(
                        Expr::col("", key),
                        Expr::raw(format!("'{val}'")),
                    )));
                }
                conjuncts.push(Some(Expr::binary(
                    Op::Gt,
                    Expr::col("", &sort_key[depth]),
                    Expr::raw(format!("'{}'", self.values[depth])),
                )));
                Expr::and_all(conjuncts)
            })
            .collect();

        Expr::or_all(disjuncts)
    }
}

/// Static execution plan for a single entity type. Built once from ontology at
/// startup. Describes *what* to extract and *how* to transform, but not *when*
/// or *where* — watermark, traversal_path, and cursor are applied dynamically
/// via [`PreparedQuery`].
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct Plan {
    pub name: String,
    pub select: Vec<SelectExpr>,
    pub from: TableRef,
    pub static_filters: Option<Expr>,
    pub watermark_column: String,
    pub sort_key: Vec<String>,
    pub batch_size: u64,
    /// Traversal path filter for namespaced plans. `None` for global plans.
    pub traversal_filter: Option<Expr>,
    pub transforms: Vec<Transformation>,
    pub enrichment: Option<EnrichmentSql>,
}

/// Pre-built SQL fragments for CTE-based enrichment (standalone edges).
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct EnrichmentSql {
    pub cte_defs: Vec<String>,
    pub join_clauses: Vec<String>,
    pub select_exprs: Vec<String>,
}

/// Pure query builder. Each `with_*` method layers a filter or parameter onto
/// the plan's static skeleton. Call [`to_sql()`](Self::to_sql) to emit the
/// final ClickHouse SQL, [`params()`](Self::params) for the parameter map.
#[derive(Clone)]
pub(in crate::modules::sdlc) struct PreparedQuery {
    plan: Plan,
    filters: Vec<Expr>,
    params: serde_json::Map<String, Value>,
}

impl Plan {
    pub fn prepare(&self) -> PreparedQuery {
        let mut filters = Vec::new();
        if let Some(filter) = &self.static_filters {
            filters.push(filter.clone());
        }
        PreparedQuery {
            plan: self.clone(),
            filters,
            params: serde_json::Map::new(),
        }
    }
}

impl PreparedQuery {
    pub fn with_base_conditions(mut self, conditions: &BTreeMap<String, String>) -> Self {
        if let Some(filter) = &self.plan.traversal_filter {
            self.filters.push(filter.clone());
        }
        for (key, value) in conditions {
            self.params
                .insert(key.clone(), Value::String(value.clone()));
        }
        self
    }

    pub fn with_watermark(mut self, last: &DateTime<Utc>, current: &DateTime<Utc>) -> Self {
        self.filters
            .push(watermark_range(&self.plan.watermark_column));
        self.params.insert(
            "last_watermark".to_string(),
            Value::String(last.format(TIMESTAMP_FORMAT).to_string()),
        );
        self.params.insert(
            "watermark".to_string(),
            Value::String(current.format(TIMESTAMP_FORMAT).to_string()),
        );
        self
    }

    pub fn with_cursor(mut self, cursor: &Cursor) -> Self {
        if let Some(expr) = cursor.to_expr(&self.plan.sort_key) {
            self.filters.push(expr);
        }
        self
    }

    pub fn to_sql(&self) -> String {
        let where_clause = Expr::and_all(self.filters.iter().cloned().map(Some));

        if let Some(enrichment) = &self.plan.enrichment {
            self.emit_cte_sql(where_clause.as_ref(), enrichment)
        } else {
            self.emit_structured_sql(where_clause)
        }
    }

    pub fn params(&self) -> Value {
        Value::Object(self.params.clone())
    }

    fn emit_structured_sql(&self, where_clause: Option<Expr>) -> String {
        let query = Query {
            select: self.plan.select.clone(),
            from: self.plan.from.clone(),
            where_clause,
            order_by: self
                .plan
                .sort_key
                .iter()
                .map(|col| OrderExpr {
                    expr: Expr::col("", col),
                })
                .collect(),
            limit: Some(self.plan.batch_size),
        };
        codegen::emit_sql(&query)
    }

    fn emit_cte_sql(&self, where_clause: Option<&Expr>, enrichment: &EnrichmentSql) -> String {
        let base_select: Vec<String> = self
            .plan
            .select
            .iter()
            .map(codegen::emit_select_expr)
            .collect();

        let from_sql = match &self.plan.from {
            TableRef::Scan { table, .. } => table.clone(),
            TableRef::Raw(r) => r.clone(),
        };

        let where_sql = where_clause
            .map(codegen::emit_expr_to_string)
            .unwrap_or_default();

        let order_by_sql = self.plan.sort_key.join(", ");

        let outer_cols: Vec<String> = self
            .plan
            .select
            .iter()
            .map(|s| {
                let name = s.alias.as_deref().unwrap_or(match &s.expr {
                    Expr::Column { column, .. } => column.as_str(),
                    Expr::Raw(r) => r.as_str(),
                    _ => "?",
                });
                format!("_batch.{name} AS {name}")
            })
            .chain(enrichment.select_exprs.iter().cloned())
            .collect();

        format!(
            "WITH _batch AS (\
             SELECT {base_select} FROM {from_sql} \
             WHERE {where_sql} \
             ORDER BY {order_by_sql} LIMIT {batch_size}\
             ), {cte_defs} \
             SELECT {outer_select} FROM _batch {joins}",
            base_select = base_select.join(", "),
            batch_size = self.plan.batch_size,
            cte_defs = enrichment.cte_defs.join(", "),
            outer_select = outer_cols.join(", "),
            joins = enrichment.join_clauses.join(" "),
        )
    }
}

pub(in crate::modules::sdlc) fn watermark_range(watermark_column: &str) -> Expr {
    Expr::and_all([
        Some(Expr::binary(
            Op::Gt,
            Expr::raw(watermark_column.to_string()),
            Expr::param("last_watermark", "String"),
        )),
        Some(Expr::binary(
            Op::Le,
            Expr::raw(watermark_column.to_string()),
            Expr::param("watermark", "String"),
        )),
    ])
    .unwrap()
}

/// Unified over nodes and edges: a node plan produces node rows + FK edge rows,
/// an edge plan produces only edge rows. The pipeline treats both identically.
#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct Transformation {
    pub query: Query,
    pub destination_table: String,
    /// Low-cardinality columns to dictionary-encode before Arrow IPC
    /// serialization. Derived from the ontology's `LowCardinality(String)`
    /// storage columns. Empty for node transforms.
    pub dict_encode_columns: HashSet<String>,
}

impl Transformation {
    pub fn to_sql(&self) -> String {
        codegen::emit_sql(&self.query)
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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn test_plan(sort_key: Vec<&str>, batch_size: u64) -> Plan {
        let sort_key: Vec<String> = sort_key.iter().map(|s| s.to_string()).collect();
        Plan {
            name: "Test".to_string(),
            select: vec![
                SelectExpr::bare(Expr::col("", "id")),
                SelectExpr::bare(Expr::col("", "name")),
                SelectExpr::new(Expr::raw("_siphon_replicated_at"), "_version"),
                SelectExpr::new(Expr::raw("_siphon_deleted"), "_deleted"),
            ],
            from: TableRef::scan("source_table", None),
            static_filters: None,
            watermark_column: "_siphon_replicated_at".to_string(),
            sort_key,
            batch_size,
            traversal_filter: None,
            transforms: vec![],
            enrichment: None,
        }
    }

    fn namespaced_plan(sort_key: Vec<&str>) -> Plan {
        let mut plan = test_plan(sort_key, 1000);
        plan.traversal_filter = Some(Expr::func(
            "startsWith",
            vec![
                Expr::col("", "traversal_path"),
                Expr::param("traversal_path", "String"),
            ],
        ));
        plan
    }

    fn plan_with_where(extra_where: &str, sort_key: Vec<&str>) -> Plan {
        let mut plan = test_plan(sort_key, 1000);
        plan.static_filters = Some(Expr::raw(extra_where.to_string()));
        plan
    }

    fn cursor_at(values: Vec<&str>) -> Cursor {
        Cursor {
            values: values.into_iter().map(String::from).collect(),
        }
    }

    // ── Cursor tests ────────────────────────────────────────────────

    #[test]
    fn first_page_cursor_produces_no_expr() {
        let cursor = Cursor::first_page();
        let sort_key = vec!["id".to_string()];
        assert!(cursor.to_expr(&sort_key).is_none());
        assert!(cursor.is_first_page());
    }

    #[test]
    fn cursor_from_completed_checkpoint_is_first_page() {
        let checkpoint = Checkpoint {
            watermark: Utc::now(),
            cursor_values: None,
        };
        let cursor = Cursor::from_checkpoint(&checkpoint);
        assert!(cursor.is_first_page());
    }

    #[test]
    fn cursor_from_in_progress_checkpoint_has_values() {
        let checkpoint = Checkpoint {
            watermark: Utc::now(),
            cursor_values: Some(vec!["42".to_string()]),
        };
        let cursor = Cursor::from_checkpoint(&checkpoint);
        assert!(!cursor.is_first_page());
    }

    #[test]
    fn cursor_dnf_single_column() {
        let cursor = cursor_at(vec!["42"]);
        let sort_key = vec!["id".to_string()];
        let expr = cursor.to_expr(&sort_key).unwrap();
        let sql = codegen::emit_expr_to_string(&expr);
        assert!(sql.contains("(id > '42')"), "sql: {sql}");
    }

    #[test]
    fn cursor_dnf_two_columns() {
        let cursor = cursor_at(vec!["1/2/", "42"]);
        let sort_key = vec!["traversal_path".to_string(), "id".to_string()];
        let expr = cursor.to_expr(&sort_key).unwrap();
        let sql = codegen::emit_expr_to_string(&expr);
        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(
            sql.contains("(traversal_path = '1/2/') AND (id > '42')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn cursor_dnf_three_columns() {
        let cursor = cursor_at(vec!["1/2/", "10", "99"]);
        let sort_key = vec![
            "traversal_path".to_string(),
            "project_id".to_string(),
            "id".to_string(),
        ];
        let expr = cursor.to_expr(&sort_key).unwrap();
        let sql = codegen::emit_expr_to_string(&expr);
        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(sql.contains("(project_id > '10')"), "sql: {sql}");
        assert!(
            sql.contains("(project_id = '10')") && sql.contains("(id > '99')"),
            "sql: {sql}"
        );
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

    // ── PreparedQuery tests ─────────────────────────────────────────

    #[test]
    fn first_page_sql_has_no_cursor_clause() {
        let plan = test_plan(vec!["traversal_path", "id"], 1000);
        let cursor = Cursor::first_page();

        let sql = plan
            .prepare()
            .with_watermark(&Utc::now(), &Utc::now())
            .with_cursor(&cursor)
            .to_sql();

        assert!(sql.contains("ORDER BY traversal_path, id"));
        assert!(sql.contains("LIMIT 1000"));
        assert!(!sql.contains("(traversal_path >"));
    }

    #[test]
    fn first_page_sql_includes_watermark_conditions() {
        let plan = test_plan(vec!["id"], 500);

        let sql = plan
            .prepare()
            .with_watermark(&Utc::now(), &Utc::now())
            .to_sql();

        assert!(sql.contains("_siphon_replicated_at > {last_watermark:String}"));
        assert!(sql.contains("_siphon_replicated_at <= {watermark:String}"));
        assert!(sql.contains("_siphon_replicated_at AS _version"));
        assert!(sql.contains("_siphon_deleted AS _deleted"));
    }

    #[test]
    fn namespaced_plan_includes_traversal_filter() {
        let plan = namespaced_plan(vec!["id"]);
        let conditions = BTreeMap::from([("traversal_path".to_string(), "1/2/".to_string())]);

        let sql = plan
            .prepare()
            .with_base_conditions(&conditions)
            .with_watermark(&Utc::now(), &Utc::now())
            .to_sql();

        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
    }

    #[test]
    fn global_plan_omits_traversal_filter() {
        let plan = test_plan(vec!["id"], 1000);

        let sql = plan
            .prepare()
            .with_base_conditions(&BTreeMap::new())
            .with_watermark(&Utc::now(), &Utc::now())
            .to_sql();

        assert!(
            !sql.contains("traversal_path"),
            "global plan should not have traversal filter: {sql}"
        );
    }

    #[test]
    fn with_cursor_adds_pagination_clause() {
        let plan = test_plan(vec!["traversal_path", "id"], 1000);
        let cursor = cursor_at(vec!["1/2/", "42"]);

        let sql = plan
            .prepare()
            .with_watermark(&Utc::now(), &Utc::now())
            .with_cursor(&cursor)
            .to_sql();

        assert!(sql.contains("(traversal_path > '1/2/')"), "sql: {sql}");
        assert!(
            sql.contains("(traversal_path = '1/2/') AND (id > '42')"),
            "sql: {sql}"
        );
    }

    #[test]
    fn params_include_watermark_and_base_conditions() {
        let plan = namespaced_plan(vec!["id"]);
        let conditions = BTreeMap::from([("traversal_path".to_string(), "1/2/".to_string())]);

        let prepared = plan
            .prepare()
            .with_base_conditions(&conditions)
            .with_watermark(&Utc::now(), &Utc::now());

        let params = prepared.params();
        let map = params.as_object().unwrap();
        assert!(map.contains_key("last_watermark"));
        assert!(map.contains_key("watermark"));
        assert_eq!(map.get("traversal_path").unwrap(), "1/2/");
    }

    #[test]
    fn static_filters_included_in_sql() {
        let plan = plan_with_where("status = 'active'", vec!["id"]);

        let sql = plan
            .prepare()
            .with_watermark(&Utc::now(), &Utc::now())
            .to_sql();

        assert!(sql.contains("status = 'active'"), "sql: {sql}");
    }

    #[test]
    fn order_by_columns_appear_in_sql() {
        let plan = test_plan(vec!["traversal_path", "id"], 1000);
        let sql = plan
            .prepare()
            .with_watermark(&Utc::now(), &Utc::now())
            .to_sql();
        assert!(sql.contains("ORDER BY traversal_path, id"), "sql: {sql}");
    }
}
