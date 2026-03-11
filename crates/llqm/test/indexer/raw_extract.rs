//! Raw extract-query frontend for query-based ETL.
//!
//! Handles ontology entities that define `etl.type: query` with explicit
//! SELECT/FROM/WHERE instead of a simple table scan. The FROM clause is
//! emitted verbatim (supporting JOINs), columns can be arbitrary SQL
//! expressions, and watermark/deleted references can be table-qualified.
//!
//! Query shape:
//!   Read_raw(from) → Filter(watermark [+ namespace + additional + cursor]) → Project → Sort → Fetch

use super::types::*;
use llqm::ir::expr::{self, DataType, Expr};
use llqm::ir::plan::{Plan, Rel};
use llqm::pipeline::Frontend;

#[derive(Debug, thiserror::Error)]
pub enum RawExtractError {
    #[error("extract has no columns")]
    NoColumns,
}

pub struct RawExtractFrontend;

impl Frontend for RawExtractFrontend {
    type Input = RawExtractInput;
    type Error = RawExtractError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        if input.columns.is_empty() {
            return Err(RawExtractError::NoColumns);
        }

        let output_cols = derive_output_columns(&input.columns);
        let mut rel = Rel::read_raw(&input.from, &output_cols);

        rel = build_watermark_filter(rel, &input.watermark);

        if input.namespaced {
            rel = build_namespace_filter(rel, input.traversal_path_filter.as_deref());
        }

        if let Some(extra) = &input.additional_where {
            rel = rel.filter(expr::raw(extra));
        }

        if !input.cursor_values.is_empty() {
            rel = build_cursor_filter(rel, &input.cursor_values);
        }

        rel = build_projection(rel, &input.columns, &input.watermark, &input.deleted);

        let sort_keys: Vec<(Expr, expr::SortDir)> = input
            .order_by
            .iter()
            .map(|k| (expr::raw(k), expr::SortDir::Asc))
            .collect();
        rel = rel.sort(&sort_keys);
        rel = rel.fetch(input.batch_size, None);

        Ok(rel.into_plan())
    }
}

fn derive_output_columns(columns: &[RawExtractColumn]) -> Vec<(&str, DataType)> {
    columns
        .iter()
        .map(|c| {
            let name = match c {
                RawExtractColumn::Bare(s) => extract_alias(s),
                RawExtractColumn::ToString(s) => s.as_str(),
            };
            (name, DataType::String)
        })
        .collect()
}

/// Extract the output alias from a raw column expression.
/// `"project.id AS id"` → `"id"`, `"id"` → `"id"`
fn extract_alias(expr: &str) -> &str {
    if let Some(pos) = expr.to_lowercase().rfind(" as ") {
        expr[pos + 4..].trim()
    } else {
        expr.trim()
    }
}

fn build_watermark_filter(rel: Rel, watermark: &str) -> Rel {
    let lower = expr::raw(watermark).gt(expr::param("last_watermark", DataType::String));
    let upper = expr::raw(watermark).le(expr::param("watermark", DataType::String));
    rel.filter(lower.and(upper))
}

fn build_namespace_filter(rel: Rel, custom_filter: Option<&str>) -> Rel {
    match custom_filter {
        Some(filter) => rel.filter(expr::raw(filter)),
        None => rel.filter(
            expr::col("", "traversal_path")
                .starts_with(expr::param("traversal_path", DataType::String)),
        ),
    }
}

fn build_cursor_filter(rel: Rel, cursor_values: &[(String, String)]) -> Rel {
    let disjuncts: Vec<Expr> = (0..cursor_values.len())
        .map(|i| {
            let mut conjuncts = Vec::with_capacity(i + 1);
            for (col_name, val) in &cursor_values[..i] {
                conjuncts.push(expr::raw(&format!("{col_name} = '{val}'")));
            }
            let (col_name, val) = &cursor_values[i];
            conjuncts.push(expr::raw(&format!("{col_name} > '{val}'")));
            expr::and(conjuncts)
        })
        .collect();

    rel.filter(expr::or(disjuncts))
}

fn build_projection(rel: Rel, columns: &[RawExtractColumn], watermark: &str, deleted: &str) -> Rel {
    let mut items: Vec<(Expr, &str)> = columns
        .iter()
        .map(|c| match c {
            RawExtractColumn::Bare(s) => (expr::raw(s), extract_alias(s)),
            RawExtractColumn::ToString(name) => (
                expr::func("toString", vec![expr::col("", name)]),
                name.as_str(),
            ),
        })
        .collect();

    items.push((expr::raw(watermark), "_version"));
    items.push((expr::raw(deleted), "_deleted"));

    rel.project(&items)
}

// ---------------------------------------------------------------------------
// Tests — all use Pipeline for construction AND emission
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use llqm::backend::clickhouse::ClickHouseBackend;
    use llqm::pipeline::Pipeline;

    /// Build + emit through the pipeline.
    fn emit_sql(input: RawExtractInput) -> String {
        Pipeline::new()
            .input(RawExtractFrontend, input)
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish()
            .sql
    }

    fn table_extract(batch_size: u64) -> RawExtractInput {
        RawExtractInput {
            columns: vec![
                RawExtractColumn::Bare("id".into()),
                RawExtractColumn::Bare("name".into()),
            ],
            from: "siphon_user".into(),
            watermark: "_siphon_replicated_at".into(),
            deleted: "_siphon_deleted".into(),
            order_by: vec!["id".into()],
            batch_size,
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            cursor_values: vec![],
        }
    }

    fn query_extract(batch_size: u64) -> RawExtractInput {
        RawExtractInput {
            columns: vec![
                RawExtractColumn::Bare("project.id AS id".into()),
                RawExtractColumn::Bare(
                    "traversal_paths.traversal_path AS traversal_path".into(),
                ),
            ],
            from: "siphon_projects project INNER JOIN traversal_paths ON project.id = traversal_paths.id".into(),
            watermark: "project._siphon_replicated_at".into(),
            deleted: "project._siphon_deleted".into(),
            order_by: vec!["traversal_path".into(), "id".into()],
            batch_size,
            namespaced: true,
            traversal_path_filter: Some(
                "startsWith(traversal_path, {traversal_path:String})".into(),
            ),
            additional_where: None,
            cursor_values: vec![],
        }
    }

    #[test]
    fn table_extract_includes_all_columns() {
        let sql = emit_sql(table_extract(1000));

        assert!(sql.contains("SELECT id, name,"), "sql: {sql}");
        assert!(
            sql.contains("_siphon_replicated_at AS _version"),
            "sql: {sql}"
        );
        assert!(sql.contains("_siphon_deleted AS _deleted"), "sql: {sql}");
        assert!(sql.contains("FROM siphon_user"), "sql: {sql}");
        assert!(sql.contains("ORDER BY id"), "sql: {sql}");
        assert!(sql.contains("LIMIT 1000"), "sql: {sql}");
    }

    #[test]
    fn query_extract_uses_structured_fields() {
        let sql = emit_sql(query_extract(500));

        assert!(sql.contains("project.id AS id"), "sql: {sql}");
        assert!(
            sql.contains("traversal_paths.traversal_path AS traversal_path"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("project._siphon_replicated_at AS _version"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("project._siphon_deleted AS _deleted"),
            "sql: {sql}"
        );
        assert!(sql.contains("INNER JOIN"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
        assert!(sql.contains("ORDER BY traversal_path"), "sql: {sql}");
        assert!(sql.contains("LIMIT 500"), "sql: {sql}");
    }

    #[test]
    fn watermark_conditions_present() {
        let sql = emit_sql(table_extract(500));
        assert!(sql.contains("{last_watermark:String}"), "sql: {sql}");
        assert!(sql.contains("{watermark:String}"), "sql: {sql}");
    }

    #[test]
    fn namespace_default_starts_with() {
        let mut input = table_extract(1000);
        input.namespaced = true;
        input.traversal_path_filter = None;

        let sql = emit_sql(input);
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
    }

    #[test]
    fn namespace_custom_filter() {
        let mut input = table_extract(1000);
        input.namespaced = true;
        input.traversal_path_filter =
            Some("startsWith(traversal_path, {traversal_path:String})".into());

        let sql = emit_sql(input);
        assert!(
            sql.contains("startsWith(traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
    }

    #[test]
    fn additional_where_clause() {
        let mut input = table_extract(1000);
        input.additional_where = Some("type = 'active'".into());

        let sql = emit_sql(input);
        assert!(sql.contains("type = 'active'"), "sql: {sql}");
    }

    #[test]
    fn cursor_pagination_single() {
        let mut input = table_extract(1000);
        input.cursor_values = vec![("id".into(), "42".into())];

        let sql = emit_sql(input);
        assert!(sql.contains("id > '42'"), "sql: {sql}");
    }

    #[test]
    fn cursor_pagination_composite() {
        let mut input = query_extract(1000);
        input.cursor_values = vec![
            ("traversal_path".into(), "1/2/".into()),
            ("id".into(), "42".into()),
        ];

        let sql = emit_sql(input);
        assert!(sql.contains("traversal_path > '1/2/'"), "sql: {sql}");
        assert!(sql.contains("traversal_path = '1/2/'"), "sql: {sql}");
        assert!(sql.contains("id > '42'"), "sql: {sql}");
        assert!(sql.contains("OR"), "sql: {sql}");
    }

    #[test]
    fn to_string_column() {
        let input = RawExtractInput {
            columns: vec![
                RawExtractColumn::Bare("id".into()),
                RawExtractColumn::ToString("uuid".into()),
            ],
            from: "siphon_user".into(),
            watermark: "_siphon_replicated_at".into(),
            deleted: "_siphon_deleted".into(),
            order_by: vec!["id".into()],
            batch_size: 1000,
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            cursor_values: vec![],
        };

        let sql = emit_sql(input);
        assert!(sql.contains("toString(uuid)"), "sql: {sql}");
    }

    #[test]
    fn rejects_empty_columns() {
        let input = RawExtractInput {
            columns: vec![],
            from: "t".into(),
            watermark: "w".into(),
            deleted: "d".into(),
            order_by: vec![],
            batch_size: 100,
            namespaced: false,
            traversal_path_filter: None,
            additional_where: None,
            cursor_values: vec![],
        };

        let result = Pipeline::new().input(RawExtractFrontend, input).lower();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no columns"));
    }
}
