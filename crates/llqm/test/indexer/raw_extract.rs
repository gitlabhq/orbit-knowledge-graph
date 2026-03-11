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
