//! Indexer extract-query frontend.
//!
//! Replaces `llqm_v1::ast` + `llqm_v1::codegen` with a proper `Frontend`
//! implementation that builds `Rel` trees for SDLC extract queries.
//!
//! Query shape:
//!   Read(source) [→ Join(traversal_paths)] → Filter(watermark [+ cursor]) → Project → Sort → Fetch

use super::types::*;
use llqm::ir::expr::{self, DataType, Expr};
use llqm::ir::plan::{Plan, Rel};
use llqm::pipeline::Frontend;

#[derive(Debug, thiserror::Error)]
pub enum LowerError {
    #[error("entity has no columns")]
    NoColumns,
    #[error("sort key '{0}' not found in columns")]
    SortKeyNotFound(String),
}

pub struct IndexerFrontend;

impl Frontend for IndexerFrontend {
    type Input = ExtractInput;
    type Error = LowerError;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
        let entity = &input.entity;
        if entity.columns.is_empty() && entity.join.as_ref().is_none_or(|j| j.columns.is_empty()) {
            return Err(LowerError::NoColumns);
        }

        let mut rel = build_read(entity);

        if let Some(join) = &entity.join {
            rel = build_join(rel, entity, join);
        }

        rel = build_watermark_filter(rel, entity);

        if !input.cursor_values.is_empty() {
            rel = build_cursor_filter(rel, &input.cursor_values);
        }

        rel = build_projection(rel, entity);
        rel = build_sort(rel, entity);
        rel = rel.fetch(input.batch_size, None);

        Ok(rel.into_plan())
    }
}

// ---------------------------------------------------------------------------
// Rel builders
// ---------------------------------------------------------------------------

fn build_read(entity: &EntityDef) -> Rel {
    let cols: Vec<(&str, DataType)> = entity
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.clone()))
        .chain(std::iter::once((
            entity.version_column.as_str(),
            DataType::String,
        )))
        .chain(std::iter::once((
            entity.deleted_column.as_str(),
            DataType::Bool,
        )))
        .collect();

    Rel::read(&entity.source_table, &entity.source_alias, &cols)
}

fn build_join(left: Rel, entity: &EntityDef, join: &JoinDef) -> Rel {
    let join_cols: Vec<(&str, DataType)> = join
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.clone()))
        .collect();

    let right = Rel::read(&join.table, &join.alias, &join_cols);
    let on =
        expr::col(&entity.source_alias, &join.left_key).eq(expr::col(&join.alias, &join.right_key));

    left.join(expr::JoinType::Inner, right, on)
}

fn build_watermark_filter(rel: Rel, entity: &EntityDef) -> Rel {
    let alias = &entity.source_alias;
    let ver = &entity.version_column;

    let lower_bound = expr::col(alias, ver).gt(expr::param("last_watermark", DataType::String));
    let upper_bound = expr::col(alias, ver).le(expr::param("watermark", DataType::String));

    rel.filter(lower_bound.and(upper_bound))
}

/// Cursor pagination using DNF (disjunctive normal form).
///
/// For sort keys `[c1, c2, c3]` with values `[v1, v2, v3]`, generates:
///   `(c1 > v1) OR (c1 = v1 AND c2 > v2) OR (c1 = v1 AND c2 = v2 AND c3 > v3)`
fn build_cursor_filter(rel: Rel, cursor_values: &[(String, String)]) -> Rel {
    let disjuncts: Vec<Expr> = (0..cursor_values.len())
        .map(|i| {
            let mut conjuncts = Vec::with_capacity(i + 1);

            // Equality prefix: c0 = v0 AND c1 = v1 AND ... AND c_{i-1} = v_{i-1}
            for (col_name, val) in &cursor_values[..i] {
                conjuncts.push(expr::raw(&format!("{col_name} = '{val}'")));
            }

            // Strict greater-than on the i-th column
            let (col_name, val) = &cursor_values[i];
            conjuncts.push(expr::raw(&format!("{col_name} > '{val}'")));

            expr::and(conjuncts)
        })
        .collect();

    rel.filter(expr::or(disjuncts))
}

fn build_projection(rel: Rel, entity: &EntityDef) -> Rel {
    let alias = &entity.source_alias;
    let mut items: Vec<(Expr, &str)> = Vec::new();

    for col in &entity.columns {
        let table = col.table_alias.as_deref().unwrap_or(alias);
        let out = col.alias.as_deref().unwrap_or(&col.name);
        items.push((expr::col(table, &col.name), out));
    }

    if let Some(join) = &entity.join {
        for col in &join.columns {
            let table = col.table_alias.as_deref().unwrap_or(&join.alias);
            let out = col.alias.as_deref().unwrap_or(&col.name);
            items.push((expr::col(table, &col.name), out));
        }
    }

    // Always include version + deleted
    let ver_alias = "_version";
    let del_alias = "_deleted";
    items.push((expr::col(alias, &entity.version_column), ver_alias));
    items.push((expr::col(alias, &entity.deleted_column), del_alias));

    rel.project(&items)
}

fn build_sort(rel: Rel, entity: &EntityDef) -> Rel {
    let keys: Vec<(Expr, expr::SortDir)> = entity
        .sort_keys
        .iter()
        .map(|k| (resolve_sort_column(entity, k), expr::SortDir::Asc))
        .collect();

    rel.sort(&keys)
}

/// Resolve a sort key name to the correct table-qualified column.
fn resolve_sort_column(entity: &EntityDef, key: &str) -> Expr {
    // Check join columns first (e.g. traversal_path comes from the join table)
    if let Some(join) = &entity.join
        && join.columns.iter().any(|c| c.name == key)
    {
        return expr::col(&join.alias, key);
    }
    expr::col(&entity.source_alias, key)
}
