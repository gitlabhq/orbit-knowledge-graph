//! Lower: Input → Plan
//!
//! Transforms indexer inputs into `Rel` trees. Called by the thin
//! `Frontend` wrappers in `frontend.rs`.
//!
//! | Entry point        | SQL target  | Query shape |
//! |--------------------|-------------|-------------|
//! | `extract`          | ClickHouse  | Read [→ Join] → Filter(watermark [+ cursor]) → Project → Sort → Fetch |
//! | `raw_extract`      | ClickHouse  | Read_raw → Filter(watermark [+ ns + extra + cursor]) → Project → Sort → Fetch |
//! | `node_transform`   | DataFusion  | Read(source_data) → Project |
//! | `fk_edge_transform`| DataFusion  | Read(source_data) [→ Filter] → Project |

use super::types::*;
use llqm::ir::expr::{self, DataType, Expr};
use llqm::ir::plan::{Plan, Rel};

const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";
const SOURCE_DATA_TABLE: &str = "source_data";

#[derive(Debug, thiserror::Error)]
pub enum LowerError {
    #[error("{0} has no columns")]
    NoColumns(&'static str),
}

// ─────────────────────────────────────────────────────────────────────────────
// Table-based Extract
// ─────────────────────────────────────────────────────────────────────────────

pub fn extract(input: ExtractInput) -> Result<Plan, LowerError> {
    let entity = &input.entity;
    if entity.columns.is_empty() && entity.join.as_ref().is_none_or(|j| j.columns.is_empty()) {
        return Err(LowerError::NoColumns("entity"));
    }

    let mut rel = extract_read(entity);

    if let Some(join) = &entity.join {
        rel = extract_join(rel, entity, join);
    }

    rel = watermark_filter(rel, expr::col(&entity.source_alias, &entity.version_column));

    if !input.cursor_values.is_empty() {
        rel = cursor_filter(rel, &input.cursor_values);
    }

    rel = extract_projection(rel, entity);

    let sort_keys: Vec<_> = entity
        .sort_keys
        .iter()
        .map(|k| (resolve_sort_column(entity, k), expr::SortDir::Asc))
        .collect();
    rel = rel.sort(&sort_keys);
    rel = rel.fetch(input.batch_size, None);

    Ok(rel.into_plan())
}

fn extract_read(entity: &EntityDef) -> Rel {
    let mut cols: Vec<(&str, DataType)> = entity
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.clone()))
        .collect();
    cols.push((entity.version_column.as_str(), DataType::String));
    cols.push((entity.deleted_column.as_str(), DataType::Bool));
    Rel::read(&entity.source_table, &entity.source_alias, &cols)
}

fn extract_join(left: Rel, entity: &EntityDef, join: &JoinDef) -> Rel {
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

fn extract_projection(rel: Rel, entity: &EntityDef) -> Rel {
    let alias = &entity.source_alias;
    let entity_cols = entity.columns.iter().map(|c| col_def_item(c, alias));
    let join_cols = entity
        .join
        .iter()
        .flat_map(|j| j.columns.iter().map(|c| col_def_item(c, &j.alias)));

    let mut items: Vec<(Expr, &str)> = entity_cols.chain(join_cols).collect();
    items.push((expr::col(alias, &entity.version_column), VERSION_ALIAS));
    items.push((expr::col(alias, &entity.deleted_column), DELETED_ALIAS));

    rel.project(&items)
}

fn col_def_item<'a>(col: &'a ColumnDef, default_table: &'a str) -> (Expr, &'a str) {
    let table = col.table_alias.as_deref().unwrap_or(default_table);
    let out = col.alias.as_deref().unwrap_or(&col.name);
    (expr::col(table, &col.name), out)
}

fn resolve_sort_column(entity: &EntityDef, key: &str) -> Expr {
    if let Some(join) = &entity.join
        && join.columns.iter().any(|c| c.name == key)
    {
        return expr::col(&join.alias, key);
    }
    expr::col(&entity.source_alias, key)
}

// ─────────────────────────────────────────────────────────────────────────────
// Query-based Extract
// ─────────────────────────────────────────────────────────────────────────────

pub fn raw_extract(input: RawExtractInput) -> Result<Plan, LowerError> {
    if input.columns.is_empty() {
        return Err(LowerError::NoColumns("extract"));
    }

    let read_cols: Vec<_> = input
        .columns
        .iter()
        .map(|c| {
            let name = match c {
                RawExtractColumn::Bare(s) => column_alias(s),
                RawExtractColumn::ToString(s) => s.as_str(),
            };
            (name, DataType::String)
        })
        .collect();
    let mut rel = Rel::read_raw(&input.from, &read_cols);

    rel = watermark_filter(rel, expr::raw(&input.watermark));

    if input.namespaced {
        rel = match input.traversal_path_filter.as_deref() {
            Some(filter) => rel.filter(expr::raw(filter)),
            None => rel.filter(
                expr::col("", "traversal_path")
                    .starts_with(expr::param("traversal_path", DataType::String)),
            ),
        };
    }

    if let Some(extra) = &input.additional_where {
        rel = rel.filter(expr::raw(extra));
    }

    if !input.cursor_values.is_empty() {
        rel = cursor_filter(rel, &input.cursor_values);
    }

    let mut items: Vec<(Expr, &str)> = input
        .columns
        .iter()
        .map(|c| match c {
            RawExtractColumn::Bare(s) => (expr::raw(s), column_alias(s)),
            RawExtractColumn::ToString(name) => (
                expr::func("toString", vec![expr::col("", name)]),
                name.as_str(),
            ),
        })
        .collect();
    items.push((expr::raw(&input.watermark), VERSION_ALIAS));
    items.push((expr::raw(&input.deleted), DELETED_ALIAS));
    rel = rel.project(&items);

    let sort_keys: Vec<_> = input
        .order_by
        .iter()
        .map(|k| (expr::raw(k), expr::SortDir::Asc))
        .collect();
    rel = rel.sort(&sort_keys);
    rel = rel.fetch(input.batch_size, None);

    Ok(rel.into_plan())
}

/// `"project.id AS id"` → `"id"`, `"id"` → `"id"`
fn column_alias(expr: &str) -> &str {
    if let Some(pos) = expr.to_lowercase().rfind(" as ") {
        expr[pos + 4..].trim()
    } else {
        expr.trim()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Node Transform
// ─────────────────────────────────────────────────────────────────────────────

pub fn node_transform(input: NodeTransformInput) -> Result<Plan, LowerError> {
    if input.columns.is_empty() {
        return Err(LowerError::NoColumns("node transform"));
    }

    let mut read_cols: Vec<(&str, DataType)> = input
        .columns
        .iter()
        .map(|c| (c.source_name(), DataType::String))
        .collect();
    read_cols.push((VERSION_ALIAS, DataType::String));
    read_cols.push((DELETED_ALIAS, DataType::Bool));
    let rel = Rel::read(SOURCE_DATA_TABLE, "", &read_cols);

    let mut items: Vec<(Expr, &str)> = input.columns.iter().map(lower_node_column).collect();
    items.push((expr::col("", VERSION_ALIAS), VERSION_ALIAS));
    items.push((expr::col("", DELETED_ALIAS), DELETED_ALIAS));

    Ok(rel.project(&items).into_plan())
}

fn lower_node_column(column: &NodeColumn) -> (Expr, &str) {
    match column {
        NodeColumn::Identity(name) => (expr::col("", name), name.as_str()),
        NodeColumn::Rename { source, target } => (expr::col("", source), target.as_str()),
        NodeColumn::IntEnum {
            source,
            target,
            values,
        } => {
            let ifs: Vec<(Expr, Expr)> = values
                .iter()
                .map(|(key, value)| {
                    (
                        expr::col("", source).eq(expr::int(*key)),
                        expr::string(value),
                    )
                })
                .collect();
            (
                expr::if_then(ifs, Some(expr::string("unknown"))),
                target.as_str(),
            )
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FK Edge Transform
// ─────────────────────────────────────────────────────────────────────────────

pub fn fk_edge_transform(input: FkEdgeTransformInput) -> Result<Plan, LowerError> {
    let mut rel = Rel::read(
        SOURCE_DATA_TABLE,
        "",
        &[
            ("id", DataType::Int64),
            ("traversal_path", DataType::String),
            (VERSION_ALIAS, DataType::String),
            (DELETED_ALIAS, DataType::Bool),
        ],
    );

    if !input.filters.is_empty() {
        let exprs: Vec<Expr> = input.filters.iter().map(lower_edge_filter).collect();
        rel = rel.filter(expr::and(exprs));
    }

    let traversal_path = if input.namespaced {
        expr::col("", "traversal_path")
    } else {
        expr::raw("'0/'")
    };

    Ok(rel
        .project(&[
            (traversal_path, "traversal_path"),
            (lower_edge_id(&input.source_id), "source_id"),
            (lower_edge_kind(&input.source_kind), "source_kind"),
            (
                expr::raw(&format!("'{}'", input.relationship_kind)),
                "relationship_kind",
            ),
            (lower_edge_id(&input.target_id), "target_id"),
            (lower_edge_kind(&input.target_kind), "target_kind"),
            (expr::col("", VERSION_ALIAS), VERSION_ALIAS),
            (expr::col("", DELETED_ALIAS), DELETED_ALIAS),
        ])
        .into_plan())
}

fn lower_edge_id(id: &EdgeId) -> Expr {
    match id {
        EdgeId::Column(column) => expr::col("", column),
        EdgeId::Exploded { column, delimiter } => Expr::Cast {
            expr: Box::new(expr::func(
                "NULLIF",
                vec![
                    expr::func(
                        "unnest",
                        vec![expr::func(
                            "string_to_array",
                            vec![expr::col("", column), expr::raw(&format!("'{delimiter}'"))],
                        )],
                    ),
                    expr::raw("''"),
                ],
            )),
            target_type: DataType::Int64,
        },
    }
}

fn lower_edge_kind(kind: &EdgeKind) -> Expr {
    match kind {
        EdgeKind::Literal(value) => expr::raw(&format!("'{value}'")),
        EdgeKind::Column(column) => expr::col("", column),
        EdgeKind::TypeMapping { column, mapping } => {
            let ifs: Vec<(Expr, Expr)> = mapping
                .iter()
                .map(|(from, to)| {
                    (
                        expr::col("", column).eq(expr::string(from)),
                        expr::string(to),
                    )
                })
                .collect();
            expr::if_then(ifs, Some(expr::col("", column)))
        }
    }
}

fn lower_edge_filter(filter: &EdgeFilter) -> Expr {
    match filter {
        EdgeFilter::IsNotNull(column) => expr::col("", column).is_not_null(),
        EdgeFilter::NotEmpty(column) => expr::col("", column).ne(expr::raw("''")),
        EdgeFilter::TypeIn { column, types } => {
            let list: Vec<Expr> = types.iter().map(|t| expr::string(t)).collect();
            expr::col("", column).in_list(list)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// `watermark > {last_watermark:String} AND watermark <= {watermark:String}`
fn watermark_filter(rel: Rel, wm: Expr) -> Rel {
    let lower = wm
        .clone()
        .gt(expr::param("last_watermark", DataType::String));
    let upper = wm.le(expr::param("watermark", DataType::String));
    rel.filter(lower.and(upper))
}

/// Cursor pagination using DNF (disjunctive normal form).
///
/// For sort keys `[c1, c2, c3]` with values `[v1, v2, v3]`, generates:
///   `(c1 > v1) OR (c1 = v1 AND c2 > v2) OR (c1 = v1 AND c2 = v2 AND c3 > v3)`
fn cursor_filter(rel: Rel, cursor_values: &[(String, String)]) -> Rel {
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
