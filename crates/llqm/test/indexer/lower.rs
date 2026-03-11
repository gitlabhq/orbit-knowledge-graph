//! Lower: Input → Plan
//!
//! Transforms indexer inputs into `Rel` trees. Called by the thin
//! `Frontend` wrappers in `frontend.rs`.
//!
//! | Entry point       | SQL target  | Query shape |
//! |-------------------|-------------|-------------|
//! | `extract`         | ClickHouse  | Read [→ Join] → Filter(watermark [+ cursor]) → Project → Sort → Fetch |
//! | `raw_extract`     | ClickHouse  | Read_raw → Filter(watermark [+ ns + extra + cursor]) → Project → Sort → Fetch |
//! | `node_transform`  | DataFusion  | Read(source_data) → Project |
//! | `fk_edge_transform` | DataFusion | Read(source_data) [→ Filter] → Project |

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

    rel = extract_watermark(rel, entity);

    if !input.cursor_values.is_empty() {
        rel = build_cursor_filter(rel, &input.cursor_values);
    }

    rel = extract_projection(rel, entity);
    rel = extract_sort(rel, entity);
    rel = rel.fetch(input.batch_size, None);

    Ok(rel.into_plan())
}

fn extract_read(entity: &EntityDef) -> Rel {
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

fn extract_watermark(rel: Rel, entity: &EntityDef) -> Rel {
    let alias = &entity.source_alias;
    let ver = &entity.version_column;

    let lower_bound = expr::col(alias, ver).gt(expr::param("last_watermark", DataType::String));
    let upper_bound = expr::col(alias, ver).le(expr::param("watermark", DataType::String));

    rel.filter(lower_bound.and(upper_bound))
}

fn extract_projection(rel: Rel, entity: &EntityDef) -> Rel {
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

    items.push((expr::col(alias, &entity.version_column), VERSION_ALIAS));
    items.push((expr::col(alias, &entity.deleted_column), DELETED_ALIAS));

    rel.project(&items)
}

fn extract_sort(rel: Rel, entity: &EntityDef) -> Rel {
    let keys: Vec<(Expr, expr::SortDir)> = entity
        .sort_keys
        .iter()
        .map(|k| (resolve_sort_column(entity, k), expr::SortDir::Asc))
        .collect();

    rel.sort(&keys)
}

/// Resolve a sort key name to the correct table-qualified column.
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

    let output_cols = derive_output_columns(&input.columns);
    let mut rel = Rel::read_raw(&input.from, &output_cols);

    rel = raw_watermark(rel, &input.watermark);

    if input.namespaced {
        rel = raw_namespace_filter(rel, input.traversal_path_filter.as_deref());
    }

    if let Some(extra) = &input.additional_where {
        rel = rel.filter(expr::raw(extra));
    }

    if !input.cursor_values.is_empty() {
        rel = build_cursor_filter(rel, &input.cursor_values);
    }

    rel = raw_projection(rel, &input.columns, &input.watermark, &input.deleted);

    let sort_keys: Vec<(Expr, expr::SortDir)> = input
        .order_by
        .iter()
        .map(|k| (expr::raw(k), expr::SortDir::Asc))
        .collect();
    rel = rel.sort(&sort_keys);
    rel = rel.fetch(input.batch_size, None);

    Ok(rel.into_plan())
}

fn derive_output_columns(columns: &[RawExtractColumn]) -> Vec<(&str, DataType)> {
    columns
        .iter()
        .map(|c| {
            let name = match c {
                RawExtractColumn::Bare(s) => extract_column_alias(s),
                RawExtractColumn::ToString(s) => s.as_str(),
            };
            (name, DataType::String)
        })
        .collect()
}

/// Extract the output alias from a raw column expression.
/// `"project.id AS id"` → `"id"`, `"id"` → `"id"`
fn extract_column_alias(expr: &str) -> &str {
    if let Some(pos) = expr.to_lowercase().rfind(" as ") {
        expr[pos + 4..].trim()
    } else {
        expr.trim()
    }
}

// {watermark_sql} > ("{last_watermark:String}") AND {watermark_sql} <= ("{watermark:String}")
fn raw_watermark(rel: Rel, watermark: &str) -> Rel {
    let lower = expr::raw(watermark).gt(expr::param("last_watermark", DataType::String));
    let upper = expr::raw(watermark).le(expr::param("watermark", DataType::String));
    rel.filter(lower.and(upper))
}

fn raw_namespace_filter(rel: Rel, custom_filter: Option<&str>) -> Rel {
    match custom_filter {
        Some(filter) => rel.filter(expr::raw(filter)),
        None => rel.filter(
            expr::col("", "traversal_path")
                .starts_with(expr::param("traversal_path", DataType::String)),
        ),
    }
}

fn raw_projection(rel: Rel, columns: &[RawExtractColumn], watermark: &str, deleted: &str) -> Rel {
    let mut items: Vec<(Expr, &str)> = columns
        .iter()
        .map(|c| match c {
            RawExtractColumn::Bare(s) => (expr::raw(s), extract_column_alias(s)),
            RawExtractColumn::ToString(name) => (
                expr::func("toString", vec![expr::col("", name)]),
                name.as_str(),
            ),
        })
        .collect();

    items.push((expr::raw(watermark), VERSION_ALIAS));
    items.push((expr::raw(deleted), DELETED_ALIAS));

    rel.project(&items)
}

// ─────────────────────────────────────────────────────────────────────────────
// Node Transform
// ─────────────────────────────────────────────────────────────────────────────

pub fn node_transform(input: NodeTransformInput) -> Result<Plan, LowerError> {
    if input.columns.is_empty() {
        return Err(LowerError::NoColumns("node transform"));
    }

    let source = source_data_read(&input.columns);
    let rel = node_projection(source, &input.columns);
    Ok(rel.into_plan())
}

fn source_data_read(columns: &[NodeColumn]) -> Rel {
    let mut cols: Vec<(&str, DataType)> = columns
        .iter()
        .map(|c| {
            let name = match c {
                NodeColumn::Identity(n) => n.as_str(),
                NodeColumn::Rename { source, .. } => source.as_str(),
                NodeColumn::IntEnum { source, .. } => source.as_str(),
            };
            (name, DataType::String)
        })
        .collect();
    cols.push((VERSION_ALIAS, DataType::String));
    cols.push((DELETED_ALIAS, DataType::Bool));
    Rel::read(SOURCE_DATA_TABLE, "", &cols)
}

fn node_projection(rel: Rel, columns: &[NodeColumn]) -> Rel {
    let mut items: Vec<(Expr, &str)> = columns.iter().map(lower_node_column).collect();
    items.push((expr::col("", VERSION_ALIAS), VERSION_ALIAS));
    items.push((expr::col("", DELETED_ALIAS), DELETED_ALIAS));
    rel.project(&items)
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
            let case = expr::if_then(ifs, Some(expr::string("unknown")));
            (case, target.as_str())
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FK Edge Transform
// ─────────────────────────────────────────────────────────────────────────────

pub fn fk_edge_transform(input: FkEdgeTransformInput) -> Result<Plan, LowerError> {
    let source = edge_source_data_read();
    let rel = build_edge_query(source, &input);
    Ok(rel.into_plan())
}

fn edge_source_data_read() -> Rel {
    Rel::read(
        SOURCE_DATA_TABLE,
        "",
        &[
            ("id", DataType::Int64),
            ("traversal_path", DataType::String),
            (VERSION_ALIAS, DataType::String),
            (DELETED_ALIAS, DataType::Bool),
        ],
    )
}

fn build_edge_query(rel: Rel, input: &FkEdgeTransformInput) -> Rel {
    let mut rel = rel;

    if let Some(filter) = build_edge_filter(&input.filters) {
        rel = rel.filter(filter);
    }

    let traversal_path_expr = if input.namespaced {
        expr::col("", "traversal_path")
    } else {
        expr::raw("'0/'")
    };

    let items: Vec<(Expr, &str)> = vec![
        (traversal_path_expr, "traversal_path"),
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
    ];

    rel.project(&items)
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
            let fallback = expr::col("", column);
            expr::if_then(ifs, Some(fallback))
        }
    }
}

fn build_edge_filter(filters: &[EdgeFilter]) -> Option<Expr> {
    let exprs: Vec<Expr> = filters.iter().map(lower_edge_filter).collect();
    if exprs.is_empty() {
        None
    } else {
        Some(expr::and(exprs))
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

/// Cursor pagination using DNF (disjunctive normal form).
///
/// For sort keys `[c1, c2, c3]` with values `[v1, v2, v3]`, generates:
///   `(c1 > v1) OR (c1 = v1 AND c2 > v2) OR (c1 = v1 AND c2 = v2 AND c3 > v3)`
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
