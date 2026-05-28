//! Shared helpers used by both plan and lower (emit), plus neighbors and pathfinding.

use std::collections::HashMap;

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::input::*;

// ─────────────────────────────────────────────────────────────────────────────
// Filter / predicate helpers
// ─────────────────────────────────────────────────────────────────────────────

pub fn filter_to_expr(alias: &str, prop: &str, filter: &InputFilter) -> Expr {
    let col = Expr::col(alias, prop);
    let val = || filter.value.clone().unwrap_or(serde_json::Value::Null);
    let str_val = || filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    let typed = |v: serde_json::Value| -> Expr {
        Expr::param(data_type_to_ch(filter.data_type.as_ref()), v)
    };

    match filter.op {
        None | Some(FilterOp::Eq) => Expr::eq(col, typed(val())),
        Some(FilterOp::Gt) => Expr::binary(Op::Gt, col, typed(val())),
        Some(FilterOp::Gte) => Expr::binary(Op::Ge, col, typed(val())),
        Some(FilterOp::Lt) => Expr::binary(Op::Lt, col, typed(val())),
        Some(FilterOp::Lte) => Expr::binary(Op::Le, col, typed(val())),
        Some(FilterOp::In) => {
            if let Some(arr) = filter.value.as_ref().and_then(|v| v.as_array()) {
                Expr::col_in(
                    alias,
                    prop,
                    data_type_to_ch(filter.data_type.as_ref()),
                    arr.clone(),
                )
                .unwrap_or_else(|| Expr::param(ChType::Bool, false))
            } else {
                Expr::param(ChType::Bool, false)
            }
        }
        Some(FilterOp::Contains) => Expr::func(
            "positionCaseInsensitive",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::StartsWith) => Expr::func(
            "startsWith",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::EndsWith) => Expr::func(
            "endsWith",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::IsNull) => Expr::unary(Op::IsNull, col),
        Some(FilterOp::IsNotNull) => Expr::unary(Op::IsNotNull, col),
        Some(FilterOp::TokenMatch) => Expr::func(
            "hasToken",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::AllTokens) => Expr::func(
            "hasAllTokens",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::AnyTokens) => Expr::func(
            "hasAnyTokens",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
    }
}

/// IN-list predicate: `alias.col IN (ids)` or `alias.col = id` for single.
pub fn id_list_predicate(alias: &str, col: &str, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::col(alias, col), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            alias,
            col,
            ChType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false))
    }
}

pub fn id_range_predicate(alias: &str, range: &InputIdRange) -> Expr {
    Expr::and(
        Expr::binary(
            Op::Ge,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::int(range.start),
        ),
        Expr::binary(
            Op::Le,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::int(range.end),
        ),
    )
}

pub fn node_ids_predicate(alias: &str, ids: &[i64]) -> Expr {
    id_list_predicate(alias, DEFAULT_PRIMARY_KEY, ids)
}

pub fn requested_columns(columns: &Option<ColumnSelection>) -> Vec<String> {
    match columns {
        Some(ColumnSelection::List(cols)) => cols.clone(),
        Some(ColumnSelection::All) => vec!["*".to_string()],
        None => vec![],
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge SELECT columns
// ─────────────────────────────────────────────────────────────────────────────

pub fn edge_select_columns(alias: &str) -> Vec<SelectExpr> {
    edge_select_columns_with_prefix(alias, alias)
}

pub fn edge_select_columns_with_prefix(alias: &str, prefix: &str) -> Vec<SelectExpr> {
    [
        (RELATIONSHIP_KIND_COLUMN, EDGE_TYPE_SUFFIX),
        (SOURCE_ID_COLUMN, EDGE_SRC_SUFFIX),
        (SOURCE_KIND_COLUMN, EDGE_SRC_TYPE_SUFFIX),
        (TARGET_ID_COLUMN, EDGE_DST_SUFFIX),
        (TARGET_KIND_COLUMN, EDGE_DST_TYPE_SUFFIX),
    ]
    .iter()
    .map(|(col, suffix)| SelectExpr::new(Expr::col(alias, *col), format!("{prefix}_{suffix}")))
    .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge table resolution
// ─────────────────────────────────────────────────────────────────────────────

pub fn resolve_edge_table(input: &Input, rel_types: &[String]) -> String {
    for t in rel_types {
        if let Some(table) = input.compiler.edge_table_for_rel.get(t) {
            return table.clone();
        }
    }
    input.compiler.default_edge_table.clone()
}

// ─────────────────────────────────────────────────────────────────────────────
// Data type conversion
// ─────────────────────────────────────────────────────────────────────────────

pub fn data_type_to_ch(dt: Option<&ontology::DataType>) -> ChType {
    match dt {
        Some(ontology::DataType::String | ontology::DataType::Enum | ontology::DataType::Uuid) => {
            ChType::String
        }
        Some(ontology::DataType::Int) => ChType::Int64,
        Some(ontology::DataType::Float) => ChType::Float64,
        Some(ontology::DataType::Bool) => ChType::Bool,
        Some(ontology::DataType::DateTime | ontology::DataType::Date) => ChType::DateTime64,
        None => ChType::String,
    }
}

pub fn rel_kind_filter_values(types: &[String]) -> Option<Vec<String>> {
    if super::normalize::is_wildcard(types) {
        None
    } else {
        Some(types.to_vec())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers used across lower/ modules
// ─────────────────────────────────────────────────────────────────────────────

/// `alias._deleted = false` predicate.
pub fn deleted_false(alias: &str) -> Expr {
    Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    )
}

pub fn rel_kind_filter(alias: &str, types: &[String]) -> Option<Expr> {
    if super::normalize::is_wildcard(types) {
        return None;
    }
    if types.len() == 1 {
        Some(Expr::eq(
            Expr::col(alias, RELATIONSHIP_KIND_COLUMN),
            Expr::string(&types[0]),
        ))
    } else {
        Expr::col_in(
            alias,
            RELATIONSHIP_KIND_COLUMN,
            ChType::String,
            types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
    }
}

/// Convert a denormalized tag filter into a ClickHouse expression.
/// Returns `None` for unsupported filter ops.
pub fn denorm_tag_expr(
    edge_alias: &str,
    tag_col: &str,
    tag_key: &str,
    filter: &InputFilter,
) -> Option<Expr> {
    match filter.op {
        None | Some(FilterOp::Eq) => {
            let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
            Some(Expr::func(
                "has",
                vec![
                    Expr::col(edge_alias, tag_col),
                    Expr::string(format!("{tag_key}:{val}")),
                ],
            ))
        }
        Some(FilterOp::In) => {
            let values = filter.value.as_ref().and_then(|v| v.as_array())?;
            let tags: Vec<String> = values
                .iter()
                .filter_map(|v| v.as_str().map(|s| format!("{tag_key}:{s}")))
                .collect();
            if tags.len() == 1 {
                Some(Expr::func(
                    "has",
                    vec![Expr::col(edge_alias, tag_col), Expr::string(&tags[0])],
                ))
            } else if !tags.is_empty() {
                Some(Expr::func(
                    "hasAny",
                    vec![
                        Expr::col(edge_alias, tag_col),
                        Expr::func("array", tags.iter().map(Expr::string).collect()),
                    ],
                ))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Build a TableRef for an edge scan: single table or UNION ALL across tables.
///
/// When multiple tables are involved, each UNION arm projects only the
/// columns common to all edge tables (the 6 reserved edge columns) so
/// that tables with extra columns (e.g. gl_code_edge's project_id/branch)
/// don't cause a ClickHouse "UNION different number of columns" error.
pub fn edge_table_scan(tables: &[String], alias: &str) -> TableRef {
    if tables.len() == 1 {
        TableRef::scan(&tables[0], alias)
    } else {
        let inner_alias = format!("_{alias}");
        let common_cols: Vec<SelectExpr> = ontology::constants::EDGE_RESERVED_COLUMNS
            .iter()
            .map(|col| SelectExpr::col(&inner_alias, *col))
            .collect();
        let arms: Vec<Query> = tables
            .iter()
            .map(|table| Query {
                select: common_cols.clone(),
                from: TableRef::scan(table, &inner_alias),
                ..Default::default()
            })
            .collect();
        TableRef::union_all(arms, alias)
    }
}

/// Build a latest-row scan over a ReplacingMergeTree node table.
///
/// `FINAL` applies the table engine's merge semantics at read time, so filters
/// are evaluated against the latest row rather than historical matching
/// versions.
pub fn dedup_query(
    alias: &str,
    table: &str,
    select: Vec<SelectExpr>,
    scan_where: Vec<Expr>,
) -> Query {
    Query {
        select,
        from: TableRef::scan_final(table, alias),
        where_clause: Expr::conjoin(scan_where),
        ..Default::default()
    }
}

/// Latest-row scan wrapped as a subquery TableRef + outer `_deleted=false` filter.
pub fn dedup_subquery(
    alias: &str,
    table: &str,
    select: Vec<SelectExpr>,
    scan_where: Vec<Expr>,
) -> (TableRef, Expr) {
    let query = dedup_query(alias, table, select, scan_where);
    (
        TableRef::Subquery {
            query: Box::new(query),
            alias: alias.to_string(),
        },
        deleted_false(alias),
    )
}

/// Whether any filter property lacks a denormalized edge column.
pub fn has_non_denorm_filters(
    entity: &str,
    filters: &[(String, InputFilter)],
    denorm_map: &HashMap<(String, String, String), (String, String)>,
) -> bool {
    filters.iter().any(|(prop, _)| {
        let src =
            denorm_map.contains_key(&(entity.to_string(), prop.clone(), "source".to_string()));
        let tgt =
            denorm_map.contains_key(&(entity.to_string(), prop.clone(), "target".to_string()));
        !src && !tgt
    })
}
