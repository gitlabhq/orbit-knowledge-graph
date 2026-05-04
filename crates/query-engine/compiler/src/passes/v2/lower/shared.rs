//! Shared helpers used by both plan and emit, plus neighbors and pathfinding.

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

pub(super) fn data_type_to_ch(dt: Option<&ontology::DataType>) -> ChType {
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

pub(super) fn rel_kind_filter_values(types: &[String]) -> Option<Vec<String>> {
    if types.is_empty() || (types.len() == 1 && types[0] == "*") {
        None
    } else {
        Some(types.to_vec())
    }
}
