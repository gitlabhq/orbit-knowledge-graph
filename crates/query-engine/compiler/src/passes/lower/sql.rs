use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::BoundFilter;
use crate::passes::plan::helpers::denorm_tag_values;

pub(crate) fn latest_row_dedup(
    alias: &str,
    sort_key: &[String],
) -> (Vec<OrderExpr>, Option<(u32, Vec<Expr>)>) {
    let keys: Vec<_> = sort_key
        .iter()
        .map(|column| Expr::col(alias, column))
        .collect();
    let mut order: Vec<_> = keys.iter().cloned().map(OrderExpr::asc).collect();
    order.push(OrderExpr::desc(Expr::col(alias, VERSION_COLUMN)));
    (order, Some((1, keys)))
}

pub fn filter_to_expr(alias: &str, prop: &str, bound: &BoundFilter) -> Expr {
    filter_expression(
        alias,
        prop,
        &bound.filter,
        bound.data_type.as_ref(),
        bound.in_sort_key,
    )
}

pub(crate) fn filter_expression(
    alias: &str,
    prop: &str,
    filter: &InputFilter,
    data_type: Option<&ontology::DataType>,
    in_sort_key: bool,
) -> Expr {
    let col = Expr::col(alias, prop);

    if let Some((rhs_alias, rhs_prop)) = &filter.rhs_column {
        let rhs = Expr::col(rhs_alias, rhs_prop);
        return comparison(col, filter.op.unwrap_or(FilterOp::Eq), rhs)
            .expect("validated property comparison");
    }

    let val = || filter.value.clone().unwrap_or(serde_json::Value::Null);
    let typed = |v: serde_json::Value| -> Expr { Expr::param(parameter_type(data_type), v) };

    match filter.op.unwrap_or(FilterOp::Eq) {
        op @ (FilterOp::Eq
        | FilterOp::Ne
        | FilterOp::Gt
        | FilterOp::Gte
        | FilterOp::Lt
        | FilterOp::Lte) => comparison(col, op, typed(val())).expect("comparison operator"),
        FilterOp::In => {
            if let Some(arr) = filter.value.as_ref().and_then(|v| v.as_array()) {
                Expr::col_in(alias, prop, parameter_type(data_type), arr.clone())
                    .unwrap_or_else(|| Expr::param(SqlType::Bool, false))
            } else {
                Expr::param(SqlType::Bool, false)
            }
        }
        FilterOp::IsNull => Expr::unary(Op::IsNull, col),
        FilterOp::IsNotNull => Expr::unary(Op::IsNotNull, col),
        op => {
            let fold = |expr: Expr| {
                if in_sort_key {
                    expr
                } else {
                    Expr::func(Function::Lower, vec![expr])
                }
            };
            let needle = fold(Expr::param(
                SqlType::String,
                filter.value_str().unwrap_or(""),
            ));
            let col = fold(col);
            let mode = match op {
                FilterOp::Contains => TextMatch::Contains,
                FilterOp::TokenMatch => TextMatch::TokenMatch,
                FilterOp::AllTokens => TextMatch::AllTokens,
                FilterOp::AnyTokens => TextMatch::AnyTokens,
                FilterOp::StartsWith => return Expr::func(Function::StartsWith, vec![col, needle]),
                FilterOp::EndsWith => return Expr::func(Function::EndsWith, vec![col, needle]),
                _ => unreachable!(),
            };
            Expr::TextSearch {
                mode,
                value: Box::new(col),
                query: Box::new(needle),
            }
        }
    }
}

pub fn comparison(left: Expr, operator: FilterOp, right: Expr) -> Result<Expr> {
    let operator = match operator {
        FilterOp::Eq => Op::Eq,
        FilterOp::Ne => Op::Ne,
        FilterOp::Gt => Op::Gt,
        FilterOp::Gte => Op::Ge,
        FilterOp::Lt => Op::Lt,
        FilterOp::Lte => Op::Le,
        _ => {
            return Err(QueryError::Lowering("invalid property comparison".into()));
        }
    };
    Ok(Expr::binary(operator, left, right))
}

pub fn id_list_predicate(alias: &str, col: &str, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::col(alias, col), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            alias,
            col,
            SqlType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(SqlType::Bool, false))
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

pub fn parameter_type(dt: Option<&ontology::DataType>) -> SqlType {
    match dt {
        Some(ontology::DataType::String | ontology::DataType::Enum | ontology::DataType::Uuid) => {
            SqlType::String
        }
        Some(ontology::DataType::Int) => SqlType::Int64,
        Some(ontology::DataType::Float) => SqlType::Float64,
        Some(ontology::DataType::Bool) => SqlType::Bool,
        Some(ontology::DataType::DateTime | ontology::DataType::Date) => SqlType::Timestamp {
            precision: 6,
            timezone: Some(TimeZone::Utc),
        },
        None => SqlType::String,
    }
}

pub fn deleted_false(alias: &str) -> Expr {
    Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(SqlType::Bool, false),
    )
}

pub fn rel_kind_filter(alias: &str, types: &[String]) -> Option<Expr> {
    if crate::passes::normalize::is_wildcard(types) {
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
            SqlType::String,
            types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
    }
}

/// Returns `None` for unsupported filter ops.
pub fn denorm_tag_expr(
    edge_alias: &str,
    tag_col: &str,
    tag_key: &str,
    filter: &InputFilter,
) -> Option<Expr> {
    denorm_tag_values(tag_key, filter).map(|values| tag_membership(edge_alias, tag_col, &values))
}

pub(crate) fn tag_membership(alias: &str, column: &str, values: &[String]) -> Expr {
    if let [value] = values {
        Expr::func(
            Function::ArrayContains,
            vec![Expr::col(alias, column), Expr::string(value)],
        )
    } else {
        Expr::func(
            Function::ArrayContainsAny,
            vec![
                Expr::col(alias, column),
                Expr::func(Function::Array, values.iter().map(Expr::string).collect()),
            ],
        )
    }
}

/// When multiple tables are involved, each UNION arm projects only the
/// columns common to all edge tables (the 6 reserved edge columns) so
/// that tables with extra columns (e.g. gl_code_edge's project_id/branch)
/// don't cause a ClickHouse "UNION different number of columns" error.
pub fn edge_table_scan(tables: &[String], alias: &str) -> TableRef {
    if tables.len() == 1 {
        return TableRef::scan(&tables[0], alias);
    }
    let inner_alias = format!("_{alias}");
    let columns: Vec<_> = EDGE_RESERVED_COLUMNS
        .iter()
        .chain([&DELETED_COLUMN])
        .map(|column| SelectExpr::col(&inner_alias, *column))
        .collect();
    let queries = tables
        .iter()
        .map(|table| Query {
            select: columns.clone(),
            from: TableRef::scan(table, &inner_alias),
            ..Default::default()
        })
        .collect();
    TableRef::union_all(queries, alias)
}

/// Like `edge_table_scan` but pushes per-arm predicates into each UNION arm; returns predicates the caller must apply on the enclosing query (single-table case).
pub fn edge_table_scan_filtered(
    tables: &[String],
    alias: &str,
    arm_where: impl Fn(&str) -> Vec<Expr>,
) -> (TableRef, Vec<Expr>) {
    if tables.len() == 1 {
        (TableRef::scan(&tables[0], alias), arm_where(alias))
    } else {
        let inner_alias = format!("_{alias}");
        let mut common_cols: Vec<SelectExpr> = ontology::constants::EDGE_RESERVED_COLUMNS
            .iter()
            .map(|col| SelectExpr::col(&inner_alias, *col))
            .collect();
        common_cols.push(SelectExpr::col(&inner_alias, DELETED_COLUMN));
        let arms: Vec<Query> = tables
            .iter()
            .map(|table| Query {
                select: common_cols.clone(),
                from: TableRef::scan(table, &inner_alias),
                where_clause: Expr::conjoin(arm_where(&inner_alias)),
                ..Default::default()
            })
            .collect();
        (TableRef::union_all(arms, alias), Vec::new())
    }
}

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
