use crate::ast::*;
use crate::passes::shared::latest_row_dedup;

pub(super) fn limit_by_scan(
    table: &str,
    alias: &str,
    select: Vec<SelectExpr>,
    sort_key: &[String],
    where_predicates: Vec<Expr>,
) -> TableRef {
    let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
    let query = Query {
        select,
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(where_predicates),
        order_by,
        limit_by,
        ..Default::default()
    };
    TableRef::subquery(query, alias)
}
