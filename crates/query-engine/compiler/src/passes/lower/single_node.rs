//! Emit: single node (no edges).

use crate::ast::*;
use crate::error::{QueryError, Result};
use ontology::constants::VERSION_COLUMN;

use super::EmitOutput;
use super::helpers::{latest_node_predicates, node_select_columns};
use crate::passes::plan::*;

pub(super) fn emit_single_node(plan: &Plan) -> Result<EmitOutput> {
    let (_, np) = plan
        .nodes
        .iter()
        .next()
        .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;
    let sort_key = plan
        .table_sort_keys
        .get(table)
        .ok_or_else(|| QueryError::Lowering(format!("no sort key for node table '{table}'")))?;

    let mut order_by: Vec<OrderExpr> = sort_key
        .iter()
        .map(|col| OrderExpr::asc(Expr::col(alias, col)))
        .collect();
    order_by.push(OrderExpr::desc(Expr::col(alias, VERSION_COLUMN)));
    let limit_by_cols: Vec<Expr> = sort_key.iter().map(|col| Expr::col(alias, col)).collect();

    let from = TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan(table, alias),
            where_clause: Expr::conjoin(latest_node_predicates(alias, np)),
            order_by,
            limit_by: Some((1, limit_by_cols)),
            ..Default::default()
        },
        alias,
    );
    let select = node_select_columns(alias, np);

    Ok(EmitOutput {
        from,
        edge_aliases: vec![],
        where_parts: vec![],
        select,
        ctes: vec![],
        edge_if_predicates: None,
    })
}
