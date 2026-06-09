//! Emit: single node (no edges).

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::ast::*;
use crate::error::{QueryError, Result};

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
    let is_aggregation = matches!(plan.body, PlanBody::Aggregation { .. });

    let from = TableRef::scan_final(table, alias);
    let mut where_parts = latest_node_predicates(alias, np);
    let select = node_select_columns(alias, np);

    let mut ctes = Vec::new();
    if is_aggregation && !where_parts.is_empty() {
        let cte_name = format!("_candidate_{alias}");
        ctes.push(Cte::new(
            &cte_name,
            Query {
                select: vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)],
                from: TableRef::scan(table, alias),
                where_clause: Expr::conjoin(where_parts.clone()),
                ..Default::default()
            },
        ));
        where_parts.push(Expr::InSubquery {
            expr: Box::new(Expr::col(alias, DEFAULT_PRIMARY_KEY)),
            cte_name,
            column: DEFAULT_PRIMARY_KEY.to_string(),
        });
    }

    Ok(EmitOutput {
        from,
        edge_aliases: vec![],
        where_parts,
        select,
        ctes,
        edge_if_predicates: None,
    })
}
