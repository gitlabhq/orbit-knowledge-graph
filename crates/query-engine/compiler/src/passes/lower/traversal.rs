use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use crate::constants::*;
use crate::passes::plan::{Plan, Strategy};
use crate::passes::shared::edge_select_columns;
use crate::passes::shared::edge_select_columns_with_prefix;
use ontology::constants::{DEFAULT_PRIMARY_KEY, SOURCE_ID_COLUMN, TARGET_ID_COLUMN};

/// Deterministic per-row sort suffix: edge id pairs when edges are scanned
/// (flat/bidirectional chains), node PKs for edge-free shapes (FK elides the
/// edge tables, so its `e0` aliases are synthesized columns, not scans).
fn tie_breakers(plan: &Plan, edge_aliases: &[String]) -> Vec<OrderExpr> {
    if edge_aliases.is_empty() || matches!(plan.strategy, Strategy::Fk(_)) {
        let mut aliases: Vec<&String> = plan.nodes.keys().collect();
        aliases.sort();
        aliases
            .into_iter()
            .map(|a| OrderExpr::asc(Expr::col(a, DEFAULT_PRIMARY_KEY)))
            .collect()
    } else {
        edge_aliases
            .iter()
            .flat_map(|ea| {
                [
                    OrderExpr::asc(Expr::col(ea, SOURCE_ID_COLUMN)),
                    OrderExpr::asc(Expr::col(ea, TARGET_ID_COLUMN)),
                ]
            })
            .collect()
    }
}

pub fn emit_traversal(plan: &Plan) -> Result<Node> {
    if matches!(plan.strategy, Strategy::SingleNode) {
        return emit_single_node(plan);
    }

    let output = plan.emit_edge_chain()?;

    let mut select = Vec::new();
    let already_has_edge_cols = output.select.iter().any(|s| {
        s.alias
            .as_deref()
            .is_some_and(|a| a.ends_with(EDGE_TYPE_SUFFIX))
    });
    if !already_has_edge_cols {
        for (i, ea) in output.edge_aliases.iter().enumerate() {
            let is_multi = plan.hops.get(i).is_some_and(|h| h.max_hops > 1);
            if is_multi {
                let prefix = format!("hop_{ea}");
                select.extend(edge_select_columns_with_prefix(ea, &prefix));
                select.push(SelectExpr::new(
                    Expr::col(ea, PATH_NODES_COLUMN),
                    format!("{prefix}_{PATH_NODES_COLUMN}"),
                ));
            } else {
                select.extend(edge_select_columns(ea));
            }
        }
    }

    let mut order_by = plan
        .order_by
        .as_ref()
        .map(|ob| {
            vec![if matches!(ob.direction, OrderDirection::Desc) {
                OrderExpr::desc(Expr::col(&ob.node, &ob.property))
            } else {
                OrderExpr::asc(Expr::col(&ob.node, &ob.property))
            }]
        })
        .unwrap_or_default();
    if plan.cursor.is_some() {
        order_by.extend(tie_breakers(plan, &output.edge_aliases));
    }

    let q = output.into_query(select, vec![], order_by, plan.limit);
    Ok(Node::Query(Box::new(q)))
}

fn emit_single_node(plan: &Plan) -> Result<Node> {
    let output = plan.emit_edge_chain()?;

    let mut order_by = plan
        .order_by
        .as_ref()
        .map(|ob| {
            vec![if matches!(ob.direction, OrderDirection::Desc) {
                OrderExpr::desc(Expr::col(&ob.node, &ob.property))
            } else {
                OrderExpr::asc(Expr::col(&ob.node, &ob.property))
            }]
        })
        .unwrap_or_default();
    if plan.cursor.is_some() {
        order_by.extend(tie_breakers(plan, &output.edge_aliases));
    }

    let q = output.into_query(vec![], vec![], order_by, plan.limit);
    Ok(Node::Query(Box::new(q)))
}
