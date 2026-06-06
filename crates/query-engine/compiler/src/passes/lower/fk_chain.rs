//! Emit: FK chain (every hop is FK-derived, linear chain, zero edge scans).
//!
//! A chain like `MergeRequest -HAS_DIFF-> MergeRequestDiff -HAS_FILE->
//! MergeRequestDiffFile` is fully encoded by the foreign keys the child nodes
//! carry (`diff.merge_request_id`, `file.merge_request_diff_id`). The edges in
//! `gl_edge` are a materialization of those FKs, so the traversal can be
//! answered by joining the node tables on their FK columns directly, skipping
//! the (large) edge scans entirely. The edge rows are synthesized from the FK
//! join for the graph formatter.

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;

use super::EmitOutput;
use super::helpers::{latest_node_predicates, node_select_columns};
use crate::passes::plan::*;

/// Latest-row, `_deleted`-filtered scan of a node (SELECT * so the FK columns,
/// the redaction id column, and requested columns are all available). The broad
/// authorization filter is layered on by the security pass against this alias;
/// `scope_prefix` is the tighter project/group prefix that lets ClickHouse seek
/// the node PK to a contiguous range instead of the whole authorized scope.
fn node_scan(np: &NodePlan, plan: &Plan, scope_prefix: Option<&str>) -> Result<TableRef> {
    let alias = &np.alias;
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' has no table")))?;
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

    let mut where_parts = latest_node_predicates(alias, np);
    if np.has_traversal_path
        && let Some(prefix) = scope_prefix
    {
        where_parts.push(Expr::func(
            "startsWith",
            vec![
                Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                Expr::string(prefix),
            ],
        ));
    }

    Ok(TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan(table, alias),
            where_clause: Expr::conjoin(where_parts),
            order_by,
            limit_by: Some((1, limit_by_cols)),
            ..Default::default()
        },
        alias,
    ))
}

pub(super) fn emit_fk_chain(plan: &Plan) -> Result<EmitOutput> {
    let root_alias = &plan.hops[0].from_node;
    let root_np = plan
        .nodes
        .get(root_alias)
        .ok_or_else(|| QueryError::Lowering(format!("FK chain root '{root_alias}' not found")))?;

    let mut from = node_scan(root_np, plan, plan.hops[0].scope_prefix.as_deref())?;
    let mut selects = node_select_columns(root_alias, root_np);
    let mut edge_aliases = Vec::new();

    for (i, hop) in plan.hops.iter().enumerate() {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FK chain hop missing FK metadata".into()))?;
        let to_np = plan.nodes.get(&hop.to_node).ok_or_else(|| {
            QueryError::Lowering(format!("FK chain node '{}' not found", hop.to_node))
        })?;

        // Join the next node via the FK column, whichever endpoint holds it.
        let on = if fk.fk_node == hop.to_node {
            Expr::eq(
                Expr::col(&hop.to_node, &fk.fk_column),
                Expr::col(&hop.from_node, DEFAULT_PRIMARY_KEY),
            )
        } else {
            Expr::eq(
                Expr::col(&hop.from_node, &fk.fk_column),
                Expr::col(&hop.to_node, DEFAULT_PRIMARY_KEY),
            )
        };
        from = TableRef::join(
            JoinType::Inner,
            from,
            node_scan(to_np, plan, hop.scope_prefix.as_deref())?,
            on,
        );
        selects.extend(node_select_columns(&hop.to_node, to_np));

        // Synthesize the edge row in physical (ontology) orientation so source
        // and target match the edge-scan path even after reorder_by_selectivity
        // reverses the hop: the edge's source_id side is from_node for an
        // outgoing hop and to_node for an incoming one.
        let ea = format!("e{i}");
        let (src_node, dst_node) = match hop.direction {
            Direction::Incoming => (&hop.to_node, &hop.from_node),
            Direction::Outgoing | Direction::Both => (&hop.from_node, &hop.to_node),
        };
        let entity = |alias: &str| {
            plan.nodes
                .get(alias)
                .and_then(|n| n.entity.as_deref())
                .unwrap_or("")
        };
        let rel_type = hop.rel_types.first().map(String::as_str).unwrap_or("");
        selects.push(SelectExpr::new(
            Expr::string(rel_type),
            format!("{ea}_{EDGE_TYPE_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::col(src_node, DEFAULT_PRIMARY_KEY),
            format!("{ea}_{EDGE_SRC_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::string(entity(src_node)),
            format!("{ea}_{EDGE_SRC_TYPE_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::col(dst_node, DEFAULT_PRIMARY_KEY),
            format!("{ea}_{EDGE_DST_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::string(entity(dst_node)),
            format!("{ea}_{EDGE_DST_TYPE_SUFFIX}"),
        ));
        edge_aliases.push(ea);
    }

    Ok(EmitOutput {
        from,
        edge_aliases,
        where_parts: Vec::new(),
        select: selects,
        ctes: Vec::new(),
        edge_if_predicates: None,
    })
}
