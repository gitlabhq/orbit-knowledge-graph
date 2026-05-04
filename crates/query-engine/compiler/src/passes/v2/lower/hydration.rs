//! Hydration emit: fetch node properties for a set of IDs.
//!
//! Produces a UNION ALL of per-entity dedup scans with inline
//! `LIMIT 1 BY id` dedup and `_deleted=false` filtering.

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::super::plan::{HydrationNodePlan, HydrationPlan};
use super::super::shared::{dedup_query, deleted_false};

// ─── Emit ────────────────────────────────────────────────────────────────────

pub fn emit_hydration(plan: &HydrationPlan) -> Result<Node> {
    let mut arms = plan.nodes.iter().map(emit_arm);
    let mut first = arms
        .next()
        .ok_or_else(|| QueryError::Lowering("hydration requires at least one node".into()))??;
    for arm in arms {
        first.union_all.push(arm?);
    }
    first.limit = Some(plan.limit);
    Ok(Node::Query(Box::new(first)))
}

fn emit_arm(node: &HydrationNodePlan) -> Result<Query> {
    let alias = &node.alias;
    let pk = &node.id_property;

    let json_expr = if node.columns.is_empty() {
        Expr::string("{}")
    } else {
        let map_args: Vec<Expr> = node
            .columns
            .iter()
            .flat_map(|col| {
                [
                    Expr::string(col),
                    Expr::func("toString", vec![Expr::col(alias, col)]),
                ]
            })
            .collect();
        Expr::func("toJSONString", vec![Expr::func("map", map_args)])
    };

    let mut scan_where = Vec::new();
    if let Some(id_filter) = Expr::col_in(
        alias,
        pk,
        ChType::Int64,
        node.node_ids
            .iter()
            .map(|id| serde_json::Value::Number((*id).into()))
            .collect(),
    ) {
        scan_where.push(id_filter);
    }

    let dedup_scan = dedup_query(
        alias,
        &node.table,
        vec![
            SelectExpr::new(Expr::col(alias, pk), pk),
            SelectExpr::new(Expr::col(alias, DELETED_COLUMN), DELETED_COLUMN),
            SelectExpr::star(),
        ],
        scan_where,
        pk,
    );

    Ok(Query {
        select: vec![
            SelectExpr::new(Expr::col(alias, pk), format!("{alias}_{pk}")),
            SelectExpr::new(Expr::string(&node.entity), format!("{alias}_entity_type")),
            SelectExpr::new(json_expr, format!("{alias}_props")),
        ],
        from: TableRef::Subquery {
            query: Box::new(dedup_scan),
            alias: alias.to_string(),
        },
        where_clause: Some(deleted_false(alias)),
        ..Default::default()
    })
}
