//! Hydration: fetch node properties for a set of IDs.
//!
//! Produces a UNION ALL of per-entity dedup scans with inline
//! `LIMIT 1 BY id` dedup and `_deleted=false` filtering.

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use super::plan::{HydrationNodePlan, HydrationPlan};

// ─── Plan ────────────────────────────────────────────────────────────────────

pub fn plan_hydration(input: &Input) -> Result<HydrationPlan> {
    if input.nodes.is_empty() {
        return Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        ));
    }
    let nodes = input
        .nodes
        .iter()
        .map(|node| {
            let table = node
                .table
                .as_ref()
                .ok_or_else(|| QueryError::Lowering("hydration node has no table".into()))?;
            let entity = node
                .entity
                .as_ref()
                .ok_or_else(|| QueryError::Lowering("hydration node has no entity".into()))?;
            let columns = match &node.columns {
                Some(ColumnSelection::List(cols)) => cols.clone(),
                _ => vec![],
            };
            Ok(HydrationNodePlan {
                alias: node.id.clone(),
                table: table.clone(),
                entity: entity.clone(),
                id_property: node.id_property.clone(),
                node_ids: node.node_ids.clone(),
                columns,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(HydrationPlan {
        nodes,
        limit: input.limit,
    })
}

// ─── Emit ────────────────────────────────────────────────────────────────────

pub fn emit_hydration(plan: HydrationPlan) -> Result<Node> {
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

    let dedup_scan = Query {
        select: vec![
            SelectExpr::new(Expr::col(alias, pk), pk),
            SelectExpr::new(Expr::col(alias, DELETED_COLUMN), DELETED_COLUMN),
            SelectExpr::star(),
        ],
        from: TableRef::scan(&node.table, alias),
        where_clause: Expr::conjoin(scan_where),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, pk)])),
        ..Default::default()
    };

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
        where_clause: Some(Expr::eq(
            Expr::col(alias, DELETED_COLUMN),
            Expr::param(ChType::Bool, false),
        )),
        ..Default::default()
    })
}
