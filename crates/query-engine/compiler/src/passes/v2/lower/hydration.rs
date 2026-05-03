//! Hydration: fetch node properties for a set of IDs.
//!
//! Produces a UNION ALL of per-entity dedup scans with inline
//! `LIMIT 1 BY id` dedup and `_deleted=false` filtering. No
//! OptimizePass or DeduplicatePass needed.

use ontology::constants::*;

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;

pub fn lower_hydration(input: &mut Input) -> Result<Node> {
    if input.nodes.is_empty() {
        return Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        ));
    }

    let mut first_query = build_arm(&input.nodes[0])?;
    for node in &input.nodes[1..] {
        first_query.union_all.push(build_arm(node)?);
    }
    first_query.limit = Some(input.limit);

    Ok(Node::Query(Box::new(first_query)))
}

fn build_arm(node: &InputNode) -> Result<Query> {
    let table = node
        .table
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("hydration node has no table".into()))?;
    let entity = node
        .entity
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("hydration node has no entity".into()))?;
    let alias = &node.id;
    let pk = &node.id_property;

    let columns: Vec<&str> = match &node.columns {
        Some(ColumnSelection::List(cols)) => cols.iter().map(|s| s.as_str()).collect(),
        _ => vec![],
    };

    let json_expr = if columns.is_empty() {
        Expr::string("{}")
    } else {
        let map_args: Vec<Expr> = columns
            .iter()
            .flat_map(|&col| {
                [
                    Expr::string(col),
                    Expr::func("toString", vec![Expr::col(alias, col)]),
                ]
            })
            .collect();
        Expr::func("toJSONString", vec![Expr::func("map", map_args)])
    };

    // Inner dedup scan: ID filter + LIMIT 1 BY for ReplacingMergeTree dedup.
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

    let dedup_select = vec![
        SelectExpr::new(Expr::col(alias, pk), pk),
        SelectExpr::new(Expr::col(alias, DELETED_COLUMN), DELETED_COLUMN),
        SelectExpr::star(),
    ];

    let dedup_scan = Query {
        select: dedup_select,
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(scan_where),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, pk)])),
        ..Default::default()
    };

    // Outer: project columns + _deleted=false after dedup.
    let select = vec![
        SelectExpr::new(Expr::col(alias, pk), format!("{alias}_{pk}")),
        SelectExpr::new(Expr::string(entity), format!("{alias}_entity_type")),
        SelectExpr::new(json_expr, format!("{alias}_props")),
    ];

    Ok(Query {
        select,
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
