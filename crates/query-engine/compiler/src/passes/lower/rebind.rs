//! Rebind node aliases onto a denormalized join table scan.
//!
//! Lowering emits node columns as `<node>.<column>`, which is what a node-table
//! scan would expose. A denormalized hop has no such scan: both endpoints live
//! in the hop's edge row, so after the query body is built every
//! `<node>.<column>` becomes `<edge alias>.<side column>` (see
//! [`Side::column_for`]). Passes after lowering never introduce node-alias
//! columns: `enforce` reads the plan's node-to-edge mappings, `cursor` derives
//! its seek from the lowered ORDER BY, and the rest work on scan aliases.

use std::collections::HashMap;

use ontology::denormalized::Side;

use crate::ast::*;
use crate::passes::plan::Hop;

/// Node alias to the edge alias and side that now carry its columns.
type Bindings<'a> = HashMap<&'a str, (String, Side)>;

pub(super) fn rebind_node_aliases(node: &mut Node, hops: &[Hop]) {
    let mut sides = Bindings::new();
    for (i, hop) in hops.iter().enumerate() {
        if let Some(denorm) = &hop.denormalized {
            let alias = format!("e{i}");
            sides.insert(denorm.source_node.as_str(), (alias.clone(), Side::Source));
            sides.insert(denorm.target_node.as_str(), (alias, Side::Target));
        }
    }
    if sides.is_empty() {
        return;
    }
    if let Node::Query(q) = node {
        rebind_query(q, &sides);
    }
}

fn rebind_query(q: &mut Query, sides: &Bindings) {
    for cte in &mut q.ctes {
        rebind_query(&mut cte.query, sides);
    }
    for s in &mut q.select {
        rebind_expr(&mut s.expr, sides);
    }
    rebind_table_ref(&mut q.from, sides);
    if let Some(w) = &mut q.where_clause {
        rebind_expr(w, sides);
    }
    for g in &mut q.group_by {
        rebind_expr(g, sides);
    }
    if let Some(h) = &mut q.having {
        rebind_expr(h, sides);
    }
    for o in &mut q.order_by {
        rebind_expr(&mut o.expr, sides);
    }
    if let Some((_, cols)) = &mut q.limit_by {
        for c in cols {
            rebind_expr(c, sides);
        }
    }
    for arm in &mut q.union_all {
        rebind_query(arm, sides);
    }
}

fn rebind_table_ref(t: &mut TableRef, sides: &Bindings) {
    match t {
        TableRef::Scan { .. } => {}
        TableRef::Join {
            left, right, on, ..
        } => {
            rebind_table_ref(left, sides);
            rebind_table_ref(right, sides);
            rebind_expr(on, sides);
        }
        TableRef::Union { queries, .. } => {
            for q in queries {
                rebind_query(q, sides);
            }
        }
        TableRef::Subquery { query, .. } => rebind_query(query, sides),
    }
}

fn rebind_expr(e: &mut Expr, sides: &Bindings) {
    match e {
        Expr::Column { table, column } => {
            if let Some((edge_alias, side)) = sides.get(table.as_str()) {
                *column = side.column_for(column);
                *table = edge_alias.clone();
            }
        }
        Expr::Identifier(_) | Expr::Literal(_) | Expr::Param { .. } | Expr::Star => {}
        Expr::FuncCall { args, .. } => {
            for a in args {
                rebind_expr(a, sides);
            }
        }
        Expr::Lambda { body, .. } => rebind_expr(body, sides),
        Expr::BinaryOp { left, right, .. } => {
            rebind_expr(left, sides);
            rebind_expr(right, sides);
        }
        Expr::UnaryOp { expr, .. } => rebind_expr(expr, sides),
        Expr::InSubquery { expr, .. } => rebind_expr(expr, sides),
        Expr::InSelect { expr, query } => {
            rebind_expr(expr, sides);
            rebind_query(query, sides);
        }
    }
}
