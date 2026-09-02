//! Rebind node aliases onto a denormalized join table scan.
//!
//! Every earlier pass (lowering, enforcement, cursor seek) emits node columns
//! as `<node>.<column>`, which is what a real node-table scan would expose. A
//! [`Strategy::Denormalized`] plan has no such scan: both endpoints live in
//! one row under `src_`/`tgt_` prefixes. This pass runs after the last
//! alias-producing pass and rewrites `<node>.<column>` to
//! `<scan>.<prefix><column>`. Columns the join table owns outright
//! (`traversal_path`, `_deleted`, ...) are rebound without a prefix.

use std::collections::HashMap;

use ontology::denormalized::{DenormalizedJoinTable, Side};

use crate::ast::*;
use crate::error::Result;
use crate::passes::plan::{DENORMALIZED_ALIAS, Plan, Strategy};

pub fn apply(node: &mut Node, plan: &Plan) -> Result<()> {
    let Strategy::Denormalized(mat) = &plan.strategy else {
        return Ok(());
    };
    let sides = HashMap::from([
        (mat.source_node.as_str(), Side::Source),
        (mat.target_node.as_str(), Side::Target),
    ]);
    if let Node::Query(q) = node {
        rebind_query(q, &sides);
    }
    Ok(())
}

fn rebind_query(q: &mut Query, sides: &HashMap<&str, Side>) {
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

fn rebind_table_ref(t: &mut TableRef, sides: &HashMap<&str, Side>) {
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

fn rebind_expr(e: &mut Expr, sides: &HashMap<&str, Side>) {
    match e {
        Expr::Column { table, column } => {
            if let Some(side) = sides.get(table.as_str()) {
                if !DenormalizedJoinTable::is_passthrough_column(column) {
                    *column = format!("{}{column}", side.prefix());
                }
                *table = DENORMALIZED_ALIAS.to_string();
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
