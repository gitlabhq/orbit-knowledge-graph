//! Rewrites `<node>.<col>` and `e{i}.<col>` in a denormalized group onto the owner scan's prefixed columns.
//! Runs at the end of lowering; no later pass introduces node-alias columns.

use std::collections::HashMap;

use ontology::denormalized::column_for;

use crate::ast::*;
use crate::passes::plan::{Hop, denormalized_owner};

/// `(owner alias, anchor table index, chain table index)`.
type Target = (String, usize, usize);

struct Bindings<'a> {
    /// Node alias to the table carrying its columns.
    nodes: HashMap<&'a str, Target>,
    /// Hop index to the table carrying its edge columns.
    edges: HashMap<usize, Target>,
}

impl Bindings<'_> {
    /// Already-prefixed columns (node filters inside cascade anchors) keep their alias untouched.
    fn resolve(&self, table: &str, column: &str) -> Option<(String, String)> {
        if already_prefixed(column) {
            return None;
        }
        if let Some((owner, anchor, idx)) = self.nodes.get(table) {
            return Some((owner.clone(), column_for(*anchor, *idx, column)));
        }
        let rest = table.strip_prefix('e')?;
        let digits = rest.trim_end_matches(|c: char| !c.is_ascii_digit());
        let suffix = &rest[digits.len()..];
        let (owner, anchor, idx) = self.edges.get(&digits.parse::<usize>().ok()?)?;
        let alias = if suffix.is_empty() {
            owner.clone()
        } else {
            table.to_string()
        };
        Some((alias, column_for(*anchor, *idx, column)))
    }
}

fn already_prefixed(column: &str) -> bool {
    column
        .strip_prefix('t')
        .and_then(|rest| rest.split_once('_'))
        .is_some_and(|(digits, _)| !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()))
}

pub(super) fn rebind_node_aliases(node: &mut Node, hops: &[Hop]) {
    let mut b = Bindings {
        nodes: HashMap::new(),
        edges: HashMap::new(),
    };
    for (i, hop) in hops.iter().enumerate() {
        let Some(d) = &hop.denormalized else {
            continue;
        };
        let owner = format!(
            "e{}",
            denormalized_owner(hops, i).expect("hop is denormalized")
        );
        b.nodes.insert(
            hop.from_node.as_str(),
            (owner.clone(), d.anchor_table, d.from_table),
        );
        b.nodes.insert(
            hop.to_node.as_str(),
            (owner.clone(), d.anchor_table, d.to_table),
        );
        b.edges.insert(i, (owner, d.anchor_table, d.edge_table));
    }
    if b.edges.is_empty() {
        return;
    }
    if let Node::Query(q) = node {
        rebind_query(q, &b);
    }
}

fn rebind_query(q: &mut Query, b: &Bindings) {
    for cte in &mut q.ctes {
        rebind_query(&mut cte.query, b);
    }
    for s in &mut q.select {
        rebind_expr(&mut s.expr, b);
    }
    rebind_table_ref(&mut q.from, b);
    if let Some(w) = &mut q.where_clause {
        rebind_expr(w, b);
    }
    for g in &mut q.group_by {
        rebind_expr(g, b);
    }
    if let Some(h) = &mut q.having {
        rebind_expr(h, b);
    }
    for o in &mut q.order_by {
        rebind_expr(&mut o.expr, b);
    }
    if let Some((_, cols)) = &mut q.limit_by {
        for c in cols {
            rebind_expr(c, b);
        }
    }
    for arm in &mut q.union_all {
        rebind_query(arm, b);
    }
}

fn rebind_table_ref(t: &mut TableRef, b: &Bindings) {
    match t {
        TableRef::Scan { .. } => {}
        TableRef::Join {
            left, right, on, ..
        } => {
            rebind_table_ref(left, b);
            rebind_table_ref(right, b);
            rebind_expr(on, b);
        }
        TableRef::Union { queries, .. } => {
            for q in queries {
                rebind_query(q, b);
            }
        }
        TableRef::Subquery { query, .. } => rebind_query(query, b),
    }
}

fn rebind_expr(e: &mut Expr, b: &Bindings) {
    match e {
        Expr::Column { table, column } => {
            if let Some((alias, col)) = b.resolve(table, column) {
                *table = alias;
                *column = col;
            }
        }
        Expr::Identifier(_) | Expr::Literal(_) | Expr::Param { .. } | Expr::Star => {}
        Expr::FuncCall { args, .. } => {
            for a in args {
                rebind_expr(a, b);
            }
        }
        Expr::Lambda { body, .. } => rebind_expr(body, b),
        Expr::BinaryOp { left, right, .. } => {
            rebind_expr(left, b);
            rebind_expr(right, b);
        }
        Expr::UnaryOp { expr, .. } => rebind_expr(expr, b),
        Expr::InSubquery { expr, .. } => rebind_expr(expr, b),
        Expr::InSelect { expr, query } => {
            rebind_expr(expr, b);
            rebind_query(query, b);
        }
    }
}
