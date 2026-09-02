//! Rebind node and edge aliases onto a denormalized join scan.
//!
//! Lowering emits `<node>.<column>` and `e{i}.<column>` as if every node and
//! edge had its own scan. Hops in a denormalized group have one scan instead,
//! owned by the group's first hop, whose columns carry a chain-table prefix
//! (`denormalized::column_for`). After the query body is built, every
//! reference into the group is rewritten onto that scan. Derived aliases such
//! as a cascade anchor's `e{i}p`, which scan the join table under their own
//! name, keep their alias and only get the column prefix. Passes after
//! lowering never introduce node-alias columns: `enforce` reads the plan's
//! node-to-edge mappings, `cursor` derives its seek from the lowered ORDER BY,
//! and the rest work on scan aliases.

use std::collections::HashMap;

use ontology::denormalized::column_for;

use crate::ast::*;
use crate::passes::plan::{Hop, denormalized_owner};

struct Bindings<'a> {
    /// Node alias to `(owner alias, chain table index)`.
    nodes: HashMap<&'a str, (String, usize)>,
    /// Hop index to `(owner alias, chain table index of its edge)`.
    edges: HashMap<usize, (String, usize)>,
}

impl Bindings<'_> {
    /// Rebinding for one column reference, or `None` to leave it alone. A
    /// column already carrying a chain prefix was resolved by the lowerer
    /// (node filters inside cascade anchors) and only needs its alias kept.
    fn resolve(&self, table: &str, column: &str) -> Option<(String, String)> {
        if already_prefixed(column) {
            return None;
        }
        if let Some((owner, idx)) = self.nodes.get(table) {
            return Some((owner.clone(), column_for(*idx, column)));
        }
        let rest = table.strip_prefix('e')?;
        let digits = rest.trim_end_matches(|c: char| !c.is_ascii_digit());
        let suffix = &rest[digits.len()..];
        let (owner, idx) = self.edges.get(&digits.parse::<usize>().ok()?)?;
        let alias = if suffix.is_empty() {
            owner.clone()
        } else {
            table.to_string()
        };
        Some((alias, column_for(*idx, column)))
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
        b.nodes
            .insert(hop.from_node.as_str(), (owner.clone(), d.from_table));
        b.nodes
            .insert(hop.to_node.as_str(), (owner.clone(), d.to_table));
        b.edges.insert(i, (owner, d.edge_table));
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
