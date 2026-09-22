//! Scope anchor elision. An aggregation anchored on a namespace,
//! `(g:Group {full_path: 'x'})-[:CONTAINS]->(p)-...`, already has every scan
//! restricted to that namespace by the scope prefix `restrict` derived from
//! `g`. When the CONTAINS hop is the only edge scan, the anchor table and
//! its edge are redundant. A guard keeps a missing anchor yielding no rows.

use super::prelude::*;

/// An aggregation anchored on a namespace (`(g:Group {full_path})-[:CONTAINS]->(p)`)
/// whose only edge scan is that containment hop: the scope prefix `restrict`
/// derived from the anchor already pins every other scan to the namespace,
/// so the anchor table and its edge are redundant. A guard keeps a missing
/// anchor yielding no rows instead of the broad fallback.
pub fn rule_scope_anchor_elision(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    if ctx.input.query_type != QueryType::Aggregation {
        return None;
    }
    let read = tree.read_aliases();
    let spine = spine_of(tree)?;
    let non_fk: Vec<_> = spine
        .leaves
        .iter()
        .filter_map(|(l, _)| ctx.edge_rel(l))
        .filter(|(_, _, r)| r.fk_column.is_none())
        .collect();
    let [(edge_alias, _, rel)] = non_fk[..] else {
        return None;
    };
    let prefix = rel.scope_prefix.as_ref()?;
    if !ctx.graph.scope_preserving(&rel.types) || !rel.filters.is_empty() {
        return None;
    }
    let degree = |a: &str| {
        ctx.input
            .relationships
            .iter()
            .filter(|r| r.from == a || r.to == a)
            .count()
    };
    let anchor = [&rel.from, &rel.to].into_iter().find(|a| {
        ctx.node(a).is_some_and(|n| {
            n.has_traversal_path
                && degree(a) == 1
                && crate::scope::is_scope_only(n)
                && !read.contains(a.as_str())
        })
    })?;
    let edge_alias = edge_alias.to_string();
    let guard = PExpr::ScopeResolved(prefix.clone());

    rewrite_spine(tree, &|root| {
        let mut sp = flatten(root);
        // Re-bind the edge's column for the surviving endpoint onto whichever
        // other column shared it, then drop the edge and anchor.
        let mut s: HashMap<Col, PExpr> = HashMap::new();
        for (x, y) in &sp.eqs {
            let (edge_side, other) = if x.0 == edge_alias {
                (x, y)
            } else if y.0 == edge_alias {
                (y, x)
            } else {
                continue;
            };
            if other.0 != *anchor {
                s.entry(edge_side.clone())
                    .or_insert_with(|| PExpr::Col(other.0.clone(), other.1.clone()));
            }
        }
        sp.subst(&s);
        sp.remove(&edge_alias);
        sp.remove(anchor);
        if sp.leaves.is_empty() {
            return None;
        }
        Some(rebuild(sp).filter(vec![guard.clone()]))
    })
}
