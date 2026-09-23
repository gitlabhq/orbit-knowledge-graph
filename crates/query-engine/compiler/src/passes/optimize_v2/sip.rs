//! Sideways information passing along the chain. When hop N-1 is pinned by
//! ids or filters, hop N only needs rows whose start id is among hop N-1's
//! end ids:
//!
//! ```sql
//! ... AND e1.source_id IN (SELECT target_id FROM gl_edge _sip_e0 WHERE <e0's predicates>)
//! ```

use super::prelude::*;

/// Sideways information passing along the chain: when hop N-1 is pinned by
/// ids or filters, hop N only needs rows whose start id appears among hop
/// N-1's end ids. Adds `e_N.start IN (SELECT e_{N-1}.end FROM ...)`.
pub fn rule_cascade_sip(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    rewrite_spine(tree, &|root| {
        let mut sp = flatten(root);
        let mut added = Vec::new();
        for (x, y) in &sp.eqs {
            let (Some(ix), Some(iy)) = (rel_index(&x.0), rel_index(&y.0)) else {
                continue;
            };
            let (prev, curr) = if ix < iy { (x, y) } else { (y, x) };
            let Some(leaf) = sp.leaf(&prev.0) else {
                continue;
            };
            let Some((_, i, rel)) = ctx.edge_rel(leaf) else {
                continue;
            };
            if sp.leaf(&curr.0).is_none() || fk_eligible(rel).is_some() || !is_selective(leaf) {
                continue;
            }
            let sip = format!("_sip_e{i}");
            if sp.leaf(&sip).is_some() {
                continue;
            }
            let body = realias(leaf.clone(), &prev.0, &sip).project(vec![pn!(
                "{sip}.{} AS {}",
                prev.1,
                prev.1
            )]);
            added.push((body, (curr.clone(), (sip, prev.1.clone()))));
        }
        if added.is_empty() {
            return None;
        }
        for (body, eq) in added {
            sp.leaves.push((body, JoinKind::Semi));
            sp.eqs.push(eq);
        }
        Some(rebuild(sp))
    })
}

/// An edge leaf pinned by ids, an id range, or a pushed node filter.
fn is_selective(leaf: &PhysOp) -> bool {
    let PhysOp::Filter { predicates, .. } = leaf else {
        return false;
    };
    predicates.iter().any(|p| match p {
        PExpr::NodeFilter { .. } => true,
        PExpr::Func(name, _) => name == "has" || name == "hasAny",
        _ => matches!(p.constrained_col(), Some((_, c)) if c == SOURCE_ID_COLUMN || c == TARGET_ID_COLUMN),
    })
}

fn realias(op: PhysOp, from: &str, to: &str) -> PhysOp {
    let op = match op {
        PhysOp::Scan {
            table,
            alias,
            dedup,
        } if alias == from => PhysOp::Scan {
            table,
            alias: to.to_string(),
            dedup,
        },
        other => other.map_exprs(&|e| e.realias(from, to)),
    };
    op.map_children(&mut |c| realias(c, from, to))
}
