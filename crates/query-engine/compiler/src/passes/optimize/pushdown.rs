//! Local rules on `Filter(Scan(edge))`: merge stacked filters, and evaluate
//! node filters on the edge scan when the edge carries the value (as a
//! denormalized tag, or as a real column).

use super::prelude::*;

// ── Local rules ─────────────────────────────────────────────────────────────

pub fn merge_filters(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    let PhysOp::Filter {
        predicates: outer,
        input,
    } = op
    else {
        return None;
    };
    let PhysOp::Filter {
        predicates: inner,
        input: leaf,
    } = input.as_ref()
    else {
        return None;
    };
    let mut merged = inner.clone();
    merged.extend(outer.iter().cloned());
    Some((**leaf).clone().filter(merged))
}

/// A node filter on a denormalized property also lives as a tag on the edges
/// of the relationships that write it. Push it onto the edge scan so the edge
/// prunes before the node join.
pub fn denorm_tags(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let (predicates, input, alias) = edge_filter(op, ctx)?;
    let rel = ctx.input.relationships.get(rel_index(alias)?)?;
    let pc = ctx.plan_ctx();
    let (sc, ec) = rel.direction.edge_columns();
    let mut pushed = Vec::new();
    for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
        let Some(node) = ctx.node(nid) else { continue };
        let dir = if ic == SOURCE_ID_COLUMN {
            "source"
        } else {
            "target"
        };
        pushed.extend(pc.denorm_tags(node, dir, alias, Some(&rel.types)));
    }
    push_predicates(predicates, input, pushed)
}

/// Edge tables that carry node columns (e.g. `project_id`, `branch`) can
/// evaluate node filters on those columns directly.
pub fn edge_columns(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let (predicates, input, alias) = edge_filter(op, ctx)?;
    let PhysOp::Scan { table, .. } = input else {
        return None;
    };
    let rel = ctx.input.relationships.get(rel_index(alias)?)?;
    let ecols = ctx.input.compiler.table_columns.get(table)?;
    let mut pushed = Vec::new();
    for nid in [&rel.from, &rel.to] {
        let Some(node) = ctx.node(nid) else { continue };
        let carried: HashMap<_, _> = node
            .filters
            .iter()
            .filter(|(p, _)| ecols.contains(*p) && !EDGE_RESERVED_COLUMNS.contains(&p.as_str()))
            .map(|(p, fs)| (p.clone(), fs.clone()))
            .collect();
        pushed.extend(node_filters(alias, &carried));
    }
    push_predicates(predicates, input, pushed)
}

/// `Filter` over an edge scan, or over a multi-hop union aliased as an edge.
fn edge_filter<'o>(op: &'o PhysOp, ctx: &RuleCtx) -> Option<(&'o [PExpr], &'o PhysOp, &'o str)> {
    let PhysOp::Filter { predicates, input } = op else {
        return None;
    };
    match input.as_ref() {
        PhysOp::Scan { table, alias, .. } if ctx.is_edge_table(table) => {
            Some((predicates, input, alias))
        }
        PhysOp::Union { alias, .. } if rel_index(alias).is_some() => {
            Some((predicates, input, alias))
        }
        _ => None,
    }
}

fn push_predicates(existing: &[PExpr], input: &PhysOp, mut pushed: Vec<PExpr>) -> Option<PhysOp> {
    pushed.retain(|p| !existing.contains(p));
    if pushed.is_empty() {
        return None;
    }
    let mut merged = existing.to_vec();
    merged.extend(pushed);
    Some(input.clone().filter(merged))
}
