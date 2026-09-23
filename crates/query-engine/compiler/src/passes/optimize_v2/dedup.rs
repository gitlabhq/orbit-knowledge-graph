//! Edge tables are ReplacingMergeTree: a row may have stale versions until
//! merged. One edge scan in an aggregation dedups with `LIMIT 1 BY` (keeps
//! column pruning and projections usable); two or more edge scans dedup
//! with `FINAL` so the self-join doesn't multiply rows.

use super::prelude::*;

/// Two or more edge scans in one spine self-join the edge table; read them
/// with `FINAL` so stale versions don't multiply rows.
pub fn rule_edge_dedup(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let edges = spine_edges(&spine_of(tree)?, ctx);
    if edges.len() < 2 {
        return None;
    }
    let updated = set_dedup(tree.clone(), &edges, Dedup::Final);
    (updated != *tree).then_some(updated)
}

/// An aggregation over one edge scan must not count stale edge versions.
/// `LIMIT 1 BY <sort key>` dedups while keeping the scan eligible for column
/// pruning and projections.
pub fn rule_single_hop_agg_limit_by(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    if ctx.input.query_type != QueryType::Aggregation {
        return None;
    }
    let edges = spine_edges(&spine_of(tree)?, ctx);
    if edges.len() != 1 {
        return None;
    }
    let updated = set_dedup(tree.clone(), &edges, Dedup::LimitBy);
    (updated != *tree).then_some(updated)
}

fn set_dedup(op: PhysOp, targets: &[String], dedup: Dedup) -> PhysOp {
    match op {
        PhysOp::Scan {
            table,
            alias,
            dedup: Dedup::None,
        } if targets.contains(&alias) => PhysOp::Scan {
            table,
            alias,
            dedup,
        },
        PhysOp::Union { .. } => op,
        other => other.map_children(&mut |c| set_dedup(c, targets, dedup)),
    }
}

fn spine_edges(spine: &Spine, ctx: &RuleCtx) -> Vec<String> {
    spine
        .leaves
        .iter()
        .filter_map(|(l, _)| ctx.edge_rel(l).map(|(a, _, _)| a.to_string()))
        .collect()
}
