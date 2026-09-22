use super::plan_v2::*;
use crate::input::*;

pub struct RuleCtx<'a> {
    pub input: &'a Input,
    pub graph: &'a JoinGraph,
}

pub fn optimize(tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    let mut tree = tree;
    loop {
        let prev = tree.clone();
        tree = apply_rules(tree, ctx);
        if tree == prev {
            break;
        }
    }
    tree
}

fn apply_rules(tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    let tree = map_children(tree, |child| apply_rules(child, ctx));
    let rules: &[fn(&PhysOp, &RuleCtx) -> Option<PhysOp>] =
        &[rule_elide_empty_sort, rule_merge_filters, rule_edge_dedup];
    for rule in rules {
        if let Some(rewritten) = rule(&tree, ctx) {
            return rewritten;
        }
    }
    tree
}

fn map_children(op: PhysOp, f: impl Fn(PhysOp) -> PhysOp) -> PhysOp {
    match op {
        PhysOp::Scan { .. } => op,
        PhysOp::Filter { input, predicates } => PhysOp::Filter {
            input: Box::new(f(*input)),
            predicates,
        },
        PhysOp::Project { input, columns } => PhysOp::Project {
            input: Box::new(f(*input)),
            columns,
        },
        PhysOp::Join {
            left,
            right,
            on,
            kind,
        } => PhysOp::Join {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            on,
            kind,
        },
        PhysOp::Aggregate {
            input,
            group_by,
            metrics,
        } => PhysOp::Aggregate {
            input: Box::new(f(*input)),
            group_by,
            metrics,
        },
        PhysOp::Union { arms } => PhysOp::Union {
            arms: arms.into_iter().map(&f).collect(),
        },
        PhysOp::Sort { input, keys } => PhysOp::Sort {
            input: Box::new(f(*input)),
            keys,
        },
        PhysOp::Limit { input, count } => PhysOp::Limit {
            input: Box::new(f(*input)),
            count,
        },
    }
}

// ── Rule 1: Elide empty Sort ────────────────────────────────────────────────

fn rule_elide_empty_sort(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    match op {
        PhysOp::Sort { keys, input } if keys.is_empty() => Some(*input.clone()),
        _ => None,
    }
}

// ── Rule 2: Merge adjacent Filters ──────────────────────────────────────────

fn rule_merge_filters(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    match op {
        PhysOp::Filter {
            predicates: p1,
            input,
        } => {
            if let PhysOp::Filter {
                predicates: p2,
                input: inner,
            } = input.as_ref()
            {
                let mut merged = p1.clone();
                merged.extend(p2.clone());
                Some(PhysOp::Filter {
                    predicates: merged,
                    input: inner.clone(),
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

// ── Rule 3: Edge dedup for multi-edge chains ────────────────────────────────

fn rule_edge_dedup(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
    let edge_count = count_edge_scans(op);
    if edge_count < 2 {
        return None;
    }
    let updated = set_edge_dedup(op.clone(), true);
    if updated == *op { None } else { Some(updated) }
}

fn count_edge_scans(op: &PhysOp) -> usize {
    match op {
        PhysOp::Scan {
            table,
            dedup: false,
            ..
        } if table.starts_with("gl_")
            && table != "gl_user"
            && !table.ends_with("_request")
            && !table.ends_with("_item") =>
        {
            // Heuristic: edge tables are gl_edge, gl_code_edge, etc.
            // Node tables are gl_merge_request, gl_project, gl_user, etc.
            // Better: check if the table is in the edge table config
            if table.contains("edge") { 1 } else { 0 }
        }
        PhysOp::Scan { .. } => 0,
        PhysOp::Filter { input, .. } => count_edge_scans(input),
        PhysOp::Project { input, .. } => count_edge_scans(input),
        PhysOp::Join { left, right, .. } => count_edge_scans(left) + count_edge_scans(right),
        PhysOp::Aggregate { input, .. } => count_edge_scans(input),
        PhysOp::Union { arms } => arms.iter().map(count_edge_scans).sum(),
        PhysOp::Sort { input, .. } => count_edge_scans(input),
        PhysOp::Limit { input, .. } => count_edge_scans(input),
    }
}

fn set_edge_dedup(op: PhysOp, dedup_val: bool) -> PhysOp {
    match op {
        PhysOp::Scan {
            table,
            alias,
            dedup,
        } if table.contains("edge") && !dedup => PhysOp::Scan {
            table,
            alias,
            dedup: dedup_val,
        },
        other => map_children(other, |child| set_edge_dedup(child, dedup_val)),
    }
}
