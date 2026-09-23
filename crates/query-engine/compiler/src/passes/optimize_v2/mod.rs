//! Rewrites a traversal or aggregation plan until nothing changes.
//!
//! Local rules run bottom-up on each node; global rules see the whole tree
//! and work on the flattened join spine (`spine.rs`). Each iteration applies
//! every rule once; the loop stops at a fixpoint.
//!
//! | rule                            | file          | effect |
//! |---------------------------------|---------------|--------|
//! | merge filters, push node filters onto edges | `pushdown.rs` | fewer, earlier predicates |
//! | scope anchor elision            | `scope.rs`    | drop the namespace anchor hop |
//! | FK elision                      | `fk.rs`       | node joins instead of edge scans |
//! | edge dedup, LIMIT BY            | `dedup.rs`    | no stale edge versions |
//! | unreferenced nodes              | `prune.rs`    | semi-join or drop node tables nothing reads |
//! | cascade SIP                     | `sip.rs`      | hop N filtered by hop N-1's ids |

mod dedup;
mod fk;
mod prune;
mod pushdown;
mod scope;
mod sip;
mod spine;

pub(crate) mod prelude {
    pub use super::spine::*;
    pub use super::{RuleCtx, fk_eligible, rel_index};
    pub use crate::input::*;
    pub use crate::passes::plan_v2::*;
    pub use crate::{pe, pn};
    pub use ontology::constants::*;
    pub use std::collections::{HashMap, HashSet};
}
use prelude::*;

pub fn optimize_plan(
    input: &mut Input,
    ontology: &ontology::Ontology,
    mut plan: PlanMetadata,
) -> PlanMetadata {
    let op = plan.phys_op.take().expect("plan phase sets phys_op");
    if !matches!(input.query_type, QueryType::Traversal | QueryType::Aggregation) {
        plan.has_semi_joins = has_semi_join(&op);
        plan.phys_op = Some(op);
        return plan;
    }
    let graph = JoinGraph::build(ontology);
    let ctx = RuleCtx {
        input,
        graph: &graph,
    };
    let mut op = if input.query_type == QueryType::Traversal {
        ctx.plan_ctx().defer_hydration_columns(op)
    } else {
        op
    };
    op = optimize(op, &ctx);
    if input.query_type == QueryType::Traversal {
        op = ctx.plan_ctx().project_joined_columns(op);
    }
    plan.node_edge_mappings = ctx.plan_ctx().node_edge_mappings(&op);
    plan.has_semi_joins = has_semi_join(&op);
    if input.query_type == QueryType::Aggregation {
        let kept: HashSet<&String> = plan.node_edge_mappings.keys().collect();
        input.nodes.retain(|n| kept.contains(&n.id));
        input
            .relationships
            .retain(|r| kept.contains(&r.from) && kept.contains(&r.to));
    }
    plan.phys_op = Some(op);
    plan
}

fn has_semi_join(op: &PhysOp) -> bool {
    matches!(
        op,
        PhysOp::Join {
            kind: JoinKind::Semi,
            ..
        }
    ) || op.children().into_iter().any(has_semi_join)
}

type Rule = fn(&PhysOp, &RuleCtx) -> Option<PhysOp>;

const LOCAL_RULES: &[Rule] = &[
    pushdown::merge_filters,
    pushdown::denorm_tags,
    pushdown::edge_columns,
];

const GLOBAL_RULES: &[Rule] = &[
    scope::rule_scope_anchor_elision,
    fk::rule_fk_elision,
    dedup::rule_edge_dedup,
    dedup::rule_single_hop_agg_limit_by,
    prune::rule_unreferenced_nodes,
    sip::rule_cascade_sip,
];

pub fn optimize(mut tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    loop {
        let prev = tree.clone();
        tree = apply_local(tree, ctx);
        for rule in GLOBAL_RULES {
            if let Some(t) = rule(&tree, ctx) {
                tree = t;
            }
        }
        if tree == prev {
            return tree;
        }
    }
}

fn apply_local(tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    let tree = tree.map_children(&mut |c| apply_local(c, ctx));
    LOCAL_RULES
        .iter()
        .find_map(|rule| rule(&tree, ctx))
        .unwrap_or(tree)
}

// ── Query facts the rules share ───────────────────────────────────────────────

pub struct RuleCtx<'a> {
    pub input: &'a Input,
    pub graph: &'a JoinGraph,
}

// ── Leaves ──────────────────────────────────────────────────────────────────

impl<'a> RuleCtx<'a> {
    pub fn node(&self, alias: &str) -> Option<&'a InputNode> {
        self.input.nodes.iter().find(|n| n.id == alias)
    }

    pub(crate) fn plan_ctx(&self) -> PlanCtx<'a> {
        PlanCtx {
            input: self.input,
            graph: self.graph,
        }
    }

    pub fn is_edge_table(&self, table: &str) -> bool {
        self.input.compiler.edge_tables.contains(table)
    }

    /// The relationship an edge scan leaf (`Filter(Scan(edge e_i))`) stands for.
    pub fn edge_rel<'o>(
        &self,
        leaf: &'o PhysOp,
    ) -> Option<(&'o str, usize, &'a InputRelationship)> {
        let PhysOp::Scan { table, alias, .. } = leaf_scan(leaf)? else {
            return None;
        };
        if !self.is_edge_table(table) {
            return None;
        }
        let i = rel_index(alias)?;
        Some((alias, i, self.input.relationships.get(i)?))
    }

    pub fn node_leaf<'o>(&self, leaf: &'o PhysOp) -> Option<&'a InputNode> {
        let PhysOp::Scan { table, alias, .. } = leaf_scan(leaf)? else {
            return None;
        };
        (!self.is_edge_table(table)).then(|| self.input.nodes.iter().find(|n| &n.id == alias))?
    }

    /// Which side of `rel` holds the FK column.
    pub fn fk_sides<'r>(&self, rel: &'r InputRelationship, fk_col: &str) -> (&'r str, &'r str) {
        let from_has = self
            .node(&rel.from)
            .and_then(|n| n.table.as_deref())
            .and_then(|t| self.input.compiler.table_columns.get(t))
            .is_some_and(|cols| cols.contains(fk_col));
        if from_has {
            (&rel.from, &rel.to)
        } else {
            (&rel.to, &rel.from)
        }
    }
}

fn leaf_scan(op: &PhysOp) -> Option<&PhysOp> {
    match op {
        PhysOp::Scan { .. } => Some(op),
        PhysOp::Filter { input, .. } => leaf_scan(input),
        _ => None,
    }
}

pub fn rel_index(alias: &str) -> Option<usize> {
    alias.strip_prefix('e')?.parse().ok()
}

/// A single-hop relationship the FK column can answer without an edge scan.
pub fn fk_eligible(rel: &InputRelationship) -> Option<&str> {
    let fk = rel.fk_column.as_deref()?;
    (rel.hops.max == 1 && !matches!(rel.direction, Direction::Both) && rel.filters.is_empty())
        .then_some(fk)
}
