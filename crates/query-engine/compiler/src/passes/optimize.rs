//! Fixpoint rewriter for traversal and aggregation plans. Local rules run
//! bottom-up on each node; global rules see the whole tree and work on the
//! flattened join spine. Rules fire until nothing changes.

use super::plan_v2::*;
use crate::input::*;
use crate::{pe, pn};
use ontology::constants::*;
use std::collections::{HashMap, HashSet};

pub struct RuleCtx<'a> {
    pub input: &'a Input,
    pub graph: &'a JoinGraph,
}

type Rule = fn(&PhysOp, &RuleCtx) -> Option<PhysOp>;

const LOCAL_RULES: &[Rule] = &[
    rule_merge_filters,
    rule_denorm_tag_pushdown,
    rule_column_pushdown,
];

const GLOBAL_RULES: &[Rule] = &[
    rule_scope_anchor_elision,
    rule_fk_elision,
    rule_edge_dedup,
    rule_single_hop_agg_limit_by,
    rule_unreferenced_nodes,
    rule_cascade_sip,
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

// ── Leaves ──────────────────────────────────────────────────────────────────

impl<'a> RuleCtx<'a> {
    fn is_edge_table(&self, table: &str) -> bool {
        self.input.compiler.edge_tables.contains(table)
    }

    /// The relationship an edge scan leaf (`Filter(Scan(edge e_i))`) stands for.
    fn edge_rel<'o>(&self, leaf: &'o PhysOp) -> Option<(&'o str, usize, &'a InputRelationship)> {
        let PhysOp::Scan { table, alias, .. } = leaf_scan(leaf)? else {
            return None;
        };
        if !self.is_edge_table(table) {
            return None;
        }
        let i = rel_index(alias)?;
        Some((alias, i, self.input.relationships.get(i)?))
    }

    fn node_leaf<'o>(&self, leaf: &'o PhysOp) -> Option<&'a InputNode> {
        let PhysOp::Scan { table, alias, .. } = leaf_scan(leaf)? else {
            return None;
        };
        (!self.is_edge_table(table)).then(|| self.input.nodes.iter().find(|n| &n.id == alias))?
    }

    fn node(&self, alias: &str) -> Option<&'a InputNode> {
        self.input.nodes.iter().find(|n| n.id == alias)
    }

    fn plan_ctx(&self) -> PlanCtx<'a> {
        PlanCtx {
            input: self.input,
            graph: self.graph,
        }
    }

    /// Which side of `rel` holds the FK column.
    fn fk_sides<'r>(&self, rel: &'r InputRelationship, fk_col: &str) -> (&'r str, &'r str) {
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

fn rel_index(alias: &str) -> Option<usize> {
    alias.strip_prefix('e')?.parse().ok()
}

/// A single-hop relationship the FK column can answer without an edge scan.
fn fk_eligible(rel: &InputRelationship) -> Option<&str> {
    let fk = rel.fk_column.as_deref()?;
    (rel.hops.max == 1 && !matches!(rel.direction, Direction::Both) && rel.filters.is_empty())
        .then_some(fk)
}

// ── Join spine ──────────────────────────────────────────────────────────────
//
// The chain planner emits one left-deep tree of inner joins over edge and
// node leaves. Rules that add, drop, or re-wire relations flatten that spine
// to (leaves, equalities), edit the flat form, and rebuild the tree.

struct Spine {
    leaves: Vec<(PhysOp, JoinKind)>,
    eqs: Vec<(Col, Col)>,
}

impl Spine {
    fn leaf(&self, alias: &str) -> Option<&PhysOp> {
        self.leaves
            .iter()
            .map(|(l, _)| l)
            .find(|l| l.alias() == Some(alias))
    }

    fn remove(&mut self, alias: &str) {
        self.leaves.retain(|(l, _)| l.alias() != Some(alias));
        self.eqs.retain(|(x, y)| x.0 != alias && y.0 != alias);
    }

    fn subst(&mut self, s: &HashMap<Col, PExpr>) {
        let sub = |c: &Col| match s.get(c) {
            Some(PExpr::Col(a, b)) => (a.clone(), b.clone()),
            _ => c.clone(),
        };
        self.eqs = self
            .eqs
            .iter()
            .map(|(x, y)| (sub(x), sub(y)))
            .filter(|(x, y)| x != y)
            .collect();
    }
}

/// The spine root: the topmost join, or the single leaf when the chain has
/// no joins at all.
fn is_spine_root(op: &PhysOp) -> bool {
    match op {
        PhysOp::Join { .. } | PhysOp::Scan { .. } | PhysOp::Union { .. } => true,
        PhysOp::Filter { input, .. } => {
            matches!(**input, PhysOp::Scan { .. } | PhysOp::Union { .. })
        }
        _ => false,
    }
}

fn flatten(op: &PhysOp) -> Spine {
    fn go(op: &PhysOp, s: &mut Spine) {
        match op {
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => {
                go(left, s);
                match kind {
                    JoinKind::Inner => go(right, s),
                    JoinKind::Semi => s.leaves.push(((**right).clone(), JoinKind::Semi)),
                }
                for eq in on {
                    let dup = s
                        .eqs
                        .iter()
                        .any(|(a, b)| (a, b) == (&eq.0, &eq.1) || (a, b) == (&eq.1, &eq.0));
                    if !dup {
                        s.eqs.push(eq.clone());
                    }
                }
            }
            leaf => s.leaves.push((leaf.clone(), JoinKind::Inner)),
        }
    }
    let mut s = Spine {
        leaves: vec![],
        eqs: vec![],
    };
    go(op, &mut s);
    s
}

/// Left-deep rebuild. Each inner leaf joins on every equality tying it to an
/// already placed alias; a leaf nothing ties to yet is deferred. Semi leaves
/// go last, each on its single equality.
fn rebuild(spine: Spine) -> PhysOp {
    let Spine { leaves, mut eqs } = spine;
    let (inner, semi): (Vec<_>, Vec<_>) =
        leaves.into_iter().partition(|(_, k)| *k == JoinKind::Inner);
    let mut pending: Vec<PhysOp> = inner.into_iter().map(|(l, _)| l).collect();
    let mut placed: HashSet<String> = HashSet::new();
    let touches = |eq: &(Col, Col), a: &str, placed: &HashSet<String>| {
        (eq.0.0 == a && placed.contains(&eq.1.0)) || (eq.1.0 == a && placed.contains(&eq.0.0))
    };
    let mut tree: Option<PhysOp> = None;
    while !pending.is_empty() {
        let pick = match tree {
            None => 0,
            Some(_) => pending
                .iter()
                .position(|l| {
                    eqs.iter()
                        .any(|eq| touches(eq, l.alias().unwrap_or_default(), &placed))
                })
                .unwrap_or(0),
        };
        let leaf = pending.remove(pick);
        let a = leaf.alias().unwrap_or_default().to_string();
        let (on, rest): (Vec<_>, Vec<_>) = eqs.into_iter().partition(|eq| touches(eq, &a, &placed));
        eqs = rest;
        placed.insert(a);
        tree = Some(match tree {
            None => leaf,
            Some(t) => t.join(leaf, on),
        });
    }
    let mut tree = tree.expect("spine has at least one leaf");
    for (leaf, _) in semi {
        let a = leaf.alias().unwrap_or_default().to_string();
        let pos = eqs
            .iter()
            .position(|(x, y)| x.0 == a || y.0 == a)
            .expect("semi leaf has its equality");
        let (x, y) = eqs.remove(pos);
        // Consumer side first so lowering reads `left IN (SELECT right)`.
        let eq = if y.0 == a { (x, y) } else { (y, x) };
        tree = tree.semi(leaf, eq);
    }
    tree
}

/// Applies `f` to the spine under the plan's wrapper operators. `None` when
/// there is no spine or `f` declines.
fn rewrite_spine(op: &PhysOp, f: &dyn Fn(&PhysOp) -> Option<PhysOp>) -> Option<PhysOp> {
    if is_spine_root(op) {
        return f(op);
    }
    match op {
        PhysOp::Filter { .. }
        | PhysOp::Project { .. }
        | PhysOp::Aggregate { .. }
        | PhysOp::Sort { .. }
        | PhysOp::Limit { .. } => {
            let child = rewrite_spine(op.children()[0], f)?;
            Some(op.clone().map_children(&mut |_| child.clone()))
        }
        _ => None,
    }
}

fn spine_of(op: &PhysOp) -> Option<Spine> {
    if is_spine_root(op) {
        return Some(flatten(op));
    }
    match op {
        PhysOp::Filter { .. }
        | PhysOp::Project { .. }
        | PhysOp::Aggregate { .. }
        | PhysOp::Sort { .. }
        | PhysOp::Limit { .. } => spine_of(op.children()[0]),
        _ => None,
    }
}

/// Rewrites column references everywhere in this scope (not inside union arms).
fn subst_tree(op: PhysOp, s: &HashMap<Col, PExpr>) -> PhysOp {
    if let PhysOp::Union { .. } = op {
        return op;
    }
    let op = match op {
        PhysOp::Join {
            left,
            right,
            on,
            kind,
        } => {
            let sub = |c: &Col| match s.get(c) {
                Some(PExpr::Col(a, b)) => (a.clone(), b.clone()),
                _ => c.clone(),
            };
            PhysOp::Join {
                left,
                right,
                on: on.iter().map(|(x, y)| (sub(x), sub(y))).collect(),
                kind,
            }
        }
        other => other.map_exprs(&|e| e.subst(s)),
    };
    op.map_children(&mut |c| subst_tree(c, s))
}

// ── Local rules ─────────────────────────────────────────────────────────────

fn rule_merge_filters(op: &PhysOp, _ctx: &RuleCtx) -> Option<PhysOp> {
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

/// A node filter on a denormalized property also lives as a tag on the edges
/// of the relationships that write it. Push it onto the edge scan so the edge
/// prunes before the node join.
fn rule_denorm_tag_pushdown(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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
fn rule_column_pushdown(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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

// ── Global rules ────────────────────────────────────────────────────────────

/// A single-hop relationship backed by a foreign key needs no edge scan: the
/// two node tables join directly on the FK column. Fires only when every
/// edge in the spine is FK-backed; a mixed chain keeps its edge scans so
/// remaining edges can still carry denorm tags and dedup uniformly.
fn rule_fk_elision(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let spine = spine_of(tree)?;
    let edges: Vec<_> = spine
        .leaves
        .iter()
        .filter(|(l, _)| l.alias().and_then(rel_index).is_some())
        .map(|(l, _)| {
            ctx.edge_rel(l)
                .and_then(|(a, _, rel)| fk_eligible(rel).map(|fk| (a, rel, fk)))
        })
        .collect();
    if edges.iter().any(Option::is_none) {
        return None;
    }
    let (alias, rel, fk_col) = edges.into_iter().flatten().next()?;
    let alias = alias.to_string();

    let (sc, _) = rel.direction.edge_columns();
    let (src, tgt) = if sc == SOURCE_ID_COLUMN {
        (&rel.from, &rel.to)
    } else {
        (&rel.to, &rel.from)
    };
    let entity = |a: &str| {
        ctx.node(a)
            .and_then(|n| n.entity.clone())
            .unwrap_or_default()
    };
    let s: HashMap<Col, PExpr> = HashMap::from([
        ((alias.clone(), SOURCE_ID_COLUMN.into()), pe!("{src}.id")),
        ((alias.clone(), TARGET_ID_COLUMN.into()), pe!("{tgt}.id")),
        (
            (alias.clone(), SOURCE_KIND_COLUMN.into()),
            PExpr::Lit(Lit::Str(entity(src))),
        ),
        (
            (alias.clone(), TARGET_KIND_COLUMN.into()),
            PExpr::Lit(Lit::Str(entity(tgt))),
        ),
        (
            (alias.clone(), RELATIONSHIP_KIND_COLUMN.into()),
            PExpr::Lit(Lit::Str(rel.types.first().cloned().unwrap_or_default())),
        ),
    ]);
    let (fk_alias, tgt_alias) = ctx.fk_sides(rel, fk_col);

    let rewritten = rewrite_spine(tree, &|root| {
        let mut sp = flatten(root);
        sp.subst(&s);
        sp.remove(&alias);
        sp.eqs.push((
            (fk_alias.into(), fk_col.into()),
            (tgt_alias.into(), DEFAULT_PRIMARY_KEY.into()),
        ));
        for a in [&rel.from, &rel.to] {
            if sp.leaf(a).is_none()
                && let Some(n) = ctx.node(a)
            {
                sp.leaves
                    .push((ctx.plan_ctx().node_scan(n), JoinKind::Inner));
            }
        }
        Some(rebuild(sp))
    })?;
    Some(subst_tree(rewritten, &s))
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

/// Two or more edge scans in one spine self-join the edge table; read them
/// with `FINAL` so stale versions don't multiply rows.
fn rule_edge_dedup(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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
fn rule_single_hop_agg_limit_by(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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

/// A node table nothing reads, joined on its primary key, is a pure filter
/// on the other side's column: it becomes a semi-join (`IN (SELECT id ...)`,
/// which never multiplies rows), or disappears when it has no filters or an
/// edge still in the spine already carries them all as denorm tags. A node
/// joined on its FK column stays inner: several rows may match one key, and
/// that multiplication is the graph semantics.
fn rule_unreferenced_nodes(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let read = tree.read_aliases();
    rewrite_spine(tree, &|root| {
        let mut sp = flatten(root);
        let present: HashSet<String> = sp
            .leaves
            .iter()
            .filter_map(|(l, _)| l.alias().map(str::to_string))
            .collect();
        let mut changed = false;
        let mut i = 0;
        while i < sp.leaves.len() {
            let (leaf, kind) = &sp.leaves[i];
            let Some(node) = (*kind == JoinKind::Inner)
                .then(|| ctx.node_leaf(leaf))
                .flatten()
            else {
                i += 1;
                continue;
            };
            let a = node.id.clone();
            let touching: Vec<_> = sp
                .eqs
                .iter()
                .filter(|(x, y)| x.0 == a || y.0 == a)
                .collect();
            let [(x, y)] = touching[..] else {
                i += 1;
                continue;
            };
            let own_col = if x.0 == a { &x.1 } else { &y.1 };
            if read.contains(&a) || own_col != DEFAULT_PRIMARY_KEY {
                i += 1;
                continue;
            }
            let pinned = !node.node_ids.is_empty() || node.id_range.is_some();
            let covered = !pinned
                && node.filters.keys().all(|prop| {
                    ctx.input.relationships.iter().enumerate().any(|(j, rel)| {
                        present.contains(&format!("e{j}"))
                            && ctx.plan_ctx().denorm_covers(node, prop, rel)
                    })
                });
            changed = true;
            if covered {
                sp.remove(&a);
            } else {
                sp.leaves[i].1 = JoinKind::Semi;
                i += 1;
            }
        }
        changed.then(|| rebuild(sp))
    })
}

/// Sideways information passing along the chain: when hop N-1 is pinned by
/// ids or filters, hop N only needs rows whose start id appears among hop
/// N-1's end ids. Adds `e_N.start IN (SELECT e_{N-1}.end FROM ...)`.
fn rule_cascade_sip(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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

/// An aggregation anchored on a namespace (`(g:Group {full_path})-[:CONTAINS]->(p)`)
/// whose only edge scan is that containment hop: the scope prefix `restrict`
/// derived from the anchor already pins every other scan to the namespace,
/// so the anchor table and its edge are redundant. A guard keeps a missing
/// anchor yielding no rows instead of the broad fallback.
fn rule_scope_anchor_elision(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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
