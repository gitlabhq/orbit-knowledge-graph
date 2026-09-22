//! Fixpoint rewriter over the naive `PhysOp` tree for traversal and
//! aggregation queries. Local rules run bottom-up on each node; global rules
//! see the whole tree. Rules fire until nothing changes.

use super::plan_v2::*;
use crate::input::*;
use ontology::constants::*;
use std::collections::{HashMap, HashSet};

pub struct RuleCtx<'a> {
    pub input: &'a Input,
    pub graph: &'a JoinGraph,
}

pub fn optimize(tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    let mut tree = tree;
    loop {
        let prev = tree.clone();
        tree = apply_local(tree, ctx);
        for rule in GLOBAL_RULES {
            if let Some(t) = rule(&tree, ctx) {
                tree = t;
            }
        }
        if tree == prev {
            break;
        }
    }
    tree
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

fn apply_local(tree: PhysOp, ctx: &RuleCtx) -> PhysOp {
    let tree = map_children(tree, |c| apply_local(c, ctx));
    for rule in LOCAL_RULES {
        if let Some(t) = rule(&tree, ctx) {
            return t;
        }
    }
    tree
}

pub fn map_children(op: PhysOp, f: impl Fn(PhysOp) -> PhysOp) -> PhysOp {
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
        PhysOp::Union { arms, alias } => PhysOp::Union {
            arms: arms.into_iter().map(f).collect(),
            alias,
        },
        PhysOp::Sort { input, keys } => PhysOp::Sort {
            input: Box::new(f(*input)),
            keys,
        },
        PhysOp::Limit { input, count } => PhysOp::Limit {
            input: Box::new(f(*input)),
            count,
        },
        PhysOp::With { ctes, input } => PhysOp::With {
            ctes: ctes.into_iter().map(|(n, c)| (n, f(c))).collect(),
            input: Box::new(f(*input)),
        },
    }
}

fn children(op: &PhysOp) -> Vec<&PhysOp> {
    match op {
        PhysOp::Scan { .. } => vec![],
        PhysOp::Filter { input, .. }
        | PhysOp::Project { input, .. }
        | PhysOp::Aggregate { input, .. }
        | PhysOp::Sort { input, .. }
        | PhysOp::Limit { input, .. } => vec![input],
        PhysOp::Join { left, right, .. } => vec![left, right],
        PhysOp::Union { arms, .. } => arms.iter().collect(),
        PhysOp::With { ctes, input } => {
            let mut v: Vec<&PhysOp> = ctes.iter().map(|(_, c)| c).collect();
            v.push(input);
            v
        }
    }
}

// ── Tree queries ────────────────────────────────────────────────────────────

/// Alias a relation is visible under: a scan's alias, a union's alias, or
/// that of a filter/project over either.
pub fn leaf_alias(op: &PhysOp) -> Option<&str> {
    match op {
        PhysOp::Scan { alias, .. } | PhysOp::Union { alias, .. } => Some(alias),
        PhysOp::Filter { input, .. } | PhysOp::Project { input, .. } => leaf_alias(input),
        _ => None,
    }
}

/// Aliases visible to the outer query: every scan or union reached without
/// crossing into a semi-join's producer side or a union arm.
pub fn inner_aliases(op: &PhysOp) -> HashSet<String> {
    fn go(op: &PhysOp, out: &mut HashSet<String>) {
        match op {
            PhysOp::Scan { alias, .. } | PhysOp::Union { alias, .. } => {
                out.insert(alias.clone());
            }
            PhysOp::Join {
                left,
                kind: JoinKind::Semi,
                ..
            } => go(left, out),
            _ => children(op).into_iter().for_each(|c| go(c, out)),
        }
    }
    let mut out = HashSet::new();
    go(op, &mut out);
    out
}

pub fn has_alias(op: &PhysOp, target: &str) -> bool {
    match op {
        PhysOp::Scan { alias, .. } | PhysOp::Union { alias, .. } if alias == target => true,
        _ => children(op).into_iter().any(|c| has_alias(c, target)),
    }
}

fn is_edge_table(ctx: &RuleCtx, table: &str) -> bool {
    ctx.input.compiler.edge_tables.contains(table)
}

/// `Filter(Scan(edge))` or `Scan(edge)`: returns the edge alias.
fn edge_scan_alias<'o>(op: &'o PhysOp, ctx: &RuleCtx) -> Option<&'o str> {
    match op {
        PhysOp::Scan { alias, table, .. } if is_edge_table(ctx, table) => Some(alias),
        PhysOp::Filter { input, .. } => edge_scan_alias(input, ctx),
        _ => None,
    }
}

fn node_scan_alias<'o>(op: &'o PhysOp, ctx: &RuleCtx) -> Option<&'o str> {
    match op {
        PhysOp::Scan { alias, table, .. } if !is_edge_table(ctx, table) => Some(alias),
        PhysOp::Filter { input, .. } => node_scan_alias(input, ctx),
        _ => None,
    }
}

fn rel_index(alias: &str) -> Option<usize> {
    alias.strip_prefix('e')?.parse().ok()
}

fn fk_sides<'r>(rel: &'r InputRelationship, fk_col: &str, ctx: &RuleCtx) -> (&'r str, &'r str) {
    let from_has = ctx
        .input
        .nodes
        .iter()
        .find(|n| n.id == rel.from)
        .and_then(|n| n.table.as_deref())
        .and_then(|t| ctx.input.compiler.table_columns.get(t))
        .is_some_and(|cols| cols.contains(fk_col));
    if from_has {
        (&rel.from, &rel.to)
    } else {
        (&rel.to, &rel.from)
    }
}

fn node_scan(n: &InputNode) -> PhysOp {
    let mut p = Vec::new();
    let mut props: Vec<_> = n.filters.iter().collect();
    props.sort_unstable_by_key(|(k, _)| *k);
    for (prop, fs) in props {
        for f in fs {
            p.push(Predicate::NodeFilter {
                property: prop.clone(),
                filter: f.clone(),
            });
        }
    }
    if !n.node_ids.is_empty() {
        p.push(Predicate::In {
            column: DEFAULT_PRIMARY_KEY.to_string(),
            values: n.node_ids.iter().map(|&id| Value::Int(id)).collect(),
        });
    }
    if let Some(ref r) = n.id_range {
        p.push(Predicate::Range {
            column: DEFAULT_PRIMARY_KEY.to_string(),
            start: r.start,
            end: r.end,
        });
    }
    p.push(deleted_false());
    filter(
        scan(n.table.as_deref().unwrap_or(""), &n.id, Dedup::Final),
        p,
    )
}

/// Aliases read by projections, group keys, metrics, and sort keys. Join
/// conditions are excluded: they tie a relation in without reading it.
/// Union arms are their own scope and are skipped.
fn referenced_aliases(op: &PhysOp, out: &mut HashSet<String>) {
    match op {
        PhysOp::Project { columns, .. } => {
            for c in columns {
                match c {
                    ProjectedColumn::Ref { table, .. } => {
                        out.insert(table.clone());
                    }
                    ProjectedColumn::NodeProperty { node, .. } => {
                        out.insert(node.clone());
                    }
                    ProjectedColumn::Computed { expr, .. } => column_expr_aliases(expr, out),
                    ProjectedColumn::Expr { .. } => {}
                }
            }
        }
        PhysOp::Aggregate {
            group_by, metrics, ..
        } => {
            out.extend(group_by.iter().map(|g| g.node.clone()));
            out.extend(metrics.iter().map(|m| m.node.clone()));
        }
        PhysOp::Sort { keys, .. } => keys.iter().for_each(|k| column_expr_aliases(&k.expr, out)),
        PhysOp::Union { .. } => return,
        _ => {}
    }
    for c in children(op) {
        referenced_aliases(c, out);
    }
}

fn column_expr_aliases(e: &ColumnExpr, out: &mut HashSet<String>) {
    match e {
        ColumnExpr::Col(a, _) => {
            out.insert(a.clone());
        }
        ColumnExpr::Lit(_) | ColumnExpr::Ident(_) => {}
        ColumnExpr::Array(items) | ColumnExpr::Tuple(items) | ColumnExpr::Func(_, items) => {
            items.iter().for_each(|i| column_expr_aliases(i, out))
        }
    }
}

// ── Join spine ──────────────────────────────────────────────────────────────
//
// The chain constructors emit one left-deep tree of inner joins over edge and
// node leaves. Rules that add, drop, or re-wire relations flatten that spine
// to (leaves, equalities), edit the flat form, and rebuild the tree.

struct Spine {
    leaves: Vec<(PhysOp, JoinKind)>,
    eqs: Vec<(Col, Col)>,
}

fn flatten(op: &PhysOp) -> Spine {
    let mut s = Spine {
        leaves: Vec::new(),
        eqs: Vec::new(),
    };
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
                s.eqs.extend(on.iter().cloned());
            }
            leaf => s.leaves.push((leaf.clone(), JoinKind::Inner)),
        }
    }
    go(op, &mut s);
    let mut seen: Vec<(Col, Col)> = Vec::new();
    for (x, y) in s.eqs {
        if !seen
            .iter()
            .any(|(a, b)| (a == &x && b == &y) || (a == &y && b == &x))
        {
            seen.push((x, y));
        }
    }
    s.eqs = seen;
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
    let mut tree: Option<PhysOp> = None;

    while !pending.is_empty() {
        let pick = if tree.is_none() {
            0
        } else {
            pending
                .iter()
                .position(|l| {
                    let a = leaf_alias(l).unwrap_or_default();
                    eqs.iter().any(|(x, y)| touches(x, y, a, &placed))
                })
                .unwrap_or(0)
        };
        let leaf = pending.remove(pick);
        let a = leaf_alias(&leaf).unwrap_or_default().to_string();
        let (on, rest): (Vec<_>, Vec<_>) = eqs
            .into_iter()
            .partition(|(x, y)| touches(x, y, &a, &placed));
        eqs = rest;
        placed.insert(a);
        tree = Some(match tree {
            None => leaf,
            Some(t) => join(t, leaf, on),
        });
    }

    let mut tree = tree.expect("spine has at least one leaf");
    for (leaf, _) in semi {
        let a = leaf_alias(&leaf).unwrap_or_default().to_string();
        let (on, rest): (Vec<_>, Vec<_>) = eqs.into_iter().partition(|(x, y)| x.0 == a || y.0 == a);
        eqs = rest;
        let eq = on.into_iter().next().expect("semi leaf has its equality");
        // Consumer side first so lowering reads `left IN (SELECT right)`.
        let eq = if eq.1.0 == a { eq } else { (eq.1, eq.0) };
        tree = semi_join(tree, leaf, eq);
    }
    tree
}

fn touches(x: &Col, y: &Col, a: &str, placed: &HashSet<String>) -> bool {
    (x.0 == a && placed.contains(&y.0)) || (y.0 == a && placed.contains(&x.0))
}

/// The spine root: the topmost join, or the single leaf when the chain has
/// no joins at all.
fn is_spine_root(op: &PhysOp) -> bool {
    match op {
        PhysOp::Join { .. } | PhysOp::Scan { .. } | PhysOp::Union { .. } => true,
        PhysOp::Filter { input, .. } => {
            matches!(input.as_ref(), PhysOp::Scan { .. } | PhysOp::Union { .. })
        }
        _ => false,
    }
}

/// Applies `f` to the spine under the plan's wrapper operators. `None` when
/// there is no spine or `f` declines.
fn rewrite_spine(op: &PhysOp, f: &dyn Fn(&PhysOp) -> Option<PhysOp>) -> Option<PhysOp> {
    if is_spine_root(op) {
        return f(op);
    }
    match op {
        PhysOp::Filter { input, predicates } => Some(PhysOp::Filter {
            input: Box::new(rewrite_spine(input, f)?),
            predicates: predicates.clone(),
        }),
        PhysOp::Project { input, columns } => Some(PhysOp::Project {
            input: Box::new(rewrite_spine(input, f)?),
            columns: columns.clone(),
        }),
        PhysOp::Aggregate {
            input,
            group_by,
            metrics,
        } => Some(PhysOp::Aggregate {
            input: Box::new(rewrite_spine(input, f)?),
            group_by: group_by.clone(),
            metrics: metrics.clone(),
        }),
        PhysOp::Sort { input, keys } => Some(PhysOp::Sort {
            input: Box::new(rewrite_spine(input, f)?),
            keys: keys.clone(),
        }),
        PhysOp::Limit { input, count } => Some(PhysOp::Limit {
            input: Box::new(rewrite_spine(input, f)?),
            count: *count,
        }),
        _ => None,
    }
}

fn spine_of(op: &PhysOp) -> Option<Spine> {
    if is_spine_root(op) {
        return Some(flatten(op));
    }
    match op {
        PhysOp::Filter { input, .. }
        | PhysOp::Project { input, .. }
        | PhysOp::Aggregate { input, .. }
        | PhysOp::Sort { input, .. }
        | PhysOp::Limit { input, .. } => spine_of(input),
        _ => None,
    }
}

// ── Column substitution ─────────────────────────────────────────────────────

type Subst = HashMap<Col, ColumnExpr>;

fn subst_col(c: &Col, s: &Subst) -> Col {
    match s.get(c) {
        Some(ColumnExpr::Col(a, b)) => (a.clone(), b.clone()),
        _ => c.clone(),
    }
}

fn subst_expr(e: &ColumnExpr, s: &Subst) -> ColumnExpr {
    match e {
        ColumnExpr::Col(a, b) => s
            .get(&(a.clone(), b.clone()))
            .cloned()
            .unwrap_or_else(|| e.clone()),
        ColumnExpr::Lit(_) | ColumnExpr::Ident(_) => e.clone(),
        ColumnExpr::Array(items) => {
            ColumnExpr::Array(items.iter().map(|i| subst_expr(i, s)).collect())
        }
        ColumnExpr::Tuple(items) => {
            ColumnExpr::Tuple(items.iter().map(|i| subst_expr(i, s)).collect())
        }
        ColumnExpr::Func(name, items) => ColumnExpr::Func(
            name.clone(),
            items.iter().map(|i| subst_expr(i, s)).collect(),
        ),
    }
}

fn subst_tree(op: PhysOp, s: &Subst) -> PhysOp {
    let op = match op {
        PhysOp::Project { input, columns } => PhysOp::Project {
            input,
            columns: columns
                .into_iter()
                .map(|c| match c {
                    ProjectedColumn::Ref {
                        table,
                        column,
                        alias,
                    } => match s.get(&(table.clone(), column.clone())) {
                        Some(expr) => ProjectedColumn::Computed {
                            expr: expr.clone(),
                            alias,
                        },
                        None => ProjectedColumn::Ref {
                            table,
                            column,
                            alias,
                        },
                    },
                    ProjectedColumn::Computed { expr, alias } => ProjectedColumn::Computed {
                        expr: subst_expr(&expr, s),
                        alias,
                    },
                    other => other,
                })
                .collect(),
        },
        PhysOp::Sort { input, keys } => PhysOp::Sort {
            input,
            keys: keys
                .into_iter()
                .map(|k| SortKey {
                    expr: subst_expr(&k.expr, s),
                    desc: k.desc,
                })
                .collect(),
        },
        PhysOp::Join {
            left,
            right,
            on,
            kind,
        } => PhysOp::Join {
            left,
            right,
            on: on
                .into_iter()
                .map(|(x, y)| (subst_col(&x, s), subst_col(&y, s)))
                .collect(),
            kind,
        },
        // Union arms are their own scope; their aliases don't leak out.
        PhysOp::Union { .. } => return op,
        other => other,
    };
    map_children(op, |c| subst_tree(c, s))
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
    Some(PhysOp::Filter {
        predicates: merged,
        input: leaf.clone(),
    })
}

/// A node filter on a denormalized property also lives as a tag on the edges
/// of the relationships that write it (`source_tags` / `target_tags`). Push
/// it onto the edge scan so the edge prunes before the node join.
fn rule_denorm_tag_pushdown(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let (predicates, _, alias) = edge_filter_parts(op, ctx)?;
    let rel = ctx.input.relationships.get(rel_index(alias)?)?;
    if crate::passes::normalize::is_wildcard(&rel.types) {
        return None;
    }
    let (sc, ec) = rel.direction.edge_columns();
    let mut pushed = Vec::new();
    for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
        let Some(node) = ctx.input.nodes.iter().find(|n| &n.id == nid) else {
            continue;
        };
        let dir = if ic == SOURCE_ID_COLUMN {
            "source"
        } else {
            "target"
        };
        let mut props: Vec<_> = node.filters.iter().collect();
        props.sort_unstable_by_key(|(k, _)| *k);
        for (prop, fs) in props {
            let Some((tc, tk)) = denorm_tag_for(ctx, node, prop, dir, rel) else {
                continue;
            };
            for f in fs {
                pushed.extend(tag_predicate(tc, tk, f));
            }
        }
    }
    if pushed.is_empty() || pushed.iter().all(|p| predicates.contains(p)) {
        return None;
    }
    let mut merged = predicates.to_vec();
    for p in pushed {
        if !merged.contains(&p) {
            merged.push(p);
        }
    }
    Some(with_predicates(op, merged))
}

/// `(tag_column, tag_key)` when `rel` writes `prop` of `node` as a denorm tag
/// on its `dir` side.
fn denorm_tag_for<'c>(
    ctx: &'c RuleCtx,
    node: &InputNode,
    prop: &str,
    dir: &str,
    rel: &InputRelationship,
) -> Option<(&'c str, &'c str)> {
    let meta = &ctx.input.compiler;
    let key = (
        node.entity.clone().unwrap_or_default(),
        prop.to_string(),
        dir.to_string(),
    );
    let kinds = meta.denorm_rel_kinds.get(&key)?;
    if !rel.types.iter().any(|t| kinds.contains(t)) {
        return None;
    }
    let (tc, tk) = meta.denormalized_columns.get(&key)?;
    Some((tc, tk))
}

fn tag_predicate(tag_col: &str, tag_key: &str, f: &InputFilter) -> Option<Predicate> {
    let scalar = |v: &serde_json::Value| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    };
    match (&f.op, &f.value) {
        (None | Some(FilterOp::Eq), Some(v)) => Some(Predicate::Func {
            name: "has".into(),
            column: tag_col.into(),
            value: Value::Str(format!("{tag_key}:{}", scalar(v)?)),
        }),
        (Some(FilterOp::In), Some(serde_json::Value::Array(arr))) => {
            let tags: Vec<String> = arr
                .iter()
                .filter_map(scalar)
                .map(|s| format!("{tag_key}:{s}"))
                .collect();
            match tags.len() {
                0 => None,
                1 => Some(Predicate::Func {
                    name: "has".into(),
                    column: tag_col.into(),
                    value: Value::Str(tags.into_iter().next().unwrap()),
                }),
                _ => Some(Predicate::Func {
                    name: "hasAny".into(),
                    column: tag_col.into(),
                    value: Value::Strs(tags),
                }),
            }
        }
        _ => None,
    }
}

/// Edge tables that carry node columns (e.g. `project_id`, `branch`) can
/// evaluate node filters on those columns directly.
fn rule_column_pushdown(op: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let (predicates, table, alias) = edge_filter_parts(op, ctx)?;
    if !matches!(op, PhysOp::Filter { input, .. } if matches!(input.as_ref(), PhysOp::Scan { .. }))
    {
        return None;
    }
    let rel = ctx.input.relationships.get(rel_index(alias)?)?;
    let ecols = ctx.input.compiler.table_columns.get(table)?;
    let mut pushed = Vec::new();
    for nid in [&rel.from, &rel.to] {
        let Some(node) = ctx.input.nodes.iter().find(|n| &n.id == nid) else {
            continue;
        };
        let mut props: Vec<_> = node.filters.iter().collect();
        props.sort_unstable_by_key(|(k, _)| *k);
        for (prop, fs) in props {
            if !ecols.contains(prop) || EDGE_RESERVED_COLUMNS.contains(&prop.as_str()) {
                continue;
            }
            for f in fs {
                pushed.push(Predicate::NodeFilter {
                    property: prop.clone(),
                    filter: f.clone(),
                });
            }
        }
    }
    pushed.retain(|p| !predicates.contains(p));
    if pushed.is_empty() {
        return None;
    }
    let mut merged = predicates.to_vec();
    merged.extend(pushed);
    Some(with_predicates(op, merged))
}

/// `Filter` over an edge scan, or over a multi-hop union aliased as an edge
/// (whose arms project the reserved edge columns and tags).
fn edge_filter_parts<'o>(
    op: &'o PhysOp,
    ctx: &'o RuleCtx,
) -> Option<(&'o [Predicate], &'o str, &'o str)> {
    let PhysOp::Filter { predicates, input } = op else {
        return None;
    };
    match input.as_ref() {
        PhysOp::Scan { table, alias, .. } if is_edge_table(ctx, table) => {
            Some((predicates, table, alias))
        }
        PhysOp::Union { alias, .. } if rel_index(alias).is_some() => {
            Some((predicates, &ctx.input.compiler.default_edge_table, alias))
        }
        _ => None,
    }
}

fn with_predicates(op: &PhysOp, predicates: Vec<Predicate>) -> PhysOp {
    match op {
        PhysOp::Filter { input, .. } => filter((**input).clone(), predicates),
        other => filter(other.clone(), predicates),
    }
}

// ── Global rules ────────────────────────────────────────────────────────────

/// A single-hop relationship backed by a foreign key needs no edge scan: the
/// two node tables join directly on the FK column. The edge leaf leaves the
/// spine, its columns are rewritten to node columns or literals, and both
/// endpoint tables are guaranteed present.
fn rule_fk_elision(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let spine = spine_of(tree)?;
    let fk_of = |leaf: &PhysOp| -> Option<(&InputRelationship, String)> {
        let alias = edge_scan_alias(leaf, ctx)?;
        let rel = ctx.input.relationships.get(rel_index(alias)?)?;
        let fk = rel.fk_column.as_ref()?;
        let eligible = rel.hops.max == 1
            && !matches!(rel.direction, Direction::Both)
            && rel.filters.is_empty();
        eligible.then(|| (rel, fk.clone()))
    };
    // Mixed chains keep every edge scan: a remaining edge may carry denorm
    // tags for the FK hop's endpoints, and edge-only chains dedup uniformly.
    let all_fk = spine.leaves.iter().all(|(leaf, _)| {
        let is_edge = edge_scan_alias(leaf, ctx).is_some()
            || matches!(leaf_alias(leaf), Some(a) if rel_index(a).is_some());
        !is_edge || fk_of(leaf).is_some()
    });
    if !all_fk {
        return None;
    }
    let (idx, rel, fk_col) = spine
        .leaves
        .iter()
        .enumerate()
        .find_map(|(i, (leaf, kind))| {
            if *kind != JoinKind::Inner {
                return None;
            }
            fk_of(leaf).map(|(rel, fk)| (i, rel, fk))
        })?;

    let alias = edge_scan_alias(&spine.leaves[idx].0, ctx)?.to_string();
    let (sc, _) = rel.direction.edge_columns();
    let (src, tgt) = if sc == SOURCE_ID_COLUMN {
        (&rel.from, &rel.to)
    } else {
        (&rel.to, &rel.from)
    };
    let entity = |a: &str| {
        ctx.input
            .nodes
            .iter()
            .find(|n| n.id == a)
            .and_then(|n| n.entity.clone())
            .unwrap_or_default()
    };
    let mut s: Subst = HashMap::new();
    s.insert(
        col(&alias, SOURCE_ID_COLUMN),
        ColumnExpr::Col(src.clone(), DEFAULT_PRIMARY_KEY.into()),
    );
    s.insert(
        col(&alias, TARGET_ID_COLUMN),
        ColumnExpr::Col(tgt.clone(), DEFAULT_PRIMARY_KEY.into()),
    );
    s.insert(
        col(&alias, SOURCE_KIND_COLUMN),
        ColumnExpr::Lit(Value::Str(entity(src))),
    );
    s.insert(
        col(&alias, TARGET_KIND_COLUMN),
        ColumnExpr::Lit(Value::Str(entity(tgt))),
    );
    s.insert(
        col(&alias, RELATIONSHIP_KIND_COLUMN),
        ColumnExpr::Lit(Value::Str(rel.types.first().cloned().unwrap_or_default())),
    );

    let (fk_alias, tgt_alias) = fk_sides(rel, &fk_col, ctx);
    let rewritten = rewrite_spine(tree, &|join_op| {
        let mut sp = flatten(join_op);
        sp.leaves.remove(idx);
        sp.eqs = sp
            .eqs
            .into_iter()
            .map(|(x, y)| (subst_col(&x, &s), subst_col(&y, &s)))
            .filter(|(x, y)| x != y)
            .collect();
        sp.eqs
            .push((col(fk_alias, &fk_col), col(tgt_alias, DEFAULT_PRIMARY_KEY)));
        for a in [&rel.from, &rel.to] {
            let present = sp
                .leaves
                .iter()
                .any(|(l, _)| leaf_alias(l) == Some(a.as_str()));
            if !present && let Some(n) = ctx.input.nodes.iter().find(|n| &n.id == a) {
                sp.leaves.push((node_scan(n), JoinKind::Inner));
            }
        }
        Some(rebuild(sp))
    })?;
    Some(subst_tree(rewritten, &s))
}

/// Two or more single-hop edge scans in one spine self-join the edge table;
/// read them with `FINAL` so stale versions don't multiply rows.
fn rule_edge_dedup(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let spine = spine_of(tree)?;
    let edges: Vec<String> = spine
        .leaves
        .iter()
        .filter_map(|(l, _)| edge_scan_alias(l, ctx).map(str::to_string))
        .collect();
    if edges.len() < 2 {
        return None;
    }
    fn set_final(op: PhysOp, edges: &[String], ctx: &RuleCtx) -> PhysOp {
        match op {
            PhysOp::Scan {
                table,
                alias,
                dedup: Dedup::None,
            } if is_edge_table(ctx, &table) && edges.contains(&alias) => PhysOp::Scan {
                table,
                alias,
                dedup: Dedup::Final,
            },
            PhysOp::Union { .. } => op,
            other => map_children(other, |c| set_final(c, edges, ctx)),
        }
    }
    let updated = set_final(tree.clone(), &edges, ctx);
    (updated != *tree).then_some(updated)
}

/// An aggregation over one edge scan must not count stale edge versions.
/// `LIMIT 1 BY <sort key>` dedups while keeping the scan eligible for column
/// pruning and projections; with two or more edges the self-join uses `FINAL`
/// instead (see `rule_edge_dedup`).
fn rule_single_hop_agg_limit_by(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    if ctx.input.query_type != QueryType::Aggregation {
        return None;
    }
    let spine = spine_of(tree)?;
    let edges: Vec<&str> = spine
        .leaves
        .iter()
        .filter_map(|(l, _)| edge_scan_alias(l, ctx))
        .collect();
    let [alias] = edges[..] else {
        return None;
    };
    let alias = alias.to_string();
    fn set(op: PhysOp, alias: &str) -> PhysOp {
        match op {
            PhysOp::Scan {
                table,
                alias: a,
                dedup: Dedup::None,
            } if a == alias => PhysOp::Scan {
                table,
                alias: a,
                dedup: Dedup::LimitBy,
            },
            PhysOp::Union { .. } => op,
            other => map_children(other, |c| set(c, alias)),
        }
    }
    let updated = set(tree.clone(), &alias);
    (updated != *tree).then_some(updated)
}

/// A node table nothing reads is a pure filter on its edge column. When
/// every filter is already carried by an edge's denorm tags the join goes
/// away; otherwise it becomes a semi-join (`IN (SELECT id ...)`), which
/// never multiplies rows and lets ClickHouse build a hash set once.
fn rule_unreferenced_nodes(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    let mut referenced = HashSet::new();
    referenced_aliases(tree, &mut referenced);
    rewrite_spine(tree, &|join_op| {
        let mut sp = flatten(join_op);
        let mut changed = false;
        let mut i = 0;
        while i < sp.leaves.len() {
            let (leaf, kind) = &sp.leaves[i];
            let Some(alias) = node_scan_alias(leaf, ctx).map(str::to_string) else {
                i += 1;
                continue;
            };
            let Some(node) = ctx.input.nodes.iter().find(|n| n.id == alias) else {
                i += 1;
                continue;
            };
            let touching: Vec<&(Col, Col)> = sp
                .eqs
                .iter()
                .filter(|(x, y)| x.0 == alias || y.0 == alias)
                .collect();
            if *kind != JoinKind::Inner || referenced.contains(&alias) || touching.len() != 1 {
                i += 1;
                continue;
            }
            let (x, y) = touching[0];
            let own_col = if x.0 == alias { &x.1 } else { &y.1 };
            if own_col != DEFAULT_PRIMARY_KEY {
                // Joined on its FK column: several rows may match one key, so
                // the inner join's row multiplication is the graph semantics.
                i += 1;
                continue;
            }
            let unconstrained =
                node.filters.is_empty() && node.node_ids.is_empty() && node.id_range.is_none();
            let edges_present: HashSet<String> = sp
                .leaves
                .iter()
                .filter_map(|(l, _)| leaf_alias(l).map(str::to_string))
                .collect();
            if unconstrained || all_filters_denormed(ctx, node, &edges_present) {
                sp.leaves.remove(i);
                sp.eqs.retain(|(x, y)| x.0 != alias && y.0 != alias);
                changed = true;
                continue;
            }
            sp.leaves[i].1 = JoinKind::Semi;
            changed = true;
            i += 1;
        }
        changed.then(|| rebuild(sp))
    })
}

/// Every property filter on `node` is carried as a denorm tag by an edge
/// that is still scanned in the spine.
fn all_filters_denormed(ctx: &RuleCtx, node: &InputNode, present: &HashSet<String>) -> bool {
    !node.filters.is_empty()
        && node.node_ids.is_empty()
        && node.id_range.is_none()
        && node.filters.keys().all(|prop| {
            ctx.input
                .relationships
                .iter()
                .enumerate()
                .filter(|(i, _)| present.contains(&format!("e{i}")))
                .any(|(_, rel)| {
                    let (sc, ec) = rel.direction.edge_columns();
                    [(&rel.from, sc), (&rel.to, ec)].into_iter().any(|(n, ic)| {
                        let dir = if ic == SOURCE_ID_COLUMN {
                            "source"
                        } else {
                            "target"
                        };
                        n == &node.id && denorm_tag_for(ctx, node, prop, dir, rel).is_some()
                    })
                })
        })
}

/// Sideways information passing along the chain: when hop N-1 is pinned by
/// ids or filters, hop N only needs the rows whose start id appears among
/// hop N-1's end ids. Adds `e_N.start IN (SELECT e_{N-1}.end FROM ...)`.
fn rule_cascade_sip(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    rewrite_spine(tree, &|join_op| {
        let mut sp = flatten(join_op);
        let edge_leaves: HashMap<String, PhysOp> = sp
            .leaves
            .iter()
            .filter_map(|(l, k)| {
                (*k == JoinKind::Inner)
                    .then(|| edge_scan_alias(l, ctx).map(|a| (a.to_string(), l.clone())))
                    .flatten()
            })
            .collect();
        let mut added = Vec::new();
        for (x, y) in &sp.eqs {
            let (Some(ix), Some(iy)) = (rel_index(&x.0), rel_index(&y.0)) else {
                continue;
            };
            if !edge_leaves.contains_key(&x.0) || !edge_leaves.contains_key(&y.0) {
                continue;
            }
            // Anchor from the earlier hop into the later one.
            let (prev, curr) = if ix < iy { (x, y) } else { (y, x) };
            let prev_rel = &ctx.input.relationships[ix.min(iy)];
            if prev_rel.fk_column.is_some() || !is_selective(&edge_leaves[&prev.0]) {
                continue;
            }
            let sip_alias = format!("_sip_{}", prev.0);
            if has_alias(join_op, &sip_alias) {
                continue;
            }
            let body = project(
                realias(edge_leaves[&prev.0].clone(), &prev.0, &sip_alias),
                vec![ProjectedColumn::Ref {
                    table: sip_alias.clone(),
                    column: prev.1.clone(),
                    alias: prev.1.clone(),
                }],
            );
            added.push((body, (curr.clone(), col(&sip_alias, &prev.1))));
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

/// An edge leaf pinned by ids, an id range, or a node-property filter.
fn is_selective(leaf: &PhysOp) -> bool {
    match leaf {
        PhysOp::Filter { predicates, .. } => predicates.iter().any(|p| {
            matches!(
                p,
                Predicate::In { column, .. } | Predicate::Range { column, .. }
                    if column == SOURCE_ID_COLUMN || column == TARGET_ID_COLUMN
            ) || matches!(p, Predicate::NodeFilter { .. } | Predicate::Func { .. })
        }),
        _ => false,
    }
}

fn realias(op: PhysOp, from: &str, to: &str) -> PhysOp {
    match op {
        PhysOp::Scan {
            table,
            alias,
            dedup,
        } if alias == from => PhysOp::Scan {
            table,
            alias: to.to_string(),
            dedup,
        },
        other => map_children(other, |c| realias(c, from, to)),
    }
}

/// An aggregation anchored on a namespace (`(g:Group {full_path})-[:CONTAINS]->(p)`)
/// whose only edge scan is that containment hop: the scope prefix restrict
/// derived from the anchor already pins every other scan to the namespace, so
/// the anchor table and its edge are redundant. A guard keeps a missing anchor
/// yielding no rows instead of the broad fallback.
fn rule_scope_anchor_elision(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
    if ctx.input.query_type != QueryType::Aggregation {
        return None;
    }
    let mut referenced = HashSet::new();
    referenced_aliases(tree, &mut referenced);
    let spine = spine_of(tree)?;
    let non_fk_edges: Vec<(usize, &InputRelationship)> = spine
        .leaves
        .iter()
        .filter_map(|(l, _)| edge_scan_alias(l, ctx))
        .filter_map(|a| rel_index(a))
        .filter_map(|i| ctx.input.relationships.get(i).map(|r| (i, r)))
        .filter(|(_, r)| r.fk_column.is_none())
        .collect();
    let [(idx, rel)] = non_fk_edges[..] else {
        return None;
    };
    let Some(prefix) = rel.scope_prefix.as_ref() else {
        return None;
    };
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
        let Some(n) = ctx.input.nodes.iter().find(|n| &n.id == *a) else {
            return false;
        };
        n.has_traversal_path
            && degree(a) == 1
            && crate::scope::is_scope_only(n)
            && !referenced.contains(a.as_str())
    })?;
    let edge_alias = format!("e{idx}");
    let guard = Predicate::Expr(prefix.resolved());

    rewrite_spine(tree, &|join_op| {
        let mut sp = flatten(join_op);
        // Re-bind columns the edge carried for the surviving endpoint onto
        // whichever other column shared them, then drop the edge and anchor.
        let mut s: Subst = HashMap::new();
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
                    .or_insert_with(|| ColumnExpr::Col(other.0.clone(), other.1.clone()));
            }
        }
        sp.leaves.retain(
            |(l, _)| !matches!(leaf_alias(l), Some(a) if a == edge_alias || a == anchor.as_str()),
        );
        sp.eqs = sp
            .eqs
            .into_iter()
            .map(|(x, y)| (subst_col(&x, &s), subst_col(&y, &s)))
            .filter(|(x, y)| {
                x != y && x.0 != edge_alias && y.0 != edge_alias && x.0 != *anchor && y.0 != *anchor
            })
            .collect();
        if sp.leaves.is_empty() {
            return None;
        }
        Some(filter(rebuild(sp), vec![guard.clone()]))
    })
}
