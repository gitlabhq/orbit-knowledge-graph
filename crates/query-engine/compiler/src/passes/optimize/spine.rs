//! The join spine: the chain planner emits one left-deep tree of inner joins
//! over edge and node leaves. Rules that add, drop, or re-wire relations
//! flatten that tree to `(leaves, equalities)`, edit the flat form, and
//! rebuild the tree.
//!
//! ```text
//!   Join(Join(e0, e1, [e0.target_id = e1.source_id]), g, [e0.target_id = g.id])
//!     ⇅ flatten / rebuild
//!   leaves: [e0, e1, g]   eqs: [e0.target_id = e1.source_id, e0.target_id = g.id]
//! ```

use super::prelude::*;

// ── Join spine ──────────────────────────────────────────────────────────────
//
// The chain planner emits one left-deep tree of inner joins over edge and
// node leaves. Rules that add, drop, or re-wire relations flatten that spine
// to (leaves, equalities), edit the flat form, and rebuild the tree.

pub struct Spine {
    pub leaves: Vec<(PhysOp, JoinKind)>,
    pub eqs: Vec<(Col, Col)>,
}

pub fn flatten(op: &PhysOp) -> Spine {
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
pub fn rebuild(spine: Spine) -> PhysOp {
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

impl Spine {
    pub fn leaf(&self, alias: &str) -> Option<&PhysOp> {
        self.leaves
            .iter()
            .map(|(l, _)| l)
            .find(|l| l.alias() == Some(alias))
    }

    pub fn remove(&mut self, alias: &str) {
        self.leaves.retain(|(l, _)| l.alias() != Some(alias));
        self.eqs.retain(|(x, y)| x.0 != alias && y.0 != alias);
    }

    pub fn subst(&mut self, s: &HashMap<Col, PExpr>) {
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

// ── Locating the spine in a plan ──────────────────────────────────────────────

/// The spine root: the topmost join, or the single leaf when the chain has
/// no joins at all.
pub fn is_spine_root(op: &PhysOp) -> bool {
    match op {
        PhysOp::Join { .. } | PhysOp::Scan { .. } | PhysOp::Union { .. } => true,
        PhysOp::Filter { input, .. } => {
            matches!(**input, PhysOp::Scan { .. } | PhysOp::Union { .. })
        }
        _ => false,
    }
}

pub fn spine_of(op: &PhysOp) -> Option<Spine> {
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

/// Applies `f` to the spine under the plan's wrapper operators. `None` when
/// there is no spine or `f` declines.
pub fn rewrite_spine(op: &PhysOp, f: &dyn Fn(&PhysOp) -> Option<PhysOp>) -> Option<PhysOp> {
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

/// Rewrites column references everywhere in this scope (not inside union arms).
pub fn subst_tree(op: PhysOp, s: &HashMap<Col, PExpr>) -> PhysOp {
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
