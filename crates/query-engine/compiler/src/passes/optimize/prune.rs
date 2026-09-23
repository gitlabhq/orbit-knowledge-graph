//! A node table nothing reads is only a filter on the column that joins it.
//! It becomes a semi-join (`IN (SELECT id ...)`, which never multiplies
//! rows); or its pinned ids become a predicate on that column directly; or
//! it disappears when it has no filters, or an edge still in the spine
//! carries all its filters as denorm tags.

use super::prelude::*;

/// A node table nothing reads, joined on its primary key, is a pure filter
/// on the other side's column: it becomes a semi-join (`IN (SELECT id ...)`,
/// which never multiplies rows), or disappears when it has no filters or an
/// edge still in the spine already carries them all as denorm tags. A node
/// joined on its FK column stays inner: several rows may match one key, and
/// that multiplication is the graph semantics.
pub fn rule_unreferenced_nodes(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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
            // Pinned by ids and nothing else: the ids constrain the other
            // side's column directly (`mr.author_id = 116`), no scan needed.
            let ids_only =
                node.filters.is_empty() && node.id_range.is_none() && !node.node_ids.is_empty();
            changed = true;
            if covered {
                sp.remove(&a);
            } else if ids_only {
                let other = if x.0 == a { y.clone() } else { x.clone() };
                let pred = id_in(&other.0, &other.1, &node.node_ids);
                sp.remove(&a);
                if let Some((leaf, _)) = sp
                    .leaves
                    .iter_mut()
                    .find(|(l, _)| l.alias() == Some(other.0.as_str()))
                {
                    *leaf = std::mem::replace(leaf, scan("", "", Dedup::None)).filter(vec![pred]);
                }
            } else {
                sp.leaves[i].1 = JoinKind::Semi;
                i += 1;
            }
        }
        changed.then(|| rebuild(sp))
    })
}
