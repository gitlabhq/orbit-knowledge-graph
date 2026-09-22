//! FK elision. `(u:User)-[:AUTHORED]->(mr)` needs no edge scan when the
//! ontology says `gl_merge_request.author_id` is the relationship:
//!
//! ```sql
//! FROM gl_merge_request mr JOIN gl_user u ON mr.author_id = u.id
//! ```
//!
//! The edge leaf leaves the spine, its columns become node columns or
//! literals everywhere they were read, and both endpoint tables are joined.

use super::prelude::*;

// ── Global rules ────────────────────────────────────────────────────────────

/// A single-hop relationship backed by a foreign key needs no edge scan: the
/// two node tables join directly on the FK column. Fires only when every
/// edge in the spine is FK-backed; a mixed chain keeps its edge scans so
/// remaining edges can still carry denorm tags and dedup uniformly.
pub fn rule_fk_elision(tree: &PhysOp, ctx: &RuleCtx) -> Option<PhysOp> {
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
