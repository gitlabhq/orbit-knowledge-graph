//! Hydration: fetch requested properties for known ids, one latest-row arm
//! per entity, returned as a JSON map per row.

use super::*;
use orbit_utils::traversal_path::{TraversalPath, prune_to_leaves};

/// Above this many dynamic paths, OR-of-startsWith risks ClickHouse parser
/// depth limits; switch to a single arrayExists.
const ARRAY_EXISTS_PATH_THRESHOLD: usize = 256;

/// Narrow a hydration scan to the traversal paths the base query touched.
/// Paths that are prefixes of other paths are dropped (the deeper one is
/// more selective; `id IN` is the correctness guarantee), then widened to
/// ancestors until the segment budget holds.
fn traversal_path_filter(
    alias: &str,
    paths: &[TraversalPath],
    is_dynamic: bool,
    budget: Option<usize>,
) -> Option<PExpr> {
    let mut leaves = prune_to_leaves(paths);
    if let Some(budget) = budget {
        while leaves.iter().map(|p| p.segment_count()).sum::<usize>() > budget {
            let parents: Vec<TraversalPath> = leaves.iter().map(|p| p.parent()).collect();
            if parents == leaves {
                break;
            }
            leaves = prune_to_leaves(&parents);
        }
    }
    if leaves.is_empty() {
        return None;
    }
    let tp = col(alias, TRAVERSAL_PATH_COLUMN);
    if is_dynamic && leaves.len() > ARRAY_EXISTS_PATH_THRESHOLD {
        let param = "_gkg_path";
        return Some(func(
            "arrayExists",
            vec![
                PExpr::Lambda(
                    param.into(),
                    Box::new(func("startsWith", vec![tp, PExpr::Ident(param.into())])),
                ),
                func("array", leaves.iter().map(|p| lit(p.as_str())).collect()),
            ],
        ));
    }
    let arms: Vec<PExpr> = leaves
        .iter()
        .map(|p| func("startsWith", vec![tp.clone(), lit(p.as_str())]))
        .collect();
    Some(match arms.len() {
        1 => arms.into_iter().next().unwrap(),
        _ => PExpr::Or(arms),
    })
}

impl<'a> PlanCtx<'a> {
    fn hydration_arm(&self, n: &InputNode) -> PhysOp {
        let a = n.id.as_str();
        let pk = n.id_property.as_str();
        let columns = crate::passes::shared::requested_columns(&n.columns);

        let mut inner = Vec::new();
        inner.extend(traversal_path_filter(
            a,
            &n.traversal_paths,
            self.input.hydration_dynamic,
            self.input.path_segment_budget,
        ));
        if !n.node_ids.is_empty() {
            inner.push(id_in(a, pk, &n.node_ids));
        }
        let inner_cols = [pk, DELETED_COLUMN]
            .into_iter()
            .chain(
                columns
                    .iter()
                    .map(String::as_str)
                    .filter(|c| *c != pk && *c != DELETED_COLUMN),
            )
            .map(|c| named(col(a, c), c))
            .collect();

        let props = if columns.is_empty() {
            lit("{}")
        } else {
            let entries = columns
                .iter()
                .flat_map(|c| [lit(c.as_str()), func("toString", vec![col(a, c)])])
                .collect();
            func("toJSONString", vec![func("map", entries)])
        };
        // Traversal paths and ids prune inside the LIMIT BY scan; `_deleted`
        // must be judged on the surviving latest row, so it filters outside.
        scan(n.table.as_deref().unwrap_or(""), a, Dedup::LimitBy)
            .filter(inner)
            .project(inner_cols)
            .filter(vec![deleted_false(a)])
            .project(vec![
                named(col(a, pk), format!("{a}_{pk}")),
                named(
                    lit(n.entity.as_deref().unwrap_or("")),
                    format!("{a}_entity_type"),
                ),
                named(props, format!("{a}_props")),
            ])
    }

    pub fn plan_hydration(&self, limit: u32) -> PhysOp {
        let arms = self
            .input
            .nodes
            .iter()
            .map(|n| self.hydration_arm(n))
            .collect();
        PhysOp::union(arms, crate::constants::HYDRATION_NODE_ALIAS).limit(limit)
    }
}
