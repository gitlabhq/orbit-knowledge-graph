//! Bidirectional BFS: `forward` and `backward` frontier CTEs (one arm per
//! depth), combined as direct hits (forward reaches the end) plus meets
//! (forward and backward share an `end_id`).

use super::chain::{hop_chain, kind_col, path_nodes};
use super::*;
use crate::constants::*;

const SCOPE_CTE: &str = "_path_scope_traversal_paths";

/// How a path endpoint constrains the first frontier hop: pinned ids go
/// straight onto the edge column; filters become an `_nf_` anchor CTE the
/// edge column is semi-joined against.
struct Anchor {
    ids: Option<Vec<i64>>,
    cte: Option<(String, PhysOp)>,
    has_tp: bool,
}

impl Anchor {
    fn cte_name(&self) -> Option<&str> {
        self.cte.as_ref().map(|(n, _)| n.as_str())
    }

    /// Constrain `alias.column` to this endpoint.
    fn apply(&self, op: PhysOp, alias: &str, column: &str) -> PhysOp {
        if let Some(ids) = &self.ids {
            return op.filter(vec![id_in(alias, column, ids)]);
        }
        match self.cte_name() {
            Some(cte) => op.semi(
                scan(cte, cte, Dedup::None),
                (
                    (alias.into(), column.into()),
                    (cte.into(), DEFAULT_PRIMARY_KEY.into()),
                ),
            ),
            None => op,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum FDir {
    Forward,
    Backward,
}

struct Frontier<'f> {
    dir: FDir,
    anchor: &'f Anchor,
    anchor_entity: &'f str,
    first_hop_types: &'f [String],
    /// Denorm tags from the anchor node's filters, on `e1`.
    anchor_tags: Vec<PExpr>,
}

impl<'a> PlanCtx<'a> {
    pub fn plan_pathfinding(&self, limit: u32) -> PhysOp {
        let cfg = self.input.path.as_ref().expect("path config");
        let start = self.node(&cfg.from).expect("start node");
        let end = self.node(&cfg.to).expect("end node");
        let (se, ee) = (
            start.entity.as_deref().unwrap_or(""),
            end.entity.as_deref().unwrap_or(""),
        );
        let scoped = start.has_traversal_path && end.has_traversal_path;
        let tables = self.input.compiler.resolve_edge_tables(&cfg.rel_types);
        let fwd_depth = cfg.max_depth / 2 + cfg.max_depth % 2;
        let bwd_depth = if cfg.max_depth >= 2 {
            cfg.max_depth / 2
        } else {
            0
        };

        let start_anchor = self.anchor(start, scoped);
        let end_anchor = self.anchor(end, scoped);
        let scope = scope_cte(&start_anchor, &end_anchor);

        let mut ctes: Vec<(String, PhysOp)> = Vec::new();
        ctes.extend(start_anchor.cte.clone());
        ctes.extend(end_anchor.cte.clone());
        ctes.extend(scope.clone());
        let frontier = |dir: FDir,
                        depth: u32,
                        anchor: &Anchor,
                        node: &InputNode,
                        entity: &str,
                        first: &[String],
                        tag_dir: &str| {
            let f = Frontier {
                dir,
                anchor,
                anchor_entity: entity,
                first_hop_types: first,
                anchor_tags: self.denorm_tags(node, tag_dir, "e1", None),
            };
            let arms = (1..=depth)
                .map(|d| self.frontier_arm(d, &f, &tables, &cfg.rel_types, scope.is_some(), scoped))
                .collect();
            PhysOp::union(arms, "_frontier")
        };
        ctes.push((
            FORWARD_CTE.into(),
            frontier(
                FDir::Forward,
                fwd_depth,
                &start_anchor,
                start,
                se,
                &cfg.forward_first_hop_rel_types,
                "source",
            ),
        ));
        if bwd_depth > 0 {
            ctes.push((
                BACKWARD_CTE.into(),
                frontier(
                    FDir::Backward,
                    bwd_depth,
                    &end_anchor,
                    end,
                    ee,
                    &cfg.backward_first_hop_rel_types,
                    "target",
                ),
            ));
        }

        let (f, b) = (FORWARD_ALIAS, BACKWARD_ALIAS);
        let endpoint =
            |t: &str, entity: &str| func("tuple", vec![col(t, ANCHOR_ID_COLUMN), lit(entity)]);

        let direct = end_anchor
            .apply(
                scan(FORWARD_CTE, f, Dedup::None).filter(vec![
                    eq(col(f, DEPTH_COLUMN), lit(1)),
                    eq(col(f, END_KIND_COLUMN), lit(ee)),
                ]),
                f,
                END_ID_COLUMN,
            )
            .project(vec![
                named(col(f, DEPTH_COLUMN), DEPTH_COLUMN),
                named(
                    func(
                        "arrayConcat",
                        vec![
                            func("array", vec![endpoint(f, se)]),
                            col(f, PATH_NODES_COLUMN),
                        ],
                    ),
                    path_column(),
                ),
                named(col(f, FRONTIER_EDGE_KINDS_COLUMN), edge_kinds_column()),
            ]);

        let mut arms = vec![direct];
        if bwd_depth > 0 {
            let depth_sum = func("plus", vec![col(f, DEPTH_COLUMN), col(b, DEPTH_COLUMN)]);
            let mut on = vec![(
                (f.into(), END_ID_COLUMN.into()),
                (b.into(), END_ID_COLUMN.into()),
            )];
            if scoped {
                on.push((
                    (f.into(), TRAVERSAL_PATH_COLUMN.into()),
                    (b.into(), TRAVERSAL_PATH_COLUMN.into()),
                ));
            }
            let meet = scan(FORWARD_CTE, f, Dedup::None)
                .join(scan(BACKWARD_CTE, b, Dedup::None), on)
                .filter(vec![cmp(
                    CmpOp::Le,
                    depth_sum.clone(),
                    lit(cfg.max_depth as i64),
                )]);
            arms.push(meet.project(vec![
                named(depth_sum, DEPTH_COLUMN),
                named(
                    func(
                        "arrayConcat",
                        vec![
                            func("array", vec![endpoint(f, se)]),
                            col(f, PATH_NODES_COLUMN),
                            func("arrayReverse", vec![col(b, PATH_NODES_COLUMN)]),
                            func("array", vec![endpoint(b, ee)]),
                        ],
                    ),
                    path_column(),
                ),
                named(
                    func(
                        "arrayConcat",
                        vec![
                            col(f, FRONTIER_EDGE_KINDS_COLUMN),
                            func("arrayReverse", vec![col(b, FRONTIER_EDGE_KINDS_COLUMN)]),
                        ],
                    ),
                    edge_kinds_column(),
                ),
            ]));
        }

        let p = PATHS_ALIAS;
        let mut keys = vec![(col(p, DEPTH_COLUMN), false)];
        if self.input.cursor.is_some() {
            keys.extend(
                [path_column(), edge_kinds_column()]
                    .iter()
                    .map(|c| (func("toString", vec![col(p, c)]), false)),
            );
        }
        let body = PhysOp::union(arms, p)
            .project(vec![
                named(col(p, &path_column()), path_column()),
                named(col(p, &edge_kinds_column()), edge_kinds_column()),
                named(col(p, DEPTH_COLUMN), DEPTH_COLUMN),
            ])
            .sort(keys)
            .limit(limit);
        PhysOp::With {
            ctes,
            input: Box::new(body),
        }
    }

    fn anchor(&self, n: &InputNode, force_cte: bool) -> Anchor {
        if !force_cte && !n.node_ids.is_empty() {
            return Anchor {
                ids: Some(n.node_ids.clone()),
                cte: None,
                has_tp: false,
            };
        }
        if n.node_ids.is_empty() && n.filters.is_empty() && n.id_range.is_none() {
            return Anchor {
                ids: None,
                cte: None,
                has_tp: false,
            };
        }
        let a = n.id.as_str();
        let mut cols = vec![named(col(a, DEFAULT_PRIMARY_KEY), DEFAULT_PRIMARY_KEY)];
        if n.has_traversal_path {
            cols.push(named(col(a, TRAVERSAL_PATH_COLUMN), TRAVERSAL_PATH_COLUMN));
        }
        let mut inner_cols = cols.clone();
        inner_cols.push(named(col(a, DELETED_COLUMN), DELETED_COLUMN));
        // node_scan filters `_deleted` inside; the anchor judges it on the
        // deduped row instead, so rebuild the scan without it.
        let PhysOp::Filter {
            input,
            mut predicates,
        } = self.node_scan(n)
        else {
            unreachable!()
        };
        predicates.retain(|p| *p != deleted_false(a));
        let body = input
            .filter(predicates)
            .project(inner_cols)
            .filter(vec![deleted_false(a)])
            .project(cols)
            .limit(crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32);
        Anchor {
            ids: None,
            cte: Some((node_filter_cte(a), body)),
            has_tp: n.has_traversal_path,
        }
    }

    fn frontier_arm(
        &self,
        depth: u32,
        f: &Frontier<'_>,
        tables: &[String],
        rel_types: &[String],
        has_scope: bool,
        include_tp: bool,
    ) -> PhysOp {
        let (anchor_col, next_col) = match f.dir {
            FDir::Forward => (SOURCE_ID_COLUMN, TARGET_ID_COLUMN),
            FDir::Backward => (TARGET_ID_COLUMN, SOURCE_ID_COLUMN),
        };
        let scope_of = |op: PhysOp, a: &str| {
            if !has_scope {
                return op;
            }
            op.semi(
                scan(SCOPE_CTE, SCOPE_CTE, Dedup::None),
                (
                    (a.into(), TRAVERSAL_PATH_COLUMN.into()),
                    (SCOPE_CTE.into(), TRAVERSAL_PATH_COLUMN.into()),
                ),
            )
        };
        // A specific first-hop filter wins; otherwise the general rel_type
        // filter keeps e1 from going unfiltered.
        let first_types = if f.first_hop_types.is_empty() {
            rel_types
        } else {
            f.first_hop_types
        };
        let edge = |a: &str| -> PhysOp {
            let base = self.edge_scan(tables, a, |_| vec![]);
            if a == "e1" {
                scope_of(f.anchor.apply(base, a, anchor_col), a)
            } else {
                scope_of(base, a)
            }
        };
        let hop_preds = |a: &str, first: bool| -> Vec<PExpr> {
            let mut p = Vec::new();
            if first {
                p.extend(rel_kind(a, first_types));
                p.push(eq(col(a, kind_col(anchor_col)), lit(f.anchor_entity)));
                p.push(deleted_false(a));
                p.extend(f.anchor_tags.iter().cloned());
            } else {
                p.extend(rel_kind(a, rel_types));
                p.push(deleted_false(a));
            }
            p
        };
        let hop_on = |prev: &str, curr: &str| -> Vec<(Col, Col)> {
            if include_tp {
                vec![(
                    (prev.into(), TRAVERSAL_PATH_COLUMN.into()),
                    (curr.into(), TRAVERSAL_PATH_COLUMN.into()),
                )]
            } else {
                vec![]
            }
        };
        let chain = hop_chain(&edge, depth, (anchor_col, next_col), &hop_preds, &hop_on);

        let last = format!("e{depth}");
        let nodes = match f.dir {
            FDir::Forward => 1..=depth,
            FDir::Backward => 1..=depth.saturating_sub(1),
        };
        // Backward depth 1 has no intermediate nodes; keep the arm's column
        // type as Array(Tuple(Int64, String)) with an empty typed array.
        let path = if nodes.is_empty() {
            func(
                "arrayResize",
                vec![
                    func("array", vec![func("tuple", vec![lit(0), lit("")])]),
                    lit(0),
                ],
            )
        } else {
            path_nodes(nodes, next_col)
        };
        let kinds = func(
            "array",
            (1..=depth)
                .map(|i| col(&format!("e{i}"), RELATIONSHIP_KIND_COLUMN))
                .collect(),
        );
        let mut columns = vec![
            named(col("e1", anchor_col), ANCHOR_ID_COLUMN),
            named(col(&last, next_col), END_ID_COLUMN),
            named(col(&last, kind_col(next_col)), END_KIND_COLUMN),
            named(path, PATH_NODES_COLUMN),
            named(kinds, FRONTIER_EDGE_KINDS_COLUMN),
            named(lit(depth as i64), DEPTH_COLUMN),
        ];
        if include_tp {
            columns.push(named(
                col("e1", TRAVERSAL_PATH_COLUMN),
                TRAVERSAL_PATH_COLUMN,
            ));
        }
        chain.project(columns)
    }
}

/// Endpoints at different namespace depths are linked by edges carrying only
/// the deeper tp, so the scope is the UNION (not intersection) of both
/// anchors' traversal paths.
fn scope_cte(start: &Anchor, end: &Anchor) -> Option<(String, PhysOp)> {
    let (s, e) = (start.cte_name()?, end.cte_name()?);
    if !start.has_tp || !end.has_tp {
        return None;
    }
    let arm = |cte: &str, a: &str| PhysOp::Aggregate {
        input: Box::new(scan(cte, a, Dedup::None)),
        group_by: vec![named(col(a, TRAVERSAL_PATH_COLUMN), TRAVERSAL_PATH_COLUMN)],
        metrics: vec![],
    };
    Some((
        SCOPE_CTE.into(),
        PhysOp::union(
            vec![arm(s, "_path_scope_start"), arm(e, "_path_scope_end")],
            "_path_scope",
        ),
    ))
}
