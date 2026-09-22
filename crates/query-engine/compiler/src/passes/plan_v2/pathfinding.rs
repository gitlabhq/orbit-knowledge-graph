//! Bidirectional BFS: `forward` and `backward` frontier CTEs (one arm per
//! depth), combined as direct hits (forward reaches the end) plus meets
//! (forward and backward share an `end_id`).

use super::prelude::*;

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
                on(&[&format!("{alias}.{column} = {cte}.id")])[0].clone(),
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
    tables: &'f [String],
    rel_types: &'f [String],
    has_scope: bool,
    include_tp: bool,
}

impl<'a> PlanCtx<'a> {
    pub fn plan_pathfinding(&self, limit: u32) -> PhysOp {
        let cfg = self.input.path.as_ref().expect("path config");
        let start = self.node(&cfg.from).expect("start node");
        let end = self.node(&cfg.to).expect("end node");
        let se = start.entity.as_deref().unwrap_or("");
        let ee = end.entity.as_deref().unwrap_or("");
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

        let frontier = |dir, depth, anchor, node: &InputNode, entity, first, tag_dir| {
            let f = Frontier {
                dir,
                anchor,
                anchor_entity: entity,
                first_hop_types: first,
                anchor_tags: self.denorm_tags(node, tag_dir, "e1", None),
                tables: &tables,
                rel_types: &cfg.rel_types,
                has_scope: scope.is_some(),
                include_tp: scoped,
            };
            PhysOp::union(
                (1..=depth).map(|d| self.frontier_arm(d, &f)).collect(),
                "_frontier",
            )
        };
        let mut ctes: Vec<(String, PhysOp)> = Vec::new();
        ctes.extend(start_anchor.cte.clone());
        ctes.extend(end_anchor.cte.clone());
        ctes.extend(scope.clone());
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

        let (f, b, p) = (FORWARD_ALIAS, BACKWARD_ALIAS, PATHS_ALIAS);
        let (path, kinds) = (path_column(), edge_kinds_column());
        let direct = end_anchor
            .apply(
                scan(FORWARD_CTE, f, Dedup::None)
                    .filter(vec![pe!("{f}.depth = 1 AND {f}.end_kind = {ee:?}")]),
                f,
                END_ID_COLUMN,
            )
            .project(vec![
                pn!("{f}.depth AS depth"),
                pn!("arrayConcat([tuple({f}.anchor_id, {se:?})], {f}.path_nodes) AS {path}"),
                pn!("{f}.edge_kinds AS {kinds}"),
            ]);

        let mut arms = vec![direct];
        if bwd_depth > 0 {
            let mut join_on = vec![format!("{f}.end_id = {b}.end_id")];
            if scoped {
                join_on.push(format!("{f}.traversal_path = {b}.traversal_path"));
            }
            let refs: Vec<&str> = join_on.iter().map(String::as_str).collect();
            let max = cfg.max_depth;
            arms.push(
                scan(FORWARD_CTE, f, Dedup::None)
                    .join(scan(BACKWARD_CTE, b, Dedup::None), on(&refs))
                    .filter(vec![pe!("plus({f}.depth, {b}.depth) <= {max}")])
                    .project(vec![
                        pn!("plus({f}.depth, {b}.depth) AS depth"),
                        pn!("arrayConcat([tuple({f}.anchor_id, {se:?})], {f}.path_nodes, arrayReverse({b}.path_nodes), [tuple({b}.anchor_id, {ee:?})]) AS {path}"),
                        pn!("arrayConcat({f}.edge_kinds, arrayReverse({b}.edge_kinds)) AS {kinds}"),
                    ]),
            );
        }

        let mut keys = vec![(pe!("{p}.depth"), false)];
        if self.input.cursor.is_some() {
            keys.push((pe!("toString({p}.{path})"), false));
            keys.push((pe!("toString({p}.{kinds})"), false));
        }
        let body = PhysOp::union(arms, p)
            .project(vec![
                pn!("{p}.{path} AS {path}"),
                pn!("{p}.{kinds} AS {kinds}"),
                pn!("{p}.depth AS depth"),
            ])
            .sort(keys)
            .limit(limit);
        PhysOp::With {
            ctes,
            input: Box::new(body),
        }
    }

    fn anchor(&self, n: &InputNode, force_cte: bool) -> Anchor {
        let none = Anchor {
            ids: None,
            cte: None,
            has_tp: false,
        };
        if !force_cte && !n.node_ids.is_empty() {
            return Anchor {
                ids: Some(n.node_ids.clone()),
                ..none
            };
        }
        if n.node_ids.is_empty() && n.filters.is_empty() && n.id_range.is_none() {
            return none;
        }
        let a = n.id.as_str();
        let mut cols = vec![pn!("{a}.id AS id")];
        if n.has_traversal_path {
            cols.push(pn!("{a}.traversal_path AS traversal_path"));
        }
        let mut inner_cols = cols.clone();
        inner_cols.push(pn!("{a}._deleted AS _deleted"));
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

    fn frontier_arm(&self, depth: u32, f: &Frontier<'_>) -> PhysOp {
        let (anchor_col, next_col) = match f.dir {
            FDir::Forward => (SOURCE_ID_COLUMN, TARGET_ID_COLUMN),
            FDir::Backward => (TARGET_ID_COLUMN, SOURCE_ID_COLUMN),
        };
        let scoped = |op: PhysOp, a: &str| {
            if !f.has_scope {
                return op;
            }
            let eq = on(&[&format!("{a}.traversal_path = {SCOPE_CTE}.traversal_path")])[0].clone();
            op.semi(scan(SCOPE_CTE, SCOPE_CTE, Dedup::None), eq)
        };
        // A specific first-hop filter wins; otherwise the general rel_type
        // filter keeps e1 from going unfiltered.
        let first_types = if f.first_hop_types.is_empty() {
            f.rel_types
        } else {
            f.first_hop_types
        };
        let edge = |a: &str| -> PhysOp {
            let base = self.edge_scan(f.tables, a, |_| vec![]);
            scoped(
                if a == "e1" {
                    f.anchor.apply(base, a, anchor_col)
                } else {
                    base
                },
                a,
            )
        };
        let hop_preds = |a: &str, first: bool| -> Vec<PExpr> {
            let mut p: Vec<PExpr> = rel_kind(a, if first { first_types } else { f.rel_types })
                .into_iter()
                .collect();
            if first {
                let kind = kind_col(anchor_col);
                p.push(pe!("{a}.{kind} = {:?}", f.anchor_entity));
                p.extend(f.anchor_tags.iter().cloned());
            }
            p.push(deleted_false(a));
            p
        };
        let hop_on = |prev: &str, curr: &str| -> Vec<(Col, Col)> {
            if f.include_tp {
                on(&[&format!("{prev}.traversal_path = {curr}.traversal_path")])
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
            pe!("arrayResize([tuple(0, '')], 0)")
        } else {
            path_nodes(nodes, next_col)
        };
        let kinds: Vec<String> = (1..=depth)
            .map(|i| format!("e{i}.relationship_kind"))
            .collect();
        let next_kind = kind_col(next_col);
        let mut columns = vec![
            pn!("e1.{anchor_col} AS anchor_id"),
            pn!("{last}.{next_col} AS end_id"),
            pn!("{last}.{next_kind} AS end_kind"),
            named(path, PATH_NODES_COLUMN),
            pn!("[{}] AS edge_kinds", kinds.join(", ")),
            pn!("{depth} AS depth"),
        ];
        if f.include_tp {
            columns.push(pn!("e1.traversal_path AS traversal_path"));
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
        group_by: vec![pn!("{a}.traversal_path AS traversal_path")],
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
