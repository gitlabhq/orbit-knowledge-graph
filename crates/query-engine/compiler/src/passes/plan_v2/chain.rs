//! Traversal and aggregation: the query types the optimizer works on.
//!
//! The naive plan is one left-deep join over every edge scan, then every node
//! table something reads:
//!
//! ```sql
//! SELECT e0.source_id AS e0_src, ..., g.name AS g_name
//! FROM gl_edge e0                                   -- one scan per relationship
//! JOIN gl_edge e1 ON e0.target_id = e1.source_id    -- joined where they share a node
//! JOIN gl_group g  ON e0.target_id = g.id           -- node tables only when read
//! WHERE e0.relationship_kind = 'MEMBER_OF' AND ...
//! LIMIT 11
//! ```
//!
//! `plan_chain_query` builds the naive plan. `optimize_v2` rewrites it for
//! ClickHouse before lowering.

use super::prelude::*;

/// Edge columns a traversal returns per hop, with their output suffixes.
const EDGE_OUTPUT: [(&str, &str); 5] = [
    (RELATIONSHIP_KIND_COLUMN, crate::constants::EDGE_TYPE_SUFFIX),
    (SOURCE_ID_COLUMN, crate::constants::EDGE_SRC_SUFFIX),
    (SOURCE_KIND_COLUMN, crate::constants::EDGE_SRC_TYPE_SUFFIX),
    (TARGET_ID_COLUMN, crate::constants::EDGE_DST_SUFFIX),
    (TARGET_KIND_COLUMN, crate::constants::EDGE_DST_TYPE_SUFFIX),
];

// ── Entry ─────────────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    pub fn plan_chain_query(&self, limit: u32) -> PhysOp {
        match self.input.query_type {
            QueryType::Traversal => self.plan_traversal(limit),
            _ => self.plan_aggregation(limit),
        }
    }
}

// ── Traversal ─────────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    fn plan_traversal(&self, limit: u32) -> PhysOp {
        let mut columns = Vec::new();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let prefix = if rel.hops.max > 1 {
                format!("hop_{ea}")
            } else {
                ea.clone()
            };
            for (c, suffix) in EDGE_OUTPUT {
                columns.push(pn!("{ea}.{c} AS {prefix}_{suffix}"));
            }
            if rel.hops.max > 1 {
                columns.push(pn!("{ea}.path_nodes AS {prefix}_path_nodes"));
            }
        }
        for n in &self.input.nodes {
            columns.extend(self.requested(n));
        }
        let mut keys = self.sort_keys();
        if self.input.cursor.is_some() {
            keys.extend(self.traversal_tie_breakers());
        }
        self.plan_chain().sort(keys).project(columns).limit(limit)
    }

    pub fn project_joined_columns(&self, op: PhysOp) -> PhysOp {
        let PhysOp::Limit { input, count } = op else {
            return op;
        };
        let PhysOp::Project { input, mut columns } = *input else {
            return input.limit(count);
        };
        let joined = input.visible_aliases();
        for n in self.input.nodes.iter().filter(|n| joined.contains(&n.id)) {
            for c in self.requested(n) {
                if !columns.contains(&c) {
                    columns.push(c);
                }
            }
        }
        input.project(columns).limit(count)
    }

    pub fn defer_hydration_columns(&self, op: PhysOp) -> PhysOp {
        let PhysOp::Limit { input, count } = op else {
            return op;
        };
        let PhysOp::Project { input, mut columns } = *input else {
            return input.limit(count);
        };
        columns.retain(|(_, alias)| {
            !self.input.nodes.iter().any(|node| {
                !self.reads_node(node) && alias.starts_with(&format!("{}_", node.id))
            })
        });
        input.project(columns).limit(count)
    }

    /// Completes the sort into a total order for keyset pagination: each
    /// edge's id pair, or the node's own id when there are no edges.
    fn traversal_tie_breakers(&self) -> Vec<(PExpr, bool)> {
        if self.input.relationships.is_empty() {
            return vec![(pe!("{}.id", self.input.nodes[0].id), false)];
        }
        (0..self.input.relationships.len())
            .flat_map(|i| {
                [
                    (pe!("e{i}.source_id"), false),
                    (pe!("e{i}.target_id"), false),
                ]
            })
            .collect()
    }
}

// ── Aggregation ───────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    fn plan_aggregation(&self, limit: u32) -> PhysOp {
        let agg = &self.input.aggregation;
        let mut group_by = Vec::new();
        for g in &agg.group_by {
            match g {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    ..
                } => {
                    let expr = match truncate {
                        None => pe!("{node}.{property}"),
                        Some(unit @ (TruncateUnit::Minute | TruncateUnit::Hour)) => {
                            pe!("{}({node}.{property})", unit.ch_function())
                        }
                        Some(unit) => pe!("toDate32({}({node}.{property}))", unit.ch_function()),
                    };
                    group_by.push(named(expr, g.output_name()));
                }
                InputGroupByKey::Node { node, .. } => {
                    if let Some(n) = self.node(node) {
                        group_by.extend(self.requested(n));
                    }
                }
            }
        }
        let metrics = agg
            .metrics
            .iter()
            .map(|m| {
                let expr = match (m.expr.function(), m.expr.property()) {
                    (AggFunction::Count, _) | (_, None) => pe!("COUNT()"),
                    (f, Some(p)) => pe!("{}({}.{p})", f.as_sql(), m.expr.node()),
                };
                named(expr, m.output_name())
            })
            .collect();
        let mut keys = Vec::new();
        if let Some(ref s) = agg.sort {
            let desc = matches!(s.direction, OrderDirection::Desc);
            keys.push((PExpr::Ident(s.column.clone()), desc));
        }
        if self.input.cursor.is_some() {
            // The group-key tuple is unique per result row, so it completes
            // the sort into a total order the keyset seek can anchor on.
            keys.extend(group_by.iter().map(|(e, _)| (e.clone(), false)));
        }
        PhysOp::Aggregate {
            input: Box::new(self.plan_chain()),
            group_by,
            metrics,
        }
        .sort(keys)
        .limit(limit)
    }
}

// ── The join chain ────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    /// Each edge joins on the columns of nodes an earlier edge already binds,
    /// so star and cycle patterns get the right conditions.
    fn plan_chain(&self) -> PhysOp {
        if self.input.relationships.is_empty() {
            return self.node_scan(&self.input.nodes[0]);
        }
        let det = &self.input.compiler.default_edge_table;
        let mut bound: HashMap<String, Col> = HashMap::new();
        let mut tree: Option<PhysOp> = None;
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let (sc, ec) = rel.direction.edge_columns();
            let table = self.graph.edge_table(&rel.types, det);
            let leaf = if rel.hops.max > 1 {
                self.multi_hop(rel, &ea, &table)
            } else {
                scan(&table, &ea, Dedup::None).filter(self.edge_preds(rel, &ea))
            };
            let mut on = Vec::new();
            for (n, c) in [(&rel.from, sc), (&rel.to, ec)] {
                let here = (ea.clone(), c.to_string());
                match bound.get(n) {
                    Some(b) => on.push((b.clone(), here)),
                    None => {
                        bound.insert(n.clone(), here);
                    }
                }
            }
            tree = Some(match tree {
                None => leaf,
                Some(t) => t.join(leaf, on),
            });
        }
        let mut tree = tree.unwrap();
        for n in &self.input.nodes {
            if let Some(b) = bound.get(&n.id)
                && self.needs_node_join(n)
            {
                tree = tree.join(
                    self.node_scan(n),
                    vec![(b.clone(), (n.id.clone(), "id".into()))],
                );
            }
        }
        tree
    }

    /// `UNION ALL` of one arm per depth, aliased as the hop's edge so the
    /// outer query reads it like a single edge with a `path_nodes` column.
    fn multi_hop(&self, rel: &InputRelationship, alias: &str, table: &str) -> PhysOp {
        let cols = rel.direction.edge_columns();
        let (sc, ec) = cols;
        let edge = |a: &str| scan(table, a, Dedup::None);
        let hop_preds = |a: &str, first: bool| {
            let mut p: Vec<PExpr> = rel_kind(a, &rel.types).into_iter().collect();
            p.push(deleted_false(a));
            p.extend(node_filters(a, &rel.filters));
            if first && let Some(ref pfx) = rel.scope_prefix {
                p.push(PExpr::Scope(a.to_string(), pfx.clone()));
            }
            p
        };
        let arms = (rel.hops.min.max(1)..=rel.hops.max)
            .map(|depth| {
                let last = format!("e{depth}");
                // Arms must agree on shape: first-hop start + last-hop end,
                // the reserved kind and tag columns the outer filter reads,
                // and the first edge's kind/tp/deleted.
                let (sk, ek) = (kind_col(sc), kind_col(ec));
                let columns = vec![
                    pn!("e1.traversal_path AS traversal_path"),
                    pn!("e1.relationship_kind AS relationship_kind"),
                    pn!("e1.{sc} AS {sc}"),
                    pn!("e1.{sk} AS {sk}"),
                    pn!("{last}.{ec} AS {ec}"),
                    pn!("{last}.{ek} AS {ek}"),
                    pn!("e1.source_tags AS source_tags"),
                    pn!("{last}.target_tags AS target_tags"),
                    named(
                        path_nodes(1..=depth, ec),
                        crate::constants::PATH_NODES_COLUMN,
                    ),
                    pn!("{depth} AS depth"),
                    pn!("e1._deleted AS _deleted"),
                ];
                hop_chain(&edge, depth, cols, &hop_preds, &|_, _| vec![]).project(columns)
            })
            .collect();

        let mut outer = Vec::new();
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            let Some(n) = self.node(nid) else { continue };
            if let Some(ref e) = n.entity {
                outer.push(pe!("{alias}.{} = {e:?}", kind_col(ic)));
            }
            if !n.node_ids.is_empty() {
                outer.push(id_in(alias, ic, &n.node_ids));
            }
        }
        outer.push(deleted_false(alias));
        PhysOp::union(arms, alias).filter(outer)
    }

    /// Predicates on a single-hop edge scan: kinds, endpoint entity kinds,
    /// relationship filters, scope, and endpoint id pins.
    fn edge_preds(&self, rel: &InputRelationship, a: &str) -> Vec<PExpr> {
        let (sc, ec) = rel.direction.edge_columns();
        let mut p: Vec<PExpr> = rel_kind(a, &rel.types).into_iter().collect();
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(ent) = self.node(nid).and_then(|n| n.entity.as_deref()) {
                p.push(pe!("{a}.{} = {ent:?}", kind_col(ic)));
            }
        }
        p.push(deleted_false(a));
        p.extend(node_filters(a, &rel.filters));
        if let Some(ref pfx) = rel.scope_prefix {
            p.push(PExpr::Scope(a.to_string(), pfx.clone()));
        }
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            let Some(n) = self.node(nid) else { continue };
            if !n.node_ids.is_empty() {
                p.push(id_in(a, ic, &n.node_ids));
            }
            if let Some(ref r) = n.id_range {
                p.push(id_range(a, ic, r));
            }
        }
        p
    }

    fn needs_node_join(&self, node: &InputNode) -> bool {
        node.id_range.is_some()
            || self.reads_node(node)
            || !crate::passes::shared::requested_columns(&node.columns).is_empty()
    }

    /// Something reads the node table: property filters, group-by, a
    /// non-count metric, or order-by.
    pub fn reads_node(&self, node: &InputNode) -> bool {
        let a = node.id.as_str();
        let agg = &self.input.aggregation;
        !node.filters.is_empty()
            || agg.group_by.iter().any(|g| g.node() == a)
            || agg.metrics.iter().any(|m| {
                m.expr.node() == a
                    && m.expr.property().is_some()
                    && !matches!(m.expr.function(), AggFunction::Count)
            })
            || self.input.order_by.as_ref().is_some_and(|ob| ob.node == a)
    }

    fn requested(&self, n: &InputNode) -> Vec<Named> {
        crate::passes::shared::requested_columns(&n.columns)
            .into_iter()
            .map(|p| named(self.property(n, &p), format!("{}_{p}", n.id)))
            .collect()
    }
}
