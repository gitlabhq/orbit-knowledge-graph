//! `PlanCtx`: the query being planned, plus the scans and predicates every
//! shape builds the same way.

use super::prelude::*;

pub struct PlanCtx<'a> {
    pub input: &'a Input,
    pub graph: &'a JoinGraph,
}

// ── Scans ─────────────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    pub fn node(&self, alias: &str) -> Option<&'a InputNode> {
        self.input.nodes.iter().find(|n| n.id == alias)
    }

    /// Latest-row scan of a node table with its user filters and pins.
    pub fn node_scan(&self, n: &InputNode) -> PhysOp {
        let a = n.id.as_str();
        let mut p = node_filters(a, &n.filters);
        if !n.node_ids.is_empty() {
            p.push(id_in(a, DEFAULT_PRIMARY_KEY, &n.node_ids));
        }
        if let Some(ref r) = n.id_range {
            p.push(id_range(a, DEFAULT_PRIMARY_KEY, r));
        }
        p.push(deleted_false(a));
        scan(n.table.as_deref().unwrap_or(""), a, Dedup::Final).filter(p)
    }

    /// Scan of one or more physical edge tables under one alias. Several
    /// tables union on the reserved edge columns so extra per-table columns
    /// don't break the union; `arm_where` is pushed into each arm.
    pub fn edge_scan(
        &self,
        tables: &[String],
        alias: &str,
        arm_where: impl Fn(&str) -> Vec<PExpr>,
    ) -> PhysOp {
        if let [table] = tables {
            return scan(table, alias, Dedup::None).filter(arm_where(alias));
        }
        let inner = format!("_{alias}");
        let arms = tables
            .iter()
            .map(|t| {
                let cols = EDGE_RESERVED_COLUMNS
                    .iter()
                    .chain([&DELETED_COLUMN])
                    .map(|c| pn!("{inner}.{c} AS {c}"))
                    .collect();
                scan(t, &inner, Dedup::None)
                    .filter(arm_where(&inner))
                    .project(cols)
            })
            .collect();
        PhysOp::union(arms, alias)
    }

    /// `node.property`, truncated to an excerpt when the ontology marks the
    /// column as long text.
    pub fn property(&self, n: &InputNode, property: &str) -> PExpr {
        let (a, max) = (&n.id, n.excerpt_max_chars);
        if !n.excerpt_columns.contains(property) || max == 0 {
            return pe!("{a}.{property}");
        }
        let excerpt = format!("substringUTF8({a}.{property}, 1, {max})");
        pe!("concat({excerpt}, if(length({a}.{property}) > length({excerpt}), ' [truncated]', ''))")
    }
}

// ── Predicates ────────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    /// Tag predicates on `edge` for every filter of `node` that the edge
    /// denormalizes on its `dir` ("source"/"target") side. With `rel_types`,
    /// only tags those relationship kinds actually write.
    pub fn denorm_tags(
        &self,
        node: &InputNode,
        dir: &str,
        edge: &str,
        rel_types: Option<&[String]>,
    ) -> Vec<PExpr> {
        let meta = &self.input.compiler;
        let entity = node.entity.clone().unwrap_or_default();
        let mut props: Vec<_> = node.filters.iter().collect();
        props.sort_unstable_by_key(|(k, _)| *k);
        let mut out = Vec::new();
        for (prop, fs) in props {
            let key = (entity.clone(), prop.clone(), dir.to_string());
            if let Some(types) = rel_types {
                let writes = meta
                    .denorm_rel_kinds
                    .get(&key)
                    .is_some_and(|ks| types.iter().any(|t| ks.contains(t)));
                if !writes {
                    continue;
                }
            }
            let Some((tag_col, tag_key)) = meta.denormalized_columns.get(&key) else {
                continue;
            };
            out.extend(
                fs.iter()
                    .filter_map(|f| denorm_tag(edge, tag_col, tag_key, f)),
            );
        }
        out
    }

    /// Whether `prop` of `node` is carried as a denorm tag by some edge
    /// adjacent to the node on the given relationship.
    pub fn denorm_covers(&self, node: &InputNode, prop: &str, rel: &InputRelationship) -> bool {
        let (sc, ec) = rel.direction.edge_columns();
        [(&rel.from, sc), (&rel.to, ec)].into_iter().any(|(n, ic)| {
            let dir = if ic == SOURCE_ID_COLUMN {
                "source"
            } else {
                "target"
            };
            let key = (
                node.entity.clone().unwrap_or_default(),
                prop.to_string(),
                dir.to_string(),
            );
            n == &node.id
                && self.input.compiler.denormalized_columns.contains_key(&key)
                && self
                    .input
                    .compiler
                    .denorm_rel_kinds
                    .get(&key)
                    .is_some_and(|ks| rel.types.iter().any(|t| ks.contains(t)))
        })
    }
}

impl<'a> PlanCtx<'a> {
    pub fn sort_keys(&self) -> Vec<(PExpr, bool)> {
        self.input
            .order_by
            .as_ref()
            .map(|ob| {
                let desc = matches!(ob.direction, OrderDirection::Desc);
                vec![(pe!("{}.{}", ob.node, ob.property), desc)]
            })
            .unwrap_or_default()
    }

    /// First edge column that carries each node's id, in relationship order.
    pub fn bindings(&self) -> HashMap<String, Col> {
        let mut bound = HashMap::new();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let (sc, ec) = rel.direction.edge_columns();
            for (n, c) in [(&rel.from, sc), (&rel.to, ec)] {
                bound
                    .entry(n.clone())
                    .or_insert_with(|| (ea.clone(), c.to_string()));
            }
        }
        bound
    }
}

// ── Result routing ────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    /// Where `enforce` reads each node's id from: its own scan when joined,
    /// otherwise the edge column that carries it.
    pub fn node_edge_mappings(&self, op: &PhysOp) -> HashMap<String, (String, String)> {
        if self.input.query_type == QueryType::Neighbors {
            let center = &self.input.nodes[0].id;
            return HashMap::from([(center.clone(), ("e".into(), SOURCE_ID_COLUMN.into()))]);
        }
        let visible = op.visible_aliases();
        let bound = self.bindings();
        self.input
            .nodes
            .iter()
            .filter_map(|n| {
                if visible.contains(&n.id) {
                    return Some((n.id.clone(), (n.id.clone(), DEFAULT_PRIMARY_KEY.into())));
                }
                let b = bound.get(&n.id)?;
                visible.contains(&b.0).then(|| (n.id.clone(), b.clone()))
            })
            .collect()
    }
}
