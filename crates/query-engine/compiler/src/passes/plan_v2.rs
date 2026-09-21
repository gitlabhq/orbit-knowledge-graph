use std::collections::{HashMap, HashSet};

use ontology::constants::*;
use ontology::Ontology;

use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::normalize::is_wildcard;
use crate::passes::shared::{
    deleted_false, denorm_tag_expr, edge_select_columns, edge_select_columns_with_prefix,
    filter_to_expr, id_list_predicate, id_range_predicate, rel_kind_filter, rel_kind_filter_values,
    requested_columns,
};

// ── Join Graph ──────────────────────────────────────────────────────────────

pub struct JoinGraph {
    by_kind: HashMap<String, JoinPath>,
}

#[derive(Clone)]
pub struct JoinPath {
    pub fk_column: Option<String>,
    pub scope_preserving: bool,
    pub edge_table: String,
}

pub enum HopStrategy {
    EdgeScan { table: String, dedup: bool },
    FkJoin { fk_column: String },
}

impl JoinGraph {
    pub fn build(ontology: &Ontology) -> Self {
        let mut by_kind = HashMap::new();
        for edge in ontology.edges() {
            by_kind.entry(edge.relationship_kind.clone()).or_insert(JoinPath {
                fk_column: edge.fk_column.clone(),
                scope_preserving: edge.scope.is_some_and(|s| s.is_scope_preserving()),
                edge_table: edge.destination_table.clone(),
            });
        }
        Self { by_kind }
    }

    pub fn resolve(&self, rel: &InputRelationship, default_table: &str, chain_len: usize) -> HopStrategy {
        if rel.hops.max == 1
            && !matches!(rel.direction, Direction::Both)
            && rel.filters.is_empty()
            && rel.fk_column.is_some()
        {
            return HopStrategy::FkJoin { fk_column: rel.fk_column.clone().unwrap() };
        }
        HopStrategy::EdgeScan {
            table: self.edge_table(&rel.types, default_table),
            dedup: chain_len >= 2 && rel.hops.max == 1,
        }
    }

    pub fn edge_table(&self, rel_types: &[String], default: &str) -> String {
        for t in rel_types {
            if let Some(jp) = self.by_kind.get(t) {
                return jp.edge_table.clone();
            }
        }
        default.to_string()
    }
}

// ── PhysOp ──────────────────────────────────────────────────────────────────

pub enum PhysOp {
    Scan { table: String, alias: String, dedup: bool, predicates: Vec<Expr>, select: Vec<SelectExpr> },
    Join { left: Box<PhysOp>, right: Box<PhysOp>, on: Expr },
    Union { arms: Vec<PhysOp>, alias: String },
    UnionQueries { arms: Vec<Query>, alias: String, outer_predicates: Vec<Expr> },
    Cte { name: String, body: Box<PhysOp>, consumer: Box<PhysOp> },
    TopN { input: Box<PhysOp>, select: Vec<SelectExpr>, order_by: Vec<OrderExpr>, limit: u32 },
    Aggregate { input: Box<PhysOp>, select: Vec<SelectExpr>, group_by: Vec<Expr>, order_by: Vec<OrderExpr>, limit: u32 },
}

impl PhysOp {
    pub fn shape(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            PhysOp::Scan { table, alias, dedup, .. } => json!({
                "op": "Scan",
                "table": table,
                "alias": alias,
                "dedup": dedup,
            }),
            PhysOp::Join { left, right, .. } => json!({
                "op": "Join",
                "left": left.shape(),
                "right": right.shape(),
            }),
            PhysOp::Union { arms, alias } => json!({
                "op": "Union",
                "alias": alias,
                "arms": arms.iter().map(|a| a.shape()).collect::<Vec<_>>(),
            }),
            PhysOp::UnionQueries { alias, arms, .. } => json!({
                "op": "UnionQueries",
                "alias": alias,
                "arm_count": arms.len(),
            }),
            PhysOp::Cte { name, body, consumer } => json!({
                "op": "Cte",
                "name": name,
                "body": body.shape(),
                "consumer": consumer.shape(),
            }),
            PhysOp::TopN { input, limit, .. } => json!({
                "op": "TopN",
                "limit": limit,
                "input": input.shape(),
            }),
            PhysOp::Aggregate { input, limit, .. } => json!({
                "op": "Aggregate",
                "limit": limit,
                "input": input.shape(),
            }),
        }
    }
}

#[derive(Default)]
pub struct PlanMetadata {
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub hop_count: usize,
    pub phys_op: Option<PhysOp>,
}

// ── Entry point ─────────────────────────────────────────────────────────────

pub fn plan(input: &mut Input, ontology: &Ontology) -> Result<(PlanMetadata, PhysOp)> {
    if input.compiler.table_sort_keys.is_empty() {
        for node in ontology.nodes() {
            input.compiler.table_sort_keys
                .insert(node.destination_table.clone(), node.sort_key.clone());
        }
    }

    let graph = JoinGraph::build(ontology);
    let ctx = PlanCtx { input, graph: &graph, ontology };
    let limit = input.fetch_limit();

    let op = match input.query_type {
        QueryType::Traversal => ctx.plan_traversal(limit),
        QueryType::Aggregation => ctx.plan_aggregation(limit),
        QueryType::Neighbors => ctx.plan_neighbors(limit),
        QueryType::PathFinding => ctx.plan_pathfinding(limit)?,
        QueryType::Hydration => ctx.plan_hydration(limit),
    };

    let mut nem = ctx.compute_node_edge_mappings();
    if input.query_type == QueryType::Neighbors {
        nem.insert(input.nodes[0].id.clone(), ("e".to_string(), SOURCE_ID_COLUMN.to_string()));
    }

    let meta = PlanMetadata {
        node_edge_mappings: nem,
        hop_count: input.relationships.len(),
        phys_op: None,
    };

    Ok((meta, op))
}

struct PlanCtx<'a> {
    input: &'a Input,
    graph: &'a JoinGraph,
    ontology: &'a Ontology,
}

impl<'a> PlanCtx<'a> {
    fn compute_node_edge_mappings(&self) -> HashMap<String, (String, String)> {
        let mut m = HashMap::new();
        let det = &self.input.compiler.default_edge_table;
        let cl = self.input.relationships.len();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            match self.graph.resolve(rel, det, cl) {
                HopStrategy::FkJoin { fk_column } => {
                    let (fk_alias, tgt_alias) = self.fk_sides(rel, &fk_column);
                    m.entry(fk_alias.to_string())
                        .or_insert_with(|| (fk_alias.to_string(), DEFAULT_PRIMARY_KEY.to_string()));
                    m.entry(tgt_alias.to_string())
                        .or_insert_with(|| (fk_alias.to_string(), fk_column));
                }
                HopStrategy::EdgeScan { .. } => {
                    let ea = format!("e{i}");
                    let (sc, ec) = rel.direction.edge_columns();
                    m.entry(rel.from.clone()).or_insert_with(|| (ea.clone(), sc.to_string()));
                    m.entry(rel.to.clone()).or_insert_with(|| (ea.clone(), ec.to_string()));
                }
            }
        }
        m
    }

    fn fk_sides<'r>(&self, rel: &'r InputRelationship, fk_col: &str) -> (&'r str, &'r str) {
        let from_has = self.input.nodes.iter()
            .find(|n| n.id == rel.from)
            .and_then(|n| n.table.as_deref())
            .and_then(|t| self.input.compiler.table_columns.get(t))
            .is_some_and(|cols| cols.contains(fk_col));
        if from_has { (&rel.from, &rel.to) } else { (&rel.to, &rel.from) }
    }

    fn node_scan(&self, node: &InputNode) -> PhysOp {
        PhysOp::Scan {
            table: node.table.as_deref().unwrap_or("").to_string(),
            alias: node.id.clone(),
            dedup: true,
            predicates: self.node_predicates(&node.id, node),
            select: vec![SelectExpr::star()],
        }
    }

    fn node_predicates(&self, alias: &str, node: &InputNode) -> Vec<Expr> {
        let mut p = Vec::new();
        let mut props: Vec<_> = node.filters.iter().collect();
        props.sort_unstable_by_key(|(k, _)| *k);
        for (prop, fs) in props {
            for f in fs { p.push(filter_to_expr(alias, prop, f)); }
        }
        if !node.node_ids.is_empty() {
            p.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &node.node_ids));
        }
        if let Some(ref r) = node.id_range { p.push(id_range_predicate(alias, r)); }
        p.push(deleted_false(alias));
        p
    }

    fn edge_predicates(&self, alias: &str, rel: &InputRelationship) -> Vec<Expr> {
        let mut p = Vec::new();
        let (sc, ec) = rel.direction.edge_columns();
        if let Some(f) = rel_kind_filter(alias, &rel.types) { p.push(f); }
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                if let Some(ref ent) = n.entity {
                    let kc = if ic == SOURCE_ID_COLUMN { SOURCE_KIND_COLUMN } else { TARGET_KIND_COLUMN };
                    p.push(Expr::eq(Expr::col(alias, kc), Expr::string(ent)));
                }
            }
        }
        p.push(deleted_false(alias));
        if let Some(ref pfx) = rel.scope_prefix { p.push(pfx.predicate(alias)); }
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                if !n.node_ids.is_empty() { p.push(id_list_predicate(alias, ic, &n.node_ids)); }
                if let Some(ref r) = n.id_range {
                    p.push(Expr::and(
                        Expr::binary(Op::Ge, Expr::col(alias, ic), Expr::int(r.start)),
                        Expr::binary(Op::Le, Expr::col(alias, ic), Expr::int(r.end)),
                    ));
                }
            }
        }
        let meta = &self.input.compiler;
        let et = self.graph.edge_table(&rel.types, &meta.default_edge_table);
        if let Some(ecols) = meta.table_columns.get(&et) {
            let reserved: HashSet<&str> = EDGE_RESERVED_COLUMNS.iter().copied().collect();
            let mut seen: HashSet<&str> = HashSet::new();
            for nid in [&rel.from, &rel.to] {
                if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                    for (prop, fs) in &n.filters {
                        if ecols.contains(prop) && !reserved.contains(prop.as_str()) && seen.insert(prop.as_str()) {
                            for f in fs { p.push(filter_to_expr(alias, prop, f)); }
                        }
                    }
                }
            }
        }
        if !is_wildcard(&rel.types) {
            for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
                if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                    let ent = n.entity.as_deref().unwrap_or("");
                    let dir = if ic == SOURCE_ID_COLUMN { "source" } else { "target" };
                    for (prop, fs) in &n.filters {
                        let key = (ent.to_string(), prop.clone(), dir.to_string());
                        if !meta.denorm_rel_kinds.get(&key).is_some_and(|ks| rel.types.iter().any(|t| ks.contains(t))) { continue; }
                        if let Some((tc, tk)) = meta.denormalized_columns.get(&key) {
                            for f in fs { if let Some(e) = denorm_tag_expr(alias, tc, tk, f) { p.push(e); } }
                        }
                    }
                }
            }
        }
        p
    }

    fn text_excerpt_expr(&self, alias: &str, col: &str) -> Expr {
        let value = Expr::col(alias, col);
        let node = self.ontology.get_node(
            self.input.nodes.iter().find(|n| n.id == alias).and_then(|n| n.entity.as_deref()).unwrap_or("")
        );
        let is_excerpt = node.is_some_and(|n| {
            n.fields.iter().any(|f| f.name == col && f.column_name().is_some() && f.data_type == ontology::DataType::String)
                && !n.fields.iter().any(|f| matches!(&f.source, ontology::FieldSource::Virtual(v) if v.depends_on.contains(&col.to_string())))
        });
        if !is_excerpt { return value; }
        let limit = self.input.fetch_limit();
        let max_chars = (8 * 1024 * 1024u32 / 4 / limit.max(1)) as u32;
        let excerpt = Expr::func("substringUTF8", vec![value.clone(), Expr::lit(1), Expr::lit(max_chars)]);
        let shortened = Expr::binary(Op::Gt, Expr::func("length", vec![value]), Expr::func("length", vec![excerpt.clone()]));
        Expr::func("concat", vec![excerpt, Expr::func("if", vec![shortened, Expr::string(" [truncated]"), Expr::string("")])])
    }

    fn node_select(&self, alias: &str, node: &InputNode) -> Vec<SelectExpr> {
        requested_columns(&node.columns).into_iter()
            .map(|col| SelectExpr::new(self.text_excerpt_expr(alias, &col), format!("{alias}_{col}")))
            .collect()
    }

    fn needs_node_join(&self, node: &InputNode) -> bool {
        let a = &node.id;
        let input = self.input;
        let has_filters = !node.filters.is_empty() || !node.node_ids.is_empty() || node.id_range.is_some();
        let in_group_by = input.aggregation.group_by.iter().any(|g| g.node() == a.as_str());
        let in_agg_prop = input.aggregation.metrics.iter().any(|m| {
            m.expr.node() == a.as_str() && m.expr.property().is_some()
                && !matches!(m.expr.function(), AggFunction::Count)
        });
        let in_order_by = input.order_by.as_ref().is_some_and(|ob| ob.node == *a);
        let has_columns = matches!(&node.columns, Some(ColumnSelection::List(c)) if !c.is_empty());

        if input.query_type == QueryType::Aggregation {
            has_filters || in_group_by || in_agg_prop || in_order_by
        } else {
            has_filters || has_columns || in_order_by || in_group_by || in_agg_prop
        }
    }

    fn needs_node_join_fk(&self, alias: &str) -> bool {
        let Some(node) = self.input.nodes.iter().find(|n| n.id == alias) else { return false };
        if self.input.query_type != QueryType::Aggregation {
            return true;
        }
        if self.needs_node_join(node) {
            return true;
        }
        self.input.aggregation.metrics.iter().any(|m| m.expr.node() == alias)
    }

    fn order_by_exprs(&self) -> Vec<OrderExpr> {
        self.input.order_by.as_ref().map(|ob| vec![
            if matches!(ob.direction, OrderDirection::Desc) { OrderExpr::desc(Expr::col(&ob.node, &ob.property)) }
            else { OrderExpr::asc(Expr::col(&ob.node, &ob.property)) }
        ]).unwrap_or_default()
    }
}

// ── plan_traversal ──────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    fn plan_traversal(&self, limit: u32) -> PhysOp {
        let chain = self.plan_chain();
        let mut select = Vec::new();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            if rel.hops.max > 1 {
                let prefix = format!("hop_{ea}");
                select.extend(edge_select_columns_with_prefix(&ea, &prefix));
                select.push(SelectExpr::new(Expr::col(&ea, PATH_NODES_COLUMN), format!("{prefix}_{PATH_NODES_COLUMN}")));
            } else {
                select.extend(edge_select_columns(&ea));
            }
        }
        for node in &self.input.nodes {
            select.extend(self.node_select(&node.id, node));
        }
        PhysOp::TopN { input: Box::new(chain), select, order_by: self.order_by_exprs(), limit }
    }

    fn plan_aggregation(&self, limit: u32) -> PhysOp {
        let chain = self.plan_chain();
        let mut select = Vec::new();
        let mut group_by = Vec::new();

        let names: Vec<String> = self.input.aggregation.group_by.iter().enumerate().map(|(_, k)| match k {
            InputGroupByKey::Property { alias: Some(a), .. } => a.clone(),
            InputGroupByKey::Property { node, property, .. } => format!("{node}_{property}"),
            InputGroupByKey::Node { alias: Some(a), .. } => a.clone(),
            InputGroupByKey::Node { node, .. } => node.clone(),
        }).collect();
        for (g, name) in self.input.aggregation.group_by.iter().zip(names) {
            match g {
                InputGroupByKey::Property { node, property, truncate, .. } => {
                    let col = Expr::col(node, property);
                    let expr = match truncate {
                        Some(unit) => {
                            let tr = Expr::func(unit.ch_function(), vec![col]);
                            match unit {
                                TruncateUnit::Minute | TruncateUnit::Hour => Expr::func("toDateTime64", vec![tr, Expr::ident("0")]),
                                _ => Expr::func("toDate32", vec![tr]),
                            }
                        }
                        None => col,
                    };
                    select.push(SelectExpr::new(expr.clone(), name));
                    if !group_by.contains(&expr) { group_by.push(expr); }
                }
                InputGroupByKey::Node { node, .. } => {
                    let cols = self.input.nodes.iter().find(|n| &n.id == node)
                        .map(|n| requested_columns(&n.columns)).unwrap_or_default();
                    for c in cols {
                        let expr = Expr::col(node, &c);
                        if !group_by.contains(&expr) { group_by.push(expr); }
                    }
                }
            }
        }
        for agg in &self.input.aggregation.metrics {
            let expr = match &agg.expr {
                AggExpr::Count(t) => match t.property.as_ref() {
                    Some(p) => Expr::func("COUNT", vec![Expr::col(&t.node, p)]),
                    None => Expr::func("COUNT", vec![]),
                },
                AggExpr::Sum(p) => Expr::func("SUM", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Avg(p) => Expr::func("AVG", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Min(p) => Expr::func("MIN", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Max(p) => Expr::func("MAX", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Collect(p) => Expr::func("groupArray", vec![Expr::col(&p.node, &p.property)]),
            };
            select.push(SelectExpr::new(expr, agg.output_name()));
        }
        let mut order_by = Vec::new();
        if let Some(ref sort) = self.input.aggregation.sort {
            order_by.push(if matches!(sort.direction, OrderDirection::Desc) {
                OrderExpr::desc(Expr::ident(&sort.column))
            } else { OrderExpr::asc(Expr::ident(&sort.column)) });
        }
        if self.input.cursor.is_some() {
            order_by.extend(group_by.iter().map(|e| OrderExpr::asc(e.clone())));
        }
        PhysOp::Aggregate { input: Box::new(chain), select, group_by, order_by, limit }
    }

    fn plan_chain(&self) -> PhysOp {
        if self.input.relationships.is_empty() {
            let n = &self.input.nodes[0];
            return PhysOp::Scan {
                table: n.table.as_deref().unwrap_or("").to_string(),
                alias: n.id.clone(), dedup: true,
                predicates: self.node_predicates(&n.id, n),
                select: vec![SelectExpr::star()],
            };
        }

        let mut tree: Option<PhysOp> = None;
        let det = &self.input.compiler.default_edge_table;
        let cl = self.input.relationships.len();
        let mut fk_joined: HashSet<String> = HashSet::new();

        let all_fk = cl >= 1 && self.input.relationships.iter().all(|r| {
            matches!(self.graph.resolve(r, det, cl), HopStrategy::FkJoin { .. })
        });

        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let (sc, _) = rel.direction.edge_columns();

            let strategy = if all_fk {
                self.graph.resolve(rel, det, cl)
            } else {
                match self.graph.resolve(rel, det, cl) {
                    HopStrategy::FkJoin { .. } => HopStrategy::EdgeScan {
                        table: self.graph.edge_table(&rel.types, det),
                        dedup: cl >= 2 && rel.hops.max == 1,
                    },
                    other => other,
                }
            };

            match strategy {
                HopStrategy::FkJoin { fk_column } => {
                    let (fk_alias, tgt_alias) = self.fk_sides(rel, &fk_column);
                    let needs_fk = self.needs_node_join_fk(fk_alias);
                    let needs_tgt = self.needs_node_join_fk(tgt_alias);
                    if tree.is_none() && needs_fk {
                        if let Some(n) = self.input.nodes.iter().find(|n| n.id == fk_alias) {
                            tree = Some(self.node_scan(n));
                            fk_joined.insert(fk_alias.to_string());
                        }
                    }
                    if !fk_joined.contains(tgt_alias) && needs_tgt {
                        if let Some(n) = self.input.nodes.iter().find(|n| n.id == tgt_alias) {
                            if tree.is_none() {
                                tree = Some(self.node_scan(n));
                            } else {
                                tree = Some(PhysOp::Join {
                                    left: Box::new(tree.unwrap()),
                                    right: Box::new(self.node_scan(n)),
                                    on: Expr::eq(Expr::col(fk_alias, &fk_column), Expr::col(tgt_alias, DEFAULT_PRIMARY_KEY)),
                                });
                            }
                            fk_joined.insert(tgt_alias.to_string());
                        }
                    }
                }
                HopStrategy::EdgeScan { table, dedup } => {
                    let edge = if rel.hops.max > 1 {
                        self.build_multi_hop_union(rel, &ea, &table)
                    } else {
                        PhysOp::Scan {
                            table, alias: ea.clone(), dedup,
                            predicates: self.edge_predicates(&ea, rel),
                            select: vec![],
                        }
                    };
                    tree = Some(match tree {
                        None => edge,
                        Some(prev) => {
                            let prev_col = if i > 0 {
                                let (_, pe) = self.input.relationships[i - 1].direction.edge_columns();
                                (format!("e{}", i - 1), pe.to_string())
                            } else { (ea.clone(), sc.to_string()) };
                            PhysOp::Join {
                                left: Box::new(prev), right: Box::new(edge),
                                on: Expr::eq(Expr::col(&prev_col.0, &prev_col.1), Expr::col(&ea, sc)),
                            }
                        }
                    });
                }
            }
        }

        let mut hydrated = fk_joined;
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let (sc, ec) = rel.direction.edge_columns();
            for (na, col) in [(&rel.from, sc), (&rel.to, ec)] {
                if !hydrated.insert(na.clone()) { continue; }
                let Some(n) = self.input.nodes.iter().find(|n| &n.id == na) else { continue; };
                if !self.needs_node_join(n) { continue; }
                tree = Some(PhysOp::Join {
                    left: Box::new(tree.unwrap()),
                    right: Box::new(self.node_scan(n)),
                    on: Expr::eq(Expr::col(na, DEFAULT_PRIMARY_KEY), Expr::col(&ea, col)),
                });
            }
        }

        tree.unwrap()
    }

    fn build_multi_hop_union(&self, rel: &InputRelationship, alias: &str, edge_table: &str) -> PhysOp {
        let (sc, ec) = rel.direction.edge_columns();
        let etc = match rel.direction {
            Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
            Direction::Incoming => SOURCE_KIND_COLUMN,
        };
        let tf = rel_kind_filter_values(&rel.types);
        let arms: Vec<Query> = (rel.hops.min.max(1)..=rel.hops.max)
            .map(|d| crate::passes::lower::helpers::build_depth_arm(
                d, edge_table, sc, ec, etc, rel.direction, &tf, rel.scope_prefix.as_ref(),
            ))
            .collect();

        let mut outer = Vec::new();
        let (fk, tk) = match rel.direction {
            Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
            Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
        };
        let (from_id_col, to_id_col) = rel.direction.edge_columns();
        for (na, kc, ic) in [(&rel.from, fk, from_id_col), (&rel.to, tk, to_id_col)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == na) {
                if let Some(ref e) = n.entity {
                    outer.push(Expr::eq(Expr::col(alias, kc), Expr::string(e)));
                }
                if !n.node_ids.is_empty() {
                    outer.push(id_list_predicate(alias, ic, &n.node_ids));
                }
                if let Some(ref r) = n.id_range {
                    outer.push(Expr::and(
                        Expr::binary(Op::Ge, Expr::col(alias, ic), Expr::int(r.start)),
                        Expr::binary(Op::Le, Expr::col(alias, ic), Expr::int(r.end)),
                    ));
                }
            }
        }
        outer.push(deleted_false(alias));

        PhysOp::UnionQueries { arms, alias: alias.to_string(), outer_predicates: outer }
    }
}

// ── plan_neighbors ──────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    fn plan_neighbors(&self, limit: u32) -> PhysOp {
        let config = self.input.neighbors.as_ref().expect("neighbors config");
        let center = &self.input.nodes[0];
        let center_entity = center.entity.as_deref().unwrap_or("");
        let et = self.graph.edge_table(&config.rel_types, &self.input.compiler.default_edge_table);

        let build_arm = |dir: Direction| -> PhysOp {
            let (center_col, center_kind, neighbor_id, neighbor_kind, is_out) = match dir {
                Direction::Outgoing => (SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN, 1i64),
                Direction::Incoming => (TARGET_ID_COLUMN, TARGET_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN, 0i64),
                Direction::Both => unreachable!(),
            };
            let ea = "e";
            let mut preds = vec![
                Expr::eq(Expr::col(ea, center_kind), Expr::string(center_entity)),
            ];
            if !center.node_ids.is_empty() {
                preds.push(id_list_predicate(ea, center_col, &center.node_ids));
            }
            if let Some(f) = rel_kind_filter(ea, &config.rel_types) { preds.push(f); }
            preds.push(deleted_false(ea));

            let denorm_dir = if dir == Direction::Outgoing { "source" } else { "target" };
            for (prop, fs) in &center.filters {
                let key = (center_entity.to_string(), prop.clone(), denorm_dir.to_string());
                if let Some((tc, tk)) = self.input.compiler.denormalized_columns.get(&key) {
                    for f in fs { if let Some(e) = denorm_tag_expr(ea, tc, tk, f) { preds.push(e); } }
                }
            }

            let select = vec![
                SelectExpr::new(Expr::col(ea, neighbor_id), neighbor_id_column()),
                SelectExpr::new(Expr::col(ea, neighbor_kind), neighbor_type_column()),
                SelectExpr::new(Expr::col(ea, RELATIONSHIP_KIND_COLUMN), relationship_type_column()),
                SelectExpr::new(Expr::int(is_out), neighbor_is_outgoing_column()),
                SelectExpr::new(Expr::col(ea, center_col), redaction_id_column(&center.id)),
                SelectExpr::new(Expr::string(center_entity), redaction_type_column(&center.id)),
            ];

            let edge_scan = PhysOp::Scan { table: et.clone(), alias: ea.to_string(), dedup: false, predicates: preds, select };

            let has_non_denorm = center.filters.iter().any(|(prop, _)| {
                let src = self.input.compiler.denormalized_columns.contains_key(
                    &(center_entity.to_string(), prop.clone(), "source".to_string()));
                let tgt = self.input.compiler.denormalized_columns.contains_key(
                    &(center_entity.to_string(), prop.clone(), "target".to_string()));
                !src && !tgt
            }) || center.id_range.is_some();

            if has_non_denorm {
                PhysOp::Join {
                    left: Box::new(edge_scan),
                    right: Box::new(self.node_scan(center)),
                    on: Expr::eq(Expr::col(ea, center_col), Expr::col(&center.id, DEFAULT_PRIMARY_KEY)),
                }
            } else {
                edge_scan
            }
        };

        let body = match config.direction {
            Direction::Both => PhysOp::Union {
                arms: vec![build_arm(Direction::Outgoing), build_arm(Direction::Incoming)],
                alias: "_neighbors".to_string(),
            },
            dir => build_arm(dir),
        };

        let order_by = self.order_by_exprs();
        PhysOp::TopN { input: Box::new(body), select: vec![], order_by, limit }
    }

    // ── plan_pathfinding ────────────────────────────────────────────────────

    fn plan_pathfinding(&self, limit: u32) -> Result<PhysOp> {
        let cfg = self.input.path.as_ref().expect("path config");
        let start = self.input.nodes.iter().find(|n| n.id == cfg.from).expect("start");
        let end = self.input.nodes.iter().find(|n| n.id == cfg.to).expect("end");
        let et = self.graph.edge_table(&cfg.rel_types, &self.input.compiler.default_edge_table);
        let max_depth = cfg.max_depth;
        let fwd_depth = max_depth / 2 + max_depth % 2;
        let bwd_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

        let _type_filter = rel_kind_filter_values(&cfg.rel_types);

        let frontier_arm = |depth: u32, dir_start: &str, dir_end: &str| -> PhysOp {
            let mut chain = PhysOp::Scan {
                table: et.clone(), alias: "e1".to_string(), dedup: false,
                predicates: vec![], select: vec![],
            };
            for j in 2..=depth {
                chain = PhysOp::Join {
                    left: Box::new(chain),
                    right: Box::new(PhysOp::Scan {
                        table: et.clone(), alias: format!("e{j}"), dedup: false,
                        predicates: vec![], select: vec![],
                    }),
                    on: Expr::eq(Expr::col(format!("e{}", j - 1), dir_end), Expr::col(format!("e{j}"), dir_start)),
                };
            }
            chain
        };

        let fwd_arms: Vec<PhysOp> = (1..=fwd_depth).map(|d| frontier_arm(d, SOURCE_ID_COLUMN, TARGET_ID_COLUMN)).collect();
        let fwd = if fwd_arms.len() == 1 { fwd_arms.into_iter().next().unwrap() }
                  else { PhysOp::Union { arms: fwd_arms, alias: "_fwd_union".to_string() } };

        let start_cte = PhysOp::Cte {
            name: "_nf_start".to_string(),
            body: Box::new(self.node_scan(start)),
            consumer: Box::new(PhysOp::Cte {
                name: "forward".to_string(),
                body: Box::new(fwd),
                consumer: Box::new(if bwd_depth > 0 {
                    let bwd_arms: Vec<PhysOp> = (1..=bwd_depth).map(|d| frontier_arm(d, TARGET_ID_COLUMN, SOURCE_ID_COLUMN)).collect();
                    let bwd = if bwd_arms.len() == 1 { bwd_arms.into_iter().next().unwrap() }
                              else { PhysOp::Union { arms: bwd_arms, alias: "_bwd_union".to_string() } };
                    PhysOp::Cte {
                        name: "_nf_end".to_string(),
                        body: Box::new(self.node_scan(end)),
                        consumer: Box::new(PhysOp::Cte {
                            name: "backward".to_string(),
                            body: Box::new(bwd),
                            consumer: Box::new(PhysOp::Union {
                                arms: vec![
                                    PhysOp::Scan { table: "forward".to_string(), alias: "f".to_string(), dedup: false, predicates: vec![], select: vec![] },
                                    PhysOp::Join {
                                        left: Box::new(PhysOp::Scan { table: "forward".to_string(), alias: "f".to_string(), dedup: false, predicates: vec![], select: vec![] }),
                                        right: Box::new(PhysOp::Scan { table: "backward".to_string(), alias: "b".to_string(), dedup: false, predicates: vec![], select: vec![] }),
                                        on: Expr::eq(Expr::col("f", "end_id"), Expr::col("b", "end_id")),
                                    },
                                ],
                                alias: "paths".to_string(),
                            }),
                        }),
                    }
                } else {
                    PhysOp::Scan { table: "forward".to_string(), alias: "f".to_string(), dedup: false, predicates: vec![], select: vec![] }
                }),
            }),
        };

        Ok(PhysOp::TopN { input: Box::new(start_cte), select: vec![], order_by: vec![], limit })
    }

    // ── plan_hydration ──────────────────────────────────────────────────────

    fn plan_hydration(&self, limit: u32) -> PhysOp {
        let arms: Vec<PhysOp> = self.input.nodes.iter().map(|n| {
            let mut preds = Vec::new();
            if !n.node_ids.is_empty() {
                preds.push(id_list_predicate(&n.id, &n.id_property, &n.node_ids));
            }
            preds.push(deleted_false(&n.id));

            let cols = match &n.columns {
                Some(ColumnSelection::List(cs)) => cs.clone(),
                _ => vec![],
            };
            let json_expr = if cols.is_empty() {
                Expr::string("{}")
            } else {
                let map_args: Vec<Expr> = cols.iter().flat_map(|c| [
                    Expr::string(c), Expr::func("toString", vec![Expr::col(&n.id, c)])
                ]).collect();
                Expr::func("toJSONString", vec![Expr::func("map", map_args)])
            };

            let select = vec![
                SelectExpr::new(Expr::col(&n.id, &n.id_property), format!("{}_{}", n.id, n.id_property)),
                SelectExpr::new(Expr::string(n.entity.as_deref().unwrap_or("")), format!("{}_entity_type", n.id)),
                SelectExpr::new(json_expr, format!("{}_props", n.id)),
            ];

            PhysOp::Scan {
                table: n.table.as_deref().unwrap_or("").to_string(),
                alias: n.id.clone(), dedup: false,
                predicates: preds, select,
            }
        }).collect();

        let body = if arms.len() == 1 { arms.into_iter().next().unwrap() }
                   else { PhysOp::Union { arms, alias: "hydrate".to_string() } };

        PhysOp::TopN { input: Box::new(body), select: vec![], order_by: vec![], limit }
    }
}
