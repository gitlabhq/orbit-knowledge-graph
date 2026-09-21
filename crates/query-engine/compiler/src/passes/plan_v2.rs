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
    _paths: HashMap<(String, String), Vec<JoinPath>>,
    by_kind: HashMap<String, JoinPath>,
}

#[derive(Clone)]
pub struct JoinPath {
    pub edge_kind: String,
    pub source_kind: String,
    pub target_kind: String,
    pub via: JoinVia,
    pub scope_preserving: bool,
    pub edge_table: String,
}

#[derive(Clone)]
pub enum JoinVia {
    EdgeTable,
    ForeignKey { fk_column: String },
}

impl JoinGraph {
    pub fn build(ontology: &Ontology) -> Self {
        let mut paths: HashMap<(String, String), Vec<JoinPath>> = HashMap::new();
        let mut by_kind: HashMap<String, JoinPath> = HashMap::new();
        for edge in ontology.edges() {
            let jp = JoinPath {
                edge_kind: edge.relationship_kind.clone(),
                source_kind: edge.source_kind.clone(),
                target_kind: edge.target_kind.clone(),
                via: match &edge.fk_column {
                    Some(fk) => JoinVia::ForeignKey { fk_column: fk.clone() },
                    None => JoinVia::EdgeTable,
                },
                scope_preserving: edge.scope.is_some_and(|s| s.is_scope_preserving()),
                edge_table: edge.destination_table.clone(),
            };
            paths
                .entry((edge.source_kind.clone(), edge.target_kind.clone()))
                .or_default()
                .push(jp.clone());
            by_kind.entry(edge.relationship_kind.clone()).or_insert(jp);
        }
        Self { _paths: paths, by_kind }
    }

    pub fn edge_table_for(&self, rel_types: &[String], default: &str) -> String {
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
    Scan {
        table: String,
        alias: String,
        final_: bool,
        predicates: Vec<Expr>,
        select: Vec<SelectExpr>,
    },
    Join {
        left: Box<PhysOp>,
        right: Box<PhysOp>,
        on: Expr,
    },
    MultiHopUnion {
        arms: Vec<Query>,
        alias: String,
        outer_predicates: Vec<Expr>,
        select: Vec<SelectExpr>,
    },
    TopN {
        input: Box<PhysOp>,
        select: Vec<SelectExpr>,
        order_by: Vec<OrderExpr>,
        limit: u32,
    },
    Aggregate {
        input: Box<PhysOp>,
        select: Vec<SelectExpr>,
        group_by: Vec<Expr>,
        order_by: Vec<OrderExpr>,
        limit: u32,
    },
    Raw(Query),
}

// ── PlanMetadata ────────────────────────────────────────────────────────────

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
    let limit = input.fetch_limit();
    let te = TextExcerptCtx::new(input, ontology);

    let op = match input.query_type {
        QueryType::Traversal => plan_traversal(input, &graph, &te, limit),
        QueryType::Aggregation => plan_aggregation(input, &graph, limit),
        QueryType::Neighbors => plan_neighbors_delegate(input)?,
        QueryType::PathFinding => plan_pathfinding_delegate(input)?,
        QueryType::Hydration => plan_hydration_delegate(input, ontology)?,
    };

    let meta = PlanMetadata {
        node_edge_mappings: compute_node_edge_mappings(input),
        hop_count: input.relationships.len(),
        phys_op: None,
    };

    Ok((meta, op))
}

fn compute_node_edge_mappings(input: &Input) -> HashMap<String, (String, String)> {
    let mut m = HashMap::new();
    for (i, rel) in input.relationships.iter().enumerate() {
        let ea = format!("e{i}");
        let (sc, ec) = rel.direction.edge_columns();
        m.entry(rel.from.clone()).or_insert_with(|| (ea.clone(), sc.to_string()));
        m.entry(rel.to.clone()).or_insert_with(|| (ea.clone(), ec.to_string()));
    }
    m
}

// ── Text excerpt context ────────────────────────────────────────────────────

struct TextExcerptCtx {
    columns: HashMap<String, HashSet<String>>,
    max_chars: u32,
}

impl TextExcerptCtx {
    fn new(input: &Input, ontology: &Ontology) -> Self {
        let limit = input.fetch_limit();
        let max_chars = (8 * 1024 * 1024 / 4 / u64::from(limit.max(1))) as u32;
        let mut columns = HashMap::new();
        for node in &input.nodes {
            if let Some(ref entity) = node.entity {
                columns.insert(node.id.clone(), text_excerpt_columns(entity, ontology));
            }
        }
        Self { columns, max_chars }
    }

    fn projection(&self, alias: &str, col: &str) -> Expr {
        let value = Expr::col(alias, col);
        let is_excerpt = self.columns.get(alias).is_some_and(|c| c.contains(col));
        if !is_excerpt || self.max_chars == 0 {
            return value;
        }
        let excerpt = Expr::func("substringUTF8", vec![value.clone(), Expr::lit(1), Expr::lit(self.max_chars)]);
        let shortened = Expr::binary(Op::Gt, Expr::func("length", vec![value]), Expr::func("length", vec![excerpt.clone()]));
        let suffix = Expr::func("if", vec![shortened, Expr::string(" [truncated]"), Expr::string("")]);
        Expr::func("concat", vec![excerpt, suffix])
    }

    fn node_select(&self, alias: &str, columns: &Option<ColumnSelection>) -> Vec<SelectExpr> {
        requested_columns(columns)
            .into_iter()
            .map(|col| SelectExpr::new(self.projection(alias, &col), format!("{alias}_{col}")))
            .collect()
    }
}

fn text_excerpt_columns(entity: &str, ontology: &Ontology) -> HashSet<String> {
    let Some(node) = ontology.get_node(entity) else { return HashSet::new() };
    let mut cols: HashSet<String> = node.fields.iter()
        .filter(|f| f.column_name().is_some() && f.data_type == ontology::DataType::String)
        .map(|f| f.name.clone())
        .collect();
    for field in &node.fields {
        if let ontology::FieldSource::Virtual(source) = &field.source {
            for dep in &source.depends_on { cols.remove(dep); }
        }
    }
    cols
}

// ── Traversal ───────────────────────────────────────────────────────────────

fn plan_traversal(input: &Input, graph: &JoinGraph, te: &TextExcerptCtx, limit: u32) -> PhysOp {
    if input.relationships.is_empty() {
        return plan_single_node(input, te, limit);
    }

    let chain = plan_edge_chain(input, graph);
    let mut select = Vec::new();

    // Edge column projections
    for (i, rel) in input.relationships.iter().enumerate() {
        let ea = format!("e{i}");
        if rel.hops.max > 1 {
            let prefix = format!("hop_{ea}");
            select.extend(edge_select_columns_with_prefix(&ea, &prefix));
            select.push(SelectExpr::new(
                Expr::col(&ea, PATH_NODES_COLUMN),
                format!("{prefix}_{PATH_NODES_COLUMN}"),
            ));
        } else {
            select.extend(edge_select_columns(&ea));
        }
    }

    // Node column projections
    for node in &input.nodes {
        select.extend(te.node_select(&node.id, &node.columns));
    }

    let order_by = order_by_exprs(input);

    PhysOp::TopN {
        input: Box::new(chain),
        select,
        order_by,
        limit,
    }
}

fn plan_single_node(input: &Input, te: &TextExcerptCtx, limit: u32) -> PhysOp {
    let n = &input.nodes[0];
    PhysOp::TopN {
        input: Box::new(PhysOp::Scan {
            table: n.table.as_deref().unwrap_or("").to_string(),
            alias: n.id.clone(),
            final_: true,
            predicates: node_predicates(&n.id, n),
            select: te.node_select(&n.id, &n.columns),
        }),
        select: vec![],
        order_by: order_by_exprs(input),
        limit,
    }
}

// ── Aggregation ─────────────────────────────────────────────────────────────

fn plan_aggregation(input: &Input, graph: &JoinGraph, limit: u32) -> PhysOp {
    let chain = if input.relationships.is_empty() {
        let n = &input.nodes[0];
        PhysOp::Scan {
            table: n.table.as_deref().unwrap_or("").to_string(),
            alias: n.id.clone(),
            final_: true,
            predicates: node_predicates(&n.id, n),
            select: vec![],
        }
    } else {
        plan_edge_chain(input, graph)
    };

    let mut select = Vec::new();
    let mut group_by = Vec::new();

    let names = group_by_names(&input.aggregation.group_by);
    for (g, name) in input.aggregation.group_by.iter().zip(names) {
        match g {
            InputGroupByKey::Property { node, property, truncate, .. } => {
                let col = Expr::col(node, property);
                let expr = match truncate {
                    Some(unit) => {
                        let tr = Expr::func(unit.ch_function(), vec![col]);
                        match unit {
                            TruncateUnit::Minute | TruncateUnit::Hour =>
                                Expr::func("toDateTime64", vec![tr, Expr::ident("0")]),
                            _ => Expr::func("toDate32", vec![tr]),
                        }
                    }
                    None => col,
                };
                select.push(SelectExpr::new(expr.clone(), name));
                if !group_by.contains(&expr) { group_by.push(expr); }
            }
            InputGroupByKey::Node { node, .. } => {
                let cols = input.nodes.iter()
                    .find(|n| &n.id == node)
                    .map(|n| requested_columns(&n.columns))
                    .unwrap_or_default();
                for c in cols {
                    let expr = Expr::col(node, &c);
                    if !group_by.contains(&expr) { group_by.push(expr); }
                }
            }
        }
    }

    for agg in &input.aggregation.metrics {
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
    if let Some(ref sort) = input.aggregation.sort {
        order_by.push(if matches!(sort.direction, OrderDirection::Desc) {
            OrderExpr::desc(Expr::ident(&sort.column))
        } else {
            OrderExpr::asc(Expr::ident(&sort.column))
        });
    }
    if input.cursor.is_some() {
        order_by.extend(group_by.iter().map(|e| OrderExpr::asc(e.clone())));
    }

    PhysOp::Aggregate {
        input: Box::new(chain),
        select,
        group_by,
        order_by,
        limit,
    }
}

fn group_by_names(keys: &[InputGroupByKey]) -> Vec<String> {
    keys.iter().map(|k| match k {
        InputGroupByKey::Property { alias: Some(a), .. } => a.clone(),
        InputGroupByKey::Property { node, property, .. } => format!("{node}_{property}"),
        InputGroupByKey::Node { alias: Some(a), .. } => a.clone(),
        InputGroupByKey::Node { node, .. } => node.clone(),
    }).collect()
}

fn order_by_exprs(input: &Input) -> Vec<OrderExpr> {
    input.order_by.as_ref().map(|ob| {
        vec![if matches!(ob.direction, OrderDirection::Desc) {
            OrderExpr::desc(Expr::col(&ob.node, &ob.property))
        } else {
            OrderExpr::asc(Expr::col(&ob.node, &ob.property))
        }]
    }).unwrap_or_default()
}

// ── Edge chain construction ─────────────────────────────────────────────────

fn plan_edge_chain(input: &Input, graph: &JoinGraph) -> PhysOp {
    let mut tree: Option<PhysOp> = None;
    let default_et = &input.compiler.default_edge_table;
    let dedup_edges = input.relationships.len() >= 2;

    for (i, rel) in input.relationships.iter().enumerate() {
        let ea = format!("e{i}");
        let (sc, _) = rel.direction.edge_columns();
        let et = graph.edge_table_for(&rel.types, default_et);

        let edge_op = if rel.hops.max > 1 {
            build_multi_hop(rel, &ea, &et, &input.nodes)
        } else {
            PhysOp::Scan {
                table: et,
                alias: ea.clone(),
                final_: dedup_edges,
                predicates: edge_predicates(&ea, rel, &input.nodes, &input.compiler, graph),
                select: vec![],
            }
        };

        tree = Some(match tree {
            None => edge_op,
            Some(prev) => {
                let pr = &input.relationships[i - 1];
                let (_, pe) = pr.direction.edge_columns();
                PhysOp::Join {
                    left: Box::new(prev),
                    right: Box::new(edge_op),
                    on: Expr::eq(Expr::col(format!("e{}", i - 1), pe), Expr::col(&ea, sc)),
                }
            }
        });
    }

    // Join node tables where needed.
    let mut hydrated: HashSet<String> = HashSet::new();
    for (i, rel) in input.relationships.iter().enumerate() {
        let ea = format!("e{i}");
        let (sc, ec) = rel.direction.edge_columns();
        for (na, col) in [(&rel.from, sc), (&rel.to, ec)] {
            if !hydrated.insert(na.clone()) { continue; }
            let Some(n) = input.nodes.iter().find(|n| &n.id == na) else { continue; };
            if !needs_node_join(n, input) { continue; }
            tree = Some(PhysOp::Join {
                left: Box::new(tree.unwrap()),
                right: Box::new(PhysOp::Scan {
                    table: n.table.as_deref().unwrap_or("").to_string(),
                    alias: na.clone(),
                    final_: true,
                    predicates: node_predicates(na, n),
                    select: vec![SelectExpr::star()],
                }),
                on: Expr::eq(Expr::col(na, DEFAULT_PRIMARY_KEY), Expr::col(&ea, col)),
            });
        }
    }

    tree.unwrap()
}

fn build_multi_hop(
    rel: &InputRelationship, alias: &str, edge_table: &str, nodes: &[InputNode],
) -> PhysOp {
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
    for (na, kc) in [(&rel.from, fk), (&rel.to, tk)] {
        if let Some(n) = nodes.iter().find(|n| &n.id == na) {
            if let Some(ref e) = n.entity {
                outer.push(Expr::eq(Expr::col(alias, kc), Expr::string(e)));
            }
        }
    }
    outer.push(deleted_false(alias));

    PhysOp::MultiHopUnion { arms, alias: alias.to_string(), outer_predicates: outer, select: vec![] }
}

fn needs_node_join(node: &InputNode, input: &Input) -> bool {
    let a = &node.id;
    !node.filters.is_empty()
        || !node.node_ids.is_empty()
        || node.id_range.is_some()
        || matches!(&node.columns, Some(ColumnSelection::List(c)) if !c.is_empty())
        || input.order_by.as_ref().is_some_and(|ob| ob.node == *a)
        || input.aggregation.group_by.iter().any(|g| g.node() == a.as_str())
        || input.aggregation.metrics.iter().any(|m| {
            m.expr.node() == a.as_str()
                && m.expr.property().is_some()
                && !matches!(m.expr.function(), AggFunction::Count)
        })
}

// ── Delegates ───────────────────────────────────────────────────────────────

fn plan_neighbors_delegate(input: &Input) -> Result<PhysOp> {
    let p = crate::passes::plan::neighbors::plan_neighbors(input)?;
    let q = match &p.body {
        crate::passes::plan::PlanBody::Neighbors {
            center, direction, edge, has_non_denorm, center_tp_lookup,
        } => crate::passes::lower::neighbors::emit_neighbors(
            &p, center, *direction, edge, *has_non_denorm, center_tp_lookup.as_ref(),
        )?,
        _ => unreachable!(),
    };
    let Node::Query(q) = q else { unreachable!() };
    Ok(PhysOp::Raw(*q))
}

fn plan_pathfinding_delegate(input: &Input) -> Result<PhysOp> {
    let p = crate::passes::plan::pathfinding::plan_pathfinding(input)?;
    let pf = match &p.body {
        crate::passes::plan::PlanBody::PathFinding(pf) => pf,
        _ => unreachable!(),
    };
    let q = crate::passes::lower::pathfinding::emit_pathfinding(&p, pf)?;
    let Node::Query(q) = q else { unreachable!() };
    Ok(PhysOp::Raw(*q))
}

fn plan_hydration_delegate(input: &Input, ontology: &Ontology) -> Result<PhysOp> {
    let mut p = crate::passes::plan::hydration::plan_hydration(input)?;
    p.resolve_text_excerpts(ontology);
    let nodes = match &p.body {
        crate::passes::plan::PlanBody::Hydration(n) => n,
        _ => unreachable!(),
    };
    let q = crate::passes::lower::hydration::emit_hydration(
        nodes, p.limit, input.hydration_dynamic, input.path_segment_budget,
    )?;
    let Node::Query(q) = q else { unreachable!() };
    Ok(PhysOp::Raw(*q))
}

// ── Predicate builders ──────────────────────────────────────────────────────

fn node_predicates(alias: &str, node: &InputNode) -> Vec<Expr> {
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

fn edge_predicates(
    alias: &str, rel: &InputRelationship, nodes: &[InputNode],
    meta: &CompilerMetadata, graph: &JoinGraph,
) -> Vec<Expr> {
    let mut p = Vec::new();
    let (sc, ec) = rel.direction.edge_columns();

    if let Some(f) = rel_kind_filter(alias, &rel.types) { p.push(f); }
    for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
        if let Some(n) = nodes.iter().find(|n| &n.id == nid) {
            if let Some(ref ent) = n.entity {
                let kc = if ic == SOURCE_ID_COLUMN { SOURCE_KIND_COLUMN } else { TARGET_KIND_COLUMN };
                p.push(Expr::eq(Expr::col(alias, kc), Expr::string(ent)));
            }
        }
    }
    p.push(deleted_false(alias));

    if let Some(ref pfx) = rel.scope_prefix { p.push(pfx.predicate(alias)); }

    for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
        if let Some(n) = nodes.iter().find(|n| &n.id == nid) {
            if !n.node_ids.is_empty() { p.push(id_list_predicate(alias, ic, &n.node_ids)); }
            if let Some(ref r) = n.id_range {
                p.push(Expr::and(
                    Expr::binary(Op::Ge, Expr::col(alias, ic), Expr::int(r.start)),
                    Expr::binary(Op::Le, Expr::col(alias, ic), Expr::int(r.end)),
                ));
            }
        }
    }

    let et = graph.edge_table_for(&rel.types, &meta.default_edge_table);
    if let Some(ecols) = meta.table_columns.get(&et) {
        let reserved: HashSet<&str> = EDGE_RESERVED_COLUMNS.iter().copied().collect();
        let mut seen: HashSet<&str> = HashSet::new();
        for nid in [&rel.from, &rel.to] {
            if let Some(n) = nodes.iter().find(|n| &n.id == nid) {
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
            if let Some(n) = nodes.iter().find(|n| &n.id == nid) {
                let ent = n.entity.as_deref().unwrap_or("");
                let dir = if ic == SOURCE_ID_COLUMN { "source" } else { "target" };
                for (prop, fs) in &n.filters {
                    let key = (ent.to_string(), prop.clone(), dir.to_string());
                    let carries = meta.denorm_rel_kinds.get(&key).is_some_and(|kinds| {
                        rel.types.iter().any(|t| kinds.iter().any(|k| k == t))
                    });
                    if !carries { continue; }
                    if let Some((tc, tk)) = meta.denormalized_columns.get(&key) {
                        for f in fs {
                            if let Some(expr) = denorm_tag_expr(alias, tc, tk, f) { p.push(expr); }
                        }
                    }
                }
            }
        }
    }

    p
}
