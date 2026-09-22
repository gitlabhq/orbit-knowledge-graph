use crate::ast::Expr;
use crate::input::*;
use ontology::Ontology;
use ontology::constants::*;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

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

impl JoinGraph {
    pub fn build(ontology: &Ontology) -> Self {
        let mut by_kind = HashMap::new();
        for edge in ontology.edges() {
            by_kind
                .entry(edge.relationship_kind.clone())
                .or_insert(JoinPath {
                    fk_column: edge.fk_column.clone(),
                    scope_preserving: edge.scope.is_some_and(|s| s.is_scope_preserving()),
                    edge_table: edge.destination_table.clone(),
                });
        }
        Self { by_kind }
    }

    /// Every relationship kind's edge carries the containing namespace's
    /// traversal path, so a scope prefix on the edge implies containment.
    pub fn scope_preserving(&self, rel_types: &[String]) -> bool {
        !rel_types.is_empty()
            && rel_types
                .iter()
                .all(|t| self.by_kind.get(t).is_some_and(|jp| jp.scope_preserving))
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

/// `(alias, column)` reference into a relation visible in the current scope.
pub type Col = (String, String);

#[derive(Clone, Copy, PartialEq, Serialize)]
pub enum Dedup {
    None,
    /// `FINAL`: ReplacingMergeTree merge-on-read.
    Final,
    /// `ORDER BY <sort_key>, _version DESC LIMIT 1 BY <sort_key>`; keeps
    /// column pruning eligible. `_deleted` must be filtered after the dedup.
    LimitBy,
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(tag = "op")]
pub enum PhysOp {
    Scan {
        table: String,
        alias: String,
        dedup: Dedup,
    },
    Filter {
        input: Box<PhysOp>,
        predicates: Vec<Predicate>,
    },
    Project {
        input: Box<PhysOp>,
        columns: Vec<ProjectedColumn>,
    },
    Join {
        left: Box<PhysOp>,
        right: Box<PhysOp>,
        on: Vec<(Col, Col)>,
        kind: JoinKind,
    },
    Aggregate {
        input: Box<PhysOp>,
        group_by: Vec<GroupKey>,
        metrics: Vec<Metric>,
    },
    Union {
        arms: Vec<PhysOp>,
        alias: String,
    },
    Sort {
        input: Box<PhysOp>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<PhysOp>,
        count: u32,
    },
    /// Named subqueries (CTEs) visible to `input`.
    With {
        ctes: Vec<(String, PhysOp)>,
        input: Box<PhysOp>,
    },
}

#[derive(Clone, Copy, PartialEq, Serialize)]
pub enum JoinKind {
    Inner,
    /// `left.col IN (SELECT right.col FROM right)`; only `on[0]` is used.
    Semi,
}

// ── Predicates ──────────────────────────────────────────────────────────────

/// Predicates apply to the alias of the `Filter`'s input relation, except
/// `Expr`, which is fully qualified and may sit over any input.
#[derive(Clone, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Predicate {
    Eq {
        column: String,
        value: Value,
    },
    In {
        column: String,
        values: Vec<Value>,
    },
    Range {
        column: String,
        start: i64,
        end: i64,
    },
    NodeFilter {
        property: String,
        #[serde(skip)]
        filter: InputFilter,
    },
    Func {
        name: String,
        column: String,
        value: Value,
    },
    ScopePrefix(#[serde(skip)] crate::scope::ScopePrefix),
    /// Opaque, fully qualified predicate. Used by the constructors that skip
    /// optimization (neighbors, pathfinding, hydration); never rewritten.
    Expr(#[serde(skip)] Expr),
}

#[derive(Clone, PartialEq, Serialize)]
pub enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    Strs(Vec<String>),
}

pub fn deleted_false() -> Predicate {
    Predicate::Eq {
        column: DELETED_COLUMN.to_string(),
        value: Value::Bool(false),
    }
}

pub fn rel_kind_predicate(types: &[String]) -> Option<Predicate> {
    if crate::passes::normalize::is_wildcard(types) {
        return None;
    }
    Some(if types.len() == 1 {
        Predicate::Eq {
            column: RELATIONSHIP_KIND_COLUMN.to_string(),
            value: Value::Str(types[0].clone()),
        }
    } else {
        Predicate::In {
            column: RELATIONSHIP_KIND_COLUMN.to_string(),
            values: types.iter().map(|t| Value::Str(t.clone())).collect(),
        }
    })
}

/// Relationship-level filters on edge columns (e.g. `project_id`, `traversal_path`).
fn rel_filter_predicates(rel: &InputRelationship) -> Vec<Predicate> {
    let mut props: Vec<_> = rel.filters.iter().collect();
    props.sort_unstable_by_key(|(k, _)| *k);
    props
        .into_iter()
        .flat_map(|(prop, fs)| {
            fs.iter().map(|f| Predicate::NodeFilter {
                property: prop.clone(),
                filter: f.clone(),
            })
        })
        .collect()
}

fn id_in(column: &str, ids: &[i64]) -> Predicate {
    Predicate::In {
        column: column.to_string(),
        values: ids.iter().map(|&id| Value::Int(id)).collect(),
    }
}

// ── Projections ─────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum ProjectedColumn {
    Ref {
        table: String,
        column: String,
        alias: String,
    },
    /// `node.property AS {node}_{property}`, with text-excerpt truncation
    /// applied when the property is an excerpt column.
    NodeProperty {
        node: String,
        property: String,
    },
    Computed {
        expr: ColumnExpr,
        alias: String,
    },
    /// Opaque projection, see `Predicate::Expr`.
    Expr {
        #[serde(skip)]
        expr: Expr,
        alias: String,
    },
}

#[derive(Clone, PartialEq, Serialize)]
pub enum ColumnExpr {
    Col(String, String),
    /// Bare identifier, e.g. an output alias in ORDER BY.
    Ident(String),
    Lit(Value),
    Array(Vec<ColumnExpr>),
    Tuple(Vec<ColumnExpr>),
    Func(String, Vec<ColumnExpr>),
}

impl ColumnExpr {
    pub fn col(alias: &str, column: &str) -> Self {
        ColumnExpr::Col(alias.to_string(), column.to_string())
    }
}

fn col_ref(table: &str, column: &str, alias: impl Into<String>) -> ProjectedColumn {
    ProjectedColumn::Ref {
        table: table.to_string(),
        column: column.to_string(),
        alias: alias.into(),
    }
}

fn expr_col(expr: Expr, alias: impl Into<String>) -> ProjectedColumn {
    ProjectedColumn::Expr {
        expr,
        alias: alias.into(),
    }
}

// ── Aggregation / Sort ──────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Serialize)]
pub struct GroupKey {
    pub node: String,
    pub property: String,
    #[serde(skip)]
    pub truncate: Option<TruncateUnit>,
    pub alias: String,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct Metric {
    #[serde(skip)]
    pub function: AggFunction,
    pub node: String,
    pub property: Option<String>,
    pub alias: String,
}

#[derive(Clone, PartialEq, Serialize)]
pub struct SortKey {
    pub expr: ColumnExpr,
    pub desc: bool,
}

impl SortKey {
    pub fn asc(expr: ColumnExpr) -> Self {
        Self { expr, desc: false }
    }
}

impl GroupKey {
    /// The grouping expression: the column, or its calendar truncation.
    pub fn expr(&self) -> ColumnExpr {
        let col = ColumnExpr::col(&self.node, &self.property);
        match self.truncate {
            None => col,
            Some(unit) => {
                let truncated = ColumnExpr::Func(unit.ch_function().to_string(), vec![col]);
                match unit {
                    TruncateUnit::Minute | TruncateUnit::Hour => ColumnExpr::Func(
                        "toDateTime64".into(),
                        vec![truncated, ColumnExpr::Ident("0".into())],
                    ),
                    _ => ColumnExpr::Func("toDate32".into(), vec![truncated]),
                }
            }
        }
    }
}

// ── Constructors ────────────────────────────────────────────────────────────

pub fn scan(table: &str, alias: &str, dedup: Dedup) -> PhysOp {
    PhysOp::Scan {
        table: table.to_string(),
        alias: alias.to_string(),
        dedup,
    }
}

pub fn filter(input: PhysOp, predicates: Vec<Predicate>) -> PhysOp {
    if predicates.is_empty() {
        return input;
    }
    PhysOp::Filter {
        input: Box::new(input),
        predicates,
    }
}

pub fn project(input: PhysOp, columns: Vec<ProjectedColumn>) -> PhysOp {
    PhysOp::Project {
        input: Box::new(input),
        columns,
    }
}

pub fn join(left: PhysOp, right: PhysOp, on: Vec<(Col, Col)>) -> PhysOp {
    PhysOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        on,
        kind: JoinKind::Inner,
    }
}

pub fn semi_join(left: PhysOp, right: PhysOp, on: (Col, Col)) -> PhysOp {
    PhysOp::Join {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![on],
        kind: JoinKind::Semi,
    }
}

pub fn col(alias: &str, column: &str) -> Col {
    (alias.to_string(), column.to_string())
}

fn union(arms: Vec<PhysOp>, alias: &str) -> PhysOp {
    if arms.len() == 1 {
        return arms.into_iter().next().unwrap();
    }
    PhysOp::Union {
        arms,
        alias: alias.to_string(),
    }
}

fn limit(input: PhysOp, count: u32) -> PhysOp {
    PhysOp::Limit {
        input: Box::new(input),
        count,
    }
}

fn sort(input: PhysOp, keys: Vec<SortKey>) -> PhysOp {
    if keys.is_empty() {
        return input;
    }
    PhysOp::Sort {
        input: Box::new(input),
        keys,
    }
}

// ── PlanMetadata ────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct PlanMetadata {
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub hop_count: usize,
    pub phys_op: Option<PhysOp>,
}

// ── Entry point ─────────────────────────────────────────────────────────────

pub fn plan(
    input: &mut Input,
    ontology: &Ontology,
) -> crate::error::Result<(PlanMetadata, PhysOp)> {
    if input.compiler.table_sort_keys.is_empty() {
        for node in ontology.nodes() {
            input
                .compiler
                .table_sort_keys
                .insert(node.destination_table.clone(), node.sort_key.clone());
        }
    }

    let graph = JoinGraph::build(ontology);
    let ctx = PlanCtx {
        input,
        graph: &graph,
    };
    let limit = input.fetch_limit();

    // Neighbors, pathfinding, and hydration are single-purpose shapes with no
    // join graph to rewrite; they bypass the optimizer.
    let op = match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => {
            let naive = if input.query_type == QueryType::Traversal {
                ctx.plan_traversal(limit)
            } else {
                ctx.plan_aggregation(limit)
            };
            let rule_ctx = super::optimize::RuleCtx {
                input,
                graph: &graph,
            };
            let optimized = super::optimize::optimize(naive, &rule_ctx);
            if input.query_type == QueryType::Traversal {
                ctx.inline_joined_columns(optimized)
            } else {
                optimized
            }
        }
        QueryType::Neighbors => ctx.plan_neighbors(limit),
        QueryType::PathFinding => ctx.plan_pathfinding(limit),
        QueryType::Hydration => ctx.plan_hydration(limit),
    };

    let meta = PlanMetadata {
        node_edge_mappings: ctx.node_edge_mappings(&op),
        hop_count: input.relationships.len(),
        phys_op: None,
    };
    // A scope anchor the optimizer elided has no representation in the query
    // at all; later passes must not expect it in the result.
    let represented: HashSet<String> = meta.node_edge_mappings.keys().cloned().collect();
    if input.query_type == QueryType::Aggregation {
        input.nodes.retain(|n| represented.contains(&n.id));
        input
            .relationships
            .retain(|r| represented.contains(&r.from) && represented.contains(&r.to));
    }
    Ok((meta, op))
}

struct PlanCtx<'a> {
    input: &'a Input,
    graph: &'a JoinGraph,
}

impl<'a> PlanCtx<'a> {
    fn node(&self, alias: &str) -> Option<&'a InputNode> {
        self.input.nodes.iter().find(|n| n.id == alias)
    }

    /// First edge column that carries each node's id, in relationship order.
    fn bindings(&self) -> HashMap<String, Col> {
        let mut bound = HashMap::new();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let (sc, ec) = rel.direction.edge_columns();
            for (n, c) in [(&rel.from, sc), (&rel.to, ec)] {
                bound.entry(n.clone()).or_insert_with(|| col(&ea, c));
            }
        }
        bound
    }

    /// Where `enforce` reads each node's id from: its own scan when joined,
    /// otherwise the edge column that carries it.
    fn node_edge_mappings(&self, op: &PhysOp) -> HashMap<String, (String, String)> {
        if self.input.query_type == QueryType::Neighbors {
            return HashMap::from([(
                self.input.nodes[0].id.clone(),
                ("e".to_string(), SOURCE_ID_COLUMN.to_string()),
            )]);
        }
        let bound = self.bindings();
        let joined = super::optimize::inner_aliases(op);
        self.input
            .nodes
            .iter()
            .filter_map(|n| {
                if joined.contains(&n.id) {
                    Some((n.id.clone(), col(&n.id, DEFAULT_PRIMARY_KEY)))
                } else {
                    bound
                        .get(&n.id)
                        .filter(|(edge, _)| joined.contains(edge))
                        .map(|b| (n.id.clone(), b.clone()))
                }
            })
            .collect()
    }

    fn node_predicates(&self, node: &InputNode) -> Vec<Predicate> {
        let mut p = Vec::new();
        let mut props: Vec<_> = node.filters.iter().collect();
        props.sort_unstable_by_key(|(k, _)| *k);
        for (prop, fs) in props {
            for f in fs {
                p.push(Predicate::NodeFilter {
                    property: prop.clone(),
                    filter: f.clone(),
                });
            }
        }
        if !node.node_ids.is_empty() {
            p.push(id_in(DEFAULT_PRIMARY_KEY, &node.node_ids));
        }
        if let Some(ref r) = node.id_range {
            p.push(Predicate::Range {
                column: DEFAULT_PRIMARY_KEY.to_string(),
                start: r.start,
                end: r.end,
            });
        }
        p.push(deleted_false());
        p
    }

    fn node_scan(&self, n: &InputNode) -> PhysOp {
        filter(
            scan(n.table.as_deref().unwrap_or(""), &n.id, Dedup::Final),
            self.node_predicates(n),
        )
    }

    fn edge_predicates(&self, rel: &InputRelationship) -> Vec<Predicate> {
        let mut p = Vec::new();
        let (sc, ec) = rel.direction.edge_columns();
        p.extend(rel_kind_predicate(&rel.types));
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.node(nid)
                && let Some(ref ent) = n.entity
            {
                let kc = if ic == SOURCE_ID_COLUMN {
                    SOURCE_KIND_COLUMN
                } else {
                    TARGET_KIND_COLUMN
                };
                p.push(Predicate::Eq {
                    column: kc.to_string(),
                    value: Value::Str(ent.clone()),
                });
            }
        }
        p.push(deleted_false());
        p.extend(rel_filter_predicates(rel));
        if let Some(ref pfx) = rel.scope_prefix {
            p.push(Predicate::ScopePrefix(pfx.clone()));
        }
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.node(nid) {
                if !n.node_ids.is_empty() {
                    p.push(id_in(ic, &n.node_ids));
                }
                if let Some(ref r) = n.id_range {
                    p.push(Predicate::Range {
                        column: ic.to_string(),
                        start: r.start,
                        end: r.end,
                    });
                }
            }
        }
        p
    }

    /// A node table is joined only when something reads it (`reads_node`) or
    /// an id range filters it. Pinned ids live on the edge columns, and
    /// requested columns come from the hydration query unless the table is
    /// read anyway.
    fn needs_node_join(&self, node: &InputNode) -> bool {
        node.id_range.is_some() || self.reads_node(node)
    }

    fn reads_node(&self, node: &InputNode) -> bool {
        let a = node.id.as_str();
        let input = self.input;
        !node.filters.is_empty()
            || input.aggregation.group_by.iter().any(|g| g.node() == a)
            || input.aggregation.metrics.iter().any(|m| {
                m.expr.node() == a
                    && m.expr.property().is_some()
                    && !matches!(m.expr.function(), AggFunction::Count)
            })
            || input.order_by.as_ref().is_some_and(|ob| ob.node == a)
    }

    fn sort_keys(&self) -> Vec<SortKey> {
        self.input
            .order_by
            .as_ref()
            .map(|ob| {
                vec![SortKey {
                    expr: ColumnExpr::col(&ob.node, &ob.property),
                    desc: matches!(ob.direction, OrderDirection::Desc),
                }]
            })
            .unwrap_or_default()
    }
}

// ── Traversal / Aggregation ─────────────────────────────────────────────────

/// Edge columns a traversal returns per hop, with their output suffixes.
const EDGE_OUTPUT_COLUMNS: [(&str, &str); 5] = [
    (RELATIONSHIP_KIND_COLUMN, crate::constants::EDGE_TYPE_SUFFIX),
    (SOURCE_ID_COLUMN, crate::constants::EDGE_SRC_SUFFIX),
    (SOURCE_KIND_COLUMN, crate::constants::EDGE_SRC_TYPE_SUFFIX),
    (TARGET_ID_COLUMN, crate::constants::EDGE_DST_SUFFIX),
    (TARGET_KIND_COLUMN, crate::constants::EDGE_DST_TYPE_SUFFIX),
];

impl<'a> PlanCtx<'a> {
    /// Left-deep join over every edge scan, then every node table something
    /// reads. Each edge joins on the columns of nodes an earlier edge already
    /// binds, so star and cycle patterns get the right conditions.
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
            let et = self.graph.edge_table(&rel.types, det);
            let leaf = if rel.hops.max > 1 {
                self.multi_hop(rel, &ea, &et)
            } else {
                filter(scan(&et, &ea, Dedup::None), self.edge_predicates(rel))
            };
            let mut on = Vec::new();
            for (n, c) in [(&rel.from, sc), (&rel.to, ec)] {
                match bound.get(n) {
                    Some(b) => on.push((b.clone(), col(&ea, c))),
                    None => {
                        bound.insert(n.clone(), col(&ea, c));
                    }
                }
            }
            tree = Some(match tree {
                None => leaf,
                Some(t) => join(t, leaf, on),
            });
        }

        let mut tree = tree.unwrap();
        for n in &self.input.nodes {
            if let Some(b) = bound.get(&n.id)
                && self.needs_node_join(n)
            {
                tree = join(
                    tree,
                    self.node_scan(n),
                    vec![(b.clone(), col(&n.id, DEFAULT_PRIMARY_KEY))],
                );
            }
        }
        tree
    }

    fn joined_node_columns(&self) -> Vec<ProjectedColumn> {
        let mut cols = Vec::new();
        let single_node = self.input.relationships.is_empty();
        for n in &self.input.nodes {
            if !single_node && !self.reads_node(n) {
                continue;
            }
            for property in crate::passes::shared::requested_columns(&n.columns) {
                cols.push(ProjectedColumn::NodeProperty {
                    node: n.id.clone(),
                    property,
                });
            }
        }
        cols
    }

    /// FK elision joins node tables the naive plan left to hydration; once
    /// a table is in the query its requested columns come from it directly.
    fn inline_joined_columns(&self, op: PhysOp) -> PhysOp {
        let PhysOp::Limit { input, count } = op else {
            return op;
        };
        let PhysOp::Project { input, mut columns } = *input else {
            return limit(*input, count);
        };
        let joined = super::optimize::inner_aliases(&input);
        for n in &self.input.nodes {
            if !joined.contains(&n.id) {
                continue;
            }
            for property in crate::passes::shared::requested_columns(&n.columns) {
                let c = ProjectedColumn::NodeProperty {
                    node: n.id.clone(),
                    property,
                };
                if !columns.contains(&c) {
                    columns.push(c);
                }
            }
        }
        limit(project(*input, columns), count)
    }

    fn plan_traversal(&self, limit_count: u32) -> PhysOp {
        let mut columns = Vec::new();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let prefix = if rel.hops.max > 1 {
                format!("hop_{ea}")
            } else {
                ea.clone()
            };
            for (column, suffix) in EDGE_OUTPUT_COLUMNS {
                columns.push(col_ref(&ea, column, format!("{prefix}_{suffix}")));
            }
            if rel.hops.max > 1 {
                columns.push(col_ref(
                    &ea,
                    crate::constants::PATH_NODES_COLUMN,
                    format!("{prefix}_{}", crate::constants::PATH_NODES_COLUMN),
                ));
            }
        }
        columns.extend(self.joined_node_columns());
        let mut keys = self.sort_keys();
        if self.input.cursor.is_some() {
            keys.extend(self.traversal_tie_breakers());
        }
        limit(project(sort(self.plan_chain(), keys), columns), limit_count)
    }

    /// Completes the sort into a total order for keyset pagination: each
    /// edge's id pair, or the node's own id when there are no edges.
    fn traversal_tie_breakers(&self) -> Vec<SortKey> {
        if self.input.relationships.is_empty() {
            return vec![SortKey::asc(ColumnExpr::col(
                &self.input.nodes[0].id,
                DEFAULT_PRIMARY_KEY,
            ))];
        }
        (0..self.input.relationships.len())
            .flat_map(|i| {
                let ea = format!("e{i}");
                [
                    SortKey::asc(ColumnExpr::col(&ea, SOURCE_ID_COLUMN)),
                    SortKey::asc(ColumnExpr::col(&ea, TARGET_ID_COLUMN)),
                ]
            })
            .collect()
    }

    fn plan_aggregation(&self, limit_count: u32) -> PhysOp {
        let mut group_by = Vec::new();
        for g in &self.input.aggregation.group_by {
            match g {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    ..
                } => group_by.push(GroupKey {
                    node: node.clone(),
                    property: property.clone(),
                    truncate: *truncate,
                    alias: g.output_name(),
                }),
                InputGroupByKey::Node { node, .. } => {
                    let cols = self
                        .node(node)
                        .map(|n| crate::passes::shared::requested_columns(&n.columns))
                        .unwrap_or_default();
                    for property in cols {
                        group_by.push(GroupKey {
                            node: node.clone(),
                            alias: format!("{node}_{property}"),
                            property,
                            truncate: None,
                        });
                    }
                }
            }
        }
        let metrics: Vec<Metric> = self
            .input
            .aggregation
            .metrics
            .iter()
            .map(|m| Metric {
                function: m.expr.function(),
                node: m.expr.node().to_string(),
                property: m.expr.property().map(str::to_string),
                alias: m.output_name(),
            })
            .collect();
        let mut keys = Vec::new();
        if let Some(ref s) = self.input.aggregation.sort {
            keys.push(SortKey {
                expr: ColumnExpr::Ident(s.column.clone()),
                desc: matches!(s.direction, OrderDirection::Desc),
            });
        }
        if self.input.cursor.is_some() {
            // The group-key tuple is unique per result row, so it completes
            // the sort into a total order the keyset seek can anchor on.
            keys.extend(group_by.iter().map(|gk| SortKey::asc(gk.expr())));
        }
        let agg = PhysOp::Aggregate {
            input: Box::new(self.plan_chain()),
            group_by,
            metrics,
        };
        limit(sort(agg, keys), limit_count)
    }

    /// `UNION ALL` of one arm per depth, aliased as the hop's edge so the
    /// outer query reads it like a single edge with a `path_nodes` column.
    fn multi_hop(&self, rel: &InputRelationship, alias: &str, edge_table: &str) -> PhysOp {
        let (sc, ec) = rel.direction.edge_columns();
        let (start_kind, end_kind) = match rel.direction {
            Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
            Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
        };
        let arms = (rel.hops.min.max(1)..=rel.hops.max)
            .map(|depth| self.depth_arm(depth, edge_table, sc, ec, end_kind, rel))
            .collect();

        let mut outer = Vec::new();
        for (n, kc, ic) in [(&rel.from, start_kind, sc), (&rel.to, end_kind, ec)] {
            let Some(n) = self.node(n) else { continue };
            if let Some(ref e) = n.entity {
                outer.push(Predicate::Eq {
                    column: kc.to_string(),
                    value: Value::Str(e.clone()),
                });
            }
            if !n.node_ids.is_empty() {
                outer.push(id_in(ic, &n.node_ids));
            }
        }
        outer.push(deleted_false());
        // Always a derived table named after the hop, even with one arm, so
        // the arm's internal e1..eN aliases never leak into the outer spine.
        filter(
            PhysOp::Union {
                arms,
                alias: alias.to_string(),
            },
            outer,
        )
    }

    fn depth_arm(
        &self,
        depth: u32,
        edge_table: &str,
        start_col: &str,
        end_col: &str,
        end_kind_col: &str,
        rel: &InputRelationship,
    ) -> PhysOp {
        let hop_preds = |first: bool| {
            let mut p: Vec<Predicate> = rel_kind_predicate(&rel.types).into_iter().collect();
            p.push(deleted_false());
            p.extend(rel_filter_predicates(rel));
            if first && let Some(ref pfx) = rel.scope_prefix {
                p.push(Predicate::ScopePrefix(pfx.clone()));
            }
            p
        };
        let mut chain = filter(scan(edge_table, "e1", Dedup::None), hop_preds(true));
        for j in 2..=depth {
            let (prev, curr) = (format!("e{}", j - 1), format!("e{j}"));
            chain = join(
                chain,
                filter(scan(edge_table, &curr, Dedup::None), hop_preds(false)),
                vec![(col(&prev, end_col), col(&curr, start_col))],
            );
        }
        let last = format!("e{depth}");
        let path_nodes = ColumnExpr::Array(
            (1..=depth)
                .map(|i| {
                    let e = format!("e{i}");
                    ColumnExpr::Tuple(vec![
                        ColumnExpr::Col(e.clone(), end_col.to_string()),
                        ColumnExpr::Col(e, end_kind_col.to_string()),
                    ])
                })
                .collect(),
        );
        // Union arms must agree on shape: first-hop start + last-hop end, plus
        // the first edge's kind/tp/deleted and the reserved kind columns the
        // outer filter reads.
        let start_kind_col = if start_col == SOURCE_ID_COLUMN {
            SOURCE_KIND_COLUMN
        } else {
            TARGET_KIND_COLUMN
        };
        project(
            chain,
            vec![
                col_ref("e1", TRAVERSAL_PATH_COLUMN, TRAVERSAL_PATH_COLUMN),
                col_ref("e1", RELATIONSHIP_KIND_COLUMN, RELATIONSHIP_KIND_COLUMN),
                col_ref("e1", start_col, start_col),
                col_ref("e1", start_kind_col, start_kind_col),
                col_ref(&last, end_col, end_col),
                col_ref(&last, end_kind_col, end_kind_col),
                col_ref("e1", SOURCE_TAGS_COLUMN, SOURCE_TAGS_COLUMN),
                col_ref(&last, TARGET_TAGS_COLUMN, TARGET_TAGS_COLUMN),
                ProjectedColumn::Computed {
                    expr: path_nodes,
                    alias: crate::constants::PATH_NODES_COLUMN.to_string(),
                },
                ProjectedColumn::Computed {
                    expr: ColumnExpr::Lit(Value::Int(depth as i64)),
                    alias: crate::constants::DEPTH_COLUMN.to_string(),
                },
                col_ref("e1", DELETED_COLUMN, DELETED_COLUMN),
            ],
        )
    }
}

// ── Hydration ───────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    /// One `LIMIT 1 BY` latest-row arm per entity, projecting the requested
    /// columns as a JSON map. Traversal paths from the base query narrow the
    /// scan via the primary key.
    fn plan_hydration(&self, limit_count: u32) -> PhysOp {
        let input = self.input;
        let arms = input
            .nodes
            .iter()
            .map(|n| {
                let alias = n.id.as_str();
                let table = n.table.as_deref().unwrap_or("");
                let pk = n.id_property.as_str();
                let columns = crate::passes::shared::requested_columns(&n.columns);

                let mut inner = Vec::new();
                if let Some(tp) = crate::passes::shared::traversal_path_filter(
                    alias,
                    &n.traversal_paths,
                    input.hydration_dynamic,
                    input.path_segment_budget,
                ) {
                    inner.push(Predicate::Expr(tp));
                }
                if !n.node_ids.is_empty() {
                    inner.push(id_in(pk, &n.node_ids));
                }
                let mut inner_cols = vec![
                    col_ref(alias, pk, pk),
                    col_ref(alias, DELETED_COLUMN, DELETED_COLUMN),
                ];
                for c in &columns {
                    if c != pk && c != DELETED_COLUMN {
                        inner_cols.push(col_ref(alias, c, c));
                    }
                }
                let props = if columns.is_empty() {
                    Expr::string("{}")
                } else {
                    let map_args = columns
                        .iter()
                        .flat_map(|c| {
                            [
                                Expr::string(c),
                                Expr::func("toString", vec![Expr::col(alias, c)]),
                            ]
                        })
                        .collect();
                    Expr::func("toJSONString", vec![Expr::func("map", map_args)])
                };
                let deduped = project(
                    filter(scan(table, alias, Dedup::LimitBy), inner),
                    inner_cols,
                );
                project(
                    filter(deduped, vec![deleted_false()]),
                    vec![
                        col_ref(alias, pk, format!("{alias}_{pk}")),
                        expr_col(
                            Expr::string(n.entity.as_deref().unwrap_or("")),
                            format!("{alias}_entity_type"),
                        ),
                        expr_col(props, format!("{alias}_props")),
                    ],
                )
            })
            .collect();
        limit(
            union(arms, crate::constants::HYDRATION_NODE_ALIAS),
            limit_count,
        )
    }
}

// ── Neighbors ───────────────────────────────────────────────────────────────

/// Physical edge tables a neighbors arm scans, narrowed per direction to the
/// tables whose relationships can have the center as source or target.
struct EdgeTables {
    all: Vec<String>,
    outgoing: Vec<String>,
    incoming: Vec<String>,
}

impl<'a> PlanCtx<'a> {
    fn neighbor_edge_tables(&self, rel_types: &[String], center_entity: &str) -> EdgeTables {
        let meta = &self.input.compiler;
        let all = meta.resolve_edge_tables(rel_types);
        let rels: Vec<&String> = if rel_types.is_empty() {
            meta.edge_table_for_rel.keys().collect()
        } else {
            rel_types.iter().collect()
        };
        let tables_for = |kinds: &HashMap<String, Vec<String>>| -> Vec<String> {
            let mut t: Vec<String> = rels
                .iter()
                .filter(|r| {
                    kinds
                        .get(**r)
                        .is_some_and(|ks| ks.iter().any(|k| k == center_entity))
                })
                .map(|r| {
                    meta.edge_table_for_rel
                        .get(*r)
                        .cloned()
                        .unwrap_or_else(|| meta.default_edge_table.clone())
                })
                .collect();
            t.sort();
            t.dedup();
            t
        };
        let outgoing = tables_for(&meta.edge_source_kinds);
        let incoming = tables_for(&meta.edge_target_kinds);
        EdgeTables {
            outgoing: if outgoing.is_empty() {
                all.clone()
            } else {
                outgoing
            },
            incoming: if incoming.is_empty() {
                all.clone()
            } else {
                incoming
            },
            all,
        }
    }

    /// Scan of one or more physical edge tables under one alias. Several
    /// tables union on the reserved edge columns so extra per-table columns
    /// don't break the union; `arm_where` is pushed into each arm.
    fn edge_scan(
        &self,
        tables: &[String],
        alias: &str,
        arm_where: impl Fn(&str) -> Vec<Predicate>,
    ) -> PhysOp {
        if tables.len() == 1 {
            return filter(scan(&tables[0], alias, Dedup::None), arm_where(alias));
        }
        let inner = format!("_{alias}");
        let arms = tables
            .iter()
            .map(|t| {
                let mut cols: Vec<ProjectedColumn> = EDGE_RESERVED_COLUMNS
                    .iter()
                    .map(|c| col_ref(&inner, c, *c))
                    .collect();
                cols.push(col_ref(&inner, DELETED_COLUMN, DELETED_COLUMN));
                project(
                    filter(scan(t, &inner, Dedup::None), arm_where(&inner)),
                    cols,
                )
            })
            .collect();
        union(arms, alias)
    }

    fn plan_neighbors(&self, limit_count: u32) -> PhysOp {
        use crate::constants::*;
        use crate::passes::shared::{denorm_tag_expr, id_list_predicate};

        let config = self.input.neighbors.as_ref().expect("neighbors config");
        let center = &self.input.nodes[0];
        let cid = center.id.as_str();
        let entity = center.entity.as_deref().unwrap_or("");
        let table = center.table.as_deref().unwrap_or("");
        let default_pk = center.redaction_id_column == DEFAULT_PRIMARY_KEY;
        let meta = &self.input.compiler;
        let tables = self.neighbor_edge_tables(&config.rel_types, entity);
        let rel_kind = rel_kind_predicate(&config.rel_types);
        let tp_lookup = meta.tp_id_lookup.get(entity);
        let has_non_denorm = center.filters.keys().any(|prop| {
            !["source", "target"].iter().any(|d| {
                meta.denormalized_columns.contains_key(&(
                    entity.to_string(),
                    prop.clone(),
                    d.to_string(),
                ))
            })
        }) || center.id_range.is_some();
        let e = "e";

        let denorm_tags = |dir: &str| -> Vec<Predicate> {
            let mut out = Vec::new();
            let mut props: Vec<_> = center.filters.iter().collect();
            props.sort_unstable_by_key(|(k, _)| *k);
            for (prop, fs) in props {
                let key = (entity.to_string(), prop.clone(), dir.to_string());
                let Some((tag_col, tag_key)) = meta.denormalized_columns.get(&key) else {
                    continue;
                };
                for f in fs {
                    if let Some(x) = denorm_tag_expr(e, tag_col, tag_key, f) {
                        out.push(Predicate::Expr(x));
                    }
                }
            }
            out
        };

        let build_arm = |dir: Direction| -> PhysOp {
            let (center_col, center_kind, nb_id, nb_kind, is_out, denorm_dir, arm_tables) =
                match dir {
                    Direction::Outgoing => (
                        SOURCE_ID_COLUMN,
                        SOURCE_KIND_COLUMN,
                        TARGET_ID_COLUMN,
                        TARGET_KIND_COLUMN,
                        1i64,
                        "source",
                        &tables.outgoing,
                    ),
                    Direction::Incoming => (
                        TARGET_ID_COLUMN,
                        TARGET_KIND_COLUMN,
                        SOURCE_ID_COLUMN,
                        SOURCE_KIND_COLUMN,
                        0i64,
                        "target",
                        &tables.incoming,
                    ),
                    Direction::Both => unreachable!(),
                };

            let arm_where = |a: &str| -> Vec<Predicate> {
                let mut p = vec![Predicate::Eq {
                    column: center_kind.to_string(),
                    value: Value::Str(entity.to_string()),
                }];
                if !center.node_ids.is_empty() {
                    p.push(id_in(center_col, &center.node_ids));
                }
                p.extend(rel_kind.clone());
                // Incoming edges to a namespace center sit at the center's own
                // tp; pin to the resolved paths for a leading-PK point lookup.
                if dir == Direction::Incoming
                    && !center.node_ids.is_empty()
                    && let Some((src, key_col)) = tp_lookup
                {
                    p.push(Predicate::Expr(Expr::InSelect {
                        expr: Box::new(Expr::col(a, TRAVERSAL_PATH_COLUMN)),
                        query: Box::new(crate::ast::Query {
                            select: vec![crate::ast::SelectExpr::col(
                                "_tpd",
                                TRAVERSAL_PATH_COLUMN,
                            )],
                            from: crate::ast::TableRef::scan(src, "_tpd"),
                            where_clause: Expr::conjoin(vec![
                                id_list_predicate("_tpd", key_col, &center.node_ids),
                                crate::passes::shared::deleted_false("_tpd"),
                            ]),
                            ..Default::default()
                        }),
                    }));
                }
                p.push(deleted_false());
                p
            };

            let mut base = self.edge_scan(arm_tables, e, arm_where);
            // Denorm tags aren't in the per-arm projection, so they filter
            // the union output alias.
            base = filter(base, denorm_tags(denorm_dir));

            let mut columns = vec![
                col_ref(e, nb_id, neighbor_id_column()),
                col_ref(e, nb_kind, neighbor_type_column()),
                col_ref(e, RELATIONSHIP_KIND_COLUMN, relationship_type_column()),
                expr_col(Expr::int(is_out), neighbor_is_outgoing_column()),
            ];

            let center_join = |base: PhysOp, dedup: Dedup, preds: Vec<Predicate>| {
                join(
                    base,
                    filter(scan(table, cid, dedup), preds),
                    vec![(col(e, center_col), col(cid, DEFAULT_PRIMARY_KEY))],
                )
            };
            if has_non_denorm {
                base = center_join(base, Dedup::Final, self.node_predicates(center));
            }
            if default_pk {
                columns.push(col_ref(e, center_col, redaction_id_column(cid)));
            } else {
                if !has_non_denorm {
                    base = center_join(base, Dedup::Final, vec![deleted_false()]);
                }
                columns.push(col_ref(
                    cid,
                    &center.redaction_id_column,
                    redaction_id_column(cid),
                ));
                columns.push(col_ref(cid, DEFAULT_PRIMARY_KEY, primary_key_column(cid)));
            }
            columns.push(expr_col(Expr::string(entity), redaction_type_column(cid)));
            if center.has_traversal_path {
                columns.push(col_ref(
                    e,
                    TRAVERSAL_PATH_COLUMN,
                    traversal_path_column(cid),
                ));
            }
            project(base, columns)
        };

        let edge_tiebreakers = || {
            [SOURCE_ID_COLUMN, TARGET_ID_COLUMN, RELATIONSHIP_KIND_COLUMN]
                .iter()
                .map(|c| SortKey::asc(ColumnExpr::col(e, c)))
                .collect::<Vec<_>>()
        };
        let projected_tiebreakers = || {
            [
                redaction_id_column(cid),
                neighbor_id_column().to_string(),
                relationship_type_column().to_string(),
                neighbor_is_outgoing_column().to_string(),
            ]
            .into_iter()
            .map(|c| SortKey::asc(ColumnExpr::Ident(c)))
            .collect::<Vec<_>>()
        };
        let tiebreakers = || {
            if config.direction == Direction::Both {
                projected_tiebreakers()
            } else {
                edge_tiebreakers()
            }
        };
        let mut keys = self.sort_keys();
        if self.input.cursor.is_some() {
            keys.extend(tiebreakers());
        }

        // Default-PK center, denorm-only filters, one physical table: both
        // directions collapse into one scan (see `fused_both_arm`).
        let fused = config.direction == Direction::Both
            && !has_non_denorm
            && default_pk
            && tables.all.len() == 1;
        let body = if fused {
            self.fused_both_arm(center, entity, &tables.all[0], e, rel_kind, &denorm_tags)
        } else {
            match config.direction {
                Direction::Both => union(
                    vec![
                        build_arm(Direction::Outgoing),
                        build_arm(Direction::Incoming),
                    ],
                    "_union",
                ),
                dir => build_arm(dir),
            }
        };
        limit(sort(body, keys), limit_count)
    }

    /// Scan the edge once with `WHERE (source side) OR (target side)`, then
    /// `arrayJoin(arrayFilter(matched, [out_tuple, in_tuple]))` so each row
    /// yields one entry per matched arm; a self-loop still yields two rows.
    fn fused_both_arm(
        &self,
        center: &InputNode,
        entity: &str,
        edge_table: &str,
        e: &str,
        rel_kind: Option<Predicate>,
        denorm_tags: &dyn Fn(&str) -> Vec<Predicate>,
    ) -> PhysOp {
        use crate::constants::*;
        use crate::passes::shared::id_list_predicate;

        let cid = center.id.as_str();
        let arm_predicate = |kind_col: &str, id_col: &str, dir: &str| -> Expr {
            let mut parts = vec![Expr::eq(Expr::col(e, kind_col), Expr::string(entity))];
            if !center.node_ids.is_empty() {
                parts.push(id_list_predicate(e, id_col, &center.node_ids));
            }
            for p in denorm_tags(dir) {
                if let Predicate::Expr(x) = p {
                    parts.push(x);
                }
            }
            Expr::conjoin(parts).expect("kind conjunct")
        };
        let source_arm = arm_predicate(SOURCE_KIND_COLUMN, SOURCE_ID_COLUMN, "source");
        let target_arm = arm_predicate(TARGET_KIND_COLUMN, TARGET_ID_COLUMN, "target");

        // (matched, is_outgoing, neighbor_id, neighbor_kind, center_id)
        let out_tuple = Expr::func(
            "tuple",
            vec![
                source_arm.clone(),
                Expr::int(1),
                Expr::col(e, TARGET_ID_COLUMN),
                Expr::col(e, TARGET_KIND_COLUMN),
                Expr::col(e, SOURCE_ID_COLUMN),
            ],
        );
        let in_tuple = Expr::func(
            "tuple",
            vec![
                target_arm.clone(),
                Expr::int(0),
                Expr::col(e, SOURCE_ID_COLUMN),
                Expr::col(e, SOURCE_KIND_COLUMN),
                Expr::col(e, TARGET_ID_COLUMN),
            ],
        );
        let matched = Expr::func(
            "arrayFilter",
            vec![
                Expr::lambda(
                    "_gkg_arm",
                    Expr::func("tupleElement", vec![Expr::ident("_gkg_arm"), Expr::int(1)]),
                ),
                Expr::func("array", vec![out_tuple, in_tuple]),
            ],
        );
        const ROW_COL: &str = "_gkg_arm_row";
        let rel_col = relationship_type_column();
        let tp_col = traversal_path_column(cid);
        let mut inner_cols = vec![
            expr_col(Expr::func("arrayJoin", vec![matched]), ROW_COL),
            col_ref(e, RELATIONSHIP_KIND_COLUMN, rel_col),
        ];
        if center.has_traversal_path {
            inner_cols.push(col_ref(e, TRAVERSAL_PATH_COLUMN, tp_col.clone()));
        }
        let mut inner_where = vec![Predicate::Expr(Expr::binary(
            crate::ast::Op::Or,
            source_arm,
            target_arm,
        ))];
        inner_where.extend(rel_kind);
        inner_where.push(deleted_false());

        let inner = project(
            filter(scan(edge_table, e, Dedup::None), inner_where),
            inner_cols,
        );
        let te = |n: i64| Expr::func("tupleElement", vec![Expr::col(e, ROW_COL), Expr::int(n)]);
        let mut columns = vec![
            expr_col(te(3), neighbor_id_column()),
            expr_col(te(4), neighbor_type_column()),
            col_ref(e, rel_col, rel_col),
            expr_col(te(2), neighbor_is_outgoing_column()),
            expr_col(te(5), redaction_id_column(cid)),
            expr_col(Expr::string(entity), redaction_type_column(cid)),
        ];
        if center.has_traversal_path {
            columns.push(col_ref(e, &tp_col, tp_col.clone()));
        }
        project(inner, columns)
    }
}

// ── Pathfinding ─────────────────────────────────────────────────────────────

/// How a path endpoint constrains the first frontier hop: pinned ids go
/// straight onto the edge column; filters become an `_nf_` anchor CTE.
struct Anchor {
    edge_filter: Option<Predicate>,
    cte: Option<(String, PhysOp)>,
    has_tp: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum FDir {
    Forward,
    Backward,
}

#[derive(Clone)]
struct Frontier<'f> {
    rel_types: &'f [String],
    first_hop_types: &'f [String],
    anchor_entity: &'f str,
    edge_tables: &'f [String],
    scope_cte: Option<&'f str>,
    include_tp: bool,
    anchor_denorm_tags: Vec<Predicate>,
}

impl<'a> PlanCtx<'a> {
    /// Bidirectional BFS: `forward` and `backward` frontier CTEs (one arm per
    /// depth), combined as direct hits (forward reaches the end) plus
    /// intersections (forward meets backward on `end_id`).
    fn plan_pathfinding(&self, limit_count: u32) -> PhysOp {
        use crate::constants::*;
        use crate::passes::shared::denorm_tag_expr;

        let cfg = self.input.path.as_ref().expect("path config");
        let start = self.node(&cfg.from).expect("start node");
        let end = self.node(&cfg.to).expect("end node");
        let start_entity = start.entity.as_deref().unwrap_or("");
        let end_entity = end.entity.as_deref().unwrap_or("");
        let scoped_by_tp = start.has_traversal_path && end.has_traversal_path;
        let edge_tables = self.input.compiler.resolve_edge_tables(&cfg.rel_types);
        let max_depth = cfg.max_depth;
        let fwd_depth = max_depth / 2 + max_depth % 2;
        let bwd_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

        let start_anchor = self.anchor(start, SOURCE_ID_COLUMN, scoped_by_tp);
        let end_anchor = self.anchor(end, TARGET_ID_COLUMN, scoped_by_tp);
        let scope_cte = scope_cte(&start_anchor, &end_anchor);
        let scope_cte_name = scope_cte.as_ref().map(|(n, _)| n.clone());

        let denorm_tags = |node: &InputNode, entity: &str, dir: &str| -> Vec<Predicate> {
            let mut out = Vec::new();
            let mut props: Vec<_> = node.filters.iter().collect();
            props.sort_unstable_by_key(|(k, _)| *k);
            for (prop, fs) in props {
                let key = (entity.to_string(), prop.clone(), dir.to_string());
                let Some((tag_col, tag_key)) = self.input.compiler.denormalized_columns.get(&key)
                else {
                    continue;
                };
                for f in fs {
                    if let Some(x) = denorm_tag_expr("e1", tag_col, tag_key, f) {
                        out.push(Predicate::Expr(x));
                    }
                }
            }
            out
        };

        let fwd = Frontier {
            rel_types: &cfg.rel_types,
            first_hop_types: &cfg.forward_first_hop_rel_types,
            anchor_entity: start_entity,
            edge_tables: &edge_tables,
            scope_cte: scope_cte_name.as_deref(),
            include_tp: scoped_by_tp,
            anchor_denorm_tags: denorm_tags(start, start_entity, "source"),
        };
        let bwd = Frontier {
            first_hop_types: &cfg.backward_first_hop_rel_types,
            anchor_entity: end_entity,
            anchor_denorm_tags: denorm_tags(end, end_entity, "target"),
            ..fwd.clone()
        };

        let mut ctes: Vec<(String, PhysOp)> = Vec::new();
        ctes.extend(start_anchor.cte.clone());
        ctes.extend(end_anchor.cte.clone());
        ctes.extend(scope_cte);
        ctes.push((
            FORWARD_CTE.to_string(),
            self.frontier(
                start_anchor.edge_filter.clone(),
                fwd_depth,
                FDir::Forward,
                &fwd,
            ),
        ));
        if bwd_depth > 0 {
            ctes.push((
                BACKWARD_CTE.to_string(),
                self.frontier(
                    end_anchor.edge_filter.clone(),
                    bwd_depth,
                    FDir::Backward,
                    &bwd,
                ),
            ));
        }

        let tuple = |t: &str, entity: &str| {
            Expr::func(
                "tuple",
                vec![Expr::col(t, ANCHOR_ID_COLUMN), Expr::string(entity)],
            )
        };
        let f = FORWARD_ALIAS;
        let b = BACKWARD_ALIAS;

        let mut direct_where = vec![
            Predicate::Eq {
                column: DEPTH_COLUMN.to_string(),
                value: Value::Int(1),
            },
            Predicate::Eq {
                column: END_KIND_COLUMN.to_string(),
                value: Value::Str(end_entity.to_string()),
            },
        ];
        direct_where.extend(endpoint_filter(end, f, END_ID_COLUMN));
        let direct = project(
            filter(scan(FORWARD_CTE, f, Dedup::None), direct_where),
            vec![
                col_ref(f, DEPTH_COLUMN, DEPTH_COLUMN),
                expr_col(
                    Expr::func(
                        "arrayConcat",
                        vec![
                            Expr::func("array", vec![tuple(f, start_entity)]),
                            Expr::col(f, PATH_NODES_COLUMN),
                        ],
                    ),
                    path_column(),
                ),
                col_ref(f, FRONTIER_EDGE_KINDS_COLUMN, edge_kinds_column()),
            ],
        );

        let mut arms = vec![direct];
        if bwd_depth > 0 {
            let depth_sum = Expr::binary(
                crate::ast::Op::Add,
                Expr::col(f, DEPTH_COLUMN),
                Expr::col(b, DEPTH_COLUMN),
            );
            let mut on = vec![(col(f, END_ID_COLUMN), col(b, END_ID_COLUMN))];
            if scoped_by_tp {
                on.push((col(f, TRAVERSAL_PATH_COLUMN), col(b, TRAVERSAL_PATH_COLUMN)));
            }
            let meet = join(
                scan(FORWARD_CTE, f, Dedup::None),
                scan(BACKWARD_CTE, b, Dedup::None),
                on,
            );
            arms.push(project(
                filter(
                    meet,
                    vec![Predicate::Expr(Expr::binary(
                        crate::ast::Op::Le,
                        depth_sum.clone(),
                        Expr::int(max_depth as i64),
                    ))],
                ),
                vec![
                    expr_col(depth_sum, DEPTH_COLUMN),
                    expr_col(
                        Expr::func(
                            "arrayConcat",
                            vec![
                                Expr::func("array", vec![tuple(f, start_entity)]),
                                Expr::col(f, PATH_NODES_COLUMN),
                                Expr::func("arrayReverse", vec![Expr::col(b, PATH_NODES_COLUMN)]),
                                Expr::func("array", vec![tuple(b, end_entity)]),
                            ],
                        ),
                        path_column(),
                    ),
                    expr_col(
                        Expr::func(
                            "arrayConcat",
                            vec![
                                Expr::col(f, FRONTIER_EDGE_KINDS_COLUMN),
                                Expr::func(
                                    "arrayReverse",
                                    vec![Expr::col(b, FRONTIER_EDGE_KINDS_COLUMN)],
                                ),
                            ],
                        ),
                        edge_kinds_column(),
                    ),
                ],
            ));
        }

        // Always a derived table named `paths`, even with one arm, so the
        // projection and sort above read a stable alias.
        let paths = PhysOp::Union {
            arms,
            alias: PATHS_ALIAS.to_string(),
        };
        let mut keys = vec![SortKey::asc(ColumnExpr::col(PATHS_ALIAS, DEPTH_COLUMN))];
        if self.input.cursor.is_some() {
            keys.extend([path_column(), edge_kinds_column()].iter().map(|c| {
                SortKey::asc(ColumnExpr::Func(
                    "toString".into(),
                    vec![ColumnExpr::col(PATHS_ALIAS, c)],
                ))
            }));
        }
        let body = limit(
            sort(
                project(
                    paths,
                    vec![
                        col_ref(PATHS_ALIAS, &path_column(), path_column()),
                        col_ref(PATHS_ALIAS, &edge_kinds_column(), edge_kinds_column()),
                        col_ref(PATHS_ALIAS, DEPTH_COLUMN, DEPTH_COLUMN),
                    ],
                ),
                keys,
            ),
            limit_count,
        );
        PhysOp::With {
            ctes,
            input: Box::new(body),
        }
    }

    fn anchor(&self, n: &InputNode, edge_col: &str, force_cte: bool) -> Anchor {
        if !force_cte && !n.node_ids.is_empty() {
            return Anchor {
                edge_filter: Some(id_in(edge_col, &n.node_ids)),
                cte: None,
                has_tp: false,
            };
        }
        if n.node_ids.is_empty() && n.filters.is_empty() && n.id_range.is_none() {
            return Anchor {
                edge_filter: None,
                cte: None,
                has_tp: false,
            };
        }
        let alias = n.id.as_str();
        let cte_name = crate::constants::node_filter_cte(alias);
        let mut preds = self.node_predicates(n);
        preds.retain(|p| *p != deleted_false());
        let mut cols = vec![col_ref(alias, DEFAULT_PRIMARY_KEY, DEFAULT_PRIMARY_KEY)];
        if n.has_traversal_path {
            cols.push(col_ref(alias, TRAVERSAL_PATH_COLUMN, TRAVERSAL_PATH_COLUMN));
        }
        let mut inner_cols = cols.clone();
        inner_cols.push(col_ref(alias, DELETED_COLUMN, DELETED_COLUMN));
        let body = limit(
            project(
                filter(
                    project(
                        filter(
                            scan(n.table.as_deref().unwrap_or(""), alias, Dedup::Final),
                            preds,
                        ),
                        inner_cols,
                    ),
                    vec![deleted_false()],
                ),
                cols,
            ),
            crate::passes::validate::MAX_PATH_ANCHOR_LIMIT as u32,
        );
        Anchor {
            edge_filter: Some(Predicate::Expr(Expr::InSubquery {
                expr: Box::new(Expr::col("e1", edge_col)),
                cte_name: cte_name.clone(),
                column: DEFAULT_PRIMARY_KEY.into(),
            })),
            cte: Some((cte_name, body)),
            has_tp: n.has_traversal_path,
        }
    }

    fn frontier(
        &self,
        anchor_cond: Option<Predicate>,
        max_depth: u32,
        dir: FDir,
        opts: &Frontier<'_>,
    ) -> PhysOp {
        let arms = (1..=max_depth)
            .map(|depth| self.frontier_arm(anchor_cond.clone(), depth, dir, opts))
            .collect();
        union(arms, "_frontier")
    }

    fn frontier_arm(
        &self,
        anchor_cond: Option<Predicate>,
        depth: u32,
        dir: FDir,
        opts: &Frontier<'_>,
    ) -> PhysOp {
        use crate::constants::*;
        let (anchor_col, next_col, next_kind_col, anchor_kind_col) = match dir {
            FDir::Forward => (
                SOURCE_ID_COLUMN,
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
                SOURCE_KIND_COLUMN,
            ),
            FDir::Backward => (
                TARGET_ID_COLUMN,
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                TARGET_KIND_COLUMN,
            ),
        };
        let scope_filter = |alias: &str| {
            opts.scope_cte.map(|sc| {
                Predicate::Expr(Expr::InSubquery {
                    expr: Box::new(Expr::col(alias, TRAVERSAL_PATH_COLUMN)),
                    cte_name: sc.to_string(),
                    column: TRAVERSAL_PATH_COLUMN.to_string(),
                })
            })
        };

        // A specific first-hop filter wins; otherwise fall back to the general
        // rel_type filter so e1 isn't left unfiltered.
        let first_types = if opts.first_hop_types.is_empty() {
            opts.rel_types
        } else {
            opts.first_hop_types
        };
        let mut first = Vec::new();
        first.extend(anchor_cond);
        first.extend(rel_kind_predicate(first_types));
        first.push(Predicate::Eq {
            column: anchor_kind_col.to_string(),
            value: Value::Str(opts.anchor_entity.to_string()),
        });
        first.extend(scope_filter("e1"));
        first.push(deleted_false());
        first.extend(opts.anchor_denorm_tags.iter().cloned());

        let mut chain = filter(
            self.edge_scan(opts.edge_tables, "e1", |_| Vec::new()),
            first,
        );
        for i in 2..=depth {
            let (prev, curr) = (format!("e{}", i - 1), format!("e{i}"));
            let mut hop = Vec::new();
            hop.extend(rel_kind_predicate(opts.rel_types));
            hop.extend(scope_filter(&curr));
            hop.push(deleted_false());
            let mut on = vec![(col(&prev, next_col), col(&curr, anchor_col))];
            if opts.include_tp {
                on.push((
                    col(&prev, TRAVERSAL_PATH_COLUMN),
                    col(&curr, TRAVERSAL_PATH_COLUMN),
                ));
            }
            let right = filter(self.edge_scan(opts.edge_tables, &curr, |_| Vec::new()), hop);
            chain = join(chain, right, on);
        }

        let last = format!("e{depth}");
        let path_range = match dir {
            FDir::Forward => 1..=depth,
            FDir::Backward => 1..=depth.saturating_sub(1),
        };
        let tuples: Vec<Expr> = path_range
            .map(|i| {
                let a = format!("e{i}");
                Expr::func(
                    "tuple",
                    vec![Expr::col(&a, next_col), Expr::col(&a, next_kind_col)],
                )
            })
            .collect();
        // Backward depth 1 has no intermediate nodes; keep the arm's column
        // type as Array(Tuple(Int64, String)) with an empty typed array.
        let path_nodes = if tuples.is_empty() {
            Expr::func(
                "arrayResize",
                vec![
                    Expr::func(
                        "array",
                        vec![Expr::func("tuple", vec![Expr::int(0), Expr::string("")])],
                    ),
                    Expr::int(0),
                ],
            )
        } else {
            Expr::func("array", tuples)
        };
        let edge_kinds = Expr::func(
            "array",
            (1..=depth)
                .map(|i| Expr::col(format!("e{i}"), RELATIONSHIP_KIND_COLUMN))
                .collect(),
        );
        let mut columns = vec![
            col_ref("e1", anchor_col, ANCHOR_ID_COLUMN),
            col_ref(&last, next_col, END_ID_COLUMN),
            col_ref(&last, next_kind_col, END_KIND_COLUMN),
            expr_col(path_nodes, PATH_NODES_COLUMN),
            expr_col(edge_kinds, FRONTIER_EDGE_KINDS_COLUMN),
            expr_col(Expr::int(depth as i64), DEPTH_COLUMN),
        ];
        if opts.include_tp {
            columns.push(col_ref("e1", TRAVERSAL_PATH_COLUMN, TRAVERSAL_PATH_COLUMN));
        }
        project(chain, columns)
    }
}

/// Endpoints at different namespace depths are linked by edges carrying only
/// the deeper tp, so the scope is the UNION (not intersection) of both anchors'
/// traversal paths.
fn scope_cte(start: &Anchor, end: &Anchor) -> Option<(String, PhysOp)> {
    let (start_cte, _) = start.cte.as_ref()?;
    let (end_cte, _) = end.cte.as_ref()?;
    if !start.has_tp || !end.has_tp {
        return None;
    }
    let arm = |cte: &str, alias: &str| PhysOp::Aggregate {
        input: Box::new(scan(cte, alias, Dedup::None)),
        group_by: vec![GroupKey {
            node: alias.to_string(),
            property: TRAVERSAL_PATH_COLUMN.to_string(),
            truncate: None,
            alias: TRAVERSAL_PATH_COLUMN.to_string(),
        }],
        metrics: vec![],
    };
    Some((
        "_path_scope_traversal_paths".to_string(),
        union(
            vec![
                arm(start_cte, "_path_scope_start"),
                arm(end_cte, "_path_scope_end"),
            ],
            "_path_scope",
        ),
    ))
}

fn endpoint_filter(n: &InputNode, alias: &str, column: &str) -> Option<Predicate> {
    if !n.node_ids.is_empty() {
        return Some(id_in(column, &n.node_ids));
    }
    if !n.filters.is_empty() || n.id_range.is_some() {
        return Some(Predicate::Expr(Expr::InSubquery {
            expr: Box::new(Expr::col(alias, column)),
            cte_name: crate::constants::node_filter_cte(&n.id),
            column: DEFAULT_PRIMARY_KEY.into(),
        }));
    }
    None
}
