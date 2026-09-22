use crate::input::*;
use ontology::Ontology;
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

pub enum HopStrategy {
    EdgeScan { table: String, dedup: bool },
    FkJoin { fk_column: String },
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

    pub fn resolve(
        &self,
        rel: &InputRelationship,
        default_table: &str,
        chain_len: usize,
    ) -> HopStrategy {
        if rel.hops.max == 1
            && !matches!(rel.direction, Direction::Both)
            && rel.filters.is_empty()
            && rel.fk_column.is_some()
        {
            return HopStrategy::FkJoin {
                fk_column: rel.fk_column.clone().unwrap(),
            };
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

#[derive(Clone, PartialEq, Serialize)]
#[serde(tag = "op")]
pub enum PhysOp {
    Scan {
        table: String,
        alias: String,
        dedup: bool,
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
        on: JoinOn,
        kind: JoinKind,
    },
    Aggregate {
        input: Box<PhysOp>,
        group_by: Vec<GroupKey>,
        metrics: Vec<Metric>,
    },
    Union {
        arms: Vec<PhysOp>,
    },
    Sort {
        input: Box<PhysOp>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<PhysOp>,
        count: u32,
    },
}

#[derive(Clone, PartialEq, Serialize)]
pub enum JoinKind {
    Inner,
    Semi { materialize: bool },
}

#[derive(Clone, PartialEq, Serialize)]
pub struct JoinOn {
    pub left: (String, String),
    pub right: (String, String),
}

// ── Predicates ──────────────────────────────────────────────────────────────

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
}

#[derive(Clone, PartialEq, Serialize)]
pub enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    Strs(Vec<String>),
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
    NodeProperty {
        property: String,
    },
    Computed {
        expr: ColumnExpr,
        alias: String,
    },
}

#[derive(Clone, PartialEq, Serialize)]
pub enum ColumnExpr {
    Col(String, String),
    Lit(Value),
    Array(Vec<ColumnExpr>),
    Tuple(Vec<ColumnExpr>),
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
    pub column: String,
    pub desc: bool,
}

// ── PlanMetadata ────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct PlanMetadata {
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub hop_count: usize,
    pub phys_op: Option<PhysOp>,
}

// ── S-expression serialization ──────────────────────────────────────────────

impl PhysOp {
    pub fn to_sexpr(&self) -> String {
        self.fmt_sexpr(0)
    }

    fn fmt_sexpr(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);

        match self {
            PhysOp::Scan {
                table,
                alias,
                dedup,
            } => {
                let d = if *dedup { " FINAL" } else { "" };
                format!("{pad}(Scan {table} {alias}{d})")
            }
            PhysOp::Filter { input, predicates } => {
                let preds = predicates
                    .iter()
                    .map(|p| p.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{pad}(Filter [{preds}]\n{})", input.fmt_sexpr(indent + 1))
            }
            PhysOp::Project { input, columns } => {
                let cols = columns
                    .iter()
                    .map(|c| c.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{pad}(Project [{cols}]\n{})", input.fmt_sexpr(indent + 1))
            }
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => {
                let k = match kind {
                    JoinKind::Inner => "Inner",
                    JoinKind::Semi { materialize: true } => "Semi/mat",
                    JoinKind::Semi { materialize: false } => "Semi",
                };
                let on_str = if on.left.1.is_empty() && on.right.1.is_empty() {
                    on.left.0.clone()
                } else {
                    format!(
                        "{}.{} = {}.{}",
                        on.left.0, on.left.1, on.right.0, on.right.1
                    )
                };
                format!(
                    "{pad}(Join {k} ({on_str})\n{}\n{})",
                    left.fmt_sexpr(indent + 1),
                    right.fmt_sexpr(indent + 1)
                )
            }
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => {
                let gk = group_by
                    .iter()
                    .map(|g| g.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                let ms = metrics
                    .iter()
                    .map(|m| m.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!(
                    "{pad}(Agg [group: {gk}] [metrics: {ms}]\n{})",
                    input.fmt_sexpr(indent + 1)
                )
            }
            PhysOp::Union { arms } => {
                let arm_strs: Vec<String> = arms.iter().map(|a| a.fmt_sexpr(indent + 1)).collect();
                format!("{pad}(Union\n{})", arm_strs.join("\n"))
            }
            PhysOp::Sort { input, keys } => {
                if keys.is_empty() {
                    return input.fmt_sexpr(indent);
                }
                let ks = keys
                    .iter()
                    .map(|k| k.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{pad}(Sort [{ks}]\n{})", input.fmt_sexpr(indent + 1))
            }
            PhysOp::Limit { input, count } => {
                format!("{pad}(Limit {count}\n{})", input.fmt_sexpr(indent + 1))
            }
        }
    }
}

impl Predicate {
    fn to_sexpr(&self) -> String {
        match self {
            Predicate::Eq { column, value } => {
                if column == "_deleted" && matches!(value, Value::Bool(false)) {
                    return "!deleted".to_string();
                }
                format!("{column}={}", value.to_sexpr())
            }
            Predicate::In { column, values } => {
                if values.len() > 5 {
                    let first3 = values[..3]
                        .iter()
                        .map(|v| v.to_sexpr())
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("{column}∈[{first3},…+{}]", values.len() - 3)
                } else {
                    let vs = values
                        .iter()
                        .map(|v| v.to_sexpr())
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("{column}∈[{vs}]")
                }
            }
            Predicate::Range { column, start, end } => format!("{column}∈{start}..{end}"),
            Predicate::NodeFilter { property, filter } => {
                let op = filter.op.as_ref().map(|o| o.as_ref()).unwrap_or("eq");
                format!("{property}:{op}")
            }
            Predicate::Func {
                name,
                column,
                value,
            } => format!("{name}({column},{value})", value = value.to_sexpr()),
            Predicate::ScopePrefix(_) => "scope(…)".to_string(),
        }
    }
}

impl Value {
    fn to_sexpr(&self) -> String {
        match self {
            Value::Int(i) => i.to_string(),
            Value::Str(s) => format!("\"{s}\""),
            Value::Bool(b) => b.to_string(),
            Value::Strs(ss) => {
                let items = ss
                    .iter()
                    .map(|s| format!("\"{s}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("[{items}]")
            }
        }
    }
}

impl ProjectedColumn {
    fn to_sexpr(&self) -> String {
        match self {
            ProjectedColumn::Ref {
                table,
                column,
                alias,
            } => {
                if table.is_empty() {
                    if column == alias {
                        column.clone()
                    } else {
                        format!("{column}:{alias}")
                    }
                } else {
                    format!("{table}.{column}:{alias}")
                }
            }
            ProjectedColumn::NodeProperty { property } => format!("@{property}"),
            ProjectedColumn::Computed { expr, alias } => match expr {
                ColumnExpr::Lit(v) => format!("{}:{alias}", v.to_sexpr()),
                _ => format!("({}):{alias}", expr.to_sexpr()),
            },
        }
    }
}

impl ColumnExpr {
    fn to_sexpr(&self) -> String {
        match self {
            ColumnExpr::Col(table, col) => format!("{table}.{col}"),
            ColumnExpr::Lit(v) => v.to_sexpr(),
            ColumnExpr::Array(items) => {
                let inner = items
                    .iter()
                    .map(|i| i.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("[{inner}]")
            }
            ColumnExpr::Tuple(items) => {
                let inner = items
                    .iter()
                    .map(|i| i.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("({inner})")
            }
        }
    }
}

impl GroupKey {
    fn to_sexpr(&self) -> String {
        let trunc = self
            .truncate
            .map(|t| format!("/{}", t.ch_function()))
            .unwrap_or_default();
        format!("{}.{}{trunc}:{}", self.node, self.property, self.alias)
    }
}

impl Metric {
    fn to_sexpr(&self) -> String {
        let func = self.function.as_sql();
        let prop = self.property.as_deref().unwrap_or("*");
        format!("{func}({}.{prop}):{}", self.node, self.alias)
    }
}

impl SortKey {
    fn to_sexpr(&self) -> String {
        let dir = if self.desc { "↓" } else { "↑" };
        format!("{}{dir}", self.column)
    }
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

    let op = match input.query_type {
        QueryType::Traversal => ctx.plan_traversal(limit),
        QueryType::Aggregation => ctx.plan_aggregation(limit),
        QueryType::Neighbors => ctx.plan_neighbors(limit),
        QueryType::PathFinding => ctx.plan_pathfinding(limit),
        QueryType::Hydration => ctx.plan_hydration(limit),
    };

    let rule_ctx = super::optimize::RuleCtx {
        input,
        graph: &graph,
    };
    let op = super::optimize::optimize(op, &rule_ctx);

    let mut nem = ctx.compute_node_edge_mappings();
    if input.query_type == QueryType::Neighbors {
        nem.insert(
            input.nodes[0].id.clone(),
            (
                "e".to_string(),
                ontology::constants::SOURCE_ID_COLUMN.to_string(),
            ),
        );
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
}

impl<'a> PlanCtx<'a> {
    fn compute_node_edge_mappings(&self) -> HashMap<String, (String, String)> {
        let mut m = HashMap::new();
        let det = &self.input.compiler.default_edge_table;
        let cl = self.input.relationships.len();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            match self.graph.resolve(rel, det, cl) {
                HopStrategy::FkJoin { fk_column } => {
                    let (fk_a, tgt_a) = self.fk_sides(rel, &fk_column);
                    m.entry(fk_a.to_string()).or_insert_with(|| {
                        (
                            fk_a.to_string(),
                            ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
                        )
                    });
                    m.entry(tgt_a.to_string())
                        .or_insert_with(|| (fk_a.to_string(), fk_column));
                }
                HopStrategy::EdgeScan { .. } => {
                    let ea = format!("e{i}");
                    let (sc, ec) = rel.direction.edge_columns();
                    m.entry(rel.from.clone())
                        .or_insert_with(|| (ea.clone(), sc.to_string()));
                    m.entry(rel.to.clone())
                        .or_insert_with(|| (ea.clone(), ec.to_string()));
                }
            }
        }
        m
    }

    fn fk_sides<'r>(&self, rel: &'r InputRelationship, fk_col: &str) -> (&'r str, &'r str) {
        let from_has = self
            .input
            .nodes
            .iter()
            .find(|n| n.id == rel.from)
            .and_then(|n| n.table.as_deref())
            .and_then(|t| self.input.compiler.table_columns.get(t))
            .is_some_and(|cols| cols.contains(fk_col));
        if from_has {
            (&rel.from, &rel.to)
        } else {
            (&rel.to, &rel.from)
        }
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
            p.push(Predicate::In {
                column: ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
                values: node.node_ids.iter().map(|&id| Value::Int(id)).collect(),
            });
        }
        if let Some(ref r) = node.id_range {
            p.push(Predicate::Range {
                column: ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
                start: r.start,
                end: r.end,
            });
        }
        p.push(Predicate::Eq {
            column: "_deleted".to_string(),
            value: Value::Bool(false),
        });
        p
    }

    fn edge_predicates(&self, rel: &InputRelationship) -> Vec<Predicate> {
        let mut p = Vec::new();
        let (sc, ec) = rel.direction.edge_columns();

        if !crate::passes::normalize::is_wildcard(&rel.types) {
            if rel.types.len() == 1 {
                p.push(Predicate::Eq {
                    column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                    value: Value::Str(rel.types[0].clone()),
                });
            } else {
                p.push(Predicate::In {
                    column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                    values: rel.types.iter().map(|t| Value::Str(t.clone())).collect(),
                });
            }
        }
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid)
                && let Some(ref ent) = n.entity
            {
                let kc = if ic == ontology::constants::SOURCE_ID_COLUMN {
                    ontology::constants::SOURCE_KIND_COLUMN
                } else {
                    ontology::constants::TARGET_KIND_COLUMN
                };
                p.push(Predicate::Eq {
                    column: kc.to_string(),
                    value: Value::Str(ent.clone()),
                });
            }
        }
        p.push(Predicate::Eq {
            column: "_deleted".to_string(),
            value: Value::Bool(false),
        });

        if let Some(ref pfx) = rel.scope_prefix {
            p.push(Predicate::ScopePrefix(pfx.clone()));
        }

        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                if !n.node_ids.is_empty() {
                    p.push(Predicate::In {
                        column: ic.to_string(),
                        values: n.node_ids.iter().map(|&id| Value::Int(id)).collect(),
                    });
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

        // Denorm tags
        let meta = &self.input.compiler;
        if !crate::passes::normalize::is_wildcard(&rel.types) {
            for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
                if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                    let ent = n.entity.as_deref().unwrap_or("");
                    let dir = if ic == ontology::constants::SOURCE_ID_COLUMN {
                        "source"
                    } else {
                        "target"
                    };
                    for (prop, fs) in &n.filters {
                        let key = (ent.to_string(), prop.clone(), dir.to_string());
                        if !meta
                            .denorm_rel_kinds
                            .get(&key)
                            .is_some_and(|ks| rel.types.iter().any(|t| ks.contains(t)))
                        {
                            continue;
                        }
                        if let Some((tc, tk)) = meta.denormalized_columns.get(&key) {
                            for f in fs {
                                match (&f.op, &f.value) {
                                    (None | Some(FilterOp::Eq), Some(val)) => {
                                        let tag_val = match val {
                                            serde_json::Value::String(s) => s.clone(),
                                            serde_json::Value::Bool(b) => b.to_string(),
                                            serde_json::Value::Number(n) => n.to_string(),
                                            _ => continue,
                                        };
                                        p.push(Predicate::Func {
                                            name: "has".to_string(),
                                            column: tc.clone(),
                                            value: Value::Str(format!("{tk}:{tag_val}")),
                                        });
                                    }
                                    (Some(FilterOp::In), Some(serde_json::Value::Array(arr))) => {
                                        let tags: Vec<Value> = arr
                                            .iter()
                                            .filter_map(|v| {
                                                let s = match v {
                                                    serde_json::Value::String(s) => s.clone(),
                                                    serde_json::Value::Bool(b) => b.to_string(),
                                                    serde_json::Value::Number(n) => n.to_string(),
                                                    _ => return None,
                                                };
                                                Some(Value::Str(format!("{tk}:{s}")))
                                            })
                                            .collect();
                                        if tags.len() == 1 {
                                            p.push(Predicate::Func {
                                                name: "has".to_string(),
                                                column: tc.clone(),
                                                value: tags.into_iter().next().unwrap(),
                                            });
                                        } else if !tags.is_empty() {
                                            let strs: Vec<String> = tags
                                                .into_iter()
                                                .map(|v| match v {
                                                    Value::Str(s) => s,
                                                    _ => String::new(),
                                                })
                                                .collect();
                                            p.push(Predicate::Func {
                                                name: "hasAny".to_string(),
                                                column: tc.clone(),
                                                value: Value::Strs(strs),
                                            });
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        // Push node filters onto edge when edge table has the column
        let et = self.graph.edge_table(&rel.types, &meta.default_edge_table);
        if let Some(ecols) = meta.table_columns.get(&et) {
            let reserved: HashSet<&str> = ontology::constants::EDGE_RESERVED_COLUMNS
                .iter()
                .copied()
                .collect();
            let mut seen: HashSet<&str> = HashSet::new();
            for nid in [&rel.from, &rel.to] {
                if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                    for (prop, fs) in &n.filters {
                        if ecols.contains(prop)
                            && !reserved.contains(prop.as_str())
                            && seen.insert(prop.as_str())
                        {
                            for f in fs {
                                p.push(Predicate::NodeFilter {
                                    property: prop.clone(),
                                    filter: f.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        p
    }

    fn needs_node_join(&self, node: &InputNode) -> bool {
        let a = &node.id;
        let input = self.input;
        let has_filters =
            !node.filters.is_empty() || !node.node_ids.is_empty() || node.id_range.is_some();
        let in_group_by = input
            .aggregation
            .group_by
            .iter()
            .any(|g| g.node() == a.as_str());
        let in_agg_prop = input.aggregation.metrics.iter().any(|m| {
            m.expr.node() == a.as_str()
                && m.expr.property().is_some()
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
}

// ── Traversal / Aggregation ─────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    fn plan_traversal(&self, limit: u32) -> PhysOp {
        let chain = self.plan_chain();
        let mut columns = Vec::new();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            if rel.hops.max > 1 {
                let prefix = format!("hop_{ea}");
                for (col, suffix) in crate::constants::EDGE_ALIAS_SUFFIXES.iter().enumerate() {
                    columns.push(ProjectedColumn::Ref {
                        table: ea.clone(),
                        column: ontology::constants::EDGE_RESERVED_COLUMNS[col].to_string(),
                        alias: format!("{prefix}_{suffix}"),
                    });
                }
                columns.push(ProjectedColumn::Ref {
                    table: ea.clone(),
                    column: crate::constants::PATH_NODES_COLUMN.to_string(),
                    alias: format!("{prefix}_{}", crate::constants::PATH_NODES_COLUMN),
                });
            } else {
                for (col_idx, suffix) in crate::constants::EDGE_ALIAS_SUFFIXES.iter().enumerate() {
                    columns.push(ProjectedColumn::Ref {
                        table: ea.clone(),
                        column: ontology::constants::EDGE_RESERVED_COLUMNS[col_idx].to_string(),
                        alias: format!("{ea}_{suffix}"),
                    });
                }
            }
        }
        for node in &self.input.nodes {
            for col in crate::passes::shared::requested_columns(&node.columns) {
                columns.push(ProjectedColumn::NodeProperty { property: col });
            }
        }
        let sorted = PhysOp::Sort {
            input: Box::new(chain),
            keys: self.sort_keys(),
        };
        let projected = PhysOp::Project {
            input: Box::new(sorted),
            columns,
        };
        PhysOp::Limit {
            input: Box::new(projected),
            count: limit,
        }
    }

    fn plan_aggregation(&self, limit: u32) -> PhysOp {
        let chain = self.plan_chain();
        let mut group_by = Vec::new();
        for g in &self.input.aggregation.group_by {
            match g {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    alias,
                    ..
                } => {
                    group_by.push(GroupKey {
                        node: node.clone(),
                        property: property.clone(),
                        truncate: *truncate,
                        alias: alias
                            .as_ref()
                            .cloned()
                            .unwrap_or_else(|| format!("{node}_{property}")),
                    });
                }
                InputGroupByKey::Node { node, alias, .. } => {
                    for col in self
                        .input
                        .nodes
                        .iter()
                        .find(|n| &n.id == node)
                        .map(|n| crate::passes::shared::requested_columns(&n.columns))
                        .unwrap_or_default()
                    {
                        group_by.push(GroupKey {
                            node: node.clone(),
                            property: col,
                            truncate: None,
                            alias: alias.as_ref().cloned().unwrap_or_else(|| node.clone()),
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
        if let Some(ref sort) = self.input.aggregation.sort {
            keys.push(SortKey {
                column: sort.column.clone(),
                desc: matches!(sort.direction, OrderDirection::Desc),
            });
        }
        if self.input.cursor.is_some() {
            for gk in &group_by {
                keys.push(SortKey {
                    column: format!("{}.{}", gk.node, gk.property),
                    desc: false,
                });
            }
        }
        PhysOp::Limit {
            input: Box::new(PhysOp::Sort {
                input: Box::new(PhysOp::Aggregate {
                    input: Box::new(chain),
                    group_by,
                    metrics,
                }),
                keys,
            }),
            count: limit,
        }
    }

    fn plan_chain(&self) -> PhysOp {
        if self.input.relationships.is_empty() {
            let n = &self.input.nodes[0];
            return self.filtered_node_scan(n);
        }

        let det = &self.input.compiler.default_edge_table;
        let mut tree: Option<PhysOp> = None;

        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let (sc, _) = rel.direction.edge_columns();
            let et = self.graph.edge_table(&rel.types, det);

            let edge = if rel.hops.max > 1 {
                self.build_multi_hop(rel, &ea, &et)
            } else {
                PhysOp::Filter {
                    input: Box::new(PhysOp::Scan {
                        table: et,
                        alias: ea.clone(),
                        dedup: false,
                    }),
                    predicates: self.edge_predicates(rel),
                }
            };

            tree = Some(match tree {
                None => edge,
                Some(prev) => {
                    let prev_col = if i > 0 {
                        let (_, pe) = self.input.relationships[i - 1].direction.edge_columns();
                        (format!("e{}", i - 1), pe.to_string())
                    } else {
                        (ea.clone(), sc.to_string())
                    };
                    PhysOp::Join {
                        left: Box::new(prev),
                        right: Box::new(edge),
                        on: JoinOn {
                            left: prev_col,
                            right: (ea.clone(), sc.to_string()),
                        },
                        kind: JoinKind::Inner,
                    }
                }
            });
        }

        let mut hydrated: HashSet<String> = HashSet::new();
        for (i, rel) in self.input.relationships.iter().enumerate() {
            let ea = format!("e{i}");
            let (sc, ec) = rel.direction.edge_columns();
            for (na, col) in [(&rel.from, sc), (&rel.to, ec)] {
                if !hydrated.insert(na.clone()) {
                    continue;
                }
                let Some(n) = self.input.nodes.iter().find(|n| &n.id == na) else {
                    continue;
                };
                if !self.needs_node_join(n) {
                    continue;
                }
                tree = Some(PhysOp::Join {
                    left: Box::new(tree.unwrap()),
                    right: Box::new(self.filtered_node_scan(n)),
                    on: JoinOn {
                        left: (ea.clone(), col.to_string()),
                        right: (
                            na.clone(),
                            ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
                        ),
                    },
                    kind: JoinKind::Inner,
                });
            }
        }

        tree.unwrap()
    }

    fn filtered_node_scan(&self, n: &InputNode) -> PhysOp {
        PhysOp::Filter {
            input: Box::new(PhysOp::Scan {
                table: n.table.as_deref().unwrap_or("").to_string(),
                alias: n.id.clone(),
                dedup: true,
            }),
            predicates: self.node_predicates(n),
        }
    }

    fn build_multi_hop(&self, rel: &InputRelationship, _alias: &str, edge_table: &str) -> PhysOp {
        let (sc, ec) = rel.direction.edge_columns();
        let end_type_col = match rel.direction {
            Direction::Outgoing | Direction::Both => ontology::constants::TARGET_KIND_COLUMN,
            Direction::Incoming => ontology::constants::SOURCE_KIND_COLUMN,
        };

        let arms: Vec<PhysOp> = (rel.hops.min.max(1)..=rel.hops.max)
            .map(|depth| self.build_depth_arm(depth, edge_table, sc, ec, end_type_col, rel))
            .collect();

        let inner = if arms.len() == 1 {
            arms.into_iter().next().unwrap()
        } else {
            PhysOp::Union { arms }
        };

        let mut outer_preds = Vec::new();
        let (fk, tk) = match rel.direction {
            Direction::Outgoing | Direction::Both => (
                ontology::constants::SOURCE_KIND_COLUMN,
                ontology::constants::TARGET_KIND_COLUMN,
            ),
            Direction::Incoming => (
                ontology::constants::TARGET_KIND_COLUMN,
                ontology::constants::SOURCE_KIND_COLUMN,
            ),
        };
        let (from_id_col, to_id_col) = rel.direction.edge_columns();
        for (na, kc, ic) in [(&rel.from, fk, from_id_col), (&rel.to, tk, to_id_col)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == na) {
                if let Some(ref e) = n.entity {
                    outer_preds.push(Predicate::Eq {
                        column: kc.to_string(),
                        value: Value::Str(e.clone()),
                    });
                }
                if !n.node_ids.is_empty() {
                    outer_preds.push(Predicate::In {
                        column: ic.to_string(),
                        values: n.node_ids.iter().map(|&id| Value::Int(id)).collect(),
                    });
                }
            }
        }
        outer_preds.push(Predicate::Eq {
            column: "_deleted".to_string(),
            value: Value::Bool(false),
        });

        PhysOp::Filter {
            input: Box::new(inner),
            predicates: outer_preds,
        }
    }

    fn build_depth_arm(
        &self,
        depth: u32,
        edge_table: &str,
        start_col: &str,
        end_col: &str,
        end_type_col: &str,
        rel: &InputRelationship,
    ) -> PhysOp {
        let mut preds = Vec::new();
        if !crate::passes::normalize::is_wildcard(&rel.types) {
            if rel.types.len() == 1 {
                preds.push(Predicate::Eq {
                    column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                    value: Value::Str(rel.types[0].clone()),
                });
            } else {
                preds.push(Predicate::In {
                    column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                    values: rel.types.iter().map(|t| Value::Str(t.clone())).collect(),
                });
            }
        }
        preds.push(Predicate::Eq {
            column: "_deleted".to_string(),
            value: Value::Bool(false),
        });
        if let Some(ref pfx) = rel.scope_prefix {
            preds.push(Predicate::ScopePrefix(pfx.clone()));
        }

        let mut chain = PhysOp::Filter {
            input: Box::new(PhysOp::Scan {
                table: edge_table.to_string(),
                alias: "e1".to_string(),
                dedup: false,
            }),
            predicates: preds,
        };
        for j in 2..=depth {
            let mut join_preds = vec![Predicate::Eq {
                column: "_deleted".to_string(),
                value: Value::Bool(false),
            }];
            if !crate::passes::normalize::is_wildcard(&rel.types) {
                if rel.types.len() == 1 {
                    join_preds.push(Predicate::Eq {
                        column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                        value: Value::Str(rel.types[0].clone()),
                    });
                } else {
                    join_preds.push(Predicate::In {
                        column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                        values: rel.types.iter().map(|t| Value::Str(t.clone())).collect(),
                    });
                }
            }
            chain = PhysOp::Join {
                left: Box::new(chain),
                right: Box::new(PhysOp::Filter {
                    input: Box::new(PhysOp::Scan {
                        table: edge_table.to_string(),
                        alias: format!("e{j}"),
                        dedup: false,
                    }),
                    predicates: join_preds,
                }),
                on: JoinOn {
                    left: (format!("e{}", j - 1), end_col.to_string()),
                    right: (format!("e{j}"), start_col.to_string()),
                },
                kind: JoinKind::Inner,
            };
        }

        let _last = format!("e{depth}");
        let last = format!("e{depth}");
        let proj_cols = vec![
            ProjectedColumn::Ref {
                table: "e1".to_string(),
                column: start_col.to_string(),
                alias: start_col.to_string(),
            },
            ProjectedColumn::Ref {
                table: last.clone(),
                column: end_col.to_string(),
                alias: end_col.to_string(),
            },
            ProjectedColumn::Ref {
                table: "e1".to_string(),
                column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                alias: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
            },
            ProjectedColumn::Computed {
                expr: ColumnExpr::Array(
                    (1..=depth)
                        .map(|i| {
                            let e = format!("e{i}");
                            ColumnExpr::Tuple(vec![
                                ColumnExpr::Col(e.clone(), end_col.to_string()),
                                ColumnExpr::Col(e, end_type_col.to_string()),
                            ])
                        })
                        .collect(),
                ),
                alias: crate::constants::PATH_NODES_COLUMN.to_string(),
            },
            ProjectedColumn::Computed {
                expr: ColumnExpr::Lit(Value::Int(depth as i64)),
                alias: crate::constants::DEPTH_COLUMN.to_string(),
            },
            ProjectedColumn::Ref {
                table: "e1".to_string(),
                column: "_deleted".to_string(),
                alias: "_deleted".to_string(),
            },
            ProjectedColumn::Ref {
                table: "e1".to_string(),
                column: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
                alias: ontology::constants::TRAVERSAL_PATH_COLUMN.to_string(),
            },
        ];

        PhysOp::Project {
            input: Box::new(chain),
            columns: proj_cols,
        }
    }

    fn sort_keys(&self) -> Vec<SortKey> {
        self.input
            .order_by
            .as_ref()
            .map(|ob| {
                vec![SortKey {
                    column: format!("{}.{}", ob.node, ob.property),
                    desc: matches!(ob.direction, OrderDirection::Desc),
                }]
            })
            .unwrap_or_default()
    }
}

// ── Neighbors ───────────────────────────────────────────────────────────────

impl<'a> PlanCtx<'a> {
    fn plan_neighbors(&self, limit: u32) -> PhysOp {
        let config = self.input.neighbors.as_ref().expect("neighbors config");
        let center = &self.input.nodes[0];
        let center_entity = center.entity.as_deref().unwrap_or("");
        let et = self
            .graph
            .edge_table(&config.rel_types, &self.input.compiler.default_edge_table);

        let build_arm = |dir: Direction| -> PhysOp {
            let (center_col, center_kind, neighbor_id, neighbor_kind, is_out) = match dir {
                Direction::Outgoing => (
                    ontology::constants::SOURCE_ID_COLUMN,
                    ontology::constants::SOURCE_KIND_COLUMN,
                    ontology::constants::TARGET_ID_COLUMN,
                    ontology::constants::TARGET_KIND_COLUMN,
                    1i64,
                ),
                Direction::Incoming => (
                    ontology::constants::TARGET_ID_COLUMN,
                    ontology::constants::TARGET_KIND_COLUMN,
                    ontology::constants::SOURCE_ID_COLUMN,
                    ontology::constants::SOURCE_KIND_COLUMN,
                    0i64,
                ),
                Direction::Both => unreachable!(),
            };
            let mut preds = vec![Predicate::Eq {
                column: center_kind.to_string(),
                value: Value::Str(center_entity.to_string()),
            }];
            if !center.node_ids.is_empty() {
                preds.push(Predicate::In {
                    column: center_col.to_string(),
                    values: center.node_ids.iter().map(|&id| Value::Int(id)).collect(),
                });
            }
            if !crate::passes::normalize::is_wildcard(&config.rel_types) {
                if config.rel_types.len() == 1 {
                    preds.push(Predicate::Eq {
                        column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                        value: Value::Str(config.rel_types[0].clone()),
                    });
                } else {
                    preds.push(Predicate::In {
                        column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                        values: config
                            .rel_types
                            .iter()
                            .map(|t| Value::Str(t.clone()))
                            .collect(),
                    });
                }
            }
            preds.push(Predicate::Eq {
                column: "_deleted".to_string(),
                value: Value::Bool(false),
            });

            let columns = vec![
                ProjectedColumn::Ref {
                    table: "e".to_string(),
                    column: neighbor_id.to_string(),
                    alias: crate::constants::neighbor_id_column().to_string(),
                },
                ProjectedColumn::Ref {
                    table: "e".to_string(),
                    column: neighbor_kind.to_string(),
                    alias: crate::constants::neighbor_type_column().to_string(),
                },
                ProjectedColumn::Ref {
                    table: "e".to_string(),
                    column: ontology::constants::RELATIONSHIP_KIND_COLUMN.to_string(),
                    alias: crate::constants::relationship_type_column().to_string(),
                },
                ProjectedColumn::Computed {
                    expr: ColumnExpr::Lit(Value::Int(is_out)),
                    alias: crate::constants::neighbor_is_outgoing_column().to_string(),
                },
                ProjectedColumn::Ref {
                    table: "e".to_string(),
                    column: center_col.to_string(),
                    alias: crate::constants::redaction_id_column(&center.id),
                },
                ProjectedColumn::Computed {
                    expr: ColumnExpr::Lit(Value::Str(center_entity.to_string())),
                    alias: crate::constants::redaction_type_column(&center.id),
                },
            ];

            let scan = PhysOp::Filter {
                input: Box::new(PhysOp::Scan {
                    table: et.clone(),
                    alias: "e".to_string(),
                    dedup: false,
                }),
                predicates: preds,
            };

            let has_non_denorm = center.filters.iter().any(|(prop, _)| {
                let src = self.input.compiler.denormalized_columns.contains_key(&(
                    center_entity.to_string(),
                    prop.clone(),
                    "source".to_string(),
                ));
                let tgt = self.input.compiler.denormalized_columns.contains_key(&(
                    center_entity.to_string(),
                    prop.clone(),
                    "target".to_string(),
                ));
                !src && !tgt
            }) || center.id_range.is_some();

            let base = if has_non_denorm {
                PhysOp::Join {
                    left: Box::new(scan),
                    right: Box::new(PhysOp::Filter {
                        input: Box::new(PhysOp::Scan {
                            table: center.table.as_deref().unwrap_or("").to_string(),
                            alias: center.id.clone(),
                            dedup: true,
                        }),
                        predicates: self.node_predicates(center),
                    }),
                    on: JoinOn {
                        left: ("e".to_string(), center_col.to_string()),
                        right: (
                            center.id.clone(),
                            ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
                        ),
                    },
                    kind: JoinKind::Inner,
                }
            } else {
                scan
            };

            PhysOp::Project {
                input: Box::new(base),
                columns,
            }
        };

        let body = match config.direction {
            Direction::Both => PhysOp::Union {
                arms: vec![
                    build_arm(Direction::Outgoing),
                    build_arm(Direction::Incoming),
                ],
            },
            dir => build_arm(dir),
        };

        PhysOp::Limit {
            input: Box::new(PhysOp::Sort {
                input: Box::new(body),
                keys: self.sort_keys(),
            }),
            count: limit,
        }
    }

    // ── Pathfinding ─────────────────────────────────────────────────────────

    fn plan_pathfinding(&self, limit: u32) -> PhysOp {
        let cfg = self.input.path.as_ref().expect("path config");
        let start = self
            .input
            .nodes
            .iter()
            .find(|n| n.id == cfg.from)
            .expect("start");
        let end = self
            .input
            .nodes
            .iter()
            .find(|n| n.id == cfg.to)
            .expect("end");
        let et = self
            .graph
            .edge_table(&cfg.rel_types, &self.input.compiler.default_edge_table);
        let max_depth = cfg.max_depth;
        let fwd_depth = max_depth / 2 + max_depth % 2;
        let bwd_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

        let frontier = |depth: u32| -> PhysOp {
            let arms: Vec<PhysOp> = (1..=depth)
                .map(|d| {
                    let mut chain = PhysOp::Scan {
                        table: et.clone(),
                        alias: "e1".to_string(),
                        dedup: false,
                    };
                    for j in 2..=d {
                        chain = PhysOp::Join {
                            left: Box::new(chain),
                            right: Box::new(PhysOp::Scan {
                                table: et.clone(),
                                alias: format!("e{j}"),
                                dedup: false,
                            }),
                            on: JoinOn {
                                left: (
                                    format!("e{}", j - 1),
                                    ontology::constants::TARGET_ID_COLUMN.to_string(),
                                ),
                                right: (
                                    format!("e{j}"),
                                    ontology::constants::SOURCE_ID_COLUMN.to_string(),
                                ),
                            },
                            kind: JoinKind::Inner,
                        };
                    }
                    chain
                })
                .collect();
            if arms.len() == 1 {
                arms.into_iter().next().unwrap()
            } else {
                PhysOp::Union { arms }
            }
        };

        let start_scan = PhysOp::Filter {
            input: Box::new(PhysOp::Scan {
                table: start.table.as_deref().unwrap_or("").to_string(),
                alias: start.id.clone(),
                dedup: true,
            }),
            predicates: self.node_predicates(start),
        };
        let end_scan = PhysOp::Filter {
            input: Box::new(PhysOp::Scan {
                table: end.table.as_deref().unwrap_or("").to_string(),
                alias: end.id.clone(),
                dedup: true,
            }),
            predicates: self.node_predicates(end),
        };

        let fwd_body = frontier(fwd_depth);

        let consumer = if bwd_depth > 0 {
            let bwd_body = frontier(bwd_depth);
            let direct = PhysOp::Scan {
                table: "forward".to_string(),
                alias: "f".to_string(),
                dedup: false,
            };
            let intersection = PhysOp::Join {
                left: Box::new(PhysOp::Scan {
                    table: "forward".to_string(),
                    alias: "f".to_string(),
                    dedup: false,
                }),
                right: Box::new(PhysOp::Scan {
                    table: "backward".to_string(),
                    alias: "b".to_string(),
                    dedup: false,
                }),
                on: JoinOn {
                    left: ("f".to_string(), "end_id".to_string()),
                    right: ("b".to_string(), "end_id".to_string()),
                },
                kind: JoinKind::Inner,
            };
            let union = PhysOp::Union {
                arms: vec![direct, intersection],
            };
            // Nest CTEs inside-out: backward → end → forward → start
            let r = PhysOp::Join {
                left: Box::new(union),
                right: Box::new(bwd_body),
                on: JoinOn {
                    left: ("backward".into(), String::new()),
                    right: (String::new(), String::new()),
                },
                kind: JoinKind::Semi { materialize: true },
            };
            let r = PhysOp::Join {
                left: Box::new(r),
                right: Box::new(end_scan),
                on: JoinOn {
                    left: ("_nf_end".into(), String::new()),
                    right: (String::new(), String::new()),
                },
                kind: JoinKind::Semi { materialize: true },
            };
            let r = PhysOp::Join {
                left: Box::new(r),
                right: Box::new(fwd_body),
                on: JoinOn {
                    left: ("forward".into(), String::new()),
                    right: (String::new(), String::new()),
                },
                kind: JoinKind::Semi { materialize: true },
            };
            PhysOp::Join {
                left: Box::new(r),
                right: Box::new(start_scan),
                on: JoinOn {
                    left: ("_nf_start".into(), String::new()),
                    right: (String::new(), String::new()),
                },
                kind: JoinKind::Semi { materialize: true },
            }
        } else {
            let direct = PhysOp::Scan {
                table: "forward".to_string(),
                alias: "f".to_string(),
                dedup: false,
            };
            let r = PhysOp::Join {
                left: Box::new(direct),
                right: Box::new(fwd_body),
                on: JoinOn {
                    left: ("forward".into(), String::new()),
                    right: (String::new(), String::new()),
                },
                kind: JoinKind::Semi { materialize: true },
            };
            PhysOp::Join {
                left: Box::new(r),
                right: Box::new(start_scan),
                on: JoinOn {
                    left: ("_nf_start".into(), String::new()),
                    right: (String::new(), String::new()),
                },
                kind: JoinKind::Semi { materialize: true },
            }
        };
        let paths = consumer;

        PhysOp::Limit {
            input: Box::new(paths),
            count: limit,
        }
    }

    // ── Hydration ───────────────────────────────────────────────────────────

    fn plan_hydration(&self, limit: u32) -> PhysOp {
        let arms: Vec<PhysOp> = self
            .input
            .nodes
            .iter()
            .map(|n| {
                let mut preds = Vec::new();
                if !n.node_ids.is_empty() {
                    preds.push(Predicate::In {
                        column: n.id_property.clone(),
                        values: n.node_ids.iter().map(|&id| Value::Int(id)).collect(),
                    });
                }
                preds.push(Predicate::Eq {
                    column: "_deleted".to_string(),
                    value: Value::Bool(false),
                });

                let _cols = match &n.columns {
                    Some(ColumnSelection::List(cs)) => cs.clone(),
                    _ => vec![],
                };
                let entity = n.entity.as_deref().unwrap_or("");
                let columns = vec![
                    ProjectedColumn::Ref {
                        table: n.id.clone(),
                        column: n.id_property.clone(),
                        alias: format!("{}_{}", n.id, n.id_property),
                    },
                    ProjectedColumn::Computed {
                        expr: ColumnExpr::Lit(Value::Str(entity.to_string())),
                        alias: format!("{}_entity_type", n.id),
                    },
                ];

                PhysOp::Project {
                    input: Box::new(PhysOp::Filter {
                        input: Box::new(PhysOp::Scan {
                            table: n.table.as_deref().unwrap_or("").to_string(),
                            alias: n.id.clone(),
                            dedup: false,
                        }),
                        predicates: preds,
                    }),
                    columns,
                }
            })
            .collect();

        let body = if arms.len() == 1 {
            arms.into_iter().next().unwrap()
        } else {
            PhysOp::Union { arms }
        };

        PhysOp::Limit {
            input: Box::new(body),
            count: limit,
        }
    }
}
