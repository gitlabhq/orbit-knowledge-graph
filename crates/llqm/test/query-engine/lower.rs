//! Prototype lowering: Input → Plan.
//!
//! Replicates query-engine's `lower.rs` patterns using the `Rel` chainable
//! API. NOT production code — this is a stress test of API expressiveness.
//! The real lowerer lives in query-engine.
//!
//! Each `lower_*` function builds a complete `Plan` directly — no intermediate
//! accumulator struct. Post-lowering passes (enforce, security) operate on the
//! `Plan` via tree-surgery methods: `extend_project`, `insert_project_after`,
//! `extend_aggregate_groups`, `inject_filter`.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use llqm::ir::expr::{self, DataType, Expr, JoinType, SortDir};
use llqm::ir::plan::{CteDef, Measure, Plan, Rel};

// ─────────────────────────────────────────────────────────────────────────────
// Constants (mirrors ontology::constants + query-engine::constants)
// ─────────────────────────────────────────────────────────────────────────────

const DEFAULT_PRIMARY_KEY: &str = "id";
const EDGE_TABLE: &str = "gl_edge";
const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";

const EDGE_RESERVED_COLUMNS: &[&str] = &[
    "traversal_path",
    "relationship_kind",
    "source_id",
    "source_kind",
    "target_id",
    "target_kind",
];

const EDGE_ALIAS_SUFFIXES: &[&str] = &["path", "type", "src", "src_type", "dst", "dst_type"];

const NEIGHBOR_ID_COLUMN: &str = "_gkg_neighbor_id";
const NEIGHBOR_TYPE_COLUMN: &str = "_gkg_neighbor_type";
const RELATIONSHIP_TYPE_COLUMN: &str = "_gkg_relationship_type";

// ─────────────────────────────────────────────────────────────────────────────
// Minimal input types (self-contained for prototype)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Traversal,
    Search,
    Aggregation,
    PathFinding,
    Neighbors,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Direction {
    #[default]
    Outgoing,
    Incoming,
    Both,
}

impl Direction {
    fn edge_columns(self) -> (&'static str, &'static str) {
        match self {
            Direction::Outgoing | Direction::Both => ("source_id", "target_id"),
            Direction::Incoming => ("target_id", "source_id"),
        }
    }

    fn union_columns(self) -> (&'static str, &'static str) {
        match self {
            Direction::Outgoing | Direction::Both => ("start_id", "end_id"),
            Direction::Incoming => ("end_id", "start_id"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    In,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone)]
pub struct InputFilter {
    pub op: Option<FilterOp>,
    pub value: Option<LiteralVal>,
}

/// Simplified literal for the prototype (production uses serde_json::Value).
#[derive(Debug, Clone)]
pub enum LiteralVal {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    IntArray(Vec<i64>),
    StrArray(Vec<String>),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl From<OrderDirection> for SortDir {
    fn from(d: OrderDirection) -> Self {
        match d {
            OrderDirection::Asc => SortDir::Asc,
            OrderDirection::Desc => SortDir::Desc,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InputNode {
    pub id: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub columns: Option<Vec<String>>,
    pub filters: HashMap<String, InputFilter>,
    pub node_ids: Vec<i64>,
    pub has_traversal_path: bool,
}

#[derive(Debug, Clone)]
pub struct InputRelationship {
    pub types: Vec<String>,
    pub from: String,
    pub to: String,
    pub min_hops: u32,
    pub max_hops: u32,
    pub direction: Direction,
    pub filters: HashMap<String, InputFilter>,
}

impl Default for InputRelationship {
    fn default() -> Self {
        Self {
            types: Vec::new(),
            from: String::new(),
            to: String::new(),
            min_hops: 1,
            max_hops: 1,
            direction: Direction::default(),
            filters: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InputAggregation {
    pub function: String,
    pub target: Option<String>,
    pub group_by: Option<String>,
    pub property: Option<String>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InputPath {
    pub from: String,
    pub to: String,
    pub max_depth: u32,
    pub rel_types: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct InputNeighbors {
    pub node: String,
    pub direction: Direction,
    pub rel_types: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct InputOrderBy {
    pub node: String,
    pub property: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub struct InputAggSort {
    pub agg_index: usize,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub struct Input {
    pub query_type: QueryType,
    pub nodes: Vec<InputNode>,
    pub relationships: Vec<InputRelationship>,
    pub aggregations: Vec<InputAggregation>,
    pub path: Option<InputPath>,
    pub neighbors: Option<InputNeighbors>,
    pub limit: u32,
    pub order_by: Option<InputOrderBy>,
    pub aggregation_sort: Option<InputAggSort>,
}

impl Default for Input {
    fn default() -> Self {
        Self {
            query_type: QueryType::Traversal,
            nodes: Vec::new(),
            relationships: Vec::new(),
            aggregations: Vec::new(),
            path: None,
            neighbors: None,
            limit: 30,
            order_by: None,
            aggregation_sort: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

pub fn lower(input: &Input) -> Result<Plan, String> {
    match input.query_type {
        QueryType::Traversal | QueryType::Search => lower_traversal(input),
        QueryType::Aggregation => lower_aggregation(input),
        QueryType::PathFinding => lower_path_finding(input),
        QueryType::Neighbors => lower_neighbors(input),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Table schema helpers
// ─────────────────────────────────────────────────────────────────────────────

fn read_node(node: &InputNode, extra_columns: &[&str]) -> Result<Rel, String> {
    let table = node
        .table
        .as_deref()
        .ok_or_else(|| format!("node '{}' has no resolved table", node.id))?;

    let mut columns: Vec<(&str, DataType)> = vec![
        (DEFAULT_PRIMARY_KEY, DataType::Int64),
        (TRAVERSAL_PATH_COLUMN, DataType::String),
    ];

    if let Some(cols) = &node.columns {
        for col in cols {
            if col != DEFAULT_PRIMARY_KEY && col != TRAVERSAL_PATH_COLUMN {
                columns.push((col, DataType::String));
            }
        }
    }

    for prop in node.filters.keys() {
        if !columns.iter().any(|(n, _)| *n == prop.as_str()) {
            columns.push((prop, DataType::String));
        }
    }

    for col in extra_columns {
        if !columns.iter().any(|(n, _)| *n == *col) {
            columns.push((col, DataType::String));
        }
    }

    let col_specs: Vec<(String, DataType)> = columns
        .iter()
        .map(|(n, dt)| (n.to_string(), dt.clone()))
        .collect();
    let col_refs: Vec<(&str, DataType)> = col_specs
        .iter()
        .map(|(n, dt)| (n.as_str(), dt.clone()))
        .collect();

    Ok(Rel::read(table, &node.id, &col_refs))
}

fn read_edge(alias: &str) -> Rel {
    Rel::read(
        EDGE_TABLE,
        alias,
        &[
            (TRAVERSAL_PATH_COLUMN, DataType::String),
            ("relationship_kind", DataType::String),
            ("source_id", DataType::Int64),
            ("source_kind", DataType::String),
            ("target_id", DataType::Int64),
            ("target_kind", DataType::String),
        ],
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression helpers
// ─────────────────────────────────────────────────────────────────────────────

fn edge_path_starts_with(edge_alias: &str, node_alias: &str) -> Expr {
    expr::col(edge_alias, TRAVERSAL_PATH_COLUMN)
        .starts_with(expr::col(node_alias, TRAVERSAL_PATH_COLUMN))
}

fn edge_select_items(alias: &str) -> Vec<(Expr, String)> {
    EDGE_RESERVED_COLUMNS
        .iter()
        .zip(EDGE_ALIAS_SUFFIXES.iter())
        .map(|(col, suffix)| (expr::col(alias, col), format!("{alias}_{suffix}")))
        .collect()
}

fn filter_expr(table: &str, column: &str, filter: &InputFilter) -> Expr {
    let col = expr::col(table, column);
    let val = || literal_from_val(filter.value.as_ref());

    match filter.op {
        None | Some(FilterOp::Eq) => col.eq(val()),
        Some(FilterOp::Gt) => col.gt(val()),
        Some(FilterOp::Lt) => col.lt(val()),
        Some(FilterOp::Gte) => col.ge(val()),
        Some(FilterOp::Lte) => col.le(val()),
        Some(FilterOp::In) => col.is_in(val()),
        Some(FilterOp::Contains) => like_pattern(col, filter, "%", "%"),
        Some(FilterOp::StartsWith) => like_pattern(col, filter, "", "%"),
        Some(FilterOp::EndsWith) => like_pattern(col, filter, "%", ""),
        Some(FilterOp::IsNull) => col.is_null(),
        Some(FilterOp::IsNotNull) => col.is_not_null(),
    }
}

fn like_pattern(col: Expr, filter: &InputFilter, prefix: &str, suffix: &str) -> Expr {
    let s = match &filter.value {
        Some(LiteralVal::Str(s)) => s.as_str(),
        _ => "",
    };
    col.like(expr::string(&format!("{prefix}{s}{suffix}")))
}

fn literal_from_val(v: Option<&LiteralVal>) -> Expr {
    match v {
        Some(LiteralVal::Str(s)) => expr::string(s),
        Some(LiteralVal::Int(n)) => expr::int(*n),
        Some(LiteralVal::Float(f)) => expr::float(*f),
        Some(LiteralVal::Bool(b)) => expr::boolean(*b),
        Some(LiteralVal::IntArray(_)) => Expr::Param {
            name: String::new(),
            data_type: DataType::Array(Box::new(DataType::Int64)),
        },
        Some(LiteralVal::StrArray(_)) => Expr::Param {
            name: String::new(),
            data_type: DataType::Array(Box::new(DataType::String)),
        },
        Some(LiteralVal::Null) | None => expr::null(),
    }
}

fn id_filter(table: &str, col: &str, ids: &[i64]) -> Option<Expr> {
    match ids.len() {
        0 => None,
        1 => Some(expr::col(table, col).eq(expr::int(ids[0]))),
        _ => {
            let list: Vec<Expr> = ids.iter().map(|&id| expr::int(id)).collect();
            Some(expr::col(table, col).in_list(list))
        }
    }
}

fn type_filter(types: &[String]) -> Option<Vec<String>> {
    if types.is_empty() || (types.len() == 1 && types[0] == "*") {
        None
    } else {
        Some(types.to_vec())
    }
}

fn edge_type_filter_expr(alias: &str, type_filter: &Option<Vec<String>>) -> Option<Expr> {
    let types = type_filter.as_ref()?;
    let kind = expr::col(alias, "relationship_kind");
    match types.len() {
        0 => None,
        1 => Some(kind.eq(expr::string(&types[0]))),
        _ => {
            let list: Vec<Expr> = types.iter().map(|t| expr::string(t)).collect();
            Some(kind.in_list(list))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Join condition helpers
// ─────────────────────────────────────────────────────────────────────────────

fn source_join_cond(node: &str, edge: &str, dir: Direction, with_path: bool) -> Expr {
    let node_id = expr::col(node, DEFAULT_PRIMARY_KEY);
    let id_cond = match dir {
        Direction::Outgoing => node_id.eq(expr::col(edge, "source_id")),
        Direction::Incoming => node_id.eq(expr::col(edge, "target_id")),
        Direction::Both => node_id
            .clone()
            .eq(expr::col(edge, "source_id"))
            .or(node_id.eq(expr::col(edge, "target_id"))),
    };
    if with_path {
        edge_path_starts_with(edge, node).and(id_cond)
    } else {
        id_cond
    }
}

fn source_join_cond_with_kind(
    node: &str,
    edge: &str,
    entity: &str,
    dir: Direction,
    with_path: bool,
) -> Expr {
    let id_and_kind = |id_col: &str, kind_col: &str| -> Expr {
        expr::col(node, DEFAULT_PRIMARY_KEY)
            .eq(expr::col(edge, id_col))
            .and(expr::col(edge, kind_col).eq(expr::string(entity)))
    };

    let id_cond = match dir {
        Direction::Outgoing => id_and_kind("source_id", "source_kind"),
        Direction::Incoming => id_and_kind("target_id", "target_kind"),
        Direction::Both => {
            id_and_kind("source_id", "source_kind").or(id_and_kind("target_id", "target_kind"))
        }
    };
    if with_path {
        edge_path_starts_with(edge, node).and(id_cond)
    } else {
        id_cond
    }
}

fn target_join_cond(edge: &str, node: &str, dir: Direction, with_path: bool) -> Expr {
    let node_id = expr::col(node, DEFAULT_PRIMARY_KEY);
    let id_cond = match dir {
        Direction::Outgoing => expr::col(edge, "target_id").eq(node_id),
        Direction::Incoming => expr::col(edge, "source_id").eq(node_id),
        Direction::Both => expr::col(edge, "target_id")
            .eq(node_id.clone())
            .or(expr::col(edge, "source_id").eq(node_id)),
    };
    if with_path {
        edge_path_starts_with(edge, node).and(id_cond)
    } else {
        id_cond
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-hop Union Building
// ─────────────────────────────────────────────────────────────────────────────

fn build_hop_union_all(rel: &InputRelationship, alias: &str) -> Rel {
    let rel_type_filter = type_filter(&rel.types);
    let arms: Vec<Rel> = (1..=rel.max_hops)
        .map(|depth| build_hop_arm(depth, &rel_type_filter, rel.direction))
        .collect();
    Rel::union_all(arms, alias)
}

fn build_hop_arm(depth: u32, type_filter: &Option<Vec<String>>, direction: Direction) -> Rel {
    let (start_col, end_col) = direction.edge_columns();

    let mut rel = read_edge("e1");
    let first_type_cond = edge_type_filter_expr("e1", type_filter);

    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let next_edge = read_edge(&curr);
        let mut join_cond = expr::col(&prev, end_col).eq(expr::col(&curr, start_col));
        if let Some(tc) = edge_type_filter_expr(&curr, type_filter) {
            join_cond = join_cond.and(tc);
        }
        rel = rel.join(JoinType::Inner, next_edge, join_cond);
    }

    if let Some(cond) = first_type_cond {
        rel = rel.filter(cond);
    }

    rel.project(&[
        (expr::col("e1", start_col), "start_id"),
        (expr::col(&format!("e{depth}"), end_col), "end_id"),
        (expr::int(depth as i64), "depth"),
        (
            expr::col("e1", TRAVERSAL_PATH_COLUMN),
            TRAVERSAL_PATH_COLUMN,
        ),
    ])
}

// ─────────────────────────────────────────────────────────────────────────────
// Join Building
// ─────────────────────────────────────────────────────────────────────────────

fn build_joins(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    extra_columns: &HashMap<String, Vec<String>>,
) -> Result<(Rel, HashMap<usize, String>), String> {
    let start = match rels.first() {
        Some(r) => find_node(nodes, &r.from)?,
        None => nodes.first().ok_or("no nodes in input")?,
    };

    let start_extra = extra_columns
        .get(&start.id)
        .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        .unwrap_or_default();
    let mut result = read_node(start, &start_extra)?;
    let mut edge_aliases = HashMap::new();
    let mut joined_nodes: HashSet<String> = HashSet::new();
    joined_nodes.insert(start.id.clone());

    for (i, rel) in rels.iter().enumerate() {
        let target = find_node(nodes, &rel.to)?;

        if !joined_nodes.contains(&rel.from) {
            let from_node = find_node(nodes, &rel.from)?;
            let from_extra = extra_columns
                .get(&from_node.id)
                .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .unwrap_or_default();
            let from_rel = read_node(from_node, &from_extra)?;
            result = result.join(JoinType::Cross, from_rel, expr::boolean(true));
            joined_nodes.insert(rel.from.clone());
        }

        if rel.max_hops > 1 {
            let alias = format!("hop_e{i}");
            edge_aliases.insert(i, alias.clone());

            let from_node = find_node(nodes, &rel.from)?;
            let union = build_hop_union_all(rel, &alias);
            let (from_col, to_col) = rel.direction.union_columns();

            let mut source_cond =
                expr::col(&rel.from, DEFAULT_PRIMARY_KEY).eq(expr::col(&alias, from_col));
            if from_node.has_traversal_path {
                source_cond = edge_path_starts_with(&alias, &rel.from).and(source_cond);
            }
            result = result.join(JoinType::Inner, union, source_cond);

            let target_extra = extra_columns
                .get(&target.id)
                .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .unwrap_or_default();
            let target_rel = read_node(target, &target_extra)?;
            let mut target_cond =
                expr::col(&alias, to_col).eq(expr::col(&rel.to, DEFAULT_PRIMARY_KEY));
            if target.has_traversal_path {
                target_cond = edge_path_starts_with(&alias, &rel.to).and(target_cond);
            }
            result = result.join(JoinType::Inner, target_rel, target_cond);
            joined_nodes.insert(rel.to.clone());
        } else {
            let alias = format!("e{i}");
            edge_aliases.insert(i, alias.clone());

            let from_node = find_node(nodes, &rel.from)?;
            let edge = read_edge(&alias);
            let tf = type_filter(&rel.types);
            let mut join_cond = source_join_cond(
                &rel.from,
                &alias,
                rel.direction,
                from_node.has_traversal_path,
            );
            if let Some(tc) = edge_type_filter_expr(&alias, &tf) {
                join_cond = join_cond.and(tc);
            }
            result = result.join(JoinType::Inner, edge, join_cond);

            let target_extra = extra_columns
                .get(&target.id)
                .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .unwrap_or_default();
            let target_rel = read_node(target, &target_extra)?;
            let target_cond =
                target_join_cond(&alias, &rel.to, rel.direction, target.has_traversal_path);
            result = result.join(JoinType::Inner, target_rel, target_cond);
            joined_nodes.insert(rel.to.clone());
        }
    }

    Ok((result, edge_aliases))
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE Clause
// ─────────────────────────────────────────────────────────────────────────────

fn build_full_where(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
) -> Option<Expr> {
    let mut conds: Vec<Expr> = Vec::new();

    for node in nodes {
        if let Some(f) = id_filter(&node.id, DEFAULT_PRIMARY_KEY, &node.node_ids) {
            conds.push(f);
        }
        for (prop, filter) in &node.filters {
            conds.push(filter_expr(&node.id, prop, filter));
        }
    }

    for (i, rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            for (prop, filter) in &rel.filters {
                conds.push(filter_expr(alias, prop, filter));
            }
            if rel.max_hops > 1 && rel.min_hops > 1 {
                conds.push(expr::col(alias, "depth").ge(expr::int(rel.min_hops as i64)));
            }
        }
    }

    expr::and_opt(conds.into_iter().map(Some))
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal & Search
// ─────────────────────────────────────────────────────────────────────────────

fn lower_traversal(input: &Input) -> Result<Plan, String> {
    let extra = extra_columns_for_order_by(input);
    let (base_rel, edge_aliases) = build_joins(&input.nodes, &input.relationships, &extra)?;

    let where_expr = build_full_where(&input.nodes, &input.relationships, &edge_aliases);
    let mut rel = match where_expr {
        Some(cond) => base_rel.filter(cond),
        None => base_rel,
    };

    // Sort before project (sort columns may not be in the projection)
    if let Some(ob) = &input.order_by {
        rel = rel.sort(&[(expr::col(&ob.node, &ob.property), ob.direction.into())]);
    }

    // Build projections
    let mut proj_owned: Vec<(Expr, String)> = Vec::new();
    for node in &input.nodes {
        if let Some(cols) = &node.columns {
            for col in cols {
                proj_owned.push((expr::col(&node.id, col), format!("{}_{col}", node.id)));
            }
        }
    }
    for (i, input_rel) in input.relationships.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            if input_rel.max_hops > 1 {
                proj_owned.push((
                    expr::col(alias, TRAVERSAL_PATH_COLUMN),
                    format!("{alias}_path"),
                ));
            } else {
                proj_owned.extend(edge_select_items(alias));
            }
        }
    }

    if !proj_owned.is_empty() {
        let proj_refs: Vec<(Expr, &str)> = proj_owned
            .iter()
            .map(|(e, a)| (e.clone(), a.as_str()))
            .collect();
        rel = rel.project(&proj_refs);
    }

    rel = rel.fetch(input.limit as u64, None);

    Ok(rel.into_plan())
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation
// ─────────────────────────────────────────────────────────────────────────────

fn lower_aggregation(input: &Input) -> Result<Plan, String> {
    let extra = extra_columns_for_order_by(input);
    let (base_rel, edge_aliases) = build_joins(&input.nodes, &input.relationships, &extra)?;

    let where_expr = build_full_where(&input.nodes, &input.relationships, &edge_aliases);
    let rel = match where_expr {
        Some(cond) => base_rel.filter(cond),
        None => base_rel,
    };

    let group_by_node_ids: HashSet<_> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.group_by.clone())
        .collect();

    let mut group_exprs: Vec<Expr> = Vec::new();
    for node in &input.nodes {
        if !group_by_node_ids.contains(&node.id) {
            continue;
        }
        if let Some(cols) = &node.columns {
            for col in cols {
                group_exprs.push(expr::col(&node.id, col));
            }
        }
    }

    let measures: Vec<Measure> = input
        .aggregations
        .iter()
        .map(|agg| {
            let alias = agg
                .alias
                .clone()
                .unwrap_or_else(|| agg.function.to_lowercase());
            Measure::new(&agg.function, &[agg_arg_expr(agg)], &alias)
        })
        .collect();

    let mut rel = rel.aggregate(&group_exprs, &measures);

    // Sort on aggregate result
    if let Some(s) = &input.aggregation_sort
        && s.agg_index < input.aggregations.len()
    {
        let agg = &input.aggregations[s.agg_index];
        let agg_expr = expr::func(&agg.function, vec![agg_arg_expr(agg)]);
        rel = rel.sort(&[(agg_expr, s.direction.into())]);
    }

    rel = rel.fetch(input.limit as u64, None);

    Ok(rel.into_plan())
}

fn agg_arg_expr(agg: &InputAggregation) -> Expr {
    match (&agg.property, &agg.target) {
        (Some(prop), Some(target)) => expr::col(target, prop),
        (None, Some(target)) => expr::col(target, DEFAULT_PRIMARY_KEY),
        _ => expr::int(1),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Path Finding (recursive CTE)
// ─────────────────────────────────────────────────────────────────────────────

fn lower_path_finding(input: &Input) -> Result<Plan, String> {
    let path = input.path.as_ref().ok_or("path config missing")?;

    let start = find_node(&input.nodes, &path.from)?;
    let end = find_node(&input.nodes, &path.to)?;
    let start_table = start
        .table
        .as_deref()
        .ok_or_else(|| format!("node '{}' has no table", start.id))?;
    let end_table = end
        .table
        .as_deref()
        .ok_or_else(|| format!("node '{}' has no table", end.id))?;
    let start_entity = start.entity.as_deref().ok_or("start node has no entity")?;

    // CTE base: start node scan with optional id filter
    let cte_base = Rel::read(
        start_table,
        &start.id,
        &[
            (DEFAULT_PRIMARY_KEY, DataType::Int64),
            (TRAVERSAL_PATH_COLUMN, DataType::String),
        ],
    );
    let cte_base = match id_filter(&start.id, DEFAULT_PRIMARY_KEY, &start.node_ids) {
        Some(cond) => cte_base.filter(cond),
        None => cte_base,
    };

    let start_id = expr::col(&start.id, DEFAULT_PRIMARY_KEY);
    let start_tuple = expr::func("tuple", vec![start_id.clone(), expr::string(start_entity)]);
    let empty_string_array = expr::func(
        "arrayResize",
        vec![expr::func("array", vec![expr::string("")]), expr::int(0)],
    );

    let cte_rel = cte_base
        .project(&[
            (start_id.clone(), "node_id"),
            (expr::func("array", vec![start_id]), "path_ids"),
            (expr::func("array", vec![start_tuple]), "path"),
            (empty_string_array, "edge_kinds"),
            (expr::int(0), "depth"),
        ])
        .fetch(1000, None);

    let cte_plan = cte_rel.into_plan();

    // Forward and reverse recursive branches
    let _forward = path_recursive_branch(path.max_depth, true, &end.node_ids, &path.rel_types);
    let _reverse = path_recursive_branch(path.max_depth, false, &end.node_ids, &path.rel_types);

    // Main query: read from CTE, join end node
    let paths = Rel::read(
        "paths",
        "paths",
        &[
            ("node_id", DataType::Int64),
            ("path_ids", DataType::String),
            ("path", DataType::String),
            ("edge_kinds", DataType::String),
            ("depth", DataType::Int64),
        ],
    );
    let end_rel = Rel::read(
        end_table,
        &end.id,
        &[
            (DEFAULT_PRIMARY_KEY, DataType::Int64),
            (TRAVERSAL_PATH_COLUMN, DataType::String),
        ],
    );

    let join_cond = expr::col("paths", "node_id").eq(expr::col(&end.id, DEFAULT_PRIMARY_KEY));
    let joined = paths.join(JoinType::Inner, end_rel, join_cond);

    let rel = match id_filter(&end.id, DEFAULT_PRIMARY_KEY, &end.node_ids) {
        Some(cond) => joined.filter(cond),
        None => joined,
    };

    let plan = rel
        .sort(&[(expr::col("paths", "depth"), SortDir::Asc)])
        .project(&[
            (expr::col("paths", "path"), "_gkg_path"),
            (expr::col("paths", "edge_kinds"), "_gkg_edge_kinds"),
            (expr::col("paths", "depth"), "depth"),
        ])
        .fetch(input.limit as u64, None)
        .into_plan_with_ctes(vec![CteDef {
            name: "paths".into(),
            plan: cte_plan,
            recursive: true,
        }]);

    Ok(plan)
}

fn path_recursive_branch(
    max_depth: u32,
    join_on_source: bool,
    target_ids: &[i64],
    rel_types: &[String],
) -> Rel {
    let paths = Rel::read(
        "paths",
        "p",
        &[
            ("node_id", DataType::Int64),
            ("path_ids", DataType::String),
            ("path", DataType::String),
            ("edge_kinds", DataType::String),
            ("depth", DataType::Int64),
        ],
    );
    let edge = read_edge("e");

    let (next_id_col, next_type_col) = if join_on_source {
        ("target_id", "target_kind")
    } else {
        ("source_id", "source_kind")
    };
    let join_col = if join_on_source {
        "source_id"
    } else {
        "target_id"
    };

    let join_cond = expr::col("p", "node_id").eq(expr::col("e", join_col));
    let joined = paths.join(JoinType::Inner, edge, join_cond);

    let mut conds = vec![
        expr::col("p", "depth").lt(expr::int(max_depth as i64)),
        expr::func(
            "has",
            vec![expr::col("p", "path_ids"), expr::col("e", next_id_col)],
        )
        .not(),
    ];

    if !target_ids.is_empty() {
        let target_array = expr::func(
            "array",
            target_ids.iter().map(|id| expr::int(*id)).collect(),
        );
        conds.push(expr::func("has", vec![target_array, expr::col("p", "node_id")]).not());
    }

    if let Some(filter) = edge_type_filter_expr("e", &type_filter(rel_types)) {
        conds.push(filter);
    }

    let next_node_id = expr::col("e", next_id_col);
    let next_tuple = expr::func(
        "tuple",
        vec![next_node_id.clone(), expr::col("e", next_type_col)],
    );

    joined.filter(expr::and(conds)).project(&[
        (next_node_id, "node_id"),
        (
            expr::func(
                "arrayConcat",
                vec![
                    expr::col("p", "path_ids"),
                    expr::func("array", vec![expr::col("e", next_id_col)]),
                ],
            ),
            "path_ids",
        ),
        (
            expr::func(
                "arrayConcat",
                vec![
                    expr::col("p", "path"),
                    expr::func("array", vec![next_tuple]),
                ],
            ),
            "path",
        ),
        (
            expr::func(
                "arrayConcat",
                vec![
                    expr::col("p", "edge_kinds"),
                    expr::func("array", vec![expr::col("e", "relationship_kind")]),
                ],
            ),
            "edge_kinds",
        ),
        (expr::col("p", "depth").add(expr::int(1)), "depth"),
    ])
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors
// ─────────────────────────────────────────────────────────────────────────────

fn lower_neighbors(input: &Input) -> Result<Plan, String> {
    let neighbors_config = input.neighbors.as_ref().ok_or("neighbors config missing")?;

    let center_node = find_node(&input.nodes, &neighbors_config.node)?;
    let center_table = center_node
        .table
        .as_deref()
        .ok_or_else(|| format!("node '{}' has no table", center_node.id))?;
    let center_entity = center_node
        .entity
        .as_deref()
        .ok_or("center node entity missing")?;

    let tf = type_filter(&neighbors_config.rel_types);
    let edge_alias = "e";

    let center = Rel::read(
        center_table,
        &center_node.id,
        &[
            (DEFAULT_PRIMARY_KEY, DataType::Int64),
            (TRAVERSAL_PATH_COLUMN, DataType::String),
        ],
    );
    let edge = read_edge(edge_alias);

    let mut join_cond = source_join_cond_with_kind(
        &center_node.id,
        edge_alias,
        center_entity,
        neighbors_config.direction,
        center_node.has_traversal_path,
    );
    if let Some(tc) = edge_type_filter_expr(edge_alias, &tf) {
        join_cond = join_cond.and(tc);
    }

    let mut rel = center.join(JoinType::Inner, edge, join_cond);

    if let Some(cond) = id_filter(&center_node.id, DEFAULT_PRIMARY_KEY, &center_node.node_ids) {
        rel = rel.filter(cond);
    }

    let neighbor_id_expr = match neighbors_config.direction {
        Direction::Outgoing => expr::col(edge_alias, "target_id"),
        Direction::Incoming => expr::col(edge_alias, "source_id"),
        Direction::Both => expr::func(
            "if",
            vec![
                expr::col(&center_node.id, DEFAULT_PRIMARY_KEY)
                    .eq(expr::col(edge_alias, "source_id")),
                expr::col(edge_alias, "target_id"),
                expr::col(edge_alias, "source_id"),
            ],
        ),
    };

    let neighbor_type_expr = match neighbors_config.direction {
        Direction::Outgoing => expr::col(edge_alias, "target_kind"),
        Direction::Incoming => expr::col(edge_alias, "source_kind"),
        Direction::Both => expr::func(
            "if",
            vec![
                expr::col(&center_node.id, DEFAULT_PRIMARY_KEY)
                    .eq(expr::col(edge_alias, "source_id")),
                expr::col(edge_alias, "target_kind"),
                expr::col(edge_alias, "source_kind"),
            ],
        ),
    };

    if let Some(ob) = &input.order_by {
        rel = rel.sort(&[(expr::col(&ob.node, &ob.property), ob.direction.into())]);
    }

    rel = rel.project(&[
        (neighbor_id_expr, NEIGHBOR_ID_COLUMN),
        (neighbor_type_expr, NEIGHBOR_TYPE_COLUMN),
        (
            expr::col(edge_alias, "relationship_kind"),
            RELATIONSHIP_TYPE_COLUMN,
        ),
    ]);

    rel = rel.fetch(input.limit as u64, None);

    Ok(rel.into_plan())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn extra_columns_for_order_by(input: &Input) -> HashMap<String, Vec<String>> {
    let mut extra: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(ob) = &input.order_by {
        extra
            .entry(ob.node.clone())
            .or_default()
            .push(ob.property.clone());
    }
    extra
}

fn find_node<'a>(nodes: &'a [InputNode], id: &str) -> Result<&'a InputNode, String> {
    nodes
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| format!("node '{id}' not found"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use llqm::backend::clickhouse::emit_clickhouse_sql;

    fn emit(input: &Input) -> String {
        let plan = lower(input).unwrap();
        let pq = emit_clickhouse_sql(&plan).unwrap();
        pq.sql
    }

    #[test]
    fn simple_search() {
        let input = Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                columns: Some(vec!["username".into()]),
                ..Default::default()
            }],
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("gl_user AS u"), "sql: {sql}");
        assert!(sql.contains("u.username AS u_username"), "sql: {sql}");
        assert!(sql.contains("LIMIT 10"), "sql: {sql}");
    }

    #[test]
    fn search_with_filter() {
        let input = Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                columns: Some(vec!["username".into()]),
                filters: HashMap::from([(
                    "username".into(),
                    InputFilter {
                        op: Some(FilterOp::Eq),
                        value: Some(LiteralVal::Str("admin".into())),
                    },
                )]),
                ..Default::default()
            }],
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("WHERE"), "sql: {sql}");
        assert!(sql.contains("u.username"), "sql: {sql}");
    }

    #[test]
    fn simple_traversal() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    table: Some("gl_user".into()),
                    columns: Some(vec!["username".into()]),
                    has_traversal_path: true,
                    ..Default::default()
                },
                InputNode {
                    id: "n".into(),
                    entity: Some("Note".into()),
                    table: Some("gl_note".into()),
                    columns: Some(vec!["confidential".into()]),
                    has_traversal_path: true,
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "u".into(),
                to: "n".into(),
                ..Default::default()
            }],
            limit: 25,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("gl_user AS u"), "sql: {sql}");
        assert!(sql.contains("INNER JOIN gl_edge AS e0"), "sql: {sql}");
        assert!(sql.contains("INNER JOIN gl_note AS n"), "sql: {sql}");
        assert!(sql.contains("LIMIT 25"), "sql: {sql}");
    }

    #[test]
    fn aggregation_query() {
        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    table: Some("gl_user".into()),
                    columns: Some(vec!["username".into()]),
                    has_traversal_path: true,
                    ..Default::default()
                },
                InputNode {
                    id: "n".into(),
                    entity: Some("Note".into()),
                    table: Some("gl_note".into()),
                    has_traversal_path: true,
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "u".into(),
                to: "n".into(),
                ..Default::default()
            }],
            aggregations: vec![InputAggregation {
                function: "COUNT".into(),
                target: Some("n".into()),
                group_by: Some("u".into()),
                property: None,
                alias: Some("note_count".into()),
            }],
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("COUNT"), "sql: {sql}");
        assert!(sql.contains("GROUP BY"), "sql: {sql}");
    }

    #[test]
    fn neighbors_outgoing() {
        let input = Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                node_ids: vec![123],
                ..Default::default()
            }],
            neighbors: Some(InputNeighbors {
                node: "u".into(),
                direction: Direction::Outgoing,
                rel_types: vec![],
            }),
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains(NEIGHBOR_ID_COLUMN), "sql: {sql}");
        assert!(sql.contains(NEIGHBOR_TYPE_COLUMN), "sql: {sql}");
        assert!(sql.contains(RELATIONSHIP_TYPE_COLUMN), "sql: {sql}");
    }

    #[test]
    fn neighbors_both_uses_if() {
        let input = Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                node_ids: vec![1],
                ..Default::default()
            }],
            neighbors: Some(InputNeighbors {
                node: "u".into(),
                direction: Direction::Both,
                rel_types: vec![],
            }),
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("if("), "both direction should use if(): {sql}");
    }

    #[test]
    fn path_finding_cte() {
        let input = Input {
            query_type: QueryType::PathFinding,
            nodes: vec![
                InputNode {
                    id: "start".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    node_ids: vec![100],
                    has_traversal_path: true,
                    ..Default::default()
                },
                InputNode {
                    id: "end".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    node_ids: vec![200],
                    has_traversal_path: true,
                    ..Default::default()
                },
            ],
            path: Some(InputPath {
                from: "start".into(),
                to: "end".into(),
                max_depth: 3,
                rel_types: vec![],
            }),
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("WITH RECURSIVE"), "sql: {sql}");
        assert!(sql.contains("paths AS ("), "sql: {sql}");
        assert!(sql.contains("_gkg_path"), "sql: {sql}");
    }

    #[test]
    fn multi_hop_traversal() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    table: Some("gl_user".into()),
                    columns: Some(vec!["username".into()]),
                    has_traversal_path: true,
                    ..Default::default()
                },
                InputNode {
                    id: "p".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    columns: Some(vec!["name".into()]),
                    has_traversal_path: true,
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["MEMBER_OF".into()],
                from: "u".into(),
                to: "p".into(),
                min_hops: 1,
                max_hops: 3,
                ..Default::default()
            }],
            limit: 25,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(
            sql.contains("UNION ALL"),
            "multi-hop should use UNION ALL: {sql}"
        );
        assert!(sql.contains("hop_e0"), "should have hop alias: {sql}");
    }

    #[test]
    fn traversal_with_order_by() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                columns: Some(vec!["username".into()]),
                ..Default::default()
            }],
            order_by: Some(InputOrderBy {
                node: "u".into(),
                property: "username".into(),
                direction: OrderDirection::Desc,
            }),
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        assert!(sql.contains("DESC"), "sql: {sql}");
    }

    #[test]
    fn traversal_with_node_ids() {
        let input = Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                columns: Some(vec!["username".into()]),
                node_ids: vec![1, 2, 3],
                ..Default::default()
            }],
            limit: 10,
            ..Default::default()
        };

        let sql = emit(&input);
        assert!(sql.contains("IN ("), "multiple ids should use IN: {sql}");
    }

    #[test]
    fn path_finding_plan_structure() {
        let input = Input {
            query_type: QueryType::PathFinding,
            nodes: vec![
                InputNode {
                    id: "s".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    node_ids: vec![1],
                    ..Default::default()
                },
                InputNode {
                    id: "e".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    node_ids: vec![2],
                    ..Default::default()
                },
            ],
            path: Some(InputPath {
                from: "s".into(),
                to: "e".into(),
                max_depth: 5,
                rel_types: vec!["DEPENDS_ON".into()],
            }),
            limit: 10,
            ..Default::default()
        };

        let plan = lower(&input).unwrap();
        assert_eq!(plan.ctes.len(), 1);
        assert_eq!(plan.ctes[0].name, "paths");
        assert!(plan.ctes[0].recursive);
    }

    #[test]
    fn enforce_can_extend_project() {
        let input = Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                columns: Some(vec!["username".into()]),
                ..Default::default()
            }],
            limit: 10,
            ..Default::default()
        };

        let mut plan = lower(&input).unwrap();
        assert_eq!(plan.output_names, vec!["u_username"]);

        // Simulate what enforce does: add redaction columns
        plan.extend_project(vec![(expr::col("u", "id"), "_gkg_u_id".into())]);
        plan.insert_project_after("_gkg_u_id", (expr::string("User"), "_gkg_u_type".into()));

        assert_eq!(
            plan.output_names,
            vec!["u_username", "_gkg_u_id", "_gkg_u_type"]
        );

        // Verify it still emits valid SQL
        let pq = emit_clickhouse_sql(&plan).unwrap();
        assert!(pq.sql.contains("_gkg_u_id"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("_gkg_u_type"), "sql: {}", pq.sql);
    }

    #[test]
    fn enforce_can_extend_aggregate() {
        let input = Input {
            query_type: QueryType::Aggregation,
            nodes: vec![InputNode {
                id: "u".into(),
                entity: Some("User".into()),
                table: Some("gl_user".into()),
                columns: Some(vec!["username".into()]),
                ..Default::default()
            }],
            aggregations: vec![InputAggregation {
                function: "COUNT".into(),
                target: Some("u".into()),
                group_by: Some("u".into()),
                property: None,
                alias: Some("cnt".into()),
            }],
            limit: 10,
            ..Default::default()
        };

        let mut plan = lower(&input).unwrap();

        // Simulate enforce adding u.id to group_by
        plan.extend_aggregate_groups(vec![(expr::col("u", "id"), "_gkg_u_id".into())]);

        // Verify valid SQL with the extra group-by
        let pq = emit_clickhouse_sql(&plan).unwrap();
        assert!(pq.sql.contains("GROUP BY"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("u.id"), "sql: {}", pq.sql);
    }

    #[test]
    fn security_can_inject_filter_on_cte() {
        let input = Input {
            query_type: QueryType::PathFinding,
            nodes: vec![
                InputNode {
                    id: "s".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    node_ids: vec![1],
                    has_traversal_path: true,
                    ..Default::default()
                },
                InputNode {
                    id: "e".into(),
                    entity: Some("Project".into()),
                    table: Some("gl_project".into()),
                    node_ids: vec![2],
                    has_traversal_path: true,
                    ..Default::default()
                },
            ],
            path: Some(InputPath {
                from: "s".into(),
                to: "e".into(),
                max_depth: 3,
                rel_types: vec![],
            }),
            limit: 10,
            ..Default::default()
        };

        let mut plan = lower(&input).unwrap();

        // Simulate security injecting a traversal_path filter on the CTE
        plan.ctes[0].plan.inject_filter(
            expr::col("s", "traversal_path").starts_with(expr::string("42/")),
        );

        // Also inject on the main query
        plan.inject_filter(
            expr::col("e", "traversal_path").starts_with(expr::string("42/")),
        );

        let pq = emit_clickhouse_sql(&plan).unwrap();
        assert!(pq.sql.contains("startsWith"), "sql: {}", pq.sql);
    }
}
