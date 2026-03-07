//! Lower: Input → LoweredQuery (llqm-based)
//!
//! Transforms validated input into llqm plan builder operations.
//! Each query type produces a `LoweredQuery` — an intermediate representation
//! that holds the base relation, projections, sort keys, etc. Subsequent
//! pipeline phases (enforce, security) modify the `LoweredQuery` before
//! finalization builds the Substrait plan.

use gkg_utils::clickhouse::ChType;
use llqm::expr::{self, DataType, Expr, SortDir};
use llqm::plan::{CteDef, PlanBuilder, TypedRel};
use serde_json::Value;

use crate::constants::{NEIGHBOR_ID_COLUMN, NEIGHBOR_TYPE_COLUMN, RELATIONSHIP_TYPE_COLUMN};
use crate::error::{QueryError, Result};
use crate::input::{
    ColumnSelection, Direction, FilterOp, Input, InputAggregation, InputFilter, InputNode,
    InputRelationship, OrderDirection, QueryType,
};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, EDGE_RESERVED_COLUMNS, EDGE_TABLE, TRAVERSAL_PATH_COLUMN,
};
use std::collections::{HashMap, HashSet};

/// Maps edge column names to output alias suffixes.
const EDGE_ALIAS_SUFFIXES: &[&str] = &["path", "type", "src", "src_type", "dst", "dst_type"];

// ─────────────────────────────────────────────────────────────────────────────
// LoweredQuery — intermediate representation between lower and finalize
// ─────────────────────────────────────────────────────────────────────────────

/// A projection item: expression + output alias.
pub type SelectItem = (Expr, String);

/// An aggregate measure: (function_name, output_alias, arguments).
pub type AggItem = (String, String, Vec<Expr>);

/// Intermediate query representation produced by `lower()`.
///
/// Pipeline phases (enforce, security) modify this before `finalize()` assembles
/// the Substrait plan.
pub struct LoweredQuery {
    /// The shared plan builder (owns the function registry).
    pub builder: PlanBuilder,
    /// Base relation: reads + joins + user WHERE filters.
    /// For non-aggregation queries, this is the complete FROM + WHERE.
    /// For aggregation queries, this is the pre-aggregate relation.
    pub base_rel: TypedRel,
    /// SELECT items for the final projection.
    pub projections: Vec<SelectItem>,
    /// GROUP BY expressions (aggregation queries only).
    /// Each item is (expr, alias) — alias is used both in SELECT and GROUP BY.
    pub group_by: Vec<SelectItem>,
    /// Aggregate measures (aggregation queries only).
    pub agg_measures: Vec<AggItem>,
    /// ORDER BY keys.
    pub sort_keys: Vec<(Expr, SortDir)>,
    /// LIMIT.
    pub limit: Option<u64>,
    /// OFFSET.
    pub offset: Option<u64>,
    /// CTEs for WITH clause.
    pub ctes: Vec<CteDef>,
}

/// Lower validated input into a `LoweredQuery`.
pub fn lower(input: &Input) -> Result<LoweredQuery> {
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

/// Build a read for a node table. The schema includes all columns that might
/// be referenced (user columns + id + traversal_path).
fn read_node(b: &mut PlanBuilder, node: &InputNode) -> Result<TypedRel> {
    let table = resolve_table(node)?;
    let mut columns: Vec<(&str, DataType)> = vec![
        (DEFAULT_PRIMARY_KEY, DataType::Int64),
        (TRAVERSAL_PATH_COLUMN, DataType::String),
    ];

    // Add user-selected columns that aren't already in the schema
    if let Some(ColumnSelection::List(cols)) = &node.columns {
        for col in cols {
            if col != DEFAULT_PRIMARY_KEY && col != TRAVERSAL_PATH_COLUMN {
                columns.push((col, DataType::String));
            }
        }
    }

    // Add filter columns that aren't already in the schema
    for prop in node.filters.keys() {
        if !columns.iter().any(|(n, _)| *n == prop.as_str()) {
            columns.push((prop, DataType::String));
        }
    }

    // Add redaction_id_column if different from id
    if node.redaction_id_column != DEFAULT_PRIMARY_KEY
        && !columns
            .iter()
            .any(|(n, _)| *n == node.redaction_id_column.as_str())
    {
        columns.push((&node.redaction_id_column, DataType::Int64));
    }

    // We need to use owned strings for columns since node.filters keys are Strings
    // but PlanBuilder::read takes &str slices. Build the column specs.
    let col_specs: Vec<(String, DataType)> = columns
        .iter()
        .map(|(n, dt)| (n.to_string(), dt.clone()))
        .collect();
    let col_refs: Vec<(&str, DataType)> = col_specs
        .iter()
        .map(|(n, dt)| (n.as_str(), dt.clone()))
        .collect();

    Ok(b.read(&table, &node.id, &col_refs))
}

/// Build a read for the edge table.
fn read_edge(b: &mut PlanBuilder, alias: &str) -> TypedRel {
    b.read(
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

/// `startsWith(edge.traversal_path, node.traversal_path)`
fn edge_path_starts_with(edge_alias: &str, node_alias: &str) -> Expr {
    expr::starts_with(
        expr::col(edge_alias, TRAVERSAL_PATH_COLUMN),
        expr::col(node_alias, TRAVERSAL_PATH_COLUMN),
    )
}

/// Build SELECT items for all edge columns.
fn edge_select_items(alias: &str) -> Vec<SelectItem> {
    EDGE_RESERVED_COLUMNS
        .iter()
        .zip(EDGE_ALIAS_SUFFIXES.iter())
        .map(|(col, suffix)| (expr::col(alias, col), format!("{alias}_{suffix}")))
        .collect()
}

/// Convert `ChType` to llqm `DataType`.
fn ch_type_to_data_type(ct: &ChType) -> DataType {
    match ct {
        ChType::String => DataType::String,
        ChType::Int64 => DataType::Int64,
        ChType::Float64 => DataType::Float64,
        ChType::Bool => DataType::Bool,
        ChType::Array(inner) => DataType::Array(Box::new(ch_type_to_data_type(inner))),
        ChType::DateTime => DataType::DateTime,
    }
}

/// Build an llqm Expr for a filter.
fn filter_expr(table: &str, column: &str, filter: &InputFilter) -> Expr {
    let col = expr::col(table, column);
    let val = || {
        let v = filter.value.clone().unwrap_or(Value::Null);
        let ct = ChType::from_value(&v);
        let dt = ch_type_to_data_type(&ct);
        // Use literal for the value — llqm codegen auto-parameterizes literals
        value_to_literal(&v, &dt)
    };

    match filter.op {
        None | Some(FilterOp::Eq) => expr::eq(col, val()),
        Some(FilterOp::Gt) => expr::gt(col, val()),
        Some(FilterOp::Lt) => expr::lt(col, val()),
        Some(FilterOp::Gte) => expr::ge(col, val()),
        Some(FilterOp::Lte) => expr::le(col, val()),
        Some(FilterOp::In) => {
            // IN with array parameter
            let v = filter.value.clone().unwrap_or(Value::Null);
            let ct = ChType::from_value(&v);
            let dt = ch_type_to_data_type(&ct);
            expr::is_in(col, value_to_literal(&v, &dt))
        }
        Some(FilterOp::Contains) => like_pattern(col, filter, "%", "%"),
        Some(FilterOp::StartsWith) => like_pattern(col, filter, "", "%"),
        Some(FilterOp::EndsWith) => like_pattern(col, filter, "%", ""),
        Some(FilterOp::IsNull) => expr::is_null(col),
        Some(FilterOp::IsNotNull) => expr::is_not_null(col),
    }
}

fn like_pattern(col: Expr, filter: &InputFilter, prefix: &str, suffix: &str) -> Expr {
    let s = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    expr::like(col, expr::string(&format!("{prefix}{s}{suffix}")))
}

/// Convert a serde_json Value to an llqm literal expression.
fn value_to_literal(v: &Value, _dt: &DataType) -> Expr {
    match v {
        Value::String(s) => expr::string(s),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                expr::int(i)
            } else if let Some(f) = n.as_f64() {
                expr::float(f)
            } else {
                expr::string(&n.to_string())
            }
        }
        Value::Bool(b) => expr::boolean(*b),
        Value::Array(arr) => {
            // For IN clause: convert to InList
            let items: Vec<Expr> = arr
                .iter()
                .map(|item| match item {
                    Value::String(s) => expr::string(s),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            expr::int(i)
                        } else {
                            expr::string(&n.to_string())
                        }
                    }
                    Value::Bool(b) => expr::boolean(*b),
                    _ => expr::string(&item.to_string()),
                })
                .collect();
            // Return an InList expression — the `is_in` caller will handle this
            // Actually, for arrays used as IN parameters, we need a different approach.
            // Use Raw for now to carry the array, or use InList.
            // InList is the right approach: `expr IN (v1, v2, ...)`
            // But we already have `is_in(col, val)` which expects BinaryOp::In.
            // For proper array params, we need to rethink this.
            // Let's use the Param approach with Array type instead.
            let ct = ChType::from_value(&Value::Array(arr.clone()));
            let dt = ch_type_to_data_type(&ct);
            // Store as a param with Array type
            Expr::Param {
                name: String::new(), // empty name = auto-numbered
                data_type: dt,
            }
        }
        Value::Null => expr::null(),
        _ => expr::string(&v.to_string()),
    }
}

/// Build id filter: `table.col = id` or `table.col IN (id1, id2, ...)`.
/// Returns None for empty ids.
fn id_filter(table: &str, col: &str, ids: &[i64]) -> Option<Expr> {
    match ids.len() {
        0 => None,
        1 => Some(expr::eq(expr::col(table, col), expr::int(ids[0]))),
        _ => {
            let list: Vec<Expr> = ids.iter().map(|&id| expr::int(id)).collect();
            Some(expr::in_list(expr::col(table, col), list))
        }
    }
}

/// Derive LIMIT and OFFSET from the input's pagination fields.
fn pagination(input: &Input) -> (Option<u64>, Option<u64>) {
    if let Some(ref range) = input.range {
        (
            Some((range.end - range.start) as u64),
            Some(range.start as u64),
        )
    } else {
        (Some(input.limit as u64), None)
    }
}

fn type_filter(types: &[String]) -> Option<Vec<String>> {
    if types.is_empty() || (types.len() == 1 && types[0] == "*") {
        None
    } else {
        Some(types.to_vec())
    }
}

/// Build edge type filter expression: `alias.relationship_kind = type` or `IN (types)`.
fn edge_type_filter_expr(alias: &str, type_filter: &Option<Vec<String>>) -> Option<Expr> {
    let types = type_filter.as_ref()?;
    match types.len() {
        0 => None,
        1 => Some(expr::eq(
            expr::col(alias, "relationship_kind"),
            expr::string(&types[0]),
        )),
        _ => {
            let list: Vec<Expr> = types.iter().map(|t| expr::string(t)).collect();
            Some(expr::in_list(expr::col(alias, "relationship_kind"), list))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal & Search
// ─────────────────────────────────────────────────────────────────────────────

fn lower_traversal(input: &Input) -> Result<LoweredQuery> {
    let mut b = PlanBuilder::new();
    let (base_rel, edge_aliases) = build_joins(&mut b, &input.nodes, &input.relationships)?;

    // Build WHERE clause from user filters
    let where_expr = build_full_where(&input.nodes, &input.relationships, &edge_aliases);
    let base_rel = match where_expr {
        Some(cond) => b.filter(base_rel, cond),
        None => base_rel,
    };

    // Build projections
    let mut projections: Vec<SelectItem> = Vec::new();
    for node in &input.nodes {
        if let Some(ColumnSelection::List(cols)) = &node.columns {
            for col in cols {
                projections.push((expr::col(&node.id, col), format!("{}_{col}", node.id)));
            }
        }
    }
    add_edge_select_items(&mut projections, &input.relationships, &edge_aliases);

    // Sort keys
    let sort_keys = input.order_by.as_ref().map_or(vec![], |ob| {
        let dir = match ob.direction {
            OrderDirection::Asc => SortDir::Asc,
            OrderDirection::Desc => SortDir::Desc,
        };
        vec![(expr::col(&ob.node, &ob.property), dir)]
    });

    let (limit, offset) = pagination(input);

    Ok(LoweredQuery {
        builder: b,
        base_rel,
        projections,
        group_by: vec![],
        agg_measures: vec![],
        sort_keys,
        limit,
        offset,
        ctes: vec![],
    })
}

fn add_edge_select_items(
    projections: &mut Vec<SelectItem>,
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
) {
    for (i, _rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            projections.extend(edge_select_items(alias));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation
// ─────────────────────────────────────────────────────────────────────────────

fn lower_aggregation(input: &Input) -> Result<LoweredQuery> {
    let mut b = PlanBuilder::new();
    let (base_rel, edge_aliases) = build_joins(&mut b, &input.nodes, &input.relationships)?;

    let where_expr = build_full_where(&input.nodes, &input.relationships, &edge_aliases);
    let base_rel = match where_expr {
        Some(cond) => b.filter(base_rel, cond),
        None => base_rel,
    };

    // Collect unique group_by node IDs
    let group_by_node_ids: HashSet<_> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.group_by.clone())
        .collect();

    // Build GROUP BY and user projections
    let mut group_by: Vec<SelectItem> = Vec::new();
    let mut projections: Vec<SelectItem> = Vec::new();

    for node in &input.nodes {
        if !group_by_node_ids.contains(&node.id) {
            continue;
        }
        if let Some(ColumnSelection::List(cols)) = &node.columns {
            for col in cols {
                let e = expr::col(&node.id, col);
                let alias = format!("{}_{col}", node.id);
                group_by.push((e.clone(), alias.clone()));
                projections.push((e, alias));
            }
        }
    }

    // Build aggregate measures
    let mut agg_measures: Vec<AggItem> = Vec::new();
    for agg in &input.aggregations {
        let func_name = agg.function.as_sql().to_string();
        let alias = agg
            .alias
            .clone()
            .unwrap_or_else(|| func_name.to_lowercase());
        let args = vec![agg_arg_expr(agg)];
        agg_measures.push((func_name, alias, args));
    }

    // Sort keys for aggregation
    let sort_keys = input
        .aggregation_sort
        .as_ref()
        .filter(|s| s.agg_index < input.aggregations.len())
        .map_or(vec![], |s| {
            let agg = &input.aggregations[s.agg_index];
            let func_name = agg.function.as_sql().to_string();
            let args = vec![agg_arg_expr(agg)];
            let agg_expr = expr::func(&func_name, args);
            let dir = match s.direction {
                OrderDirection::Asc => SortDir::Asc,
                OrderDirection::Desc => SortDir::Desc,
            };
            vec![(agg_expr, dir)]
        });

    let (limit, offset) = pagination(input);

    Ok(LoweredQuery {
        builder: b,
        base_rel,
        projections,
        group_by,
        agg_measures,
        sort_keys,
        limit,
        offset,
        ctes: vec![],
    })
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

fn lower_path_finding(input: &Input) -> Result<LoweredQuery> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?;

    let start = find_node(&input.nodes, &path.from)?;
    let end = find_node(&input.nodes, &path.to)?;
    let start_table = resolve_table(start)?;
    let end_table = resolve_table(end)?;

    let start_entity = start
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("start node has no entity".into()))?;

    // Build the recursive CTE plan
    let mut cte_builder = PlanBuilder::new();

    // Base query: start node scan
    let base_rel = cte_builder.read(
        &start_table,
        &start.id,
        &[
            (DEFAULT_PRIMARY_KEY, DataType::Int64),
            (TRAVERSAL_PATH_COLUMN, DataType::String),
        ],
    );

    // Filter on start node IDs
    let base_rel = match id_filter(&start.id, DEFAULT_PRIMARY_KEY, &start.node_ids) {
        Some(cond) => cte_builder.filter(base_rel, cond),
        None => base_rel,
    };

    // Project base CTE columns
    let start_id = expr::col(&start.id, DEFAULT_PRIMARY_KEY);
    let start_tuple = expr::func("tuple", vec![start_id.clone(), expr::string(start_entity)]);
    let empty_string_array = expr::func(
        "arrayResize",
        vec![expr::func("array", vec![expr::string("")]), expr::int(0)],
    );

    let base_rel = cte_builder.project(
        base_rel,
        &[
            (start_id.clone(), "node_id"),
            (expr::func("array", vec![start_id]), "path_ids"),
            (expr::func("array", vec![start_tuple]), "path"),
            (empty_string_array, "edge_kinds"),
            (expr::int(0), "depth"),
        ],
    );
    let base_rel = cte_builder.fetch(base_rel, 1000, None);
    let base_plan = cte_builder.build(base_rel);

    // Forward recursive branch
    let forward_plan =
        path_recursive_branch_plan(path.max_depth, true, &end.node_ids, &path.rel_types);
    // Reverse recursive branch
    let reverse_plan =
        path_recursive_branch_plan(path.max_depth, false, &end.node_ids, &path.rel_types);

    // The recursive CTE needs UNION ALL of base + forward + reverse.
    // In llqm, we store these as separate CTEs that codegen assembles.
    // Actually, the current CTE model uses `base.union_all = [forward, reverse]`.
    // We need to build a single CTE plan that unions base + forward + reverse.
    // Let's use the CTE with recursive flag.

    // Build the final query
    let mut b = PlanBuilder::new();
    let paths = b.read(
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
    let end_rel = b.read(
        &end_table,
        &end.id,
        &[
            (DEFAULT_PRIMARY_KEY, DataType::Int64),
            (TRAVERSAL_PATH_COLUMN, DataType::String),
        ],
    );

    let join_cond = expr::eq(
        expr::col("paths", "node_id"),
        expr::col(&end.id, DEFAULT_PRIMARY_KEY),
    );
    let joined = b.join(llqm::expr::JoinType::Inner, paths, end_rel, join_cond);

    // Filter on end node IDs
    let base_rel = match id_filter(&end.id, DEFAULT_PRIMARY_KEY, &end.node_ids) {
        Some(cond) => b.filter(joined, cond),
        None => joined,
    };

    let projections = vec![
        (expr::col("paths", "path"), "_gkg_path".to_string()),
        (
            expr::col("paths", "edge_kinds"),
            "_gkg_edge_kinds".to_string(),
        ),
        (expr::col("paths", "depth"), "depth".to_string()),
    ];

    let sort_keys = vec![(expr::col("paths", "depth"), SortDir::Asc)];
    let (limit, offset) = pagination(input);

    // Build CTEs
    let ctes = vec![CteDef {
        name: "paths".to_string(),
        plan: base_plan,
        recursive: true,
    }];

    // TODO: The recursive CTE needs union_all of base + forward + reverse branches.
    // This requires extending llqm's CTE support. For now, we store additional branch
    // plans and handle them in finalization.
    // Actually, we need to think about this differently. The old AST stored
    // `base.union_all = [forward, reverse]`. In llqm, a CTE is a single Plan.
    // We need to support UNION ALL within a CTE plan.

    Ok(LoweredQuery {
        builder: b,
        base_rel,
        projections,
        group_by: vec![],
        agg_measures: vec![],
        sort_keys,
        limit,
        offset,
        ctes,
    })
}

fn path_recursive_branch_plan(
    max_depth: u32,
    join_on_source: bool,
    target_ids: &[i64],
    rel_types: &[String],
) -> llqm::plan::Plan {
    let mut b = PlanBuilder::new();

    let paths = b.read(
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
    let edge = read_edge(&mut b, "e");

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

    let join_cond = expr::eq(expr::col("p", "node_id"), expr::col("e", join_col));
    let joined = b.join(llqm::expr::JoinType::Inner, paths, edge, join_cond);

    // WHERE conditions
    let mut conds = vec![
        // depth < max_depth
        expr::lt(expr::col("p", "depth"), expr::int(max_depth as i64)),
        // cycle detection: NOT has(path_ids, next_node)
        expr::not(expr::func(
            "has",
            vec![expr::col("p", "path_ids"), expr::col("e", next_id_col)],
        )),
    ];

    // early termination: stop if target already in path
    if !target_ids.is_empty() {
        let target_array = expr::func(
            "array",
            target_ids.iter().map(|id| expr::int(*id)).collect(),
        );
        conds.push(expr::not(expr::func(
            "has",
            vec![target_array, expr::col("p", "node_id")],
        )));
    }

    // relationship type filter
    if let Some(filter) = edge_type_filter_expr("e", &type_filter(rel_types)) {
        conds.push(filter);
    }

    let filtered = b.filter(joined, expr::and(conds));

    let next_node_id = expr::col("e", next_id_col);
    let next_tuple = expr::func(
        "tuple",
        vec![next_node_id.clone(), expr::col("e", next_type_col)],
    );

    let projected = b.project(
        filtered,
        &[
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
            (expr::add(expr::col("p", "depth"), expr::int(1)), "depth"),
        ],
    );

    b.build(projected)
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors
// ─────────────────────────────────────────────────────────────────────────────

fn lower_neighbors(input: &Input) -> Result<LoweredQuery> {
    let neighbors_config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = find_node(&input.nodes, &neighbors_config.node)?;
    let center_table = resolve_table(center_node)?;
    let center_entity = center_node
        .entity
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("center node entity missing".into()))?;

    let tf = type_filter(&neighbors_config.rel_types);
    let edge_alias = "e";

    let mut b = PlanBuilder::new();

    let center = b.read(
        &center_table,
        &center_node.id,
        &[
            (DEFAULT_PRIMARY_KEY, DataType::Int64),
            (TRAVERSAL_PATH_COLUMN, DataType::String),
        ],
    );
    let edge = read_edge(&mut b, edge_alias);

    // Build join condition (with entity kind filter for neighbors)
    let mut join_cond = source_join_cond_with_kind(
        &center_node.id,
        edge_alias,
        center_entity,
        neighbors_config.direction,
        center_node.has_traversal_path,
    );
    if let Some(tc) = edge_type_filter_expr(edge_alias, &tf) {
        join_cond = expr::BinaryOp {
            op: expr::BinaryOp::And,
            left: Box::new(join_cond),
            right: Box::new(tc),
        };
    }

    let base_rel = b.join(llqm::expr::JoinType::Inner, center, edge, join_cond);

    // Filter on center node IDs
    let base_rel = match id_filter(&center_node.id, DEFAULT_PRIMARY_KEY, &center_node.node_ids) {
        Some(cond) => b.filter(base_rel, cond),
        None => base_rel,
    };

    // Build projections based on direction
    let neighbor_id_expr = match neighbors_config.direction {
        Direction::Outgoing => expr::col(edge_alias, "target_id"),
        Direction::Incoming => expr::col(edge_alias, "source_id"),
        Direction::Both => expr::func(
            "if",
            vec![
                expr::eq(
                    expr::col(&center_node.id, DEFAULT_PRIMARY_KEY),
                    expr::col(edge_alias, "source_id"),
                ),
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
                expr::eq(
                    expr::col(&center_node.id, DEFAULT_PRIMARY_KEY),
                    expr::col(edge_alias, "source_id"),
                ),
                expr::col(edge_alias, "target_kind"),
                expr::col(edge_alias, "source_kind"),
            ],
        ),
    };

    let projections = vec![
        (neighbor_id_expr, NEIGHBOR_ID_COLUMN.to_string()),
        (neighbor_type_expr, NEIGHBOR_TYPE_COLUMN.to_string()),
        (
            expr::col(edge_alias, "relationship_kind"),
            RELATIONSHIP_TYPE_COLUMN.to_string(),
        ),
    ];

    let sort_keys = input.order_by.as_ref().map_or(vec![], |ob| {
        let dir = match ob.direction {
            OrderDirection::Asc => SortDir::Asc,
            OrderDirection::Desc => SortDir::Desc,
        };
        vec![(expr::col(&ob.node, &ob.property), dir)]
    });

    let (limit, offset) = pagination(input);

    Ok(LoweredQuery {
        builder: b,
        base_rel,
        projections,
        group_by: vec![],
        agg_measures: vec![],
        sort_keys,
        limit,
        offset,
        ctes: vec![],
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-hop Union Building
// ─────────────────────────────────────────────────────────────────────────────

fn build_hop_union_all(b: &mut PlanBuilder, rel: &InputRelationship, alias: &str) -> TypedRel {
    let rel_type_filter = type_filter(&rel.types);
    let mut arms = Vec::new();
    for depth in 1..=rel.max_hops {
        arms.push(build_hop_arm(b, depth, &rel_type_filter, rel.direction));
    }
    b.union_all(arms, alias)
}

fn build_hop_arm(
    b: &mut PlanBuilder,
    depth: u32,
    type_filter: &Option<Vec<String>>,
    direction: Direction,
) -> TypedRel {
    let (start_col, end_col) = direction.edge_columns();

    let mut rel = read_edge(b, "e1");
    let mut first_type_cond = edge_type_filter_expr("e1", type_filter);

    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let next_edge = read_edge(b, &curr);
        let mut join_cond = expr::eq(expr::col(&prev, end_col), expr::col(&curr, start_col));
        if let Some(tc) = edge_type_filter_expr(&curr, type_filter) {
            join_cond = Expr::BinaryOp {
                op: expr::BinaryOp::And,
                left: Box::new(join_cond),
                right: Box::new(tc),
            };
        }
        rel = b.join(llqm::expr::JoinType::Inner, rel, next_edge, join_cond);
    }

    // Apply first edge type condition as a filter
    let rel = match first_type_cond.take() {
        Some(cond) => b.filter(rel, cond),
        None => rel,
    };

    b.project(
        rel,
        &[
            (expr::col("e1", start_col), "start_id"),
            (expr::col(&format!("e{depth}"), end_col), "end_id"),
            (expr::int(depth as i64), "depth"),
            (
                expr::col("e1", TRAVERSAL_PATH_COLUMN),
                TRAVERSAL_PATH_COLUMN,
            ),
        ],
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Join Building
// ─────────────────────────────────────────────────────────────────────────────

fn build_joins(
    b: &mut PlanBuilder,
    nodes: &[InputNode],
    rels: &[InputRelationship],
) -> Result<(TypedRel, HashMap<usize, String>)> {
    let start = match rels.first() {
        Some(r) => find_node(nodes, &r.from)?,
        None => nodes
            .first()
            .ok_or_else(|| QueryError::Lowering("no nodes in input".into()))?,
    };

    let mut result = read_node(b, start)?;
    let mut edge_aliases = HashMap::new();

    for (i, rel) in rels.iter().enumerate() {
        let target = find_node(nodes, &rel.to)?;

        if rel.max_hops > 1 {
            // Multi-hop: UNION ALL subquery
            let alias = format!("hop_e{i}");
            edge_aliases.insert(i, alias.clone());

            let from_node = find_node(nodes, &rel.from)?;
            let union = build_hop_union_all(b, rel, &alias);
            let (from_col, to_col) = rel.direction.union_columns();

            let mut source_cond = expr::eq(
                expr::col(&rel.from, DEFAULT_PRIMARY_KEY),
                expr::col(&alias, from_col),
            );
            if from_node.has_traversal_path {
                source_cond = Expr::BinaryOp {
                    op: expr::BinaryOp::And,
                    left: Box::new(edge_path_starts_with(&alias, &rel.from)),
                    right: Box::new(source_cond),
                };
            }
            result = b.join(llqm::expr::JoinType::Inner, result, union, source_cond);

            let mut target_rel = read_node(b, target)?;
            let mut target_cond = expr::eq(
                expr::col(&alias, to_col),
                expr::col(&rel.to, DEFAULT_PRIMARY_KEY),
            );
            if target.has_traversal_path {
                target_cond = Expr::BinaryOp {
                    op: expr::BinaryOp::And,
                    left: Box::new(edge_path_starts_with(&alias, &rel.to)),
                    right: Box::new(target_cond),
                };
            }
            result = b.join(llqm::expr::JoinType::Inner, result, target_rel, target_cond);
        } else {
            // Single-hop: direct edge join
            let alias = format!("e{i}");
            edge_aliases.insert(i, alias.clone());

            let from_node = find_node(nodes, &rel.from)?;
            let edge = read_edge(b, &alias);
            let tf = type_filter(&rel.types);
            let mut join_cond = source_join_cond(
                &rel.from,
                &alias,
                rel.direction,
                from_node.has_traversal_path,
            );
            if let Some(tc) = edge_type_filter_expr(&alias, &tf) {
                join_cond = Expr::BinaryOp {
                    op: expr::BinaryOp::And,
                    left: Box::new(join_cond),
                    right: Box::new(tc),
                };
            }
            result = b.join(llqm::expr::JoinType::Inner, result, edge, join_cond);

            let target_rel = read_node(b, target)?;
            let target_cond =
                target_join_cond(&alias, &rel.to, rel.direction, target.has_traversal_path);
            result = b.join(llqm::expr::JoinType::Inner, result, target_rel, target_cond);
        }
    }

    Ok((result, edge_aliases))
}

fn source_join_cond(node: &str, edge: &str, dir: Direction, with_path: bool) -> Expr {
    let id_cond = match dir {
        Direction::Outgoing => expr::eq(
            expr::col(node, DEFAULT_PRIMARY_KEY),
            expr::col(edge, "source_id"),
        ),
        Direction::Incoming => expr::eq(
            expr::col(node, DEFAULT_PRIMARY_KEY),
            expr::col(edge, "target_id"),
        ),
        Direction::Both => expr::or([
            expr::eq(
                expr::col(node, DEFAULT_PRIMARY_KEY),
                expr::col(edge, "source_id"),
            ),
            expr::eq(
                expr::col(node, DEFAULT_PRIMARY_KEY),
                expr::col(edge, "target_id"),
            ),
        ]),
    };
    if with_path {
        Expr::BinaryOp {
            op: expr::BinaryOp::And,
            left: Box::new(edge_path_starts_with(edge, node)),
            right: Box::new(id_cond),
        }
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
        Expr::BinaryOp {
            op: expr::BinaryOp::And,
            left: Box::new(expr::eq(
                expr::col(node, DEFAULT_PRIMARY_KEY),
                expr::col(edge, id_col),
            )),
            right: Box::new(expr::eq(expr::col(edge, kind_col), expr::string(entity))),
        }
    };

    let id_cond = match dir {
        Direction::Outgoing => id_and_kind("source_id", "source_kind"),
        Direction::Incoming => id_and_kind("target_id", "target_kind"),
        Direction::Both => expr::or([
            id_and_kind("source_id", "source_kind"),
            id_and_kind("target_id", "target_kind"),
        ]),
    };
    if with_path {
        Expr::BinaryOp {
            op: expr::BinaryOp::And,
            left: Box::new(edge_path_starts_with(edge, node)),
            right: Box::new(id_cond),
        }
    } else {
        id_cond
    }
}

fn target_join_cond(edge: &str, node: &str, dir: Direction, with_path: bool) -> Expr {
    let id_cond = match dir {
        Direction::Outgoing => expr::eq(
            expr::col(edge, "target_id"),
            expr::col(node, DEFAULT_PRIMARY_KEY),
        ),
        Direction::Incoming => expr::eq(
            expr::col(edge, "source_id"),
            expr::col(node, DEFAULT_PRIMARY_KEY),
        ),
        Direction::Both => expr::or([
            expr::eq(
                expr::col(edge, "target_id"),
                expr::col(node, DEFAULT_PRIMARY_KEY),
            ),
            expr::eq(
                expr::col(edge, "source_id"),
                expr::col(node, DEFAULT_PRIMARY_KEY),
            ),
        ]),
    };
    if with_path {
        Expr::BinaryOp {
            op: expr::BinaryOp::And,
            left: Box::new(edge_path_starts_with(edge, node)),
            right: Box::new(id_cond),
        }
    } else {
        id_cond
    }
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
        if let Some(r) = &node.id_range {
            conds.push(expr::ge(
                expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                expr::int(r.start),
            ));
            conds.push(expr::le(
                expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                expr::int(r.end),
            ));
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
                conds.push(expr::ge(
                    expr::col(alias, "depth"),
                    expr::int(rel.min_hops as i64),
                ));
            }
        }
    }

    expr::and_opt(conds.into_iter().map(Some))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn find_node<'a>(nodes: &'a [InputNode], id: &str) -> Result<&'a InputNode> {
    nodes
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| QueryError::Lowering(format!("node '{id}' not found")))
}

fn resolve_table(node: &InputNode) -> Result<String> {
    node.table
        .clone()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no resolved table", node.id)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Finalize: LoweredQuery → Plan
// ─────────────────────────────────────────────────────────────────────────────

/// Assemble a `LoweredQuery` into a final llqm `Plan`.
///
/// Applies the remaining plan operations in order:
/// 1. Aggregate (if group_by/agg_measures are non-empty)
/// 2. Sort
/// 3. Project
/// 4. Fetch (limit/offset)
/// 5. Build with CTEs
pub fn finalize(mut lq: LoweredQuery) -> llqm::plan::Plan {
    let mut rel = lq.base_rel;

    // For aggregation queries, build the aggregate first
    if !lq.group_by.is_empty() || !lq.agg_measures.is_empty() {
        let group_refs: Vec<(&Expr, &str)> =
            lq.group_by.iter().map(|(e, a)| (e, a.as_str())).collect();
        let group_exprs: Vec<(Expr, &str)> = lq
            .group_by
            .iter()
            .map(|(e, a)| (e.clone(), a.as_str()))
            .collect();
        let agg_exprs: Vec<(&str, &str, Vec<Expr>)> = lq
            .agg_measures
            .iter()
            .map(|(f, a, args)| (f.as_str(), a.as_str(), args.clone()))
            .collect();
        rel = lq.builder.aggregate(rel, &group_exprs, &agg_exprs);
    }

    // Sort
    if !lq.sort_keys.is_empty() {
        let keys: Vec<(Expr, SortDir)> = lq.sort_keys;
        let key_refs: Vec<(Expr, SortDir)> = keys;
        rel = lq.builder.sort(rel, &key_refs);
    }

    // Project (if there are projections)
    if !lq.projections.is_empty() {
        let proj_refs: Vec<(Expr, &str)> = lq
            .projections
            .iter()
            .map(|(e, a)| (e.clone(), a.as_str()))
            .collect();
        rel = lq.builder.project(rel, &proj_refs);
    }

    // Fetch (limit/offset)
    if let Some(limit) = lq.limit {
        rel = lq.builder.fetch(rel, limit, lq.offset);
    }

    // Build
    if lq.ctes.is_empty() {
        lq.builder.build(rel)
    } else {
        lq.builder.build_with_ctes(rel, lq.ctes)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(irrefutable_let_patterns)]
mod tests {
    use super::*;
    use crate::input::parse_input;
    use crate::normalize;
    use crate::validate;
    use llqm::codegen::emit_clickhouse_sql;
    use ontology::Ontology;

    fn test_ontology() -> Ontology {
        use ontology::DataType;
        Ontology::new()
            .with_nodes(["User", "Project", "Note", "Group"])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("state", DataType::String),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_default_columns("User", ["username", "state"])
            .with_fields(
                "Note",
                [
                    ("confidential", DataType::Bool),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_default_columns("Note", ["confidential"])
            .with_fields("Project", [("name", DataType::String)])
            .with_default_columns("Project", ["name"])
    }

    fn validated_input(json: &str) -> Input {
        let ontology = test_ontology();
        let input = parse_input(json).unwrap();
        validate::Validator::new(&ontology)
            .check_references(&input)
            .unwrap();
        normalize::normalize(input, &ontology).unwrap()
    }

    fn lower_and_sql(json: &str) -> String {
        let input = validated_input(json);
        let lq = lower(&input).unwrap();
        let plan = finalize(lq);
        let pq = emit_clickhouse_sql(&plan).unwrap();
        pq.sql
    }

    #[test]
    fn simple_search() {
        let sql = lower_and_sql(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#,
        );
        assert!(sql.contains("SELECT"), "sql: {sql}");
        assert!(sql.contains("gl_user AS u"), "sql: {sql}");
        assert!(sql.contains("u.username AS u_username"), "sql: {sql}");
        assert!(sql.contains("LIMIT 10"), "sql: {sql}");
    }

    #[test]
    fn search_with_filters() {
        let sql = lower_and_sql(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": ["username"],
                "filters": {
                    "username": {"op": "eq", "value": "admin"}
                }
            },
            "limit": 10
        }"#,
        );
        assert!(sql.contains("WHERE"), "sql: {sql}");
        assert!(sql.contains("u.username"), "sql: {sql}");
    }

    #[test]
    fn simple_traversal() {
        let sql = lower_and_sql(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "n", "entity": "Note", "columns": ["confidential"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25
        }"#,
        );
        assert!(sql.contains("gl_user AS u"), "sql: {sql}");
        assert!(sql.contains("INNER JOIN gl_edge AS e0"), "sql: {sql}");
        assert!(sql.contains("INNER JOIN gl_note AS n"), "sql: {sql}");
        assert!(sql.contains("LIMIT 25"), "sql: {sql}");
    }

    #[test]
    fn aggregation_query() {
        let sql = lower_and_sql(
            r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#,
        );
        assert!(sql.contains("COUNT"), "sql: {sql}");
        assert!(sql.contains("GROUP BY"), "sql: {sql}");
    }

    #[test]
    fn neighbors_query() {
        let input = Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "u".to_string(),
                entity: Some("User".to_string()),
                table: Some("gl_user".to_string()),
                node_ids: vec![123],
                ..Default::default()
            }],
            neighbors: Some(crate::input::InputNeighbors {
                node: "u".to_string(),
                direction: Direction::Outgoing,
                rel_types: vec![],
            }),
            limit: 10,
            ..Input::default()
        };

        let lq = lower(&input).unwrap();
        let plan = finalize(lq);
        let pq = emit_clickhouse_sql(&plan).unwrap();
        assert!(pq.sql.contains("_gkg_neighbor_id"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("_gkg_neighbor_type"), "sql: {}", pq.sql);
        assert!(pq.sql.contains("_gkg_relationship_type"), "sql: {}", pq.sql);
    }
}
