//! Skeleton builders and emitters.
//!
//! `Skeleton::plan()` — reads Input, populates the IR.
//! `Skeleton::emit()` — reads the IR, produces SQL AST fragments.

use std::collections::{HashMap, HashSet};

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use super::types::*;

// ─────────────────────────────────────────────────────────────────────────────
// Plan: build the IR from Input
// ─────────────────────────────────────────────────────────────────────────────

impl Skeleton {
    /// Build the skeleton plan from query input.
    pub fn plan(input: &mut Input) -> Self {
        let hops = build_hops(input);
        let mut nodes = build_node_plans(input);

        // Partial FK elision: when a hop has an FK column and the far-end
        // node is pinned (concrete node_ids), absorb the FK constraint as a
        // filter on the FK-holding node and drop the hop entirely. This
        // eliminates an edge table scan. E.g. IN_PROJECT with project_id FK
        // becomes `mr.project_id = 278964` on the MR dedup, no edge needed.
        let (mut hops, elided_fks, input) = elide_fk_hops(hops, &mut nodes, input);

        // Reorder chain so the most selective node drives the scan.
        // Also reorder input.relationships to stay in sync — the enforce
        // pass uses relationship index to build EdgeMeta (e0_, e1_, etc.).
        let (reordered_hops, reversed) = reorder_by_selectivity(hops, &nodes);
        hops = reordered_hops;
        if reversed {
            input.relationships.reverse();
        }

        assign_id_sources(&hops, &mut nodes);

        for node_plan in nodes.values_mut() {
            node_plan.hydration = determine_hydration(node_plan, input);
        }

        let strategy = if hops.is_empty() {
            Strategy::SingleNode
        } else if let Some(center) = detect_fk_star(&hops) {
            Strategy::FkStar { center }
        } else {
            Strategy::Flat
        };

        // Pre-resolve join columns for each hop based on shared-node topology.
        resolve_join_columns(&mut hops);

        // Pre-compute denorm tags per node (only on the first edge alias
        // where the node appears).
        resolve_denorm_tags(&hops, &mut nodes, &input.compiler.denormalized_columns);

        // Pre-compute node_edge_col mappings from hops + elided_fks.
        let node_edge_mappings = compute_node_edge_mappings(&hops, &elided_fks, &strategy, &nodes);

        // Pre-compute IN-narrowing decisions.
        resolve_narrowing(&hops, &mut nodes);

        // Pre-resolve elevated-access FilterOnly for Skip nodes.
        resolve_elevated_access(&mut nodes, input);

        // Pre-compute dedup columns for each node.
        resolve_dedup_columns(&mut nodes, input);

        // Pre-resolve FK target join needs.
        resolve_fk_join_needs(&hops, &mut nodes, input);

        // Pre-resolve which nodes emit SELECT columns.
        // In aggregation queries, only group_by nodes emit columns.
        if input.query_type == QueryType::Aggregation {
            let group_by_nodes: HashSet<&str> = input
                .aggregations
                .iter()
                .filter_map(|a| a.group_by.as_deref())
                .collect();
            for np in nodes.values_mut() {
                np.emit_select = group_by_nodes.contains(np.alias.as_str());
            }
        }

        let synthesize_fk_edge_metadata = matches!(strategy, Strategy::FkStar { .. })
            && input.query_type != QueryType::Aggregation;

        Self {
            hops,
            nodes,
            strategy,
            elided_fks,
            node_edge_mappings,
            synthesize_fk_edge_metadata,
        }
    }

    /// Emit SQL AST from the plan. Pure AST generation — reads only
    /// from Skeleton fields, does not consult Input.
    pub fn emit(&self, _input: &mut Input) -> Result<SkeletonOutput> {
        match self.strategy {
            Strategy::SingleNode => emit_single_node(self),
            Strategy::FkStar { ref center } => emit_fk_star(self, center),
            Strategy::Flat | Strategy::Bidirectional { .. } => emit_flat_chain(self),
        }
    }
}

/// The output of emitting a skeleton — ready for query-type modules to wrap.
pub struct SkeletonOutput {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
}

impl SkeletonOutput {
    /// Assemble into a final Query.
    pub fn into_query(
        self,
        mut select: Vec<SelectExpr>,
        group_by: Vec<Expr>,
        order_by: Vec<OrderExpr>,
        limit: u32,
    ) -> Query {
        select.extend(self.select);
        Query {
            ctes: self.ctes,
            select,
            from: self.from,
            where_clause: Expr::conjoin(self.where_parts),
            group_by,
            order_by,
            limit: Some(limit),
            ..Default::default()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared primitives: dedup, columns, predicates
// ─────────────────────────────────────────────────────────────────────────────

/// Build a dedup subquery: SELECT cols FROM table ORDER BY _version DESC LIMIT 1 BY id.
fn build_dedup_subquery(alias: &str, table: &str, select: Vec<SelectExpr>) -> Query {
    Query {
        select,
        from: TableRef::scan(table, alias),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    }
}

/// Build SelectExpr list from the pre-computed dedup_columns on NodePlan.
fn collect_dedup_columns(alias: &str, np: &NodePlan) -> Vec<SelectExpr> {
    np.dedup_columns
        .iter()
        .map(|col| SelectExpr::new(Expr::col(alias, col), col.as_str()))
        .collect()
}

/// WHERE predicates for a node: filters + _deleted=false.
fn node_where_predicates(alias: &str, np: &NodePlan) -> Vec<Expr> {
    let mut w = Vec::new();
    w.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));
    for (prop, filter) in &np.filters {
        w.push(filter_to_expr(alias, prop, filter));
    }
    if !np.node_ids.is_empty() {
        w.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
    }
    if let Some(ref range) = np.id_range {
        w.push(id_range_predicate(alias, range));
    }
    w
}

/// Whether an entity requires a higher access level than the default (20).
/// Only these entities need a FilterOnly subquery in edge-based queries so
/// the security pass can enforce their stricter min_access_level.
fn has_elevated_access_level(np: &NodePlan, input: &Input) -> bool {
    let Some(ref entity) = np.entity else {
        return false;
    };
    input
        .entity_auth
        .get(entity)
        .is_some_and(|cfg| cfg.required_access_level > crate::types::DEFAULT_PATH_ACCESS_LEVEL)
}

/// IN-list predicate: `alias.col IN (ids)` or `alias.col = id` for single.
pub fn id_list_predicate(alias: &str, col: &str, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::col(alias, col), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            alias,
            col,
            ChType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false))
    }
}

/// Node columns for the outer SELECT, aliased as `{alias}_{col}` for the
/// graph formatter. Only for non-aggregation queries (aggregation builds
/// its own SELECT).
fn node_select_columns(alias: &str, np: &NodePlan) -> Vec<SelectExpr> {
    if !np.emit_select {
        return vec![];
    }
    requested_columns(&np.columns)
        .into_iter()
        .map(|col| SelectExpr::new(Expr::col(alias, &col), format!("{alias}_{col}")))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan builders
// ─────────────────────────────────────────────────────────────────────────────

/// Elide hops that have an FK column when the far-end node is pinned.
/// Converts the FK into a node-level filter and removes the hop + its
/// input.relationships entry so edge alias indices stay in sync.
#[allow(clippy::type_complexity)]
fn elide_fk_hops<'a>(
    hops: Vec<Hop>,
    nodes: &mut HashMap<String, NodePlan>,
    input: &'a mut Input,
) -> (Vec<Hop>, Vec<(String, String, String)>, &'a mut Input) {
    let mut keep_hops = Vec::new();
    let mut keep_rels = Vec::new();
    let mut elided_fks = Vec::new();

    for (i, hop) in hops.into_iter().enumerate() {
        // Only elide if at least one non-FK hop would remain — otherwise
        // the emit loop has no edges to populate node_edge_col from.
        let remaining_non_fk = keep_hops.len();
        let would_be_last = remaining_non_fk == 0;

        let elide_info = hop.fk.as_ref().and_then(|fk| {
            if would_be_last {
                return None;
            }
            let np = nodes.get(&fk.target_node)?;
            if np.selectivity == Selectivity::Pinned
                && !np.node_ids.is_empty()
                && hop.filters.is_empty()
            {
                Some((
                    fk.fk_node.clone(),
                    fk.fk_column.clone(),
                    np.node_ids.clone(),
                ))
            } else {
                None
            }
        });

        let elided = if let Some((fk_node, fk_column, pinned_ids)) = elide_info.clone() {
            if let Some(fk_np) = nodes.get_mut(&fk_node) {
                let filter = if pinned_ids.len() == 1 {
                    InputFilter {
                        op: Some(FilterOp::Eq),
                        value: Some(serde_json::Value::Number(pinned_ids[0].into())),
                        data_type: None,
                    }
                } else {
                    InputFilter {
                        op: Some(FilterOp::In),
                        value: Some(serde_json::Value::Array(
                            pinned_ids
                                .iter()
                                .map(|&id| serde_json::Value::Number(id.into()))
                                .collect(),
                        )),
                        data_type: None,
                    }
                };
                fk_np.filters.push((fk_column, filter));
                if fk_np.selectivity > Selectivity::Filtered {
                    fk_np.selectivity = Selectivity::Filtered;
                }
                true
            } else {
                false
            }
        } else {
            false
        };

        if elided {
            let (fk_node, fk_column, _) = elide_info.unwrap();
            let target_node = hop.fk.as_ref().map(|fk| fk.target_node.clone()).unwrap();
            elided_fks.push((target_node, fk_node, fk_column));
        } else {
            keep_hops.push(hop);
            if i < input.relationships.len() {
                keep_rels.push(input.relationships[i].clone());
            }
        }
    }

    input.relationships = keep_rels;
    (keep_hops, elided_fks, input)
}

fn detect_fk_star(hops: &[Hop]) -> Option<String> {
    let first_center = hops.first()?.fk.as_ref().map(|fk| &fk.fk_node)?;
    for hop in &hops[1..] {
        let center = hop.fk.as_ref().map(|fk| &fk.fk_node)?;
        if center != first_center {
            return None;
        }
    }
    Some(first_center.clone())
}

fn reorder_by_selectivity(
    mut hops: Vec<Hop>,
    nodes: &HashMap<String, NodePlan>,
) -> (Vec<Hop>, bool) {
    if hops.len() <= 1 {
        return (hops, false);
    }
    let start_sel = nodes
        .get(&hops[0].from_node)
        .map(|np| np.selectivity)
        .unwrap_or(Selectivity::Open);
    let end_sel = nodes
        .get(&hops.last().unwrap().to_node)
        .map(|np| np.selectivity)
        .unwrap_or(Selectivity::Open);

    if end_sel < start_sel {
        hops.reverse();
        for hop in &mut hops {
            std::mem::swap(&mut hop.from_node, &mut hop.to_node);
            hop.direction = match hop.direction {
                Direction::Outgoing => Direction::Incoming,
                Direction::Incoming => Direction::Outgoing,
                Direction::Both => Direction::Both,
            };
        }
        (hops, true)
    } else {
        (hops, false)
    }
}

fn build_hops(input: &Input) -> Vec<Hop> {
    input
        .relationships
        .iter()
        .map(|rel| {
            let edge_table = resolve_edge_table(input, &rel.types);
            let fk = rel.fk_column.as_ref().and_then(|col| {
                let from_table = input
                    .nodes
                    .iter()
                    .find(|n| n.id == rel.from)
                    .and_then(|n| n.table.as_deref())
                    .unwrap_or("");
                let to_table = input
                    .nodes
                    .iter()
                    .find(|n| n.id == rel.to)
                    .and_then(|n| n.table.as_deref())
                    .unwrap_or("");

                let from_has = input
                    .compiler
                    .table_columns
                    .get(from_table)
                    .is_some_and(|cols| cols.contains(col));
                let to_has = input
                    .compiler
                    .table_columns
                    .get(to_table)
                    .is_some_and(|cols| cols.contains(col));

                let (fk_node, target_node) = if from_has {
                    (rel.from.clone(), rel.to.clone())
                } else if to_has {
                    (rel.to.clone(), rel.from.clone())
                } else {
                    return None;
                };
                Some(HopFk {
                    fk_node,
                    fk_column: col.clone(),
                    target_node,
                })
            });
            Hop {
                rel_types: rel.types.clone(),
                edge_table,
                from_node: rel.from.clone(),
                to_node: rel.to.clone(),
                direction: rel.direction,
                min_hops: rel.min_hops,
                max_hops: rel.max_hops,
                fk,
                filters: rel.filters.clone().into_iter().collect(),
                join_prev: None,
            }
        })
        .collect()
}

fn build_node_plans(input: &Input) -> HashMap<String, NodePlan> {
    input
        .nodes
        .iter()
        .map(|n| {
            (
                n.id.clone(),
                NodePlan {
                    alias: n.id.clone(),
                    entity: n.entity.clone(),
                    table: n.table.clone(),
                    selectivity: Selectivity::from_node(n),
                    hydration: HydrationStrategy::Skip,
                    id_source: None,
                    has_traversal_path: n.has_traversal_path,
                    redaction_id_column: n.redaction_id_column.clone(),
                    filters: n.filters.clone().into_iter().collect(),
                    node_ids: n.node_ids.clone(),
                    id_range: n.id_range.clone(),
                    columns: n.columns.clone(),
                    denorm_tags: Vec::new(),
                    dedup_columns: Vec::new(),
                    use_narrowing: false,
                    needs_elevated_filter: false,
                    edge_col_mapping: None,
                    fk_needs_join: false,
                    emit_select: true,
                },
            )
        })
        .collect()
}

fn assign_id_sources(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>) {
    for (i, hop) in hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        for (node, col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if let Some(np) = nodes.get_mut(node)
                && np.id_source.is_none()
            {
                np.id_source = Some(IdSource {
                    edge_alias: alias.clone(),
                    column: col.to_string(),
                });
            }
        }
    }
}

fn determine_hydration(node_plan: &NodePlan, input: &Input) -> HydrationStrategy {
    let alias = &node_plan.alias;

    let is_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(alias.as_str()));
    let is_agg_property_target = input.aggregations.iter().any(|a| {
        a.target.as_deref() == Some(alias.as_str())
            && a.property.is_some()
            && !matches!(a.function, AggFunction::Count)
    });
    let is_order_by_target = input.order_by.as_ref().is_some_and(|ob| ob.node == *alias);

    if is_group_by || is_agg_property_target || is_order_by_target {
        return HydrationStrategy::Join;
    }

    let has_non_denorm_filters = node_plan.filters.iter().any(|(prop, _)| {
        let entity = node_plan.entity.as_deref().unwrap_or("");
        let k1 = (entity.to_string(), prop.clone(), "source".to_string());
        let k2 = (entity.to_string(), prop.clone(), "target".to_string());
        !input.compiler.denormalized_columns.contains_key(&k1)
            && !input.compiler.denormalized_columns.contains_key(&k2)
    });

    if has_non_denorm_filters {
        return HydrationStrategy::FilterOnly;
    }

    HydrationStrategy::Skip
}

/// Pre-resolve join columns for each hop based on shared-node topology
/// with the previous hop.
fn resolve_join_columns(hops: &mut [Hop]) {
    for i in 1..hops.len() {
        let prev_hop = &hops[i - 1];
        let prev_alias = format!("e{}", i - 1);
        let (prev_start, prev_end) = prev_hop.direction.edge_columns();

        let curr_hop = &hops[i];
        let (start_col, end_col) = curr_hop.direction.edge_columns();

        let (prev_col, curr_col) = if prev_hop.to_node == curr_hop.from_node {
            (prev_end, start_col)
        } else if prev_hop.to_node == curr_hop.to_node {
            (prev_end, end_col)
        } else if prev_hop.from_node == curr_hop.from_node {
            (prev_start, start_col)
        } else if prev_hop.from_node == curr_hop.to_node {
            (prev_start, end_col)
        } else {
            (prev_end, start_col)
        };

        hops[i].join_prev = Some(JoinColumns {
            prev_alias,
            prev_col: prev_col.to_string(),
            curr_col: curr_col.to_string(),
        });
    }
}

/// Pre-compute denorm tags per node. Only applies tags on the first edge
/// alias where the node appears (later hops already join on filtered IDs).
fn resolve_denorm_tags(
    hops: &[Hop],
    nodes: &mut HashMap<String, NodePlan>,
    denorm_map: &HashMap<(String, String, String), (String, String)>,
) {
    if denorm_map.is_empty() {
        return;
    }
    let mut applied: HashSet<String> = HashSet::new();
    for (i, hop) in hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if !applied.insert(node_alias.clone()) {
                continue;
            }
            let Some(np) = nodes.get_mut(node_alias) else {
                continue;
            };
            let Some(ref entity) = np.entity else {
                continue;
            };
            let dir = if id_col == SOURCE_ID_COLUMN {
                "source"
            } else {
                "target"
            };
            for (prop, filter) in &np.filters {
                let key = (entity.clone(), prop.clone(), dir.to_string());
                let Some((edge_column, tag_key)) = denorm_map.get(&key) else {
                    continue;
                };
                match filter.op {
                    None | Some(FilterOp::Eq) => {
                        let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                        np.denorm_tags.push(DenormTag {
                            edge_alias: alias.clone(),
                            tag_column: edge_column.clone(),
                            tag_key: tag_key.clone(),
                            tag_value: format!("{tag_key}:{val}"),
                            op: DenormTagOp::Has,
                        });
                    }
                    Some(FilterOp::In) => {
                        if let Some(values) = filter.value.as_ref().and_then(|v| v.as_array()) {
                            let tags: Vec<String> = values
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| format!("{tag_key}:{s}")))
                                .collect();
                            if tags.len() == 1 {
                                np.denorm_tags.push(DenormTag {
                                    edge_alias: alias.clone(),
                                    tag_column: edge_column.clone(),
                                    tag_key: tag_key.clone(),
                                    tag_value: tags[0].clone(),
                                    op: DenormTagOp::Has,
                                });
                            } else if !tags.is_empty() {
                                np.denorm_tags.push(DenormTag {
                                    edge_alias: alias.clone(),
                                    tag_column: edge_column.clone(),
                                    tag_key: tag_key.clone(),
                                    tag_value: String::new(),
                                    op: DenormTagOp::HasAny(tags),
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Pre-compute node_edge_col mappings from hops + elided_fks + strategy.
fn compute_node_edge_mappings(
    hops: &[Hop],
    elided_fks: &[(String, String, String)],
    strategy: &Strategy,
    nodes: &HashMap<String, NodePlan>,
) -> HashMap<String, (String, String)> {
    let mut mappings = HashMap::new();

    match strategy {
        Strategy::FkStar { center } => {
            // Center node maps to itself.
            mappings.insert(
                center.clone(),
                (center.clone(), DEFAULT_PRIMARY_KEY.to_string()),
            );
            // Each hop's target maps via the FK column on the center.
            for hop in hops {
                if let Some(ref fk) = hop.fk {
                    let fk_alias = if fk.fk_node == *center {
                        center.clone()
                    } else {
                        fk.fk_node.clone()
                    };
                    mappings.insert(fk.target_node.clone(), (fk_alias, fk.fk_column.clone()));
                }
            }
        }
        _ => {
            // Flat/Bidirectional: each hop contributes from_node and to_node.
            for (i, hop) in hops.iter().enumerate() {
                let alias = format!("e{i}");
                let (start_col, end_col) = hop.direction.edge_columns();
                mappings
                    .entry(hop.from_node.clone())
                    .or_insert_with(|| (alias.clone(), start_col.to_string()));
                mappings
                    .entry(hop.to_node.clone())
                    .or_insert_with(|| (alias.clone(), end_col.to_string()));
            }
        }
    }

    // Elided FK target nodes.
    for (target_node, fk_node, fk_column) in elided_fks {
        mappings
            .entry(target_node.clone())
            .or_insert_with(|| (fk_node.clone(), fk_column.clone()));
    }

    // Store per-node for convenience.
    let _ = nodes;
    mappings
}

/// Pre-compute IN-narrowing decisions. A node needs narrowing when:
/// - it has Join hydration
/// - it has no user filters, node_ids, or id_range
/// - another node in ANY hop has FilterOnly hydration (i.e. a _filter_ CTE exists)
fn resolve_narrowing(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>) {
    // Check if any node has FilterOnly hydration (will produce a _filter_ CTE).
    let has_filter_only = nodes
        .values()
        .any(|np| np.hydration == HydrationStrategy::FilterOnly);
    if !has_filter_only {
        return;
    }
    // Also check elevated-access nodes that will emit filter CTEs. But we
    // haven't resolved those yet, so check has_elevated_access_level won't
    // work here. Instead, the narrowing decision is based solely on whether
    // _filter_ CTEs exist from FilterOnly nodes. We'll update if
    // needs_elevated_filter also generates CTEs (it does, checked below).
    for hop in hops {
        let (start_col, end_col) = hop.direction.edge_columns();
        for (node_alias, _edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            let Some(np) = nodes.get(node_alias) else {
                continue;
            };
            if np.hydration == HydrationStrategy::Join
                && np.filters.is_empty()
                && np.node_ids.is_empty()
                && np.id_range.is_none()
            {
                // Mark for narrowing. We can't do get_mut while iterating
                // immutably so collect the aliases first.
                let alias = node_alias.clone();
                if let Some(np_mut) = nodes.get_mut(&alias) {
                    np_mut.use_narrowing = true;
                }
            }
        }
    }
}

/// Pre-resolve elevated-access FilterOnly for Skip nodes.
fn resolve_elevated_access(nodes: &mut HashMap<String, NodePlan>, input: &Input) {
    let aliases: Vec<String> = nodes.keys().cloned().collect();
    for alias in aliases {
        let np = &nodes[&alias];
        if np.hydration == HydrationStrategy::Skip
            && np.has_traversal_path
            && np.table.is_some()
            && has_elevated_access_level(np, input)
        {
            nodes.get_mut(&alias).unwrap().needs_elevated_filter = true;
        }
    }
}

/// Pre-compute dedup columns for each node from the query input.
fn resolve_dedup_columns(nodes: &mut HashMap<String, NodePlan>, input: &Input) {
    let aliases: Vec<String> = nodes.keys().cloned().collect();
    for alias in aliases {
        let np = &nodes[&alias];
        let mut seen = HashSet::new();
        let mut cols = Vec::new();

        let mut push = |col: &str| {
            if seen.insert(col.to_string()) {
                cols.push(col.to_string());
            }
        };

        push(DEFAULT_PRIMARY_KEY);
        push(VERSION_COLUMN);
        if np.has_traversal_path {
            push(TRAVERSAL_PATH_COLUMN);
        }

        for col in requested_columns(&np.columns) {
            push(&col);
        }

        for (prop, _) in &np.filters {
            push(prop);
        }

        for agg in &input.aggregations {
            if agg.target.as_deref() == Some(alias.as_str())
                && let Some(ref prop) = agg.property
            {
                push(prop);
            }
        }

        if let Some(ref ob) = input.order_by
            && ob.node == alias
        {
            push(&ob.property);
        }

        if np.redaction_id_column != DEFAULT_PRIMARY_KEY {
            push(&np.redaction_id_column);
        }

        push(DELETED_COLUMN);

        nodes.get_mut(&alias).unwrap().dedup_columns = cols;
    }
}

/// Pre-resolve whether FK target nodes need inline JOIN hydration.
fn resolve_fk_join_needs(hops: &[Hop], nodes: &mut HashMap<String, NodePlan>, input: &Input) {
    for hop in hops {
        let Some(ref fk) = hop.fk else { continue };
        let Some(np) = nodes.get(&fk.target_node) else {
            continue;
        };
        let needs = np.hydration == HydrationStrategy::Join
            || (input.query_type != QueryType::Aggregation
                && matches!(&np.columns, Some(ColumnSelection::List(cols)) if !cols.is_empty()));
        if needs && let Some(np_mut) = nodes.get_mut(&fk.target_node) {
            np_mut.fk_needs_join = true;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit: single node (no edges)
// ─────────────────────────────────────────────────────────────────────────────

fn emit_single_node(skeleton: &Skeleton) -> Result<SkeletonOutput> {
    let (_, np) = skeleton
        .nodes
        .iter()
        .next()
        .ok_or_else(|| QueryError::Lowering("no nodes in skeleton".into()))?;
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;

    let from = TableRef::Subquery {
        query: Box::new(build_dedup_subquery(alias, table, vec![SelectExpr::star()])),
        alias: alias.to_string(),
    };

    let where_parts = node_where_predicates(alias, np);
    let select = node_select_columns(alias, np);

    Ok(SkeletonOutput {
        from,
        edge_aliases: vec![],
        where_parts,
        select,
        ctes: vec![],
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit: flat edge chain
// ─────────────────────────────────────────────────────────────────────────────

fn emit_flat_chain(skeleton: &Skeleton) -> Result<SkeletonOutput> {
    let mut where_parts = Vec::new();
    let mut edge_aliases = Vec::new();
    let mut ctes = Vec::new();
    let mut from: Option<TableRef> = None;

    for (i, hop) in skeleton.hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        let is_multi_hop = hop.max_hops > 1;

        // Build edge source: UNION ALL for multi-hop, plain scan for single.
        let edge_source = if is_multi_hop {
            let (union, union_wheres) = build_multi_hop_union(hop, &alias, &skeleton.nodes);
            where_parts.extend(union_wheres);
            union
        } else {
            TableRef::scan(&hop.edge_table, &alias)
        };

        // JOIN to previous hop (or set as initial FROM) using pre-resolved
        // join columns.
        if let Some(prev_from) = from.take() {
            let jc = hop
                .join_prev
                .as_ref()
                .expect("non-first hop must have join_prev");
            from = Some(TableRef::join(
                JoinType::Inner,
                prev_from,
                edge_source,
                Expr::eq(
                    Expr::col(&jc.prev_alias, &jc.prev_col),
                    Expr::col(&alias, &jc.curr_col),
                ),
            ));
        } else {
            from = Some(edge_source);
        }

        if !is_multi_hop {
            push_edge_predicates(
                &mut where_parts,
                &alias,
                hop,
                &skeleton.nodes,
                start_col,
                end_col,
            );
        }

        // Relationship-level filters (edge property predicates from the query).
        for (prop, filter) in &hop.filters {
            where_parts.push(filter_to_expr(&alias, prop, filter));
        }

        // Apply pre-computed denorm tags from the skeleton nodes.
        emit_precomputed_denorm_tags(&mut where_parts, &skeleton.nodes, hop, start_col, end_col);
        emit_node_ids_on_edge(
            &mut where_parts,
            &alias,
            hop,
            &skeleton.nodes,
            start_col,
            end_col,
        );

        edge_aliases.push(alias);
    }

    let mut from = from.ok_or_else(|| QueryError::Lowering("no hops in skeleton".into()))?;
    let mut selects = Vec::new();
    let mut hydrated: HashSet<String> = HashSet::new();

    for (i, hop) in skeleton.hops.iter().enumerate() {
        let edge_alias = &edge_aliases[i];
        let (start_col, end_col) = hop.direction.edge_columns();

        for (node_alias, edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if !hydrated.insert(node_alias.clone()) {
                continue;
            }
            let Some(np) = skeleton.nodes.get(node_alias) else {
                continue;
            };
            match np.hydration {
                HydrationStrategy::Join => {
                    // Use pre-resolved narrowing decision from plan().
                    let narrow_cte = if np.use_narrowing
                        && ctes.iter().any(|c: &Cte| c.name.starts_with("_filter_"))
                    {
                        let narrow_name = format!("_narrow_{}", np.alias);
                        let narrow_query = Query {
                            select: vec![SelectExpr::new(
                                Expr::col(edge_alias, edge_col),
                                DEFAULT_PRIMARY_KEY,
                            )],
                            from: TableRef::scan(&hop.edge_table, format!("{edge_alias}n")),
                            where_clause: {
                                let mut nw = Vec::new();
                                push_edge_predicates(
                                    &mut nw,
                                    &format!("{edge_alias}n"),
                                    hop,
                                    &skeleton.nodes,
                                    start_col,
                                    end_col,
                                );
                                Expr::conjoin(nw)
                            },
                            ..Default::default()
                        };
                        ctes.push(Cte::new(&narrow_name, narrow_query));
                        Some(narrow_name)
                    } else {
                        None
                    };

                    let (new_from, ns, nw) = emit_node_join_with_narrowing(
                        from,
                        np,
                        edge_alias,
                        edge_col,
                        false,
                        narrow_cte.as_deref(),
                    )?;
                    from = new_from;
                    selects.extend(ns);
                    where_parts.extend(nw);
                }
                HydrationStrategy::FilterOnly => {
                    where_parts.extend(emit_filter_subquery(np, edge_alias, edge_col, &mut ctes)?);
                }
                HydrationStrategy::Skip => {
                    // Use pre-resolved elevated-access decision from plan().
                    if np.needs_elevated_filter {
                        where_parts
                            .extend(emit_filter_subquery(np, edge_alias, edge_col, &mut ctes)?);
                    }
                }
            }
        }
    }

    Ok(SkeletonOutput {
        from,
        edge_aliases,
        where_parts,
        select: selects,
        ctes,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit: FK star (all hops FK to same center node, zero edges)
// Also handles single-hop FK (FkDirect is just FkStar with 1 hop).
// ─────────────────────────────────────────────────────────────────────────────

fn emit_fk_star(skeleton: &Skeleton, center_alias: &str) -> Result<SkeletonOutput> {
    let center_np = skeleton.nodes.get(center_alias).ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' not found"))
    })?;
    let center_table = center_np.table.as_deref().ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' has no table"))
    })?;

    // Build center dedup columns from pre-computed list + FK columns.
    let mut center_cols = collect_dedup_columns(center_alias, center_np);
    // Add FK columns for each hop (not covered by dedup_columns).
    for hop in &skeleton.hops {
        if let Some(ref fk) = hop.fk
            && !center_cols
                .iter()
                .any(|s| s.alias.as_deref() == Some(fk.fk_column.as_str()))
        {
            center_cols.push(SelectExpr::new(
                Expr::col(center_alias, &fk.fk_column),
                fk.fk_column.as_str(),
            ));
        }
    }

    let center_dedup = build_dedup_subquery(center_alias, center_table, center_cols);
    let mut from = TableRef::Subquery {
        query: Box::new(center_dedup),
        alias: center_alias.to_string(),
    };

    let mut where_parts = node_where_predicates(center_alias, center_np);
    let mut selects = node_select_columns(center_alias, center_np);
    let mut ctes = Vec::new();

    // Each hop: target node connected via FK column.
    for hop in &skeleton.hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FkStar hop missing FK metadata".into()))?;
        let target_np = skeleton.nodes.get(&fk.target_node).ok_or_else(|| {
            QueryError::Lowering(format!("FK target '{}' not found", fk.target_node))
        })?;

        let fk_alias = if fk.fk_node == center_alias {
            center_alias.to_string()
        } else {
            fk.fk_node.clone()
        };

        // Pinned target IDs.
        if !target_np.node_ids.is_empty() {
            where_parts.push(id_list_predicate(
                &fk_alias,
                &fk.fk_column,
                &target_np.node_ids,
            ));
        }

        // Target hydration — use pre-resolved fk_needs_join.
        if target_np.fk_needs_join {
            let (new_from, ns, nw) =
                emit_node_join(from, target_np, &fk_alias, &fk.fk_column, true)?;
            from = new_from;
            selects.extend(ns);
            where_parts.extend(nw);
        } else if target_np.hydration == HydrationStrategy::FilterOnly
            || target_np.needs_elevated_filter
        {
            where_parts.extend(emit_filter_subquery(
                target_np,
                &fk_alias,
                &fk.fk_column,
                &mut ctes,
            )?);
        }
    }

    // Synthesize edge metadata columns for the graph formatter.
    // FK paths have no edge table, but traversal queries need e0_type,
    // e0_src, e0_src_type, e0_dst, e0_dst_type for each relationship.
    // Aggregation queries don't need edge columns — the flag was pre-computed.
    let mut edge_aliases = Vec::new();
    if !skeleton.synthesize_fk_edge_metadata {
        return Ok(SkeletonOutput {
            from,
            edge_aliases,
            where_parts,
            select: selects,
            ctes,
        });
    }
    for (i, hop) in skeleton.hops.iter().enumerate() {
        let ea = format!("e{i}");
        let fk = hop.fk.as_ref().unwrap();
        let from_np = skeleton.nodes.get(&hop.from_node);
        let to_np = skeleton.nodes.get(&hop.to_node);
        let from_entity = from_np.and_then(|n| n.entity.as_deref()).unwrap_or("");
        let to_entity = to_np.and_then(|n| n.entity.as_deref()).unwrap_or("");
        let rel_type = hop.rel_types.first().map(|s| s.as_str()).unwrap_or("");

        // Source ID/kind and target ID/kind from the FK relationship.
        let (src_id_expr, src_kind, tgt_id_expr, tgt_kind) = if fk.fk_node == hop.from_node {
            (
                Expr::col(center_alias, DEFAULT_PRIMARY_KEY),
                from_entity,
                Expr::col(center_alias, &fk.fk_column),
                to_entity,
            )
        } else {
            (
                Expr::col(center_alias, &fk.fk_column),
                from_entity,
                Expr::col(center_alias, DEFAULT_PRIMARY_KEY),
                to_entity,
            )
        };

        selects.push(SelectExpr::new(
            Expr::string(rel_type),
            format!("{ea}_{EDGE_TYPE_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            src_id_expr,
            format!("{ea}_{EDGE_SRC_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::string(src_kind),
            format!("{ea}_{EDGE_SRC_TYPE_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            tgt_id_expr,
            format!("{ea}_{EDGE_DST_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::string(tgt_kind),
            format!("{ea}_{EDGE_DST_TYPE_SUFFIX}"),
        ));
        edge_aliases.push(ea);
    }

    Ok(SkeletonOutput {
        from,
        edge_aliases,
        where_parts,
        select: selects,
        ctes,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers: node hydration
// ─────────────────────────────────────────────────────────────────────────────

fn emit_node_join_with_narrowing(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
    narrow_cte: Option<&str>,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    emit_node_join_inner(
        from,
        np,
        edge_alias,
        edge_col,
        use_traversal_path_join,
        narrow_cte,
    )
}

/// JOIN a node's dedup subquery into the FROM tree.
///
/// `use_traversal_path_join`: true for FK paths (node-to-node), false for
/// edge paths (edge.traversal_path has different semantics than node's).
fn emit_node_join(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    emit_node_join_inner(
        from,
        np,
        edge_alias,
        edge_col,
        use_traversal_path_join,
        None,
    )
}

fn emit_node_join_inner(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
    narrow_cte: Option<&str>,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;

    let dedup_cols = collect_dedup_columns(alias, np);
    let mut dedup_query = build_dedup_subquery(alias, table, dedup_cols);

    // Push user filters + node_ids + id_range INTO the dedup scan so
    // ClickHouse can use them as prewhere predicates to skip granules.
    // _deleted stays OUTSIDE (after LIMIT 1 BY) so a deleted latest
    // version correctly suppresses the entity.
    let mut scan_where = Vec::new();
    for (prop, filter) in &np.filters {
        scan_where.push(filter_to_expr(alias, prop, filter));
    }
    if !np.node_ids.is_empty() {
        scan_where.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
    }
    if let Some(ref range) = np.id_range {
        scan_where.push(id_range_predicate(alias, range));
    }

    // IN-narrowing: restrict the dedup scan to IDs from the narrow CTE.
    if let Some(cte_name) = narrow_cte {
        scan_where.push(Expr::InSubquery {
            expr: Box::new(Expr::col(alias, DEFAULT_PRIMARY_KEY)),
            cte_name: cte_name.to_string(),
            column: DEFAULT_PRIMARY_KEY.to_string(),
        });
    }

    if let Some(combined) = Expr::conjoin(scan_where) {
        dedup_query.where_clause = match dedup_query.where_clause {
            Some(existing) => Some(Expr::and(existing, combined)),
            None => Some(combined),
        };
    }

    let mut on = Expr::eq(
        Expr::col(alias, DEFAULT_PRIMARY_KEY),
        Expr::col(edge_alias, edge_col),
    );
    if use_traversal_path_join && np.has_traversal_path {
        on = Expr::and(
            on,
            Expr::eq(
                Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                Expr::col(edge_alias, TRAVERSAL_PATH_COLUMN),
            ),
        );
    }

    let joined = TableRef::join(
        JoinType::Inner,
        from,
        TableRef::Subquery {
            query: Box::new(dedup_query),
            alias: alias.to_string(),
        },
        on,
    );

    let selects = node_select_columns(alias, np);
    // Only _deleted=false in the outer WHERE; user filters are already
    // inside the dedup scan.
    let wheres = vec![Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    )];

    Ok((joined, selects, wheres))
}

fn emit_filter_subquery(
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    ctes: &mut Vec<Cte>,
) -> Result<Vec<Expr>> {
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;
    let cte_name = format!("_filter_{alias}");

    // User filters + node_ids + id_range go INSIDE the scan so ClickHouse
    // can use them as prewhere predicates and skip non-matching granules.
    // _deleted goes OUTSIDE (after dedup) so a deleted latest version
    // correctly suppresses the entity even if an older version matches.
    let mut scan_where = Vec::new();
    for (prop, filter) in &np.filters {
        scan_where.push(filter_to_expr(alias, prop, filter));
    }
    if !np.node_ids.is_empty() {
        scan_where.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
    }
    if let Some(ref range) = np.id_range {
        scan_where.push(id_range_predicate(alias, range));
    }

    let dedup = Query {
        select: vec![
            SelectExpr::new(Expr::col(alias, DEFAULT_PRIMARY_KEY), DEFAULT_PRIMARY_KEY),
            SelectExpr::new(Expr::col(alias, DELETED_COLUMN), DELETED_COLUMN),
        ],
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(scan_where),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    let inner = Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::Subquery {
            query: Box::new(dedup),
            alias: alias.to_string(),
        },
        where_clause: Some(Expr::eq(
            Expr::col(alias, DELETED_COLUMN),
            Expr::param(ChType::Bool, false),
        )),
        ..Default::default()
    };

    ctes.push(Cte::new(&cte_name, inner));

    Ok(vec![Expr::InSubquery {
        expr: Box::new(Expr::col(edge_alias, edge_col)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    }])
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers: edge predicates
// ─────────────────────────────────────────────────────────────────────────────

fn push_edge_predicates(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    start_col: &str,
    end_col: &str,
) {
    if let Some(f) = rel_kind_filter(alias, &hop.rel_types) {
        where_parts.push(f);
    }
    // Entity kind filters.
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        if let Some(np) = nodes.get(node_alias)
            && let Some(ref entity) = np.entity
        {
            let kind_col = if id_col == SOURCE_ID_COLUMN {
                SOURCE_KIND_COLUMN
            } else {
                TARGET_KIND_COLUMN
            };
            where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
        }
    }
    where_parts.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));
}

/// Emit pre-computed denorm tags from the skeleton's NodePlans.
/// Tags were resolved in plan() and stored on each NodePlan.
fn emit_precomputed_denorm_tags(
    where_parts: &mut Vec<Expr>,
    nodes: &HashMap<String, NodePlan>,
    hop: &Hop,
    start_col: &str,
    end_col: &str,
) {
    for (node_alias, _id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = nodes.get(node_alias) else {
            continue;
        };
        for tag in &np.denorm_tags {
            match &tag.op {
                DenormTagOp::Has => {
                    where_parts.push(Expr::func(
                        "has",
                        vec![
                            Expr::col(&tag.edge_alias, &tag.tag_column),
                            Expr::string(&tag.tag_value),
                        ],
                    ));
                }
                DenormTagOp::HasAny(tags) => {
                    where_parts.push(Expr::func(
                        "hasAny",
                        vec![
                            Expr::col(&tag.edge_alias, &tag.tag_column),
                            Expr::func("array", tags.iter().map(Expr::string).collect()),
                        ],
                    ));
                }
            }
        }
    }
}

fn emit_node_ids_on_edge(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    start_col: &str,
    end_col: &str,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = nodes.get(node_alias) else {
            continue;
        };
        if let Some(ref range) = np.id_range {
            where_parts.push(Expr::and(
                Expr::binary(Op::Ge, Expr::col(alias, id_col), Expr::int(range.start)),
                Expr::binary(Op::Le, Expr::col(alias, id_col), Expr::int(range.end)),
            ));
        }
        if !np.node_ids.is_empty() {
            where_parts.push(id_list_predicate(alias, id_col, &np.node_ids));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge SELECT columns
// ─────────────────────────────────────────────────────────────────────────────

pub fn edge_select_columns(alias: &str) -> Vec<SelectExpr> {
    edge_select_columns_with_prefix(alias, alias)
}

pub fn edge_select_columns_with_prefix(alias: &str, prefix: &str) -> Vec<SelectExpr> {
    [
        (RELATIONSHIP_KIND_COLUMN, EDGE_TYPE_SUFFIX),
        (SOURCE_ID_COLUMN, EDGE_SRC_SUFFIX),
        (SOURCE_KIND_COLUMN, EDGE_SRC_TYPE_SUFFIX),
        (TARGET_ID_COLUMN, EDGE_DST_SUFFIX),
        (TARGET_KIND_COLUMN, EDGE_DST_TYPE_SUFFIX),
    ]
    .iter()
    .map(|(col, suffix)| SelectExpr::new(Expr::col(alias, *col), format!("{prefix}_{suffix}")))
    .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Generic helpers
// ─────────────────────────────────────────────────────────────────────────────

pub fn resolve_edge_table(input: &Input, rel_types: &[String]) -> String {
    for t in rel_types {
        if let Some(table) = input.compiler.edge_table_for_rel.get(t) {
            return table.clone();
        }
    }
    input.compiler.default_edge_table.clone()
}

fn rel_kind_filter(alias: &str, types: &[String]) -> Option<Expr> {
    if types.is_empty() || (types.len() == 1 && types[0] == "*") {
        return None;
    }
    if types.len() == 1 {
        Some(Expr::eq(
            Expr::col(alias, RELATIONSHIP_KIND_COLUMN),
            Expr::string(&types[0]),
        ))
    } else {
        Expr::col_in(
            alias,
            RELATIONSHIP_KIND_COLUMN,
            ChType::String,
            types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
    }
}

pub fn filter_to_expr(alias: &str, prop: &str, filter: &InputFilter) -> Expr {
    let col = Expr::col(alias, prop);
    let val = || filter.value.clone().unwrap_or(serde_json::Value::Null);
    let str_val = || filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    let typed = |v: serde_json::Value| -> Expr {
        Expr::param(data_type_to_ch(filter.data_type.as_ref()), v)
    };

    match filter.op {
        None | Some(FilterOp::Eq) => Expr::eq(col, typed(val())),
        Some(FilterOp::Gt) => Expr::binary(Op::Gt, col, typed(val())),
        Some(FilterOp::Gte) => Expr::binary(Op::Ge, col, typed(val())),
        Some(FilterOp::Lt) => Expr::binary(Op::Lt, col, typed(val())),
        Some(FilterOp::Lte) => Expr::binary(Op::Le, col, typed(val())),
        Some(FilterOp::In) => {
            if let Some(arr) = filter.value.as_ref().and_then(|v| v.as_array()) {
                Expr::col_in(
                    alias,
                    prop,
                    data_type_to_ch(filter.data_type.as_ref()),
                    arr.clone(),
                )
                .unwrap_or_else(|| Expr::param(ChType::Bool, false))
            } else {
                Expr::param(ChType::Bool, false)
            }
        }
        Some(FilterOp::Contains) => Expr::func(
            "positionCaseInsensitive",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::StartsWith) => Expr::func(
            "startsWith",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::EndsWith) => Expr::func(
            "endsWith",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::IsNull) => Expr::unary(Op::IsNull, col),
        Some(FilterOp::IsNotNull) => Expr::unary(Op::IsNotNull, col),
        Some(FilterOp::TokenMatch) => Expr::func(
            "hasToken",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::AllTokens) => Expr::func(
            "hasAllTokens",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::AnyTokens) => Expr::func(
            "hasAnyTokens",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
    }
}

fn data_type_to_ch(dt: Option<&ontology::DataType>) -> ChType {
    match dt {
        Some(ontology::DataType::String | ontology::DataType::Enum | ontology::DataType::Uuid) => {
            ChType::String
        }
        Some(ontology::DataType::Int) => ChType::Int64,
        Some(ontology::DataType::Float) => ChType::Float64,
        Some(ontology::DataType::Bool) => ChType::Bool,
        Some(ontology::DataType::DateTime | ontology::DataType::Date) => ChType::DateTime64,
        None => ChType::String,
    }
}

pub fn requested_columns(columns: &Option<ColumnSelection>) -> Vec<String> {
    match columns {
        Some(ColumnSelection::List(cols)) => cols.clone(),
        Some(ColumnSelection::All) => vec!["*".to_string()],
        None => vec![],
    }
}

pub fn node_ids_predicate(alias: &str, ids: &[i64]) -> Expr {
    id_list_predicate(alias, DEFAULT_PRIMARY_KEY, ids)
}

pub fn id_range_predicate(alias: &str, range: &InputIdRange) -> Expr {
    Expr::and(
        Expr::binary(
            Op::Ge,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::int(range.start),
        ),
        Expr::binary(
            Op::Le,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::int(range.end),
        ),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Variable-length: UNION ALL of edge chains
// ─────────────────────────────────────────────────────────────────────────────

fn build_multi_hop_union(
    hop: &Hop,
    alias: &str,
    nodes: &HashMap<String, NodePlan>,
) -> (TableRef, Vec<Expr>) {
    let start = hop.min_hops.max(1);
    let (start_col, end_col) = hop.direction.edge_columns();
    let end_type_col = match hop.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };

    let type_filter = rel_kind_filter_values(&hop.rel_types);

    let queries: Vec<Query> = (start..=hop.max_hops)
        .map(|depth| {
            build_depth_arm(
                depth,
                &hop.edge_table,
                start_col,
                end_col,
                end_type_col,
                hop.direction,
                &type_filter,
            )
        })
        .collect();

    let union = TableRef::union_all(queries, alias);

    // For incoming edges, the from_node is on the target side and the
    // to_node is on the source side (the depth arm already swaps the
    // projected source/target columns, so the outer alias exposes
    // source_id/source_kind as the "start" of the incoming traversal).
    let mut where_parts = Vec::new();
    let (from_kind_col, to_kind_col) = match hop.direction {
        Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
        Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
    };
    for (node_alias, kind_col) in [(&hop.from_node, from_kind_col), (&hop.to_node, to_kind_col)] {
        if let Some(np) = nodes.get(node_alias)
            && let Some(ref entity) = np.entity
        {
            where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
        }
    }
    where_parts.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    (union, where_parts)
}

fn rel_kind_filter_values(types: &[String]) -> Option<Vec<String>> {
    if types.is_empty() || (types.len() == 1 && types[0] == "*") {
        None
    } else {
        Some(types.to_vec())
    }
}

fn build_depth_arm(
    depth: u32,
    edge_table: &str,
    start_col: &str,
    end_col: &str,
    end_type_col: &str,
    direction: Direction,
    type_filter: &Option<Vec<String>>,
) -> Query {
    let mut from = TableRef::scan(edge_table, "e1");
    // First edge: relationship kind + _deleted filter.
    let mut where_parts = Vec::new();
    if let Some(types) = type_filter
        && let Some(f) = Expr::col_in(
            "e1",
            RELATIONSHIP_KIND_COLUMN,
            ChType::String,
            types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
    {
        where_parts.push(f);
    }
    where_parts.push(Expr::eq(
        Expr::col("e1", DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));
    let where_clause = Expr::conjoin(where_parts);

    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let right = TableRef::scan(edge_table, &curr);
        let mut join_on = Expr::eq(Expr::col(&prev, end_col), Expr::col(&curr, start_col));
        // _deleted = false on every chained edge.
        join_on = Expr::and(
            join_on,
            Expr::eq(
                Expr::col(&curr, DELETED_COLUMN),
                Expr::param(ChType::Bool, false),
            ),
        );
        if let Some(types) = type_filter
            && let Some(tc) = Expr::col_in(
                &curr,
                RELATIONSHIP_KIND_COLUMN,
                ChType::String,
                types
                    .iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            )
        {
            join_on = Expr::and(join_on, tc);
        }
        from = TableRef::join(JoinType::Inner, from, right, join_on);
    }

    let last = format!("e{depth}");

    let (rel_kind, src_id, src_kind, tgt_id, tgt_kind) = match direction {
        Direction::Outgoing | Direction::Both => (
            Expr::col("e1", RELATIONSHIP_KIND_COLUMN),
            Expr::col("e1", SOURCE_ID_COLUMN),
            Expr::col("e1", SOURCE_KIND_COLUMN),
            Expr::col(&last, TARGET_ID_COLUMN),
            Expr::col(&last, TARGET_KIND_COLUMN),
        ),
        Direction::Incoming => (
            Expr::col(&last, RELATIONSHIP_KIND_COLUMN),
            Expr::col(&last, SOURCE_ID_COLUMN),
            Expr::col(&last, SOURCE_KIND_COLUMN),
            Expr::col("e1", TARGET_ID_COLUMN),
            Expr::col("e1", TARGET_KIND_COLUMN),
        ),
    };

    let path_nodes = Expr::func(
        "array",
        (1..=depth)
            .map(|i| {
                let e = format!("e{i}");
                Expr::func(
                    "tuple",
                    vec![Expr::col(&e, end_col), Expr::col(&e, end_type_col)],
                )
            })
            .collect(),
    );

    Query {
        select: vec![
            SelectExpr::new(Expr::col("e1", start_col), start_col),
            SelectExpr::new(Expr::col(&last, end_col), end_col),
            SelectExpr::new(rel_kind, RELATIONSHIP_KIND_COLUMN),
            SelectExpr::new(src_id, SOURCE_ID_COLUMN),
            SelectExpr::new(src_kind, SOURCE_KIND_COLUMN),
            SelectExpr::new(tgt_id, TARGET_ID_COLUMN),
            SelectExpr::new(tgt_kind, TARGET_KIND_COLUMN),
            SelectExpr::new(path_nodes, PATH_NODES_COLUMN),
            SelectExpr::new(Expr::int(depth as i64), DEPTH_COLUMN),
            SelectExpr::new(Expr::col("e1", DELETED_COLUMN), DELETED_COLUMN),
            SelectExpr::new(
                Expr::col("e1", TRAVERSAL_PATH_COLUMN),
                TRAVERSAL_PATH_COLUMN,
            ),
        ],
        from,
        where_clause,
        ..Default::default()
    }
}
