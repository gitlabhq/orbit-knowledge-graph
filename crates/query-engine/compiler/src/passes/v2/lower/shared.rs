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
    pub fn plan(input: &Input) -> Self {
        let hops = build_hops(input);
        let nodes = build_node_plans(input);

        // Reorder chain so the most selective node drives the scan.
        let hops = reorder_by_selectivity(hops, &nodes);

        let mut nodes = nodes;
        // Assign ID sources: which edge alias + column carries each node's ID.
        assign_id_sources(&hops, &input.relationships, &mut nodes);

        // Determine hydration strategy per node.
        for (_, node_plan) in &mut nodes {
            node_plan.hydration = determine_hydration(node_plan, input);
        }

        let strategy = if hops.is_empty() {
            Strategy::SingleNode
        } else if let Some(center) = detect_fk_star(&hops) {
            Strategy::FkStar { center }
        } else {
            Strategy::Flat
        };

        Self {
            hops,
            nodes,
            strategy,
        }
    }

    /// Emit SQL AST from the plan. Returns (from, edge_aliases, where_parts, ctes).
    /// Also registers node-edge mappings on the compiler context.
    pub fn emit(&self, input: &mut Input) -> Result<SkeletonOutput> {
        match self.strategy {
            Strategy::SingleNode => emit_single_node(self, input),
            Strategy::FkStar { ref center } => emit_fk_star(self, center, input),
            Strategy::Flat => emit_flat_chain(self, input),
            Strategy::Bidirectional { .. } => emit_flat_chain(self, input),
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
// Plan builders
// ─────────────────────────────────────────────────────────────────────────────

/// Detect if all hops have FKs on the same center node (star-schema pattern).
/// Returns the center node alias if so.
fn detect_fk_star(hops: &[Hop]) -> Option<String> {
    if hops.is_empty() {
        return None;
    }
    let first_center = hops[0].fk.as_ref().map(|fk| &fk.fk_node)?;
    for hop in &hops[1..] {
        let center = hop.fk.as_ref().map(|fk| &fk.fk_node)?;
        if center != first_center {
            return None;
        }
    }
    Some(first_center.clone())
}

/// Reorder the hop chain so the most selective node is at the start.
/// ClickHouse processes JOINs left-to-right, so the leftmost edge becomes
/// the build side of the first hash join. Putting the selective end first
/// means a tiny hash table probing outward instead of a large scan.
fn reorder_by_selectivity(mut hops: Vec<Hop>, nodes: &HashMap<String, NodePlan>) -> Vec<Hop> {
    if hops.len() <= 1 {
        return hops;
    }

    // Find the most selective node in the chain.
    let chain_start = &hops[0].from_node;
    let chain_end = &hops.last().unwrap().to_node;

    let start_sel = nodes
        .get(chain_start)
        .map(|np| np.selectivity)
        .unwrap_or(Selectivity::Open);
    let end_sel = nodes
        .get(chain_end)
        .map(|np| np.selectivity)
        .unwrap_or(Selectivity::Open);

    // If the end is more selective than the start, reverse the chain.
    if end_sel < start_sel {
        hops.reverse();
        for hop in &mut hops {
            std::mem::swap(&mut hop.from_node, &mut hop.to_node);
            hop.direction = flip_direction(hop.direction);
        }
    }

    hops
}

fn flip_direction(d: Direction) -> Direction {
    match d {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn build_hops(input: &Input) -> Vec<Hop> {
    input
        .relationships
        .iter()
        .map(|rel| {
            let edge_table = resolve_edge_table(input, &rel.types);
            let fk = rel.fk_column.as_ref().and_then(|col| {
                // Resolve which node has the FK column by checking node tables.
                // If neither node has the column, skip FK optimization.
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

                let from_has_col = input
                    .compiler
                    .table_columns
                    .get(from_table)
                    .is_some_and(|cols| cols.contains(col));
                let to_has_col = input
                    .compiler
                    .table_columns
                    .get(to_table)
                    .is_some_and(|cols| cols.contains(col));

                let (fk_node, target_node) = if from_has_col {
                    (rel.from.clone(), rel.to.clone())
                } else if to_has_col {
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
                max_hops: rel.max_hops,
                fk,
            }
        })
        .collect()
}

fn build_node_plans(input: &Input) -> HashMap<String, NodePlan> {
    input
        .nodes
        .iter()
        .map(|n| {
            let plan = NodePlan {
                alias: n.id.clone(),
                entity: n.entity.clone(),
                table: n.table.clone(),
                selectivity: Selectivity::from_node(n),
                hydration: HydrationStrategy::Skip, // set later
                id_source: None,                    // set later
                has_traversal_path: n.has_traversal_path,
                redaction_id_column: n.redaction_id_column.clone(),
                filters: n.filters.clone().into_iter().collect(),
                node_ids: n.node_ids.clone(),
                id_range: n.id_range.clone(),
                columns: n.columns.clone(),
            };
            (n.id.clone(), plan)
        })
        .collect()
}

fn assign_id_sources(
    hops: &[Hop],
    _rels: &[InputRelationship],
    nodes: &mut HashMap<String, NodePlan>,
) {
    for (i, hop) in hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();

        if let Some(np) = nodes.get_mut(&hop.from_node) {
            if np.id_source.is_none() {
                np.id_source = Some(IdSource {
                    edge_alias: alias.clone(),
                    column: start_col.to_string(),
                });
            }
        }
        if let Some(np) = nodes.get_mut(&hop.to_node) {
            if np.id_source.is_none() {
                np.id_source = Some(IdSource {
                    edge_alias: alias.clone(),
                    column: end_col.to_string(),
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

    // For traversal queries, nodes with requested columns need a hydration
    // JOIN so their properties appear in the main result. Without this, the
    // formatter only sees the node's ID from the edge and properties are empty.
    let needs_columns = input.query_type != QueryType::Aggregation
        && matches!(&node_plan.columns, Some(ColumnSelection::List(cols)) if !cols.is_empty());

    if is_group_by || is_agg_property_target || is_order_by_target || needs_columns {
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

// ─────────────────────────────────────────────────────────────────────────────
// Emit: single node (no edges)
// ─────────────────────────────────────────────────────────────────────────────

fn emit_single_node(skeleton: &Skeleton, input: &mut Input) -> Result<SkeletonOutput> {
    let (_, node_plan) = skeleton
        .nodes
        .iter()
        .next()
        .ok_or_else(|| QueryError::Lowering("no nodes in skeleton".into()))?;

    let table = node_plan
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node_plan.alias)))?;
    let alias = &node_plan.alias;

    // Dedup subquery: ReplacingMergeTree may have unmerged duplicates.
    // SELECT * because enforce/check may add columns we can't predict
    // (cursor tie-breakers, _gkg_* identity columns).
    let dedup = Query {
        select: vec![SelectExpr::star()],
        from: TableRef::scan(table, alias),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    let from = TableRef::Subquery {
        query: Box::new(dedup),
        alias: alias.to_string(),
    };

    let mut where_parts = Vec::new();

    where_parts.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    for (prop, filter) in &node_plan.filters {
        where_parts.push(filter_to_expr(alias, prop, filter));
    }
    if !node_plan.node_ids.is_empty() {
        where_parts.push(node_ids_predicate(alias, &node_plan.node_ids));
    }
    if let Some(ref range) = node_plan.id_range {
        where_parts.push(id_range_predicate(alias, range));
    }

    // Node columns go into skeleton SELECT only for non-aggregation queries.
    // Aggregation builds its own SELECT via build_aggregation(); adding node
    // columns here would violate GROUP BY constraints.
    // The graph formatter expects columns aliased as `{alias}_{col}`.
    let select = if input.query_type != QueryType::Aggregation {
        let mut s = Vec::new();
        for col in requested_columns(&node_plan.columns) {
            s.push(SelectExpr::new(
                Expr::col(alias, &col),
                format!("{alias}_{col}"),
            ));
        }
        s
    } else {
        vec![]
    };

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

fn emit_flat_chain(skeleton: &Skeleton, input: &mut Input) -> Result<SkeletonOutput> {
    // Single-hop with FK: skip the edge table entirely, join nodes directly.
    if skeleton.hops.len() == 1 {
        if let Some(ref fk) = skeleton.hops[0].fk {
            return emit_fk_direct(skeleton, fk, input);
        }
    }

    let mut where_parts = Vec::new();
    let mut edge_aliases = Vec::new();
    let mut ctes = Vec::new();

    // Build the edge chain FROM tree.
    let mut from: Option<TableRef> = None;

    for (i, hop) in skeleton.hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();

        if let Some(prev_from) = from.take() {
            let prev_alias = &edge_aliases[i - 1];
            let prev_end = skeleton.hops[i - 1].direction.edge_columns().1;
            let right = TableRef::scan(&hop.edge_table, &alias);
            let join_on = Expr::eq(
                Expr::col(prev_alias, prev_end),
                Expr::col(&alias, start_col),
            );
            from = Some(TableRef::Join {
                join_type: JoinType::Inner,
                left: Box::new(prev_from),
                right: Box::new(right),
                on: join_on,
            });
        } else {
            from = Some(TableRef::scan(&hop.edge_table, &alias));
        }

        // Edge predicates.
        push_edge_predicates(
            &mut where_parts,
            &alias,
            hop,
            &skeleton.nodes,
            start_col,
            end_col,
        );

        // Denorm tags.
        emit_denorm_tags(
            &mut where_parts,
            &alias,
            hop,
            &skeleton.nodes,
            input,
            start_col,
            end_col,
        );

        // Pinned node_ids on edges.
        emit_node_ids_on_edge(
            &mut where_parts,
            &alias,
            hop,
            &skeleton.nodes,
            start_col,
            end_col,
        );

        // Register node-edge mappings for enforce.
        input
            .compiler
            .node_edge_col
            .entry(hop.from_node.clone())
            .or_insert_with(|| (alias.clone(), start_col.to_string()));
        input
            .compiler
            .node_edge_col
            .entry(hop.to_node.clone())
            .or_insert_with(|| (alias.clone(), end_col.to_string()));

        edge_aliases.push(alias);
    }

    let mut from = from.ok_or_else(|| QueryError::Lowering("no hops in skeleton".into()))?;

    // Hydrate nodes.
    let mut selects = Vec::new();
    let mut hydrated: HashSet<String> = HashSet::new();

    for (i, hop) in skeleton.hops.iter().enumerate() {
        let edge_alias = &edge_aliases[i];
        let (start_col, end_col) = hop.direction.edge_columns();

        for (node_alias, edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if hydrated.contains(node_alias) {
                continue;
            }
            if let Some(np) = skeleton.nodes.get(node_alias) {
                match np.hydration {
                    HydrationStrategy::Join => {
                        let (new_from, ns, nw) =
                            emit_node_join(from, np, edge_alias, edge_col, input)?;
                        from = new_from;
                        selects.extend(ns);
                        where_parts.extend(nw);
                    }
                    HydrationStrategy::FilterOnly => {
                        where_parts
                            .extend(emit_filter_subquery(np, edge_alias, edge_col, &mut ctes)?);
                    }
                    HydrationStrategy::Skip => {}
                }
            }
            hydrated.insert(node_alias.clone());
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
// Emit: FK direct (no edge table)
// ─────────────────────────────────────────────────────────────────────────────

/// Single-hop with FK: join the two node tables directly using the FK column.
/// No edge table scan. The FK node's table drives, the target node joins via
/// FK column = target.id.
fn emit_fk_direct(skeleton: &Skeleton, fk: &HopFk, input: &mut Input) -> Result<SkeletonOutput> {
    let hop = &skeleton.hops[0];

    let fk_np = skeleton.nodes.get(&fk.fk_node).ok_or_else(|| {
        QueryError::Lowering(format!("FK node '{}' not found in skeleton", fk.fk_node))
    })?;
    let target_np = skeleton.nodes.get(&fk.target_node).ok_or_else(|| {
        QueryError::Lowering(format!(
            "target node '{}' not found in skeleton",
            fk.target_node
        ))
    })?;

    let fk_table = fk_np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("FK node '{}' has no table", fk.fk_node)))?;
    let fk_alias = &fk_np.alias;

    // FK direct: the node table IS the driving table. SELECT * because
    // downstream passes (enforce, check) add columns we can't predict
    // (cursor tie-breakers like created_at, updated_at).
    let fk_needed = requested_columns(&fk_np.columns);

    let fk_dedup = Query {
        select: vec![SelectExpr::star()],
        from: TableRef::scan(fk_table, fk_alias),
        order_by: vec![OrderExpr {
            expr: Expr::col(fk_alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(fk_alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    let from = TableRef::Subquery {
        query: Box::new(fk_dedup),
        alias: fk_alias.to_string(),
    };

    let mut where_parts = Vec::new();

    // FK node filters.
    for (prop, filter) in &fk_np.filters {
        where_parts.push(filter_to_expr(fk_alias, prop, filter));
    }
    where_parts.push(Expr::eq(
        Expr::col(fk_alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    // Target node constraint via FK column.
    if !target_np.node_ids.is_empty() {
        where_parts.push(ids_on_edge(fk_alias, &fk.fk_column, &target_np.node_ids));
    }

    // Register node-edge mappings for enforce.
    // FK node: id comes from its own table.
    input.compiler.node_edge_col.insert(
        fk.fk_node.clone(),
        (fk_alias.to_string(), DEFAULT_PRIMARY_KEY.to_string()),
    );
    // Target node: id comes from the FK column on the FK node.
    input.compiler.node_edge_col.insert(
        fk.target_node.clone(),
        (fk_alias.to_string(), fk.fk_column.clone()),
    );

    // Outer SELECT: FK node's columns — only for traversal or if this node
    // is the group-by target. For aggregation where this node is just the
    // driving scan, its columns don't belong in SELECT (GROUP BY violation).
    let is_fk_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(fk_alias.as_str()));
    let mut selects = Vec::new();
    if is_fk_group_by || input.query_type != QueryType::Aggregation {
        for col in &fk_needed {
            selects.push(SelectExpr::new(
                Expr::col(fk_alias, col),
                format!("{fk_alias}_{col}"),
            ));
        }
    }

    // If the target node needs hydration (group-by, columns, etc.), join it.
    let mut ctes = Vec::new();
    if target_np.hydration == HydrationStrategy::Join {
        let (new_from, ns, nw) = emit_node_join(from, target_np, fk_alias, &fk.fk_column, input)?;
        selects.extend(ns);
        where_parts.extend(nw);
        return Ok(SkeletonOutput {
            from: new_from,
            edge_aliases: vec![],
            where_parts,
            select: selects,
            ctes,
        });
    }
    if target_np.hydration == HydrationStrategy::FilterOnly {
        where_parts.extend(emit_filter_subquery(
            target_np,
            fk_alias,
            &fk.fk_column,
            &mut ctes,
        )?);
    }

    Ok(SkeletonOutput {
        from,
        edge_aliases: vec![],
        where_parts,
        select: selects,
        ctes,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit: FK star (all hops FK to same center node, zero edges)
// ─────────────────────────────────────────────────────────────────────────────

fn emit_fk_star(
    skeleton: &Skeleton,
    center_alias: &str,
    input: &mut Input,
) -> Result<SkeletonOutput> {
    let center_np = skeleton.nodes.get(center_alias).ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' not found"))
    })?;
    let center_table = center_np.table.as_deref().ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' has no table"))
    })?;

    // Center node: dedup subquery with explicit columns.
    // SELECT * doesn't reliably expose all columns through LIMIT 1 BY
    // subqueries in ClickHouse 26.2.
    let mut center_cols = vec![
        SelectExpr::new(
            Expr::col(center_alias, DEFAULT_PRIMARY_KEY),
            DEFAULT_PRIMARY_KEY,
        ),
        SelectExpr::new(Expr::col(center_alias, VERSION_COLUMN), VERSION_COLUMN),
        SelectExpr::new(Expr::col(center_alias, DELETED_COLUMN), DELETED_COLUMN),
    ];
    if center_np.has_traversal_path {
        center_cols.push(SelectExpr::new(
            Expr::col(center_alias, TRAVERSAL_PATH_COLUMN),
            TRAVERSAL_PATH_COLUMN,
        ));
    }
    // FK columns for each hop.
    for hop in &skeleton.hops {
        if let Some(ref fk) = hop.fk {
            if !center_cols
                .iter()
                .any(|s| s.alias.as_deref() == Some(fk.fk_column.as_str()))
            {
                center_cols.push(SelectExpr::new(
                    Expr::col(center_alias, &fk.fk_column),
                    fk.fk_column.as_str(),
                ));
            }
        }
    }
    // Requested columns.
    for col in requested_columns(&center_np.columns) {
        if !center_cols
            .iter()
            .any(|s| s.alias.as_deref() == Some(col.as_str()))
        {
            center_cols.push(SelectExpr::new(Expr::col(center_alias, &col), col.as_str()));
        }
    }
    // Filter columns.
    for (prop, _) in &center_np.filters {
        if !center_cols
            .iter()
            .any(|s| s.alias.as_deref() == Some(prop.as_str()))
        {
            center_cols.push(SelectExpr::new(
                Expr::col(center_alias, prop),
                prop.as_str(),
            ));
        }
    }
    // Agg property columns.
    for agg in &input.aggregations {
        if agg.target.as_deref() == Some(center_alias) {
            if let Some(ref prop) = agg.property {
                if !center_cols
                    .iter()
                    .any(|s| s.alias.as_deref() == Some(prop.as_str()))
                {
                    center_cols.push(SelectExpr::new(
                        Expr::col(center_alias, prop),
                        prop.as_str(),
                    ));
                }
            }
        }
    }
    // ORDER BY column.
    if let Some(ref ob) = input.order_by {
        if ob.node == center_alias
            && !center_cols
                .iter()
                .any(|s| s.alias.as_deref() == Some(ob.property.as_str()))
        {
            center_cols.push(SelectExpr::new(
                Expr::col(center_alias, &ob.property),
                ob.property.as_str(),
            ));
        }
    }
    // Redaction column.
    if center_np.redaction_id_column != DEFAULT_PRIMARY_KEY
        && !center_cols
            .iter()
            .any(|s| s.alias.as_deref() == Some(center_np.redaction_id_column.as_str()))
    {
        center_cols.push(SelectExpr::new(
            Expr::col(center_alias, &center_np.redaction_id_column),
            center_np.redaction_id_column.as_str(),
        ));
    }

    let center_dedup = Query {
        select: center_cols,
        from: TableRef::scan(center_table, center_alias),
        order_by: vec![OrderExpr {
            expr: Expr::col(center_alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(center_alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    let mut from = TableRef::Subquery {
        query: Box::new(center_dedup),
        alias: center_alias.to_string(),
    };

    let mut where_parts = Vec::new();
    let mut selects = Vec::new();
    let mut ctes = Vec::new();

    // Center node filters.
    for (prop, filter) in &center_np.filters {
        where_parts.push(filter_to_expr(center_alias, prop, filter));
    }
    where_parts.push(Expr::eq(
        Expr::col(center_alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    // Center node: register ID mapping.
    input.compiler.node_edge_col.insert(
        center_alias.to_string(),
        (center_alias.to_string(), DEFAULT_PRIMARY_KEY.to_string()),
    );

    // Center node SELECT columns (traversal or group-by).
    let is_center_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(center_alias));
    if is_center_group_by || input.query_type != QueryType::Aggregation {
        for col in requested_columns(&center_np.columns) {
            selects.push(SelectExpr::new(
                Expr::col(center_alias, &col),
                format!("{center_alias}_{col}"),
            ));
        }
    }

    // Each hop: target node is connected via FK column on center.
    for hop in &skeleton.hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FkStar hop missing FK metadata".into()))?;

        let target_alias = &fk.target_node;
        let target_np = skeleton.nodes.get(target_alias).ok_or_else(|| {
            QueryError::Lowering(format!("FK star target '{target_alias}' not found"))
        })?;

        // Register target node ID mapping: comes from center's FK column.
        input.compiler.node_edge_col.insert(
            target_alias.clone(),
            (center_alias.to_string(), fk.fk_column.clone()),
        );

        // Pinned target: push WHERE center.fk_col = X.
        if !target_np.node_ids.is_empty() {
            where_parts.push(ids_on_edge(
                center_alias,
                &fk.fk_column,
                &target_np.node_ids,
            ));
        }

        // Target hydration.
        match target_np.hydration {
            HydrationStrategy::Join => {
                let (new_from, ns, nw) =
                    emit_node_join(from, target_np, center_alias, &fk.fk_column, input)?;
                from = new_from;
                selects.extend(ns);
                where_parts.extend(nw);
            }
            HydrationStrategy::FilterOnly => {
                where_parts.extend(emit_filter_subquery(
                    target_np,
                    center_alias,
                    &fk.fk_column,
                    &mut ctes,
                )?);
            }
            HydrationStrategy::Skip => {}
        }
    }

    Ok(SkeletonOutput {
        from,
        edge_aliases: vec![],
        where_parts,
        select: selects,
        ctes,
    })
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
    // Relationship kind.
    if let Some(f) = rel_kind_filter(alias, &hop.rel_types) {
        where_parts.push(f);
    }

    // Entity kind on source side.
    if let Some(np) = nodes.get(&hop.from_node) {
        if let Some(ref entity) = np.entity {
            let kind_col = if start_col == SOURCE_ID_COLUMN {
                SOURCE_KIND_COLUMN
            } else {
                TARGET_KIND_COLUMN
            };
            where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
        }
    }

    // Entity kind on target side.
    if let Some(np) = nodes.get(&hop.to_node) {
        if let Some(ref entity) = np.entity {
            let kind_col = if end_col == TARGET_ID_COLUMN {
                TARGET_KIND_COLUMN
            } else {
                SOURCE_KIND_COLUMN
            };
            where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
        }
    }

    // _deleted filter.
    where_parts.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));
}

fn emit_denorm_tags(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    input: &Input,
    start_col: &str,
    end_col: &str,
) {
    if input.compiler.denormalized_columns.is_empty() {
        return;
    }
    if let Some(np) = nodes.get(&hop.from_node) {
        let dir = if start_col == SOURCE_ID_COLUMN {
            "source"
        } else {
            "target"
        };
        where_parts.extend(denorm_tag_exprs_from_plan(
            np,
            dir,
            alias,
            &input.compiler.denormalized_columns,
        ));
    }
    if let Some(np) = nodes.get(&hop.to_node) {
        let dir = if end_col == TARGET_ID_COLUMN {
            "target"
        } else {
            "source"
        };
        where_parts.extend(denorm_tag_exprs_from_plan(
            np,
            dir,
            alias,
            &input.compiler.denormalized_columns,
        ));
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
    if let Some(np) = nodes.get(&hop.from_node) {
        if !np.node_ids.is_empty() {
            where_parts.push(ids_on_edge(alias, start_col, &np.node_ids));
        }
    }
    if let Some(np) = nodes.get(&hop.to_node) {
        if !np.node_ids.is_empty() {
            where_parts.push(ids_on_edge(alias, end_col, &np.node_ids));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers: node hydration
// ─────────────────────────────────────────────────────────────────────────────

fn emit_node_join(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    input: &Input,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;

    // Determine which columns we need first — this drives both the
    // dedup subquery SELECT and the outer SELECT.
    let is_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(alias.as_str()));

    let needed_cols: Vec<String> = if is_group_by || input.query_type != QueryType::Aggregation {
        requested_columns(&np.columns)
    } else {
        let mut cols = Vec::new();
        if let Some(ref ob) = input.order_by {
            if ob.node == *alias && !cols.contains(&ob.property) {
                cols.push(ob.property.clone());
            }
        }
        cols
    };

    // Build dedup subquery with only the columns we need.
    let mut dedup_cols = vec![
        SelectExpr::new(Expr::col(alias, DEFAULT_PRIMARY_KEY), DEFAULT_PRIMARY_KEY),
        SelectExpr::new(Expr::col(alias, VERSION_COLUMN), VERSION_COLUMN),
    ];
    if np.has_traversal_path {
        dedup_cols.push(SelectExpr::new(
            Expr::col(alias, TRAVERSAL_PATH_COLUMN),
            TRAVERSAL_PATH_COLUMN,
        ));
    }
    for col in &needed_cols {
        if col != DEFAULT_PRIMARY_KEY && col != VERSION_COLUMN && col != TRAVERSAL_PATH_COLUMN {
            dedup_cols.push(SelectExpr::new(Expr::col(alias, col), col.as_str()));
        }
    }
    for (prop, _) in &np.filters {
        if !dedup_cols
            .iter()
            .any(|s| s.alias.as_deref() == Some(prop.as_str()))
        {
            dedup_cols.push(SelectExpr::new(Expr::col(alias, prop), prop.as_str()));
        }
    }
    // Aggregate property columns (sum, avg, min, max targets).
    for agg in &input.aggregations {
        if agg.target.as_deref() == Some(alias) {
            if let Some(ref prop) = agg.property {
                if !dedup_cols
                    .iter()
                    .any(|s| s.alias.as_deref() == Some(prop.as_str()))
                {
                    dedup_cols.push(SelectExpr::new(Expr::col(alias, prop), prop.as_str()));
                }
            }
        }
    }
    // ORDER BY column.
    if let Some(ref ob) = input.order_by {
        if ob.node == *alias
            && !dedup_cols
                .iter()
                .any(|s| s.alias.as_deref() == Some(ob.property.as_str()))
        {
            dedup_cols.push(SelectExpr::new(
                Expr::col(alias, &ob.property),
                ob.property.as_str(),
            ));
        }
    }
    // Enforce adds redaction_id_column — include it if it's not "id".
    if np.redaction_id_column != DEFAULT_PRIMARY_KEY
        && !dedup_cols
            .iter()
            .any(|s| s.alias.as_deref() == Some(np.redaction_id_column.as_str()))
    {
        dedup_cols.push(SelectExpr::new(
            Expr::col(alias, &np.redaction_id_column),
            np.redaction_id_column.as_str(),
        ));
    }
    // _deleted needed for WHERE filter.
    if !dedup_cols
        .iter()
        .any(|s| s.alias.as_deref() == Some(DELETED_COLUMN))
    {
        dedup_cols.push(SelectExpr::new(
            Expr::col(alias, DELETED_COLUMN),
            DELETED_COLUMN,
        ));
    }

    let dedup_query = Query {
        select: dedup_cols,
        from: TableRef::scan(table, alias),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    let joined = TableRef::Join {
        join_type: JoinType::Inner,
        left: Box::new(from),
        right: Box::new(TableRef::Subquery {
            query: Box::new(dedup_query),
            alias: alias.to_string(),
        }),
        on: {
            let id_join = Expr::eq(
                Expr::col(alias, DEFAULT_PRIMARY_KEY),
                Expr::col(edge_alias, edge_col),
            );
            if np.has_traversal_path {
                Expr::and(
                    id_join,
                    Expr::eq(
                        Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                        Expr::col(edge_alias, TRAVERSAL_PATH_COLUMN),
                    ),
                )
            } else {
                id_join
            }
        },
    };

    let selects: Vec<SelectExpr> = needed_cols
        .iter()
        .map(|col| SelectExpr::new(Expr::col(alias, col), format!("{alias}_{col}")))
        .collect();

    let mut wheres = Vec::new();
    for (prop, filter) in &np.filters {
        wheres.push(filter_to_expr(alias, prop, filter));
    }
    wheres.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

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

    let mut inner_where = Vec::new();
    for (prop, filter) in &np.filters {
        inner_where.push(filter_to_expr(alias, prop, filter));
    }
    inner_where.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    let inner = Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(inner_where),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
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
// Denorm tag expressions from NodePlan
// ─────────────────────────────────────────────────────────────────────────────

fn denorm_tag_exprs_from_plan(
    np: &NodePlan,
    dir_prefix: &str,
    edge_alias: &str,
    denorm_map: &HashMap<(String, String, String), (String, String)>,
) -> Vec<Expr> {
    let entity = match &np.entity {
        Some(e) => e,
        None => return vec![],
    };
    let mut exprs = Vec::new();
    for (prop, filter) in &np.filters {
        let key = (entity.clone(), prop.clone(), dir_prefix.to_string());
        let Some((edge_column, tag_key)) = denorm_map.get(&key) else {
            continue;
        };
        match filter.op {
            None | Some(FilterOp::Eq) => {
                let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                exprs.push(Expr::func(
                    "has",
                    vec![
                        Expr::col(edge_alias, edge_column),
                        Expr::string(format!("{tag_key}:{val}")),
                    ],
                ));
            }
            Some(FilterOp::In) => {
                if let Some(values) = filter.value.as_ref().and_then(|v| v.as_array()) {
                    let tags: Vec<String> = values
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| format!("{tag_key}:{s}")))
                        .collect();
                    if tags.len() == 1 {
                        exprs.push(Expr::func(
                            "has",
                            vec![Expr::col(edge_alias, edge_column), Expr::string(&tags[0])],
                        ));
                    } else if !tags.is_empty() {
                        exprs.push(Expr::func(
                            "hasAny",
                            vec![
                                Expr::col(edge_alias, edge_column),
                                Expr::func("array", tags.iter().map(Expr::string).collect()),
                            ],
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    exprs
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge SELECT columns
// ─────────────────────────────────────────────────────────────────────────────

pub fn edge_select_columns(alias: &str) -> Vec<SelectExpr> {
    [
        (RELATIONSHIP_KIND_COLUMN, EDGE_TYPE_SUFFIX),
        (SOURCE_ID_COLUMN, EDGE_SRC_SUFFIX),
        (SOURCE_KIND_COLUMN, EDGE_SRC_TYPE_SUFFIX),
        (TARGET_ID_COLUMN, EDGE_DST_SUFFIX),
        (TARGET_KIND_COLUMN, EDGE_DST_TYPE_SUFFIX),
    ]
    .iter()
    .map(|(col, suffix)| SelectExpr::new(Expr::col(alias, *col), format!("{alias}_{suffix}")))
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

fn ids_on_edge(edge_alias: &str, edge_col: &str, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::col(edge_alias, edge_col), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            edge_alias,
            edge_col,
            ChType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false))
    }
}

pub fn node_ids_predicate(alias: &str, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::col(alias, DEFAULT_PRIMARY_KEY), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            alias,
            DEFAULT_PRIMARY_KEY,
            ChType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false))
    }
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
