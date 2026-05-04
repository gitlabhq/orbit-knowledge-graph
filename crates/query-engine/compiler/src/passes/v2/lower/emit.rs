//! Edge chain plan emitters: read the IR, produce SQL AST fragments.
//!
//! `EdgeChainPlan::emit()` dispatches by strategy to one of:
//! - `emit_single_node` — no edges
//! - `emit_flat_chain` — flat edge chain
//! - `emit_fk_star` — FK star (all hops FK to same center)

use std::collections::{HashMap, HashSet};

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use super::plan::*;
use super::shared::{
    filter_to_expr, id_list_predicate, id_range_predicate, rel_kind_filter_values,
};

// ─────────────────────────────────────────────────────────────────────────────
// EdgeChainPlan::emit()
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeChainPlan {
    /// Emit SQL AST from the plan. Pure AST generation — reads only
    /// from plan fields, does not consult Input.
    pub fn emit(&self, _input: &mut Input) -> Result<EmitOutput> {
        match self.strategy {
            Strategy::SingleNode => emit_single_node(self),
            Strategy::FkStar { ref center } => emit_fk_star(self, center),
            Strategy::Flat | Strategy::Bidirectional { .. } => emit_flat_chain(self),
        }
    }
}

/// The output of emitting a plan — ready for query-type modules to wrap.
pub struct EmitOutput {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
}

impl EmitOutput {
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

/// Node columns for the outer SELECT, aliased as `{alias}_{col}` for the
/// graph formatter. Only for non-aggregation queries (aggregation builds
/// its own SELECT).
fn node_select_columns(alias: &str, np: &NodePlan) -> Vec<SelectExpr> {
    if !np.emit_select {
        return vec![];
    }
    super::shared::requested_columns(&np.columns)
        .into_iter()
        .map(|col| SelectExpr::new(Expr::col(alias, &col), format!("{alias}_{col}")))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit: single node (no edges)
// ─────────────────────────────────────────────────────────────────────────────

fn emit_single_node(plan: &EdgeChainPlan) -> Result<EmitOutput> {
    let (_, np) = plan
        .nodes
        .iter()
        .next()
        .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
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

    Ok(EmitOutput {
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

fn emit_flat_chain(plan: &EdgeChainPlan) -> Result<EmitOutput> {
    let mut where_parts = Vec::new();
    let mut edge_aliases = Vec::new();
    let mut ctes = Vec::new();
    let mut from: Option<TableRef> = None;

    for (i, hop) in plan.hops.iter().enumerate() {
        let alias = format!("e{i}");
        let (start_col, end_col) = hop.direction.edge_columns();
        let is_multi_hop = hop.max_hops > 1;

        // Build edge source: UNION ALL for multi-hop, plain scan for single.
        let edge_source = if is_multi_hop {
            let (union, union_wheres) = build_multi_hop_union(hop, &alias, &plan.nodes);
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
                &plan.nodes,
                start_col,
                end_col,
            );
        }

        // Relationship-level filters (edge property predicates from the query).
        for (prop, filter) in &hop.filters {
            where_parts.push(filter_to_expr(&alias, prop, filter));
        }

        // Apply pre-computed denorm tags from the plan nodes.
        emit_precomputed_denorm_tags(&mut where_parts, &plan.nodes, hop, start_col, end_col);
        emit_node_ids_on_edge(
            &mut where_parts,
            &alias,
            hop,
            &plan.nodes,
            start_col,
            end_col,
        );

        edge_aliases.push(alias);
    }

    let mut from = from.ok_or_else(|| QueryError::Lowering("no hops in plan".into()))?;
    let mut selects = Vec::new();
    let mut hydrated: HashSet<String> = HashSet::new();

    for (i, hop) in plan.hops.iter().enumerate() {
        let edge_alias = &edge_aliases[i];
        let (start_col, end_col) = hop.direction.edge_columns();

        for (node_alias, edge_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
            if !hydrated.insert(node_alias.clone()) {
                continue;
            }
            let Some(np) = plan.nodes.get(node_alias) else {
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
                                    &plan.nodes,
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

    Ok(EmitOutput {
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

fn emit_fk_star(plan: &EdgeChainPlan, center_alias: &str) -> Result<EmitOutput> {
    let center_np = plan.nodes.get(center_alias).ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' not found"))
    })?;
    let center_table = center_np.table.as_deref().ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' has no table"))
    })?;

    // Build center dedup columns from pre-computed list + FK columns.
    let mut center_cols = collect_dedup_columns(center_alias, center_np);
    // Add FK columns for each hop (not covered by dedup_columns).
    for hop in &plan.hops {
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
    for hop in &plan.hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FkStar hop missing FK metadata".into()))?;
        let target_np = plan.nodes.get(&fk.target_node).ok_or_else(|| {
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
    if !plan.synthesize_fk_edge_metadata {
        return Ok(EmitOutput {
            from,
            edge_aliases,
            where_parts,
            select: selects,
            ctes,
        });
    }
    for (i, hop) in plan.hops.iter().enumerate() {
        let ea = format!("e{i}");
        let fk = hop.fk.as_ref().unwrap();
        let from_np = plan.nodes.get(&hop.from_node);
        let to_np = plan.nodes.get(&hop.to_node);
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

    Ok(EmitOutput {
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

/// Emit pre-computed denorm tags from the plan's NodePlans.
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
