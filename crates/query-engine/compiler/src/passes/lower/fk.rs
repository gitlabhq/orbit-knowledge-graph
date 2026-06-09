//! Emit FK-derived traversals as node joins on FK columns (no `gl_edge` scans).
//! `Star` scans one center with `FINAL` + candidate-CTE narrowing; `Chain` joins
//! a linear chain end-to-end with `LIMIT 1 BY` scans. Both synthesize the per-hop
//! edge columns the formatter expects.

use ontology::constants::*;
use std::collections::{HashMap, HashSet};

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;

use super::EmitOutput;
use super::helpers::{
    NarrowSource, emit_filter_subquery, emit_node_join_with_narrowing,
    fk_values_from_candidate_scan, latest_node_predicates, node_ids_from_candidate_scan,
    node_select_columns,
};
use crate::passes::plan::*;
use crate::passes::shared::id_list_predicate;

pub(super) fn emit_fk(plan: &Plan, shape: &FkShape) -> Result<EmitOutput> {
    match shape {
        FkShape::Star { center } => emit_star(plan, center),
        FkShape::Chain => emit_chain(plan),
    }
}

fn emit_star(plan: &Plan, center_alias: &str) -> Result<EmitOutput> {
    let center_np = plan.nodes.get(center_alias).ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' not found"))
    })?;
    let center_table = center_np.table.as_deref().ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' has no table"))
    })?;

    let mut center_where_parts = latest_node_predicates(center_alias, center_np);
    let mut where_parts = Vec::new();
    let mut selects = node_select_columns(center_alias, center_np);
    let mut ctes = Vec::new();
    let mut candidate_ctes = HashMap::new();
    let mut candidate_extra_predicates = fk_candidate_extra_predicates(plan)?;

    // Elevated access: FilterOnly CTE so SecurityPass injects the role-gated filter.
    if center_np.needs_elevated_filter {
        let center_sk = plan
            .table_sort_keys
            .get(center_table)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        center_where_parts.extend(emit_filter_subquery(
            center_np,
            center_alias,
            DEFAULT_PRIMARY_KEY,
            &mut ctes,
            center_sk,
        )?);
    }

    emit_join_target_candidate_ctes(
        plan,
        &mut ctes,
        &mut candidate_ctes,
        &candidate_extra_predicates,
    )?;

    for hop in &plan.hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FkStar hop missing FK metadata".into()))?;
        let fk_alias = if fk.fk_node == center_alias {
            center_alias.to_string()
        } else {
            fk.fk_node.clone()
        };
        if let Some(cte_name) = candidate_ctes.get(&fk.target_node) {
            candidate_extra_predicates
                .entry(fk_alias)
                .or_default()
                .push(Expr::InSubquery {
                    expr: Box::new(Expr::col(&fk.fk_node, &fk.fk_column)),
                    cte_name: cte_name.clone(),
                    column: DEFAULT_PRIMARY_KEY.to_string(),
                });
        }
    }

    let joins_latest_node = plan.hops.iter().any(|hop| {
        hop.fk
            .as_ref()
            .and_then(|fk| plan.nodes.get(&fk.target_node))
            .is_some_and(|np| np.fk_needs_join)
    });
    let center_has_extra_predicates = candidate_extra_predicates
        .get(center_alias)
        .is_some_and(|predicates| !predicates.is_empty());
    if joins_latest_node && center_has_extra_predicates {
        let cte_name = candidate_cte_name(center_alias);
        ctes.push(Cte::new(
            &cte_name,
            node_ids_from_candidate_scan(
                center_alias,
                center_table,
                center_np,
                candidate_extra_predicates
                    .get(center_alias)
                    .cloned()
                    .unwrap_or_default(),
            ),
        ));
        candidate_ctes.insert(center_alias.to_string(), cte_name.clone());
        center_where_parts.push(Expr::InSubquery {
            expr: Box::new(Expr::col(center_alias, DEFAULT_PRIMARY_KEY)),
            cte_name,
            column: DEFAULT_PRIMARY_KEY.to_string(),
        });
    }

    for hop in &plan.hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FkStar hop missing FK metadata".into()))?;
        if fk.fk_node != center_alias {
            continue;
        }
        let target_np = plan.nodes.get(&fk.target_node).ok_or_else(|| {
            QueryError::Lowering(format!("FK target '{}' not found", fk.target_node))
        })?;
        if !target_np.node_ids.is_empty() {
            center_where_parts.push(id_list_predicate(
                center_alias,
                &fk.fk_column,
                &target_np.node_ids,
            ));
        }
    }

    let mut from = TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan_final(center_table, center_alias),
            where_clause: Expr::conjoin(center_where_parts),
            ..Default::default()
        },
        center_alias,
    );

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

        if !target_np.node_ids.is_empty() && fk_alias != center_alias {
            where_parts.push(id_list_predicate(
                &fk_alias,
                &fk.fk_column,
                &target_np.node_ids,
            ));
        }

        if target_np.fk_needs_join {
            // The aggregation's GROUP BY plus the `target.id = center.fk_column`
            // join already narrow the target, so a `_narrow_*` re-scan is redundant.
            let narrowed_by_center_join =
                !matches!(plan.body, PlanBody::Traversal) && fk_alias == center_alias;
            // Narrow the target scan to the FK values the center references, else
            // it scans the full org (e.g. all Jobs) just to join a handful.
            let narrow = if let Some(cte_name) = candidate_ctes.get(&fk.target_node) {
                Some(NarrowSource::Cte(cte_name.clone()))
            } else if !narrowed_by_center_join
                && target_np.filters.is_empty()
                && target_np.node_ids.is_empty()
                && target_np.id_range.is_none()
                && center_np.has_selective_filters()
            {
                let narrow_name = format!("_narrow_{}", fk.target_node);
                ctes.push(Cte::new(
                    &narrow_name,
                    fk_values_from_candidate_scan(
                        center_alias,
                        center_table,
                        &fk.fk_column,
                        center_np,
                        candidate_extra_predicates
                            .get(center_alias)
                            .cloned()
                            .unwrap_or_default(),
                    ),
                ));
                Some(NarrowSource::Cte(narrow_name))
            } else {
                None
            };
            // No traversal_path equality on FK JOINs: entities at different depths
            // have different TP prefixes (WorkItem '1/100/' vs Project '1/100/1000/').
            let target_table = target_np.table.as_deref().ok_or_else(|| {
                QueryError::Lowering(format!("node '{}' has no table", target_np.alias))
            })?;
            let node_sort_key = plan.table_sort_keys.get(target_table).ok_or_else(|| {
                QueryError::Lowering(format!("no sort key for node table '{target_table}'"))
            })?;
            let (new_from, ns, nw) = emit_node_join_with_narrowing(
                from,
                target_np,
                &fk_alias,
                &fk.fk_column,
                false,
                narrow,
                node_sort_key,
            )?;
            from = new_from;
            selects.extend(ns);
            where_parts.extend(nw);
        } else if target_np.hydration == HydrationStrategy::FilterOnly
            || target_np.needs_elevated_filter
        {
            let target_table = target_np.table.as_deref().unwrap_or("");
            let target_sk = plan
                .table_sort_keys
                .get(target_table)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            where_parts.extend(emit_filter_subquery(
                target_np,
                &fk_alias,
                &fk.fk_column,
                &mut ctes,
                target_sk,
            )?);
        }
    }

    // Synthesize per-hop edge columns for the formatter; aggregations need none.
    let mut edge_aliases = Vec::new();
    if !matches!(plan.body, PlanBody::Traversal) {
        return Ok(EmitOutput {
            from,
            edge_aliases,
            where_parts,
            select: selects,
            ctes,
            edge_if_predicates: None,
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
        edge_if_predicates: None,
    })
}

fn candidate_cte_name(alias: &str) -> String {
    format!("_candidate_{alias}")
}

fn candidate_selective(np: &NodePlan, extra_predicates: &HashMap<String, Vec<Expr>>) -> bool {
    !np.filters.is_empty()
        || !np.node_ids.is_empty()
        || np.id_range.is_some()
        || extra_predicates
            .get(&np.alias)
            .is_some_and(|predicates| !predicates.is_empty())
}

fn fk_candidate_extra_predicates(plan: &Plan) -> Result<HashMap<String, Vec<Expr>>> {
    let mut predicates: HashMap<String, Vec<Expr>> = HashMap::new();
    for hop in &plan.hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FkStar hop missing FK metadata".into()))?;
        let target_np = plan.nodes.get(&fk.target_node).ok_or_else(|| {
            QueryError::Lowering(format!("FK target '{}' not found", fk.target_node))
        })?;
        if target_np.node_ids.is_empty() {
            continue;
        }
        let fk_alias = fk.fk_node.clone();
        predicates
            .entry(fk_alias)
            .or_default()
            .push(id_list_predicate(
                &fk.fk_node,
                &fk.fk_column,
                &target_np.node_ids,
            ));
    }
    Ok(predicates)
}

fn emit_join_target_candidate_ctes(
    plan: &Plan,
    ctes: &mut Vec<Cte>,
    candidate_ctes: &mut HashMap<String, String>,
    candidate_extra_predicates: &HashMap<String, Vec<Expr>>,
) -> Result<()> {
    let mut emitted = HashSet::new();
    for hop in &plan.hops {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FkStar hop missing FK metadata".into()))?;
        let target_np = plan.nodes.get(&fk.target_node).ok_or_else(|| {
            QueryError::Lowering(format!("FK target '{}' not found", fk.target_node))
        })?;
        if !target_np.fk_needs_join || !emitted.insert(fk.target_node.clone()) {
            continue;
        }
        if !candidate_selective(target_np, candidate_extra_predicates) {
            continue;
        }
        let table = target_np.table.as_deref().ok_or_else(|| {
            QueryError::Lowering(format!("FK target '{}' has no table", fk.target_node))
        })?;
        let cte_name = candidate_cte_name(&fk.target_node);
        ctes.push(Cte::new(
            &cte_name,
            node_ids_from_candidate_scan(
                &fk.target_node,
                table,
                target_np,
                candidate_extra_predicates
                    .get(&fk.target_node)
                    .cloned()
                    .unwrap_or_default(),
            ),
        ));
        candidate_ctes.insert(fk.target_node.clone(), cte_name);
    }
    Ok(())
}

/// Latest-row, `_deleted`-filtered `SELECT *` scan. `scope_prefix` is the tighter
/// project/group prefix that lets ClickHouse seek the node PK to a contiguous range.
fn node_scan(np: &NodePlan, plan: &Plan, scope_prefix: Option<&str>) -> Result<TableRef> {
    let alias = &np.alias;
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' has no table")))?;
    let sort_key = plan
        .table_sort_keys
        .get(table)
        .ok_or_else(|| QueryError::Lowering(format!("no sort key for node table '{table}'")))?;

    let mut order_by: Vec<OrderExpr> = sort_key
        .iter()
        .map(|col| OrderExpr::asc(Expr::col(alias, col)))
        .collect();
    order_by.push(OrderExpr::desc(Expr::col(alias, VERSION_COLUMN)));
    let limit_by_cols: Vec<Expr> = sort_key.iter().map(|col| Expr::col(alias, col)).collect();

    let mut where_parts = latest_node_predicates(alias, np);
    if np.has_traversal_path
        && let Some(prefix) = scope_prefix
    {
        where_parts.push(Expr::func(
            "startsWith",
            vec![
                Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                Expr::string(prefix),
            ],
        ));
    }

    Ok(TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan(table, alias),
            where_clause: Expr::conjoin(where_parts),
            order_by,
            limit_by: Some((1, limit_by_cols)),
            ..Default::default()
        },
        alias,
    ))
}

fn emit_chain(plan: &Plan) -> Result<EmitOutput> {
    let root_alias = &plan.hops[0].from_node;
    let root_np = plan
        .nodes
        .get(root_alias)
        .ok_or_else(|| QueryError::Lowering(format!("FK chain root '{root_alias}' not found")))?;

    let mut from = node_scan(root_np, plan, plan.hops[0].scope_prefix.as_deref())?;
    let mut selects = node_select_columns(root_alias, root_np);
    let mut edge_aliases = Vec::new();

    let mut reached: HashSet<&str> = HashSet::from([root_alias.as_str()]);
    for (i, hop) in plan.hops.iter().enumerate() {
        let fk = hop
            .fk
            .as_ref()
            .ok_or_else(|| QueryError::Lowering("FK chain hop missing FK metadata".into()))?;
        // Join whichever endpoint isn't reached yet, so the chain emits in any
        // orientation without a pre-sort.
        let new_alias = if reached.contains(hop.from_node.as_str()) {
            &hop.to_node
        } else {
            &hop.from_node
        };
        let new_np = plan.nodes.get(new_alias).ok_or_else(|| {
            QueryError::Lowering(format!("FK chain node '{new_alias}' not found"))
        })?;

        let on = if fk.fk_node == hop.to_node {
            Expr::eq(
                Expr::col(&hop.to_node, &fk.fk_column),
                Expr::col(&hop.from_node, DEFAULT_PRIMARY_KEY),
            )
        } else {
            Expr::eq(
                Expr::col(&hop.from_node, &fk.fk_column),
                Expr::col(&hop.to_node, DEFAULT_PRIMARY_KEY),
            )
        };
        from = TableRef::join(
            JoinType::Inner,
            from,
            node_scan(new_np, plan, hop.scope_prefix.as_deref())?,
            on,
        );
        selects.extend(node_select_columns(new_alias, new_np));
        reached.insert(hop.from_node.as_str());
        reached.insert(hop.to_node.as_str());

        // Aggregations group by node properties only; per-hop edge columns would
        // be unaggregated SELECT items. Emit them solely for traversal output.
        if !matches!(plan.body, PlanBody::Traversal) {
            continue;
        }

        // Emit edges in physical (source->target) orientation so a reversed hop
        // still reports the same orientation as the edge-scan path.
        let ea = format!("e{i}");
        let (src_node, dst_node) = match hop.direction {
            Direction::Incoming => (&hop.to_node, &hop.from_node),
            Direction::Outgoing | Direction::Both => (&hop.from_node, &hop.to_node),
        };
        let entity = |alias: &str| {
            plan.nodes
                .get(alias)
                .and_then(|n| n.entity.as_deref())
                .unwrap_or("")
        };
        let rel_type = hop.rel_types.first().map(String::as_str).unwrap_or("");
        selects.push(SelectExpr::new(
            Expr::string(rel_type),
            format!("{ea}_{EDGE_TYPE_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::col(src_node, DEFAULT_PRIMARY_KEY),
            format!("{ea}_{EDGE_SRC_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::string(entity(src_node)),
            format!("{ea}_{EDGE_SRC_TYPE_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::col(dst_node, DEFAULT_PRIMARY_KEY),
            format!("{ea}_{EDGE_DST_SUFFIX}"),
        ));
        selects.push(SelectExpr::new(
            Expr::string(entity(dst_node)),
            format!("{ea}_{EDGE_DST_TYPE_SUFFIX}"),
        ));
        edge_aliases.push(ea);
    }

    Ok(EmitOutput {
        from,
        edge_aliases,
        where_parts: Vec::new(),
        select: selects,
        ctes: Vec::new(),
        edge_if_predicates: None,
    })
}
