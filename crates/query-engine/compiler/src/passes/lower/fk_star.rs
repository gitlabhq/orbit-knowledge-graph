//! Emit: FK star (all hops FK to same center node, zero edges).
//! Also handles single-hop FK (FkDirect is just FkStar with 1 hop).

use ontology::constants::*;
use std::collections::{HashMap, HashSet};

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};

use super::EmitOutput;
use super::helpers::{
    NarrowSource, emit_filter_subquery, emit_node_join_with_narrowing,
    fk_values_from_candidate_scan, latest_node_predicates, node_ids_from_candidate_scan,
    node_select_columns,
};
use crate::passes::plan::*;
use crate::passes::shared::id_list_predicate;

pub(super) fn emit_fk_star(plan: &Plan, center_alias: &str) -> Result<EmitOutput> {
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

    // Elevated-access center node: emit a FilterOnly CTE so SecurityPass
    // can inject the stricter role-gated startsWith filter.
    if center_np.needs_elevated_filter {
        center_where_parts.extend(emit_filter_subquery(
            center_np,
            center_alias,
            DEFAULT_PRIMARY_KEY,
            &mut ctes,
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
        if !target_np.node_ids.is_empty() && fk_alias != center_alias {
            where_parts.push(id_list_predicate(
                &fk_alias,
                &fk.fk_column,
                &target_np.node_ids,
            ));
        }

        // Target hydration — use pre-resolved fk_needs_join.
        if target_np.fk_needs_join {
            // Narrow the target's latest-row scan to only IDs the center
            // actually references via its FK column. Without this, the
            // target scans the full org (e.g., all Jobs) just to join
            // on the handful of FK values from the center.
            let narrow = if let Some(cte_name) = candidate_ctes.get(&fk.target_node) {
                Some(NarrowSource::Cte(cte_name.clone()))
            } else if target_np.filters.is_empty()
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
            // Don't add traversal_path equality to FK JOINs: entities
            // at different depths have different TP prefixes (e.g.
            // WorkItem at '1/100/' vs Project at '1/100/1000/').
            let node_sort_key = target_np
                .table
                .as_deref()
                .and_then(|t| plan.table_sort_keys.get(t))
                .map(|v| v.as_slice());
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
    // Aggregation queries don't need edge columns.
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
