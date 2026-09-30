//! Emit FK-derived traversals as node joins on FK columns (no `gl_edge` scans).
//! `Star` scans one center with `FINAL` + candidate-CTE narrowing; `Chain` joins
//! a linear chain end-to-end with `FINAL` scans. Both synthesize the per-hop edge
//! columns the formatter expects.

use ontology::constants::*;
use std::collections::{HashMap, HashSet};

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;

use super::helpers::{
    NarrowSource, emit_filter_subquery, emit_node_join_with_narrowing, latest_node_predicates,
    node_select_columns,
};
use super::{EmitOutput, NodeBinding};
use crate::passes::plan::physical::PhysicalPlan;
use crate::passes::plan::*;
use crate::passes::shared::id_list_predicate;

pub(super) fn emit_fk(plan: &Plan, shape: &FkShape) -> Result<EmitOutput> {
    match shape {
        FkShape::Star { center } => emit_star(plan, center),
        FkShape::Chain(root) => super::physical::emit(root),
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
    let mut nodes = HashMap::from([(center_alias.to_string(), NodeBinding::table(center_alias))]);
    let mut candidate_ctes = HashMap::new();
    let mut candidate_extra_predicates = fk_candidate_extra_predicates(plan)?;

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
            super::physical::query(&PhysicalPlan::candidate_keys(
                center_np,
                DEFAULT_PRIMARY_KEY,
                candidate_extra_predicates
                    .get(center_alias)
                    .cloned()
                    .unwrap_or_default(),
            )?),
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
        if !target_np.node_ids.is_empty() && fk.referenced_column == DEFAULT_PRIMARY_KEY {
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

        if !target_np.node_ids.is_empty()
            && fk_alias != center_alias
            && fk.referenced_column == DEFAULT_PRIMARY_KEY
        {
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
                !matches!(plan.body, PlanBody::Traversal { .. }) && fk_alias == center_alias;
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
                    super::physical::query(&PhysicalPlan::candidate_keys(
                        center_np,
                        &fk.fk_column,
                        candidate_extra_predicates
                            .get(center_alias)
                            .cloned()
                            .unwrap_or_default(),
                    )?),
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
                &fk.referenced_column,
                narrow,
                node_sort_key,
            )?;
            from = new_from;
            selects.extend(ns);
            where_parts.extend(nw);
        } else if target_np.hydration == HydrationStrategy::FilterOnly {
            where_parts.extend(emit_filter_subquery(
                target_np,
                &fk_alias,
                &fk.fk_column,
                &fk.referenced_column,
                &mut ctes,
            )?);
        }
        let (identity_alias, identity_column) = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
            (fk_alias.as_str(), fk.fk_column.as_str())
        } else {
            (fk.target_node.as_str(), DEFAULT_PRIMARY_KEY)
        };
        nodes.insert(
            fk.target_node.clone(),
            NodeBinding::source(
                identity_alias,
                identity_column,
                target_np.fk_needs_join.then(|| fk.target_node.clone()),
            ),
        );
    }

    // Synthesize per-hop edge columns for the formatter; aggregations need none.
    let mut edge_aliases = Vec::new();
    if !matches!(plan.body, PlanBody::Traversal { .. }) {
        return Ok(EmitOutput {
            nodes,
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
        let (source, target) = match hop.direction {
            Direction::Incoming => (&hop.to_node, &hop.from_node),
            Direction::Outgoing | Direction::Both => (&hop.from_node, &hop.to_node),
        };
        let source_entity = plan
            .nodes
            .get(source)
            .and_then(|node| node.entity.as_deref())
            .unwrap_or("");
        let target_entity = plan
            .nodes
            .get(target)
            .and_then(|node| node.entity.as_deref())
            .unwrap_or("");
        let rel_type = hop.rel_types.first().map(|s| s.as_str()).unwrap_or("");

        let target_id = if fk.referenced_column == DEFAULT_PRIMARY_KEY {
            Expr::col(center_alias, &fk.fk_column)
        } else {
            Expr::col(&fk.target_node, DEFAULT_PRIMARY_KEY)
        };
        let (src_id_expr, src_kind, tgt_id_expr, tgt_kind) = if fk.fk_node == *source {
            (
                Expr::col(center_alias, DEFAULT_PRIMARY_KEY),
                source_entity,
                target_id,
                target_entity,
            )
        } else {
            (
                target_id,
                source_entity,
                Expr::col(center_alias, DEFAULT_PRIMARY_KEY),
                target_entity,
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
        nodes,
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
        if target_np.node_ids.is_empty() || fk.referenced_column != DEFAULT_PRIMARY_KEY {
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
        let cte_name = candidate_cte_name(&fk.target_node);
        ctes.push(Cte::new(
            &cte_name,
            super::physical::query(&PhysicalPlan::candidate_keys(
                target_np,
                &fk.referenced_column,
                candidate_extra_predicates
                    .get(&fk.target_node)
                    .cloned()
                    .unwrap_or_default(),
            )?),
        ));
        candidate_ctes.insert(fk.target_node.clone(), cte_name);
    }
    Ok(())
}
