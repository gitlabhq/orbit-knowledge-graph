//! Emit FK-derived traversals as node joins on FK columns (no `gl_edge` scans).
//! `Star` scans one center with `FINAL` + candidate-CTE narrowing; `Chain` joins
//! a linear chain end-to-end with `FINAL` scans. Both synthesize the per-hop edge
//! columns the formatter expects.

use ontology::constants::*;
use std::collections::HashMap;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::Direction;

use super::helpers::{
    NarrowSource, emit_filter_subquery, emit_node_join_with_narrowing, latest_node_predicates,
    node_select_columns,
};
use super::{EmitOutput, NodeBinding};
use crate::passes::plan::fk::{StarCandidates, TargetNarrowing};
use crate::passes::plan::*;
use crate::passes::shared::id_list_predicate;

pub(super) fn emit_fk(plan: &Plan, shape: &FkShape) -> Result<EmitOutput> {
    match shape {
        FkShape::Star { center, candidates } => emit_star(plan, center, candidates),
        FkShape::Chain(root) => super::physical::emit(root),
    }
}

fn emit_star(plan: &Plan, center_alias: &str, candidates: &StarCandidates) -> Result<EmitOutput> {
    let center_np = plan.nodes.get(center_alias).ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' not found"))
    })?;
    let center_table = center_np.table.as_deref().ok_or_else(|| {
        QueryError::Lowering(format!("FK star center '{center_alias}' has no table"))
    })?;

    let mut center_where_parts = latest_node_predicates(center_alias, center_np);
    let mut where_parts = Vec::new();
    let mut selects = node_select_columns(center_alias, center_np);
    let mut ctes: Vec<_> = candidates
        .definitions
        .iter()
        .map(|(name, keys)| Cte::new(name, super::physical::query(keys)))
        .collect();
    let mut nodes = HashMap::from([(center_alias.to_string(), NodeBinding::table(center_alias))]);
    if let Some(predicate) = &candidates.center_filter {
        center_where_parts.push(predicate.clone());
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
            let narrow = candidates.targets.get(&fk.target_node).map(|target| {
                let name = match target {
                    TargetNarrowing::Reference(name) => name,
                    TargetNarrowing::Define { name, keys } => {
                        ctes.push(Cte::new(name, super::physical::query(keys)));
                        name
                    }
                };
                NarrowSource::Cte(name.clone())
            });
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
