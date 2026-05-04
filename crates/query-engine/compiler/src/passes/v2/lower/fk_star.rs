//! Emit: FK star (all hops FK to same center node, zero edges).
//! Also handles single-hop FK (FkDirect is just FkStar with 1 hop).

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};

use super::super::plan::*;
use super::super::shared::{deleted_false, id_list_predicate};
use super::EmitOutput;
use super::helpers::{
    build_dedup_subquery, collect_dedup_columns, emit_filter_subquery, emit_node_join,
    node_select_columns,
};

pub(super) fn emit_fk_star(plan: &EdgeChainPlan, center_alias: &str) -> Result<EmitOutput> {
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

    let center_dedup = build_dedup_subquery(center_alias, center_table, center_cols, center_np);

    let mut from = TableRef::Subquery {
        query: Box::new(center_dedup),
        alias: center_alias.to_string(),
    };

    // Only _deleted=false in the outer WHERE — user filters are inside the dedup.
    let mut where_parts = vec![deleted_false(center_alias)];
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
