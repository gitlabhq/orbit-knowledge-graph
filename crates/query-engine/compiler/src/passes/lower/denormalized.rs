//! Emit a single-hop traversal or aggregation as one `FINAL` scan of the
//! hop's denormalized join table. Node columns are still emitted against the
//! node aliases here; the `rebind` pass maps them onto the scan's `src_`/`tgt_`
//! columns after every alias-producing pass has run.

use ontology::constants::*;
use ontology::denormalized::Side;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::DenormalizedEdge;

use super::EmitOutput;
use super::helpers::{node_filter_predicates, node_select_columns};
use crate::passes::plan::*;
use crate::passes::shared::deleted_false;

pub(super) fn emit_denormalized(plan: &Plan, mat: &DenormalizedEdge) -> Result<EmitOutput> {
    let hop = plan
        .hops
        .first()
        .ok_or_else(|| QueryError::Lowering("denormalized plan has no hop".into()))?;
    let alias = DENORMALIZED_ALIAS;

    // The join row's `_deleted` already folds in both endpoints' liveness.
    let mut where_parts = vec![deleted_false(alias)];
    let mut select = Vec::new();
    for node_alias in [&mat.source_node, &mat.target_node] {
        let np = plan.nodes.get(node_alias).ok_or_else(|| {
            QueryError::Lowering(format!("denormalized endpoint '{node_alias}' not in plan"))
        })?;
        where_parts.extend(node_filter_predicates(node_alias, np));
        select.extend(node_select_columns(node_alias, np));
    }
    if let Some(prefix) = &hop.scope_prefix {
        where_parts.push(Expr::func(
            "startsWith",
            vec![
                Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                Expr::string(prefix.as_str()),
            ],
        ));
    }

    if matches!(plan.body, PlanBody::Traversal) {
        select.extend(edge_columns(plan, mat, alias));
    }

    Ok(EmitOutput {
        from: TableRef::scan_final(&mat.table, alias),
        edge_aliases: vec![alias.to_string()],
        where_parts,
        select,
        ctes: vec![],
        edge_if_predicates: None,
    })
}

/// The per-hop edge columns the graph formatter expects, synthesized from the
/// join row since there is no edge-table scan to select them from.
fn edge_columns(plan: &Plan, mat: &DenormalizedEdge, alias: &str) -> [SelectExpr; 5] {
    let entity = |node: &str| {
        plan.nodes
            .get(node)
            .and_then(|np| np.entity.clone())
            .unwrap_or_default()
    };
    [
        SelectExpr::new(
            Expr::col(alias, RELATIONSHIP_KIND_COLUMN),
            format!("{alias}_{EDGE_TYPE_SUFFIX}"),
        ),
        SelectExpr::new(
            Expr::col(
                alias,
                format!("{}{DEFAULT_PRIMARY_KEY}", Side::Source.prefix()),
            ),
            format!("{alias}_{EDGE_SRC_SUFFIX}"),
        ),
        SelectExpr::new(
            Expr::string(entity(&mat.source_node)),
            format!("{alias}_{EDGE_SRC_TYPE_SUFFIX}"),
        ),
        SelectExpr::new(
            Expr::col(
                alias,
                format!("{}{DEFAULT_PRIMARY_KEY}", Side::Target.prefix()),
            ),
            format!("{alias}_{EDGE_DST_SUFFIX}"),
        ),
        SelectExpr::new(
            Expr::string(entity(&mat.target_node)),
            format!("{alias}_{EDGE_DST_TYPE_SUFFIX}"),
        ),
    ]
}
