//! Emit: single node (no edges).

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::EmitOutput;
use super::helpers::{latest_node_predicates, latest_node_scan, node_select_columns};
use crate::passes::plan::*;

pub(super) fn emit_single_node(plan: &Plan) -> Result<EmitOutput> {
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

    let sort_key = plan.table_sort_keys.get(table).map(|v| v.as_slice());
    let where_parts = latest_node_predicates(alias, np);
    let from = latest_node_scan(table, alias, where_parts, sort_key);
    let select = node_select_columns(alias, np);

    Ok(EmitOutput {
        from,
        edge_aliases: vec![],
        where_parts: vec![],
        select,
        ctes: vec![],
        edge_if_predicates: None,
    })
}
