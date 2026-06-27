use crate::ast::*;
use crate::error::{QueryError, Result};

use super::EmitOutput;
use super::helpers::{latest_node_predicates, node_select_columns};
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

    let from = TableRef::scan_final(table, alias);
    let where_parts = latest_node_predicates(alias, np);
    let select = node_select_columns(alias, np);

    Ok(EmitOutput {
        from,
        edge_aliases: vec![],
        where_parts,
        select,
        ctes: vec![],
        edge_if_predicates: None,
    })
}
