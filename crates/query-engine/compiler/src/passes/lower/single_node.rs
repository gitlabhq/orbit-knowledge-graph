//! Emit: single node (no edges).

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::EmitOutput;
use super::helpers::{build_dedup_subquery, node_select_columns};
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

    let from = TableRef::Subquery {
        query: Box::new(build_dedup_subquery(
            alias,
            table,
            vec![SelectExpr::star()],
            np,
        )),
        alias: alias.to_string(),
    };

    // Only _deleted=false in the outer WHERE — user filters are inside the dedup.
    let where_parts = vec![crate::passes::shared::deleted_false(alias)];
    let select = node_select_columns(alias, np);

    Ok(EmitOutput {
        from,
        edge_aliases: vec![],
        where_parts,
        select,
        ctes: vec![],
    })
}
