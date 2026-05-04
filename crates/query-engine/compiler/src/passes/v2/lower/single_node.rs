//! Emit: single node (no edges).

use crate::ast::*;
use crate::error::{QueryError, Result};

use super::super::plan::*;
use super::EmitOutput;
use super::helpers::{build_dedup_subquery, node_select_columns, node_where_predicates};

pub(super) fn emit_single_node(plan: &EdgeChainPlan) -> Result<EmitOutput> {
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
