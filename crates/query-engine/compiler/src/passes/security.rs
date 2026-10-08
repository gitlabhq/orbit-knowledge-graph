use crate::error::{QueryError, Result};
use crate::query_graph::{Column, Expr, LoweredGraph, OperationKind, QueryId, lit};
pub use crate::types::SecurityContext;
use orbit_utils::traversal_path::TraversalPathTrie;
use query_data_model::QueryDataModel;

pub(crate) fn require_authorized_paths(context: &SecurityContext) -> Result<()> {
    if context.traversal_paths.is_empty() {
        return Err(QueryError::Security(
            "security context has no traversal_path entries".into(),
        ));
    }
    Ok(())
}

pub(crate) fn path_predicate(
    column: &Column,
    minimum_access: u32,
    context: &SecurityContext,
) -> Expr {
    let paths = context.paths_at_least(minimum_access);
    TraversalPathTrie::from_paths(&paths)
        .to_minimal_prefixes()
        .iter()
        .map(|path| column.starts_with(path.as_str()))
        .reduce(Expr::or)
        .unwrap_or_else(|| lit(false))
}

pub fn apply_graph_security<'a, M: QueryDataModel + ?Sized>(
    graph: LoweredGraph<'a, M>,
    root: QueryId,
    context: &SecurityContext,
) -> Result<LoweredGraph<'a, M>> {
    require_authorized_paths(context)?;
    graph.rewrite(root, |q, rows| {
        let OperationKind::Scan { table, .. } = rows.kind() else {
            return Ok(rows);
        };
        if !q.catalog().table_has_path_columns(table.name()) {
            return Ok(rows);
        }
        let path = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
        let predicate = path_predicate(
            &path,
            q.catalog().table_minimum_access_level(table.name()),
            context,
        );
        Ok(q.filter(rows, predicate)?)
    })
}
