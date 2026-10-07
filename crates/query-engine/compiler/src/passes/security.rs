use crate::error::{QueryError, Result};
use crate::query_graph::{
    BlockId, BlockView, ColumnRef, Expression, OperationKind, QueryGraph, RelationId, Source,
};
pub use crate::types::SecurityContext;
use orbit_utils::traversal_path::TraversalPathTrie;
use query_data_model::QueryDataModel;
use std::convert::Infallible;

pub(crate) fn require_authorized_paths(context: &SecurityContext) -> Result<()> {
    if context.traversal_paths.is_empty() {
        return Err(QueryError::Security(
            "security context has no traversal_path entries".into(),
        ));
    }
    Ok(())
}

fn path_predicate<'a>(
    column: ColumnRef<'a>,
    minimum_access: u32,
    context: &SecurityContext,
) -> Expression<'a> {
    let paths = context.paths_at_least(minimum_access);
    TraversalPathTrie::from_paths(&paths)
        .to_minimal_prefixes()
        .iter()
        .map(|path| {
            Expression::StartsWith(
                Box::new(Expression::Column(column)),
                Box::new(Expression::Text(path.as_str().into())),
            )
        })
        .reduce(|left, right| Expression::Or(Box::new(left), Box::new(right)))
        .unwrap_or(Expression::Boolean(false))
}

pub(crate) fn scan_predicate<'a, M: QueryDataModel + ?Sized>(
    block: &BlockView<'_, 'a, M>,
    relation: RelationId,
    context: &SecurityContext,
) -> Result<Option<Expression<'a>>> {
    let Source::Stored(table) = block.relation(relation)?.source else {
        return Ok(None);
    };
    if !block.catalog.table_has_path_columns(table.name()) {
        return Ok(None);
    }
    Ok(Some(path_predicate(
        block.stored_column(relation, ontology::TRAVERSAL_PATH_COLUMN)?,
        block.catalog.table_minimum_access_level(table.name()),
        context,
    )))
}

pub fn apply_graph_security<'a, M: QueryDataModel + ?Sized>(
    graph: QueryGraph<'a, M, Infallible>,
    root: BlockId,
    context: &SecurityContext,
) -> Result<QueryGraph<'a, M, Infallible>> {
    require_authorized_paths(context)?;
    graph.rewrite_operations(root, |graph, operation| {
        let OperationKind::Source { relation, .. } = operation.kind() else {
            return Ok(operation);
        };
        let Source::Stored(table) = graph.relation(*relation)?.source else {
            return Ok(operation);
        };
        if !graph.catalog().table_has_path_columns(table.name()) {
            return Ok(operation);
        }
        let predicate = path_predicate(
            graph.stored_column(*relation, ontology::TRAVERSAL_PATH_COLUMN)?,
            graph.catalog().table_minimum_access_level(table.name()),
            context,
        );
        Ok(graph.filter_relation(operation, predicate)?)
    })
}
