use crate::error::{QueryError, Result};
use crate::query_graph::{
    BlockId, BlockView, Expression, OperationKind, QueryGraph, RelationId, Source,
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
    let paths = context.paths_at_least(block.catalog.table_minimum_access_level(table.name()));
    let paths = TraversalPathTrie::from_paths(&paths).to_minimal_prefixes();
    let column = block.stored_column(relation, ontology::TRAVERSAL_PATH_COLUMN)?;
    Ok(Some(
        paths
            .iter()
            .map(|path| {
                Expression::StartsWith(
                    Box::new(Expression::Column(column)),
                    Box::new(Expression::Text(path.as_str().into())),
                )
            })
            .reduce(|left, right| Expression::Or(Box::new(left), Box::new(right)))
            .unwrap_or(Expression::Boolean(false)),
    ))
}

pub fn apply_graph_security<'a, M: QueryDataModel + ?Sized>(
    graph: &mut QueryGraph<'a, M, Infallible>,
    root: BlockId,
    context: &SecurityContext,
) -> Result<()> {
    require_authorized_paths(context)?;
    let mut restrictions = Vec::new();
    graph.walk_operations(root, |block, operation, _| {
        if let OperationKind::Source { relation, .. } = operation.kind()
            && let Some(predicate) = scan_predicate(block, *relation, context)?
        {
            restrictions.push((*relation, predicate));
        }
        Ok::<_, QueryError>(())
    })?;
    for (relation, predicate) in restrictions {
        graph.restrict_scan(relation, predicate)?;
    }
    Ok(())
}
