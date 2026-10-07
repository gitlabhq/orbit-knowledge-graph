use crate::error::{QueryError, Result};
use crate::query_graph::{
    BlockId, BlockView, Expression, LoweredOperation, QueryGraph, RelationId, Source,
};
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
    graph: &mut QueryGraph<'a, M, Expression<'a>, LoweredOperation<'a>>,
    root: BlockId,
    context: &SecurityContext,
) -> Result<()> {
    require_authorized_paths(context)?;
    graph.walk_operations_mut(root, |block, operation| {
        if let LoweredOperation::Source { relation, .. } = operation
            && let Some(predicate) = scan_predicate(block, *relation, context)?
        {
            *operation = std::mem::replace(operation, LoweredOperation::One).filter(predicate);
        }
        Ok(())
    })
}
