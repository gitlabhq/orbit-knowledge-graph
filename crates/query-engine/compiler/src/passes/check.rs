use crate::error::{QueryError, Result};
use crate::types::SecurityContext;

pub fn check_graph<'a, M: query_data_model::QueryDataModel + ?Sized>(
    graph: &crate::query_graph::QueryGraph<
        'a,
        M,
        crate::query_graph::Expression<'a>,
        crate::query_graph::LoweredOperation<'a>,
    >,
    root: crate::query_graph::BlockId,
    context: &SecurityContext,
) -> Result<()> {
    use crate::query_graph::LoweredOperation;
    crate::passes::security::require_authorized_paths(context)?;
    graph.walk_operations(root, |block, operation, parent| {
        if let LoweredOperation::Source { relation, .. } = operation
            && let Some(expected) = crate::passes::security::scan_predicate(block, *relation, context)?
            && !matches!(parent, Some(LoweredOperation::Filter { predicate, .. }) if *predicate == expected)
        {
            return Err(QueryError::Security(
                "post-check failed: scan missing authorized path predicate".into(),
            ));
        }
        Ok(())
    })
}
