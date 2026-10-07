use crate::error::{QueryError, Result};
use crate::query_graph::{BlockId, OperationKind, QueryGraph};
use crate::types::SecurityContext;
use query_data_model::QueryDataModel;
use std::convert::Infallible;

pub fn check_graph<'a, M: QueryDataModel + ?Sized>(
    graph: &QueryGraph<'a, M, Infallible>,
    root: BlockId,
    context: &SecurityContext,
) -> Result<()> {
    super::security::require_authorized_paths(context)?;
    graph.walk_operations(root, |block, operation, parent| {
        if let OperationKind::Source { relation, .. } = operation.kind()
            && let Some(expected) = super::security::scan_predicate(block, *relation, context)?
            && !matches!(parent.map(|operation| operation.kind()), Some(OperationKind::Filter { predicate, .. }) if *predicate == expected)
        {
            return Err(QueryError::Security("post-check failed: scan missing authorized path predicate".into()));
        }
        Ok(())
    })
}
