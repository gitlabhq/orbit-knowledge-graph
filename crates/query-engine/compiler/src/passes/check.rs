use crate::error::{QueryError, Result};
use crate::query_graph::{LoweredGraph, OperationKind, QueryId, Rows};
use crate::types::SecurityContext;
use query_data_model::QueryDataModel;

pub fn check_graph<'a, M: QueryDataModel + ?Sized>(
    graph: &LoweredGraph<'a, M>,
    root: QueryId,
    context: &SecurityContext,
) -> Result<()> {
    super::security::require_authorized_paths(context)?;
    let graph = graph.graph();
    for query in graph.reachable(root)? {
        check_rows(graph.catalog(), graph.rows(query)?, None, context)?;
    }
    Ok(())
}

fn check_rows(
    model: &(impl QueryDataModel + ?Sized),
    rows: &Rows<'_>,
    parent: Option<&Rows<'_>>,
    context: &SecurityContext,
) -> Result<()> {
    if let OperationKind::Scan { table, .. } = rows.kind()
        && model.table_has_path_columns(table.name())
    {
        let column = rows.column(ontology::TRAVERSAL_PATH_COLUMN)?;
        let expected = super::security::path_predicate(
            &column,
            model.table_minimum_access_level(table.name()),
            context,
        );
        let Some(OperationKind::Filter { predicate, .. }) = parent.map(Rows::kind) else {
            return Err(QueryError::Security(
                "post-check failed: scan missing authorized path predicate".into(),
            ));
        };
        if predicate != &expected {
            return Err(QueryError::Security(
                "post-check failed: scan has a different authorized path predicate".into(),
            ));
        }
    }
    for input in rows.inputs() {
        check_rows(model, input, Some(rows), context)?;
    }
    Ok(())
}
