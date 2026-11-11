use crate::passes::codegen::SqlBackend;
use query_data_model::Catalog;
use query_data_model::Model;
use query_data_model::plan::logical::{Columns, Projection};

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::logical::Query;
use orbit_authorization::Grants;

/// Each arm reads an entity the caller may read, through listed columns that pass the same
/// field checks as a query's.
pub fn restrict_hydration(
    request: &HydrationRequest,
    logical: &Model,
    model: &Catalog<impl SqlBackend>,
    grants: &Grants,
) -> Result<()> {
    for arm in &request.arms {
        let query = Query::new(&arm.plan, logical);
        for binding in query.bindings() {
            super::readable(binding.entity, model, grants)?;
        }
        let expanded = query.selected().into_iter().all(|projection| {
            !matches!(
                projection,
                Projection::Graph {
                    columns: Columns::All | Columns::Default,
                    ..
                }
            )
        });
        if !expanded {
            return Err(QueryError::PipelineInvariant(
                "hydration requires expanded columns".into(),
            ));
        }
        super::properties::fields(query, model, grants.caller().admin())?;
    }
    Ok(())
}
