use crate::passes::codegen::SqlBackend;
use query_data_model::Catalog;
use query_data_model::Model;
use query_data_model::plan::logical::{Columns, Projection};

use crate::error::{QueryError, Result};
use crate::hydration::HydrationRequest;
use crate::logical::Query;
use crate::types::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};

pub fn restrict_hydration(
    request: &HydrationRequest,
    logical: &Model,
    model: &Catalog<impl SqlBackend>,
    security_ctx: &SecurityContext,
) -> Result<()> {
    let graph = model.graph();
    for arm in &request.arms {
        let query = Query::new(&arm.plan, logical);
        for binding in query.bindings() {
            let entity = graph.entity(binding.entity).name.as_str();
            if let Some(min_role) = model
                .min_role(binding.entity)
                .filter(|level| *level > DEFAULT_PATH_ACCESS_LEVEL)
                && security_ctx.paths_at_least(min_role).is_empty()
            {
                return Err(QueryError::Authorization(format!(
                    "{entity} requires access level {min_role} on at least one authorized path"
                )));
            }
            if security_ctx.admin {
                continue;
            }
            let columns = query
                .selected()
                .into_iter()
                .filter_map(|projection| match projection {
                    Projection::Graph {
                        binding: projected,
                        columns,
                    } if *projected == binding.output => Some(columns),
                    _ => None,
                });
            for columns in columns {
                let Columns::Listed(listed) = columns else {
                    return Err(QueryError::PipelineInvariant(
                        "hydration requires expanded columns".into(),
                    ));
                };
                if let Some(column) = listed
                    .iter()
                    .find(|property| model.admin_only(**property))
                    .map(|property| graph.property(*property).name.as_str())
                {
                    return Err(QueryError::Restrict(format!(
                        "column \"{column}\" for {entity}: field requires administrator access"
                    )));
                }
            }
        }
    }
    Ok(())
}
