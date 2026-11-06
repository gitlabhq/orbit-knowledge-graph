use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Input};
use crate::types::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};

pub fn restrict_hydration(
    input: &Input,
    model: &impl OrbitQueryModel,
    security_ctx: &SecurityContext,
) -> Result<()> {
    for node in &input.nodes {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };
        if let Some(min_role) = model
            .entity_minimum_access_level(entity)
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
        let columns = match &node.columns {
            Some(ColumnSelection::List(columns)) => columns.as_slice(),
            Some(ColumnSelection::All) => {
                return Err(QueryError::PipelineInvariant(
                    "hydration requires expanded columns".into(),
                ));
            }
            None => &[],
        };
        let filtered = node.filters.keys();
        if let Some(column) = columns
            .iter()
            .chain(filtered)
            .find(|column| model.admin_only(entity, column))
        {
            return Err(QueryError::Restrict(format!(
                "column \"{column}\" for {entity}: field requires administrator access"
            )));
        }
    }
    Ok(())
}
