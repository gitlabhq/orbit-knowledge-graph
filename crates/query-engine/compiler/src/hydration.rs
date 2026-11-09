//! A hydration request: one single-entity logical plan per entity (an id pin and the columns to
//! read), each with the caller scope its rows are read under.

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::Model;
use query_data_model::plan::logical::limits::{Limits, MAX_COLUMNS};
use query_data_model::plan::logical::{Columns, LogicalPlan, PlanError, Projection};

use crate::constants::MAX_DYNAMIC_HYDRATION_RESULTS;
use crate::error::Result;

const HYDRATION_LIMITS: Limits = Limits {
    pins: MAX_DYNAMIC_HYDRATION_RESULTS,
    columns: MAX_COLUMNS,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct HydrationCompileOptions {
    pub dynamic: bool,
    pub path_segment_budget: Option<usize>,
}

pub struct HydrationRequest {
    pub arms: Vec<HydrationArm>,
    pub options: HydrationCompileOptions,
}

pub struct HydrationArm {
    pub plan: LogicalPlan,
    pub traversal_paths: Vec<TraversalPath>,
}

impl HydrationArm {
    pub fn new(
        model: &Model,
        entity: &str,
        ids: &[i64],
        columns: &[String],
        traversal_paths: Vec<TraversalPath>,
    ) -> Result<Self> {
        let catalog = model.logical().with_limits(HYDRATION_LIMITS);
        let plan = catalog.build(|q| {
            let declared = catalog.entity(entity)?;
            let name = q.internal_name("hydrate")?;
            let rows = q.entities_as(declared, &name)?;
            let binding = rows.endpoint()?;
            let id = binding.property("id")?;
            let rows = q.pin(rows, id, ids)?;
            let entity = binding.entity().ok_or(PlanError::NoEntity)?;
            let listed = columns
                .iter()
                .map(|column| Ok(model.property(entity, column)?.id))
                .collect::<std::result::Result<_, PlanError>>()?;
            let projection = Projection::Graph {
                binding: binding.output(),
                columns: Columns::Listed(listed),
            };
            q.select(rows, [projection.named(&*name)])
        })?;
        Ok(Self {
            plan,
            traversal_paths,
        })
    }
}

#[cfg(test)]
mod tests {
    use query_data_model::{Catalog, ClickHouse};

    use super::*;

    #[test]
    fn every_entity_hydrates_within_the_column_cap() {
        let model = Catalog::<ClickHouse>::embedded().unwrap();
        for entity in model.model().graph().entities() {
            assert!(
                entity.properties.len() <= HYDRATION_LIMITS.columns,
                "{} has {} properties",
                entity.name,
                entity.properties.len()
            );
        }
    }
}
