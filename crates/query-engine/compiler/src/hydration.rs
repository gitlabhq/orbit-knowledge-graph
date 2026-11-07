//! A hydration request: one single-entity logical plan per entity (an id pin and the columns to
//! read), each with the caller scope its rows are read under.

use orbit_utils::traversal_path::TraversalPath;
use query_data_model::Model;
use query_data_model::plan::logical::limits::Limits;
use query_data_model::plan::logical::{Columns, LogicalPlan, PlanError, Projection};

use crate::error::Result;

const CALLER_CAPPED: Limits = Limits {
    pins: usize::MAX,
    columns: usize::MAX,
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
        let catalog = model.logical().with_limits(CALLER_CAPPED);
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
