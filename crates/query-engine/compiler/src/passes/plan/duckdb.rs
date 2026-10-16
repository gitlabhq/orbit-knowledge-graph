use query_data_model::DuckDbDataModel;

use super::{HydrationCompileOptions, QueryPlan, context::PlanningContext};
use crate::{Input, Result, input::QueryType};

pub fn plan(
    input: &Input,
    model: &DuckDbDataModel,
    hydration_options: HydrationCompileOptions,
) -> Result<QueryPlan> {
    let context = PlanningContext::new(input, model);
    match input.query_type {
        QueryType::Traversal => super::traversal::plan(context),
        QueryType::Aggregation => super::aggregation::plan(context),
        QueryType::Neighbors => super::neighbors::plan_neighbors(context).map(|mut plan| {
            plan.operation.fused_table = None;
            QueryPlan::Neighbors(plan)
        }),
        QueryType::PathFinding => {
            super::pathfinding::plan_pathfinding(context).map(QueryPlan::PathFinding)
        }
        QueryType::Hydration => {
            super::hydration::plan_hydration(context, hydration_options).map(QueryPlan::Hydration)
        }
    }
}
