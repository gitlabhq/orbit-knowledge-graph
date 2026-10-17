use std::collections::HashSet;

use query_data_model::ClickHouseDataModel;

use super::{HydrationCompileOptions, QueryPlan, context::PlanningContext};
use crate::{Input, Result, input::QueryType};

pub fn plan(
    input: &Input,
    model: &ClickHouseDataModel,
    hydration_options: HydrationCompileOptions,
    table_scans: &HashSet<String>,
) -> Result<QueryPlan> {
    let context = PlanningContext::new(input, model);
    match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => {
            let mut plan = super::edge_chain::plan(context, table_scans)?;
            match &mut plan {
                QueryPlan::Traversal(traversal) => {
                    super::latest::resolve(&mut traversal.operation.execution, model);
                }
                QueryPlan::Aggregation(aggregation) => {
                    super::latest::resolve(&mut aggregation.operation.execution, model);
                }
                _ => {}
            }
            Ok(plan)
        }
        QueryType::Neighbors => super::neighbors::plan_neighbors(context).map(QueryPlan::Neighbors),
        QueryType::PathFinding => {
            super::pathfinding::plan_pathfinding(context).map(QueryPlan::PathFinding)
        }
        QueryType::Hydration => {
            super::hydration::plan_hydration(context, hydration_options).map(QueryPlan::Hydration)
        }
    }
}
