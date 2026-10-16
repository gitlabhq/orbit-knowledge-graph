use query_data_model::OrbitQueryModel;

use super::context::PlanningContext;
use super::{QueryPlan, Traversal, edge_relation};
use crate::error::Result;

pub(super) fn plan<M>(context: PlanningContext<'_, M>) -> Result<QueryPlan>
where
    M: OrbitQueryModel + ?Sized,
{
    let planned = edge_relation::plan(context)?;
    Ok(QueryPlan::Traversal(planned.context.finish(Traversal {
        execution: planned.execution,
    })))
}
