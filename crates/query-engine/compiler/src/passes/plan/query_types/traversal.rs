//! Traversal: plan the pattern, build its relation graph, and return it as the operation.

use query_data_model::OrbitQueryModel;

use crate::error::Result;
use crate::input::Input;
use crate::logical::Query;
use crate::passes::plan::Plan;
use crate::passes::plan::graph::ExecutionPlan;
use crate::passes::plan::pattern::Pattern;

pub struct Traversal {
    pub execution: ExecutionPlan,
}

pub(crate) fn plan<'a, M: OrbitQueryModel + ?Sized>(
    query: Query<'a>,
    input: &'a Input,
    model: &'a M,
) -> Result<Plan<Traversal>> {
    let pattern = Pattern::new(query, input, model)?;
    let execution = pattern.execution()?;
    Ok(pattern.into_plan(execution, |execution| Traversal { execution }))
}
