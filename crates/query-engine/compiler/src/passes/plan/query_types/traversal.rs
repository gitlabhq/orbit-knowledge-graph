//! Traversal: plan the pattern, build its relation graph, and return it as the operation.

use query_data_model::OrbitQueryModel;

use crate::error::Result;
use crate::input::Input;
use crate::passes::plan::Plan;
use crate::passes::plan::graph::ExecutionPlan;
use crate::passes::plan::pattern::Pattern;

pub struct Traversal {
    pub execution: ExecutionPlan,
}

pub(crate) fn plan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
) -> Result<Plan<Traversal>> {
    let pattern = Pattern::new(input, model)?;
    let execution = pattern.execution()?;
    Ok(pattern.into_plan(execution, |execution| Traversal { execution }))
}
