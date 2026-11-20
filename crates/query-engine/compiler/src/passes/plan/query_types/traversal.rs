//! Traversal: plan the pattern, build its relation graph, and return it as the operation.

use query_data_model::OrbitQueryModel;

use crate::error::Result;
use crate::input::Input;
use crate::passes::plan::graph::ExecutionPlan;
use crate::passes::plan::pattern::Pattern;
use crate::passes::plan::{Plan, TopK};

pub struct Traversal {
    pub execution: ExecutionPlan,
}

pub(crate) fn plan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
    top_k: TopK,
) -> Result<Plan<Traversal>> {
    let pattern = Pattern::new(input, model)?;
    let execution = pattern.execution(top_k)?;
    Ok(pattern.into_plan(execution, |execution| Traversal { execution }))
}
