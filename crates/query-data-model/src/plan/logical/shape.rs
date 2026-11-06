use super::limits::RESERVED_PREFIX;
use super::{LogicalPlan, PlanError};

pub(super) fn plan(plan: &LogicalPlan) -> Result<(), PlanError> {
    reserved(plan)
}

fn reserved(plan: &LogicalPlan) -> Result<(), PlanError> {
    let names = plan
        .nodes()
        .flat_map(|node| plan.outputs(node))
        .filter_map(|output| plan.name(output));
    for name in names {
        if format!("{name}_").starts_with(RESERVED_PREFIX) {
            return Err(PlanError::shape(format!(
                "identifier {name:?} conflicts with reserved prefix {RESERVED_PREFIX:?}"
            )));
        }
    }
    Ok(())
}
