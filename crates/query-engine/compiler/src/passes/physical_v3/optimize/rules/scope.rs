use super::super::super::*;
use crate::passes::logical_v3::Plan;

pub fn apply<B: Backend>(plan: PhysicalPlan<B>, catalog: &PhysicalCatalog<'_>) -> PhysicalPlan<B> {
    let Some(anchor) = catalog.scope_anchor() else {
        return plan;
    };
    let before = plan.visible_relations();
    let rewritten = rewrite_joins(plan, &mut |join| {
        if join.required(anchor.relation) {
            return;
        }
        join.remove(anchor.edge);
        join.remove(anchor.relation);
    });
    let after = rewritten.visible_relations();
    if before.contains(&anchor.relation)
        && !after.contains(&anchor.relation)
        && !after.contains(&anchor.edge)
    {
        Plan::unary(PhysicalOp::ScopeGuard(anchor.prefix.clone()), rewritten)
    } else {
        rewritten
    }
}
