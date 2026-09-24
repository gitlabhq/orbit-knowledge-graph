use super::super::super::*;
use crate::passes::logical_v3::Plan;
use std::collections::HashMap;

pub fn apply<B: Backend>(plan: PhysicalPlan<B>) -> PhysicalPlan<B> {
    let required = plan.required_columns();
    let mut columns: HashMap<_, Vec<_>> = HashMap::new();
    for column in required {
        columns.entry(column.relation).or_default().push(column.name);
    }
    plan.transform_up(&mut |plan| {
        let Some(relation) = plan.relation_id() else {
            return plan;
        };
        if !matches!(plan.op, PhysicalOp::Scan { .. }) {
            return plan;
        }
        let Some(mut required) = columns.get(&relation).cloned() else {
            return plan;
        };
        required.sort();
        required.dedup();
        Plan::unary(PhysicalOp::ReadColumns(required), plan)
    })
}
