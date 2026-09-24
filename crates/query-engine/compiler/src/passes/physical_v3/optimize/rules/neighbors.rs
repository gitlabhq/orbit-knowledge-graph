use super::super::super::*;
use crate::passes::logical_v3::Plan;

pub fn apply(mut plan: PhysicalPlan<ClickHouse>) -> PhysicalPlan<ClickHouse> {
    if !matches!(plan.op, PhysicalOp::Union) || plan.inputs.len() != 2 {
        return plan;
    }
    let [left, right] = plan.inputs.as_slice() else {
        return plan;
    };
    let (PhysicalOp::Project(left_columns), PhysicalOp::Project(right_columns)) =
        (&left.op, &right.op)
    else {
        return plan;
    };
    let (Some((outgoing, outgoing_input)), Some((incoming, incoming_input))) =
        (filter(left), filter(right))
    else {
        return plan;
    };
    if outgoing_input != incoming_input || left_columns.len() != right_columns.len() {
        return plan;
    }
    plan.inputs = vec![Plan::unary(
        PhysicalOp::FusedNeighbors {
            outgoing_predicate: outgoing,
            incoming_predicate: incoming,
            columns: left_columns.clone(),
        },
        outgoing_input,
    )];
    plan
}

fn filter(plan: &PhysicalPlan<ClickHouse>) -> Option<(crate::passes::logical_v3::Expr, PhysicalPlan<ClickHouse>)> {
    let filter = plan.inputs.first()?;
    let PhysicalOp::Filter(predicate) = &filter.op else {
        return None;
    };
    Some((predicate.clone(), filter.inputs.first()?.clone()))
}
