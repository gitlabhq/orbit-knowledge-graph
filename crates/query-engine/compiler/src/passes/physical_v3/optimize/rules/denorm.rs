use super::super::super::*;
use crate::passes::logical_v3::{column, Expr, Plan, Value};

pub fn apply<B: Backend>(plan: PhysicalPlan<B>, catalog: &PhysicalCatalog<'_>) -> PhysicalPlan<B> {
    plan.transform_up(&mut |plan| {
        if !matches!(plan.op, PhysicalOp::Scan { .. }) {
            return plan;
        }
        let Some(relation) = plan.relation_id() else {
            return plan;
        };
        let filters = catalog.denormalized_filters(relation);
        if filters.is_empty() {
            plan
        } else {
            Plan::unary(
                PhysicalOp::Filter(Expr::And(
                    filters
                        .into_iter()
                        .map(|filter| Expr::ListContains {
                            list: Box::new(column(filter.edge, filter.column)),
                            value: Value::String(filter.token),
                        })
                        .collect(),
                )),
                plan,
            )
        }
    })
}
