use super::super::super::*;
use crate::passes::logical_v3::Plan;

pub fn apply(
    plan: PhysicalPlan<ClickHouse>,
    catalog: &PhysicalCatalog<'_>,
) -> PhysicalPlan<ClickHouse> {
    let edge_count = plan
        .visible_relations()
        .into_iter()
        .filter(|relation| matches!(catalog.relation(*relation), Some(Relation::Edge { .. })))
        .count();
    plan.transform_up(&mut |plan| {
        if matches!(plan.op, PhysicalOp::Deduplicate { .. }) {
            return plan;
        }
        let Some(relation) = plan.relation_id() else {
            return plan;
        };
        let strategy = match catalog.dedup(relation, edge_count) {
            DedupRequirement::None => return plan,
            DedupRequirement::Final => ClickHouseDedup::Final,
            DedupRequirement::LimitBy => ClickHouseDedup::LimitBy,
        };
        Plan::unary(
            PhysicalOp::Deduplicate {
                keys: catalog
                    .sort_key(relation)
                    .iter()
                    .map(|key| crate::passes::logical_v3::column(relation, key))
                    .collect(),
                strategy,
            },
            plan,
        )
    })
}
